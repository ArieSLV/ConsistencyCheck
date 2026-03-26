using Raven.Client.Documents;
using Raven.Client.Exceptions.Database;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;

namespace ConsistencyCheck;

/// <summary>
/// RavenDB-backed durable storage for runs, dedup state, mismatches, repairs and diagnostics.
/// </summary>
public sealed class StateStore : IAsyncDisposable
{
    private readonly StateStoreConfig _config;
    private readonly IDocumentStore _serverStore;
    private readonly IDocumentStore _databaseStore;

    public StateStore(StateStoreConfig config)
    {
        _config = config;
        _serverStore = new DocumentStore
        {
            Urls = [config.ServerUrl]
        }.Initialize();

        _databaseStore = new DocumentStore
        {
            Urls = [config.ServerUrl],
            Database = config.DatabaseName
        }.Initialize();

        Dedup = new DedupBucketStore(_databaseStore);
    }

    public DedupBucketStore Dedup { get; }

    public async Task EnsureDatabaseExistsAsync(CancellationToken ct)
    {
        try
        {
            await _serverStore.Maintenance.Server.SendAsync(
                    new GetDatabaseRecordOperation(_config.DatabaseName), ct)
                .ConfigureAwait(false);
        }
        catch (DatabaseDoesNotExistException)
        {
            await _serverStore.Maintenance.Server.SendAsync(
                    new CreateDatabaseOperation(new DatabaseRecord(_config.DatabaseName)), ct)
                .ConfigureAwait(false);
        }
    }

    public async Task<RunHeadDocument?> LoadRunHeadAsync(string customerDatabaseName, CancellationToken ct)
    {
        using var session = _databaseStore.OpenAsyncSession();
        return await session.LoadAsync<RunHeadDocument>(GetRunHeadId(customerDatabaseName), ct)
            .ConfigureAwait(false);
    }

    public async Task<RunStateDocument?> LoadRunAsync(string runId, CancellationToken ct)
    {
        using var session = _databaseStore.OpenAsyncSession();
        return await session.LoadAsync<RunStateDocument>(runId, ct).ConfigureAwait(false);
    }

    public async Task<RunStateDocument> CreateRunAsync(
        AppConfig config,
        long startEtag,
        ChangeVectorSemanticsSnapshot semanticsSnapshot,
        CancellationToken ct)
    {
        var runId = $"runs/{Guid.NewGuid():N}";
        var now = DateTimeOffset.UtcNow;
        var run = new RunStateDocument
        {
            Id = runId,
            RunId = runId,
            CustomerDatabaseName = config.DatabaseName,
            ClusterNodes = config.Nodes.Select(n => new NodeConfig
            {
                Label = n.Label,
                Url = n.Url
            }).ToList(),
            RunMode = config.RunMode,
            ScanMode = config.Mode,
            StartedAt = now,
            LastSavedAt = now,
            NodeCursors = config.Nodes.Select(node => new NodeCursorState
            {
                NodeUrl = node.Url,
                NextEtag = startEtag
            }).ToList(),
            ChangeVectorSemanticsSnapshot = CloneSemanticsSnapshot(semanticsSnapshot),
            ConfigSnapshot = CloneConfig(config)
        };

        using var session = _databaseStore.OpenAsyncSession();
        await session.StoreAsync(run, run.Id, ct).ConfigureAwait(false);

        var headId = GetRunHeadId(config.DatabaseName);
        var head = await session.LoadAsync<RunHeadDocument>(headId, ct).ConfigureAwait(false) ?? new RunHeadDocument
        {
            Id = headId,
            CustomerDatabaseName = config.DatabaseName
        };
        head.LatestRunId = run.Id;
        head.UpdatedAt = now;
        await session.StoreAsync(head, head.Id, ct).ConfigureAwait(false);

        await session.SaveChangesAsync(ct).ConfigureAwait(false);
        return run;
    }

    public async Task SaveRunAsync(RunStateDocument run, CancellationToken ct)
    {
        await PersistBatchAsync(run, [], [], [], [], ct).ConfigureAwait(false);
    }

    public async Task PersistBatchAsync(
        RunStateDocument run,
        IReadOnlyCollection<MismatchDocument> mismatches,
        IReadOnlyCollection<RepairDocument> repairs,
        IReadOnlyCollection<RepairGuardDocument> guards,
        IReadOnlyCollection<DiagnosticDocument> diagnostics,
        CancellationToken ct)
    {
        run.LastSavedAt = DateTimeOffset.UtcNow;

        using var session = _databaseStore.OpenAsyncSession();
        await session.StoreAsync(run, run.Id, ct).ConfigureAwait(false);

        foreach (var mismatch in mismatches)
            await session.StoreAsync(mismatch, mismatch.Id, ct).ConfigureAwait(false);

        foreach (var repair in repairs)
            await session.StoreAsync(repair, repair.Id, ct).ConfigureAwait(false);

        foreach (var guard in guards)
            await session.StoreAsync(guard, guard.Id, ct).ConfigureAwait(false);

        if (_config.EnableDiagnostics)
        {
            foreach (var diagnostic in diagnostics)
                await session.StoreAsync(diagnostic, diagnostic.Id, ct).ConfigureAwait(false);
        }

        var headId = GetRunHeadId(run.CustomerDatabaseName);
        var head = await session.LoadAsync<RunHeadDocument>(headId, ct).ConfigureAwait(false) ?? new RunHeadDocument
        {
            Id = headId,
            CustomerDatabaseName = run.CustomerDatabaseName
        };

        head.LatestRunId = run.Id;
        if (run.IsComplete)
        {
            head.LastCompletedRunId = run.Id;
            head.LastCompletedSafeRestartEtag = run.SafeRestartEtag;
        }

        head.UpdatedAt = DateTimeOffset.UtcNow;

        await session.StoreAsync(head, head.Id, ct).ConfigureAwait(false);
        await session.SaveChangesAsync(ct).ConfigureAwait(false);
    }

    public async Task<HashSet<string>> GetPatchedDocumentIdsAsync(
        string runId,
        IReadOnlyCollection<string> documentIds,
        CancellationToken ct)
    {
        var guarded = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        if (documentIds.Count == 0)
            return guarded;

        var ids = documentIds.Select(id => GetRepairGuardId(runId, id)).ToArray();
        using var session = _databaseStore.OpenAsyncSession();
        var docs = await session.LoadAsync<RepairGuardDocument>(ids, ct).ConfigureAwait(false);

        foreach (var doc in docs.Values)
        {
            if (doc != null)
                guarded.Add(doc.DocumentId);
        }

        return guarded;
    }

    public async Task StoreRepairGuardsAsync(
        IReadOnlyCollection<RepairGuardDocument> guards,
        CancellationToken ct)
    {
        if (guards.Count == 0)
            return;

        using var session = _databaseStore.OpenAsyncSession();
        foreach (var guard in guards)
            await session.StoreAsync(guard, guard.Id, ct).ConfigureAwait(false);

        await session.SaveChangesAsync(ct).ConfigureAwait(false);
    }

    public async Task StoreMismatchesAsync(
        IReadOnlyCollection<MismatchDocument> mismatches,
        CancellationToken ct)
    {
        if (mismatches.Count == 0)
            return;

        using var session = _databaseStore.OpenAsyncSession();
        foreach (var mismatch in mismatches)
            await session.StoreAsync(mismatch, mismatch.Id, ct).ConfigureAwait(false);

        await session.SaveChangesAsync(ct).ConfigureAwait(false);
    }

    public async Task StoreRepairsAsync(
        IReadOnlyCollection<RepairDocument> repairs,
        CancellationToken ct)
    {
        if (repairs.Count == 0)
            return;

        using var session = _databaseStore.OpenAsyncSession();
        foreach (var repair in repairs)
            await session.StoreAsync(repair, repair.Id, ct).ConfigureAwait(false);

        await session.SaveChangesAsync(ct).ConfigureAwait(false);
    }

    public async Task StoreDiagnosticsAsync(
        IReadOnlyCollection<DiagnosticDocument> diagnostics,
        CancellationToken ct)
    {
        if (_config.EnableDiagnostics == false || diagnostics.Count == 0)
            return;

        using var session = _databaseStore.OpenAsyncSession();
        foreach (var diagnostic in diagnostics)
            await session.StoreAsync(diagnostic, diagnostic.Id, ct).ConfigureAwait(false);

        await session.SaveChangesAsync(ct).ConfigureAwait(false);
    }

    public async ValueTask DisposeAsync()
    {
        await Dedup.DisposeAsync().ConfigureAwait(false);
        _databaseStore.Dispose();
        _serverStore.Dispose();
    }

    public static string CreateMismatchId(string runId) => $"mismatches/{runId}/{Guid.NewGuid():N}";

    public static string CreateRepairId(string runId) => $"repairs/{runId}/{Guid.NewGuid():N}";

    public static string CreateDiagnosticId(string runId) => $"diagnostics/{runId}/{Guid.NewGuid():N}";

    public static string GetRepairGuardId(string runId, string documentId)
        => $"repair-guards/{runId}/{HashId(documentId)}";

    private static string GetRunHeadId(string customerDatabaseName)
        => $"run-heads/{HashId(customerDatabaseName)}";

    private static string HashId(string value)
        => Convert.ToHexString(System.Security.Cryptography.SHA256.HashData(System.Text.Encoding.UTF8.GetBytes(value)));

    private static AppConfig CloneConfig(AppConfig config)
    {
        return new AppConfig
        {
            DatabaseName = config.DatabaseName,
            Nodes = config.Nodes.Select(n => new NodeConfig
            {
                Label = n.Label,
                Url = n.Url
            }).ToList(),
            SourceNodeIndex = config.SourceNodeIndex,
            CertificateThumbprint = config.CertificateThumbprint,
            CertificatePath = config.CertificatePath,
            CertificatePassword = config.CertificatePassword,
            Mode = config.Mode,
            RunMode = config.RunMode,
            AllowInvalidServerCertificates = config.AllowInvalidServerCertificates,
            StartEtag = config.StartEtag,
            Throttle = new ThrottleConfig
            {
                PageSize = config.Throttle.PageSize,
                DelayBetweenBatchesMs = config.Throttle.DelayBetweenBatchesMs,
                MaxRetries = config.Throttle.MaxRetries,
                RetryBaseDelayMs = config.Throttle.RetryBaseDelayMs,
                ClusterLookupBatchSize = config.Throttle.ClusterLookupBatchSize
            },
            StateStore = new StateStoreConfig
            {
                ServerUrl = config.StateStore.ServerUrl,
                DatabaseName = config.StateStore.DatabaseName,
                EnableDiagnostics = config.StateStore.EnableDiagnostics
            }
        };
    }

    private static ChangeVectorSemanticsSnapshot CloneSemanticsSnapshot(ChangeVectorSemanticsSnapshot snapshot)
    {
        return new ChangeVectorSemanticsSnapshot
        {
            Nodes = snapshot.Nodes.Select(node => new ChangeVectorSemanticsNodeInfo
            {
                NodeUrl = node.NodeUrl,
                Label = node.Label,
                DatabaseId = node.DatabaseId,
                DatabaseChangeVector = node.DatabaseChangeVector
            }).ToList(),
            ExplicitUnusedDatabaseIds = snapshot.ExplicitUnusedDatabaseIds.ToList(),
            PotentialUnusedDatabaseIds = snapshot.PotentialUnusedDatabaseIds.ToList()
        };
    }
}
