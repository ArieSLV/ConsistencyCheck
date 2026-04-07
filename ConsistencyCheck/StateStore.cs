using System.Globalization;
using Raven.Client.Documents;
using Raven.Client.Documents.BulkInsert;
using Raven.Client.Documents.Indexes;
using Raven.Client.Exceptions.Database;
using Raven.Client.Exceptions.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Indexes;
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

    internal async Task EnsureLocalIndexesAsync(CancellationToken ct)
    {
        await _databaseStore.Maintenance
            .SendAsync(
                new PutIndexesOperation(
                [
                    NodeDocumentSnapshots.BuildProblemDocumentsIndexDefinition(),
                    SnapshotImportSkippedIndexes.BuildByRunAndOriginalDocumentIdIndexDefinition(),
                    UnsafeSnapshotIdIndexes.BuildByRunAndOriginalDocumentIdIndexDefinition(),
                    RepairStateChangedIndexes.BuildByApplyRunAndDocumentIdIndexDefinition(),
                    MismatchDocumentIndexes.BuildByRunAndDocumentIdIndexDefinition(),
                    MismatchDocumentIndexes.BuildUnhealthyDocumentsIndexDefinition(),
                    MismatchDocumentIndexes.BuildManualReviewDocumentsIndexDefinition(),
                    MismatchDocumentIndexes.BuildAutomaticRepairPlanDocumentsIndexDefinition(),
                    MismatchDocumentIndexes.BuildByRunIndexDefinition(),
                    MismatchDocumentIndexes.BuildSkippedAmbiguousCvIndexDefinition(),
                    MismatchDocumentIndexes.BuildSkippedCollectionMismatchIndexDefinition(),
                    MismatchDocumentIndexes.BuildSkippedAlreadyPlannedIndexDefinition(),
                    RunStateDocumentIndexes.BuildLiveETagScanRangesIndexDefinition(),
                    RepairDocumentIndexes.BuildBlockedApplyExecutionCountsIndexDefinition()
                ]),
                ct)
            .ConfigureAwait(false);
    }

    internal BulkInsertOperation OpenSnapshotBulkInsert()
        => _databaseStore.BulkInsert(new BulkInsertOptions
        {
            SkipOverwriteIfUnchanged = true
        });

    internal async Task ClearLatestSnapshotsAsync(CancellationToken ct)
    {
        var operation = await _databaseStore.Operations
            .SendAsync(new DeleteByQueryOperation($"from {NodeDocumentSnapshots.CollectionName}"), token: ct)
            .ConfigureAwait(false);

        await operation.WaitForCompletionAsync(ct).ConfigureAwait(false);
    }

    internal async Task WaitForSnapshotProblemIndexAsync(TimeSpan? timeout, CancellationToken ct)
    {
        var deadline = timeout is { } finiteTimeout && finiteTimeout != Timeout.InfiniteTimeSpan
            ? DateTimeOffset.UtcNow + finiteTimeout
            : (DateTimeOffset?)null;
        while (true)
        {
            ct.ThrowIfCancellationRequested();

            var status = await GetSnapshotProblemIndexStatusAsync(ct).ConfigureAwait(false);
            if (status.IsStale == false)
                return;

            if (deadline.HasValue && DateTimeOffset.UtcNow >= deadline.Value)
                throw new TimeoutException($"Timed out waiting for index '{NodeDocumentSnapshots.ProblemDocumentsIndexName}' to become non-stale.");

            await Task.Delay(TimeSpan.FromSeconds(1), ct).ConfigureAwait(false);
        }
    }

    internal async Task<SnapshotProblemIndexStatus> GetSnapshotProblemIndexStatusAsync(CancellationToken ct)
    {
        try
        {
            var stats = await _databaseStore.Maintenance
                .SendAsync(new GetIndexStatisticsOperation(NodeDocumentSnapshots.ProblemDocumentsIndexName), ct)
                .ConfigureAwait(false);

            return new SnapshotProblemIndexStatus(
                IsStale: stats.IsStale,
                EntriesCount: stats.EntriesCount,
                LastIndexingTime: NormalizeIndexTimestamp(stats.LastIndexingTime),
                LastQueryingTime: NormalizeIndexTimestamp(stats.LastQueryingTime));
        }
        catch (IndexDoesNotExistException)
        {
            return new SnapshotProblemIndexStatus(
                IsStale: true,
                EntriesCount: 0,
                LastIndexingTime: null,
                LastQueryingTime: null);
        }
    }

    internal async Task<IReadOnlyList<ProblemDocumentIndexEntry>> LoadProblemDocumentCandidatesAsync(
        int configuredNodeCount,
        int start,
        int pageSize,
        CancellationToken ct)
    {
        using var session = _databaseStore.OpenAsyncSession();
        var query = session.Advanced.AsyncRawQuery<ProblemDocumentIndexEntry>(
                $"""
                 from index '{NodeDocumentSnapshots.ProblemDocumentsIndexName}'
                 where Count < $configuredNodeCount or ChangeVectorsCount > 1
                 order by OriginalDocumentId
                 """)
            .AddParameter("configuredNodeCount", configuredNodeCount)
            .Skip(start)
            .Take(pageSize);

        return await query.ToListAsync(ct).ConfigureAwait(false);
    }

    internal async Task<long> GetSnapshotDocumentCountAsync(CancellationToken ct)
    {
        var stats = await _databaseStore.Maintenance
            .SendAsync(new GetCollectionStatisticsOperation(), ct)
            .ConfigureAwait(false);

        return stats.Collections.TryGetValue(NodeDocumentSnapshots.CollectionName, out var count)
            ? count
            : 0;
    }

    internal async Task<Dictionary<string, NodeDocumentSnapshot?>> LoadNodeSnapshotsByIdsAsync(
        IReadOnlyCollection<string> snapshotIds,
        CancellationToken ct)
    {
        if (snapshotIds.Count == 0)
            return new Dictionary<string, NodeDocumentSnapshot?>(StringComparer.OrdinalIgnoreCase);

        using var session = _databaseStore.OpenAsyncSession();
        var loaded = await session.LoadAsync<NodeDocumentSnapshot>(snapshotIds, ct).ConfigureAwait(false);
        var result = new Dictionary<string, NodeDocumentSnapshot?>(loaded.Count, StringComparer.OrdinalIgnoreCase);
        foreach (var (id, snapshot) in loaded)
            result[id] = snapshot;

        return result;
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

    internal async Task<IReadOnlyList<RepairPlanRunInfo>> LoadCompletedRepairPlanRunsAsync(
        string customerDatabaseName,
        int take,
        CancellationToken ct)
    {
        var runs = await LoadStartingWithAsync<RunStateDocument>("runs/", ct).ConfigureAwait(false);

        return runs
            .Where(run =>
                string.Equals(run.CustomerDatabaseName, customerDatabaseName, StringComparison.OrdinalIgnoreCase) &&
                ((run.RunMode == RunMode.DryRunRepair && run.IsComplete) ||
                 (run.RunMode == RunMode.LiveETagScan && run.RepairsPlanned > 0) ||
                 (run.RunMode == RunMode.SnapshotCrossCheck && run.RepairsPlanned > 0)))
            .OrderByDescending(run => run.CompletedAt ?? DateTimeOffset.MinValue)
            .ThenByDescending(run => run.StartedAt)
            .Take(take)
            .Select(run => new RepairPlanRunInfo
            {
                RunId = string.IsNullOrWhiteSpace(run.RunId) ? run.Id : run.RunId,
                CustomerDatabaseName = run.CustomerDatabaseName,
                StartedAt = run.StartedAt,
                CompletedAt = run.CompletedAt,
                MismatchesFound = run.MismatchesFound,
                RepairsPlanned = run.RepairsPlanned
            })
            .ToList();
    }

    internal async Task<IReadOnlyList<ScanResumeRunInfo>> LoadIncompleteSnapshotImportRunsAsync(
        string customerDatabaseName,
        int take,
        CancellationToken ct)
    {
        var runs = await LoadStartingWithAsync<RunStateDocument>("runs/", ct).ConfigureAwait(false);

        return runs
            .Where(run =>
                string.Equals(run.CustomerDatabaseName, customerDatabaseName, StringComparison.OrdinalIgnoreCase) &&
                ScanExecutionState.CanResumeDirectSnapshotImport(run))
            .OrderByDescending(run => run.LastSavedAt)
            .ThenByDescending(run => run.StartedAt)
            .Take(take)
            .Select(run => new ScanResumeRunInfo
            {
                RunId = string.IsNullOrWhiteSpace(run.RunId) ? run.Id : run.RunId,
                RunMode = run.RunMode,
                StartedAt = run.StartedAt,
                LastSavedAt = NormalizeOptionalTimestamp(run.LastSavedAt),
                SnapshotDocumentsImported = run.SnapshotDocumentsImported,
                CurrentPhase = run.CurrentPhase,
                SnapshotImportCompleted = run.SnapshotImportCompleted
            })
            .ToList();
    }

    internal async Task<IReadOnlyList<SnapshotCacheDownloadRunInfo>> LoadIncompleteSnapshotCacheDownloadRunsAsync(
        string customerDatabaseName,
        int take,
        CancellationToken ct)
    {
        var runs = await LoadStartingWithAsync<RunStateDocument>("runs/", ct).ConfigureAwait(false);

        return runs
            .Where(run =>
                string.Equals(run.CustomerDatabaseName, customerDatabaseName, StringComparison.OrdinalIgnoreCase) &&
                ScanExecutionState.CanResumeSnapshotCacheDownload(run))
            .OrderByDescending(run => run.LastSavedAt)
            .ThenByDescending(run => run.StartedAt)
            .Take(take)
            .Select(run => new SnapshotCacheDownloadRunInfo
            {
                RunId = string.IsNullOrWhiteSpace(run.RunId) ? run.Id : run.RunId,
                RunMode = run.RunMode,
                StartedAt = run.StartedAt,
                LastSavedAt = NormalizeOptionalTimestamp(run.LastSavedAt),
                CurrentPhase = run.CurrentPhase,
                DownloadedRows = run.SnapshotCacheNodes.Sum(node => node.DownloadedRows),
                CompletedNodes = run.SnapshotCacheNodes.Count(node => node.IsDownloadComplete)
            })
            .ToList();
    }

    internal async Task<IReadOnlyList<CachedSnapshotImportRunInfo>> LoadIncompleteCachedSnapshotImportRunsAsync(
        string customerDatabaseName,
        int take,
        CancellationToken ct)
    {
        var runs = await LoadStartingWithAsync<RunStateDocument>("runs/", ct).ConfigureAwait(false);

        return runs
            .Where(run =>
                string.Equals(run.CustomerDatabaseName, customerDatabaseName, StringComparison.OrdinalIgnoreCase) &&
                ScanExecutionState.CanResumeCachedSnapshotImport(run))
            .OrderByDescending(run => run.LastSavedAt)
            .ThenByDescending(run => run.StartedAt)
            .Take(take)
            .Select(run => new CachedSnapshotImportRunInfo
            {
                RunId = string.IsNullOrWhiteSpace(run.RunId) ? run.Id : run.RunId,
                RunMode = run.RunMode,
                SourceSnapshotCacheRunId = run.SourceSnapshotCacheRunId ?? string.Empty,
                StartedAt = run.StartedAt,
                LastSavedAt = NormalizeOptionalTimestamp(run.LastSavedAt),
                CurrentPhase = run.CurrentPhase,
                SnapshotDocumentsImported = run.SnapshotDocumentsImported,
                SnapshotDocumentsSkipped = run.SnapshotDocumentsSkipped,
                SnapshotBulkInsertRestarts = run.SnapshotBulkInsertRestarts
            })
            .ToList();
    }

    internal async Task<IReadOnlyList<AnalysisRunInfo>> LoadIncompleteAnalysisRunsAsync(
        string customerDatabaseName,
        RunMode runMode,
        int take,
        CancellationToken ct)
    {
        var runs = await LoadStartingWithAsync<RunStateDocument>("runs/", ct).ConfigureAwait(false);

        return runs
            .Where(run =>
                string.Equals(run.CustomerDatabaseName, customerDatabaseName, StringComparison.OrdinalIgnoreCase) &&
                run.RunMode == runMode &&
                ScanExecutionState.CanResumeAnalysis(run))
            .OrderByDescending(run => run.LastSavedAt)
            .ThenByDescending(run => run.StartedAt)
            .Take(take)
            .Select(run => new AnalysisRunInfo
            {
                RunId = string.IsNullOrWhiteSpace(run.RunId) ? run.Id : run.RunId,
                RunMode = run.RunMode,
                SourceSnapshotCacheRunId = run.SourceSnapshotCacheRunId,
                SourceSnapshotRunId = run.SourceSnapshotRunId ?? string.Empty,
                StartedAt = run.StartedAt,
                LastSavedAt = NormalizeOptionalTimestamp(run.LastSavedAt),
                CurrentPhase = run.CurrentPhase,
                CandidateDocumentsFound = run.CandidateDocumentsFound,
                CandidateDocumentsProcessed = run.CandidateDocumentsProcessed,
                MismatchesFound = run.MismatchesFound,
                RepairsPlanned = run.RepairsPlanned,
                RepairsPatchedOnWinner = run.RepairsPatchedOnWinner,
                RepairsFailed = run.RepairsFailed
            })
            .ToList();
    }

    internal async Task<IReadOnlyList<RepairDocument>> LoadExecutableRepairPlanDocumentsAsync(
        string sourcePlanRunId,
        CancellationToken ct)
    {
        var repairs = await LoadStartingWithAsync<RepairDocument>($"repairs/{sourcePlanRunId}/", ct).ConfigureAwait(false);

        return repairs
            .Where(repair =>
                string.Equals(repair.RunId, sourcePlanRunId, StringComparison.Ordinal) &&
                string.Equals(repair.RepairStatus, "PatchPlannedDryRun", StringComparison.Ordinal))
            .OrderBy(repair => repair.WinnerNode, StringComparer.Ordinal)
            .ThenBy(repair => repair.Collection, StringComparer.Ordinal)
            .ThenBy(repair => repair.Id, StringComparer.Ordinal)
            .ToList();
    }

    internal async Task<IReadOnlyList<ApplyRepairRunInfo>> LoadIncompleteApplyRepairRunsAsync(
        string customerDatabaseName,
        string sourceRepairPlanRunId,
        int take,
        CancellationToken ct)
    {
        var runs = await LoadStartingWithAsync<RunStateDocument>("runs/", ct).ConfigureAwait(false);

        return runs
            .Where(run =>
                string.Equals(run.CustomerDatabaseName, customerDatabaseName, StringComparison.OrdinalIgnoreCase) &&
                run.RunMode == RunMode.ApplyRepairPlan &&
                run.IsComplete == false &&
                string.Equals(run.SourceRepairPlanRunId, sourceRepairPlanRunId, StringComparison.Ordinal))
            .OrderByDescending(run => run.LastSavedAt)
            .ThenByDescending(run => run.StartedAt)
            .Take(take)
            .Select(run => new ApplyRepairRunInfo
            {
                RunId = string.IsNullOrWhiteSpace(run.RunId) ? run.Id : run.RunId,
                SourceRepairPlanRunId = run.SourceRepairPlanRunId ?? string.Empty,
                StartedAt = run.StartedAt,
                LastSavedAt = NormalizeOptionalTimestamp(run.LastSavedAt),
                RepairsPlanned = run.RepairsPlanned,
                RepairsAttempted = run.RepairsAttempted,
                RepairsPatchedOnWinner = run.RepairsPatchedOnWinner,
                RepairsFailed = run.RepairsFailed
            })
            .ToList();
    }

    internal async Task<IReadOnlyList<RepairDocument>> LoadApplyExecutionDocumentsAsync(
        string applyRunId,
        string sourceRepairPlanRunId,
        CancellationToken ct)
    {
        var repairs = await LoadStartingWithAsync<RepairDocument>($"repairs/{applyRunId}/", ct).ConfigureAwait(false);

        return repairs
            .Where(repair =>
                string.Equals(repair.RunId, applyRunId, StringComparison.Ordinal) &&
                string.Equals(repair.SourceRepairPlanRunId, sourceRepairPlanRunId, StringComparison.Ordinal))
            .OrderBy(repair => repair.Id, StringComparer.Ordinal)
            .ToList();
    }

    public async Task<RunStateDocument> CreateRunAsync(
        AppConfig config,
        long startEtag,
        ChangeVectorSemanticsSnapshot? semanticsSnapshot,
        CancellationToken ct)
    {
        var now = DateTimeOffset.UtcNow;
        var runId = CreateRunId(now);
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
            SnapshotCacheNodes = config.Nodes.Select((node, index) => new SnapshotCacheNodeState
            {
                NodeUrl = node.Url,
                NodeLabel = node.Label,
                NodeAlias = NodeDocumentSnapshots.GetNodeAlias(index),
                CurrentSegmentId = 1,
                LastUpdatedAt = now
            }).ToList(),
            SnapshotImportNodes = config.Nodes.Select(node => new SnapshotImportNodeState
            {
                NodeUrl = node.Url,
                NodeLabel = node.Label,
                CurrentSegmentId = 1,
                CurrentPageNumber = 1,
                LastUpdatedAt = now
            }).ToList(),
            ChangeVectorSemanticsSnapshot = semanticsSnapshot == null ? null : CloneSemanticsSnapshot(semanticsSnapshot),
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
        ApplyRunHeadState(head, run);
        head.UpdatedAt = now;
        await session.StoreAsync(head, head.Id, ct).ConfigureAwait(false);

        await session.SaveChangesAsync(ct).ConfigureAwait(false);
        return run;
    }

    public async Task SaveRunAsync(RunStateDocument run, CancellationToken ct)
        => await PersistBatchAsync(
                run,
                Array.Empty<MismatchDocument>(),
                Array.Empty<RepairDocument>(),
                Array.Empty<RepairActionGuardDocument>(),
                Array.Empty<RepairStateChangedDocument>(),
                Array.Empty<SnapshotImportSkippedDocument>(),
                Array.Empty<DiagnosticDocument>(),
                ct)
            .ConfigureAwait(false);

    public async Task PersistBatchAsync(
        RunStateDocument run,
        IReadOnlyCollection<MismatchDocument> mismatches,
        IReadOnlyCollection<RepairDocument> repairs,
        IReadOnlyCollection<RepairActionGuardDocument> guards,
        IReadOnlyCollection<RepairStateChangedDocument> stateChangedDocuments,
        IReadOnlyCollection<SnapshotImportSkippedDocument> skippedSnapshots,
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

        foreach (var stateChangedDocument in stateChangedDocuments)
            await session.StoreAsync(stateChangedDocument, stateChangedDocument.Id, ct).ConfigureAwait(false);

        foreach (var skippedSnapshot in skippedSnapshots)
            await session.StoreAsync(skippedSnapshot, skippedSnapshot.Id, ct).ConfigureAwait(false);

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

        ApplyRunHeadState(head, run);
        head.UpdatedAt = DateTimeOffset.UtcNow;

        await session.StoreAsync(head, head.Id, ct).ConfigureAwait(false);
        await session.SaveChangesAsync(ct).ConfigureAwait(false);
    }

    public async Task PersistBatchAsync(
        RunStateDocument run,
        IReadOnlyCollection<MismatchDocument> mismatches,
        IReadOnlyCollection<RepairDocument> repairs,
        IReadOnlyCollection<RepairActionGuardDocument> guards,
        IReadOnlyCollection<RepairStateChangedDocument> stateChangedDocuments,
        IReadOnlyCollection<DiagnosticDocument> diagnostics,
        CancellationToken ct)
        => await PersistBatchAsync(run, mismatches, repairs, guards, stateChangedDocuments, [], diagnostics, ct).ConfigureAwait(false);

    public async Task PersistBatchAsync(
        RunStateDocument run,
        IReadOnlyCollection<MismatchDocument> mismatches,
        IReadOnlyCollection<RepairDocument> repairs,
        IReadOnlyCollection<RepairActionGuardDocument> guards,
        IReadOnlyCollection<SnapshotImportSkippedDocument> skippedSnapshots,
        IReadOnlyCollection<DiagnosticDocument> diagnostics,
        CancellationToken ct)
        => await PersistBatchAsync(run, mismatches, repairs, guards, [], skippedSnapshots, diagnostics, ct).ConfigureAwait(false);

    public async Task PersistBatchAsync(
        RunStateDocument run,
        IReadOnlyCollection<MismatchDocument> mismatches,
        IReadOnlyCollection<RepairDocument> repairs,
        IReadOnlyCollection<RepairActionGuardDocument> guards,
        IReadOnlyCollection<DiagnosticDocument> diagnostics,
        CancellationToken ct)
        => await PersistBatchAsync(run, mismatches, repairs, guards, [], [], diagnostics, ct).ConfigureAwait(false);

    public async Task<HashSet<string>> GetGuardedDocumentIdsAsync(
        string runId,
        IReadOnlyCollection<string> documentIds,
        CancellationToken ct)
    {
        var guarded = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        if (documentIds.Count == 0)
            return guarded;

        var ids = documentIds.Select(id => GetRepairActionGuardId(runId, id)).ToArray();
        using var session = _databaseStore.OpenAsyncSession();
        var docs = await session.LoadAsync<RepairActionGuardDocument>(ids, ct).ConfigureAwait(false);

        foreach (var doc in docs.Values)
        {
            if (doc != null)
                guarded.Add(doc.DocumentId);
        }

        return guarded;
    }

    /// <summary>
    /// Loads all DocumentIds that were successfully patched across ALL runs.
    /// Used by SnapshotCrossCheck to skip already-repaired documents.
    /// </summary>
    internal async Task<HashSet<string>> LoadSuccessfullyRepairedDocumentIdsAsync(CancellationToken ct)
    {
        var repairs = await LoadStartingWithAsync<RepairDocument>("repairs/", ct).ConfigureAwait(false);
        var ids = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var repair in repairs)
        {
            if (string.Equals(repair.RepairStatus, "PatchCompletedOnWinner", StringComparison.Ordinal))
            {
                foreach (var docId in repair.DocumentIds)
                    ids.Add(docId);
            }
        }
        return ids;
    }

    /// <summary>
    /// Loads one page of NodeDocumentSnapshot for a given node alias,
    /// ordered by OriginalDocumentId, using offset-based pagination via RQL limit.
    /// </summary>
    internal async Task<List<NodeDocumentSnapshot>> LoadSnapshotPageByNodeAliasAsync(
        string nodeAlias,
        int skip,
        int pageSize,
        CancellationToken ct)
    {
        using var session = _databaseStore.OpenAsyncSession();
        return await session.Advanced
            .AsyncRawQuery<NodeDocumentSnapshot>(
                $"from index '{NodeDocumentSnapshots.ByFromNodeAndOriginalDocumentIdIndexName}' where FromNode == $alias limit $skip, $pageSize")
            .AddParameter("alias", nodeAlias)
            .AddParameter("skip", skip)
            .AddParameter("pageSize", pageSize)
            .ToListAsync(ct).ConfigureAwait(false);
    }

    internal async Task<Dictionary<string, NodeDocumentSnapshot?>> LoadSnapshotsByDocumentIdsAsync(
        string[] documentIds, string[] nodeAliases, CancellationToken ct)
    {
        var ids = documentIds
            .SelectMany(docId => nodeAliases.Select(alias => NodeDocumentSnapshots.GetSnapshotId(docId, alias)))
            .ToArray();
        using var session = _databaseStore.OpenAsyncSession();
        var result = await session.LoadAsync<NodeDocumentSnapshot>(ids, ct).ConfigureAwait(false);
        return result!;
    }

    /// <summary>
    /// Loads all MismatchDocuments for a specific run using ID-prefix scan.
    /// Used by MismatchDecisionFixup to enumerate and correct mismatches overwritten
    /// with "SkippedAlreadyPlanned" due to the cross-node duplicate encounter bug.
    /// </summary>
    internal async Task<List<MismatchDocument>> LoadMismatchesByRunIdPrefixAsync(string runId, CancellationToken ct)
        => await LoadStartingWithAsync<MismatchDocument>($"mismatches/{runId}/", ct).ConfigureAwait(false);

    /// <summary>
    /// Loads all SnapshotCrossCheck runs for the given customer database, ordered newest-first.
    /// Used to populate the run-selection prompt in MismatchDecisionFixup mode.
    /// </summary>
    internal async Task<IReadOnlyList<AnalysisRunInfo>> LoadSnapshotCrossCheckRunsForFixupAsync(
        string customerDatabaseName,
        int take,
        CancellationToken ct)
    {
        var runs = await LoadStartingWithAsync<RunStateDocument>("runs/", ct).ConfigureAwait(false);

        return runs
            .Where(run =>
                string.Equals(run.CustomerDatabaseName, customerDatabaseName, StringComparison.OrdinalIgnoreCase) &&
                run.RunMode == RunMode.SnapshotCrossCheck)
            .OrderByDescending(run => run.CompletedAt ?? run.LastSavedAt)
            .ThenByDescending(run => run.StartedAt)
            .Take(take)
            .Select(run => new AnalysisRunInfo
            {
                RunId = string.IsNullOrWhiteSpace(run.RunId) ? run.Id : run.RunId,
                RunMode = run.RunMode,
                SourceSnapshotRunId = run.SourceSnapshotRunId ?? string.Empty,
                StartedAt = run.StartedAt,
                LastSavedAt = NormalizeOptionalTimestamp(run.LastSavedAt),
                CurrentPhase = run.CurrentPhase,
                CandidateDocumentsFound = run.CandidateDocumentsFound,
                CandidateDocumentsProcessed = run.CandidateDocumentsProcessed,
                MismatchesFound = run.MismatchesFound,
                RepairsPlanned = run.RepairsPlanned
            })
            .ToList();
    }

    public async Task StoreRepairActionGuardsAsync(
        IReadOnlyCollection<RepairActionGuardDocument> guards,
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

    internal async Task<IReadOnlyList<MismatchDocument>> LoadMismatchesByRunAndDocumentIdsAsync(
        string runId,
        IReadOnlyCollection<string> documentIds,
        CancellationToken ct)
    {
        if (documentIds.Count == 0)
            return [];

        var normalizedDocumentIds = documentIds
            .Where(id => string.IsNullOrWhiteSpace(id) == false)
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToArray();
        if (normalizedDocumentIds.Length == 0)
            return [];

        var loadedByDocumentId = new Dictionary<string, MismatchDocument>(StringComparer.OrdinalIgnoreCase);

        using (var deterministicLoadSession = _databaseStore.OpenAsyncSession())
        {
            var deterministicIds = normalizedDocumentIds
                .Select(documentId => GetMismatchId(runId, documentId))
                .ToArray();
            var deterministicMatches = await deterministicLoadSession.LoadAsync<MismatchDocument>(deterministicIds, ct)
                .ConfigureAwait(false);

            foreach (var mismatch in deterministicMatches.Values)
            {
                if (mismatch == null || string.IsNullOrWhiteSpace(mismatch.DocumentId))
                    continue;

                loadedByDocumentId[mismatch.DocumentId] = mismatch;
            }
        }

        var missingDocumentIds = normalizedDocumentIds
            .Where(documentId => loadedByDocumentId.ContainsKey(documentId) == false)
            .ToArray();
        if (missingDocumentIds.Length == 0)
            return loadedByDocumentId.Values.ToList();

        using var lookupSession = _databaseStore.OpenAsyncSession();
        var lookupEntries = await lookupSession.Advanced.AsyncRawQuery<MismatchDocumentLookupEntry>(
                $"""
                 from index '{MismatchDocumentIndexes.ByRunAndDocumentIdIndexName}'
                 where RunId = $runId
                   and DocumentId in ($documentIds)
                 """)
            .AddParameter("runId", runId)
            .AddParameter("documentIds", missingDocumentIds)
            .ToListAsync(ct)
            .ConfigureAwait(false);

        if (lookupEntries.Count == 0)
            return loadedByDocumentId.Values.ToList();

        var ids = lookupEntries
            .Select(entry => entry.Id)
            .Where(id => string.IsNullOrWhiteSpace(id) == false)
            .Distinct(StringComparer.Ordinal)
            .ToArray();

        using var loadSession = _databaseStore.OpenAsyncSession();
        var loaded = await loadSession.LoadAsync<MismatchDocument>(ids, ct).ConfigureAwait(false);
        foreach (var group in loaded.Values
                     .Where(document => document != null)
                     .Cast<MismatchDocument>()
                     .GroupBy(document => document.DocumentId, StringComparer.OrdinalIgnoreCase))
        {
            loadedByDocumentId[group.Key] = SelectPreferredMismatch(group);
        }

        return loadedByDocumentId.Values.ToList();
    }

    internal async Task<HashSet<string>> LoadSkippedOriginalDocumentIdsAsync(
        string runId,
        CancellationToken ct)
    {
        using var session = _databaseStore.OpenAsyncSession();
        var entries = await session.Advanced.AsyncRawQuery<SnapshotImportSkippedLookupEntry>(
                $"""
                 from index '{SnapshotImportSkippedIndexes.ByRunAndOriginalDocumentIdIndexName}'
                 where RunId = $runId
                 """)
            .AddParameter("runId", runId)
            .ToListAsync(ct)
            .ConfigureAwait(false);

        return entries
            .Select(entry => entry.OriginalDocumentId)
            .Where(id => string.IsNullOrWhiteSpace(id) == false)
            .ToHashSet(StringComparer.OrdinalIgnoreCase);
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

    public async Task StoreUnsafeSnapshotIdsAsync(
        IReadOnlyCollection<UnsafeSnapshotIdDocument> unsafeSnapshotIds,
        CancellationToken ct)
    {
        if (unsafeSnapshotIds.Count == 0)
            return;

        using var session = _databaseStore.OpenAsyncSession();
        foreach (var unsafeSnapshotId in unsafeSnapshotIds)
            await session.StoreAsync(unsafeSnapshotId, unsafeSnapshotId.Id, ct).ConfigureAwait(false);

        await session.SaveChangesAsync(ct).ConfigureAwait(false);
    }

    public async Task StoreRepairStateChangedDocumentsAsync(
        IReadOnlyCollection<RepairStateChangedDocument> stateChangedDocuments,
        CancellationToken ct)
    {
        if (stateChangedDocuments.Count == 0)
            return;

        using var session = _databaseStore.OpenAsyncSession();
        foreach (var stateChangedDocument in stateChangedDocuments)
            await session.StoreAsync(stateChangedDocument, stateChangedDocument.Id, ct).ConfigureAwait(false);

        await session.SaveChangesAsync(ct).ConfigureAwait(false);
    }

    public async ValueTask DisposeAsync()
    {
        await Dedup.DisposeAsync().ConfigureAwait(false);
        _databaseStore.Dispose();
        _serverStore.Dispose();
    }

    public static string CreateRunId(DateTimeOffset startedAtUtc)
        => $"runs/{startedAtUtc.UtcDateTime.ToString("yyyyMMdd-HHmmssfffffff", CultureInfo.InvariantCulture)}";

    public static string GetMismatchId(string runId, string documentId)
        => $"mismatches/{runId}/{HashId(documentId)}";

    public static string CreateRepairId(string runId) => $"repairs/{runId}/{Guid.NewGuid():N}";

    public static string CreateApplyRepairExecutionId(string runId, string sourceRepairPlanId)
        => $"repairs/{runId}/{HashId(sourceRepairPlanId)}";

    public static string CreateDiagnosticId(string runId) => $"diagnostics/{runId}/{Guid.NewGuid():N}";

    public static string GetRepairActionGuardId(string runId, string documentId)
        => $"repair-guards/{runId}/{HashId(documentId)}";

    public static string CreateSnapshotImportSkippedId(string runId, string snapshotDocumentId)
        => $"snapshot-import-skipped/{runId}/{HashId(snapshotDocumentId)}";

    public static string CreateUnsafeSnapshotIdDocumentId(string runId, string fromNode, string originalDocumentId)
        => $"unsafe-snapshot-ids/{runId}/{HashId($"{fromNode}\n{originalDocumentId}")}";

    public static string CreateRepairStateChangedId(string applyRunId, string documentId)
        => $"repair-state-changed/{applyRunId}/{HashId(documentId)}";

    public static string CreateRepairPlanDocumentItemId(string sourceRepairPlanId, string documentId)
        => $"repair-plan-item/{HashId($"{sourceRepairPlanId}\n{documentId}")}";

    private static string GetRunHeadId(string customerDatabaseName)
        => $"run-heads/{HashId(customerDatabaseName)}";

    private static MismatchDocument SelectPreferredMismatch(IEnumerable<MismatchDocument> candidates)
    {
        return candidates
            .OrderByDescending(candidate => string.Equals(
                MismatchDocumentIndexes.ResolveCurrentRepairDecision(candidate),
                "PatchPlannedDryRun",
                StringComparison.Ordinal))
            .ThenByDescending(candidate => candidate.LastRepairUpdatedAt ?? DateTimeOffset.MinValue)
            .ThenByDescending(candidate => candidate.DetectedAt)
            .First();
    }

    private static void ApplyRunHeadState(RunHeadDocument head, RunStateDocument run)
    {
        switch (run.RunMode)
        {
            case RunMode.ImportSnapshots:
                if (run.IsComplete)
                {
                    head.PendingSnapshotRunId = null;
                    head.PendingSnapshotStartedAt = null;
                    head.ActiveSnapshotRunId = run.Id;
                    head.ActiveSnapshotCompletedAt = run.CompletedAt ?? run.LastSavedAt;
                    return;
                }

                head.PendingSnapshotRunId = run.Id;
                head.PendingSnapshotStartedAt = run.StartedAt;
                return;

            case RunMode.DownloadSnapshotsToCache:
                if (run.IsComplete)
                {
                    head.PendingSnapshotCacheRunId = null;
                    head.PendingSnapshotCacheStartedAt = null;
                    head.ActiveSnapshotCacheRunId = run.Id;
                    head.ActiveSnapshotCacheCompletedAt = run.CompletedAt ?? run.LastSavedAt;
                    return;
                }

                head.PendingSnapshotCacheRunId = run.Id;
                head.PendingSnapshotCacheStartedAt = run.StartedAt;
                return;

            case RunMode.ImportCachedSnapshotsToStateStore:
                if (run.IsComplete)
                {
                    head.PendingSnapshotRunId = null;
                    head.PendingSnapshotStartedAt = null;
                    head.ActiveSnapshotRunId = run.Id;
                    head.ActiveSnapshotCompletedAt = run.CompletedAt ?? run.LastSavedAt;
                    return;
                }

                head.PendingSnapshotRunId = run.Id;
                head.PendingSnapshotStartedAt = run.StartedAt;
                return;
        }
    }

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
            ApplyExecutionMode = config.ApplyExecutionMode,
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
        => ChangeVectorSemantics.CloneSnapshot(snapshot);

    private async Task<List<T>> LoadStartingWithAsync<T>(string idPrefix, CancellationToken ct) where T : class
    {
        const int PageSize = 1024;
        var results = new List<T>();

        for (var start = 0; ; start += PageSize)
        {
            using var session = _databaseStore.OpenAsyncSession();
            var page = (await session.Advanced
                    .LoadStartingWithAsync<T>(idPrefix, start: start, pageSize: PageSize, token: ct)
                    .ConfigureAwait(false))
                .OfType<T>()
                .ToList();

            if (page.Count == 0)
                break;

            results.AddRange(page);

            if (page.Count < PageSize)
                break;
        }

        return results;
    }

    private static DateTimeOffset? NormalizeOptionalTimestamp(DateTimeOffset timestamp)
        => timestamp == default ? null : timestamp;

    private static DateTimeOffset? NormalizeIndexTimestamp(DateTime? timestamp)
    {
        if (timestamp.HasValue == false)
            return null;

        var normalized = timestamp.Value.Kind == DateTimeKind.Unspecified
            ? DateTime.SpecifyKind(timestamp.Value, DateTimeKind.Utc)
            : timestamp.Value.ToUniversalTime();

        return new DateTimeOffset(normalized);
    }
}
