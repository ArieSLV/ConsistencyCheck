using System.Security.Cryptography.X509Certificates;
using System.Globalization;
using System.Diagnostics;
using Polly;
using Polly.Retry;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Queries;
using Raven.Client.Exceptions;
using Sparrow.Json;

namespace ConsistencyCheck;

/// <summary>
/// Indexed scan engine that snapshots per-node metadata into the local state store,
/// discovers candidate mismatches via a local map-reduce index, then revalidates only
/// those candidates live against the customer cluster.
/// </summary>
public sealed class IndexedConsistencyChecker : IAsyncDisposable
{
    private static readonly TimeSpan AnalysisHeartbeatInterval = TimeSpan.FromSeconds(1);
    private static readonly TimeSpan SlowCandidatePageDiagnosticThreshold = TimeSpan.FromSeconds(5);

    private readonly AppConfig _config;
    private readonly StateStore _stateStore;
    private readonly X509Certificate2? _certificate;
    private readonly ResiliencePipeline _retry;
    private readonly Dictionary<string, IDocumentStore> _storesByNodeUrl;
    private readonly Dictionary<string, string> _labelsByNodeUrl;
    private readonly Dictionary<string, string> _aliasesByNodeUrl;
    private readonly ISnapshotBulkInsertSessionFactory _snapshotBulkInsertSessionFactory;
    private readonly SnapshotCacheStore _snapshotCacheStore;
    private readonly Dictionary<string, long> _snapshotImportExpectedCountsByNodeUrl = new(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<string, long> _snapshotCacheExpectedCountsByNodeUrl = new(StringComparer.OrdinalIgnoreCase);
    private readonly SemaphoreSlim _importCheckpointGate = new(1, 1);

    public IndexedConsistencyChecker(AppConfig config, StateStore stateStore)
    {
        _config = config;
        _stateStore = stateStore;
        _certificate = ConfigWizard.LoadCertificate(config);
        _retry = BuildRetryPipeline(config.Throttle);
        _snapshotBulkInsertSessionFactory = new StateStoreSnapshotBulkInsertSessionFactory(stateStore);
        _snapshotCacheStore = new SnapshotCacheStore(Path.Combine(AppContext.BaseDirectory, "output"));
        _storesByNodeUrl = config.Nodes.ToDictionary(
            node => node.Url,
            node => new DocumentStore
            {
                Urls = [node.Url],
                Database = config.DatabaseName,
                Certificate = _certificate,
                Conventions = new DocumentConventions
                {
                    DisableTopologyUpdates = true,
                    DisposeCertificate = false,
                    CreateHttpClient = handler =>
                    {
                        handler.ClientCertificateOptions = ClientCertificateOption.Manual;
                        return new HttpClient(handler, disposeHandler: true);
                    }
                }
            }.Initialize(),
            StringComparer.OrdinalIgnoreCase);
        _labelsByNodeUrl = config.Nodes.ToDictionary(
            node => node.Url,
            node => node.Label,
            StringComparer.OrdinalIgnoreCase);
        _aliasesByNodeUrl = config.Nodes
            .Select((node, index) => new KeyValuePair<string, string>(node.Url, NodeDocumentSnapshots.GetNodeAlias(index)))
            .ToDictionary(static pair => pair.Key, static pair => pair.Value, StringComparer.OrdinalIgnoreCase);
    }

    internal event Action<IndexedRunProgressUpdate>? ProgressUpdated;

    public async Task<RunStateDocument> RunAsync(RunStateDocument run, CancellationToken ct)
        => run.RunMode switch
        {
            RunMode.ImportSnapshots
                => await RunImportStageAsync(run, ct).ConfigureAwait(false),
            RunMode.DownloadSnapshotsToCache
                => await RunDownloadCacheStageAsync(run, ct).ConfigureAwait(false),
            RunMode.ImportCachedSnapshotsToStateStore
                => await RunCachedSnapshotImportStageAsync(run, ct).ConfigureAwait(false),
            _
                => await RunAnalysisStageAsync(run, ct).ConfigureAwait(false)
        };

    private async Task<RunStateDocument> RunDownloadCacheStageAsync(RunStateDocument run, CancellationToken ct)
    {
        var selectedNode = ResolveSelectedImportNode();
        EnsureSnapshotCacheNodeStates(run);
        var selectedAlias = _aliasesByNodeUrl[selectedNode.Url];

        run.CurrentPhase = ScanExecutionPhase.SnapshotCacheDownload;
        run.IsComplete = false;
        run.CompletedAt = null;
        await _stateStore.SaveRunAsync(run, ct).ConfigureAwait(false);

        var setManifest = await _snapshotCacheStore
            .LoadOrCreateSetManifestAsync(_config.DatabaseName, run.RunId, _config.Nodes, ct)
            .ConfigureAwait(false);
        var nodeManifest = await _snapshotCacheStore
            .LoadOrCreateNodeManifestAsync(_config.DatabaseName, run.RunId, selectedNode, selectedAlias, ct)
            .ConfigureAwait(false);
        await _snapshotCacheStore.PrepareNodeForResumeAsync(nodeManifest, ct).ConfigureAwait(false);

        var expectedDocumentCount = await LoadSnapshotCacheExpectedCountAsync(selectedNode, ct).ConfigureAwait(false);
        _snapshotCacheExpectedCountsByNodeUrl.Clear();
        _snapshotCacheExpectedCountsByNodeUrl[selectedNode.Url] = expectedDocumentCount;

        PublishProgress(
            "downloading snapshots to cache",
            selectedNode.Label,
            $"loading metadata-only pages from {selectedNode.Label}",
            currentNodeStreamedSnapshots: nodeManifest.DownloadedRows,
            currentNodeTotalDocuments: expectedDocumentCount,
            run: run);

        await DownloadNodeSnapshotsToCacheAsync(run, selectedNode, nodeManifest, expectedDocumentCount, ct).ConfigureAwait(false);

        var finalNodeState = GetSnapshotCacheNodeState(run, selectedNode);
        finalNodeState.IsDownloadComplete = true;
        finalNodeState.DownloadCompletedAt = DateTimeOffset.UtcNow;
        finalNodeState.LastUpdatedAt = DateTimeOffset.UtcNow;
        UpsertSnapshotCacheNodeState(run, finalNodeState);
        await _snapshotCacheStore.MarkNodeDownloadCompleteAsync(nodeManifest, ct).ConfigureAwait(false);
        await _snapshotCacheStore.SaveSetManifestAsync(
                _snapshotCacheStore.BuildSetManifest(_config.DatabaseName, run.RunId, run.SnapshotCacheNodes),
                ct)
            .ConfigureAwait(false);

        var completedNodes = run.SnapshotCacheNodes.Count(nodeState => nodeState.IsDownloadComplete);
        if (completedNodes == _config.Nodes.Count)
        {
            run.CurrentPhase = ScanExecutionPhase.Completed;
            run.IsComplete = true;
            run.CompletedAt = DateTimeOffset.UtcNow;
        }

        await _stateStore.PersistBatchAsync(
                run,
                [],
                [],
                [],
                CreateDiagnostics(CreateDiagnostic(
                    run.RunId,
                    "SnapshotCacheDownloadSummary",
                    $"Complete={run.IsComplete}, DownloadedRows={run.SnapshotCacheNodes.Sum(node => node.DownloadedRows):N0}, DownloadedNodes={completedNodes}/{_config.Nodes.Count}.",
                    selectedNode.Url)),
                ct)
            .ConfigureAwait(false);

        PublishProgress(
            run.IsComplete ? "cache download complete" : "partial cache download complete",
            selectedNode.Label,
            run.IsComplete
                ? "all configured nodes were downloaded to the local cache"
                : $"downloaded {completedNodes:N0}/{_config.Nodes.Count:N0} nodes into the local cache",
            currentNodeStreamedSnapshots: finalNodeState.DownloadedRows,
            currentNodeTotalDocuments: expectedDocumentCount,
            run: run);

        return run;
    }

    private async Task<RunStateDocument> RunCachedSnapshotImportStageAsync(RunStateDocument run, CancellationToken ct)
    {
        if (string.IsNullOrWhiteSpace(run.SourceSnapshotCacheRunId))
            throw new InvalidOperationException("Cached snapshot import run is missing SourceSnapshotCacheRunId.");

        var selectedNode = ResolveSelectedImportNode();
        var selectedAlias = _aliasesByNodeUrl[selectedNode.Url];
        var head = await _stateStore.LoadRunHeadAsync(_config.DatabaseName, ct).ConfigureAwait(false);
        if (!string.Equals(head?.ActiveSnapshotCacheRunId, run.SourceSnapshotCacheRunId, StringComparison.Ordinal))
        {
            throw new InvalidOperationException(
                $"Cached snapshot import run '{run.RunId}' targets cache set '{run.SourceSnapshotCacheRunId}', but the active cache set is '{head?.ActiveSnapshotCacheRunId ?? "<none>"}'. Start a new import from the current active cache set.");
        }

        EnsureSnapshotImportNodeStates(run);
        var isFreshImportRun = run.SnapshotImportNodes.All(nodeState =>
            nodeState.IsImportComplete == false &&
            nodeState.ImportedRows == 0 &&
            nodeState.SnapshotsCommitted == 0 &&
            nodeState.SnapshotsSkipped == 0 &&
            nodeState.BulkInsertRestartCount == 0);

        run.CurrentPhase = ScanExecutionPhase.SnapshotImport;
        run.SnapshotImportCompleted = false;
        run.IsComplete = false;
        run.CompletedAt = null;
        await _stateStore.SaveRunAsync(run, ct).ConfigureAwait(false);

        if (isFreshImportRun)
        {
            PublishProgress("clearing local snapshots", null, "removing the previous imported snapshot set", run);
            await _stateStore.ClearLatestSnapshotsAsync(ct).ConfigureAwait(false);
        }

        PublishProgress("initializing local indexes", null, "ensuring local snapshot and mismatch indexes", run);
        await _stateStore.EnsureLocalIndexesAsync(ct).ConfigureAwait(false);

        var nodeManifest = await _snapshotCacheStore
            .LoadRequiredNodeManifestAsync(_config.DatabaseName, run.SourceSnapshotCacheRunId, selectedAlias, ct)
            .ConfigureAwait(false);
        var expectedDocumentCount = nodeManifest.DownloadedRows;
        _snapshotImportExpectedCountsByNodeUrl.Clear();
        _snapshotImportExpectedCountsByNodeUrl[selectedNode.Url] = expectedDocumentCount;

        PublishProgress(
            "importing cached snapshots",
            selectedNode.Label,
            $"reading cached segment pages for {selectedNode.Label}",
            currentNodeStreamedSnapshots: GetSnapshotImportNodeState(run, selectedNode).ImportedRows,
            currentNodeTotalDocuments: expectedDocumentCount,
            run: run);

        var result = await ImportCachedNodeSnapshotsAsync(run, selectedNode, nodeManifest, expectedDocumentCount, ct).ConfigureAwait(false);
        await PersistImportNodeCompletionAsync(run, result.NodeState, expectedDocumentCount, ct).ConfigureAwait(false);

        run.SnapshotDocumentsImported = await _stateStore.GetSnapshotDocumentCountAsync(ct).ConfigureAwait(false);
        var completedImportNodes = run.SnapshotImportNodes.Count(nodeState => nodeState.IsImportComplete);
        if (completedImportNodes == _config.Nodes.Count)
        {
            run.SnapshotImportCompleted = true;
            run.CurrentPhase = ScanExecutionPhase.Completed;
            run.IsComplete = true;
            run.CompletedAt = DateTimeOffset.UtcNow;
        }

        await _stateStore.PersistBatchAsync(
                run,
                [],
                [],
                [],
                CreateDiagnostics(CreateDiagnostic(
                    run.RunId,
                    "CachedSnapshotImportSummary",
                    $"Complete={run.IsComplete}, ImportedSnapshots={run.SnapshotDocumentsImported:N0}, ImportedNodes={completedImportNodes}/{_config.Nodes.Count}, SnapshotDocumentsSkipped={run.SnapshotDocumentsSkipped:N0}, SnapshotBulkInsertRestarts={run.SnapshotBulkInsertRestarts:N0}.",
                    selectedNode.Url)),
                ct)
            .ConfigureAwait(false);

        PublishProgress(
            run.IsComplete ? "cached snapshot import complete" : "partial cached snapshot import complete",
            selectedNode.Label,
            run.IsComplete
                ? "all configured cached nodes were imported into the local state store"
                : $"imported {completedImportNodes:N0}/{_config.Nodes.Count:N0} cached nodes into the local state store",
            currentNodeStreamedSnapshots: result.NodeState.ImportedRows,
            currentNodeTotalDocuments: expectedDocumentCount,
            run: run);

        return run;
    }

    private async Task DownloadNodeSnapshotsToCacheAsync(
        RunStateDocument run,
        NodeConfig node,
        SnapshotCacheNodeManifest nodeManifest,
        long expectedDocumentCount,
        CancellationToken ct)
    {
        while (true)
        {
            ct.ThrowIfCancellationRequested();

            var page = await FetchSnapshotCachePageAsync(node, nodeManifest.LastDownloadedDocumentId, ct).ConfigureAwait(false);
            if (page.Count == 0)
                break;

            await _snapshotCacheStore.AppendPageAsync(nodeManifest, page, ct).ConfigureAwait(false);

            var updatedState = new SnapshotCacheNodeState
            {
                NodeUrl = node.Url,
                NodeLabel = node.Label,
                NodeAlias = nodeManifest.NodeAlias,
                IsDownloadComplete = false,
                DownloadCompletedAt = null,
                LastDownloadedDocumentId = nodeManifest.LastDownloadedDocumentId,
                DownloadedRows = nodeManifest.DownloadedRows,
                CompletedSegmentCount = nodeManifest.CompletedSegmentCount,
                CompressedBytesWritten = nodeManifest.CompressedBytesWritten,
                CurrentSegmentId = nodeManifest.CurrentSegmentId,
                CurrentSegmentCommittedBytes = nodeManifest.CurrentSegmentCommittedBytes,
                CurrentSegmentCommittedRows = nodeManifest.CurrentSegmentCommittedRows,
                LastUpdatedAt = DateTimeOffset.UtcNow
            };

            await PersistSnapshotCacheDownloadCheckpointAsync(
                    run,
                    updatedState,
                    $"downloaded {nodeManifest.DownloadedRows:N0}/{expectedDocumentCount:N0} metadata rows into the local cache for {node.Label}",
                    ct)
                .ConfigureAwait(false);

            if (_config.Throttle.DelayBetweenBatchesMs > 0)
                await Task.Delay(_config.Throttle.DelayBetweenBatchesMs, ct).ConfigureAwait(false);
        }
    }

    private async Task<IReadOnlyList<SnapshotCacheRow>> FetchSnapshotCachePageAsync(
        NodeConfig node,
        string? startAfter,
        CancellationToken ct)
    {
        return await _retry.ExecuteAsync(async token =>
        {
            var store = _storesByNodeUrl[node.Url];
            var executor = store.GetRequestExecutor(_config.DatabaseName);
            using var contextScope = executor.ContextPool.AllocateOperationContext(out JsonOperationContext context);

            var command = new GetDocumentsCommand(
                store.Conventions,
                startWith: string.Empty,
                startAfter: startAfter,
                matches: null,
                exclude: null,
                start: 0,
                pageSize: SnapshotCacheStore.DownloadPageSize,
                metadataOnly: true);

            await executor.ExecuteAsync(command, context, null, token).ConfigureAwait(false);

            var documents = command.Result?.Results;
            if (documents == null || documents.Length == 0)
                return Array.Empty<SnapshotCacheRow>();

            var page = new List<SnapshotCacheRow>(documents.Length);
            for (var index = 0; index < documents.Length; index++)
            {
                if (documents[index] is not BlittableJsonReaderObject document)
                    continue;

                if (document.TryGet("@metadata", out BlittableJsonReaderObject metadata) == false)
                    continue;

                if (metadata.TryGet("@id", out string documentId) == false || string.IsNullOrWhiteSpace(documentId))
                    continue;

                metadata.TryGet("@change-vector", out string changeVector);
                metadata.TryGet("@collection", out string collection);
                metadata.TryGet("@last-modified", out string lastModifiedText);

                DateTimeOffset? lastModified = null;
                if (string.IsNullOrWhiteSpace(lastModifiedText) == false &&
                    DateTimeOffset.TryParse(lastModifiedText, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal, out var parsed))
                {
                    lastModified = parsed;
                }

                page.Add(new SnapshotCacheRow
                {
                    Id = documentId,
                    ChangeVector = changeVector,
                    Collection = collection,
                    LastModified = lastModified
                });
            }

            return (IReadOnlyList<SnapshotCacheRow>)page;
        }, ct).ConfigureAwait(false);
    }

    private async Task PersistSnapshotCacheDownloadCheckpointAsync(
        RunStateDocument run,
        SnapshotCacheNodeState nodeState,
        string detail,
        CancellationToken ct)
    {
        await _importCheckpointGate.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            UpsertSnapshotCacheNodeState(run, nodeState);
            await _stateStore.SaveRunAsync(run, ct).ConfigureAwait(false);
            await _snapshotCacheStore.SaveSetManifestAsync(
                    _snapshotCacheStore.BuildSetManifest(_config.DatabaseName, run.RunId, run.SnapshotCacheNodes),
                    ct)
                .ConfigureAwait(false);

            PublishProgress(
                "downloading snapshots to cache",
                nodeState.NodeLabel,
                detail,
                currentNodeStreamedSnapshots: nodeState.DownloadedRows,
                currentNodeTotalDocuments: TryGetSnapshotCacheExpectedCount(nodeState.NodeUrl),
                run: run);
        }
        finally
        {
            _importCheckpointGate.Release();
        }
    }

    private async Task<NodeSnapshotImportResult> ImportCachedNodeSnapshotsAsync(
        RunStateDocument run,
        NodeConfig node,
        SnapshotCacheNodeManifest nodeManifest,
        long expectedDocumentCount,
        CancellationToken ct)
    {
        var alias = _aliasesByNodeUrl[node.Url];
        var initialNodeState = GetSnapshotImportNodeState(run, node);
        var startCursor = SnapshotCacheImportCursor.FromNodeState(initialNodeState);
        var pendingCursorLock = new object();
        var pendingCursors = new Queue<PendingImportCursor>();
        var processedRows = startCursor.ImportedRows;

        SnapshotImportNodeState ApplyConfirmedCursor(SnapshotImportNodeState checkpointState)
        {
            var updated = CloneSnapshotImportNodeState(checkpointState);
            lock (pendingCursorLock)
            {
                if (string.IsNullOrWhiteSpace(updated.LastConfirmedSnapshotId))
                    return updated;

                if (pendingCursors.Any(current =>
                        string.Equals(current.SnapshotId, updated.LastConfirmedSnapshotId, StringComparison.Ordinal)) == false)
                {
                    return updated;
                }

                SnapshotCacheImportCursor? confirmedCursor = null;
                while (pendingCursors.Count > 0)
                {
                    var current = pendingCursors.Dequeue();
                    confirmedCursor = current.NextCursor;
                    if (string.Equals(current.SnapshotId, updated.LastConfirmedSnapshotId, StringComparison.Ordinal))
                        break;
                }

                if (confirmedCursor != null)
                {
                    updated.CurrentSegmentId = confirmedCursor.SegmentId;
                    updated.CurrentPageNumber = confirmedCursor.PageNumber;
                    updated.CurrentRowOffsetInPage = confirmedCursor.RowOffsetInPage;
                    updated.ImportedRows = confirmedCursor.ImportedRows;
                }
            }

            return updated;
        }

        await using var sink = new SnapshotImportSink(
            run.RunId,
            alias,
            node.Url,
            node.Label,
            initialNodeState,
            _snapshotBulkInsertSessionFactory,
            async (checkpoint, token) =>
            {
                var adjustedState = ApplyConfirmedCursor(checkpoint.NodeState);
                await PersistImportCheckpointAsync(
                        run,
                        checkpoint with { NodeState = adjustedState },
                        token)
                    .ConfigureAwait(false);
            },
            (runId, kind, message, nodeUrl) => CreateDiagnostic(runId, kind, message, nodeUrl));

        await foreach (var page in _snapshotCacheStore.ReadPagesAsync(nodeManifest, startCursor, ct).ConfigureAwait(false))
        {
            var rowOffset = page.SegmentId == startCursor.SegmentId && page.PageNumber == startCursor.PageNumber
                ? startCursor.RowOffsetInPage
                : 0;

            for (var rowIndex = rowOffset; rowIndex < page.Rows.Count; rowIndex++)
            {
                ct.ThrowIfCancellationRequested();

                var row = page.Rows[rowIndex];
                var snapshot = NodeDocumentSnapshots.Create(
                    row.Id,
                    alias,
                    node.Label,
                    node.Url,
                    row.ChangeVector,
                    row.Collection,
                    row.LastModified);
                processedRows++;

                var nextCursor = rowIndex + 1 < page.Rows.Count
                    ? new SnapshotCacheImportCursor(
                        page.SegmentId,
                        page.PageNumber,
                        rowIndex + 1,
                        processedRows)
                    : new SnapshotCacheImportCursor(
                        page.NextSegmentId ?? page.SegmentId,
                        page.NextPageNumber ?? page.PageNumber,
                        page.NextPageNumber is null ? page.Rows.Count : 0,
                        processedRows);

                lock (pendingCursorLock)
                    pendingCursors.Enqueue(new PendingImportCursor(snapshot.Id, nextCursor));

                await sink.StoreAsync(snapshot, ct).ConfigureAwait(false);

                if ((startCursor.ImportedRows + pendingCursors.Count) % 1_000 == 0)
                {
                    PublishProgress(
                        "importing cached snapshots",
                        node.Label,
                        $"imported {sink.SnapshotState.ImportedRows:N0}/{expectedDocumentCount:N0} cached rows from {node.Label}",
                        currentNodeStreamedSnapshots: processedRows,
                        currentNodeTotalDocuments: expectedDocumentCount,
                        run: run);
                }
            }

            if (_config.Throttle.DelayBetweenBatchesMs > 0)
                await Task.Delay(_config.Throttle.DelayBetweenBatchesMs, ct).ConfigureAwait(false);
        }

        await sink.CompleteAsync(ct).ConfigureAwait(false);
        var completedState = CloneSnapshotImportNodeState(sink.SnapshotState);
        completedState.ImportedRows = expectedDocumentCount;
        completedState.CurrentSegmentId = nodeManifest.CurrentSegmentId;
        completedState.CurrentPageNumber = nodeManifest.Segments.LastOrDefault()?.Pages.LastOrDefault()?.PageNumber ?? 1;
        completedState.CurrentRowOffsetInPage = nodeManifest.Segments.LastOrDefault()?.Pages.LastOrDefault()?.RowCount ?? 0;
        completedState.LastUpdatedAt = DateTimeOffset.UtcNow;

        await RefreshImportProgressAsync(
                run,
                completedState,
                $"completed cached import for {node.Label}; imported {expectedDocumentCount:N0} rows",
                expectedDocumentCount,
                expectedDocumentCount,
                CancellationToken.None)
            .ConfigureAwait(false);

        return new NodeSnapshotImportResult(run.RunId, node.Label, node.Url, completedState);
    }

    private async Task<RunStateDocument> RunImportStageAsync(RunStateDocument run, CancellationToken ct)
    {
        var resumingSnapshotImport = ScanExecutionState.CanResumeDirectSnapshotImport(run);

        run.CurrentPhase = ScanExecutionPhase.SnapshotImport;
        run.SnapshotImportCompleted = false;
        run.SnapshotDocumentsImported = resumingSnapshotImport
            ? await _stateStore.GetSnapshotDocumentCountAsync(ct).ConfigureAwait(false)
            : 0;
        await _stateStore.SaveRunAsync(run, ct).ConfigureAwait(false);

        if (resumingSnapshotImport)
        {
            ResetSnapshotImportAttemptState(run);
            PublishProgress(
                "importing snapshots",
                currentNodeLabel: null,
                detail: "resuming snapshot import over existing local snapshot documents",
                run);
        }
        else
        {
            PublishProgress("cleaning snapshots", currentNodeLabel: null, "removing previous local snapshot set", run);
            await _stateStore.ClearLatestSnapshotsAsync(ct).ConfigureAwait(false);
        }

        PublishProgress("initializing local indexes", currentNodeLabel: null, "ensuring local snapshot and mismatch indexes", run);
        await _stateStore.EnsureLocalIndexesAsync(ct).ConfigureAwait(false);

        await ImportSnapshotsAsync(run, ct).ConfigureAwait(false);

        run.SnapshotDocumentsImported = await _stateStore.GetSnapshotDocumentCountAsync(ct).ConfigureAwait(false);
        var completedImportNodes = run.SnapshotImportNodes.Count(nodeState => nodeState.IsImportComplete);
        if (completedImportNodes == _config.Nodes.Count)
        {
            run.SnapshotImportCompleted = true;
            run.CurrentPhase = ScanExecutionPhase.Completed;
            run.IsComplete = true;
            run.CompletedAt = DateTimeOffset.UtcNow;
        }
        else
        {
            run.SnapshotImportCompleted = false;
            run.CurrentPhase = ScanExecutionPhase.SnapshotImport;
            run.IsComplete = false;
            run.CompletedAt = null;
        }

        await _stateStore.PersistBatchAsync(
                run,
                [],
                [],
                [],
                CreateDiagnostics(CreateDiagnostic(
                    run.RunId,
                    "IndexedRunSummary",
                    $"Complete={run.IsComplete}, Phase=ImportSnapshots, ImportedNodes={completedImportNodes}/{_config.Nodes.Count}, SnapshotDocumentsImported={run.SnapshotDocumentsImported}, SnapshotDocumentsSkipped={run.SnapshotDocumentsSkipped}, SnapshotBulkInsertRestarts={run.SnapshotBulkInsertRestarts}, CandidateDocumentsFound={run.CandidateDocumentsFound}, CandidateDocumentsProcessed={run.CandidateDocumentsProcessed}, CandidateDocumentsExcludedBySkippedSnapshots={run.CandidateDocumentsExcludedBySkippedSnapshots}, DocumentsInspected={run.DocumentsInspected}, UniqueVersionsCompared={run.UniqueVersionsCompared}, MismatchesFound={run.MismatchesFound}, RepairsPlanned={run.RepairsPlanned}, RepairsAttempted={run.RepairsAttempted}, RepairsPatchedOnWinner={run.RepairsPatchedOnWinner}, RepairsFailed={run.RepairsFailed}.")),
                ct)
            .ConfigureAwait(false);

        PublishProgress(
            run.IsComplete ? "complete" : "partial import complete",
            currentNodeLabel: null,
            detail: run.IsComplete
                ? "snapshot import completed for all configured nodes"
                : $"loaded {completedImportNodes:N0}/{_config.Nodes.Count:N0} nodes; snapshot set remains pending until the remaining nodes are imported",
            run);

        return run;
    }

    private async Task<RunStateDocument> RunAnalysisStageAsync(RunStateDocument run, CancellationToken ct)
    {
        if (string.IsNullOrWhiteSpace(run.SourceSnapshotRunId))
            throw new InvalidOperationException("Analysis run is missing SourceSnapshotRunId.");

        var head = await _stateStore.LoadRunHeadAsync(_config.DatabaseName, ct).ConfigureAwait(false);
        if (!string.Equals(head?.ActiveSnapshotRunId, run.SourceSnapshotRunId, StringComparison.Ordinal))
        {
            throw new InvalidOperationException(
                $"Analysis run '{run.RunId}' targets snapshot set '{run.SourceSnapshotRunId}', but the active snapshot set is '{head?.ActiveSnapshotRunId ?? "<none>"}'. Start a new analysis run from the current active snapshot set.");
        }

        run.SnapshotDocumentsImported = await _stateStore.GetSnapshotDocumentCountAsync(ct).ConfigureAwait(false);
        await _stateStore.SaveRunAsync(run, ct).ConfigureAwait(false);

        PublishProgress("initializing local indexes", currentNodeLabel: null, "ensuring local snapshot and mismatch indexes", run);
        await _stateStore.EnsureLocalIndexesAsync(ct).ConfigureAwait(false);

        if (run.CurrentPhase != ScanExecutionPhase.CandidateProcessing)
        {
            run.CurrentPhase = ScanExecutionPhase.WaitingForSnapshotIndex;
            await _stateStore.SaveRunAsync(run, ct).ConfigureAwait(false);
        }

        var snapshotProblemIndexStatus = await WaitForSnapshotProblemIndexReadyAsync(run, ct).ConfigureAwait(false);

        run.CurrentPhase = ScanExecutionPhase.CandidateProcessing;
        if (run.CandidateDocumentsFound > run.CandidateDocumentsProcessed)
            run.CandidateDocumentsFound = run.CandidateDocumentsProcessed;

        await _stateStore.SaveRunAsync(run, ct).ConfigureAwait(false);

        var snapshotEvaluationNodes = await ResolveSnapshotEvaluationNodesAsync(run, ct).ConfigureAwait(false);
        var stoppedEarly = await ProcessCandidatesAsync(run, snapshotEvaluationNodes, snapshotProblemIndexStatus, ct).ConfigureAwait(false);

        if (!stoppedEarly)
        {
            run.CurrentPhase = ScanExecutionPhase.Completed;
            run.IsComplete = true;
            run.CompletedAt = DateTimeOffset.UtcNow;
        }

        await _stateStore.PersistBatchAsync(
                run,
                [],
                [],
                [],
                CreateDiagnostics(CreateDiagnostic(
                    run.RunId,
                    "IndexedRunSummary",
                    $"Complete={run.IsComplete}, Phase=Analysis, SnapshotDocumentsImported={run.SnapshotDocumentsImported}, SnapshotDocumentsSkipped={run.SnapshotDocumentsSkipped}, SnapshotBulkInsertRestarts={run.SnapshotBulkInsertRestarts}, CandidateDocumentsFound={run.CandidateDocumentsFound}, CandidateDocumentsProcessed={run.CandidateDocumentsProcessed}, CandidateDocumentsExcludedBySkippedSnapshots={run.CandidateDocumentsExcludedBySkippedSnapshots}, DocumentsInspected={run.DocumentsInspected}, UniqueVersionsCompared={run.UniqueVersionsCompared}, MismatchesFound={run.MismatchesFound}, ManualReviewDocumentsFound={run.ManualReviewDocumentsFound}, RepairsPlanned={run.RepairsPlanned}, RepairsAttempted={run.RepairsAttempted}, RepairsPatchedOnWinner={run.RepairsPatchedOnWinner}, RepairsFailed={run.RepairsFailed}.")),
                ct)
            .ConfigureAwait(false);

        PublishProgress(
            stoppedEarly ? "stopped early" : "complete",
            currentNodeLabel: null,
            stoppedEarly
                ? RunModePolicies.UsesLocalSnapshotEvaluation(run.RunMode)
                    ? "first locally evaluated mismatch reached"
                    : "first revalidated mismatch reached"
                : "analysis completed",
            run);

        return run;
    }

    public async ValueTask DisposeAsync()
    {
        foreach (var store in _storesByNodeUrl.Values)
            store.Dispose();

        _importCheckpointGate.Dispose();
        _certificate?.Dispose();
        await Task.CompletedTask.ConfigureAwait(false);
    }

    private async Task ImportSnapshotsAsync(RunStateDocument run, CancellationToken ct)
    {
        EnsureSnapshotImportNodeStates(run);
        _snapshotImportExpectedCountsByNodeUrl.Clear();

        var expectedCountsByNodeUrl = new Dictionary<string, long>(StringComparer.OrdinalIgnoreCase);
        foreach (var node in _config.Nodes)
        {
            PublishProgress(
                "importing snapshots",
                currentNodeLabel: node.Label,
                detail: $"loading document total for {node.Label}",
                run);

            var expectedDocumentCount = await LoadSnapshotImportExpectedCountAsync(node, ct).ConfigureAwait(false);
            expectedCountsByNodeUrl[node.Url] = expectedDocumentCount;
            _snapshotImportExpectedCountsByNodeUrl[node.Url] = expectedDocumentCount;

            PublishProgress(
                "importing snapshots",
                node.Label,
                $"discovered {expectedDocumentCount:N0} documents on {node.Label}",
                currentNodeStreamedSnapshots: 0,
                currentNodeTotalDocuments: expectedDocumentCount,
                run: run);
        }

        PublishProgress(
            "importing snapshots",
            currentNodeLabel: null,
            detail: $"starting remote query streams for {_config.Nodes.Count:N0} nodes",
            run);

        var remainingTasks = _config.Nodes
            .Select(node => StreamNodeSnapshotsAsync(run, node, expectedCountsByNodeUrl[node.Url], ct))
            .ToList();

        while (remainingTasks.Count > 0)
        {
            var completedTask = await Task.WhenAny(remainingTasks).ConfigureAwait(false);
            remainingTasks.Remove(completedTask);

            var result = await completedTask.ConfigureAwait(false);
            await PersistImportNodeCompletionAsync(run, result.NodeState, expectedCountsByNodeUrl[result.NodeUrl], ct).ConfigureAwait(false);
            await _stateStore.StoreDiagnosticsAsync(
                    [CreateNodeSnapshotImportSummaryDiagnostic(result)],
                    ct)
                .ConfigureAwait(false);
        }

        PublishProgress(
            "importing snapshots",
            currentNodeLabel: null,
            detail: $"confirmed {run.SnapshotDocumentsImported:N0} snapshot rows; skipped {run.SnapshotDocumentsSkipped:N0}; restarted local bulk insert {run.SnapshotBulkInsertRestarts:N0} times",
            run);
    }

    private async Task<NodeSnapshotImportResult> StreamNodeSnapshotsAsync(
        RunStateDocument run,
        NodeConfig node,
        long expectedDocumentCount,
        CancellationToken ct)
    {
        PublishProgress(
            "importing snapshots",
            node.Label,
            $"opening query stream for {node.Label}",
            currentNodeStreamedSnapshots: 0,
            currentNodeTotalDocuments: expectedDocumentCount,
            run: run);

        try
        {
            var store = _storesByNodeUrl[node.Url];
            var alias = _aliasesByNodeUrl[node.Url];
            var streamedCount = 0L;
            var initialNodeState = GetSnapshotImportNodeState(run, node);

            await using var sink = new SnapshotImportSink(
                run.RunId,
                alias,
                node.Url,
                node.Label,
                initialNodeState,
                _snapshotBulkInsertSessionFactory,
                (checkpoint, token) => PersistImportCheckpointAsync(run, checkpoint, token),
                (runId, kind, message, nodeUrl) => CreateDiagnostic(runId, kind, message, nodeUrl));

            using var session = store.OpenAsyncSession();
            var query = session.Advanced.AsyncRawQuery<SnapshotQueryProjection>(NodeDocumentSnapshots.QueryProjection);

            await using var stream = await session.Advanced.StreamAsync(query, ct).ConfigureAwait(false);
            while (await stream.MoveNextAsync().ConfigureAwait(false))
            {
                ct.ThrowIfCancellationRequested();

                var projection = stream.Current?.Document;
                if (projection == null || string.IsNullOrWhiteSpace(projection.OriginalDocumentId))
                    continue;

                var snapshot = NodeDocumentSnapshots.Create(
                    projection.OriginalDocumentId,
                    alias,
                    node.Label,
                    node.Url,
                    projection.ChangeVector,
                    projection.Collection,
                    projection.LastModified);

                if (NodeDocumentSnapshots.ContainsBulkInsertUnsafeCharacters(projection.OriginalDocumentId))
                {
                    await _stateStore.StoreUnsafeSnapshotIdsAsync(
                            [CreateUnsafeSnapshotIdDocument(run.RunId, snapshot)],
                            ct)
                        .ConfigureAwait(false);
                }

                await sink.StoreAsync(snapshot, ct).ConfigureAwait(false);
                streamedCount++;

                if (streamedCount % 1_000 == 0)
                {
                    await RefreshImportProgressAsync(
                            run,
                            sink.SnapshotState,
                            $"streamed {streamedCount:N0} snapshot rows from {node.Label}",
                            streamedCount,
                            expectedDocumentCount,
                            CancellationToken.None)
                        .ConfigureAwait(false);
                }
            }

            await sink.CompleteAsync(ct).ConfigureAwait(false);
            await RefreshImportProgressAsync(
                    run,
                    sink.SnapshotState,
                    $"completed stream for {node.Label}; committed {sink.SnapshotState.SnapshotsCommitted:N0}, skipped {sink.SnapshotState.SnapshotsSkipped:N0}",
                    streamedCount,
                    expectedDocumentCount,
                    CancellationToken.None)
                .ConfigureAwait(false);

            return new NodeSnapshotImportResult(run.RunId, node.Label, node.Url, sink.SnapshotState);
        }
        catch (Exception ex)
        {
            try
            {
                await _stateStore.StoreDiagnosticsAsync(
                        CreateDiagnostics(CreateDiagnostic(
                            run.RunId,
                            "NodeSnapshotStreamFailed",
                            $"Query stream for node '{node.Label}' failed: {ex.Message}",
                            node.Url)),
                        CancellationToken.None)
                    .ConfigureAwait(false);
            }
            catch
            {
                // Best-effort diagnostics only.
            }

            throw;
        }
    }

    private async Task PersistImportCheckpointAsync(
        RunStateDocument run,
        SnapshotImportSinkCheckpoint checkpoint,
        CancellationToken ct)
    {
        await _importCheckpointGate.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            UpsertSnapshotImportNodeState(run, checkpoint.NodeState);
            RecomputeSnapshotImportAggregates(run);
            run.SnapshotDocumentsImported = await _stateStore.GetSnapshotDocumentCountAsync(ct).ConfigureAwait(false);

            await _stateStore.PersistBatchAsync(
                    run,
                    [],
                    [],
                    [],
                    checkpoint.SkippedSnapshots,
                    checkpoint.Diagnostics,
                    ct)
                .ConfigureAwait(false);

            PublishProgress(
                "importing snapshots",
                checkpoint.NodeState.NodeLabel,
                checkpoint.SkippedSnapshots.Count > 0
                    ? $"skipped {checkpoint.SkippedSnapshots.Count:N0} poison snapshot row(s) on {checkpoint.NodeState.NodeLabel}"
                    : checkpoint.Diagnostics.Count > 0
                        ? $"restarted local bulk insert for {checkpoint.NodeState.NodeLabel}"
                        : $"confirmed {run.SnapshotDocumentsImported:N0} snapshot rows",
                currentNodeStreamedSnapshots: null,
                currentNodeTotalDocuments: TryGetSnapshotImportExpectedCount(checkpoint.NodeState.NodeUrl),
                run: run);
        }
        finally
        {
            _importCheckpointGate.Release();
        }
    }

    private async Task RefreshImportProgressAsync(
        RunStateDocument run,
        SnapshotImportNodeState nodeState,
        string detail,
        long? currentNodeStreamedSnapshots,
        long? currentNodeTotalDocuments,
        CancellationToken ct)
    {
        await _importCheckpointGate.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            UpsertSnapshotImportNodeState(run, nodeState);
            RecomputeSnapshotImportAggregates(run);
            PublishProgress(
                "importing snapshots",
                nodeState.NodeLabel,
                detail,
                currentNodeStreamedSnapshots,
                currentNodeTotalDocuments,
                run);
        }
        finally
        {
            _importCheckpointGate.Release();
        }
    }

    private async Task PersistImportNodeCompletionAsync(
        RunStateDocument run,
        SnapshotImportNodeState nodeState,
        long expectedDocumentCount,
        CancellationToken ct)
    {
        await _importCheckpointGate.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            nodeState.IsImportComplete = true;
            nodeState.ImportCompletedAt = DateTimeOffset.UtcNow;
            nodeState.ImportedRows = Math.Max(nodeState.ImportedRows, expectedDocumentCount);
            nodeState.LastUpdatedAt = DateTimeOffset.UtcNow;
            UpsertSnapshotImportNodeState(run, nodeState);
            RecomputeSnapshotImportAggregates(run);
            run.SnapshotDocumentsImported = await _stateStore.GetSnapshotDocumentCountAsync(ct).ConfigureAwait(false);
            await _stateStore.SaveRunAsync(run, ct).ConfigureAwait(false);

            PublishProgress(
                "importing snapshots",
                nodeState.NodeLabel,
                $"completed import for {nodeState.NodeLabel}",
                currentNodeStreamedSnapshots: expectedDocumentCount,
                currentNodeTotalDocuments: expectedDocumentCount,
                run: run);
        }
        finally
        {
            _importCheckpointGate.Release();
        }
    }

    private void EnsureSnapshotCacheNodeStates(RunStateDocument run)
    {
        foreach (var node in _config.Nodes.Select((node, index) => new
                 {
                     Node = node,
                     Alias = NodeDocumentSnapshots.GetNodeAlias(index)
                 }))
        {
            if (run.SnapshotCacheNodes.Any(existing =>
                    string.Equals(existing.NodeUrl, node.Node.Url, StringComparison.OrdinalIgnoreCase)))
            {
                continue;
            }

            run.SnapshotCacheNodes.Add(new SnapshotCacheNodeState
            {
                NodeUrl = node.Node.Url,
                NodeLabel = node.Node.Label,
                NodeAlias = node.Alias,
                CurrentSegmentId = 1,
                LastUpdatedAt = DateTimeOffset.UtcNow
            });
        }
    }

    private void EnsureSnapshotImportNodeStates(RunStateDocument run)
    {
        foreach (var node in _config.Nodes)
        {
            if (run.SnapshotImportNodes.Any(existing =>
                    string.Equals(existing.NodeUrl, node.Url, StringComparison.OrdinalIgnoreCase)))
            {
                continue;
            }

            run.SnapshotImportNodes.Add(new SnapshotImportNodeState
            {
                NodeUrl = node.Url,
                NodeLabel = node.Label,
                CurrentSegmentId = 1,
                CurrentPageNumber = 1,
                LastUpdatedAt = DateTimeOffset.UtcNow
            });
        }

        RecomputeSnapshotImportAggregates(run);
    }

    private static void ResetSnapshotImportAttemptState(RunStateDocument run)
    {
        foreach (var nodeState in run.SnapshotImportNodes)
        {
            nodeState.SnapshotsCommitted = 0;
            nodeState.LastConfirmedSnapshotId = null;
            nodeState.LastError = null;
            nodeState.LastUpdatedAt = DateTimeOffset.UtcNow;
        }
    }

    private SnapshotImportNodeState GetSnapshotImportNodeState(RunStateDocument run, NodeConfig node)
    {
        EnsureSnapshotImportNodeStates(run);

        var existing = run.SnapshotImportNodes.FirstOrDefault(state =>
            string.Equals(state.NodeUrl, node.Url, StringComparison.OrdinalIgnoreCase));
        if (existing != null)
        {
            return new SnapshotImportNodeState
            {
                NodeUrl = existing.NodeUrl,
                NodeLabel = existing.NodeLabel,
                IsImportComplete = existing.IsImportComplete,
                ImportCompletedAt = existing.ImportCompletedAt,
                CurrentSegmentId = existing.CurrentSegmentId,
                CurrentPageNumber = existing.CurrentPageNumber,
                CurrentRowOffsetInPage = existing.CurrentRowOffsetInPage,
                ImportedRows = existing.ImportedRows,
                SnapshotsCommitted = existing.SnapshotsCommitted,
                SnapshotsSkipped = existing.SnapshotsSkipped,
                BulkInsertRestartCount = existing.BulkInsertRestartCount,
                LastConfirmedSnapshotId = existing.LastConfirmedSnapshotId,
                LastSkippedSnapshotId = existing.LastSkippedSnapshotId,
                LastError = existing.LastError,
                LastUpdatedAt = existing.LastUpdatedAt
            };
        }

        return new SnapshotImportNodeState
        {
            NodeUrl = node.Url,
            NodeLabel = node.Label,
            IsImportComplete = false,
            CurrentSegmentId = 1,
            CurrentPageNumber = 1,
            LastUpdatedAt = DateTimeOffset.UtcNow
        };
    }

    private void UpsertSnapshotImportNodeState(RunStateDocument run, SnapshotImportNodeState updatedState)
    {
        var existing = run.SnapshotImportNodes.FirstOrDefault(state =>
            string.Equals(state.NodeUrl, updatedState.NodeUrl, StringComparison.OrdinalIgnoreCase));
        if (existing == null)
        {
            run.SnapshotImportNodes.Add(new SnapshotImportNodeState
            {
                NodeUrl = updatedState.NodeUrl,
                NodeLabel = updatedState.NodeLabel,
                IsImportComplete = updatedState.IsImportComplete,
                ImportCompletedAt = updatedState.ImportCompletedAt,
                CurrentSegmentId = updatedState.CurrentSegmentId,
                CurrentPageNumber = updatedState.CurrentPageNumber,
                CurrentRowOffsetInPage = updatedState.CurrentRowOffsetInPage,
                ImportedRows = updatedState.ImportedRows,
                SnapshotsCommitted = updatedState.SnapshotsCommitted,
                SnapshotsSkipped = updatedState.SnapshotsSkipped,
                BulkInsertRestartCount = updatedState.BulkInsertRestartCount,
                LastConfirmedSnapshotId = updatedState.LastConfirmedSnapshotId,
                LastSkippedSnapshotId = updatedState.LastSkippedSnapshotId,
                LastError = updatedState.LastError,
                LastUpdatedAt = updatedState.LastUpdatedAt
            });
            return;
        }

        existing.NodeLabel = updatedState.NodeLabel;
        existing.IsImportComplete = updatedState.IsImportComplete;
        existing.ImportCompletedAt = updatedState.ImportCompletedAt;
        existing.CurrentSegmentId = updatedState.CurrentSegmentId;
        existing.CurrentPageNumber = updatedState.CurrentPageNumber;
        existing.CurrentRowOffsetInPage = updatedState.CurrentRowOffsetInPage;
        existing.ImportedRows = updatedState.ImportedRows;
        existing.SnapshotsCommitted = updatedState.SnapshotsCommitted;
        existing.SnapshotsSkipped = updatedState.SnapshotsSkipped;
        existing.BulkInsertRestartCount = updatedState.BulkInsertRestartCount;
        existing.LastConfirmedSnapshotId = updatedState.LastConfirmedSnapshotId;
        existing.LastSkippedSnapshotId = updatedState.LastSkippedSnapshotId;
        existing.LastError = updatedState.LastError;
        existing.LastUpdatedAt = updatedState.LastUpdatedAt;
    }

    private SnapshotCacheNodeState GetSnapshotCacheNodeState(RunStateDocument run, NodeConfig node)
    {
        EnsureSnapshotCacheNodeStates(run);

        var existing = run.SnapshotCacheNodes.FirstOrDefault(state =>
            string.Equals(state.NodeUrl, node.Url, StringComparison.OrdinalIgnoreCase));
        if (existing != null)
        {
            return new SnapshotCacheNodeState
            {
                NodeUrl = existing.NodeUrl,
                NodeLabel = existing.NodeLabel,
                NodeAlias = existing.NodeAlias,
                IsDownloadComplete = existing.IsDownloadComplete,
                DownloadCompletedAt = existing.DownloadCompletedAt,
                LastDownloadedDocumentId = existing.LastDownloadedDocumentId,
                DownloadedRows = existing.DownloadedRows,
                CompletedSegmentCount = existing.CompletedSegmentCount,
                CompressedBytesWritten = existing.CompressedBytesWritten,
                CurrentSegmentId = existing.CurrentSegmentId,
                CurrentSegmentCommittedBytes = existing.CurrentSegmentCommittedBytes,
                CurrentSegmentCommittedRows = existing.CurrentSegmentCommittedRows,
                LastUpdatedAt = existing.LastUpdatedAt
            };
        }

        return new SnapshotCacheNodeState
        {
            NodeUrl = node.Url,
            NodeLabel = node.Label,
            NodeAlias = _aliasesByNodeUrl[node.Url],
            CurrentSegmentId = 1,
            LastUpdatedAt = DateTimeOffset.UtcNow
        };
    }

    private void UpsertSnapshotCacheNodeState(RunStateDocument run, SnapshotCacheNodeState updatedState)
    {
        var existing = run.SnapshotCacheNodes.FirstOrDefault(state =>
            string.Equals(state.NodeUrl, updatedState.NodeUrl, StringComparison.OrdinalIgnoreCase));
        if (existing == null)
        {
            run.SnapshotCacheNodes.Add(new SnapshotCacheNodeState
            {
                NodeUrl = updatedState.NodeUrl,
                NodeLabel = updatedState.NodeLabel,
                NodeAlias = updatedState.NodeAlias,
                IsDownloadComplete = updatedState.IsDownloadComplete,
                DownloadCompletedAt = updatedState.DownloadCompletedAt,
                LastDownloadedDocumentId = updatedState.LastDownloadedDocumentId,
                DownloadedRows = updatedState.DownloadedRows,
                CompletedSegmentCount = updatedState.CompletedSegmentCount,
                CompressedBytesWritten = updatedState.CompressedBytesWritten,
                CurrentSegmentId = updatedState.CurrentSegmentId,
                CurrentSegmentCommittedBytes = updatedState.CurrentSegmentCommittedBytes,
                CurrentSegmentCommittedRows = updatedState.CurrentSegmentCommittedRows,
                LastUpdatedAt = updatedState.LastUpdatedAt
            });
            return;
        }

        existing.NodeLabel = updatedState.NodeLabel;
        existing.NodeAlias = updatedState.NodeAlias;
        existing.IsDownloadComplete = updatedState.IsDownloadComplete;
        existing.DownloadCompletedAt = updatedState.DownloadCompletedAt;
        existing.LastDownloadedDocumentId = updatedState.LastDownloadedDocumentId;
        existing.DownloadedRows = updatedState.DownloadedRows;
        existing.CompletedSegmentCount = updatedState.CompletedSegmentCount;
        existing.CompressedBytesWritten = updatedState.CompressedBytesWritten;
        existing.CurrentSegmentId = updatedState.CurrentSegmentId;
        existing.CurrentSegmentCommittedBytes = updatedState.CurrentSegmentCommittedBytes;
        existing.CurrentSegmentCommittedRows = updatedState.CurrentSegmentCommittedRows;
        existing.LastUpdatedAt = updatedState.LastUpdatedAt;
    }

    private static void RecomputeSnapshotImportAggregates(RunStateDocument run)
    {
        run.SnapshotDocumentsSkipped = run.SnapshotImportNodes.Sum(state => state.SnapshotsSkipped);
        run.SnapshotBulkInsertRestarts = run.SnapshotImportNodes.Sum(state => state.BulkInsertRestartCount);
    }

    private async Task<long> LoadSnapshotImportExpectedCountAsync(NodeConfig node, CancellationToken ct)
    {
        var statistics = await _retry.ExecuteAsync(async token =>
                await _storesByNodeUrl[node.Url]
                    .Maintenance
                    .SendAsync(new GetStatisticsOperation(), token)
                    .ConfigureAwait(false),
            ct)
            .ConfigureAwait(false);

        return statistics.CountOfDocuments;
    }

    private Task<long> LoadSnapshotCacheExpectedCountAsync(NodeConfig node, CancellationToken ct)
        => LoadSnapshotImportExpectedCountAsync(node, ct);

    private long? TryGetSnapshotImportExpectedCount(string nodeUrl)
        => _snapshotImportExpectedCountsByNodeUrl.TryGetValue(nodeUrl, out var expectedCount)
            ? expectedCount
            : null;

    private long? TryGetSnapshotCacheExpectedCount(string nodeUrl)
        => _snapshotCacheExpectedCountsByNodeUrl.TryGetValue(nodeUrl, out var expectedCount)
            ? expectedCount
            : null;

    private NodeConfig ResolveSelectedImportNode()
    {
        if (string.IsNullOrWhiteSpace(_config.SelectedImportNodeUrl))
            throw new InvalidOperationException("Import snapshots mode requires a selected import node for the current launch.");

        return _config.Nodes.FirstOrDefault(node =>
                   string.Equals(node.Url, _config.SelectedImportNodeUrl, StringComparison.OrdinalIgnoreCase))
               ?? throw new InvalidOperationException(
                   $"Selected import node '{_config.SelectedImportNodeUrl}' is not present in the configured cluster nodes.");
    }

    private DiagnosticDocument CreateNodeSnapshotImportSummaryDiagnostic(NodeSnapshotImportResult result)
    {
        return CreateDiagnostic(
            runId: result.RunId,
            kind: "NodeSnapshotImportSummary",
            message:
            $"Node '{result.NodeLabel}' summary: committed {result.NodeState.SnapshotsCommitted:N0}, skipped {result.NodeState.SnapshotsSkipped:N0}, local bulk-insert restarts {result.NodeState.BulkInsertRestartCount:N0}.",
            nodeUrl: result.NodeUrl);
    }

    private static UnsafeSnapshotIdDocument CreateUnsafeSnapshotIdDocument(string runId, NodeDocumentSnapshot snapshot)
    {
        return new UnsafeSnapshotIdDocument
        {
            Id = StateStore.CreateUnsafeSnapshotIdDocumentId(runId, snapshot.FromNode, snapshot.OriginalDocumentId),
            RunId = runId,
            OriginalDocumentId = snapshot.OriginalDocumentId,
            NormalizedSnapshotDocumentId = snapshot.Id,
            FromNode = snapshot.FromNode,
            NodeUrl = snapshot.NodeUrl,
            NormalizationReason = NodeDocumentSnapshots.DescribeBulkInsertUnsafeCharacters(snapshot.OriginalDocumentId),
            DetectedAt = DateTimeOffset.UtcNow
        };
    }

    private async Task<bool> ProcessCandidatesAsync(
        RunStateDocument run,
        IReadOnlyList<NodeConfig> snapshotEvaluationNodes,
        SnapshotProblemIndexStatus readyIndexStatus,
        CancellationToken ct)
    {
        var pageStart = checked((int)run.CandidateDocumentsProcessed);
        var pageNumber = 0;
        var lastKnownIndexStatus = readyIndexStatus;
        var skippedOriginalDocumentIds = string.IsNullOrWhiteSpace(run.SourceSnapshotRunId)
            ? new HashSet<string>(StringComparer.OrdinalIgnoreCase)
            : await _stateStore.LoadSkippedOriginalDocumentIdsAsync(run.SourceSnapshotRunId, ct).ConfigureAwait(false);

        while (true)
        {
            ct.ThrowIfCancellationRequested();

            var nextPageNumber = pageNumber + 1;
            var pageLoad = await LoadCandidatePageWithProgressAsync(
                    run,
                    nextPageNumber,
                    pageStart,
                    _config.Throttle.PageSize,
                    snapshotEvaluationNodes.Count,
                    lastKnownIndexStatus,
                    ct)
                .ConfigureAwait(false);
            var page = pageLoad.Page;
            lastKnownIndexStatus = pageLoad.IndexStatus;

            if (page.Count == 0)
            {
                PublishProgress(
                    "loading candidates",
                    currentNodeLabel: null,
                    AnalysisProgressFormatter.BuildCandidatePageLoadedDetail(nextPageNumber, 0, pageLoad.Duration),
                    run);
                return false;
            }

            pageNumber = nextPageNumber;
            run.CandidateDocumentsFound += page.Count;

            var candidateIds = page
                .Select(candidate => candidate.OriginalDocumentId)
                .Where(documentId => skippedOriginalDocumentIds.Contains(documentId) == false)
                .ToArray();
            var excludedBySkippedSnapshots = page.Count - candidateIds.Length;
            if (excludedBySkippedSnapshots > 0)
            {
                run.CandidateDocumentsExcludedBySkippedSnapshots += excludedBySkippedSnapshots;
                run.CandidateDocumentsProcessed += excludedBySkippedSnapshots;
                await _stateStore.SaveRunAsync(run, ct).ConfigureAwait(false);
            }

            PublishProgress(
                "processing candidates",
                currentNodeLabel: null,
                excludedBySkippedSnapshots == 0
                    ? $"page {pageNumber}: {page.Count:N0} candidate ids"
                    : $"page {pageNumber}: {page.Count:N0} candidate ids, excluded {excludedBySkippedSnapshots:N0} skipped-import originals",
                run);

            foreach (var chunk in candidateIds.Chunk(_config.Throttle.ClusterLookupBatchSize))
            {
                var lookupOutcome = await ProcessLookupChunkAsync(run, chunk, snapshotEvaluationNodes, ct).ConfigureAwait(false);

                run.CandidateDocumentsProcessed += lookupOutcome.DocumentsRevalidated;
                run.DocumentsInspected += lookupOutcome.DocumentsRevalidated;
                run.UniqueVersionsCompared += lookupOutcome.UniqueVersionsCompared;
                run.MismatchesFound += lookupOutcome.MismatchesFound;
                run.ManualReviewDocumentsFound += lookupOutcome.ManualReviewDocumentsFound;
                run.RepairsPlanned += lookupOutcome.RepairsPlanned;
                run.RepairsAttempted += lookupOutcome.RepairsAttempted;
                run.RepairsPatchedOnWinner += lookupOutcome.RepairsPatchedOnWinner;
                run.RepairsFailed += lookupOutcome.RepairsFailed;

                await _stateStore.PersistBatchAsync(
                        run,
                        lookupOutcome.Mismatches,
                        lookupOutcome.Repairs,
                        lookupOutcome.RepairGuards,
                        lookupOutcome.Diagnostics,
                        ct)
                    .ConfigureAwait(false);

                PublishProgress(
                    "processing candidates",
                    currentNodeLabel: null,
                    BuildCandidateProgressDetail(run),
                    run);

                if (run.RunMode == RunMode.ScanOnly &&
                    _config.Mode == CheckMode.FirstMismatch &&
                    lookupOutcome.MismatchesFound > 0)
                {
                    return true;
                }

                if (_config.Throttle.DelayBetweenBatchesMs > 0)
                    await Task.Delay(_config.Throttle.DelayBetweenBatchesMs, ct).ConfigureAwait(false);
            }

            pageStart += page.Count;
        }
    }

    private async Task<SnapshotProblemIndexStatus> WaitForSnapshotProblemIndexReadyAsync(
        RunStateDocument run,
        CancellationToken ct)
    {
        PublishProgress("checking local candidate index", currentNodeLabel: null, "reading local candidate index status", run);

        var initialStatus = await _stateStore.GetSnapshotProblemIndexStatusAsync(ct).ConfigureAwait(false);
        PublishProgress(
            "checking local candidate index",
            currentNodeLabel: null,
            AnalysisProgressFormatter.BuildCheckingIndexDetail(initialStatus),
            run);
        await _stateStore.StoreDiagnosticsAsync(
                CreateDiagnostics(CreateDiagnostic(
                    run.RunId,
                    "SnapshotIndexStatusChecked",
                    AnalysisProgressFormatter.BuildIndexDiagnosticMessage(initialStatus))),
                ct)
            .ConfigureAwait(false);

        if (initialStatus.IsStale == false)
        {
            PublishProgress(
                "loading candidates",
                currentNodeLabel: null,
                AnalysisProgressFormatter.BuildIndexReadyDetail(initialStatus, TimeSpan.Zero),
                run);

            await _stateStore.StoreDiagnosticsAsync(
                    CreateDiagnostics(CreateDiagnostic(
                        run.RunId,
                        "SnapshotIndexReady",
                        AnalysisProgressFormatter.BuildIndexDiagnosticMessage(initialStatus, TimeSpan.Zero))),
                    ct)
                .ConfigureAwait(false);

            return initialStatus;
        }

        var waitStopwatch = Stopwatch.StartNew();
        PublishProgress(
            "waiting for index",
            currentNodeLabel: null,
            AnalysisProgressFormatter.BuildWaitingForIndexDetail(initialStatus, waitStopwatch.Elapsed),
            run);

        var status = initialStatus;
        while (status.IsStale)
        {
            await Task.Delay(AnalysisHeartbeatInterval, ct).ConfigureAwait(false);
            status = await _stateStore.GetSnapshotProblemIndexStatusAsync(ct).ConfigureAwait(false);

            if (status.IsStale)
            {
                PublishProgress(
                    "waiting for index",
                    currentNodeLabel: null,
                    AnalysisProgressFormatter.BuildWaitingForIndexDetail(status, waitStopwatch.Elapsed),
                    run);
            }
        }

        PublishProgress(
            "loading candidates",
            currentNodeLabel: null,
            AnalysisProgressFormatter.BuildIndexReadyDetail(status, waitStopwatch.Elapsed),
            run);

        await _stateStore.StoreDiagnosticsAsync(
                CreateDiagnostics(CreateDiagnostic(
                    run.RunId,
                    "SnapshotIndexReady",
                    AnalysisProgressFormatter.BuildIndexDiagnosticMessage(status, waitStopwatch.Elapsed))),
                ct)
            .ConfigureAwait(false);

        return status;
    }

    private async Task<CandidatePageLoadResult> LoadCandidatePageWithProgressAsync(
        RunStateDocument run,
        int pageNumber,
        int pageStart,
        int pageSize,
        int configuredNodeCount,
        SnapshotProblemIndexStatus lastKnownIndexStatus,
        CancellationToken ct)
    {
        var loadStopwatch = Stopwatch.StartNew();
        PublishProgress(
            "loading candidates",
            currentNodeLabel: null,
            AnalysisProgressFormatter.BuildLoadingCandidatesDetail(
                pageNumber,
                pageStart,
                pageSize,
                configuredNodeCount,
                loadStopwatch.Elapsed,
                lastKnownIndexStatus),
            run);

        var loadTask = _stateStore.LoadProblemDocumentCandidatesAsync(
            configuredNodeCount,
            pageStart,
            pageSize,
            ct);

        var currentIndexStatus = lastKnownIndexStatus;
        while (true)
        {
            var completedTask = await Task.WhenAny(loadTask, Task.Delay(AnalysisHeartbeatInterval, ct)).ConfigureAwait(false);
            if (completedTask == loadTask)
                break;

            currentIndexStatus = await _stateStore.GetSnapshotProblemIndexStatusAsync(ct).ConfigureAwait(false);
            PublishProgress(
                "loading candidates",
                currentNodeLabel: null,
                AnalysisProgressFormatter.BuildLoadingCandidatesDetail(
                    pageNumber,
                    pageStart,
                    pageSize,
                    configuredNodeCount,
                    loadStopwatch.Elapsed,
                    currentIndexStatus),
                run);
        }

        var page = await loadTask.ConfigureAwait(false);
        currentIndexStatus = await _stateStore.GetSnapshotProblemIndexStatusAsync(ct).ConfigureAwait(false);
        var duration = loadStopwatch.Elapsed;
        var shouldPersistDiagnostic = pageNumber == 1 || duration >= SlowCandidatePageDiagnosticThreshold;
        if (shouldPersistDiagnostic)
        {
            await _stateStore.StoreDiagnosticsAsync(
                    CreateDiagnostics(CreateDiagnostic(
                        run.RunId,
                        pageNumber == 1 ? "CandidatePageLoad" : "SlowCandidatePageLoad",
                        AnalysisProgressFormatter.BuildCandidatePageLoadDiagnosticMessage(
                            pageNumber,
                            pageStart,
                            pageSize,
                            configuredNodeCount,
                            page.Count,
                            duration,
                            currentIndexStatus))),
                    ct)
                .ConfigureAwait(false);
        }

        return new CandidatePageLoadResult(page, currentIndexStatus, duration);
    }

    private async Task<LookupOutcome> ProcessLookupChunkAsync(
        RunStateDocument run,
        IReadOnlyCollection<string> documentIds,
        IReadOnlyList<NodeConfig> snapshotEvaluationNodes,
        CancellationToken ct)
    {
        if (documentIds.Count == 0)
            return new LookupOutcome([], [], [], [], 0, 0, 0, 0, 0, 0, 0, 0);

        var distinctDocumentIds = documentIds
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToArray();

        var diagnostics = new List<DiagnosticDocument>();
        if (distinctDocumentIds.Length != documentIds.Count)
        {
            diagnostics.Add(CreateDiagnostic(
                run.RunId,
                "DuplicateCandidateIdsCollapsed",
                $"Collapsed {documentIds.Count - distinctDocumentIds.Length:N0} duplicate candidate id(s) in a lookup chunk of {documentIds.Count:N0} entries."));
        }

        var existingMismatches = await _stateStore
            .LoadMismatchesByRunAndDocumentIdsAsync(run.RunId, distinctDocumentIds, ct)
            .ConfigureAwait(false);
        var existingMismatchByDocumentId = existingMismatches
            .GroupBy(mismatch => mismatch.DocumentId, StringComparer.OrdinalIgnoreCase)
            .ToDictionary(group => group.Key, group => group.First(), StringComparer.OrdinalIgnoreCase);

        if (existingMismatchByDocumentId.Count > 0)
        {
            diagnostics.Add(CreateDiagnostic(
                run.RunId,
                "ExistingMismatchDocumentsSkipped",
                $"Skipped {existingMismatchByDocumentId.Count:N0} candidate id(s) that already have a persisted mismatch document in run '{run.RunId}'."));
        }

        var pendingDocumentIds = distinctDocumentIds
            .Where(documentId => existingMismatchByDocumentId.ContainsKey(documentId) == false)
            .ToArray();
        if (pendingDocumentIds.Length == 0)
            return new LookupOutcome([], [], [], diagnostics, 0, 0, 0, 0, 0, 0, 0, 0);

        var snapshots = await LoadObservedStatesAsync(run, pendingDocumentIds, snapshotEvaluationNodes, ct).ConfigureAwait(false);
        var evaluations = new List<DocumentEvaluation>();

        foreach (var documentId in pendingDocumentIds)
        {
            var evaluation = ConsistencyDecisionEngine.EvaluateDocument(
                documentId,
                snapshots[documentId],
                run.ChangeVectorSemanticsSnapshot);
            if (evaluation != null)
                evaluations.Add(evaluation);
        }

        var mismatches = new List<MismatchDocument>();
        var repairs = new List<RepairDocument>();
        var repairGuards = new List<RepairActionGuardDocument>();
        var repairCandidates = new List<RepairCandidate>();
        var supportsRepairPlanning = run.RunMode is RunMode.ScanAndRepair or RunMode.DryRunRepair;
        var suppressedGuardedDocumentIds = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        long manualReviewDocumentsFound = 0;

        foreach (var evaluation in evaluations)
        {
            if (run.RunMode == RunMode.ScanOnly)
            {
                evaluation.RepairDecision = "ScanOnly_NoRepair";
                continue;
            }

            if (string.Equals(evaluation.MismatchType, "AMBIGUOUS_CV", StringComparison.Ordinal))
            {
                evaluation.RepairDecision = "SkippedAmbiguousCv";
                manualReviewDocumentsFound++;
                continue;
            }

            if (string.Equals(evaluation.MismatchType, "COLLECTION_MISMATCH", StringComparison.Ordinal) ||
                string.IsNullOrWhiteSpace(evaluation.Collection) ||
                string.IsNullOrWhiteSpace(evaluation.WinnerNode))
            {
                evaluation.RepairDecision = "SkippedCollectionMismatch";
                continue;
            }

            repairCandidates.Add(new RepairCandidate(
                evaluation.DocumentId,
                evaluation.Collection!,
                evaluation.WinnerNode!,
                evaluation.WinnerCV,
                evaluation.AffectedNodes));
        }

        long repairsPlanned = 0;
        long repairsAttempted = 0;
        long repairsPatchedOnWinner = 0;
        long repairsFailed = 0;

        if (supportsRepairPlanning && repairCandidates.Count > 0)
        {
            var guardedIds = await _stateStore.GetGuardedDocumentIdsAsync(
                    run.RunId,
                    repairCandidates.Select(candidate => candidate.DocumentId).ToArray(),
                    ct)
                .ConfigureAwait(false);
            if (guardedIds.Count > 0)
            {
                diagnostics.Add(CreateDiagnostic(
                    run.RunId,
                    "ExistingRepairGuardsSkipped",
                    $"Skipped {guardedIds.Count:N0} candidate id(s) that already have a repair-action guard in run '{run.RunId}'."));
            }

            var candidatesToExecute = new List<RepairCandidate>();
            foreach (var candidate in repairCandidates)
            {
                if (guardedIds.Contains(candidate.DocumentId))
                {
                    suppressedGuardedDocumentIds.Add(candidate.DocumentId);
                    continue;
                }

                candidatesToExecute.Add(candidate);
            }

            foreach (var group in candidatesToExecute
                         .GroupBy(candidate => new RepairGroupKey(candidate.WinnerNode, candidate.Collection, candidate.WinnerCV))
                         .SelectMany(group => group.Chunk(_config.Throttle.PageSize)))
            {
                var repairOutcome = await ExecuteRepairGroupAsync(run, group, evaluations, ct).ConfigureAwait(false);
                repairs.Add(repairOutcome.RepairDocument);
                repairGuards.AddRange(repairOutcome.RepairGuards);
                repairsPlanned += repairOutcome.RepairsPlanned;
                repairsAttempted += repairOutcome.RepairsAttempted;
                repairsPatchedOnWinner += repairOutcome.RepairsPatchedOnWinner;
                repairsFailed += repairOutcome.RepairsFailed;
            }
        }

        var evaluationsToPersist = evaluations
            .Where(evaluation => suppressedGuardedDocumentIds.Contains(evaluation.DocumentId) == false)
            .ToList();

        foreach (var evaluation in evaluationsToPersist)
        {
            mismatches.Add(new MismatchDocument
            {
                Id = StateStore.GetMismatchId(run.RunId, evaluation.DocumentId),
                RunId = run.RunId,
                DocumentId = evaluation.DocumentId,
                Collection = evaluation.Collection,
                MismatchType = evaluation.MismatchType,
                WinnerNode = evaluation.WinnerNode,
                WinnerCV = evaluation.WinnerCV,
                ObservedState = evaluation.ObservedState
                    .Select(CloneObservedState)
                    .ToList(),
                RepairDecision = evaluation.RepairDecision,
                CurrentRepairDecision = evaluation.RepairDecision,
                DetectedAt = DateTimeOffset.UtcNow
            });
        }

        return new LookupOutcome(
            mismatches,
            repairs,
            repairGuards,
            diagnostics,
            pendingDocumentIds.Length,
            pendingDocumentIds.Length,
            mismatches.Count,
            manualReviewDocumentsFound,
            repairsPlanned,
            repairsAttempted,
            repairsPatchedOnWinner,
            repairsFailed);
    }

    private async Task<RepairGroupOutcome> ExecuteRepairGroupAsync(
        RunStateDocument run,
        IReadOnlyCollection<RepairCandidate> candidates,
        IReadOnlyCollection<DocumentEvaluation> evaluations,
        CancellationToken ct)
    {
        var first = candidates.First();
        var documentIds = candidates.Select(candidate => candidate.DocumentId).ToArray();
        var affectedNodes = candidates
            .SelectMany(candidate => candidate.AffectedNodes)
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .OrderBy(node => node, StringComparer.OrdinalIgnoreCase)
            .ToList();

        var query = RepairPatchQueryBuilder.BuildTouchWinnerPatchQuery(first.Collection);
        var indexQuery = new IndexQuery
        {
            Query = query,
            QueryParameters = new Parameters
            {
                ["ids"] = documentIds
            }
        };

        if (run.RunMode == RunMode.DryRunRepair)
        {
            var dryRunDocument = new RepairDocument
            {
                Id = StateStore.CreateRepairId(run.RunId),
                RunId = run.RunId,
                DocumentIds = documentIds.ToList(),
                Collection = first.Collection,
                WinnerNode = first.WinnerNode,
                WinnerCV = first.WinnerCV,
                AffectedNodes = affectedNodes,
                PatchOperationId = null,
                RepairStatus = "PatchPlannedDryRun",
                CompletedAt = DateTimeOffset.UtcNow
            };

            var dryRunGuards = documentIds
                .Select(documentId => new RepairActionGuardDocument
                {
                    Id = StateStore.GetRepairActionGuardId(run.RunId, documentId),
                    RunId = run.RunId,
                    DocumentId = documentId,
                    WinnerNode = first.WinnerNode,
                    WinnerCV = first.WinnerCV,
                    PatchOperationId = null,
                    RecordedAt = DateTimeOffset.UtcNow
                })
                .ToList();

            foreach (var candidate in candidates)
            {
                var evaluation = evaluations.First(e =>
                    string.Equals(e.DocumentId, candidate.DocumentId, StringComparison.OrdinalIgnoreCase));
                evaluation.RepairDecision = "PatchPlannedDryRun";
            }

            return new RepairGroupOutcome(
                dryRunDocument,
                dryRunGuards,
                documentIds.Length,
                0,
                0,
                0);
        }

        var repairDocument = new RepairDocument
        {
            Id = StateStore.CreateRepairId(run.RunId),
            RunId = run.RunId,
            DocumentIds = documentIds.ToList(),
            Collection = first.Collection,
            WinnerNode = first.WinnerNode,
            WinnerCV = first.WinnerCV,
            AffectedNodes = affectedNodes,
            RepairStatus = "PatchFailed",
            CompletedAt = DateTimeOffset.UtcNow
        };

        var guards = documentIds
            .Select(documentId => new RepairActionGuardDocument
            {
                Id = StateStore.GetRepairActionGuardId(run.RunId, documentId),
                RunId = run.RunId,
                DocumentId = documentId,
                WinnerNode = first.WinnerNode,
                WinnerCV = first.WinnerCV,
                RecordedAt = DateTimeOffset.UtcNow
            })
            .ToList();

        try
        {
            var operation = await _storesByNodeUrl[first.WinnerNode]
                .Operations
                .SendAsync(
                    new PatchByQueryOperation(indexQuery, new QueryOperationOptions { RetrieveDetails = true }),
                    token: ct)
                .ConfigureAwait(false);

            var result = await operation.WaitForCompletionAsync<BulkOperationResult>(ct).ConfigureAwait(false);
            repairDocument.CompletedAt = DateTimeOffset.UtcNow;

            var statusById = BuildPatchStatusById(result, documentIds);
            var patchedCount = 0L;
            var failedCount = 0L;

            foreach (var candidate in candidates)
            {
                var evaluation = evaluations.First(e =>
                    string.Equals(e.DocumentId, candidate.DocumentId, StringComparison.OrdinalIgnoreCase));

                if (!statusById.TryGetValue(candidate.DocumentId, out var status))
                {
                    evaluation.RepairDecision = result.Total > 0
                        ? "PatchCompletedOnWinner"
                        : "SkippedStateChanged";
                }
                else
                {
                    evaluation.RepairDecision = MapPatchStatusToDecision(status);
                }

                if (string.Equals(evaluation.RepairDecision, "PatchCompletedOnWinner", StringComparison.Ordinal))
                    patchedCount++;
                else
                    failedCount++;
            }

            repairDocument.RepairStatus = patchedCount > 0
                ? "PatchCompletedOnWinner"
                : "SkippedStateChanged";
            repairDocument.Error = failedCount > 0
                ? $"Non-patched documents in group: {failedCount} of {documentIds.Length}."
                : null;

            return new RepairGroupOutcome(
                repairDocument,
                guards,
                0,
                documentIds.Length,
                patchedCount,
                failedCount);
        }
        catch (Exception ex)
        {
            repairDocument.CompletedAt = DateTimeOffset.UtcNow;
            repairDocument.RepairStatus = "PatchFailed";
            repairDocument.Error = ex.Message;

            foreach (var candidate in candidates)
            {
                var evaluation = evaluations.First(e =>
                    string.Equals(e.DocumentId, candidate.DocumentId, StringComparison.OrdinalIgnoreCase));
                evaluation.RepairDecision = "PatchFailed";
            }

            return new RepairGroupOutcome(
                repairDocument,
                guards,
                0,
                documentIds.Length,
                0,
                documentIds.Length);
        }
    }

    private async Task<IReadOnlyList<NodeConfig>> ResolveSnapshotEvaluationNodesAsync(
        RunStateDocument run,
        CancellationToken ct)
    {
        if (RunModePolicies.UsesLocalSnapshotEvaluation(run.RunMode) == false)
            return _config.Nodes;

        if (string.IsNullOrWhiteSpace(run.SourceSnapshotRunId))
            return run.ClusterNodes.Count > 0 ? run.ClusterNodes : _config.Nodes;

        var sourceSnapshotRun = await _stateStore.LoadRunAsync(run.SourceSnapshotRunId, ct).ConfigureAwait(false);
        if (sourceSnapshotRun?.ClusterNodes.Count > 0)
            return sourceSnapshotRun.ClusterNodes;

        return run.ClusterNodes.Count > 0 ? run.ClusterNodes : _config.Nodes;
    }

    private async Task<Dictionary<string, List<NodeObservedState>>> LoadObservedStatesAsync(
        RunStateDocument run,
        string[] documentIds,
        IReadOnlyList<NodeConfig> snapshotEvaluationNodes,
        CancellationToken ct)
    {
        return RunModePolicies.UsesLocalSnapshotEvaluation(run.RunMode)
            ? await FetchImportedSnapshotsAsync(documentIds, snapshotEvaluationNodes, ct).ConfigureAwait(false)
            : await FetchClusterSnapshotsAsync(documentIds, snapshotEvaluationNodes, ct).ConfigureAwait(false);
    }

    private async Task<Dictionary<string, List<NodeObservedState>>> FetchImportedSnapshotsAsync(
        string[] documentIds,
        IReadOnlyList<NodeConfig> snapshotEvaluationNodes,
        CancellationToken ct)
    {
        var snapshotIds = ImportedSnapshotStateBuilder.BuildPhysicalSnapshotIds(documentIds, snapshotEvaluationNodes);
        var snapshotsByPhysicalId = await _stateStore.LoadNodeSnapshotsByIdsAsync(snapshotIds, ct).ConfigureAwait(false);
        return ImportedSnapshotStateBuilder.BuildObservedStates(documentIds, snapshotEvaluationNodes, snapshotsByPhysicalId);
    }

    private static string BuildCandidateProgressDetail(RunStateDocument run)
        => RunModePolicies.UsesLocalSnapshotEvaluation(run.RunMode)
            ? $"evaluated locally {run.CandidateDocumentsProcessed:N0} / {run.CandidateDocumentsFound:N0} candidates"
            : $"revalidated {run.CandidateDocumentsProcessed:N0} / {run.CandidateDocumentsFound:N0} candidates";

    private async Task<Dictionary<string, List<NodeObservedState>>> FetchClusterSnapshotsAsync(
        string[] documentIds,
        IReadOnlyList<NodeConfig> snapshotEvaluationNodes,
        CancellationToken ct)
    {
        var tasks = snapshotEvaluationNodes.Select(async node =>
        {
            var metadata = await FetchBatchMetadataAsync(
                    _storesByNodeUrl[node.Url],
                    node.Url,
                    documentIds,
                    ct)
                .ConfigureAwait(false);
            return (NodeUrl: node.Url, Metadata: metadata);
        });

        var results = await Task.WhenAll(tasks).ConfigureAwait(false);
        var metadataByNode = results.ToDictionary(
            result => result.NodeUrl,
            result => result.Metadata,
            StringComparer.OrdinalIgnoreCase);

        var snapshots = documentIds.ToDictionary(
            id => id,
            _ => new List<NodeObservedState>(snapshotEvaluationNodes.Count),
            StringComparer.OrdinalIgnoreCase);

        foreach (var node in snapshotEvaluationNodes)
        {
            var perNode = metadataByNode[node.Url];
            foreach (var documentId in documentIds)
                snapshots[documentId].Add(CloneObservedState(perNode[documentId]));
        }

        return snapshots;
    }

    private async Task<Dictionary<string, NodeObservedState>> FetchBatchMetadataAsync(
        IDocumentStore store,
        string nodeUrl,
        string[] documentIds,
        CancellationToken ct)
    {
        return await _retry.ExecuteAsync(async token =>
        {
            using var contextScope = store.GetRequestExecutor(_config.DatabaseName)
                .ContextPool
                .AllocateOperationContext(out JsonOperationContext context);

            var command = new GetDocumentsCommand(
                store.Conventions,
                documentIds,
                includes: null,
                metadataOnly: true);

            await store.GetRequestExecutor(_config.DatabaseName)
                .ExecuteAsync(command, context, null, token)
                .ConfigureAwait(false);

            var results = documentIds.ToDictionary(
                documentId => documentId,
                _ => new NodeObservedState
                {
                    NodeUrl = nodeUrl,
                    Present = false
                },
                StringComparer.OrdinalIgnoreCase);

            var documents = command.Result?.Results;
            if (documents == null)
                return results;

            for (var i = 0; i < documents.Length; i++)
            {
                if (documents[i] is not BlittableJsonReaderObject document)
                    continue;

                if (!document.TryGet("@metadata", out BlittableJsonReaderObject metadata))
                    continue;

                if (!metadata.TryGet("@id", out string actualId) ||
                    string.IsNullOrWhiteSpace(actualId) ||
                    !results.TryGetValue(actualId, out var state))
                {
                    continue;
                }

                metadata.TryGet("@change-vector", out string changeVector);
                metadata.TryGet("@last-modified", out string lastModified);
                metadata.TryGet("@collection", out string collection);

                state.Present = true;
                state.ChangeVector = changeVector;
                state.LastModified = lastModified;
                state.Collection = collection;
            }

            return results;
        }, ct).ConfigureAwait(false);
    }

    private void PublishProgress(
        string phase,
        string? currentNodeLabel,
        string? detail,
        RunStateDocument run)
        => PublishProgress(
            phase,
            currentNodeLabel,
            detail,
            currentNodeStreamedSnapshots: null,
            currentNodeTotalDocuments: null,
            run: run);

    private void PublishProgress(
        string phase,
        string? currentNodeLabel,
        string? detail,
        long? currentNodeStreamedSnapshots,
        long? currentNodeTotalDocuments,
        RunStateDocument run)
    {
        ProgressUpdated?.Invoke(new IndexedRunProgressUpdate(
            Phase: phase,
            CurrentNodeLabel: currentNodeLabel,
            Detail: detail,
            CurrentNodeStreamedSnapshots: currentNodeStreamedSnapshots,
            CurrentNodeTotalDocuments: currentNodeTotalDocuments,
            SnapshotDocumentsImported: run.SnapshotDocumentsImported,
            SnapshotDocumentsSkipped: run.SnapshotDocumentsSkipped,
            SnapshotBulkInsertRestarts: run.SnapshotBulkInsertRestarts,
            CandidateDocumentsFound: run.CandidateDocumentsFound,
            CandidateDocumentsProcessed: run.CandidateDocumentsProcessed,
            CandidateDocumentsExcludedBySkippedSnapshots: run.CandidateDocumentsExcludedBySkippedSnapshots,
            DocumentsInspected: run.DocumentsInspected,
            UniqueVersionsCompared: run.UniqueVersionsCompared,
            MismatchesFound: run.MismatchesFound,
            RepairsPlanned: run.RepairsPlanned,
            RepairsAttempted: run.RepairsAttempted,
            RepairsPatchedOnWinner: run.RepairsPatchedOnWinner,
            RepairsFailed: run.RepairsFailed));
    }

    private sealed record CandidatePageLoadResult(
        IReadOnlyList<ProblemDocumentIndexEntry> Page,
        SnapshotProblemIndexStatus IndexStatus,
        TimeSpan Duration);

    private static IReadOnlyCollection<DiagnosticDocument> CreateDiagnostics(params DiagnosticDocument[] diagnostics)
        => diagnostics;

    private static DiagnosticDocument CreateDiagnostic(
        string runId,
        string kind,
        string message,
        string? nodeUrl = null)
    {
        return new DiagnosticDocument
        {
            Id = StateStore.CreateDiagnosticId(runId),
            RunId = runId,
            Kind = kind,
            NodeUrl = nodeUrl,
            Message = message,
            Timestamp = DateTimeOffset.UtcNow
        };
    }

    private static NodeObservedState CloneObservedState(NodeObservedState state)
    {
        return new NodeObservedState
        {
            NodeUrl = state.NodeUrl,
            Present = state.Present,
            Collection = state.Collection,
            ChangeVector = state.ChangeVector,
            LastModified = state.LastModified
        };
    }

    private static SnapshotImportNodeState CloneSnapshotImportNodeState(SnapshotImportNodeState state)
    {
        return new SnapshotImportNodeState
        {
            NodeUrl = state.NodeUrl,
            NodeLabel = state.NodeLabel,
            IsImportComplete = state.IsImportComplete,
            ImportCompletedAt = state.ImportCompletedAt,
            CurrentSegmentId = state.CurrentSegmentId,
            CurrentPageNumber = state.CurrentPageNumber,
            CurrentRowOffsetInPage = state.CurrentRowOffsetInPage,
            ImportedRows = state.ImportedRows,
            SnapshotsCommitted = state.SnapshotsCommitted,
            SnapshotsSkipped = state.SnapshotsSkipped,
            BulkInsertRestartCount = state.BulkInsertRestartCount,
            LastConfirmedSnapshotId = state.LastConfirmedSnapshotId,
            LastSkippedSnapshotId = state.LastSkippedSnapshotId,
            LastError = state.LastError,
            LastUpdatedAt = state.LastUpdatedAt
        };
    }

    private static Dictionary<string, PatchStatus> BuildPatchStatusById(
        BulkOperationResult result,
        IReadOnlyCollection<string> fallbackIds)
    {
        var statusById = new Dictionary<string, PatchStatus>(StringComparer.OrdinalIgnoreCase);
        foreach (var detail in result.Details.OfType<BulkOperationResult.PatchDetails>())
        {
            if (!string.IsNullOrWhiteSpace(detail.Id))
                statusById[detail.Id] = detail.Status;
        }

        if (statusById.Count == 0 && result.Total > 0)
        {
            foreach (var id in fallbackIds)
                statusById[id] = PatchStatus.Patched;
        }

        return statusById;
    }

    private static string MapPatchStatusToDecision(PatchStatus status)
        => status switch
        {
            PatchStatus.Patched => "PatchCompletedOnWinner",
            PatchStatus.DocumentDoesNotExist => "SkippedStateChanged",
            PatchStatus.NotModified => "PatchFailed",
            PatchStatus.Skipped => "PatchFailed",
            PatchStatus.Created => "PatchFailed",
            _ => "PatchFailed"
        };

    private static RepairDocument CreateRepairDecision(
        RunStateDocument run,
        DocumentEvaluation evaluation,
        string status,
        string? error)
    {
        return new RepairDocument
        {
            Id = StateStore.CreateRepairId(run.RunId),
            RunId = run.RunId,
            DocumentIds = [evaluation.DocumentId],
            Collection = evaluation.Collection,
            WinnerNode = evaluation.WinnerNode ?? string.Empty,
            WinnerCV = evaluation.WinnerCV,
            AffectedNodes = evaluation.AffectedNodes.ToList(),
            RepairStatus = status,
            CompletedAt = DateTimeOffset.UtcNow,
            Error = error
        };
    }

    private static ResiliencePipeline BuildRetryPipeline(ThrottleConfig throttle)
        => new ResiliencePipelineBuilder()
            .AddRetry(new RetryStrategyOptions
            {
                MaxRetryAttempts = throttle.MaxRetries,
                Delay = TimeSpan.FromMilliseconds(throttle.RetryBaseDelayMs),
                BackoffType = DelayBackoffType.Exponential,
                UseJitter = true,
                ShouldHandle = new PredicateBuilder()
                    .Handle<HttpRequestException>()
                    .Handle<IOException>()
                    .Handle<TaskCanceledException>(exception =>
                        !exception.CancellationToken.IsCancellationRequested)
                    .Handle<RavenException>(exception =>
                        exception.Message.Contains("timeout", StringComparison.OrdinalIgnoreCase) ||
                        exception.Message.Contains("connect", StringComparison.OrdinalIgnoreCase) ||
                        exception.Message.Contains("unavailable", StringComparison.OrdinalIgnoreCase))
            })
            .Build();

    private sealed record NodeSnapshotImportResult(
        string RunId,
        string NodeLabel,
        string NodeUrl,
        SnapshotImportNodeState NodeState);

    private sealed record PendingImportCursor(
        string SnapshotId,
        SnapshotCacheImportCursor NextCursor);

    private sealed record RepairCandidate(
        string DocumentId,
        string Collection,
        string WinnerNode,
        string? WinnerCV,
        List<string> AffectedNodes);

    private sealed record RepairGroupKey(
        string WinnerNode,
        string Collection,
        string? WinnerCV);

    private sealed record LookupOutcome(
        List<MismatchDocument> Mismatches,
        List<RepairDocument> Repairs,
        List<RepairActionGuardDocument> RepairGuards,
        List<DiagnosticDocument> Diagnostics,
        long DocumentsRevalidated,
        long UniqueVersionsCompared,
        long MismatchesFound,
        long ManualReviewDocumentsFound,
        long RepairsPlanned,
        long RepairsAttempted,
        long RepairsPatchedOnWinner,
        long RepairsFailed);

    private sealed record RepairGroupOutcome(
        RepairDocument RepairDocument,
        List<RepairActionGuardDocument> RepairGuards,
        long RepairsPlanned,
        long RepairsAttempted,
        long RepairsPatchedOnWinner,
        long RepairsFailed);
}
