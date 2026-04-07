using System.Threading.Channels;

namespace ConsistencyCheck;

/// <summary>
/// [TEMPORARY] Enumerates all already-imported snapshots node by node (A→B→C),
/// compares snapshot metadata across all nodes purely from local data,
/// and builds a repair plan. No cluster access required.
/// Skips documents that were successfully repaired in any previous run.
/// </summary>
internal sealed class SnapshotCrossCheckPlanner : IAsyncDisposable
{
    private readonly AppConfig _config;
    private readonly StateStore _stateStore;

    // Fully offline: reads only from local state store. No cluster access.
    // Large page size is safe — no server-side throttling needed.
    private const int PageSize = 32768;

    // Cursor field names reused for SnapshotCrossCheck:
    //   run.SafeRestartEtag     = current node index (0=A, 1=B, 2=C)
    //   run.SourceSnapshotRunId = last processed OriginalDocumentId (cursor within node)

    public SnapshotCrossCheckPlanner(AppConfig config, StateStore stateStore)
    {
        _config = config;
        _stateStore = stateStore;
    }

    public event Action<IndexedRunProgressUpdate>? ProgressUpdated;

    public async Task<RunStateDocument> RunAsync(RunStateDocument run, CancellationToken ct)
    {
        PublishProgress("loading repaired set", null, "loading successfully repaired documents", run);

        var repairedSet = await _stateStore.LoadSuccessfullyRepairedDocumentIdsAsync(ct).ConfigureAwait(false);

        PublishProgress("repaired set loaded", null, $"skipping {repairedSet.Count:N0} already-repaired documents", run);

        // Cursor encoding:
        //   run.SafeRestartEtag    = nodeIndex (0=A, 1=B, 2=C)
        //   run.SourceSnapshotRunId = skip offset within that node (stored as decimal string)
        var startNodeIndex = (int)(run.SafeRestartEtag ?? 0L);
        var startSkip = int.TryParse(run.SourceSnapshotRunId, out var parsedSkip) ? parsedSkip : 0;

        // Producer/consumer pipeline: producer reads pages from local DB into a bounded channel
        // while the consumer processes them. This keeps disk reads and DB writes overlapped.
        var channel = Channel.CreateBounded<PageWork>(new BoundedChannelOptions(4)
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleReader = true,
            SingleWriter = true
        });

        // pipelineCts lets the consumer cancel the producer on abnormal exit
        using var pipelineCts = CancellationTokenSource.CreateLinkedTokenSource(ct);

        var producerTask = Task.Run(
            () => ProducePagesAsync(channel.Writer, repairedSet, startNodeIndex, startSkip, run, pipelineCts.Token),
            pipelineCts.Token);

        try
        {
            await foreach (var work in channel.Reader.ReadAllAsync(ct).ConfigureAwait(false))
            {
                if (work.IsNodeComplete)
                {
                    PublishProgress($"node {work.NodeAlias} complete", work.NodeLabel,
                        $"finished scanning node {work.NodeAlias}", run);
                    continue;
                }

                // Update cursor before any DB writes so resume lands past this page
                run.SafeRestartEtag = work.NodeIndex;
                run.SourceSnapshotRunId = work.SkipAfterThisPage.ToString();

                if (work.DocumentIds.Length > 0)
                {
                    var result = await ProcessChunkAsync(run, work.NodeIndex, work.DocumentIds, work.SnapshotByDocId, ct)
                        .ConfigureAwait(false);

                    run.DocumentsInspected += work.DocumentIds.Length;
                    run.MismatchesFound += result.MismatchesFound;
                    run.RepairsPlanned += result.RepairsPlanned;

                    await _stateStore.PersistBatchAsync(
                            run,
                            result.Mismatches,
                            result.Repairs,
                            result.Guards,
                            result.Diagnostics,
                            ct)
                        .ConfigureAwait(false);
                }
                else
                {
                    await _stateStore.SaveRunAsync(run, ct).ConfigureAwait(false);
                }

                PublishProgress($"scanning node {work.NodeAlias}", work.NodeLabel,
                    $"offset {work.SkipAfterThisPage}", run);
            }
        }
        finally
        {
            // Cancel the producer so it does not stay blocked on WriteAsync if consumer exited early
            pipelineCts.Cancel();
            try { await producerTask.ConfigureAwait(false); }
            catch { /* producer was cancelled or its error already surfaced via channel */ }
        }

        run.IsComplete = true;
        run.CompletedAt = DateTimeOffset.UtcNow;

        await _stateStore.PersistBatchAsync(
                run,
                [],
                [],
                [],
                [
                    new DiagnosticDocument
                    {
                        Id = StateStore.CreateDiagnosticId(run.RunId),
                        RunId = run.RunId,
                        Kind = "SnapshotCrossCheckSummary",
                        Message =
                            $"Complete. DocumentsInspected={run.DocumentsInspected:N0}, MismatchesFound={run.MismatchesFound:N0}, RepairsPlanned={run.RepairsPlanned:N0}",
                        Timestamp = DateTimeOffset.UtcNow
                    }
                ],
                ct)
            .ConfigureAwait(false);

        PublishProgress("complete", null, "snapshot cross-check complete", run);
        return run;
    }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;

    private async Task ProducePagesAsync(
        ChannelWriter<PageWork> writer,
        HashSet<string> repairedSet,
        int startNodeIndex,
        int startSkip,
        RunStateDocument run,
        CancellationToken ct)
    {
        try
        {
            for (var nodeIndex = startNodeIndex; nodeIndex < _config.Nodes.Count; nodeIndex++)
            {
                var nodeAlias = NodeDocumentSnapshots.GetNodeAlias(nodeIndex);
                var nodeLabel = _config.Nodes[nodeIndex].Label;
                var skip = nodeIndex == startNodeIndex ? startSkip : 0;

                PublishProgress($"scanning node {nodeAlias}", nodeLabel, $"starting at offset {skip}", run);

                while (true)
                {
                    ct.ThrowIfCancellationRequested();

                    var page = await _stateStore.LoadSnapshotPageByNodeAliasAsync(nodeAlias, skip, PageSize, ct)
                        .ConfigureAwait(false);

                    if (page.Count == 0)
                        break;

                    var documentIds = new List<string>(page.Count);
                    var snapshotByDocId = new Dictionary<string, NodeDocumentSnapshot>(StringComparer.OrdinalIgnoreCase);

                    foreach (var snapshot in page)
                    {
                        if (repairedSet.Contains(snapshot.OriginalDocumentId))
                            continue;
                        documentIds.Add(snapshot.OriginalDocumentId);
                        snapshotByDocId[snapshot.OriginalDocumentId] = snapshot;
                    }

                    skip += page.Count;

                    await writer.WriteAsync(
                            new PageWork(nodeIndex, nodeAlias, nodeLabel, skip,
                                documentIds.ToArray(), snapshotByDocId),
                            ct)
                        .ConfigureAwait(false);

                    if (page.Count < PageSize)
                        break;
                }

                // Sentinel: notifies the consumer that the node is fully read
                await writer.WriteAsync(
                        new PageWork(nodeIndex, nodeAlias, nodeLabel, skip,
                            [], new Dictionary<string, NodeDocumentSnapshot>(), IsNodeComplete: true),
                        ct)
                    .ConfigureAwait(false);
            }
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            writer.TryComplete(ex); // surface producer errors through ReadAllAsync
            throw;
        }
        finally
        {
            writer.TryComplete();
        }
    }

    private async Task<ChunkResult> ProcessChunkAsync(
        RunStateDocument run,
        int sourceNodeIndex,
        string[] documentIds,
        IReadOnlyDictionary<string, NodeDocumentSnapshot> snapshotByDocId,
        CancellationToken ct)
    {
        var sourceNodeAlias = NodeDocumentSnapshots.GetNodeAlias(sourceNodeIndex);

        // Load snapshots for all non-source nodes from local DB
        var nonSourceAliases = Enumerable.Range(0, _config.Nodes.Count)
            .Where(i => i != sourceNodeIndex)
            .Select(NodeDocumentSnapshots.GetNodeAlias)
            .ToArray();

        var otherSnapshots = await _stateStore
            .LoadSnapshotsByDocumentIdsAsync(documentIds, nonSourceAliases, ct)
            .ConfigureAwait(false);

        // Build per-document node states (one entry per node, in config order)
        var snapshots = documentIds.ToDictionary(
            id => id,
            _ => new List<NodeObservedState>(_config.Nodes.Count),
            StringComparer.OrdinalIgnoreCase);

        for (var i = 0; i < _config.Nodes.Count; i++)
        {
            var node = _config.Nodes[i];
            var alias = NodeDocumentSnapshots.GetNodeAlias(i);

            foreach (var documentId in documentIds)
            {
                NodeObservedState state;
                if (i == sourceNodeIndex)
                {
                    state = snapshotByDocId.TryGetValue(documentId, out var snap)
                        ? SnapshotToObservedState(snap, node.Url)
                        : new NodeObservedState { NodeUrl = node.Url, Present = false };
                }
                else
                {
                    var snapshotId = NodeDocumentSnapshots.GetSnapshotId(documentId, alias);
                    state = otherSnapshots.TryGetValue(snapshotId, out var otherSnap) && otherSnap != null
                        ? SnapshotToObservedState(otherSnap, node.Url)
                        : new NodeObservedState { NodeUrl = node.Url, Present = false };
                }
                snapshots[documentId].Add(state);
            }
        }

        // Evaluate each document
        var evaluations = new List<DocumentEvaluation>();
        foreach (var documentId in documentIds)
        {
            var evaluation = ConsistencyDecisionEngine.EvaluateDocument(
                documentId,
                snapshots[documentId],
                run.ChangeVectorSemanticsSnapshot);
            if (evaluation != null)
                evaluations.Add(evaluation);
        }

        // Build repair candidates (skip AMBIGUOUS_CV and COLLECTION_MISMATCH)
        var repairCandidates = new List<RepairCandidate>();
        foreach (var evaluation in evaluations)
        {
            if (string.Equals(evaluation.MismatchType, "AMBIGUOUS_CV", StringComparison.Ordinal))
            {
                evaluation.RepairDecision = "SkippedAmbiguousCv";
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

        // Idempotency: skip already-guarded documents in this run
        var guardedIds = repairCandidates.Count > 0
            ? await _stateStore.GetGuardedDocumentIdsAsync(
                    run.RunId,
                    repairCandidates.Select(c => c.DocumentId).ToArray(),
                    ct)
                .ConfigureAwait(false)
            : (IReadOnlySet<string>)new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        var mismatches = new List<MismatchDocument>();
        var repairs = new List<RepairDocument>();
        var guards = new List<RepairActionGuardDocument>();
        var repairsPlanned = 0L;

        foreach (var group in repairCandidates
                     .Where(c => !guardedIds.Contains(c.DocumentId))
                     .GroupBy(c => new RepairGroupKey(c.WinnerNode, c.Collection, c.WinnerCV)))
        {
            var candidates = group.ToList();
            var first = candidates[0];
            var groupDocIds = candidates.Select(c => c.DocumentId).ToArray();
            var affectedNodes = candidates
                .SelectMany(c => c.AffectedNodes)
                .Distinct(StringComparer.OrdinalIgnoreCase)
                .OrderBy(static n => n, StringComparer.OrdinalIgnoreCase)
                .ToList();

            repairs.Add(new RepairDocument
            {
                Id = StateStore.CreateRepairId(run.RunId),
                RunId = run.RunId,
                DocumentIds = groupDocIds.ToList(),
                Collection = first.Collection,
                WinnerNode = first.WinnerNode,
                WinnerCV = first.WinnerCV,
                AffectedNodes = affectedNodes,
                PatchOperationId = null,
                RepairStatus = "PatchPlannedDryRun",
                CompletedAt = DateTimeOffset.UtcNow
            });

            foreach (var candidate in candidates)
            {
                guards.Add(new RepairActionGuardDocument
                {
                    Id = StateStore.GetRepairActionGuardId(run.RunId, candidate.DocumentId),
                    RunId = run.RunId,
                    DocumentId = candidate.DocumentId,
                    WinnerNode = first.WinnerNode,
                    WinnerCV = first.WinnerCV,
                    PatchOperationId = null,
                    RecordedAt = DateTimeOffset.UtcNow
                });

                var eval = evaluations.First(e =>
                    string.Equals(e.DocumentId, candidate.DocumentId, StringComparison.OrdinalIgnoreCase));
                eval.RepairDecision = "PatchPlannedDryRun";
            }

            repairsPlanned += groupDocIds.Length;
        }

        // Mark guarded candidates
        foreach (var candidate in repairCandidates.Where(c => guardedIds.Contains(c.DocumentId)))
        {
            var eval = evaluations.First(e =>
                string.Equals(e.DocumentId, candidate.DocumentId, StringComparison.OrdinalIgnoreCase));
            if (string.IsNullOrWhiteSpace(eval.RepairDecision))
                eval.RepairDecision = "SkippedAlreadyPlanned";
        }

        foreach (var evaluation in evaluations)
        {
            // If the guard fired (same document already seen from another node in this run),
            // skip writing a new MismatchDocument — the existing one already has the correct
            // PatchPlannedDryRun decision and writing SkippedAlreadyPlanned would overwrite it.
            if (string.Equals(evaluation.RepairDecision, "SkippedAlreadyPlanned", StringComparison.Ordinal))
                continue;

            mismatches.Add(new MismatchDocument
            {
                Id = StateStore.GetMismatchId(run.RunId, evaluation.DocumentId),
                RunId = run.RunId,
                DocumentId = evaluation.DocumentId,
                Collection = evaluation.Collection,
                MismatchType = evaluation.MismatchType,
                WinnerNode = evaluation.WinnerNode,
                WinnerCV = evaluation.WinnerCV,
                ObservedState = evaluation.ObservedState,
                RepairDecision = evaluation.RepairDecision,
                CurrentRepairDecision = evaluation.RepairDecision,
                DetectedAt = DateTimeOffset.UtcNow
            });
        }

        var diagnostic = new DiagnosticDocument
        {
            Id = StateStore.CreateDiagnosticId(run.RunId),
            RunId = run.RunId,
            Kind = "SnapshotCrossCheckBatch",
            UniqueIdsInLookupBatch = documentIds.Length,
            MismatchesInBatch = evaluations.Count,
            RepairsTriggered = (int)repairsPlanned,
            Message = $"Node={sourceNodeAlias}, UniqueIds={documentIds.Length}, Mismatches={evaluations.Count}, RepairsPlanned={repairsPlanned}",
            Timestamp = DateTimeOffset.UtcNow
        };

        return new ChunkResult(mismatches, repairs, guards, [diagnostic], evaluations.Count, repairsPlanned);
    }

    private static NodeObservedState SnapshotToObservedState(NodeDocumentSnapshot snapshot, string nodeUrl)
        => new()
        {
            NodeUrl = nodeUrl,
            Present = true,
            ChangeVector = snapshot.ChangeVector,
            Collection = snapshot.Collection,
            LastModified = snapshot.LastModified?.UtcDateTime.ToString("O", System.Globalization.CultureInfo.InvariantCulture)
        };

    private void PublishProgress(string phase, string? currentNodeLabel, string? detail, RunStateDocument run)
    {
        ProgressUpdated?.Invoke(new IndexedRunProgressUpdate(
            Phase: phase,
            CurrentNodeLabel: currentNodeLabel,
            Detail: detail,
            CurrentNodeStreamedSnapshots: null,
            CurrentNodeTotalDocuments: null,
            SnapshotDocumentsImported: 0,
            SnapshotDocumentsSkipped: 0,
            SnapshotBulkInsertRestarts: 0,
            CandidateDocumentsFound: run.MismatchesFound,
            CandidateDocumentsProcessed: run.MismatchesFound,
            CandidateDocumentsExcludedBySkippedSnapshots: 0,
            DocumentsInspected: run.DocumentsInspected,
            UniqueVersionsCompared: run.DocumentsInspected,
            MismatchesFound: run.MismatchesFound,
            RepairsPlanned: run.RepairsPlanned,
            RepairsAttempted: 0,
            RepairsPatchedOnWinner: 0,
            RepairsFailed: 0));
    }

    private sealed record RepairCandidate(
        string DocumentId,
        string Collection,
        string WinnerNode,
        string? WinnerCV,
        List<string> AffectedNodes);

    private sealed record RepairGroupKey(string WinnerNode, string Collection, string? WinnerCV);

    private sealed record ChunkResult(
        IReadOnlyList<MismatchDocument> Mismatches,
        IReadOnlyList<RepairDocument> Repairs,
        IReadOnlyList<RepairActionGuardDocument> Guards,
        IReadOnlyList<DiagnosticDocument> Diagnostics,
        long MismatchesFound,
        long RepairsPlanned);

    private sealed record PageWork(
        int NodeIndex,
        string NodeAlias,
        string NodeLabel,
        int SkipAfterThisPage,
        string[] DocumentIds,
        IReadOnlyDictionary<string, NodeDocumentSnapshot> SnapshotByDocId,
        bool IsNodeComplete = false);
}