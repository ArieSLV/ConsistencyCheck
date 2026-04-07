namespace ConsistencyCheck;

/// <summary>
/// [TEMPORARY] Fixes MismatchDocuments where RepairDecision was incorrectly overwritten to
/// "SkippedAlreadyPlanned" during a SnapshotCrossCheck run. This happened because the
/// MismatchDocument ID is deterministic (based on runId + documentId hash), so when the same
/// document was encountered again while scanning a second node, the new "SkippedAlreadyPlanned"
/// mismatch silently replaced the existing "PatchPlannedDryRun" one.
///
/// This planner restores all affected MismatchDocuments in the target run to "PatchPlannedDryRun"
/// so they appear in the AutomaticRepairPlanDocuments index and can be applied.
/// </summary>
internal sealed class MismatchDecisionFixupPlanner : IAsyncDisposable
{
    private readonly StateStore _stateStore;

    // run.SourceSnapshotRunId holds the target SnapshotCrossCheck RunId to fix.

    public MismatchDecisionFixupPlanner(StateStore stateStore)
    {
        _stateStore = stateStore;
    }

    public event Action<IndexedRunProgressUpdate>? ProgressUpdated;

    public async Task<RunStateDocument> RunAsync(RunStateDocument run, CancellationToken ct)
    {
        var targetRunId = run.SourceSnapshotRunId
            ?? throw new InvalidOperationException(
                $"MismatchDecisionFixup run '{run.RunId}' is missing SourceSnapshotRunId (the target SnapshotCrossCheck run to fix).");

        PublishProgress("loading", null, "loading successfully repaired documents", run);
        var repairedSet = await _stateStore.LoadSuccessfullyRepairedDocumentIdsAsync(ct).ConfigureAwait(false);

        PublishProgress("loading mismatches", null, $"loading mismatches from {targetRunId}", run);
        var allMismatches = await _stateStore.LoadMismatchesByRunIdPrefixAsync(targetRunId, ct).ConfigureAwait(false);

        run.DocumentsInspected = allMismatches.Count;
        PublishProgress("inspecting", null, $"found {allMismatches.Count:N0} total mismatches", run);

        var toFix = allMismatches
            .Where(m =>
                string.Equals(m.RepairDecision, "SkippedAlreadyPlanned", StringComparison.Ordinal) &&
                !repairedSet.Contains(m.DocumentId))
            .ToList();

        if (toFix.Count == 0)
        {
            PublishProgress("complete", null, "nothing to fix — no SkippedAlreadyPlanned mismatches found", run);
            run.MismatchesFound = 0;
            run.IsComplete = true;
            run.CompletedAt = DateTimeOffset.UtcNow;
            await _stateStore.SaveRunAsync(run, ct).ConfigureAwait(false);
            return run;
        }

        PublishProgress("fixing", null, $"restoring {toFix.Count:N0} mismatches to PatchPlannedDryRun", run);

        foreach (var mismatch in toFix)
        {
            mismatch.RepairDecision = "PatchPlannedDryRun";
            mismatch.CurrentRepairDecision = "PatchPlannedDryRun";
        }

        // Save in batches to avoid oversized sessions
        const int batchSize = 512;
        var saved = 0;
        for (var i = 0; i < toFix.Count; i += batchSize)
        {
            ct.ThrowIfCancellationRequested();
            var batch = toFix.Skip(i).Take(batchSize).ToList();
            await _stateStore.StoreMismatchesAsync(batch, ct).ConfigureAwait(false);
            saved += batch.Count;
            run.MismatchesFound = saved;
            PublishProgress("fixing", null, $"restored {saved:N0}/{toFix.Count:N0}", run);
        }

        run.MismatchesFound = toFix.Count;
        run.IsComplete = true;
        run.CompletedAt = DateTimeOffset.UtcNow;
        await _stateStore.SaveRunAsync(run, ct).ConfigureAwait(false);

        PublishProgress("complete", null,
            $"restored {toFix.Count:N0} mismatches out of {allMismatches.Count:N0} total in {targetRunId}",
            run);
        return run;
    }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;

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
            RepairsPlanned: 0,
            RepairsAttempted: 0,
            RepairsPatchedOnWinner: 0,
            RepairsFailed: 0));
    }
}
