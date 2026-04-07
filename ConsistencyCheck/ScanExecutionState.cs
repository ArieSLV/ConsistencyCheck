namespace ConsistencyCheck;

internal static class ScanExecutionState
{
    public static bool CanResumeSnapshotImport(RunStateDocument run)
        => CanResumeDirectSnapshotImport(run);

    public static bool CanResumeDirectSnapshotImport(RunStateDocument run)
    {
        if (run.IsComplete)
            return false;

        if (run.RunMode != RunMode.ImportSnapshots)
            return false;

        return run.CurrentPhase == ScanExecutionPhase.SnapshotImport &&
               run.SnapshotImportCompleted == false;
    }

    public static bool CanResumeSnapshotCacheDownload(RunStateDocument run)
    {
        if (run.IsComplete)
            return false;

        if (run.RunMode == RunMode.ApplyRepairPlan)
            return false;

        if (run.RunMode == RunMode.DownloadSnapshotsToCache &&
            run.CurrentPhase == ScanExecutionPhase.SnapshotCacheDownload)
            return true;

        return false;
    }

    public static bool CanResumeCachedSnapshotImport(RunStateDocument run)
    {
        if (run.IsComplete)
            return false;

        if (run.RunMode != RunMode.ImportCachedSnapshotsToStateStore)
            return false;

        if (string.IsNullOrWhiteSpace(run.SourceSnapshotCacheRunId))
            return false;

        return run.CurrentPhase == ScanExecutionPhase.SnapshotImport &&
               run.SnapshotImportCompleted == false;
    }

    public static bool CanResumeAnalysis(RunStateDocument run)
    {
        if (run.IsComplete)
            return false;

        if (run.RunMode is RunMode.ImportSnapshots or RunMode.DownloadSnapshotsToCache or RunMode.ImportCachedSnapshotsToStateStore or RunMode.ApplyRepairPlan or RunMode.MismatchDecisionFixup)
            return false;

        // LiveETagScan and SnapshotCrossCheck use SafeRestartEtag as cursor and do not require a SourceSnapshotRunId.
        if (run.RunMode == RunMode.LiveETagScan || run.RunMode == RunMode.SnapshotCrossCheck)
            return true;

        if (string.IsNullOrWhiteSpace(run.SourceSnapshotRunId))
            return false;

        if (run.RunMode == RunMode.ScanOnly &&
            run.ScanMode == CheckMode.FirstMismatch &&
            run.MismatchesFound > 0)
        {
            return false;
        }

        return run.CurrentPhase is ScanExecutionPhase.WaitingForSnapshotIndex or ScanExecutionPhase.CandidateProcessing;
    }

    public static bool IsLegacyInterruptedCombinedImport(RunStateDocument run) => false;
}
