using Xunit;

namespace ConsistencyCheck.Tests;

public sealed class ScanExecutionStateTests
{
    [Fact]
    public void CanResumeDirectSnapshotImport_RunInSnapshotImportPhase_ReturnsTrue()
    {
        var run = new RunStateDocument
        {
            RunMode = RunMode.ImportSnapshots,
            CurrentPhase = ScanExecutionPhase.SnapshotImport,
            SnapshotImportCompleted = false
        };

        Assert.True(ScanExecutionState.CanResumeDirectSnapshotImport(run));
        Assert.True(ScanExecutionState.CanResumeSnapshotImport(run));
    }

    [Fact]
    public void CanResumeSnapshotCacheDownload_NewRunInDownloadPhase_ReturnsTrue()
    {
        var run = new RunStateDocument
        {
            RunMode = RunMode.DownloadSnapshotsToCache,
            CurrentPhase = ScanExecutionPhase.SnapshotCacheDownload
        };

        Assert.True(ScanExecutionState.CanResumeSnapshotCacheDownload(run));
    }

    [Fact]
    public void CanResumeCachedSnapshotImport_RunWithSourceCacheAndSnapshotImportPhase_ReturnsTrue()
    {
        var run = new RunStateDocument
        {
            RunMode = RunMode.ImportCachedSnapshotsToStateStore,
            CurrentPhase = ScanExecutionPhase.SnapshotImport,
            SnapshotImportCompleted = false,
            SourceSnapshotCacheRunId = "runs/20260329-1200000000000"
        };

        Assert.True(ScanExecutionState.CanResumeCachedSnapshotImport(run));
    }

    [Fact]
    public void CanResumeSnapshotImport_CandidateProcessingAlreadyStarted_ReturnsFalse()
    {
        var run = new RunStateDocument
        {
            RunMode = RunMode.ScanAndRepair,
            CurrentPhase = ScanExecutionPhase.CandidateProcessing,
            SnapshotImportCompleted = true,
            CandidateDocumentsFound = 10
        };

        Assert.False(ScanExecutionState.CanResumeDirectSnapshotImport(run));
        Assert.False(ScanExecutionState.CanResumeSnapshotImport(run));
    }

    [Fact]
    public void CanResumeSnapshotCacheDownload_ApplyRepairPlan_ReturnsFalse()
    {
        var run = new RunStateDocument
        {
            RunMode = RunMode.ApplyRepairPlan,
            CurrentPhase = ScanExecutionPhase.SnapshotCacheDownload
        };

        Assert.False(ScanExecutionState.CanResumeSnapshotCacheDownload(run));
    }

    [Fact]
    public void CanResumeAnalysis_WaitingForSnapshotIndexWithSourceSnapshotRun_ReturnsTrue()
    {
        var run = new RunStateDocument
        {
            RunMode = RunMode.DryRunRepair,
            CurrentPhase = ScanExecutionPhase.WaitingForSnapshotIndex,
            SourceSnapshotRunId = "runs/20260329-1200000000000"
        };

        Assert.True(ScanExecutionState.CanResumeAnalysis(run));
    }

    [Fact]
    public void CanResumeAnalysis_CandidateProcessingWithSourceSnapshotRun_ReturnsTrue()
    {
        var run = new RunStateDocument
        {
            RunMode = RunMode.ScanAndRepair,
            CurrentPhase = ScanExecutionPhase.CandidateProcessing,
            SourceSnapshotRunId = "runs/20260329-1200000000000"
        };

        Assert.True(ScanExecutionState.CanResumeAnalysis(run));
    }

    [Fact]
    public void CanResumeAnalysis_FirstMismatchRunThatAlreadyFoundMismatch_ReturnsFalse()
    {
        var run = new RunStateDocument
        {
            RunMode = RunMode.ScanOnly,
            ScanMode = CheckMode.FirstMismatch,
            CurrentPhase = ScanExecutionPhase.CandidateProcessing,
            SourceSnapshotRunId = "runs/20260329-1200000000000",
            MismatchesFound = 1
        };

        Assert.False(ScanExecutionState.CanResumeAnalysis(run));
    }
}
