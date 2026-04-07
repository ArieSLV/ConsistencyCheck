namespace ConsistencyCheck;

internal sealed class RepairPlanRunInfo
{
    public string RunId { get; set; } = string.Empty;
    public string CustomerDatabaseName { get; set; } = string.Empty;
    public DateTimeOffset StartedAt { get; set; }
    public DateTimeOffset? CompletedAt { get; set; }
    public long? MismatchesFound { get; set; }
    public long? RepairsPlanned { get; set; }
}

internal sealed class ApplyRepairRunInfo
{
    public string RunId { get; set; } = string.Empty;
    public string SourceRepairPlanRunId { get; set; } = string.Empty;
    public DateTimeOffset StartedAt { get; set; }
    public DateTimeOffset? LastSavedAt { get; set; }
    public long? RepairsPlanned { get; set; }
    public long? RepairsAttempted { get; set; }
    public long? RepairsPatchedOnWinner { get; set; }
    public long? RepairsFailed { get; set; }
}

internal sealed class ScanResumeRunInfo
{
    public string RunId { get; set; } = string.Empty;
    public RunMode RunMode { get; set; }
    public DateTimeOffset StartedAt { get; set; }
    public DateTimeOffset? LastSavedAt { get; set; }
    public long SnapshotDocumentsImported { get; set; }
    public ScanExecutionPhase CurrentPhase { get; set; }
    public bool SnapshotImportCompleted { get; set; }
}

internal sealed class SnapshotCacheDownloadRunInfo
{
    public string RunId { get; set; } = string.Empty;
    public RunMode RunMode { get; set; }
    public DateTimeOffset StartedAt { get; set; }
    public DateTimeOffset? LastSavedAt { get; set; }
    public ScanExecutionPhase CurrentPhase { get; set; }
    public long DownloadedRows { get; set; }
    public int CompletedNodes { get; set; }
}

internal sealed class CachedSnapshotImportRunInfo
{
    public string RunId { get; set; } = string.Empty;
    public RunMode RunMode { get; set; }
    public string SourceSnapshotCacheRunId { get; set; } = string.Empty;
    public DateTimeOffset StartedAt { get; set; }
    public DateTimeOffset? LastSavedAt { get; set; }
    public ScanExecutionPhase CurrentPhase { get; set; }
    public long SnapshotDocumentsImported { get; set; }
    public long SnapshotDocumentsSkipped { get; set; }
    public long SnapshotBulkInsertRestarts { get; set; }
}

internal sealed class AnalysisRunInfo
{
    public string RunId { get; set; } = string.Empty;
    public RunMode RunMode { get; set; }
    public string? SourceSnapshotCacheRunId { get; set; }
    public string SourceSnapshotRunId { get; set; } = string.Empty;
    public DateTimeOffset StartedAt { get; set; }
    public DateTimeOffset? LastSavedAt { get; set; }
    public ScanExecutionPhase CurrentPhase { get; set; }
    public long CandidateDocumentsFound { get; set; }
    public long CandidateDocumentsProcessed { get; set; }
    public long MismatchesFound { get; set; }
    public long RepairsPlanned { get; set; }
    public long RepairsPatchedOnWinner { get; set; }
    public long RepairsFailed { get; set; }
}
