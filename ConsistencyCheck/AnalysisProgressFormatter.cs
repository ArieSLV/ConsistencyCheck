namespace ConsistencyCheck;

internal sealed record SnapshotProblemIndexStatus(
    bool IsStale,
    long EntriesCount,
    DateTimeOffset? LastIndexingTime,
    DateTimeOffset? LastQueryingTime);

internal static class AnalysisProgressFormatter
{
    public static string BuildCheckingIndexDetail(
        SnapshotProblemIndexStatus status,
        DateTimeOffset? now = null)
        => $"local candidate index status: {DescribeIndexStatus(status, now)}";

    public static string BuildWaitingForIndexDetail(
        SnapshotProblemIndexStatus status,
        TimeSpan elapsed,
        DateTimeOffset? now = null)
        => $"candidate index is stale; waiting... elapsed {FormatDuration(elapsed)}; {DescribeIndexStatus(status, now)}";

    public static string BuildLoadingCandidatesDetail(
        int pageNumber,
        int start,
        int pageSize,
        int configuredNodeCount,
        TimeSpan elapsed,
        SnapshotProblemIndexStatus status,
        DateTimeOffset? now = null)
        => $"page {pageNumber}: loading candidate ids (skip {start:N0}, take {pageSize:N0}, nodes {configuredNodeCount:N0}); elapsed {FormatDuration(elapsed)}; {DescribeIndexStatus(status, now)}";

    public static string BuildIndexReadyDetail(
        SnapshotProblemIndexStatus status,
        TimeSpan elapsed,
        DateTimeOffset? now = null)
        => $"candidate index ready after {FormatDuration(elapsed)}; {DescribeIndexStatus(status, now)}";

    public static string BuildIndexDiagnosticMessage(
        SnapshotProblemIndexStatus status,
        TimeSpan? elapsed = null,
        DateTimeOffset? now = null)
    {
        var prefix = status.IsStale ? "Candidate index is stale" : "Candidate index is ready";
        return elapsed.HasValue
            ? $"{prefix}; elapsed {FormatDuration(elapsed.Value)}; {DescribeIndexStatus(status, now)}."
            : $"{prefix}; {DescribeIndexStatus(status, now)}.";
    }

    public static string BuildCandidatePageLoadedDetail(
        int pageNumber,
        int candidateCount,
        TimeSpan elapsed)
        => candidateCount == 0
            ? $"page {pageNumber}: no candidate ids returned after {FormatDuration(elapsed)}"
            : $"page {pageNumber}: loaded {candidateCount:N0} candidate ids in {FormatDuration(elapsed)}";

    public static string BuildCandidatePageLoadDiagnosticMessage(
        int pageNumber,
        int start,
        int pageSize,
        int configuredNodeCount,
        int returnedCount,
        TimeSpan elapsed,
        SnapshotProblemIndexStatus status,
        DateTimeOffset? now = null)
        => $"Page {pageNumber}: skip {start:N0}, take {pageSize:N0}, nodes {configuredNodeCount:N0}, returned {returnedCount:N0}, duration {FormatDuration(elapsed)}; {DescribeIndexStatus(status, now)}.";

    public static string DescribeIndexStatus(
        SnapshotProblemIndexStatus status,
        DateTimeOffset? now = null)
    {
        var effectiveNow = now ?? DateTimeOffset.UtcNow;
        var staleText = status.IsStale ? "stale" : "non-stale";
        return $"status {staleText}, entries {status.EntriesCount:N0}, last indexed {FormatAge(status.LastIndexingTime, effectiveNow)}, last queried {FormatAge(status.LastQueryingTime, effectiveNow)}";
    }

    internal static string FormatDuration(TimeSpan duration)
    {
        var safe = duration < TimeSpan.Zero ? TimeSpan.Zero : duration;
        return $"{(int)safe.TotalHours:00}:{safe.Minutes:00}:{safe.Seconds:00}";
    }

    private static string FormatAge(DateTimeOffset? timestamp, DateTimeOffset now)
    {
        if (timestamp.HasValue == false)
            return "never";

        return $"{FormatDuration(now - timestamp.Value)} ago";
    }
}
