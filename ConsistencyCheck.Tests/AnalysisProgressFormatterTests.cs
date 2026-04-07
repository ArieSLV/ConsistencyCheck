using Xunit;

namespace ConsistencyCheck.Tests;

public sealed class AnalysisProgressFormatterTests
{
    [Fact]
    public void BuildWaitingForIndexDetail_IncludesElapsedAndIndexStatus()
    {
        var now = DateTimeOffset.Parse("2026-03-31T12:00:00Z");
        var status = new SnapshotProblemIndexStatus(
            IsStale: true,
            EntriesCount: 1_234_567,
            LastIndexingTime: now.AddMinutes(-2),
            LastQueryingTime: now.AddSeconds(-15));

        var detail = AnalysisProgressFormatter.BuildWaitingForIndexDetail(
            status,
            TimeSpan.FromSeconds(12),
            now);

        Assert.Contains("candidate index is stale; waiting", detail);
        Assert.Contains("elapsed 00:00:12", detail);
        Assert.Contains("status stale", detail);
        Assert.Contains("entries 1,234,567", detail);
        Assert.Contains("last indexed 00:02:00 ago", detail);
        Assert.Contains("last queried 00:00:15 ago", detail);
    }

    [Fact]
    public void BuildLoadingCandidatesDetail_IncludesPageShapeAndElapsed()
    {
        var now = DateTimeOffset.Parse("2026-03-31T12:00:00Z");
        var status = new SnapshotProblemIndexStatus(
            IsStale: false,
            EntriesCount: 9_876,
            LastIndexingTime: now.AddSeconds(-5),
            LastQueryingTime: null);

        var detail = AnalysisProgressFormatter.BuildLoadingCandidatesDetail(
            pageNumber: 1,
            start: 0,
            pageSize: 8192,
            configuredNodeCount: 3,
            elapsed: TimeSpan.FromSeconds(3),
            status: status,
            now: now);

        Assert.Contains("page 1: loading candidate ids", detail);
        Assert.Contains("skip 0", detail);
        Assert.Contains("take 8,192", detail);
        Assert.Contains("nodes 3", detail);
        Assert.Contains("elapsed 00:00:03", detail);
        Assert.Contains("status non-stale", detail);
        Assert.Contains("entries 9,876", detail);
        Assert.Contains("last indexed 00:00:05 ago", detail);
        Assert.Contains("last queried never", detail);
    }
}
