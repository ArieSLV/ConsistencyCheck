using Xunit;

namespace ConsistencyCheck.Tests;

public sealed class HealthStatusReporterTests
{
    [Fact]
    public void BuildResponse_ConfiguredButNoCoordinatorProgress_IsStarting()
    {
        var now = new DateTimeOffset(2026, 4, 23, 10, 0, 0, TimeSpan.Zero);
        var options = SchedulerLaunchOptions.CreateForTests();
        var reporter = new HealthStatusReporter(options);
        var config = CreateSchedulerConfig(options);

        reporter.MarkConfigured(config);

        var response = reporter.BuildResponse(now);

        Assert.Equal("starting", response.Status);
        Assert.False(response.Ready);
        Assert.Equal("LiveETagScan", response.Mode);
        Assert.Equal("AllNodesAutomatic", response.LaunchMode);
        Assert.Equal("Recurring", response.ExecutionMode);
        Assert.Equal(3, response.IntervalMinutes);
    }

    [Fact]
    public void BuildResponse_FreshRecurringCoordinatorProgress_IsOk()
    {
        var now = new DateTimeOffset(2026, 4, 23, 10, 0, 0, TimeSpan.Zero);
        var options = SchedulerLaunchOptions.CreateForTests();
        var reporter = new HealthStatusReporter(options);
        var config = CreateSchedulerConfig(options);

        reporter.MarkCoordinatorStarting(config);
        reporter.RecordProgress(CreateProgressUpdate("starting live etag recovery"), now);

        var response = reporter.BuildResponse(now.AddSeconds(5));

        Assert.Equal("ok", response.Status);
        Assert.True(response.Ready);
        Assert.Equal("starting live etag recovery", response.Phase);
        Assert.Equal(5, response.SecondsSinceProgress);
        Assert.Equal(10, response.DocumentsInspected);
        Assert.Equal(2, response.RepairsPlanned);
    }

    [Fact]
    public void BuildResponse_WrongMode_IsWrongMode()
    {
        var now = new DateTimeOffset(2026, 4, 23, 10, 0, 0, TimeSpan.Zero);
        var options = SchedulerLaunchOptions.CreateForTests();
        var reporter = new HealthStatusReporter(options);
        var config = CreateSchedulerConfig(options);
        config.RunMode = RunMode.ScanOnly;

        reporter.MarkConfigured(config);
        reporter.RecordProgress(CreateProgressUpdate("running"), now);

        var response = reporter.BuildResponse(now);

        Assert.Equal("wrong-mode", response.Status);
        Assert.False(response.Ready);
    }

    [Fact]
    public void BuildResponse_StaleProgress_IsStale()
    {
        var now = new DateTimeOffset(2026, 4, 23, 10, 0, 0, TimeSpan.Zero);
        var options = SchedulerLaunchOptions.CreateForTests(healthStaleThreshold: TimeSpan.FromSeconds(600));
        var reporter = new HealthStatusReporter(options);
        var config = CreateSchedulerConfig(options);

        reporter.MarkCoordinatorStarting(config);
        reporter.RecordProgress(CreateProgressUpdate("running"), now);

        var response = reporter.BuildResponse(now.AddSeconds(601));

        Assert.Equal("stale", response.Status);
        Assert.False(response.Ready);
    }

    [Fact]
    public void BuildResponse_FatalError_IsFatal()
    {
        var now = new DateTimeOffset(2026, 4, 23, 10, 0, 0, TimeSpan.Zero);
        var options = SchedulerLaunchOptions.CreateForTests();
        var reporter = new HealthStatusReporter(options);
        var config = CreateSchedulerConfig(options);

        reporter.MarkCoordinatorStarting(config);
        reporter.RecordProgress(CreateProgressUpdate("running"), now);
        reporter.MarkFatal(new InvalidOperationException("bad config"));

        var response = reporter.BuildResponse(now);

        Assert.Equal("fatal", response.Status);
        Assert.False(response.Ready);
        Assert.Equal("bad config", response.LastError);
    }

    private static AppConfig CreateSchedulerConfig(SchedulerLaunchOptions options)
    {
        var config = new AppConfig
        {
            DatabaseName = "Orders",
            CertificatePath = "",
            CertificatePassword = "",
            Nodes =
            [
                new NodeConfig { Label = "A", Url = "http://127.0.0.1:8080" },
                new NodeConfig { Label = "B", Url = "http://127.0.0.1:8081" }
            ]
        };
        SchedulerLaunchProfile.ApplyToConfig(config, options);
        return config;
    }

    private static IndexedRunProgressUpdate CreateProgressUpdate(string phase)
        => new(
            Phase: phase,
            CurrentNodeLabel: "A",
            Detail: "detail",
            CurrentNodeStreamedSnapshots: null,
            CurrentNodeTotalDocuments: null,
            SnapshotDocumentsImported: 0,
            SnapshotDocumentsSkipped: 0,
            SnapshotBulkInsertRestarts: 0,
            CandidateDocumentsFound: 1,
            CandidateDocumentsProcessed: 1,
            CandidateDocumentsExcludedBySkippedSnapshots: 0,
            DocumentsInspected: 10,
            UniqueVersionsCompared: 10,
            MismatchesFound: 1,
            RepairsPlanned: 2,
            RepairsAttempted: 3,
            RepairsPatchedOnWinner: 4,
            RepairsFailed: 5);
}
