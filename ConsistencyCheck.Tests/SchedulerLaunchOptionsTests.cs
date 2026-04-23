using Xunit;

namespace ConsistencyCheck.Tests;

public sealed class SchedulerLaunchOptionsTests
{
    [Fact]
    public void Parse_NoArguments_UsesInteractiveMode()
    {
        var options = SchedulerLaunchOptions.Parse([]);

        Assert.False(options.IsSchedulerProfile);
        Assert.Null(options.Profile);
    }

    [Fact]
    public void Parse_LiveEtagSchedulerProfile_ParsesAllOptions()
    {
        var options = SchedulerLaunchOptions.Parse(
        [
            "--profile", "live-etag-all-nodes-recurring",
            "--interval-minutes", "3",
            "--health-bind", "http://0.0.0.0:9000",
            "--health-path", "/health-check",
            "--startup-retry-delay-seconds", "1",
            "--startup-attempt-timeout-seconds", "2",
            "--health-stale-threshold-seconds", "30"
        ]);

        Assert.True(options.IsSchedulerProfile);
        Assert.Equal(3, options.IntervalMinutes);
        Assert.Equal("http://0.0.0.0:9000", options.HealthBindUrl);
        Assert.Equal("/health-check", options.HealthPath);
        Assert.Equal(TimeSpan.FromSeconds(1), options.StartupRetryDelay);
        Assert.Equal(TimeSpan.FromSeconds(2), options.StartupAttemptTimeout);
        Assert.Equal(TimeSpan.FromSeconds(30), options.HealthStaleThreshold);
    }

    [Fact]
    public void Parse_ValidEqualsSyntax_ParsesSchedulerProfile()
    {
        var options = SchedulerLaunchOptions.Parse(
        [
            "--profile=live-etag-all-nodes-recurring",
            "--interval-minutes=3"
        ]);

        Assert.True(options.IsSchedulerProfile);
        Assert.Equal(3, options.IntervalMinutes);
    }

    [Theory]
    [InlineData("--profile", "unknown", "--interval-minutes", "3")]
    [InlineData("--profile", "live-etag-all-nodes-recurring")]
    [InlineData("--profile", "live-etag-all-nodes-recurring", "--interval-minutes", "0")]
    [InlineData("--profile", "live-etag-all-nodes-recurring", "--interval-minutes", "3", "--health-bind", "https://0.0.0.0:9000")]
    [InlineData("--profile", "live-etag-all-nodes-recurring", "--interval-minutes", "3", "--health-bind", "http://0.0.0.0:9000/health")]
    [InlineData("--profile", "live-etag-all-nodes-recurring", "--interval-minutes", "3", "--health-path", "health-check")]
    public void Parse_InvalidArguments_Throws(params string[] args)
    {
        Assert.Throws<InvalidOperationException>(() => SchedulerLaunchOptions.Parse(args));
    }

    [Fact]
    public void ApplyToConfig_ForSchedulerProfile_ForcesRecurringAllNodeLiveEtagMode()
    {
        var config = CreateConfig();
        var options = SchedulerLaunchOptions.Parse(
        [
            "--profile", "live-etag-all-nodes-recurring",
            "--interval-minutes", "3"
        ]);

        SchedulerLaunchProfile.ApplyToConfig(config, options);

        Assert.Equal(RunMode.LiveETagScan, config.RunMode);
        Assert.Equal(LiveETagScanLaunchMode.AllNodesAutomatic, config.LiveETagScanLaunchMode);
        Assert.Equal(LiveETagClusterExecutionMode.Recurring, config.LiveETagClusterExecutionMode);
        Assert.Equal(3, config.LiveETagRecurringIntervalMinutes);
        Assert.Null(config.StartEtag);
        Assert.Equal(0, config.SourceNodeIndex);
        Assert.True(SchedulerLaunchProfile.IsExpectedRecurringLiveEtagMode(config, options));
    }

    private static AppConfig CreateConfig()
        => new()
        {
            DatabaseName = "Orders",
            RunMode = RunMode.ScanOnly,
            SourceNodeIndex = 1,
            StartEtag = 128,
            CertificatePath = "",
            CertificatePassword = "",
            Nodes =
            [
                new NodeConfig { Label = "A", Url = "http://127.0.0.1:8080" },
                new NodeConfig { Label = "B", Url = "http://127.0.0.1:8081" }
            ]
        };
}
