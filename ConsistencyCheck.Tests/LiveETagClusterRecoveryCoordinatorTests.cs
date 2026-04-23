using Xunit;

namespace ConsistencyCheck.Tests;

public sealed class LiveETagClusterRecoveryCoordinatorTests
{
    [Fact]
    public void ResolveNodeWork_NoHistory_StartsFreshScanFromZero()
    {
        var work = LiveETagClusterRecoveryCoordinator.ResolveNodeWork([], []);

        Assert.Equal(LiveETagNodeExecutionAction.StartScan, work.Action);
        Assert.Equal(0L, work.StartEtag);
        Assert.Null(work.ScanRun);
        Assert.Null(work.ApplyRun);
    }

    [Fact]
    public void ResolveNodeWork_IncompleteLatestScan_ResumesScan()
    {
        var scanRun = CreateScanRun(
            runId: "runs/scan-1",
            isComplete: false,
            safeRestartEtag: 128,
            repairsPlanned: 0,
            startedAt: new DateTimeOffset(2026, 4, 9, 10, 0, 0, TimeSpan.Zero));

        var work = LiveETagClusterRecoveryCoordinator.ResolveNodeWork([scanRun], []);

        Assert.Equal(LiveETagNodeExecutionAction.ResumeScan, work.Action);
        Assert.Equal(128L, work.StartEtag);
        Assert.Same(scanRun, work.ScanRun);
        Assert.Null(work.ApplyRun);
    }

    [Fact]
    public void ResolveNodeWork_CompletedScanWithRepairsAndNoApply_StartsApply()
    {
        var scanRun = CreateScanRun(
            runId: "runs/scan-2",
            isComplete: true,
            safeRestartEtag: 256,
            repairsPlanned: 17,
            startedAt: new DateTimeOffset(2026, 4, 9, 11, 0, 0, TimeSpan.Zero));

        var work = LiveETagClusterRecoveryCoordinator.ResolveNodeWork([scanRun], []);

        Assert.Equal(LiveETagNodeExecutionAction.StartApply, work.Action);
        Assert.Equal(256L, work.StartEtag);
        Assert.Same(scanRun, work.ScanRun);
        Assert.Null(work.ApplyRun);
    }

    [Fact]
    public void ResolveNodeWork_IncompleteApplyForLatestScan_ResumesApply()
    {
        var scanRun = CreateScanRun(
            runId: "runs/scan-3",
            isComplete: true,
            safeRestartEtag: 512,
            repairsPlanned: 4,
            startedAt: new DateTimeOffset(2026, 4, 9, 12, 0, 0, TimeSpan.Zero));
        var applyRun = CreateApplyRun(
            runId: "runs/apply-3",
            sourceRepairPlanRunId: scanRun.RunId,
            isComplete: false,
            startedAt: new DateTimeOffset(2026, 4, 9, 12, 5, 0, TimeSpan.Zero));

        var work = LiveETagClusterRecoveryCoordinator.ResolveNodeWork([scanRun], [applyRun]);

        Assert.Equal(LiveETagNodeExecutionAction.ResumeApply, work.Action);
        Assert.Equal(512L, work.StartEtag);
        Assert.Same(scanRun, work.ScanRun);
        Assert.Same(applyRun, work.ApplyRun);
    }

    [Fact]
    public void ResolveNodeWork_CompletedApplyForLatestScan_StartsNextScanFromSavedCursor()
    {
        var scanRun = CreateScanRun(
            runId: "runs/scan-4",
            isComplete: true,
            safeRestartEtag: 1024,
            repairsPlanned: 9,
            startedAt: new DateTimeOffset(2026, 4, 9, 13, 0, 0, TimeSpan.Zero));
        var applyRun = CreateApplyRun(
            runId: "runs/apply-4",
            sourceRepairPlanRunId: scanRun.RunId,
            isComplete: true,
            startedAt: new DateTimeOffset(2026, 4, 9, 13, 10, 0, TimeSpan.Zero));

        var work = LiveETagClusterRecoveryCoordinator.ResolveNodeWork([scanRun], [applyRun]);

        Assert.Equal(LiveETagNodeExecutionAction.StartScan, work.Action);
        Assert.Equal(1024L, work.StartEtag);
        Assert.Same(scanRun, work.ScanRun);
        Assert.Null(work.ApplyRun);
    }

    [Fact]
    public void ResolveNodeWork_PrefersLatestScanOverOlderPendingApply()
    {
        var olderScan = CreateScanRun(
            runId: "runs/scan-old",
            isComplete: true,
            safeRestartEtag: 200,
            repairsPlanned: 3,
            startedAt: new DateTimeOffset(2026, 4, 9, 8, 0, 0, TimeSpan.Zero));
        var olderApply = CreateApplyRun(
            runId: "runs/apply-old",
            sourceRepairPlanRunId: olderScan.RunId,
            isComplete: false,
            startedAt: new DateTimeOffset(2026, 4, 9, 8, 10, 0, TimeSpan.Zero));
        var newerScan = CreateScanRun(
            runId: "runs/scan-new",
            isComplete: true,
            safeRestartEtag: 350,
            repairsPlanned: 7,
            startedAt: new DateTimeOffset(2026, 4, 9, 9, 0, 0, TimeSpan.Zero));

        var work = LiveETagClusterRecoveryCoordinator.ResolveNodeWork([olderScan, newerScan], [olderApply]);

        Assert.Equal(LiveETagNodeExecutionAction.StartApply, work.Action);
        Assert.Equal(350L, work.StartEtag);
        Assert.Same(newerScan, work.ScanRun);
        Assert.Null(work.ApplyRun);
    }

    [Fact]
    public void TryGetLiveETagSourceNodeUrl_InvalidSourceNodeIndex_ReturnsNull()
    {
        var run = CreateScanRun(
            runId: "runs/scan-invalid",
            isComplete: true,
            safeRestartEtag: 10,
            repairsPlanned: 0,
            startedAt: new DateTimeOffset(2026, 4, 9, 14, 0, 0, TimeSpan.Zero),
            sourceNodeIndex: 99);

        var sourceNodeUrl = LiveETagClusterRecoveryCoordinator.TryGetLiveETagSourceNodeUrl(run);

        Assert.Null(sourceNodeUrl);
    }

    [Fact]
    public async Task RunAsync_SinglePass_RunsExactlyOneCycle()
    {
        var config = CreateConfig(0);
        config.LiveETagScanLaunchMode = LiveETagScanLaunchMode.AllNodesAutomatic;
        config.LiveETagClusterExecutionMode = LiveETagClusterExecutionMode.SinglePass;

        var cycleCount = 0;
        var delayCalls = 0;
        var coordinator = new LiveETagClusterRecoveryCoordinator(
            config,
            null!,
            ct =>
            {
                cycleCount++;
                return Task.FromResult(CreateCycleSummary(config.Nodes.Count));
            },
            (delay, ct) =>
            {
                delayCalls++;
                return Task.CompletedTask;
            });

        var summary = await coordinator.RunAsync(CancellationToken.None);

        Assert.Equal(1, cycleCount);
        Assert.Equal(0, delayCalls);
        Assert.True(summary.IsComplete);
        Assert.Equal(config.Nodes.Count, summary.NodesCompleted);
    }

    [Fact]
    public async Task RunAsync_RecurringMode_WaitsAfterCompletedCycleBeforeNextPass()
    {
        var config = CreateConfig(0);
        config.LiveETagScanLaunchMode = LiveETagScanLaunchMode.AllNodesAutomatic;
        config.LiveETagClusterExecutionMode = LiveETagClusterExecutionMode.Recurring;
        config.LiveETagRecurringIntervalMinutes = 15;

        var cycleCount = 0;
        TimeSpan? observedDelay = null;
        var coordinator = new LiveETagClusterRecoveryCoordinator(
            config,
            null!,
            ct =>
            {
                cycleCount++;
                return Task.FromResult(CreateCycleSummary(config.Nodes.Count));
            },
            (delay, ct) =>
            {
                observedDelay = delay;
                throw new OperationCanceledException("stop after the first completed cycle");
            });

        await Assert.ThrowsAsync<OperationCanceledException>(() => coordinator.RunAsync(CancellationToken.None));

        Assert.Equal(1, cycleCount);
        Assert.Equal(TimeSpan.FromMinutes(15), observedDelay);
    }

    private static RunStateDocument CreateScanRun(
        string runId,
        bool isComplete,
        long? safeRestartEtag,
        long repairsPlanned,
        DateTimeOffset startedAt,
        int sourceNodeIndex = 0)
    {
        return new RunStateDocument
        {
            Id = runId,
            RunId = runId,
            RunMode = RunMode.LiveETagScan,
            StartedAt = startedAt,
            LastSavedAt = startedAt.AddMinutes(1),
            IsComplete = isComplete,
            SafeRestartEtag = safeRestartEtag,
            RepairsPlanned = repairsPlanned,
            ConfigSnapshot = CreateConfig(sourceNodeIndex)
        };
    }

    private static RunStateDocument CreateApplyRun(
        string runId,
        string sourceRepairPlanRunId,
        bool isComplete,
        DateTimeOffset startedAt)
    {
        return new RunStateDocument
        {
            Id = runId,
            RunId = runId,
            RunMode = RunMode.ApplyRepairPlan,
            StartedAt = startedAt,
            LastSavedAt = startedAt.AddMinutes(1),
            IsComplete = isComplete,
            SourceRepairPlanRunId = sourceRepairPlanRunId
        };
    }

    private static AppConfig CreateConfig(int sourceNodeIndex)
    {
        return new AppConfig
        {
            DatabaseName = "Orders",
            RunMode = RunMode.LiveETagScan,
            Nodes =
            [
                new NodeConfig { Label = "Node A", Url = "https://node-a" },
                new NodeConfig { Label = "Node B", Url = "https://node-b" }
            ],
            SourceNodeIndex = sourceNodeIndex
        };
    }

    [Fact]
    public async Task RunAsync_RecurringMode_SurvivesTransientException_RetriesAndSucceeds()
    {
        var config = CreateConfig(0);
        config.LiveETagScanLaunchMode = LiveETagScanLaunchMode.AllNodesAutomatic;
        config.LiveETagClusterExecutionMode = LiveETagClusterExecutionMode.Recurring;
        config.LiveETagRecurringIntervalMinutes = 15;

        var cycleCount = 0;
        var observedDelays = new List<TimeSpan>();
        var coordinator = new LiveETagClusterRecoveryCoordinator(
            config,
            null!,
            ct =>
            {
                cycleCount++;
                if (cycleCount == 1)
                    throw new HttpRequestException("node unreachable");
                return Task.FromResult(CreateCycleSummary(config.Nodes.Count));
            },
            (delay, ct) =>
            {
                observedDelays.Add(delay);
                if (observedDelays.Count == 2)
                    throw new OperationCanceledException("stop after success delay");
                return Task.CompletedTask;
            });

        await Assert.ThrowsAsync<OperationCanceledException>(() => coordinator.RunAsync(CancellationToken.None));

        Assert.Equal(2, cycleCount);
        Assert.Equal(2, observedDelays.Count);
        Assert.Equal(TimeSpan.FromSeconds(30), observedDelays[0]);
        Assert.Equal(TimeSpan.FromMinutes(15), observedDelays[1]);
    }

    [Fact]
    public async Task RunAsync_RecurringMode_ConsecutiveFailures_BackoffEscalates()
    {
        var config = CreateConfig(0);
        config.LiveETagScanLaunchMode = LiveETagScanLaunchMode.AllNodesAutomatic;
        config.LiveETagClusterExecutionMode = LiveETagClusterExecutionMode.Recurring;
        config.LiveETagRecurringIntervalMinutes = 15;

        var cycleCount = 0;
        var observedDelays = new List<TimeSpan>();
        var coordinator = new LiveETagClusterRecoveryCoordinator(
            config,
            null!,
            ct =>
            {
                cycleCount++;
                if (cycleCount <= 3)
                    throw new InvalidOperationException($"failure #{cycleCount}");
                return Task.FromResult(CreateCycleSummary(config.Nodes.Count));
            },
            (delay, ct) =>
            {
                observedDelays.Add(delay);
                if (observedDelays.Count == 4)
                    throw new OperationCanceledException("stop after success delay");
                return Task.CompletedTask;
            });

        await Assert.ThrowsAsync<OperationCanceledException>(() => coordinator.RunAsync(CancellationToken.None));

        Assert.Equal(4, cycleCount);
        Assert.Equal(4, observedDelays.Count);
        Assert.Equal(TimeSpan.FromSeconds(30), observedDelays[0]);
        Assert.Equal(TimeSpan.FromSeconds(60), observedDelays[1]);
        Assert.Equal(TimeSpan.FromSeconds(60), observedDelays[2]);
        Assert.Equal(TimeSpan.FromMinutes(15), observedDelays[3]);
    }

    [Fact]
    public async Task RunAsync_RecurringMode_BackoffCapsAtSixtySeconds()
    {
        var config = CreateConfig(0);
        config.LiveETagScanLaunchMode = LiveETagScanLaunchMode.AllNodesAutomatic;
        config.LiveETagClusterExecutionMode = LiveETagClusterExecutionMode.Recurring;
        config.LiveETagRecurringIntervalMinutes = 15;

        var cycleCount = 0;
        var observedDelays = new List<TimeSpan>();
        var coordinator = new LiveETagClusterRecoveryCoordinator(
            config,
            null!,
            ct =>
            {
                cycleCount++;
                throw new IOException($"failure #{cycleCount}");
            },
            (delay, ct) =>
            {
                observedDelays.Add(delay);
                if (observedDelays.Count >= 6)
                    throw new OperationCanceledException("stop after enough samples");
                return Task.CompletedTask;
            });

        await Assert.ThrowsAsync<OperationCanceledException>(() => coordinator.RunAsync(CancellationToken.None));

        Assert.Equal(6, observedDelays.Count);
        Assert.True(observedDelays.All(d => d <= TimeSpan.FromSeconds(60)),
            $"All delays should be <= 60s, but got: {string.Join(", ", observedDelays.Select(d => d.TotalSeconds))}");
    }

    [Fact]
    public async Task RunAsync_RecurringMode_OperationCanceledException_Propagates()
    {
        var config = CreateConfig(0);
        config.LiveETagScanLaunchMode = LiveETagScanLaunchMode.AllNodesAutomatic;
        config.LiveETagClusterExecutionMode = LiveETagClusterExecutionMode.Recurring;
        config.LiveETagRecurringIntervalMinutes = 15;

        var coordinator = new LiveETagClusterRecoveryCoordinator(
            config,
            null!,
            ct => throw new OperationCanceledException("user cancelled"),
            (delay, ct) => Task.CompletedTask);

        await Assert.ThrowsAsync<OperationCanceledException>(() => coordinator.RunAsync(CancellationToken.None));
    }

    private static LiveETagClusterRecoverySummary CreateCycleSummary(int totalNodes)
    {
        return new LiveETagClusterRecoverySummary
        {
            IsComplete = true,
            TotalNodes = totalNodes,
            NodesCompleted = totalNodes,
            CompletedAt = new DateTimeOffset(2026, 4, 9, 15, 0, 0, TimeSpan.Zero)
        };
    }
}