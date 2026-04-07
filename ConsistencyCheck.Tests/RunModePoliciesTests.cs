using Xunit;

namespace ConsistencyCheck.Tests;

public sealed class RunModePoliciesTests
{
    [Theory]
    [InlineData(RunMode.ImportSnapshots, true)]
    [InlineData(RunMode.DownloadSnapshotsToCache, true)]
    [InlineData(RunMode.ImportCachedSnapshotsToStateStore, false)]
    [InlineData(RunMode.ScanOnly, false)]
    [InlineData(RunMode.DryRunRepair, false)]
    [InlineData(RunMode.ScanAndRepair, true)]
    [InlineData(RunMode.ApplyRepairPlan, true)]
    public void RequiresCustomerClusterConnectivity_ReturnsExpectedValue(RunMode runMode, bool expected)
    {
        Assert.Equal(expected, RunModePolicies.RequiresCustomerClusterConnectivity(runMode));
    }

    [Theory]
    [InlineData(RunMode.ImportSnapshots, true)]
    [InlineData(RunMode.DownloadSnapshotsToCache, true)]
    [InlineData(RunMode.ImportCachedSnapshotsToStateStore, false)]
    [InlineData(RunMode.ScanOnly, false)]
    [InlineData(RunMode.DryRunRepair, false)]
    [InlineData(RunMode.ScanAndRepair, true)]
    [InlineData(RunMode.ApplyRepairPlan, false)]
    public void RequiresFreshSemanticsFromCluster_ReturnsExpectedValue(RunMode runMode, bool expected)
    {
        Assert.Equal(expected, RunModePolicies.RequiresFreshSemanticsFromCluster(runMode));
    }

    [Theory]
    [InlineData(RunMode.ImportSnapshots, true)]
    [InlineData(RunMode.DownloadSnapshotsToCache, true)]
    [InlineData(RunMode.ImportCachedSnapshotsToStateStore, false)]
    [InlineData(RunMode.ScanOnly, true)]
    [InlineData(RunMode.DryRunRepair, true)]
    [InlineData(RunMode.ScanAndRepair, true)]
    [InlineData(RunMode.ApplyRepairPlan, false)]
    public void RequiresSemanticsSnapshot_ReturnsExpectedValue(RunMode runMode, bool expected)
    {
        Assert.Equal(expected, RunModePolicies.RequiresSemanticsSnapshot(runMode));
    }

    [Theory]
    [InlineData(RunMode.ImportSnapshots, false)]
    [InlineData(RunMode.DownloadSnapshotsToCache, false)]
    [InlineData(RunMode.ImportCachedSnapshotsToStateStore, false)]
    [InlineData(RunMode.ScanOnly, true)]
    [InlineData(RunMode.DryRunRepair, true)]
    [InlineData(RunMode.ScanAndRepair, false)]
    [InlineData(RunMode.ApplyRepairPlan, false)]
    public void UsesLocalSnapshotEvaluation_ReturnsExpectedValue(RunMode runMode, bool expected)
    {
        Assert.Equal(expected, RunModePolicies.UsesLocalSnapshotEvaluation(runMode));
    }
}
