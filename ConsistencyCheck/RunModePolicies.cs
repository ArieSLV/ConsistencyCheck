namespace ConsistencyCheck;

internal static class RunModePolicies
{
    public static bool RequiresCustomerClusterConnectivity(RunMode runMode)
        => runMode is RunMode.ImportSnapshots
            or RunMode.DownloadSnapshotsToCache
            or RunMode.ScanAndRepair
            or RunMode.ApplyRepairPlan
            or RunMode.LiveETagScan;

    /// <summary>
    /// Whether to validate cluster reachability at startup (before the run is created).
    /// SnapshotCrossCheck connects to the cluster only during the scan itself, not at setup time.
    /// </summary>
    public static bool RequiresStartupConnectivityValidation(RunMode runMode)
        => runMode is RunMode.ImportSnapshots
            or RunMode.DownloadSnapshotsToCache
            or RunMode.ScanAndRepair
            or RunMode.ApplyRepairPlan
            or RunMode.LiveETagScan;

    public static bool RequiresFreshSemanticsFromCluster(RunMode runMode)
        => runMode is RunMode.ImportSnapshots
            or RunMode.DownloadSnapshotsToCache
            or RunMode.ScanAndRepair
            or RunMode.LiveETagScan;

    public static bool RequiresSemanticsSnapshot(RunMode runMode)
        => runMode is RunMode.ImportSnapshots
            or RunMode.DownloadSnapshotsToCache
            or RunMode.ScanOnly
            or RunMode.DryRunRepair
            or RunMode.ScanAndRepair
            or RunMode.LiveETagScan;

    public static bool UsesLocalSnapshotEvaluation(RunMode runMode)
        => runMode is RunMode.ScanOnly or RunMode.DryRunRepair;
}
