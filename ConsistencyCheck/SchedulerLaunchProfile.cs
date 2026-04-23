namespace ConsistencyCheck;

internal static class SchedulerLaunchProfile
{
    public static void ApplyToConfig(AppConfig config, SchedulerLaunchOptions options)
    {
        if (!options.IsSchedulerProfile)
            return;

        if (options.IntervalMinutes is not > 0)
            throw new InvalidOperationException("Scheduler Live ETag profile requires a positive interval.");

        config.RunMode = RunMode.LiveETagScan;
        config.LiveETagScanLaunchMode = LiveETagScanLaunchMode.AllNodesAutomatic;
        config.LiveETagClusterExecutionMode = LiveETagClusterExecutionMode.Recurring;
        config.LiveETagRecurringIntervalMinutes = options.IntervalMinutes.Value;
        config.StartEtag = null;
        config.SourceNodeIndex = 0;
    }

    public static bool IsExpectedRecurringLiveEtagMode(AppConfig? config, SchedulerLaunchOptions options)
    {
        if (!options.IsSchedulerProfile || config == null)
            return false;

        return config.RunMode == RunMode.LiveETagScan &&
               config.LiveETagScanLaunchMode == LiveETagScanLaunchMode.AllNodesAutomatic &&
               config.LiveETagClusterExecutionMode == LiveETagClusterExecutionMode.Recurring &&
               config.LiveETagRecurringIntervalMinutes == options.IntervalMinutes;
    }
}
