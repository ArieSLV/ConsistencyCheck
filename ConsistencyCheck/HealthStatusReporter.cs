namespace ConsistencyCheck;

internal sealed class HealthStatusReporter
{
    private readonly object _locker = new();
    private readonly SchedulerLaunchOptions _options;
    private AppConfig? _config;
    private IndexedRunProgressUpdate? _lastProgress;
    private DateTimeOffset? _lastProgressUtc;
    private string _runtimeStatus;
    private string? _lastError;
    private int? _startupAttempt;
    private bool _fatal;

    public HealthStatusReporter(SchedulerLaunchOptions options)
    {
        _options = options;
        _runtimeStatus = options.IsSchedulerProfile ? "starting" : "wrong-mode";
    }

    public void MarkStarting()
    {
        lock (_locker)
        {
            if (_fatal)
                return;

            _runtimeStatus = _options.IsSchedulerProfile ? "starting" : "wrong-mode";
            _lastError = null;
        }
    }

    public void MarkConfigured(AppConfig config)
    {
        lock (_locker)
        {
            _config = config;
            if (_fatal)
                return;

            _runtimeStatus = SchedulerLaunchProfile.IsExpectedRecurringLiveEtagMode(config, _options)
                ? "starting"
                : "wrong-mode";
            _lastError = null;
        }
    }

    public void MarkCoordinatorStarting(AppConfig config)
    {
        lock (_locker)
        {
            _config = config;
            if (_fatal)
                return;

            _runtimeStatus = SchedulerLaunchProfile.IsExpectedRecurringLiveEtagMode(config, _options)
                ? "starting"
                : "wrong-mode";
            _lastError = null;
        }
    }

    public void MarkStartupRetry(Exception exception, int attempt)
    {
        lock (_locker)
        {
            if (_fatal)
                return;

            _runtimeStatus = "retrying";
            _startupAttempt = attempt;
            _lastError = exception.Message;
        }
    }

    public void RecordProgress(IndexedRunProgressUpdate update, DateTimeOffset? now = null)
    {
        lock (_locker)
        {
            _lastProgress = update;
            _lastProgressUtc = now ?? DateTimeOffset.UtcNow;
            if (_fatal)
                return;

            _runtimeStatus = SchedulerLaunchProfile.IsExpectedRecurringLiveEtagMode(_config, _options)
                ? "ok"
                : "wrong-mode";
            _lastError = null;
        }
    }

    public void MarkFatal(Exception exception)
    {
        lock (_locker)
        {
            _fatal = true;
            _runtimeStatus = "fatal";
            _lastError = exception.Message;
        }
    }

    public HealthCheckResponse BuildResponse(DateTimeOffset now)
    {
        lock (_locker)
        {
            var expectedMode = SchedulerLaunchProfile.IsExpectedRecurringLiveEtagMode(_config, _options);
            var secondsSinceProgress = _lastProgressUtc.HasValue
                ? Math.Max(0, (now - _lastProgressUtc.Value).TotalSeconds)
                : (double?)null;
            var hasFreshProgress = secondsSinceProgress.HasValue &&
                                   secondsSinceProgress.Value <= _options.HealthStaleThreshold.TotalSeconds;

            var status = ResolveStatus(expectedMode, hasFreshProgress);
            var ready = status == "ok";

            return new HealthCheckResponse
            {
                Status = status,
                Ready = ready,
                Profile = _options.Profile,
                Mode = _config?.RunMode.ToString(),
                LaunchMode = _config?.LiveETagScanLaunchMode.ToString(),
                ExecutionMode = _config?.LiveETagClusterExecutionMode.ToString(),
                IntervalMinutes = _config?.LiveETagRecurringIntervalMinutes ?? _options.IntervalMinutes,
                Phase = _lastProgress?.Phase,
                CurrentNodeLabel = _lastProgress?.CurrentNodeLabel,
                LastProgressUtc = _lastProgressUtc,
                SecondsSinceProgress = secondsSinceProgress,
                StartupAttempt = _startupAttempt,
                LastError = _lastError,
                DocumentsInspected = _lastProgress?.DocumentsInspected ?? 0,
                MismatchesFound = _lastProgress?.MismatchesFound ?? 0,
                RepairsPlanned = _lastProgress?.RepairsPlanned ?? 0,
                RepairsAttempted = _lastProgress?.RepairsAttempted ?? 0,
                RepairsPatchedOnWinner = _lastProgress?.RepairsPatchedOnWinner ?? 0,
                RepairsFailed = _lastProgress?.RepairsFailed ?? 0
            };
        }
    }

    private string ResolveStatus(bool expectedMode, bool hasFreshProgress)
    {
        if (_fatal)
            return "fatal";

        if (!_options.IsSchedulerProfile)
            return "wrong-mode";

        if (_config == null)
            return _runtimeStatus == "retrying" ? "retrying" : "starting";

        if (!expectedMode)
            return "wrong-mode";

        if (_lastProgress == null)
            return _runtimeStatus == "retrying" ? "retrying" : "starting";

        return hasFreshProgress ? "ok" : "stale";
    }
}

internal sealed class HealthCheckResponse
{
    public string Status { get; init; } = "starting";
    public bool Ready { get; init; }
    public string? Profile { get; init; }
    public string? Mode { get; init; }
    public string? LaunchMode { get; init; }
    public string? ExecutionMode { get; init; }
    public int? IntervalMinutes { get; init; }
    public string? Phase { get; init; }
    public string? CurrentNodeLabel { get; init; }
    public DateTimeOffset? LastProgressUtc { get; init; }
    public double? SecondsSinceProgress { get; init; }
    public int? StartupAttempt { get; init; }
    public string? LastError { get; init; }
    public long DocumentsInspected { get; init; }
    public long MismatchesFound { get; init; }
    public long RepairsPlanned { get; init; }
    public long RepairsAttempted { get; init; }
    public long RepairsPatchedOnWinner { get; init; }
    public long RepairsFailed { get; init; }
}
