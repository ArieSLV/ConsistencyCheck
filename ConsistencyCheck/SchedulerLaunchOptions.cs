using System.Globalization;

namespace ConsistencyCheck;

internal sealed class SchedulerLaunchOptions
{
    public const string LiveEtagAllNodesRecurringProfile = "live-etag-all-nodes-recurring";

    private static readonly Uri DefaultHealthBind = new("http://127.0.0.1:9000");

    private SchedulerLaunchOptions(
        string? profile,
        int? intervalMinutes,
        Uri healthBind,
        string healthPath,
        TimeSpan startupRetryDelay,
        TimeSpan startupAttemptTimeout,
        TimeSpan healthStaleThreshold)
    {
        Profile = profile;
        IntervalMinutes = intervalMinutes;
        HealthBind = healthBind;
        HealthPath = healthPath;
        StartupRetryDelay = startupRetryDelay;
        StartupAttemptTimeout = startupAttemptTimeout;
        HealthStaleThreshold = healthStaleThreshold;
    }

    public string? Profile { get; }

    public int? IntervalMinutes { get; }

    public Uri HealthBind { get; }

    public string HealthPath { get; }

    public TimeSpan StartupRetryDelay { get; }

    public TimeSpan StartupAttemptTimeout { get; }

    public TimeSpan HealthStaleThreshold { get; }

    public bool IsSchedulerProfile =>
        string.Equals(Profile, LiveEtagAllNodesRecurringProfile, StringComparison.OrdinalIgnoreCase);

    public string HealthBindUrl => HealthBind.GetLeftPart(UriPartial.Authority);

    public static SchedulerLaunchOptions Interactive { get; } = new(
        profile: null,
        intervalMinutes: null,
        healthBind: DefaultHealthBind,
        healthPath: "/health-check",
        startupRetryDelay: TimeSpan.FromSeconds(10),
        startupAttemptTimeout: TimeSpan.FromSeconds(30),
        healthStaleThreshold: TimeSpan.FromSeconds(600));

    public static SchedulerLaunchOptions CreateForTests(
        string? profile = LiveEtagAllNodesRecurringProfile,
        int? intervalMinutes = 3,
        Uri? healthBind = null,
        string healthPath = "/health-check",
        TimeSpan? startupRetryDelay = null,
        TimeSpan? startupAttemptTimeout = null,
        TimeSpan? healthStaleThreshold = null)
        => new(
            profile,
            intervalMinutes,
            healthBind ?? DefaultHealthBind,
            healthPath,
            startupRetryDelay ?? TimeSpan.FromSeconds(10),
            startupAttemptTimeout ?? TimeSpan.FromSeconds(30),
            healthStaleThreshold ?? TimeSpan.FromSeconds(600));

    public static SchedulerLaunchOptions Parse(IReadOnlyList<string> args)
    {
        if (args.Count == 0)
            return Interactive;

        string? profile = null;
        int? intervalMinutes = null;
        var healthBind = DefaultHealthBind;
        var healthPath = "/health-check";
        var startupRetryDelay = TimeSpan.FromSeconds(10);
        var startupAttemptTimeout = TimeSpan.FromSeconds(30);
        var healthStaleThreshold = TimeSpan.FromSeconds(600);

        for (var i = 0; i < args.Count; i++)
        {
            var raw = args[i];
            if (string.IsNullOrWhiteSpace(raw))
                continue;

            if (!raw.StartsWith("--", StringComparison.Ordinal))
                throw new InvalidOperationException($"Unexpected argument '{raw}'. Options must start with '--'.");

            var key = raw[2..];
            string value;
            var equalsIndex = key.IndexOf('=', StringComparison.Ordinal);
            if (equalsIndex >= 0)
            {
                value = key[(equalsIndex + 1)..];
                key = key[..equalsIndex];
            }
            else
            {
                if (i + 1 >= args.Count || args[i + 1].StartsWith("--", StringComparison.Ordinal))
                    throw new InvalidOperationException($"Missing value for '--{key}'.");

                value = args[++i];
            }

            switch (key)
            {
                case "profile":
                    profile = RequireNonEmpty(key, value);
                    break;
                case "interval-minutes":
                    intervalMinutes = ParsePositiveInt(key, value);
                    break;
                case "health-bind":
                    healthBind = ParseHealthBind(value);
                    break;
                case "health-path":
                    healthPath = ParseHealthPath(value);
                    break;
                case "startup-retry-delay-seconds":
                    startupRetryDelay = TimeSpan.FromSeconds(ParsePositiveInt(key, value));
                    break;
                case "startup-attempt-timeout-seconds":
                    startupAttemptTimeout = TimeSpan.FromSeconds(ParsePositiveInt(key, value));
                    break;
                case "health-stale-threshold-seconds":
                    healthStaleThreshold = TimeSpan.FromSeconds(ParsePositiveInt(key, value));
                    break;
                default:
                    throw new InvalidOperationException($"Unknown option '--{key}'.");
            }
        }

        if (string.IsNullOrWhiteSpace(profile))
            throw new InvalidOperationException("Scheduler options require '--profile live-etag-all-nodes-recurring'.");

        if (!string.Equals(profile, LiveEtagAllNodesRecurringProfile, StringComparison.OrdinalIgnoreCase))
            throw new InvalidOperationException(
                $"Unsupported profile '{profile}'. Supported profile: '{LiveEtagAllNodesRecurringProfile}'.");

        if (intervalMinutes is null)
            throw new InvalidOperationException("'--interval-minutes' is required for the scheduler profile.");

        return new SchedulerLaunchOptions(
            profile,
            intervalMinutes,
            healthBind,
            healthPath,
            startupRetryDelay,
            startupAttemptTimeout,
            healthStaleThreshold);
    }

    private static string RequireNonEmpty(string key, string value)
        => string.IsNullOrWhiteSpace(value)
            ? throw new InvalidOperationException($"'--{key}' must not be empty.")
            : value.Trim();

    private static int ParsePositiveInt(string key, string value)
    {
        if (!int.TryParse(value, NumberStyles.None, CultureInfo.InvariantCulture, out var parsed) || parsed <= 0)
            throw new InvalidOperationException($"'--{key}' must be a positive integer.");

        return parsed;
    }

    private static Uri ParseHealthBind(string value)
    {
        value = RequireNonEmpty("health-bind", value);
        if (!Uri.TryCreate(value, UriKind.Absolute, out var uri) ||
            !string.Equals(uri.Scheme, Uri.UriSchemeHttp, StringComparison.OrdinalIgnoreCase))
        {
            throw new InvalidOperationException("'--health-bind' must be an absolute http URL, for example http://0.0.0.0:9000.");
        }

        if (!string.IsNullOrEmpty(uri.AbsolutePath) && uri.AbsolutePath != "/" ||
            !string.IsNullOrEmpty(uri.Query) ||
            !string.IsNullOrEmpty(uri.Fragment))
        {
            throw new InvalidOperationException("'--health-bind' must not include a path, query string, or fragment.");
        }

        return uri;
    }

    private static string ParseHealthPath(string value)
    {
        value = RequireNonEmpty("health-path", value);
        if (!value.StartsWith("/", StringComparison.Ordinal) ||
            value.Contains('?', StringComparison.Ordinal) ||
            value.Contains('#', StringComparison.Ordinal))
        {
            throw new InvalidOperationException("'--health-path' must be an absolute path, for example /health-check.");
        }

        return value;
    }
}
