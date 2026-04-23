namespace ConsistencyCheck;

internal static class FailoverLog
{
    private static readonly string LogPath = Path.Combine(
        AppContext.BaseDirectory, "output", "failover.log");

    internal static void Append(string message)
    {
        try
        {
            var line = $"[{DateTimeOffset.UtcNow:yyyy-MM-dd HH:mm:ss.fff} UTC] {message}{Environment.NewLine}";
            File.AppendAllText(LogPath, line);
        }
        catch
        {
            // logging must never crash the process
        }
    }
}
