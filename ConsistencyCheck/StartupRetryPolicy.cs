using System.Net.Http;
using System.Net.Sockets;
using System.Security.Cryptography;
using Raven.Client.Exceptions.Security;

namespace ConsistencyCheck;

internal static class StartupRetryPolicy
{
    public static async Task<T> ExecuteAsync<T>(
        Func<CancellationToken, Task<T>> operation,
        SchedulerLaunchOptions options,
        HealthStatusReporter healthStatus,
        Action<StartupRetryAttempt>? onRetry,
        CancellationToken ct)
    {
        var attempt = 0;

        while (true)
        {
            ct.ThrowIfCancellationRequested();
            attempt++;

            using var attemptCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
            attemptCts.CancelAfter(options.StartupAttemptTimeout);

            try
            {
                return await operation(attemptCts.Token).ConfigureAwait(false);
            }
            catch (Exception ex) when (!ct.IsCancellationRequested && IsTransient(ex))
            {
                healthStatus.MarkStartupRetry(ex, attempt);
                onRetry?.Invoke(new StartupRetryAttempt(attempt, ex, options.StartupRetryDelay));
                await Task.Delay(options.StartupRetryDelay, ct).ConfigureAwait(false);
            }
        }
    }

    internal static bool IsTransient(Exception exception)
    {
        var sawTransient = false;

        foreach (var current in EnumerateExceptionTree(exception))
        {
            if (IsPermanentStartupFailure(current))
                return false;

            if (IsKnownTransient(current) || HasTransientMessage(current.Message))
                sawTransient = true;
        }

        return sawTransient;
    }

    private static IEnumerable<Exception> EnumerateExceptionTree(Exception exception)
    {
        if (exception is AggregateException aggregate)
        {
            foreach (var inner in aggregate.Flatten().InnerExceptions)
            {
                foreach (var nested in EnumerateExceptionTree(inner))
                    yield return nested;
            }

            yield break;
        }

        for (var current = exception; current != null; current = current.InnerException!)
            yield return current;
    }

    private static bool IsPermanentStartupFailure(Exception exception)
        => exception is System.Security.Authentication.AuthenticationException
           || exception is CertificateNameMismatchException
           || exception is CryptographicException
           || exception is UnauthorizedAccessException
           || exception.GetType().Namespace?.StartsWith("Raven.Client.Exceptions.Security", StringComparison.Ordinal) == true;

    private static bool IsKnownTransient(Exception exception)
        => exception is HttpRequestException
           || exception is TimeoutException
           || exception is OperationCanceledException
           || exception is SocketException;

    private static bool HasTransientMessage(string? message)
    {
        if (string.IsNullOrWhiteSpace(message))
            return false;

        return message.Contains("actively refused", StringComparison.OrdinalIgnoreCase)
               || message.Contains("connection refused", StringComparison.OrdinalIgnoreCase)
               || message.Contains("No connection could be made", StringComparison.OrdinalIgnoreCase)
               || message.Contains("timed out", StringComparison.OrdinalIgnoreCase);
    }
}

internal sealed record StartupRetryAttempt(int Attempt, Exception Exception, TimeSpan Delay);
