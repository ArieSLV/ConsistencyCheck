using System.Net.Http;
using System.Net.Sockets;
using Xunit;

namespace ConsistencyCheck.Tests;

public sealed class StartupRetryPolicyTests
{
    [Fact]
    public async Task ExecuteAsync_TransientFailure_RetriesAndUpdatesHealth()
    {
        var options = SchedulerLaunchOptions.CreateForTests(
            startupRetryDelay: TimeSpan.FromMilliseconds(1),
            startupAttemptTimeout: TimeSpan.FromSeconds(1));
        var reporter = new HealthStatusReporter(options);
        reporter.MarkConfigured(CreateSchedulerConfig(options));
        var attempts = 0;
        var retryStatuses = new List<string>();

        var result = await StartupRetryPolicy.ExecuteAsync(
            _ =>
            {
                attempts++;
                if (attempts == 1)
                {
                    throw new HttpRequestException(
                        "No connection could be made because the target machine actively refused it.",
                        new SocketException((int)SocketError.ConnectionRefused));
                }

                return Task.FromResult("ready");
            },
            options,
            reporter,
            _ => retryStatuses.Add(reporter.BuildResponse(DateTimeOffset.UtcNow).Status),
            CancellationToken.None);

        Assert.Equal("ready", result);
        Assert.Equal(2, attempts);
        Assert.Equal(["retrying"], retryStatuses);
    }

    [Fact]
    public async Task ExecuteAsync_PermanentFailure_IsNotSwallowed()
    {
        var options = SchedulerLaunchOptions.CreateForTests(
            startupRetryDelay: TimeSpan.FromMilliseconds(1),
            startupAttemptTimeout: TimeSpan.FromSeconds(1));
        var reporter = new HealthStatusReporter(options);
        var attempts = 0;

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            StartupRetryPolicy.ExecuteAsync<string>(
                _ =>
                {
                    attempts++;
                    throw new InvalidOperationException("configuration is incomplete");
                },
                options,
                reporter,
                _ => { },
                CancellationToken.None));

        Assert.Equal("configuration is incomplete", ex.Message);
        Assert.Equal(1, attempts);
    }

    [Fact]
    public void IsTransient_AuthenticationFailureWrappedInInvalidOperation_IsPermanent()
    {
        var ex = new InvalidOperationException(
            "Failed to connect: TLS handshake failed.",
            new System.Security.Authentication.AuthenticationException("bad cert"));

        Assert.False(StartupRetryPolicy.IsTransient(ex));
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
}
