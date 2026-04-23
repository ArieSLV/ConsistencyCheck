using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Hosting;

namespace ConsistencyCheck;

internal sealed class HealthEndpointServer : IAsyncDisposable
{
    private readonly WebApplication _app;

    private HealthEndpointServer(WebApplication app)
    {
        _app = app;
    }

    public static HealthEndpointServer Create(
        SchedulerLaunchOptions options,
        HealthStatusReporter healthStatus)
    {
        var builder = WebApplication.CreateSlimBuilder(new WebApplicationOptions
        {
            Args = []
        });
        builder.WebHost.UseUrls(options.HealthBindUrl);

        var app = builder.Build();
        app.MapGet(options.HealthPath, async (HttpContext context) =>
        {
            var response = healthStatus.BuildResponse(DateTimeOffset.UtcNow);
            context.Response.StatusCode = response.Ready
                ? StatusCodes.Status200OK
                : StatusCodes.Status503ServiceUnavailable;
            await context.Response.WriteAsJsonAsync(response, cancellationToken: context.RequestAborted).ConfigureAwait(false);
        });

        return new HealthEndpointServer(app);
    }

    public Task StartAsync(CancellationToken ct)
        => _app.StartAsync(ct);

    public async ValueTask DisposeAsync()
    {
        await _app.DisposeAsync().ConfigureAwait(false);
    }
}
