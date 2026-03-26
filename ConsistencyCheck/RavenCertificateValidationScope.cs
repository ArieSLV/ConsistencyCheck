using System.Net.Security;
using Raven.Client.Http;

namespace ConsistencyCheck;

internal sealed class RavenCertificateValidationScope : IDisposable
{
    private readonly RemoteCertificateValidationCallback? _callback;

    private RavenCertificateValidationScope(RemoteCertificateValidationCallback? callback)
    {
        _callback = callback;
    }

    public static RavenCertificateValidationScope Create(IEnumerable<string> urls, bool allowInvalidServerCertificates)
    {
        if (allowInvalidServerCertificates == false)
            return new RavenCertificateValidationScope(null);

        var allowedHosts = urls
            .Select(TryGetHost)
            .Where(static host => string.IsNullOrWhiteSpace(host) == false)
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToHashSet(StringComparer.OrdinalIgnoreCase);

        if (allowedHosts.Count == 0)
            return new RavenCertificateValidationScope(null);

        RemoteCertificateValidationCallback callback = (sender, _, _, _) =>
        {
            var host = RequestExecutor.ConvertSenderObjectToHostname(sender);
            return string.IsNullOrWhiteSpace(host) == false && allowedHosts.Contains(host);
        };

        try
        {
            RequestExecutor.RemoteCertificateValidationCallback += callback;
        }
        catch (PlatformNotSupportedException ex)
        {
            throw new InvalidOperationException(
                "AllowInvalidServerCertificates is not supported on this platform.",
                ex);
        }

        return new RavenCertificateValidationScope(callback);
    }

    public void Dispose()
    {
        if (_callback != null)
            RequestExecutor.RemoteCertificateValidationCallback -= _callback;
    }

    private static string? TryGetHost(string url)
    {
        if (!Uri.TryCreate(url, UriKind.Absolute, out var uri))
            return null;

        return uri.Host;
    }
}
