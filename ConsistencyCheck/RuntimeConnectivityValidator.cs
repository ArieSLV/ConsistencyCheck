using Raven.Client.Documents.Operations;
using Raven.Client.Exceptions.Security;

namespace ConsistencyCheck;

internal static class RuntimeConnectivityValidator
{
    public static async Task ValidateAsync(AppConfig config, CancellationToken ct)
    {
        var certificate = ConfigWizard.LoadCertificate(config);
        foreach (var node in config.Nodes)
        {
            using var store = ConfigWizard.BuildStore(node.Url, config.DatabaseName, certificate);
            try
            {
                await store.Maintenance.SendAsync(new GetStatisticsOperation(), ct).ConfigureAwait(false);
            }
            catch (Exception ex) when (TryGetCertificateNameMismatch(ex, out var mismatch))
            {
                throw new InvalidOperationException(
                    $"Node '{node.Label}' at '{node.Url}' does not match the server certificate hostname. {mismatch.Message}",
                    ex);
            }
            catch (Exception ex) when (TryGetAuthenticationException(ex, out _))
            {
                var hint = config.AllowInvalidServerCertificates
                    ? "The cluster certificate is still being rejected even with AllowInvalidServerCertificates enabled."
                    : "If this is a local synthetic/test cluster with self-signed certificates, enable AllowInvalidServerCertificates in config.json.";
                throw new InvalidOperationException(
                    $"Failed to connect to node '{node.Label}' at '{node.Url}': TLS handshake failed. {hint}",
                    ex);
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException(
                    $"Failed to connect to node '{node.Label}' at '{node.Url}': {ex.Message}",
                    ex);
            }
        }
    }

    private static bool TryGetCertificateNameMismatch(Exception ex, out CertificateNameMismatchException mismatch)
    {
        for (var current = ex; current != null; current = current.InnerException!)
        {
            if (current is CertificateNameMismatchException typed)
            {
                mismatch = typed;
                return true;
            }
        }

        mismatch = null!;
        return false;
    }

    private static bool TryGetAuthenticationException(
        Exception ex,
        out System.Security.Authentication.AuthenticationException authentication)
    {
        for (var current = ex; current != null; current = current.InnerException!)
        {
            if (current is System.Security.Authentication.AuthenticationException typed)
            {
                authentication = typed;
                return true;
            }
        }

        authentication = null!;
        return false;
    }
}
