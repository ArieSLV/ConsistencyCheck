using System.Security.Cryptography.X509Certificates;
using Raven.Client.Documents.Operations;
using Raven.Client.ServerWide.Operations;

namespace ConsistencyCheck;

internal static class ChangeVectorSemanticsResolver
{
    public static async Task<ChangeVectorSemanticsSnapshot> ResolveAsync(AppConfig config, CancellationToken ct)
    {
        using var certificate = ConfigWizard.LoadCertificate(config);
        using var certificateValidationScope = RavenCertificateValidationScope.Create(
            config.Nodes.Select(node => node.Url),
            config.AllowInvalidServerCertificates);

        var explicitUnusedDatabaseIds = await LoadExplicitUnusedDatabaseIdsAsync(config, certificate, ct)
            .ConfigureAwait(false);

        var nodeInfos = new List<ChangeVectorSemanticsNodeInfo>(config.Nodes.Count);
        foreach (var node in config.Nodes)
        {
            using var store = ConfigWizard.BuildStore(node.Url, config.DatabaseName, certificate);
            var statistics = await store.Maintenance.SendAsync(new GetStatisticsOperation(), ct).ConfigureAwait(false);

            nodeInfos.Add(new ChangeVectorSemanticsNodeInfo
            {
                NodeUrl = node.Url,
                Label = node.Label,
                DatabaseId = statistics.DatabaseId,
                DatabaseChangeVector = statistics.DatabaseChangeVector
            });
        }

        return ChangeVectorSemantics.CreateSnapshot(nodeInfos, explicitUnusedDatabaseIds);
    }

    private static async Task<IReadOnlyCollection<string>> LoadExplicitUnusedDatabaseIdsAsync(
        AppConfig config,
        X509Certificate2? certificate,
        CancellationToken ct)
    {
        using var store = ConfigWizard.BuildStore(config.Nodes[0].Url, config.DatabaseName, certificate);
        var record = await store.Maintenance.Server
            .SendAsync(new GetDatabaseRecordOperation(config.DatabaseName), ct)
            .ConfigureAwait(false);

        return record.UnusedDatabaseIds?.ToArray() ?? [];
    }
}