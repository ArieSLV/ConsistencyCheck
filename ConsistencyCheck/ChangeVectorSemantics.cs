using System.Text.Json;

namespace ConsistencyCheck;

internal static class ChangeVectorSemantics
{
    public static ChangeVectorSemanticsSnapshot CreateSnapshot(
        IEnumerable<ChangeVectorSemanticsNodeInfo> nodes,
        IEnumerable<string>? explicitUnusedDatabaseIds)
    {
        var normalizedNodes = nodes
            .Select(node => new ChangeVectorSemanticsNodeInfo
            {
                NodeUrl = node.NodeUrl,
                Label = node.Label,
                DatabaseId = string.IsNullOrWhiteSpace(node.DatabaseId) ? null : node.DatabaseId,
                DatabaseChangeVector = string.IsNullOrWhiteSpace(node.DatabaseChangeVector) ? null : node.DatabaseChangeVector
            })
            .OrderBy(node => node.NodeUrl, StringComparer.OrdinalIgnoreCase)
            .ToList();

        var explicitUnusedIds = explicitUnusedDatabaseIds?
            .Where(static id => string.IsNullOrWhiteSpace(id) == false)
            .Distinct(StringComparer.Ordinal)
            .OrderBy(static id => id, StringComparer.Ordinal)
            .ToList() ?? [];

        var currentDatabaseIds = normalizedNodes
            .Select(node => node.DatabaseId)
            .Where(static id => string.IsNullOrWhiteSpace(id) == false)
            .Cast<string>()
            .ToHashSet(StringComparer.Ordinal);

        var potentialUnusedIds = normalizedNodes
            .SelectMany(node => ConsistencyDecisionEngine.ParseChangeVector(node.DatabaseChangeVector).Keys)
            .Where(id => !currentDatabaseIds.Contains(id))
            .Where(id => !explicitUnusedIds.Contains(id, StringComparer.Ordinal))
            .Distinct(StringComparer.Ordinal)
            .OrderBy(static id => id, StringComparer.Ordinal)
            .ToList();

        return new ChangeVectorSemanticsSnapshot
        {
            Nodes = normalizedNodes,
            ExplicitUnusedDatabaseIds = explicitUnusedIds,
            PotentialUnusedDatabaseIds = potentialUnusedIds
        };
    }

    public static async Task<ChangeVectorSemanticsSnapshot> EnsureSnapshotAsync(
        RunStateDocument run,
        Func<CancellationToken, Task<ChangeVectorSemanticsSnapshot>> resolveSnapshotAsync,
        Func<RunStateDocument, CancellationToken, Task> persistRunAsync,
        CancellationToken ct)
    {
        if (run.ChangeVectorSemanticsSnapshot != null)
            return run.ChangeVectorSemanticsSnapshot;

        var snapshot = await resolveSnapshotAsync(ct).ConfigureAwait(false);
        run.ChangeVectorSemanticsSnapshot = snapshot;
        await persistRunAsync(run, ct).ConfigureAwait(false);
        return snapshot;
    }

    public static DiagnosticDocument CreateDiagnostic(string runId, ChangeVectorSemanticsSnapshot snapshot)
    {
        var explicitCount = snapshot.ExplicitUnusedDatabaseIds.Count;
        var potentialCount = snapshot.PotentialUnusedDatabaseIds.Count;

        return new DiagnosticDocument
        {
            Id = CreateDiagnosticId(runId),
            RunId = runId,
            Kind = "ChangeVectorSemantics",
            Message =
                $"ExplicitUnusedDatabaseIds={explicitCount}, PotentialUnusedDatabaseIds={potentialCount} (warning only).",
            JsonPayload = JsonSerializer.Serialize(snapshot),
            Timestamp = DateTimeOffset.UtcNow
        };
    }

    private static string CreateDiagnosticId(string runId) => $"diagnostics/{runId}/{Guid.NewGuid():N}";
}
