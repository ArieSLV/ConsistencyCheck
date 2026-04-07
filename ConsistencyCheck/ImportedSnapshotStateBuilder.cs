using System.Globalization;

namespace ConsistencyCheck;

internal static class ImportedSnapshotStateBuilder
{
    public static IReadOnlyCollection<string> BuildPhysicalSnapshotIds(
        IReadOnlyCollection<string> originalDocumentIds,
        IReadOnlyList<NodeConfig> snapshotNodes)
    {
        var distinctOriginalDocumentIds = DistinctDocumentIds(originalDocumentIds);
        if (distinctOriginalDocumentIds.Count == 0 || snapshotNodes.Count == 0)
            return [];

        var ids = new List<string>(distinctOriginalDocumentIds.Count * snapshotNodes.Count);
        for (var nodeIndex = 0; nodeIndex < snapshotNodes.Count; nodeIndex++)
        {
            var alias = NodeDocumentSnapshots.GetNodeAlias(nodeIndex);
            foreach (var originalDocumentId in distinctOriginalDocumentIds)
                ids.Add(NodeDocumentSnapshots.GetSnapshotId(originalDocumentId, alias));
        }

        return ids;
    }

    public static Dictionary<string, List<NodeObservedState>> BuildObservedStates(
        IReadOnlyCollection<string> originalDocumentIds,
        IReadOnlyList<NodeConfig> snapshotNodes,
        IReadOnlyDictionary<string, NodeDocumentSnapshot?> snapshotsByPhysicalId)
    {
        var distinctOriginalDocumentIds = DistinctDocumentIds(originalDocumentIds);

        var observedStatesByDocumentId = distinctOriginalDocumentIds.ToDictionary(
            documentId => documentId,
            _ => new List<NodeObservedState>(snapshotNodes.Count),
            StringComparer.OrdinalIgnoreCase);

        for (var nodeIndex = 0; nodeIndex < snapshotNodes.Count; nodeIndex++)
        {
            var node = snapshotNodes[nodeIndex];
            var alias = NodeDocumentSnapshots.GetNodeAlias(nodeIndex);
            foreach (var originalDocumentId in distinctOriginalDocumentIds)
            {
                var snapshotId = NodeDocumentSnapshots.GetSnapshotId(originalDocumentId, alias);
                snapshotsByPhysicalId.TryGetValue(snapshotId, out var snapshot);
                observedStatesByDocumentId[originalDocumentId].Add(ToObservedState(node.Url, snapshot));
            }
        }

        return observedStatesByDocumentId;
    }

    private static NodeObservedState ToObservedState(string nodeUrl, NodeDocumentSnapshot? snapshot)
    {
        return new NodeObservedState
        {
            NodeUrl = nodeUrl,
            Present = snapshot != null,
            Collection = snapshot?.Collection,
            ChangeVector = snapshot?.ChangeVector,
            LastModified = snapshot?.LastModified?.UtcDateTime.ToString("O", CultureInfo.InvariantCulture)
        };
    }

    private static IReadOnlyList<string> DistinctDocumentIds(IReadOnlyCollection<string> originalDocumentIds)
    {
        if (originalDocumentIds.Count == 0)
            return [];

        var distinct = new List<string>(originalDocumentIds.Count);
        var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var originalDocumentId in originalDocumentIds)
        {
            if (seen.Add(originalDocumentId))
                distinct.Add(originalDocumentId);
        }

        return distinct;
    }
}
