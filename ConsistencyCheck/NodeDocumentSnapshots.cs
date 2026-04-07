using System.Security.Cryptography;
using System.Text;
using Raven.Client.Documents.Indexes;

namespace ConsistencyCheck;

internal sealed class NodeDocumentSnapshot
{
    public string Id { get; set; } = string.Empty;
    public string OriginalDocumentId { get; set; } = string.Empty;
    public string FromNode { get; set; } = string.Empty;
    public string NodeLabel { get; set; } = string.Empty;
    public string NodeUrl { get; set; } = string.Empty;
    public string ChangeVector { get; set; } = string.Empty;
    public string? Collection { get; set; }
    public DateTimeOffset? LastModified { get; set; }
}

internal sealed class ProblemDocumentIndexEntry
{
    public string OriginalDocumentId { get; set; } = string.Empty;
    public int Count { get; set; }
    public List<string> FromNodes { get; set; } = [];
    public int FromNodesCount { get; set; }
    public List<string> ChangeVectors { get; set; } = [];
    public int ChangeVectorsCount { get; set; }
}

internal sealed class SnapshotQueryProjection
{
    public string? OriginalDocumentId { get; set; }
    public string? ChangeVector { get; set; }
    public string? Collection { get; set; }
    public DateTimeOffset? LastModified { get; set; }
}

internal static class NodeDocumentSnapshots
{
    public const string CollectionName = "NodeDocumentSnapshots";
    public const string ProblemDocumentsIndexName = "NodeDocumentSnapshots/ProblemDocuments";
    public const string ByFromNodeAndOriginalDocumentIdIndexName = "Index/NodeDocumentSnapshots/ByFromNodeAndOriginalDocumentId";
    private const string StaticSearchEngineTypeSetting = "Indexing.Static.SearchEngineType";

    public const string QueryProjection = """
                                         from @all_docs as doc
                                         select {
                                             OriginalDocumentId: id(doc),
                                             ChangeVector: getMetadata(doc)['@change-vector'],
                                             Collection: getMetadata(doc)['@collection'],
                                             LastModified: getMetadata(doc)['@last-modified']
                                         }
                                         """;

    public static NodeDocumentSnapshot Create(
        string originalDocumentId,
        string fromNode,
        string nodeLabel,
        string nodeUrl,
        string? changeVector,
        string? collection,
        DateTimeOffset? lastModified)
    {
        return new NodeDocumentSnapshot
        {
            Id = GetSnapshotId(originalDocumentId, fromNode),
            OriginalDocumentId = originalDocumentId,
            FromNode = fromNode,
            NodeLabel = nodeLabel,
            NodeUrl = nodeUrl,
            ChangeVector = changeVector ?? string.Empty,
            Collection = collection,
            LastModified = lastModified
        };
    }

    public static string GetSnapshotId(string originalDocumentId, string fromNode)
        => ContainsBulkInsertUnsafeCharacters(originalDocumentId)
            ? $"{CollectionName}/{fromNode}/{HashOriginalDocumentId(originalDocumentId)}"
            : $"{originalDocumentId}/{fromNode}";

    public static bool ContainsBulkInsertUnsafeCharacters(string value)
    {
        if (string.IsNullOrEmpty(value))
            return false;

        foreach (var ch in value)
        {
            if (ch == '\\' || ch < ' ')
                return true;
        }

        return false;
    }

    public static string DescribeBulkInsertUnsafeCharacters(string value)
    {
        var reasons = new List<string>(2);

        if (string.IsNullOrEmpty(value) == false && value.IndexOf('\\') >= 0)
            reasons.Add("ContainsBackslash");

        if (string.IsNullOrEmpty(value) == false && value.Any(static ch => ch < ' '))
            reasons.Add("ContainsControlCharacter");

        return reasons.Count == 0
            ? "None"
            : string.Join(",", reasons);
    }

    public static string GetNodeAlias(int index)
    {
        if (index < 0)
            throw new ArgumentOutOfRangeException(nameof(index));

        return ((char)('A' + index)).ToString();
    }

    public static bool IsProblemCandidate(int configuredNodeCount, int count, int changeVectorsCount)
        => count < configuredNodeCount || changeVectorsCount > 1;

    public static ProblemDocumentIndexEntry AggregateLatest(
        IEnumerable<NodeDocumentSnapshot> snapshots,
        int configuredNodeCount)
    {
        var latest = snapshots
            .GroupBy(snapshot => snapshot.Id, StringComparer.OrdinalIgnoreCase)
            .Select(group => group
                .OrderByDescending(snapshot => snapshot.LastModified ?? DateTimeOffset.MinValue)
                .ThenByDescending(snapshot => snapshot.ChangeVector, StringComparer.OrdinalIgnoreCase)
                .First())
            .ToList();

        if (latest.Count == 0)
            throw new InvalidOperationException("Cannot aggregate an empty snapshot set.");

        var fromNodes = latest
            .Select(snapshot => snapshot.FromNode)
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .OrderBy(static node => node, StringComparer.OrdinalIgnoreCase)
            .ToList();

        var changeVectors = latest
            .Select(snapshot => snapshot.ChangeVector)
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .OrderBy(static changeVector => changeVector, StringComparer.OrdinalIgnoreCase)
            .ToList();

        return new ProblemDocumentIndexEntry
        {
            OriginalDocumentId = latest
                .Select(snapshot => snapshot.OriginalDocumentId)
                .OrderBy(static id => id, StringComparer.Ordinal)
                .First(),
            Count = latest.Count,
            FromNodes = fromNodes,
            FromNodesCount = fromNodes.Count,
            ChangeVectors = changeVectors,
            ChangeVectorsCount = changeVectors.Count
        };
    }

    public static IndexDefinition BuildProblemDocumentsIndexDefinition()
    {
        return new IndexDefinition
        {
            Name = ProblemDocumentsIndexName,
            Configuration =
            {
                [StaticSearchEngineTypeSetting] = SearchEngineType.Corax.ToString()
            },
            Maps =
            {
                $$"""
                  from doc in docs.{{CollectionName}}
                  select new
                  {
                      doc.OriginalDocumentId,
                      Count = 1,
                      FromNodes = new[] { doc.FromNode },
                      FromNodesCount = 1,
                      ChangeVectors = new[] { doc.ChangeVector },
                      ChangeVectorsCount = 1
                  }
                  """
            },
            Reduce =
                """
                from result in results
                group result by result.OriginalDocumentId into g
                let fromNodes = g.SelectMany(x => x.FromNodes).Distinct()
                let changeVectors = g.SelectMany(x => x.ChangeVectors).Distinct()
                select new
                {
                    OriginalDocumentId = g.Key,
                    Count = g.Sum(x => x.Count),
                    FromNodes = fromNodes.ToArray(),
                    FromNodesCount = fromNodes.Count(),
                    ChangeVectors = changeVectors.ToArray(),
                    ChangeVectorsCount = changeVectors.Count()
                }
                """
        };
    }

    private static string HashOriginalDocumentId(string originalDocumentId)
        => Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(originalDocumentId)));
}

internal sealed record IndexedRunProgressUpdate(
    string Phase,
    string? CurrentNodeLabel,
    string? Detail,
    long? CurrentNodeStreamedSnapshots,
    long? CurrentNodeTotalDocuments,
    long SnapshotDocumentsImported,
    long SnapshotDocumentsSkipped,
    long SnapshotBulkInsertRestarts,
    long CandidateDocumentsFound,
    long CandidateDocumentsProcessed,
    long CandidateDocumentsExcludedBySkippedSnapshots,
    long DocumentsInspected,
    long UniqueVersionsCompared,
    long MismatchesFound,
    long RepairsPlanned,
    long RepairsAttempted,
    long RepairsPatchedOnWinner,
    long RepairsFailed,
    long? RepairsCompleted = null);
