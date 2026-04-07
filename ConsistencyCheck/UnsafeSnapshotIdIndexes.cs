using Raven.Client.Documents.Indexes;

namespace ConsistencyCheck;

internal sealed class UnsafeSnapshotIdLookupEntry
{
    public string RunId { get; set; } = string.Empty;
    public string OriginalDocumentId { get; set; } = string.Empty;
    public string FromNode { get; set; } = string.Empty;
}

internal static class UnsafeSnapshotIdIndexes
{
    public const string CollectionName = "UnsafeSnapshotIdDocuments";
    public const string ByRunAndOriginalDocumentIdIndexName = "UnsafeSnapshotIdDocuments/ByRunAndOriginalDocumentId";

    public static IndexDefinition BuildByRunAndOriginalDocumentIdIndexDefinition()
    {
        return new IndexDefinition
        {
            Name = ByRunAndOriginalDocumentIdIndexName,
            Maps =
            {
                $$"""
                  from doc in docs.{{CollectionName}}
                  select new
                  {
                      doc.RunId,
                      doc.OriginalDocumentId,
                      doc.FromNode
                  }
                  """
            }
        };
    }
}
