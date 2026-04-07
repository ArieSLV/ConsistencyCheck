using Raven.Client.Documents.Indexes;

namespace ConsistencyCheck;

internal sealed class SnapshotImportSkippedLookupEntry
{
    public string RunId { get; set; } = string.Empty;
    public string OriginalDocumentId { get; set; } = string.Empty;
}

internal static class SnapshotImportSkippedIndexes
{
    public const string CollectionName = "SnapshotImportSkippedDocuments";
    public const string ByRunAndOriginalDocumentIdIndexName = "SnapshotImportSkippedDocuments/ByRunAndOriginalDocumentId";

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
                      doc.OriginalDocumentId
                  }
                  """
            }
        };
    }
}
