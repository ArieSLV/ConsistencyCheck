using Raven.Client.Documents.Indexes;

namespace ConsistencyCheck;

internal sealed class RepairStateChangedLookupEntry
{
    public string ApplyRunId { get; set; } = string.Empty;
    public string DocumentId { get; set; } = string.Empty;
    public bool LiveIsConsistent { get; set; }
    public string Reason { get; set; } = string.Empty;
    public DateTimeOffset RecordedAt { get; set; }
}

internal static class RepairStateChangedIndexes
{
    public const string CollectionName = "RepairStateChangedDocuments";
    public const string ByApplyRunAndDocumentIdIndexName = "RepairStateChangedDocuments/ByApplyRunAndDocumentId";

    public static IndexDefinition BuildByApplyRunAndDocumentIdIndexDefinition()
    {
        return new IndexDefinition
        {
            Name = ByApplyRunAndDocumentIdIndexName,
            Maps =
            {
                $$"""
                  from doc in docs.{{CollectionName}}
                  select new
                  {
                      doc.ApplyRunId,
                      doc.DocumentId,
                      doc.LiveIsConsistent,
                      doc.Reason,
                      doc.RecordedAt
                  }
                  """
            }
        };
    }
}
