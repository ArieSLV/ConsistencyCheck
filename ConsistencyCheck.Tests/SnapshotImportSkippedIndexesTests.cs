using Xunit;

namespace ConsistencyCheck.Tests;

public sealed class SnapshotImportSkippedIndexesTests
{
    [Fact]
    public void BuildByRunAndOriginalDocumentIdIndexDefinition_UsesExpectedIdentityAndFields()
    {
        var definition = SnapshotImportSkippedIndexes.BuildByRunAndOriginalDocumentIdIndexDefinition();

        Assert.Equal(SnapshotImportSkippedIndexes.ByRunAndOriginalDocumentIdIndexName, definition.Name);
        Assert.Contains("docs.SnapshotImportSkippedDocuments", definition.Maps.Single());
        Assert.Contains("doc.RunId", definition.Maps.Single());
        Assert.Contains("doc.OriginalDocumentId", definition.Maps.Single());
    }
}
