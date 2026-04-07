using Xunit;

namespace ConsistencyCheck.Tests;

public sealed class UnsafeSnapshotIdIndexesTests
{
    [Fact]
    public void BuildByRunAndOriginalDocumentIdIndexDefinition_UsesExpectedIdentityAndFields()
    {
        var definition = UnsafeSnapshotIdIndexes.BuildByRunAndOriginalDocumentIdIndexDefinition();

        Assert.Equal(UnsafeSnapshotIdIndexes.ByRunAndOriginalDocumentIdIndexName, definition.Name);
        Assert.Contains("docs.UnsafeSnapshotIdDocuments", definition.Maps.Single());
        Assert.Contains("doc.RunId", definition.Maps.Single());
        Assert.Contains("doc.OriginalDocumentId", definition.Maps.Single());
        Assert.Contains("doc.FromNode", definition.Maps.Single());
    }
}
