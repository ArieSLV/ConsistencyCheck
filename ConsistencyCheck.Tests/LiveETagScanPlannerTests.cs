using Xunit;

namespace ConsistencyCheck.Tests;

public sealed class LiveETagScanPlannerTests
{
    [Theory]
    [InlineData("Document", null, true)]
    [InlineData("document", null, true)]
    [InlineData("Counter", null, false)]
    [InlineData("Document", "Conflicted", false)]
    [InlineData("Document", "DeleteRevision", false)]
    [InlineData("Document", "Conflicted, DeleteRevision", false)]
    [InlineData("Document", "HasRevisions", true)]
    public void ShouldIncludePageItem_AppliesTypeAndFlagFilters(string type, string? flags, bool expected)
    {
        var actual = LiveETagScanPlanner.ShouldIncludePageItem(type, flags);

        Assert.Equal(expected, actual);
    }
}