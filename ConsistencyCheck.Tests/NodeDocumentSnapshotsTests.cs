using Xunit;

namespace ConsistencyCheck.Tests;

public sealed class NodeDocumentSnapshotsTests
{
    [Fact]
    public void Create_UsesOriginalDocumentIdAndOrdinalNodeAliasInPhysicalId()
    {
        var snapshot = NodeDocumentSnapshots.Create(
            "Shippers/1",
            NodeDocumentSnapshots.GetNodeAlias(1),
            "Node B",
            "https://localhost:61743",
            "A:2-uooarG43xkifSIGaxra6pw",
            "Shippers",
            DateTimeOffset.Parse("2026-03-29T07:56:14.4592341Z"));

        Assert.Equal("Shippers/1/B", snapshot.Id);
        Assert.Equal("Shippers/1", snapshot.OriginalDocumentId);
        Assert.Equal("B", snapshot.FromNode);
        Assert.Equal("Node B", snapshot.NodeLabel);
    }

    [Fact]
    public void Create_NormalizesPhysicalIdWhenOriginalDocumentIdContainsBackslash()
    {
        var snapshot = NodeDocumentSnapshots.Create(
            @"elvDecodedVehicles\5/CHROMEDATA",
            "A",
            "Node A",
            "https://localhost/A",
            "A:2-db",
            "Vehicles",
            DateTimeOffset.Parse("2026-03-29T07:56:14.4592341Z"));

        Assert.Equal(@"elvDecodedVehicles\5/CHROMEDATA", snapshot.OriginalDocumentId);
        Assert.StartsWith("NodeDocumentSnapshots/A/", snapshot.Id);
        Assert.DoesNotContain('\\', snapshot.Id);
    }

    [Fact]
    public void AggregateLatest_ThreeEqualChangeVectors_IsNotProblemCandidate()
    {
        var aggregate = NodeDocumentSnapshots.AggregateLatest(
            [
                Snapshot("Users/1", "A", "A:2-db"),
                Snapshot("Users/1", "B", "A:2-db"),
                Snapshot("Users/1", "C", "A:2-db")
            ],
            configuredNodeCount: 3);

        Assert.Equal(3, aggregate.Count);
        Assert.Equal(1, aggregate.ChangeVectorsCount);
        Assert.False(NodeDocumentSnapshots.IsProblemCandidate(3, aggregate.Count, aggregate.ChangeVectorsCount));
    }

    [Fact]
    public void AggregateLatest_MissingNode_IsProblemCandidate()
    {
        var aggregate = NodeDocumentSnapshots.AggregateLatest(
            [
                Snapshot("Users/1", "A", "A:2-db"),
                Snapshot("Users/1", "B", "A:2-db")
            ],
            configuredNodeCount: 3);

        Assert.Equal(2, aggregate.Count);
        Assert.Equal(1, aggregate.ChangeVectorsCount);
        Assert.True(NodeDocumentSnapshots.IsProblemCandidate(3, aggregate.Count, aggregate.ChangeVectorsCount));
    }

    [Fact]
    public void AggregateLatest_DistinctChangeVectors_IsProblemCandidate()
    {
        var aggregate = NodeDocumentSnapshots.AggregateLatest(
            [
                Snapshot("Users/1", "A", "A:1-db"),
                Snapshot("Users/1", "B", "A:2-db"),
                Snapshot("Users/1", "C", "A:2-db")
            ],
            configuredNodeCount: 3);

        Assert.Equal(3, aggregate.Count);
        Assert.Equal(2, aggregate.ChangeVectorsCount);
        Assert.True(NodeDocumentSnapshots.IsProblemCandidate(3, aggregate.Count, aggregate.ChangeVectorsCount));
    }

    [Fact]
    public void IsProblemCandidate_CountAboveConfiguredWithoutCvMismatch_IsNotProblemCandidate()
    {
        Assert.False(NodeDocumentSnapshots.IsProblemCandidate(3, count: 4, changeVectorsCount: 1));
    }

    [Fact]
    public void AggregateLatest_RepeatedUpsertOnSamePhysicalSnapshot_KeepsLatestVersionOnly()
    {
        var aggregate = NodeDocumentSnapshots.AggregateLatest(
            [
                Snapshot("Users/1", "A", "A:1-db", lastModified: DateTimeOffset.Parse("2026-03-29T08:00:00Z")),
                Snapshot("Users/1", "A", "A:2-db", lastModified: DateTimeOffset.Parse("2026-03-29T08:01:00Z")),
                Snapshot("Users/1", "B", "A:2-db", lastModified: DateTimeOffset.Parse("2026-03-29T08:01:00Z")),
                Snapshot("Users/1", "C", "A:2-db", lastModified: DateTimeOffset.Parse("2026-03-29T08:01:00Z"))
            ],
            configuredNodeCount: 3);

        Assert.Equal(3, aggregate.Count);
        Assert.Equal(["A", "B", "C"], aggregate.FromNodes);
        Assert.Equal(["A:2-db"], aggregate.ChangeVectors);
        Assert.False(NodeDocumentSnapshots.IsProblemCandidate(3, aggregate.Count, aggregate.ChangeVectorsCount));
    }

    [Fact]
    public void Create_ProducesStableSnapshotBodyForIdenticalImportReplay()
    {
        var first = NodeDocumentSnapshots.Create(
            "Users/1",
            "A",
            "Node A",
            "https://localhost/A",
            "A:2-db",
            "Users",
            DateTimeOffset.Parse("2026-03-29T08:01:00Z"));
        var replay = NodeDocumentSnapshots.Create(
            "Users/1",
            "A",
            "Node A",
            "https://localhost/A",
            "A:2-db",
            "Users",
            DateTimeOffset.Parse("2026-03-29T08:01:00Z"));

        Assert.Equal(first.Id, replay.Id);
        Assert.Equal(first.OriginalDocumentId, replay.OriginalDocumentId);
        Assert.Equal(first.FromNode, replay.FromNode);
        Assert.Equal(first.NodeLabel, replay.NodeLabel);
        Assert.Equal(first.NodeUrl, replay.NodeUrl);
        Assert.Equal(first.ChangeVector, replay.ChangeVector);
        Assert.Equal(first.Collection, replay.Collection);
        Assert.Equal(first.LastModified, replay.LastModified);
    }

    [Fact]
    public void BuildProblemDocumentsIndexDefinition_UsesExpectedIdentityAndFields()
    {
        var definition = NodeDocumentSnapshots.BuildProblemDocumentsIndexDefinition();

        Assert.Equal(NodeDocumentSnapshots.ProblemDocumentsIndexName, definition.Name);
        Assert.Contains("docs.NodeDocumentSnapshots", definition.Maps.Single());
        Assert.DoesNotContain("OriginalDocumentKey", definition.Maps.Single());
        Assert.Contains("FromNodesCount = 1", definition.Maps.Single());
        Assert.Contains("ChangeVectorsCount = 1", definition.Maps.Single());
        Assert.Contains("ChangeVectorsCount", definition.Reduce);
        Assert.Contains("FromNodesCount", definition.Reduce);
        Assert.Contains("group result by result.OriginalDocumentId", definition.Reduce);
        Assert.Equal("Corax", definition.Configuration["Indexing.Static.SearchEngineType"]);
    }

    private static NodeDocumentSnapshot Snapshot(
        string originalDocumentId,
        string fromNode,
        string changeVector,
        DateTimeOffset? lastModified = null)
    {
        return NodeDocumentSnapshots.Create(
            originalDocumentId,
            fromNode,
            $"Node {fromNode}",
            $"https://localhost/{fromNode}",
            changeVector,
            "Users",
            lastModified ?? DateTimeOffset.Parse("2026-03-29T07:56:18.6460075Z"));
    }
}
