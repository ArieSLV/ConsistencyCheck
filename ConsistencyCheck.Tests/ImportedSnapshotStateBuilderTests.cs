using Xunit;

namespace ConsistencyCheck.Tests;

public sealed class ImportedSnapshotStateBuilderTests
{
    private static readonly IReadOnlyList<NodeConfig> Nodes =
    [
        new() { Label = "Node A", Url = "https://node-a" },
        new() { Label = "Node B", Url = "https://node-b" },
        new() { Label = "Node C", Url = "https://node-c" }
    ];

    [Fact]
    public void BuildObservedStates_MissingReplica_ProducesMissingEvaluation()
    {
        var states = BuildStates(
            Snapshot("Users/1", 0, "A:2-db", "Users"),
            Snapshot("Users/1", 1, "A:2-db", "Users"));

        var evaluation = ConsistencyDecisionEngine.EvaluateDocument("Users/1", states["Users/1"]);

        Assert.NotNull(evaluation);
        Assert.Equal("MISSING", evaluation!.MismatchType);
        Assert.Equal("https://node-a", evaluation.WinnerNode);
        Assert.Contains(evaluation.ObservedState, state => state.NodeUrl == "https://node-c" && state.Present == false);
    }

    [Fact]
    public void BuildObservedStates_CvMismatch_ProducesWinnerFromImportedSnapshots()
    {
        var states = BuildStates(
            Snapshot("Users/1", 0, "A:1-db", "Users"),
            Snapshot("Users/1", 1, "A:2-db", "Users"),
            Snapshot("Users/1", 2, "A:2-db", "Users"));

        var evaluation = ConsistencyDecisionEngine.EvaluateDocument("Users/1", states["Users/1"]);

        Assert.NotNull(evaluation);
        Assert.Equal("CV_MISMATCH", evaluation!.MismatchType);
        Assert.Equal("https://node-b", evaluation.WinnerNode);
        Assert.Equal("A:2-db", evaluation.WinnerCV);
    }

    [Fact]
    public void BuildObservedStates_AmbiguousVectors_ProducesAmbiguousEvaluation()
    {
        var states = BuildStates(
            Snapshot("Users/1", 0, "A:1-dbA", "Users"),
            Snapshot("Users/1", 1, "B:1-dbB", "Users"),
            Snapshot("Users/1", 2, "A:1-dbA", "Users"));

        var evaluation = ConsistencyDecisionEngine.EvaluateDocument("Users/1", states["Users/1"]);

        Assert.NotNull(evaluation);
        Assert.Equal("AMBIGUOUS_CV", evaluation!.MismatchType);
        Assert.Null(evaluation.WinnerNode);
    }

    [Fact]
    public void BuildObservedStates_CollectionMismatch_ProducesCollectionMismatchEvaluation()
    {
        var states = BuildStates(
            Snapshot("Users/1", 0, "A:2-db", "Users"),
            Snapshot("Users/1", 1, "A:2-db", "Orders"),
            Snapshot("Users/1", 2, "A:2-db", "Users"));

        var evaluation = ConsistencyDecisionEngine.EvaluateDocument("Users/1", states["Users/1"]);

        Assert.NotNull(evaluation);
        Assert.Equal("COLLECTION_MISMATCH", evaluation!.MismatchType);
    }

    [Fact]
    public void BuildObservedStates_DuplicateOriginalDocumentIds_AreCollapsed()
    {
        var byId = new Dictionary<string, NodeDocumentSnapshot?>(StringComparer.OrdinalIgnoreCase)
        {
            [NodeDocumentSnapshots.GetSnapshotId("Users/1", "A")] = Snapshot("Users/1", 0, "A:2-db", "Users"),
            [NodeDocumentSnapshots.GetSnapshotId("Users/1", "B")] = Snapshot("Users/1", 1, "A:2-db", "Users"),
            [NodeDocumentSnapshots.GetSnapshotId("Users/1", "C")] = Snapshot("Users/1", 2, "A:2-db", "Users")
        };

        var states = ImportedSnapshotStateBuilder.BuildObservedStates(["Users/1", "Users/1"], Nodes, byId);

        Assert.Single(states);
        Assert.True(states.ContainsKey("Users/1"));
        Assert.Equal(3, states["Users/1"].Count);
    }

    private static Dictionary<string, List<NodeObservedState>> BuildStates(params NodeDocumentSnapshot[] snapshots)
    {
        var byId = snapshots.ToDictionary(snapshot => snapshot.Id, snapshot => (NodeDocumentSnapshot?)snapshot, StringComparer.OrdinalIgnoreCase);
        return ImportedSnapshotStateBuilder.BuildObservedStates(["Users/1"], Nodes, byId);
    }

    private static NodeDocumentSnapshot Snapshot(string originalDocumentId, int nodeIndex, string changeVector, string collection)
    {
        var node = Nodes[nodeIndex];
        return NodeDocumentSnapshots.Create(
            originalDocumentId,
            NodeDocumentSnapshots.GetNodeAlias(nodeIndex),
            node.Label,
            node.Url,
            changeVector,
            collection,
            DateTimeOffset.Parse("2026-03-31T00:00:00Z"));
    }
}
