using Xunit;

namespace ConsistencyCheck.Tests;

public sealed class RepairPlanExecutionPlannerTests
{
    [Fact]
    public void ExpandToSingleDocumentPlans_SplitsGroupedRepairIntoDeterministicPerDocumentItems()
    {
        var grouped = new RepairDocument
        {
            Id = "repairs/runs/source/group-1",
            RunId = "runs/source",
            DocumentIds = ["orders/1-A", "orders/2-A"],
            Collection = "Orders",
            WinnerNode = "http://node-a",
            WinnerCV = "A:5-db1",
            AffectedNodes = ["http://node-b", "http://node-c"],
            RepairStatus = "PatchPlannedDryRun"
        };

        var expanded = RepairPlanExecutionPlanner.ExpandToSingleDocumentPlans([grouped]);

        Assert.Collection(
            expanded.OrderBy(plan => plan.DocumentIds.Single(), StringComparer.OrdinalIgnoreCase),
            first =>
            {
                Assert.Equal(["orders/1-A"], first.DocumentIds);
                Assert.Equal(grouped.Collection, first.Collection);
                Assert.Equal(grouped.WinnerNode, first.WinnerNode);
                Assert.Equal(grouped.Id, first.SourceRepairPlanId);
                Assert.Equal(grouped.RunId, first.SourceRepairPlanRunId);
            },
            second =>
            {
                Assert.Equal(["orders/2-A"], second.DocumentIds);
                Assert.Equal(grouped.Collection, second.Collection);
                Assert.Equal(grouped.WinnerNode, second.WinnerNode);
                Assert.Equal(grouped.Id, second.SourceRepairPlanId);
                Assert.Equal(grouped.RunId, second.SourceRepairPlanRunId);
            });
    }

    [Fact]
    public void ObservedStatesMatch_IgnoresUnusedChangeVectorSegments()
    {
        IReadOnlyList<NodeConfig> nodes =
        [
            new NodeConfig { Label = "Node A", Url = "http://node-a" },
            new NodeConfig { Label = "Node B", Url = "http://node-b" },
            new NodeConfig { Label = "Node C", Url = "http://node-c" }
        ];
        var semantics = new ChangeVectorSemanticsSnapshot
        {
            ExplicitUnusedDatabaseIds = ["unused-db"]
        };
        IReadOnlyCollection<NodeObservedState> expected =
        [
            new NodeObservedState { NodeUrl = "http://node-a", Present = true, Collection = "Orders", ChangeVector = "A:5-live, A:3-unused-db" },
            new NodeObservedState { NodeUrl = "http://node-b", Present = true, Collection = "Orders", ChangeVector = "A:5-live" },
            new NodeObservedState { NodeUrl = "http://node-c", Present = false }
        ];
        IReadOnlyCollection<NodeObservedState> actual =
        [
            new NodeObservedState { NodeUrl = "http://node-a", Present = true, Collection = "Orders", ChangeVector = "A:5-live" },
            new NodeObservedState { NodeUrl = "http://node-b", Present = true, Collection = "Orders", ChangeVector = "A:5-live" },
            new NodeObservedState { NodeUrl = "http://node-c", Present = false }
        ];

        var matches = RepairPlanExecutionPlanner.ObservedStatesMatch(expected, actual, nodes, semantics);

        Assert.True(matches);
    }
}
