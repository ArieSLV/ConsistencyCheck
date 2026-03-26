using Xunit;

namespace ConsistencyCheck.Tests;

public sealed class ConsistencyDecisionEngineFocusedTests
{
    [Fact]
    public void RawDifferenceOnlyUnusedIds_RemainsMismatchWithoutExplicitIgnoreSet()
    {
        var evaluation = ConsistencyDecisionEngine.EvaluateDocument(
            "docs/difference-only-unused/raw",
            ConsistencyScenarioData.BuildSnapshot(
                $"A:7-{ConsistencyScenarioData.CurrentDbIdA},TRXN:3-{ConsistencyScenarioData.UnusedDbId1}",
                $"RAFT:7-{ConsistencyScenarioData.CurrentDbIdA}",
                $"SINK:7-{ConsistencyScenarioData.CurrentDbIdA}"));

        Assert.NotNull(evaluation);
        Assert.Equal("CV_MISMATCH", evaluation.MismatchType);
        Assert.Equal("A", evaluation.WinnerNode);
    }

    [Fact]
    public void DifferenceOnlyInExplicitUnusedIds_BecomesConsistentAfterStripping()
    {
        var semanticsSnapshot = new ChangeVectorSemanticsSnapshot
        {
            ExplicitUnusedDatabaseIds = [ConsistencyScenarioData.UnusedDbId1]
        };

        var evaluation = ConsistencyDecisionEngine.EvaluateDocument(
            "docs/difference-only-unused/stripped",
            ConsistencyScenarioData.BuildSnapshot(
                $"A:7-{ConsistencyScenarioData.CurrentDbIdA},TRXN:3-{ConsistencyScenarioData.UnusedDbId1}",
                $"RAFT:7-{ConsistencyScenarioData.CurrentDbIdA}",
                $"SINK:7-{ConsistencyScenarioData.CurrentDbIdA}"),
            semanticsSnapshot);

        Assert.Null(evaluation);
    }

    [Fact]
    public void ParseChangeVector_CollapsesSameDatabaseIdAcrossDifferentTags()
    {
        var parsed = ConsistencyDecisionEngine.ParseChangeVector(
            $"A:2-{ConsistencyScenarioData.SharedDbId},TRXN:5-{ConsistencyScenarioData.SharedDbId},SINK:3-{ConsistencyScenarioData.SharedDbId}");

        Assert.Single(parsed);
        Assert.True(parsed.TryGetValue(ConsistencyScenarioData.SharedDbId, out var etag));
        Assert.Equal(5, etag);
    }

    [Fact]
    public void SharedDatabaseIdAcrossTags_KeepsStatesInSameDominantGroup()
    {
        var evaluation = ConsistencyDecisionEngine.EvaluateDocument(
            "docs/same-dbid-different-tags",
            ConsistencyScenarioData.BuildSnapshot(
                $"A:2-{ConsistencyScenarioData.SharedDbId},TRXN:5-{ConsistencyScenarioData.SharedDbId}",
                $"SINK:5-{ConsistencyScenarioData.SharedDbId}",
                null));

        Assert.NotNull(evaluation);
        Assert.Equal("MISSING", evaluation.MismatchType);
        Assert.Equal("A", evaluation.WinnerNode);
    }

    [Fact]
    public void CreateSnapshot_SurfacesPotentialUnusedIdsAsWarningOnly()
    {
        var snapshot = ChangeVectorSemantics.CreateSnapshot(
            [
                new ChangeVectorSemanticsNodeInfo
                {
                    NodeUrl = "A",
                    Label = "Node A",
                    DatabaseId = ConsistencyScenarioData.CurrentDbIdA,
                    DatabaseChangeVector = $"A:7-{ConsistencyScenarioData.CurrentDbIdA},TRXN:3-{ConsistencyScenarioData.LegacyPotentialDbId}"
                },
                new ChangeVectorSemanticsNodeInfo
                {
                    NodeUrl = "B",
                    Label = "Node B",
                    DatabaseId = ConsistencyScenarioData.CurrentDbIdB,
                    DatabaseChangeVector = $"B:7-{ConsistencyScenarioData.CurrentDbIdB},RAFT:3-{ConsistencyScenarioData.LegacyPotentialDbId}"
                },
                new ChangeVectorSemanticsNodeInfo
                {
                    NodeUrl = "C",
                    Label = "Node C",
                    DatabaseId = ConsistencyScenarioData.CurrentDbIdC,
                    DatabaseChangeVector = $"C:7-{ConsistencyScenarioData.CurrentDbIdC}"
                }
            ],
            explicitUnusedDatabaseIds: []);

        var potential = Assert.Single(snapshot.PotentialUnusedDatabaseIds);
        Assert.Equal(ConsistencyScenarioData.LegacyPotentialDbId, potential);
    }

    [Fact]
    public void PotentialUnusedIds_DoNotAffectDecisionLogic()
    {
        var semanticsSnapshot = ChangeVectorSemantics.CreateSnapshot(
            [
                new ChangeVectorSemanticsNodeInfo
                {
                    NodeUrl = "A",
                    Label = "Node A",
                    DatabaseId = ConsistencyScenarioData.CurrentDbIdA,
                    DatabaseChangeVector = $"A:7-{ConsistencyScenarioData.CurrentDbIdA},TRXN:3-{ConsistencyScenarioData.LegacyPotentialDbId}"
                },
                new ChangeVectorSemanticsNodeInfo
                {
                    NodeUrl = "B",
                    Label = "Node B",
                    DatabaseId = ConsistencyScenarioData.CurrentDbIdB,
                    DatabaseChangeVector = $"B:7-{ConsistencyScenarioData.CurrentDbIdB},RAFT:3-{ConsistencyScenarioData.LegacyPotentialDbId}"
                },
                new ChangeVectorSemanticsNodeInfo
                {
                    NodeUrl = "C",
                    Label = "Node C",
                    DatabaseId = ConsistencyScenarioData.CurrentDbIdC,
                    DatabaseChangeVector = $"C:7-{ConsistencyScenarioData.CurrentDbIdC}"
                }
            ],
            explicitUnusedDatabaseIds: []);

        var evaluation = ConsistencyDecisionEngine.EvaluateDocument(
            "docs/potential-unused-warning",
            ConsistencyScenarioData.BuildSnapshot(
                $"A:7-{ConsistencyScenarioData.CurrentDbIdA},TRXN:3-{ConsistencyScenarioData.LegacyPotentialDbId}",
                $"A:7-{ConsistencyScenarioData.CurrentDbIdA}",
                null),
            semanticsSnapshot);

        Assert.NotNull(evaluation);
        Assert.Equal("MISSING", evaluation.MismatchType);
        Assert.Equal("A", evaluation.WinnerNode);
    }
}