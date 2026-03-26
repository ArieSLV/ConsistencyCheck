using Xunit;

namespace ConsistencyCheck.Tests;

internal static class ConsistencyScenarioAssertions
{
    public static void AssertMatchesExpected(ScenarioTestCase testCase, DocumentEvaluation? evaluation)
    {
        var scenario = testCase.Scenario;
        var ignoredDatabaseIds = testCase.SemanticsSnapshot?.ExplicitUnusedDatabaseIdSet;

        if (scenario.ExpectedMismatchType is null)
        {
            Assert.Null(evaluation);
            return;
        }

        Assert.NotNull(evaluation);
        Assert.Equal(scenario.ExpectedMismatchType, evaluation.MismatchType);
        Assert.Equal(scenario.ExpectedWinnerNode, evaluation.WinnerNode);

        var expectedWinnerCv = scenario.ExpectedWinnerNode switch
        {
            "A" => ConsistencyScenarioData.ExpandMaybeMissing(scenario.A),
            "B" => ConsistencyScenarioData.ExpandMaybeMissing(scenario.B),
            "C" => ConsistencyScenarioData.ExpandMaybeMissing(scenario.C),
            _ => null
        };

        if (expectedWinnerCv is null)
        {
            Assert.Null(evaluation.WinnerCV);
            return;
        }

        var expectedNormalized = ConsistencyDecisionEngine.NormalizeChangeVector(expectedWinnerCv, ignoredDatabaseIds);
        var actualNormalized = ConsistencyDecisionEngine.NormalizeChangeVector(evaluation.WinnerCV, ignoredDatabaseIds);

        Assert.Equal(expectedNormalized, actualNormalized);
    }
}