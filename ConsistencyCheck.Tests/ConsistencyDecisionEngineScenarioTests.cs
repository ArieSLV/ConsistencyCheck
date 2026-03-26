using Xunit;

namespace ConsistencyCheck.Tests;

public sealed class ConsistencyDecisionEngineScenarioTests
{
    [Theory]
    [MemberData(nameof(ConsistencyScenarioData.BaselineCases), MemberType = typeof(ConsistencyScenarioData))]
    public void BaselineScenarioMatrix_ProducesExpectedOutcome(ScenarioTestCase testCase)
    {
        var evaluation = ConsistencyDecisionEngine.EvaluateDocument(
            testCase.DocumentId,
            testCase.Snapshot,
            testCase.SemanticsSnapshot);

        ConsistencyScenarioAssertions.AssertMatchesExpected(testCase, evaluation);
    }
}