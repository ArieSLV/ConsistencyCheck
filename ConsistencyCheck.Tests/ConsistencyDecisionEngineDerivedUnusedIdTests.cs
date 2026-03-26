using Xunit;

namespace ConsistencyCheck.Tests;

public sealed class ConsistencyDecisionEngineDerivedUnusedIdTests
{
    [Theory]
    [MemberData(nameof(ConsistencyScenarioData.AllPresentUnusedCases), MemberType = typeof(ConsistencyScenarioData))]
    public void AllPresentUnusedMatrix_ProducesExpectedOutcome(ScenarioTestCase testCase)
    {
        var evaluation = ConsistencyDecisionEngine.EvaluateDocument(
            testCase.DocumentId,
            testCase.Snapshot,
            testCase.SemanticsSnapshot);

        ConsistencyScenarioAssertions.AssertMatchesExpected(testCase, evaluation);
    }

    [Theory]
    [MemberData(nameof(ConsistencyScenarioData.AsymmetricUnusedCases), MemberType = typeof(ConsistencyScenarioData))]
    public void AsymmetricUnusedMatrix_ProducesExpectedOutcome(ScenarioTestCase testCase)
    {
        var evaluation = ConsistencyDecisionEngine.EvaluateDocument(
            testCase.DocumentId,
            testCase.Snapshot,
            testCase.SemanticsSnapshot);

        ConsistencyScenarioAssertions.AssertMatchesExpected(testCase, evaluation);
    }

    [Theory]
    [MemberData(nameof(ConsistencyScenarioData.SpecialTagsUnusedCases), MemberType = typeof(ConsistencyScenarioData))]
    public void SpecialTagsUnusedMatrix_ProducesExpectedOutcome(ScenarioTestCase testCase)
    {
        var evaluation = ConsistencyDecisionEngine.EvaluateDocument(
            testCase.DocumentId,
            testCase.Snapshot,
            testCase.SemanticsSnapshot);

        ConsistencyScenarioAssertions.AssertMatchesExpected(testCase, evaluation);
    }
}