namespace ConsistencyCheck.Tests;

public static class ConsistencyScenarioData
{
    public const string UnusedDbId1 = "unusedDbIdAlpha000001";
    public const string UnusedDbId2 = "unusedDbIdBeta000002";
    public const string UnusedDbId3 = "unusedDbIdGamma00003";
    public const string SharedDbId = "sharedDbId00000000001";
    public const string CurrentDbIdA = "currentDbIdNodeA00001";
    public const string CurrentDbIdB = "currentDbIdNodeB00001";
    public const string CurrentDbIdC = "currentDbIdNodeC00001";
    public const string LegacyPotentialDbId = "legacyPotentialDbId01";

    public static IEnumerable<object[]> BaselineCases()
    {
        foreach (var scenario in ScenarioMatrix())
        {
            yield return
            [
                new ScenarioTestCase(
                    CaseId: $"baseline::{scenario.Name}",
                    DocumentId: $"docs/{scenario.Name}",
                    Scenario: scenario,
                    Snapshot: BuildSnapshot(
                        ExpandMaybeMissing(scenario.A),
                        ExpandMaybeMissing(scenario.B),
                        ExpandMaybeMissing(scenario.C)),
                    SemanticsSnapshot: null)
            ];
        }
    }

    public static IEnumerable<object[]> AllPresentUnusedCases()
    {
        foreach (var scenario in ScenarioMatrix())
        {
            var baseVectors = GetExpandedVectors(scenario);
            yield return
            [
                new ScenarioTestCase(
                    CaseId: $"derived::all-present-unused::{scenario.Name}",
                    DocumentId: $"docs/{scenario.Name}/all-present-unused",
                    Scenario: scenario,
                    Snapshot: BuildSnapshot(
                        AddSegments(baseVectors.A, ("UNUSED", 91, UnusedDbId1)),
                        AddSegments(baseVectors.B, ("UNUSED", 91, UnusedDbId1)),
                        AddSegments(baseVectors.C, ("UNUSED", 91, UnusedDbId1))),
                    SemanticsSnapshot: new ChangeVectorSemanticsSnapshot
                    {
                        ExplicitUnusedDatabaseIds = [UnusedDbId1]
                    })
            ];
        }
    }

    public static IEnumerable<object[]> AsymmetricUnusedCases()
    {
        foreach (var scenario in ScenarioMatrix())
        {
            var baseVectors = GetExpandedVectors(scenario);
            yield return
            [
                new ScenarioTestCase(
                    CaseId: $"derived::asymmetric-unused::{scenario.Name}",
                    DocumentId: $"docs/{scenario.Name}/asymmetric-unused",
                    Scenario: scenario,
                    Snapshot: BuildSnapshot(
                        AddSegments(baseVectors.A, ("UNUSED", 77, UnusedDbId1)),
                        AddSegments(baseVectors.B, ("UNUSED", 77, UnusedDbId1), ("UNUSED", 88, UnusedDbId2)),
                        AddSegments(baseVectors.C, ("UNUSED", 88, UnusedDbId2))),
                    SemanticsSnapshot: new ChangeVectorSemanticsSnapshot
                    {
                        ExplicitUnusedDatabaseIds = [UnusedDbId1, UnusedDbId2]
                    })
            ];
        }
    }

    public static IEnumerable<object[]> SpecialTagsUnusedCases()
    {
        foreach (var scenario in ScenarioMatrix())
        {
            var baseVectors = GetExpandedVectors(scenario);
            yield return
            [
                new ScenarioTestCase(
                    CaseId: $"derived::special-tags-unused::{scenario.Name}",
                    DocumentId: $"docs/{scenario.Name}/special-tags-unused",
                    Scenario: scenario,
                    Snapshot: BuildSnapshot(
                        ShuffleSegments(AddSegments(baseVectors.A,
                            ("RAFT", 61, UnusedDbId2),
                            ("TRXN", 75, UnusedDbId3))),
                        ShuffleSegments(AddSegments(baseVectors.B,
                            ("SINK", 61, UnusedDbId2),
                            ("TRXN", 75, UnusedDbId3))),
                        ShuffleSegments(AddSegments(baseVectors.C,
                            ("SINK", 75, UnusedDbId3),
                            ("RAFT", 61, UnusedDbId2)))),
                    SemanticsSnapshot: new ChangeVectorSemanticsSnapshot
                    {
                        ExplicitUnusedDatabaseIds = [UnusedDbId2, UnusedDbId3]
                    })
            ];
        }
    }

    private static (string? A, string? B, string? C) GetExpandedVectors(ScenarioDefinition scenario)
        => (
            ExpandMaybeMissing(scenario.A),
            ExpandMaybeMissing(scenario.B),
            ExpandMaybeMissing(scenario.C));

    public static List<NodeObservedState> BuildSnapshot(string? a, string? b, string? c)
        => [
            CreateState("A", a),
            CreateState("B", b),
            CreateState("C", c)
        ];

    public static string? ExpandMaybeMissing(string shorthand)
    {
        if (string.Equals(shorthand, "missing", StringComparison.OrdinalIgnoreCase))
            return null;

        return string.Join(",",
            shorthand.Split(',', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries)
                .Select(segment =>
                {
                    var parts = segment.Split(':', 2);
                    return $"{parts[0]}:{parts[1]}-{parts[0].ToLowerInvariant()}";
                }));
    }

    private static string? AddSegments(string? changeVector, params (string Tag, long Etag, string DbId)[] extraSegments)
    {
        if (string.IsNullOrWhiteSpace(changeVector))
            return null;

        var segments = changeVector
            .Split(',', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries)
            .ToList();

        foreach (var (tag, etag, dbId) in extraSegments)
            segments.Add($"{tag}:{etag}-{dbId}");

        return string.Join(",", segments);
    }

    private static string? ShuffleSegments(string? changeVector)
    {
        if (string.IsNullOrWhiteSpace(changeVector))
            return null;

        return string.Join(",",
            changeVector
                .Split(',', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries)
                .OrderByDescending(static segment => segment, StringComparer.Ordinal));
    }

    private static IEnumerable<ScenarioDefinition> ScenarioMatrix()
    {
        yield return Case("only-a", "A:1,B:1,C:1", "missing", "missing", "MISSING", "A");
        yield return Case("only-b", "missing", "A:1,B:1,C:1", "missing", "MISSING", "B");
        yield return Case("only-c", "missing", "missing", "A:1,B:1,C:1", "MISSING", "C");
        yield return Case("ab-consistent-missing-c", "A:2,B:1,C:1", "A:2,B:1,C:1", "missing", "MISSING", "A");
        yield return Case("ac-consistent-missing-b", "A:2,B:1,C:1", "missing", "A:2,B:1,C:1", "MISSING", "A");
        yield return Case("bc-consistent-missing-a", "missing", "A:1,B:2,C:1", "A:1,B:2,C:1", "MISSING", "B");
        yield return Case("ab-inconsistent-a-wins-missing-c", "A:3,B:2,C:1", "A:2,B:1,C:1", "missing", "MISSING", "A");
        yield return Case("ab-inconsistent-b-wins-missing-c", "A:1,B:2,C:1", "A:2,B:3,C:1", "missing", "MISSING", "B");
        yield return Case("ac-inconsistent-a-wins-missing-b", "A:3,B:1,C:2", "missing", "A:2,B:1,C:1", "MISSING", "A");
        yield return Case("ac-inconsistent-c-wins-missing-b", "A:1,B:1,C:2", "missing", "A:2,B:1,C:3", "MISSING", "C");
        yield return Case("bc-inconsistent-b-wins-missing-a", "missing", "A:1,B:3,C:2", "A:1,B:2,C:1", "MISSING", "B");
        yield return Case("bc-inconsistent-c-wins-missing-a", "missing", "A:1,B:1,C:2", "A:1,B:2,C:3", "MISSING", "C");
        yield return Case("all-three-a-wins-one-stale-b", "A:4,B:2,C:2", "A:3,B:1,C:1", "A:4,B:2,C:2", "CV_MISMATCH", "A");
        yield return Case("all-three-a-wins-one-stale-c", "A:4,B:2,C:2", "A:4,B:2,C:2", "A:3,B:1,C:1", "CV_MISMATCH", "A");
        yield return Case("all-three-b-wins-one-stale-a", "A:1,B:3,C:1", "A:2,B:4,C:2", "A:2,B:4,C:2", "CV_MISMATCH", "B");
        yield return Case("all-three-b-wins-one-stale-c", "A:2,B:4,C:2", "A:2,B:4,C:2", "A:1,B:3,C:1", "CV_MISMATCH", "A");
        yield return Case("all-three-c-wins-one-stale-a", "A:1,B:1,C:3", "A:2,B:2,C:4", "A:2,B:2,C:4", "CV_MISMATCH", "B");
        yield return Case("all-three-c-wins-one-stale-b", "A:2,B:2,C:4", "A:1,B:1,C:3", "A:2,B:2,C:4", "CV_MISMATCH", "A");
        yield return Case("all-three-a-wins-two-stale", "A:4,B:2,C:2", "A:3,B:1,C:1", "A:3,B:1,C:1", "CV_MISMATCH", "A");
        yield return Case("all-three-b-wins-two-stale", "A:1,B:3,C:1", "A:2,B:4,C:2", "A:1,B:3,C:1", "CV_MISMATCH", "B");
        yield return Case("all-three-c-wins-two-stale", "A:1,B:1,C:3", "A:1,B:1,C:3", "A:2,B:2,C:4", "CV_MISMATCH", "C");
        yield return Case("all-three-all-different-a-wins", "A:5,B:3,C:2", "A:4,B:2,C:1", "A:3,B:1,C:1", "CV_MISMATCH", "A");
        yield return Case("all-three-all-different-b-wins", "A:3,B:1,C:1", "A:5,B:3,C:2", "A:4,B:2,C:1", "CV_MISMATCH", "B");
        yield return Case("all-three-all-different-c-wins", "A:4,B:2,C:1", "A:3,B:1,C:1", "A:5,B:3,C:2", "CV_MISMATCH", "C");
        yield return Case("all-three-tie-ab-stale-c", "A:4,B:2,C:2", "A:4,B:2,C:2", "A:3,B:1,C:1", "CV_MISMATCH", "A");
        yield return Case("all-three-tie-ac-stale-b", "A:4,B:2,C:2", "A:3,B:1,C:1", "A:4,B:2,C:2", "CV_MISMATCH", "A");
        yield return Case("all-three-tie-bc-stale-a", "A:1,B:3,C:1", "A:2,B:4,C:2", "A:2,B:4,C:2", "CV_MISMATCH", "B");
        yield return Case("ab-ambiguous-a-vs-b-missing-c", "A:3,B:1,C:1", "A:1,B:3,C:1", "missing", "AMBIGUOUS_CV", null);
        yield return Case("ac-ambiguous-a-vs-c-missing-b", "A:3,B:1,C:1", "missing", "A:1,B:1,C:3", "AMBIGUOUS_CV", null);
        yield return Case("bc-ambiguous-b-vs-c-missing-a", "missing", "A:1,B:3,C:1", "A:1,B:1,C:3", "AMBIGUOUS_CV", null);
        yield return Case("all-three-ambiguous-a-vs-b-base-c", "A:3,B:2,C:1", "A:2,B:3,C:1", "A:1,B:1,C:1", "AMBIGUOUS_CV", null);
        yield return Case("all-three-ambiguous-a-vs-c-base-b", "A:3,B:1,C:2", "A:1,B:1,C:1", "A:2,B:1,C:3", "AMBIGUOUS_CV", null);
        yield return Case("all-three-ambiguous-b-vs-c-base-a", "A:1,B:1,C:1", "A:1,B:3,C:2", "A:1,B:2,C:3", "AMBIGUOUS_CV", null);
        yield return Case("all-three-consistent", "A:2,B:2,C:2", "A:2,B:2,C:2", "A:2,B:2,C:2", null, null);
        yield return Case("all-three-mutually-ambiguous", "A:2,B:1,C:1", "A:1,B:2,C:1", "A:1,B:1,C:2", "AMBIGUOUS_CV", null);
        yield return Case("ab-consistent-ambiguous-vs-c", "A:2,B:2,C:1", "A:2,B:2,C:1", "A:1,B:1,C:2", "AMBIGUOUS_CV", null);
        yield return Case("ac-consistent-ambiguous-vs-b", "A:2,B:1,C:2", "A:1,B:2,C:1", "A:2,B:1,C:2", "AMBIGUOUS_CV", null);
        yield return Case("bc-consistent-ambiguous-vs-a", "A:2,B:1,C:1", "A:1,B:2,C:2", "A:1,B:2,C:2", "AMBIGUOUS_CV", null);
        yield return Case("a-dominates-b-ambiguous-vs-c", "A:3,B:2,C:1", "A:2,B:2,C:1", "A:1,B:1,C:3", "AMBIGUOUS_CV", null);
        yield return Case("b-dominates-a-ambiguous-vs-c", "A:2,B:2,C:1", "A:2,B:3,C:1", "A:1,B:1,C:3", "AMBIGUOUS_CV", null);
        yield return Case("a-dominates-c-ambiguous-vs-b", "A:3,B:1,C:2", "A:1,B:3,C:1", "A:2,B:1,C:2", "AMBIGUOUS_CV", null);
        yield return Case("c-dominates-a-ambiguous-vs-b", "A:2,B:1,C:2", "A:1,B:3,C:1", "A:2,B:1,C:3", "AMBIGUOUS_CV", null);
        yield return Case("b-dominates-c-ambiguous-vs-a", "A:3,B:1,C:1", "A:1,B:3,C:2", "A:1,B:2,C:2", "AMBIGUOUS_CV", null);
        yield return Case("c-dominates-b-ambiguous-vs-a", "A:3,B:1,C:1", "A:1,B:2,C:2", "A:1,B:2,C:3", "AMBIGUOUS_CV", null);
    }

    private static NodeObservedState CreateState(string nodeUrl, string? changeVector)
    {
        if (string.IsNullOrWhiteSpace(changeVector))
        {
            return new NodeObservedState
            {
                NodeUrl = nodeUrl,
                Present = false,
                Collection = null,
                ChangeVector = null,
                LastModified = null
            };
        }

        return new NodeObservedState
        {
            NodeUrl = nodeUrl,
            Present = true,
            Collection = "Users",
            ChangeVector = changeVector,
            LastModified = "2026-03-26T00:00:00.0000000Z"
        };
    }

    private static ScenarioDefinition Case(
        string name,
        string a,
        string b,
        string c,
        string? expectedMismatchType,
        string? expectedWinnerNode)
        => new(name, a, b, c, expectedMismatchType, expectedWinnerNode);
}

public sealed record ScenarioDefinition(
    string Name,
    string A,
    string B,
    string C,
    string? ExpectedMismatchType,
    string? ExpectedWinnerNode);

public sealed record ScenarioTestCase(
    string CaseId,
    string DocumentId,
    ScenarioDefinition Scenario,
    List<NodeObservedState> Snapshot,
    ChangeVectorSemanticsSnapshot? SemanticsSnapshot)
{
    public override string ToString() => CaseId;
}