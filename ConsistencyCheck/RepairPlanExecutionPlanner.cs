namespace ConsistencyCheck;

internal static class RepairPlanExecutionPlanner
{
    public static IReadOnlyList<RepairDocument> ExpandToSingleDocumentPlans(
        IReadOnlyCollection<RepairDocument> groupedPlans)
    {
        return groupedPlans
            .SelectMany(group =>
                group.DocumentIds
                    .Where(documentId => string.IsNullOrWhiteSpace(documentId) == false)
                    .Distinct(StringComparer.OrdinalIgnoreCase)
                    .Select(documentId => new RepairDocument
                    {
                        Id = StateStore.CreateRepairPlanDocumentItemId(group.Id, documentId),
                        RunId = group.RunId,
                        DocumentIds = [documentId],
                        Collection = group.Collection,
                        WinnerNode = group.WinnerNode,
                        WinnerCV = group.WinnerCV,
                        AffectedNodes = group.AffectedNodes
                            .Distinct(StringComparer.OrdinalIgnoreCase)
                            .OrderBy(node => node, StringComparer.OrdinalIgnoreCase)
                            .ToList(),
                        SourceRepairPlanId = group.Id,
                        SourceRepairPlanRunId = group.RunId,
                        RepairStatus = group.RepairStatus,
                        CompletedAt = group.CompletedAt,
                        Error = group.Error
                    }))
            .ToList();
    }

    public static bool ObservedStatesMatch(
        IReadOnlyCollection<NodeObservedState> expected,
        IReadOnlyCollection<NodeObservedState> actual,
        IReadOnlyList<NodeConfig> configuredNodes,
        ChangeVectorSemanticsSnapshot? semanticsSnapshot)
    {
        var ignoredDatabaseIds = semanticsSnapshot?.ExplicitUnusedDatabaseIdSet;
        var expectedByNode = expected.ToDictionary(
            state => state.NodeUrl,
            state => state,
            StringComparer.OrdinalIgnoreCase);
        var actualByNode = actual.ToDictionary(
            state => state.NodeUrl,
            state => state,
            StringComparer.OrdinalIgnoreCase);

        foreach (var node in configuredNodes)
        {
            var expectedState = expectedByNode.GetValueOrDefault(node.Url) ?? new NodeObservedState
            {
                NodeUrl = node.Url,
                Present = false
            };
            var actualState = actualByNode.GetValueOrDefault(node.Url) ?? new NodeObservedState
            {
                NodeUrl = node.Url,
                Present = false
            };

            if (expectedState.Present != actualState.Present)
                return false;

            if (string.Equals(expectedState.Collection, actualState.Collection, StringComparison.OrdinalIgnoreCase) == false)
                return false;

            var expectedNormalizedCv = ConsistencyDecisionEngine.NormalizeChangeVector(
                expectedState.ChangeVector,
                ignoredDatabaseIds);
            var actualNormalizedCv = ConsistencyDecisionEngine.NormalizeChangeVector(
                actualState.ChangeVector,
                ignoredDatabaseIds);

            if (string.Equals(expectedNormalizedCv, actualNormalizedCv, StringComparison.OrdinalIgnoreCase) == false)
                return false;
        }

        return true;
    }
}
