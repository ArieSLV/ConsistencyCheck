namespace ConsistencyCheck;

internal sealed class DocumentEvaluation
{
    public string DocumentId { get; init; } = string.Empty;
    public string? Collection { get; init; }
    public string MismatchType { get; init; } = string.Empty;
    public string? WinnerNode { get; init; }
    public string? WinnerCV { get; init; }
    public List<NodeObservedState> ObservedState { get; init; } = [];
    public List<string> AffectedNodes { get; init; } = [];
    public string RepairDecision { get; set; } = string.Empty;
}

internal static class ConsistencyDecisionEngine
{
    public static DocumentEvaluation? EvaluateDocument(
        string documentId,
        List<NodeObservedState> snapshot,
        ChangeVectorSemanticsSnapshot? semanticsSnapshot = null)
    {
        var ignoredDatabaseIds = semanticsSnapshot?.ExplicitUnusedDatabaseIdSet;
        var presentStates = snapshot.Where(state => state.Present).ToList();
        if (presentStates.Count == 0)
            return null;

        var collections = presentStates
            .Select(state => state.Collection)
            .Where(collection => !string.IsNullOrWhiteSpace(collection))
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToList();

        var winner = DetermineWinner(snapshot, semanticsSnapshot);
        var hasMissingState = snapshot.Any(state => !state.Present);

        string mismatchType;
        if (collections.Count > 1)
        {
            mismatchType = "COLLECTION_MISMATCH";
        }
        else if (!hasMissingState && presentStates
                     .Select(state => NormalizeChangeVector(state.ChangeVector, ignoredDatabaseIds))
                     .Distinct(StringComparer.OrdinalIgnoreCase)
                     .Count() == 1)
        {
            return null;
        }
        else if (winner == null)
        {
            mismatchType = "AMBIGUOUS_CV";
        }
        else
        {
            mismatchType = hasMissingState ? "MISSING" : "CV_MISMATCH";
        }

        var winnerNode = winner?.NodeUrl;
        var winnerCv = winner?.ChangeVector;
        var winnerNormalizedCv = winner is null
            ? null
            : NormalizeChangeVector(winner.ChangeVector, ignoredDatabaseIds);

        var affectedNodes = snapshot
            .Where(state =>
                winner == null ||
                !string.Equals(state.NodeUrl, winner.NodeUrl, StringComparison.OrdinalIgnoreCase) ||
                !state.Present ||
                !string.Equals(
                    NormalizeChangeVector(state.ChangeVector, ignoredDatabaseIds),
                    winnerNormalizedCv,
                    StringComparison.OrdinalIgnoreCase))
            .Select(state => state.NodeUrl)
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .OrderBy(node => node, StringComparer.OrdinalIgnoreCase)
            .ToList();

        return new DocumentEvaluation
        {
            DocumentId = documentId,
            Collection = collections.Count == 1 ? collections[0] : presentStates.FirstOrDefault()?.Collection,
            MismatchType = mismatchType,
            WinnerNode = winnerNode,
            WinnerCV = winnerCv,
            ObservedState = snapshot.Select(CloneObservedState).ToList(),
            AffectedNodes = affectedNodes
        };
    }

    public static NodeObservedState? DetermineWinner(
        List<NodeObservedState> snapshot,
        ChangeVectorSemanticsSnapshot? semanticsSnapshot = null)
    {
        var ignoredDatabaseIds = semanticsSnapshot?.ExplicitUnusedDatabaseIdSet;
        var presentStates = snapshot.Where(state => state.Present).ToList();
        if (presentStates.Count == 0)
            return null;

        var groups = presentStates
            .GroupBy(
                state => NormalizeChangeVector(state.ChangeVector, ignoredDatabaseIds),
                StringComparer.OrdinalIgnoreCase)
            .Select(group => new VectorGroup(
                group.Key,
                ParseChangeVector(group.First().ChangeVector, ignoredDatabaseIds),
                group.OrderBy(state => state.NodeUrl, StringComparer.OrdinalIgnoreCase).ToList()))
            .ToList();

        if (groups.Count == 1)
            return groups[0].States[0];

        VectorGroup? dominantGroup = null;
        foreach (var group in groups)
        {
            var dominatesAll = true;
            foreach (var other in groups)
            {
                if (ReferenceEquals(group, other))
                    continue;

                if (!Dominates(group.ParsedVector, other.ParsedVector))
                {
                    dominatesAll = false;
                    break;
                }
            }

            if (!dominatesAll)
                continue;

            if (dominantGroup != null)
                return null;

            dominantGroup = group;
        }

        return dominantGroup?.States[0];
    }

    public static Dictionary<string, long> ParseChangeVector(
        string? changeVector,
        IReadOnlySet<string>? ignoredDatabaseIds = null)
    {
        var parsed = new Dictionary<string, long>(StringComparer.Ordinal);
        if (string.IsNullOrWhiteSpace(changeVector))
            return parsed;

        foreach (var segment in changeVector.Split(',', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries))
        {
            var colonIndex = segment.IndexOf(':');
            if (colonIndex <= 0)
                continue;

            var dashIndex = segment.IndexOf('-', colonIndex);
            if (dashIndex <= colonIndex)
                continue;

            var databaseId = segment[(dashIndex + 1)..];
            if (ignoredDatabaseIds != null && ignoredDatabaseIds.Contains(databaseId))
                continue;

            var etagText = segment[(colonIndex + 1)..dashIndex];

            if (long.TryParse(etagText, out var etag) == false)
                continue;

            if (parsed.TryGetValue(databaseId, out var existingEtag))
                parsed[databaseId] = Math.Max(existingEtag, etag);
            else
                parsed[databaseId] = etag;
        }

        return parsed;
    }

    public static string NormalizeChangeVector(
        string? changeVector,
        IReadOnlySet<string>? ignoredDatabaseIds = null)
    {
        var parsed = ParseChangeVector(changeVector, ignoredDatabaseIds);
        if (parsed.Count == 0)
            return string.Empty;

        return string.Join(",",
            parsed
                .OrderBy(entry => entry.Key, StringComparer.Ordinal)
                .Select(entry => $"{entry.Key}:{entry.Value}"));
    }

    public static bool Dominates(
        Dictionary<string, long> candidate,
        Dictionary<string, long> other)
    {
        var strictlyGreater = false;

        foreach (var (segment, otherEtag) in other)
        {
            if (!candidate.TryGetValue(segment, out var candidateEtag))
                return false;

            if (candidateEtag < otherEtag)
                return false;

            if (candidateEtag > otherEtag)
                strictlyGreater = true;
        }

        if (!strictlyGreater)
        {
            foreach (var segment in candidate.Keys)
            {
                if (!other.ContainsKey(segment))
                {
                    strictlyGreater = true;
                    break;
                }
            }
        }

        return strictlyGreater || candidate.Count == other.Count;
    }

    private static NodeObservedState CloneObservedState(NodeObservedState state)
    {
        return new NodeObservedState
        {
            NodeUrl = state.NodeUrl,
            Present = state.Present,
            Collection = state.Collection,
            ChangeVector = state.ChangeVector,
            LastModified = state.LastModified
        };
    }

    private sealed record VectorGroup(
        string NormalizedChangeVector,
        Dictionary<string, long> ParsedVector,
        List<NodeObservedState> States);
}
