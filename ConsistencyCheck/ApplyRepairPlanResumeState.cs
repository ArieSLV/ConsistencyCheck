namespace ConsistencyCheck;

internal sealed class ApplyRepairPlanResumeState
{
    public required IReadOnlyDictionary<string, RepairDocument> ExecutionsBySourceRepairPlanId { get; init; }
    public required IReadOnlyList<RepairDocument> BlockedExecutions { get; init; }
    public long RepairsAttempted { get; init; }
    public long RepairsPatchedOnWinner { get; init; }
    public long RepairsFailed { get; init; }
    public int CompletedItemCount { get; init; }
}

internal static class ApplyRepairPlanExecution
{
    public const string PatchDispatching = "PatchDispatching";
    public const string PatchStarted = "PatchStarted";

    public static bool IsTerminalStatus(string status)
        => string.Equals(status, "PatchCompletedOnWinner", StringComparison.Ordinal) ||
           string.Equals(status, "SkippedStateChanged", StringComparison.Ordinal) ||
           string.Equals(status, "SkippedAlreadyConsistent", StringComparison.Ordinal) ||
           string.Equals(status, "SkippedByOperator", StringComparison.Ordinal) ||
           string.Equals(status, "PatchFailed", StringComparison.Ordinal);

    public static bool IsBlockedDispatchState(RepairDocument repair)
        => string.Equals(repair.RepairStatus, PatchDispatching, StringComparison.Ordinal) &&
           repair.PatchOperationId == null;

    public static bool IsStartedState(RepairDocument repair)
        => string.Equals(repair.RepairStatus, PatchStarted, StringComparison.Ordinal) &&
           repair.PatchOperationId != null;

    public static ApplyRepairPlanResumeState Rebuild(IReadOnlyCollection<RepairDocument> executions)
    {
        var bySourcePlanId = new Dictionary<string, RepairDocument>(StringComparer.OrdinalIgnoreCase);
        var blocked = new List<RepairDocument>();
        long repairsAttempted = 0;
        long repairsPatchedOnWinner = 0;
        long repairsFailed = 0;
        var completedItemCount = 0;

        foreach (var execution in executions)
        {
            if (string.IsNullOrWhiteSpace(execution.SourceRepairPlanId))
            {
                throw new InvalidOperationException(
                    $"Apply repair execution '{execution.Id}' is missing SourceRepairPlanId and cannot be resumed safely.");
            }

            if (bySourcePlanId.TryGetValue(execution.SourceRepairPlanId, out var existing))
            {
                throw new InvalidOperationException(
                    $"Apply repair run contains duplicate execution records for source plan '{execution.SourceRepairPlanId}': '{existing.Id}' and '{execution.Id}'.");
            }

            bySourcePlanId[execution.SourceRepairPlanId] = execution;

            if (IsBlockedDispatchState(execution))
                blocked.Add(execution);

            if (IsTerminalStatus(execution.RepairStatus) == false)
                continue;

            completedItemCount++;

            var attempted = execution.DocumentsAttempted ?? InferAttemptedCount(execution);
            var patched = execution.DocumentsPatchedOnWinner ?? InferPatchedCount(execution);
            var failed = execution.DocumentsFailed ?? InferFailedCount(execution, attempted, patched);

            repairsAttempted += attempted;
            repairsPatchedOnWinner += patched;
            repairsFailed += failed;
        }

        return new ApplyRepairPlanResumeState
        {
            ExecutionsBySourceRepairPlanId = bySourcePlanId,
            BlockedExecutions = blocked,
            RepairsAttempted = repairsAttempted,
            RepairsPatchedOnWinner = repairsPatchedOnWinner,
            RepairsFailed = repairsFailed,
            CompletedItemCount = completedItemCount
        };
    }

    private static long InferPatchedCount(RepairDocument execution)
    {
        return string.Equals(execution.RepairStatus, "PatchCompletedOnWinner", StringComparison.Ordinal)
            ? execution.DocumentIds.Count
            : 0;
    }

    private static long InferAttemptedCount(RepairDocument execution)
    {
        if (string.Equals(execution.RepairStatus, "SkippedStateChanged", StringComparison.Ordinal) ||
            string.Equals(execution.RepairStatus, "SkippedAlreadyConsistent", StringComparison.Ordinal) ||
            string.Equals(execution.RepairStatus, "SkippedByOperator", StringComparison.Ordinal))
        {
            return 0;
        }

        return execution.DocumentIds.Count;
    }

    private static long InferFailedCount(RepairDocument execution, long attempted, long patched)
    {
        if (string.Equals(execution.RepairStatus, "PatchCompletedOnWinner", StringComparison.Ordinal))
            return Math.Max(0, attempted - patched);

        if (string.Equals(execution.RepairStatus, "SkippedStateChanged", StringComparison.Ordinal) ||
            string.Equals(execution.RepairStatus, "SkippedAlreadyConsistent", StringComparison.Ordinal) ||
            string.Equals(execution.RepairStatus, "SkippedByOperator", StringComparison.Ordinal))
        {
            return 0;
        }

        return attempted;
    }
}
