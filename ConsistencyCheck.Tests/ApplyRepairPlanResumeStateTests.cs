using Xunit;

namespace ConsistencyCheck.Tests;

public sealed class ApplyRepairPlanResumeStateTests
{
    [Fact]
    public void Rebuild_UsesPersistedCountersForTerminalExecutions()
    {
        var state = ApplyRepairPlanExecution.Rebuild(
        [
            Execution(
                sourceRepairPlanId: "repairs/source-run/1",
                repairStatus: "PatchCompletedOnWinner",
                patchOperationId: 11,
                documentsAttempted: 3,
                documentsPatchedOnWinner: 2,
                documentsFailed: 1),
            Execution(
                sourceRepairPlanId: "repairs/source-run/2",
                repairStatus: "PatchFailed",
                patchOperationId: 12,
                documentsAttempted: 2,
                documentsPatchedOnWinner: 0,
                documentsFailed: 2)
        ]);

        Assert.Equal(5, state.RepairsAttempted);
        Assert.Equal(2, state.RepairsPatchedOnWinner);
        Assert.Equal(3, state.RepairsFailed);
        Assert.Equal(2, state.CompletedItemCount);
        Assert.Empty(state.BlockedExecutions);
    }

    [Fact]
    public void Rebuild_TreatsPatchStartedWithOperationIdAsResumableAndNotCompleted()
    {
        var execution = Execution(
            sourceRepairPlanId: "repairs/source-run/1",
            repairStatus: ApplyRepairPlanExecution.PatchStarted,
            patchOperationId: 42);

        var state = ApplyRepairPlanExecution.Rebuild([execution]);

        Assert.True(state.ExecutionsBySourceRepairPlanId.ContainsKey(execution.SourceRepairPlanId!));
        Assert.Equal(0, state.CompletedItemCount);
        Assert.Equal(0, state.RepairsAttempted);
        Assert.Empty(state.BlockedExecutions);
    }

    [Fact]
    public void Rebuild_DoesNotTreatPatchStartedWithoutOperationIdAsCompleted()
    {
        var execution = Execution(
            sourceRepairPlanId: "repairs/source-run/1",
            repairStatus: ApplyRepairPlanExecution.PatchStarted,
            patchOperationId: null);

        var state = ApplyRepairPlanExecution.Rebuild([execution]);

        Assert.True(state.ExecutionsBySourceRepairPlanId.ContainsKey(execution.SourceRepairPlanId!));
        Assert.Equal(0, state.CompletedItemCount);
        Assert.Empty(state.BlockedExecutions);
    }

    [Fact]
    public void Rebuild_TreatsPatchDispatchingWithoutOperationIdAsBlocked()
    {
        var execution = Execution(
            sourceRepairPlanId: "repairs/source-run/1",
            repairStatus: ApplyRepairPlanExecution.PatchDispatching,
            patchOperationId: null);

        var state = ApplyRepairPlanExecution.Rebuild([execution]);

        Assert.Single(state.BlockedExecutions);
        Assert.Same(execution, state.BlockedExecutions[0]);
        Assert.Equal(0, state.CompletedItemCount);
    }

    [Fact]
    public void Rebuild_ThrowsWhenDuplicateSourceRepairPlanIdsExist()
    {
        var first = Execution(sourceRepairPlanId: "repairs/source-run/1", repairStatus: "PatchFailed", patchOperationId: 1);
        var second = Execution(sourceRepairPlanId: "repairs/source-run/1", repairStatus: "PatchCompletedOnWinner", patchOperationId: 2);

        var ex = Assert.Throws<InvalidOperationException>(() => ApplyRepairPlanExecution.Rebuild([first, second]));

        Assert.Contains("duplicate execution records", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void CreateApplyRepairExecutionId_IsDeterministicPerRunAndSourcePlan()
    {
        var first = StateStore.CreateApplyRepairExecutionId("runs/20260329-1200000", "repairs/source-run/1");
        var second = StateStore.CreateApplyRepairExecutionId("runs/20260329-1200000", "repairs/source-run/1");
        var third = StateStore.CreateApplyRepairExecutionId("runs/20260329-1200000", "repairs/source-run/2");

        Assert.Equal(first, second);
        Assert.NotEqual(first, third);
        Assert.StartsWith("repairs/runs/20260329-1200000/", first, StringComparison.Ordinal);
    }

    [Fact]
    public void Rebuild_FallsBackToLegacyTerminalCountsWhenPerDocumentCountersAreMissing()
    {
        var state = ApplyRepairPlanExecution.Rebuild(
        [
            Execution(
                sourceRepairPlanId: "repairs/source-run/1",
                repairStatus: "PatchCompletedOnWinner",
                patchOperationId: 11,
                documentIds: ["users/1", "users/2"]),
            Execution(
                sourceRepairPlanId: "repairs/source-run/2",
                repairStatus: "SkippedStateChanged",
                patchOperationId: 12,
                documentIds: ["users/3"])
        ]);

        Assert.Equal(2, state.RepairsAttempted);
        Assert.Equal(2, state.RepairsPatchedOnWinner);
        Assert.Equal(0, state.RepairsFailed);
    }

    private static RepairDocument Execution(
        string sourceRepairPlanId,
        string repairStatus,
        long? patchOperationId,
        long? documentsAttempted = null,
        long? documentsPatchedOnWinner = null,
        long? documentsFailed = null,
        List<string>? documentIds = null)
    {
        return new RepairDocument
        {
            Id = $"repairs/runs/apply/{Guid.NewGuid():N}",
            RunId = "runs/apply",
            SourceRepairPlanId = sourceRepairPlanId,
            SourceRepairPlanRunId = "runs/source-plan",
            RepairStatus = repairStatus,
            PatchOperationId = patchOperationId,
            WinnerNode = "https://localhost:61739",
            DocumentIds = documentIds ?? ["users/1"],
            DocumentsAttempted = documentsAttempted,
            DocumentsPatchedOnWinner = documentsPatchedOnWinner,
            DocumentsFailed = documentsFailed,
            CompletedAt = DateTimeOffset.Parse("2026-03-29T12:00:00Z")
        };
    }
}
