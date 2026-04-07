using Xunit;

namespace ConsistencyCheck.Tests;

public sealed class MismatchDocumentIndexesTests
{
    [Fact]
    public void ResolveCurrentRepairDecision_FallsBackToOriginalDecision_WhenCurrentIsMissing()
    {
        var mismatch = CreateRepairableMismatch();
        mismatch.RepairDecision = "PatchPlannedDryRun";
        mismatch.CurrentRepairDecision = null;

        var current = MismatchDocumentIndexes.ResolveCurrentRepairDecision(mismatch);

        Assert.Equal("PatchPlannedDryRun", current);
    }

    [Theory]
    [InlineData("PatchCompletedOnWinner", false)]
    [InlineData("SkippedStateChanged", false)]
    [InlineData("SkippedAlreadyPatchedThisRun", false)]
    [InlineData("PatchPlannedDryRun", true)]
    [InlineData("PatchFailed", true)]
    public void IsRepairable_UsesCurrentRepairDecision(string currentDecision, bool expected)
    {
        var mismatch = CreateRepairableMismatch();
        mismatch.CurrentRepairDecision = currentDecision;

        var actual = MismatchDocumentIndexes.IsRepairable(mismatch);

        Assert.Equal(expected, actual);
    }

    [Fact]
    public void HasAutomaticRepairPlan_IsTrueOnlyForPatchPlannedDryRun()
    {
        var mismatch = CreateRepairableMismatch();
        mismatch.CurrentRepairDecision = "PatchPlannedDryRun";

        Assert.True(MismatchDocumentIndexes.HasAutomaticRepairPlan(mismatch));

        mismatch.CurrentRepairDecision = "PatchCompletedOnWinner";

        Assert.False(MismatchDocumentIndexes.HasAutomaticRepairPlan(mismatch));
    }

    [Fact]
    public void RequiresManualReview_IsTrueForAmbiguousCv()
    {
        var mismatch = CreateRepairableMismatch();
        mismatch.MismatchType = "AMBIGUOUS_CV";

        Assert.True(MismatchDocumentIndexes.RequiresManualReview(mismatch));
    }

    private static MismatchDocument CreateRepairableMismatch()
    {
        return new MismatchDocument
        {
            Id = "mismatches/runs/source/1",
            RunId = "runs/source",
            DocumentId = "users/1",
            Collection = "Users",
            MismatchType = "CV_MISMATCH",
            WinnerNode = "https://localhost:61739",
            WinnerCV = "A:1-db",
            RepairDecision = "PatchPlannedDryRun"
        };
    }
}
