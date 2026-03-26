using Xunit;

namespace ConsistencyCheck.Tests;

public sealed class ChangeVectorSemanticsSnapshotTests
{
    [Fact]
    public async Task ExistingRun_ReusesPersistedSnapshotWithoutResolvingOrPersisting()
    {
        var persistedSnapshot = new ChangeVectorSemanticsSnapshot
        {
            ExplicitUnusedDatabaseIds = [ConsistencyScenarioData.UnusedDbId1],
            PotentialUnusedDatabaseIds = [ConsistencyScenarioData.UnusedDbId2]
        };
        var alternateSnapshot = new ChangeVectorSemanticsSnapshot
        {
            ExplicitUnusedDatabaseIds = [ConsistencyScenarioData.UnusedDbId3]
        };
        var run = new RunStateDocument
        {
            RunId = "runs/existing",
            ChangeVectorSemanticsSnapshot = persistedSnapshot
        };

        var resolverInvoked = 0;
        var persistInvoked = 0;

        var snapshot = await ChangeVectorSemantics.EnsureSnapshotAsync(
            run,
            _ =>
            {
                resolverInvoked++;
                return Task.FromResult(alternateSnapshot);
            },
            (_, _) =>
            {
                persistInvoked++;
                return Task.CompletedTask;
            },
            CancellationToken.None);

        Assert.Same(persistedSnapshot, snapshot);
        Assert.Equal(0, resolverInvoked);
        Assert.Equal(0, persistInvoked);
    }

    [Fact]
    public async Task LegacyRun_ResolvesAndPersistsSnapshotExactlyOnce()
    {
        var resolvedSnapshot = new ChangeVectorSemanticsSnapshot
        {
            ExplicitUnusedDatabaseIds = [ConsistencyScenarioData.UnusedDbId3]
        };
        var run = new RunStateDocument
        {
            RunId = "runs/legacy"
        };

        var resolverInvoked = 0;
        var persistInvoked = 0;

        var snapshot = await ChangeVectorSemantics.EnsureSnapshotAsync(
            run,
            _ =>
            {
                resolverInvoked++;
                return Task.FromResult(resolvedSnapshot);
            },
            (persistedRun, _) =>
            {
                persistInvoked++;
                Assert.Same(resolvedSnapshot, persistedRun.ChangeVectorSemanticsSnapshot);
                return Task.CompletedTask;
            },
            CancellationToken.None);

        Assert.Same(resolvedSnapshot, snapshot);
        Assert.Same(resolvedSnapshot, run.ChangeVectorSemanticsSnapshot);
        Assert.Equal(1, resolverInvoked);
        Assert.Equal(1, persistInvoked);
    }
}