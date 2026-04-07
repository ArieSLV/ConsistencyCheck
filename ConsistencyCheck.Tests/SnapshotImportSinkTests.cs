using Raven.Client.Documents.Operations;
using Raven.Client.Exceptions.Documents.BulkInsert;
using Xunit;

namespace ConsistencyCheck.Tests;

public sealed class SnapshotImportSinkTests
{
    [Fact]
    public async Task StoreAsync_ReplaysOnlyUnconfirmedSnapshotsAfterConfirmedProgress()
    {
        var first = Snapshot("users/1", "A");
        var second = Snapshot("users/2", "A");
        var session1 = new FakeSnapshotBulkInsertSession(
        [
            StoreAction.Succeed(first.Id),
            StoreAction.Fail(new BulkInsertAbortedException("boom"))
        ]);
        var session2 = new FakeSnapshotBulkInsertSession(
        [
            StoreAction.Succeed()
        ]);
        var factory = new FakeSnapshotBulkInsertSessionFactory(session1, session2);
        var checkpoints = new List<SnapshotImportSinkCheckpoint>();

        await using var sink = new SnapshotImportSink(
            "runs/1",
            "A",
            "https://localhost/A",
            "Node A",
            new SnapshotImportNodeState
            {
                NodeUrl = "https://localhost/A",
                NodeLabel = "Node A"
            },
            factory,
            (checkpoint, _) =>
            {
                checkpoints.Add(checkpoint);
                return Task.CompletedTask;
            },
            CreateDiagnostic);

        await sink.StoreAsync(first, CancellationToken.None);
        await sink.StoreAsync(second, CancellationToken.None);
        await sink.CompleteAsync(CancellationToken.None);

        Assert.Equal(2, factory.OpenCount);
        Assert.Equal([first.Id, second.Id], session1.StoredIds);
        Assert.Equal([second.Id], session2.StoredIds);
        Assert.Equal(2, sink.SnapshotState.SnapshotsCommitted);
        Assert.Equal(0, sink.SnapshotState.SnapshotsSkipped);
        Assert.Equal(1, sink.SnapshotState.BulkInsertRestartCount);
        Assert.Contains(checkpoints.SelectMany(checkpoint => checkpoint.Diagnostics), diagnostic => diagnostic.Kind == "NodeSnapshotBulkInsertRestarted");
    }

    [Fact]
    public async Task StoreAsync_SkipsKnownUnsafePhysicalSnapshotAfterSecondAbortWithoutProgress()
    {
        var poison = new NodeDocumentSnapshot
        {
            Id = @"users\5/A",
            OriginalDocumentId = @"users\5",
            FromNode = "A",
            NodeLabel = "Node A",
            NodeUrl = "https://localhost/A",
            ChangeVector = "A:1-db",
            Collection = "Users",
            LastModified = DateTimeOffset.Parse("2026-03-29T08:01:00Z")
        };
        var safe = Snapshot("users/2", "A");
        var session1 = new FakeSnapshotBulkInsertSession(
        [
            StoreAction.Fail(new BulkInsertAbortedException("Invalid escape char, numeric value is 53"))
        ]);
        var session2 = new FakeSnapshotBulkInsertSession(
        [
            StoreAction.Fail(new BulkInsertAbortedException("Invalid escape char, numeric value is 53"))
        ]);
        var session3 = new FakeSnapshotBulkInsertSession(
        [
            StoreAction.Succeed()
        ]);
        var factory = new FakeSnapshotBulkInsertSessionFactory(session1, session2, session3);
        var checkpoints = new List<SnapshotImportSinkCheckpoint>();

        await using var sink = new SnapshotImportSink(
            "runs/1",
            "A",
            "https://localhost/A",
            "Node A",
            new SnapshotImportNodeState
            {
                NodeUrl = "https://localhost/A",
                NodeLabel = "Node A"
            },
            factory,
            (checkpoint, _) =>
            {
                checkpoints.Add(checkpoint);
                return Task.CompletedTask;
            },
            CreateDiagnostic);

        await sink.StoreAsync(poison, CancellationToken.None);
        await sink.StoreAsync(safe, CancellationToken.None);
        await sink.CompleteAsync(CancellationToken.None);

        Assert.Equal(3, factory.OpenCount);
        Assert.Equal([poison.Id], session1.StoredIds);
        Assert.Equal([poison.Id], session2.StoredIds);
        Assert.Equal([safe.Id], session3.StoredIds);
        Assert.Equal(1, sink.SnapshotState.SnapshotsCommitted);
        Assert.Equal(1, sink.SnapshotState.SnapshotsSkipped);
        Assert.Equal(2, sink.SnapshotState.BulkInsertRestartCount);

        var skipped = checkpoints
            .SelectMany(checkpoint => checkpoint.SkippedSnapshots)
            .Single();
        Assert.Equal(poison.Id, skipped.SnapshotDocumentId);
        Assert.Equal(poison.OriginalDocumentId, skipped.OriginalDocumentId);
        Assert.Equal(2, skipped.FailureCount);
    }

    private static DiagnosticDocument CreateDiagnostic(string runId, string kind, string message, string? nodeUrl)
    {
        return new DiagnosticDocument
        {
            Id = $"{kind}/{Guid.NewGuid():N}",
            RunId = runId,
            Kind = kind,
            Message = message,
            NodeUrl = nodeUrl
        };
    }

    private static NodeDocumentSnapshot Snapshot(string originalDocumentId, string fromNode)
    {
        return NodeDocumentSnapshots.Create(
            originalDocumentId,
            fromNode,
            $"Node {fromNode}",
            $"https://localhost/{fromNode}",
            "A:1-db",
            "Users",
            DateTimeOffset.Parse("2026-03-29T08:01:00Z"));
    }

    private sealed class FakeSnapshotBulkInsertSessionFactory : ISnapshotBulkInsertSessionFactory
    {
        private readonly Queue<FakeSnapshotBulkInsertSession> _sessions;

        public FakeSnapshotBulkInsertSessionFactory(params FakeSnapshotBulkInsertSession[] sessions)
        {
            _sessions = new Queue<FakeSnapshotBulkInsertSession>(sessions);
        }

        public int OpenCount { get; private set; }

        public ISnapshotBulkInsertSession Open()
        {
            OpenCount++;
            return _sessions.Dequeue();
        }
    }

    private sealed class FakeSnapshotBulkInsertSession : ISnapshotBulkInsertSession
    {
        private readonly Queue<StoreAction> _actions;

        public FakeSnapshotBulkInsertSession(IEnumerable<StoreAction> actions)
        {
            _actions = new Queue<StoreAction>(actions);
        }

        public List<string> StoredIds { get; } = [];

        public event Action<BulkInsertProgress?>? ProgressChanged;

        public Task StoreAsync(NodeDocumentSnapshot snapshot, CancellationToken ct)
        {
            StoredIds.Add(snapshot.Id);

            var action = _actions.Count == 0 ? StoreAction.Succeed() : _actions.Dequeue();
            if (!string.IsNullOrWhiteSpace(action.ExpectedSnapshotId) &&
                !string.Equals(action.ExpectedSnapshotId, snapshot.Id, StringComparison.Ordinal))
            {
                throw new InvalidOperationException(
                    $"Expected snapshot '{action.ExpectedSnapshotId}', got '{snapshot.Id}'.");
            }

            if (!string.IsNullOrWhiteSpace(action.ProgressLastProcessedId))
            {
                ProgressChanged?.Invoke(new BulkInsertProgress
                {
                    LastProcessedId = action.ProgressLastProcessedId
                });
            }

            if (action.Exception != null)
                throw action.Exception;

            return Task.CompletedTask;
        }

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    private sealed record StoreAction(
        string? ExpectedSnapshotId,
        string? ProgressLastProcessedId,
        Exception? Exception)
    {
        public static StoreAction Succeed(string? progressLastProcessedId = null)
            => new(null, progressLastProcessedId, null);

        public static StoreAction Fail(Exception exception, string? expectedSnapshotId = null)
            => new(expectedSnapshotId, null, exception);
    }
}
