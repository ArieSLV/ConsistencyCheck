using Raven.Client.Documents.BulkInsert;
using Raven.Client.Documents.Operations;
using Raven.Client.Exceptions.Documents.BulkInsert;

namespace ConsistencyCheck;

internal sealed record SnapshotImportSinkCheckpoint(
    SnapshotImportNodeState NodeState,
    IReadOnlyCollection<SnapshotImportSkippedDocument> SkippedSnapshots,
    IReadOnlyCollection<DiagnosticDocument> Diagnostics,
    bool ForcePersist);

internal interface ISnapshotBulkInsertSessionFactory
{
    ISnapshotBulkInsertSession Open();
}

internal interface ISnapshotBulkInsertSession : IAsyncDisposable
{
    event Action<BulkInsertProgress?>? ProgressChanged;

    Task StoreAsync(NodeDocumentSnapshot snapshot, CancellationToken ct);
}

internal sealed class StateStoreSnapshotBulkInsertSessionFactory : ISnapshotBulkInsertSessionFactory
{
    private readonly StateStore _stateStore;

    public StateStoreSnapshotBulkInsertSessionFactory(StateStore stateStore)
    {
        _stateStore = stateStore;
    }

    public ISnapshotBulkInsertSession Open() => new RavenSnapshotBulkInsertSession(_stateStore.OpenSnapshotBulkInsert());
}

internal sealed class RavenSnapshotBulkInsertSession : ISnapshotBulkInsertSession
{
    private readonly BulkInsertOperation _bulkInsert;

    public RavenSnapshotBulkInsertSession(BulkInsertOperation bulkInsert)
    {
        _bulkInsert = bulkInsert;
        _bulkInsert.OnProgress += HandleProgress;
    }

    public event Action<BulkInsertProgress?>? ProgressChanged;

    public Task StoreAsync(NodeDocumentSnapshot snapshot, CancellationToken ct)
        => _bulkInsert.StoreAsync(snapshot, snapshot.Id);

    public async ValueTask DisposeAsync()
    {
        _bulkInsert.OnProgress -= HandleProgress;
        await _bulkInsert.DisposeAsync().ConfigureAwait(false);
    }

    private void HandleProgress(object? sender, Raven.Client.Documents.Session.BulkInsertOnProgressEventArgs args)
        => ProgressChanged?.Invoke(args.Progress);
}

internal sealed class SnapshotImportSink : IAsyncDisposable
{
    private const long PersistCommittedThreshold = 100_000;
    private const int MaxSessionRestartAttemptsWithoutHead = 3;

    private readonly string _runId;
    private readonly string _nodeAlias;
    private readonly string _nodeUrl;
    private readonly string _nodeLabel;
    private readonly ISnapshotBulkInsertSessionFactory _sessionFactory;
    private readonly Func<SnapshotImportSinkCheckpoint, CancellationToken, Task> _checkpointAsync;
    private readonly Func<string, string, string, string?, DiagnosticDocument> _diagnosticFactory;
    private readonly object _sync = new();
    private readonly Queue<NodeDocumentSnapshot> _unconfirmed = new();
    private readonly SnapshotImportNodeState _state;

    private ISnapshotBulkInsertSession? _session;
    private int _sessionGeneration;
    private long _committedSinceLastPersist;
    private string? _lastObservedProgressId;
    private string? _lastFailedHeadSnapshotId;
    private string? _lastFailedConfirmedSnapshotId;
    private int _emptyQueueRestartAttempts;
    private bool _disposed;

    public SnapshotImportSink(
        string runId,
        string nodeAlias,
        string nodeUrl,
        string nodeLabel,
        SnapshotImportNodeState initialState,
        ISnapshotBulkInsertSessionFactory sessionFactory,
        Func<SnapshotImportSinkCheckpoint, CancellationToken, Task> checkpointAsync,
        Func<string, string, string, string?, DiagnosticDocument> diagnosticFactory)
    {
        _runId = runId;
        _nodeAlias = nodeAlias;
        _nodeUrl = nodeUrl;
        _nodeLabel = nodeLabel;
        _sessionFactory = sessionFactory;
        _checkpointAsync = checkpointAsync;
        _diagnosticFactory = diagnosticFactory;
        _state = CloneState(initialState);
        _state.NodeUrl = nodeUrl;
        _state.NodeLabel = nodeLabel;
        _state.LastUpdatedAt ??= DateTimeOffset.UtcNow;
    }

    public SnapshotImportNodeState SnapshotState
    {
        get
        {
            lock (_sync)
                return CloneState(_state);
        }
    }

    public async Task StoreAsync(NodeDocumentSnapshot snapshot, CancellationToken ct)
    {
        ThrowIfDisposed();
        ct.ThrowIfCancellationRequested();

        await EnsureSessionAsync().ConfigureAwait(false);

        lock (_sync)
        {
            _unconfirmed.Enqueue(snapshot);
            _state.LastUpdatedAt = DateTimeOffset.UtcNow;
        }

        try
        {
            await _session!.StoreAsync(snapshot, ct).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex) when (IsRecoverableBulkInsertFailure(ex))
        {
            await RecoverFromBulkInsertFailureAsync(ex, ct).ConfigureAwait(false);
        }

        await FlushCheckpointIfNeededAsync(forcePersist: false, [], [], CancellationToken.None).ConfigureAwait(false);
    }

    public async Task CompleteAsync(CancellationToken ct)
    {
        ThrowIfDisposed();

        while (true)
        {
            try
            {
                await DisposeCurrentSessionAsync().ConfigureAwait(false);
                MarkAllPendingAsCommitted();
                await FlushCheckpointIfNeededAsync(forcePersist: true, [], [], CancellationToken.None).ConfigureAwait(false);
                return;
            }
            catch (Exception ex) when (IsRecoverableBulkInsertFailure(ex))
            {
                await RecoverFromBulkInsertFailureAsync(ex, ct).ConfigureAwait(false);
            }
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed)
            return;

        _disposed = true;
        await DisposeCurrentSessionIgnoringErrorsAsync().ConfigureAwait(false);
    }

    private async Task RecoverFromBulkInsertFailureAsync(Exception ex, CancellationToken ct)
    {
        var currentFailure = ex;

        while (true)
        {
            await DisposeCurrentSessionIgnoringErrorsAsync().ConfigureAwait(false);

            string? confirmedSnapshotId;
            NodeDocumentSnapshot? headSnapshot;
            lock (_sync)
            {
                PruneConfirmedSnapshots_NoLock();
                confirmedSnapshotId = _state.LastConfirmedSnapshotId;
                headSnapshot = _unconfirmed.Count > 0 ? _unconfirmed.Peek() : null;
            }

            if (headSnapshot == null)
            {
                _emptyQueueRestartAttempts++;
                if (_emptyQueueRestartAttempts > MaxSessionRestartAttemptsWithoutHead)
                {
                    throw new InvalidOperationException(
                        $"Snapshot import for node '{_nodeLabel}' could not reopen the local bulk insert session after {MaxSessionRestartAttemptsWithoutHead} retries without any unconfirmed snapshot head.",
                        currentFailure);
                }

                var restartDiagnostics = RegisterRestart(currentFailure, confirmedSnapshotId, headSnapshot);
                await EnsureSessionAsync().ConfigureAwait(false);
                await FlushCheckpointIfNeededAsync(forcePersist: true, [], restartDiagnostics, CancellationToken.None).ConfigureAwait(false);
                return;
            }

            _emptyQueueRestartAttempts = 0;

            if (string.Equals(_lastFailedHeadSnapshotId, headSnapshot.Id, StringComparison.Ordinal) &&
                string.Equals(_lastFailedConfirmedSnapshotId, confirmedSnapshotId, StringComparison.Ordinal))
            {
                if (CanSafelyAutoSkipKnownPoisonHead(headSnapshot, currentFailure) == false)
                {
                    throw new InvalidOperationException(
                        $"Cannot safely auto-skip snapshot '{headSnapshot.Id}' for node '{_nodeLabel}'. The repeated local bulk-insert failure occurred without confirmed progress advancement, but the current queue head is not a known bulk-insert-unsafe snapshot id. The real poison snapshot may be later in the pending replay queue.",
                        currentFailure);
                }

                var skippedSnapshot = SkipHeadSnapshot(headSnapshot, currentFailure, confirmedSnapshotId);
                var diagnostics = new[]
                {
                    _diagnosticFactory(
                        _runId,
                        "NodeSnapshotSkipped",
                        $"Skipped poison snapshot '{headSnapshot.Id}' for node '{_nodeLabel}' after one replay retry without confirmed progress advancement. Last confirmed snapshot before skip: '{confirmedSnapshotId ?? "<none>"}'. Error: {currentFailure.Message}",
                        _nodeUrl)
                };

                _lastFailedHeadSnapshotId = null;
                _lastFailedConfirmedSnapshotId = null;

                var restartDiagnostics = RegisterRestart(currentFailure, confirmedSnapshotId, headSnapshot);
                await EnsureSessionAsync().ConfigureAwait(false);
                await FlushCheckpointIfNeededAsync(
                        forcePersist: true,
                        [skippedSnapshot],
                        restartDiagnostics.Concat(diagnostics).ToArray(),
                        CancellationToken.None)
                    .ConfigureAwait(false);

                try
                {
                    await ReplayPendingQueueAsync(ct).ConfigureAwait(false);
                    return;
                }
                catch (Exception replayEx) when (IsRecoverableBulkInsertFailure(replayEx))
                {
                    currentFailure = replayEx;
                    continue;
                }
            }

            _lastFailedHeadSnapshotId = headSnapshot.Id;
            _lastFailedConfirmedSnapshotId = confirmedSnapshotId;

            var diagnosticsForRestart = RegisterRestart(currentFailure, confirmedSnapshotId, headSnapshot);
            await EnsureSessionAsync().ConfigureAwait(false);
            await FlushCheckpointIfNeededAsync(forcePersist: true, [], diagnosticsForRestart, CancellationToken.None).ConfigureAwait(false);

            try
            {
                await ReplayPendingQueueAsync(ct).ConfigureAwait(false);
                return;
            }
            catch (Exception replayEx) when (IsRecoverableBulkInsertFailure(replayEx))
            {
                currentFailure = replayEx;
            }
        }
    }

    private async Task ReplayPendingQueueAsync(CancellationToken ct)
    {
        foreach (var snapshot in GetPendingSnapshotReplayOrder())
        {
            ct.ThrowIfCancellationRequested();
            await _session!.StoreAsync(snapshot, ct).ConfigureAwait(false);
        }

        await FlushCheckpointIfNeededAsync(forcePersist: false, [], [], CancellationToken.None).ConfigureAwait(false);
    }

    private async Task EnsureSessionAsync()
    {
        if (_session != null)
            return;

        var generation = Interlocked.Increment(ref _sessionGeneration);
        var session = _sessionFactory.Open();
        session.ProgressChanged += progress => HandleProgress(generation, progress);
        _session = session;
    }

    private async Task DisposeCurrentSessionAsync()
    {
        if (_session == null)
            return;

        var current = _session;
        _session = null;
        await current.DisposeAsync().ConfigureAwait(false);
    }

    private async Task DisposeCurrentSessionIgnoringErrorsAsync()
    {
        if (_session == null)
            return;

        var current = _session;
        _session = null;
        try
        {
            await current.DisposeAsync().ConfigureAwait(false);
        }
        catch
        {
            // Best-effort cleanup for a broken local bulk-insert session.
        }
    }

    private void HandleProgress(int generation, BulkInsertProgress? progress)
    {
        if (progress == null || generation != Volatile.Read(ref _sessionGeneration))
            return;

        lock (_sync)
        {
            if (string.IsNullOrWhiteSpace(progress.LastProcessedId))
                return;

            _lastObservedProgressId = progress.LastProcessedId;
            PruneConfirmedSnapshots_NoLock();
        }
    }

    private void PruneConfirmedSnapshots_NoLock()
    {
        if (string.IsNullOrWhiteSpace(_lastObservedProgressId))
            return;

        if (_unconfirmed.Count == 0)
        {
            _state.LastConfirmedSnapshotId = _lastObservedProgressId;
            return;
        }

        if (_unconfirmed.Any(snapshot => string.Equals(snapshot.Id, _lastObservedProgressId, StringComparison.Ordinal)) == false)
            return;

        var pruned = 0L;
        string? lastPrunedSnapshotId = null;
        while (_unconfirmed.Count > 0)
        {
            var snapshot = _unconfirmed.Dequeue();
            pruned++;
            lastPrunedSnapshotId = snapshot.Id;

            if (string.Equals(snapshot.Id, _lastObservedProgressId, StringComparison.Ordinal))
                break;
        }

        if (pruned == 0)
            return;

        _state.SnapshotsCommitted += pruned;
        _committedSinceLastPersist += pruned;
        _state.LastConfirmedSnapshotId = lastPrunedSnapshotId;
        _state.LastUpdatedAt = DateTimeOffset.UtcNow;
    }

    private void MarkAllPendingAsCommitted()
    {
        lock (_sync)
        {
            if (_unconfirmed.Count == 0)
            {
                _state.LastUpdatedAt = DateTimeOffset.UtcNow;
                return;
            }

            var committed = _unconfirmed.Count;
            var lastCommittedId = _unconfirmed.Last().Id;
            _unconfirmed.Clear();
            _state.SnapshotsCommitted += committed;
            _committedSinceLastPersist += committed;
            _state.LastConfirmedSnapshotId = lastCommittedId;
            _state.LastUpdatedAt = DateTimeOffset.UtcNow;
        }
    }

    private SnapshotImportSkippedDocument SkipHeadSnapshot(
        NodeDocumentSnapshot headSnapshot,
        Exception failure,
        string? confirmedSnapshotId)
    {
        lock (_sync)
        {
            if (_unconfirmed.Count == 0)
                throw new InvalidOperationException("Cannot skip a poison snapshot when the unconfirmed queue is empty.");

            var removed = _unconfirmed.Dequeue();
            if (!string.Equals(removed.Id, headSnapshot.Id, StringComparison.Ordinal))
            {
                throw new InvalidOperationException(
                    $"Expected poison snapshot head '{headSnapshot.Id}', but dequeued '{removed.Id}'.");
            }

            _state.SnapshotsSkipped++;
            _state.LastSkippedSnapshotId = removed.Id;
            _state.LastError = failure.Message;
            _state.LastUpdatedAt = DateTimeOffset.UtcNow;

            return new SnapshotImportSkippedDocument
            {
                Id = StateStore.CreateSnapshotImportSkippedId(_runId, removed.Id),
                RunId = _runId,
                OriginalDocumentId = removed.OriginalDocumentId,
                SnapshotDocumentId = removed.Id,
                FromNode = removed.FromNode,
                NodeUrl = removed.NodeUrl,
                FailureCount = 2,
                LastConfirmedSnapshotIdBeforeSkip = confirmedSnapshotId,
                Error = failure.Message,
                SkippedAt = DateTimeOffset.UtcNow
            };
        }
    }

    private DiagnosticDocument[] RegisterRestart(
        Exception failure,
        string? confirmedSnapshotId,
        NodeDocumentSnapshot? headSnapshot)
    {
        lock (_sync)
        {
            _state.BulkInsertRestartCount++;
            _state.LastError = failure.Message;
            _state.LastUpdatedAt = DateTimeOffset.UtcNow;
        }

        return
        [
            _diagnosticFactory(
                _runId,
                "NodeSnapshotBulkInsertRestarted",
                $"Reopened local bulk insert for node '{_nodeLabel}' after failure. Last confirmed snapshot: '{confirmedSnapshotId ?? "<none>"}'. Current replay head: '{headSnapshot?.Id ?? "<none>"}'. Error: {failure.Message}",
                _nodeUrl)
        ];
    }

    private IReadOnlyList<NodeDocumentSnapshot> GetPendingSnapshotReplayOrder()
    {
        lock (_sync)
            return _unconfirmed.ToArray();
    }

    private async Task FlushCheckpointIfNeededAsync(
        bool forcePersist,
        IReadOnlyCollection<SnapshotImportSkippedDocument> skippedSnapshots,
        IReadOnlyCollection<DiagnosticDocument> diagnostics,
        CancellationToken ct)
    {
        SnapshotImportNodeState? stateToPersist = null;
        lock (_sync)
        {
            if (!forcePersist &&
                skippedSnapshots.Count == 0 &&
                diagnostics.Count == 0 &&
                _committedSinceLastPersist < PersistCommittedThreshold)
            {
                return;
            }

            stateToPersist = CloneState(_state);
        }

        await _checkpointAsync(
                new SnapshotImportSinkCheckpoint(stateToPersist, skippedSnapshots, diagnostics, forcePersist),
                ct)
            .ConfigureAwait(false);

        lock (_sync)
            _committedSinceLastPersist = 0;
    }

    private void ThrowIfDisposed()
    {
        if (_disposed)
            throw new ObjectDisposedException(nameof(SnapshotImportSink));
    }

    private static SnapshotImportNodeState CloneState(SnapshotImportNodeState state)
    {
        return new SnapshotImportNodeState
        {
            NodeUrl = state.NodeUrl,
            NodeLabel = state.NodeLabel,
            IsImportComplete = state.IsImportComplete,
            ImportCompletedAt = state.ImportCompletedAt,
            CurrentSegmentId = state.CurrentSegmentId,
            CurrentPageNumber = state.CurrentPageNumber,
            CurrentRowOffsetInPage = state.CurrentRowOffsetInPage,
            ImportedRows = state.ImportedRows,
            SnapshotsCommitted = state.SnapshotsCommitted,
            SnapshotsSkipped = state.SnapshotsSkipped,
            BulkInsertRestartCount = state.BulkInsertRestartCount,
            LastConfirmedSnapshotId = state.LastConfirmedSnapshotId,
            LastSkippedSnapshotId = state.LastSkippedSnapshotId,
            LastError = state.LastError,
            LastUpdatedAt = state.LastUpdatedAt
        };
    }

    private static bool IsRecoverableBulkInsertFailure(Exception ex)
        => ex is BulkInsertAbortedException ||
           ex is BulkInsertClientException ||
           ex is HttpRequestException ||
           ex is IOException;

    private static bool CanSafelyAutoSkipKnownPoisonHead(NodeDocumentSnapshot headSnapshot, Exception failure)
        => NodeDocumentSnapshots.ContainsBulkInsertUnsafeCharacters(headSnapshot.Id) &&
           ContainsKnownInvalidEscapeSignature(failure);

    private static bool ContainsKnownInvalidEscapeSignature(Exception failure)
    {
        foreach (var current in EnumerateExceptions(failure))
        {
            if (current.Message.Contains("Invalid escape char, numeric value is 53", StringComparison.Ordinal))
                return true;
        }

        return false;
    }

    private static IEnumerable<Exception> EnumerateExceptions(Exception failure)
    {
        var pending = new Stack<Exception>();
        pending.Push(failure);

        while (pending.Count > 0)
        {
            var current = pending.Pop();
            yield return current;

            if (current is AggregateException aggregate)
            {
                foreach (var inner in aggregate.InnerExceptions)
                    pending.Push(inner);
            }
            else if (current.InnerException != null)
            {
                pending.Push(current.InnerException);
            }
        }
    }
}
