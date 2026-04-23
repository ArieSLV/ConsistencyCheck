namespace ConsistencyCheck;

internal enum LiveETagNodeExecutionAction
{
    StartScan,
    ResumeScan,
    StartApply,
    ResumeApply
}

internal sealed class LiveETagNodeWorkItem
{
    public LiveETagNodeExecutionAction Action { get; init; }
    public long StartEtag { get; init; }
    public RunStateDocument? ScanRun { get; init; }
    public RunStateDocument? ApplyRun { get; init; }
}

internal sealed class LiveETagNodeExecutionSummary
{
    public string NodeLabel { get; set; } = string.Empty;
    public string NodeUrl { get; set; } = string.Empty;
    public LiveETagNodeExecutionAction InitialAction { get; set; }
    public long StartEtag { get; set; }
    public string? ScanRunId { get; set; }
    public string? ApplyRunId { get; set; }
    public bool ScanCompleted { get; set; }
    public bool ApplyCompleted { get; set; }
    public bool ApplySkipped { get; set; }
    public long DocumentsInspected { get; set; }
    public long MismatchesFound { get; set; }
    public long RepairsPlanned { get; set; }
    public long RepairsAttempted { get; set; }
    public long RepairsPatchedOnWinner { get; set; }
    public long RepairsFailed { get; set; }
}

internal sealed class LiveETagClusterRecoverySummary
{
    public DateTimeOffset StartedAt { get; set; } = DateTimeOffset.UtcNow;
    public DateTimeOffset? CompletedAt { get; set; }
    public bool IsComplete { get; set; }
    public int TotalNodes { get; set; }
    public int NodesCompleted { get; set; }
    public long DocumentsInspected { get; set; }
    public long MismatchesFound { get; set; }
    public long RepairsPlanned { get; set; }
    public long RepairsAttempted { get; set; }
    public long RepairsPatchedOnWinner { get; set; }
    public long RepairsFailed { get; set; }
    public List<LiveETagNodeExecutionSummary> NodeSummaries { get; } = [];
}

internal sealed class LiveETagClusterRecoveryCoordinator
{
    private readonly AppConfig _config;
    private readonly StateStore _stateStore;
    private readonly Func<CancellationToken, Task<LiveETagClusterRecoverySummary>>? _runSingleCycleOverride;
    private readonly Func<TimeSpan, CancellationToken, Task>? _delayAsyncOverride;

    public LiveETagClusterRecoveryCoordinator(AppConfig config, StateStore stateStore)
        : this(config, stateStore, null, null)
    {
    }

    internal LiveETagClusterRecoveryCoordinator(
        AppConfig config,
        StateStore stateStore,
        Func<CancellationToken, Task<LiveETagClusterRecoverySummary>>? runSingleCycleOverride,
        Func<TimeSpan, CancellationToken, Task>? delayAsyncOverride)
    {
        _config = config;
        _stateStore = stateStore;
        _runSingleCycleOverride = runSingleCycleOverride;
        _delayAsyncOverride = delayAsyncOverride;
    }

    public event Action<IndexedRunProgressUpdate>? ProgressUpdated;

    public async Task<LiveETagClusterRecoverySummary> RunAsync(CancellationToken ct)
    {
        if (_config.LiveETagClusterExecutionMode != LiveETagClusterExecutionMode.Recurring)
            return await RunSingleCycleCoreAsync(ct).ConfigureAwait(false);

        var delayMinutes = _config.LiveETagRecurringIntervalMinutes;
        if (delayMinutes is not > 0)
        {
            throw new InvalidOperationException(
                "Recurring all-node Live ETag execution requires a positive delay interval.");
        }

        var consecutiveFailures = 0;

        while (true)
        {
            ct.ThrowIfCancellationRequested();

            try
            {
                var cycleSummary = await RunSingleCycleCoreAsync(ct).ConfigureAwait(false);
                consecutiveFailures = 0;

                PublishCoordinatorProgress(
                    cycleSummary,
                    "waiting between cycles",
                    null,
                    $"cycle complete; waiting {delayMinutes.Value:N0} minute(s) before the next full pass");

                await DelayAsync(TimeSpan.FromMinutes(delayMinutes.Value), ct).ConfigureAwait(false);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                consecutiveFailures++;
                var backoffSeconds = Math.Min(
                    30.0 * Math.Pow(2, consecutiveFailures - 1),
                    60.0);
                var backoff = TimeSpan.FromSeconds(backoffSeconds);

                FailoverLog.Append(
                    $"Cycle failed (attempt {consecutiveFailures}); " +
                    $"retrying in {backoff.TotalSeconds:N0}s. Error: {ex}");

                PublishCoordinatorProgress(
                    new LiveETagClusterRecoverySummary(),
                    "cycle failed — waiting to retry",
                    null,
                    $"attempt {consecutiveFailures} failed: {ex.Message}; retrying in {backoff.TotalSeconds:N0}s");

                await DelayAsync(backoff, ct).ConfigureAwait(false);
            }
        }
    }

    private Task<LiveETagClusterRecoverySummary> RunSingleCycleCoreAsync(CancellationToken ct)
        => _runSingleCycleOverride?.Invoke(ct) ?? RunSingleCycleAsync(ct);

    private Task DelayAsync(TimeSpan delay, CancellationToken ct)
        => _delayAsyncOverride?.Invoke(delay, ct) ?? Task.Delay(delay, ct);

    private async Task<LiveETagClusterRecoverySummary> RunSingleCycleAsync(CancellationToken ct)
    {
        var summary = new LiveETagClusterRecoverySummary
        {
            StartedAt = DateTimeOffset.UtcNow,
            TotalNodes = _config.Nodes.Count
        };

        PublishCoordinatorProgress(
            summary,
            "starting live etag recovery",
            null,
            $"processing {_config.Nodes.Count:N0} nodes sequentially");

        foreach (var indexedNode in _config.Nodes.Select((node, index) => new { Node = node, Index = index }))
        {
            ct.ThrowIfCancellationRequested();

            var work = await _stateStore
                .ResolveLiveETagNodeWorkAsync(_config.DatabaseName, indexedNode.Node.Url, ct)
                .ConfigureAwait(false);

            var nodeSummary = new LiveETagNodeExecutionSummary
            {
                NodeLabel = indexedNode.Node.Label,
                NodeUrl = indexedNode.Node.Url,
                InitialAction = work.Action,
                StartEtag = work.StartEtag,
                ScanRunId = work.ScanRun?.RunId,
                ApplyRunId = work.ApplyRun?.RunId
            };
            summary.NodeSummaries.Add(nodeSummary);

            PublishCoordinatorProgress(summary, "preparing node cycle", indexedNode.Node.Label, DescribeNodeWork(work));

            var scanRun = work.ScanRun;
            if (work.Action == LiveETagNodeExecutionAction.StartScan)
            {
                scanRun = await CreateFreshLiveScanRunAsync(indexedNode.Index, work.StartEtag, ct).ConfigureAwait(false);
                nodeSummary.ScanRunId = scanRun.RunId;
                PublishCoordinatorProgress(
                    summary,
                    "created node scan run",
                    indexedNode.Node.Label,
                    $"scan run {scanRun.RunId} from etag {work.StartEtag:N0}");
            }

            if (work.Action is LiveETagNodeExecutionAction.StartScan or LiveETagNodeExecutionAction.ResumeScan)
            {
                var progressBase = CaptureProgressTotals(summary);
                var scanConfig = CreateChildConfig(
                    RunMode.LiveETagScan,
                    indexedNode.Index,
                    scanRun?.ConfigSnapshot?.StartEtag ?? work.StartEtag,
                    ApplyExecutionMode.InteractivePerDocument);

                await using (var scanner = new LiveETagScanPlanner(scanConfig, _stateStore))
                {
                    scanner.ProgressUpdated += update =>
                        ProgressUpdated?.Invoke(ComposeScanProgressUpdate(progressBase, indexedNode.Node.Label, update));
                    scanRun = await scanner.RunAsync(scanRun!, ct).ConfigureAwait(false);
                }

                nodeSummary.ScanRunId = scanRun.RunId;
                nodeSummary.ScanCompleted = scanRun.IsComplete;
                nodeSummary.StartEtag = scanRun.ConfigSnapshot?.StartEtag ?? work.StartEtag;
                ApplyScanOutcome(summary, nodeSummary, scanRun);

                PublishCoordinatorProgress(
                    summary,
                    "node scan complete",
                    indexedNode.Node.Label,
                    scanRun.RepairsPlanned > 0
                        ? $"planned {scanRun.RepairsPlanned:N0} repairs"
                        : "no repairs planned");

                if (scanRun.RepairsPlanned == 0)
                {
                    nodeSummary.ApplySkipped = true;
                    summary.NodesCompleted++;
                    PublishCoordinatorProgress(summary, "node cycle complete", indexedNode.Node.Label, "scan complete; apply skipped");
                    continue;
                }

                work = new LiveETagNodeWorkItem
                {
                    Action = LiveETagNodeExecutionAction.StartApply,
                    StartEtag = scanRun.SafeRestartEtag ?? work.StartEtag,
                    ScanRun = scanRun
                };
            }
            else if (scanRun != null)
            {
                nodeSummary.ScanCompleted = scanRun.IsComplete;
                ApplyScanOutcome(summary, nodeSummary, scanRun);
            }

            if (work.Action is not LiveETagNodeExecutionAction.StartApply and not LiveETagNodeExecutionAction.ResumeApply)
                continue;

            var applyRun = work.ApplyRun;
            if (applyRun == null)
            {
                applyRun = await CreateFreshApplyRunAsync(indexedNode.Index, scanRun!, ct).ConfigureAwait(false);
                nodeSummary.ApplyRunId = applyRun.RunId;
                PublishCoordinatorProgress(
                    summary,
                    "created node apply run",
                    indexedNode.Node.Label,
                    $"apply run {applyRun.RunId} from source scan {scanRun!.RunId}");
            }
            else
            {
                nodeSummary.ApplyRunId = applyRun.RunId;
            }

            var applyProgressBase = CaptureProgressTotals(summary);
            var applyConfig = CreateChildConfig(
                RunMode.ApplyRepairPlan,
                indexedNode.Index,
                startEtag: null,
                ApplyExecutionMode.Automatic);

            await using (var executor = new RepairPlanExecutor(applyConfig, _stateStore))
            {
                executor.ProgressUpdated += update =>
                    ProgressUpdated?.Invoke(ComposeApplyProgressUpdate(applyProgressBase, indexedNode.Node.Label, update));
                applyRun = await executor.RunAsync(applyRun, ct).ConfigureAwait(false);
            }

            nodeSummary.ApplyCompleted = applyRun.IsComplete;
            ApplyApplyOutcome(summary, nodeSummary, applyRun);
            summary.NodesCompleted++;

            PublishCoordinatorProgress(
                summary,
                "node cycle complete",
                indexedNode.Node.Label,
                $"apply complete for source scan {scanRun!.RunId}");
        }

        summary.IsComplete = true;
        summary.CompletedAt = DateTimeOffset.UtcNow;

        PublishCoordinatorProgress(summary, "complete", null, "all configured nodes processed");
        return summary;
    }

    internal static LiveETagNodeWorkItem ResolveNodeWork(
        IReadOnlyCollection<RunStateDocument> liveScanRuns,
        IReadOnlyCollection<RunStateDocument> applyRuns)
    {
        var latestScan = liveScanRuns
            .OrderByDescending(GetRunRecency)
            .ThenByDescending(run => run.StartedAt)
            .FirstOrDefault();

        if (latestScan == null)
        {
            return new LiveETagNodeWorkItem
            {
                Action = LiveETagNodeExecutionAction.StartScan,
                StartEtag = 0L
            };
        }

        var latestScanApplyRuns = applyRuns
            .Where(run => string.Equals(run.SourceRepairPlanRunId, latestScan.RunId, StringComparison.Ordinal))
            .OrderByDescending(GetRunRecency)
            .ThenByDescending(run => run.StartedAt)
            .ToList();

        var startEtag = latestScan.SafeRestartEtag ?? latestScan.ConfigSnapshot?.StartEtag ?? 0L;
        var incompleteApply = latestScanApplyRuns.FirstOrDefault(run => run.IsComplete == false);
        if (incompleteApply != null)
        {
            return new LiveETagNodeWorkItem
            {
                Action = LiveETagNodeExecutionAction.ResumeApply,
                StartEtag = startEtag,
                ScanRun = latestScan,
                ApplyRun = incompleteApply
            };
        }

        if (latestScan.IsComplete == false)
        {
            return new LiveETagNodeWorkItem
            {
                Action = LiveETagNodeExecutionAction.ResumeScan,
                StartEtag = startEtag,
                ScanRun = latestScan
            };
        }

        var hasCompletedApply = latestScanApplyRuns.Any(run => run.IsComplete);
        if (latestScan.RepairsPlanned > 0 && hasCompletedApply == false)
        {
            return new LiveETagNodeWorkItem
            {
                Action = LiveETagNodeExecutionAction.StartApply,
                StartEtag = startEtag,
                ScanRun = latestScan
            };
        }

        return new LiveETagNodeWorkItem
        {
            Action = LiveETagNodeExecutionAction.StartScan,
            StartEtag = startEtag,
            ScanRun = latestScan
        };
    }

    internal static string? TryGetLiveETagSourceNodeUrl(RunStateDocument run)
    {
        var config = run.ConfigSnapshot;
        if (config == null || config.Nodes.Count == 0)
            return null;

        if (config.SourceNodeIndex < 0 || config.SourceNodeIndex >= config.Nodes.Count)
            return null;

        return config.Nodes[config.SourceNodeIndex].Url;
    }

    private static DateTimeOffset GetRunRecency(RunStateDocument run)
        => run.LastSavedAt == default ? run.StartedAt : run.LastSavedAt;

    private async Task<RunStateDocument> CreateFreshLiveScanRunAsync(int sourceNodeIndex, long startEtag, CancellationToken ct)
    {
        var childConfig = CreateChildConfig(
            RunMode.LiveETagScan,
            sourceNodeIndex,
            startEtag,
            ApplyExecutionMode.InteractivePerDocument);

        ChangeVectorSemanticsSnapshot? semanticsSnapshot = null;
        if (RunModePolicies.RequiresFreshSemanticsFromCluster(childConfig.RunMode))
            semanticsSnapshot = await ChangeVectorSemanticsResolver.ResolveAsync(childConfig, ct).ConfigureAwait(false);

        var run = await _stateStore.CreateRunAsync(childConfig, startEtag, semanticsSnapshot, ct).ConfigureAwait(false);
        run.SafeRestartEtag = startEtag;
        await _stateStore.SaveRunAsync(run, ct).ConfigureAwait(false);
        return run;
    }

    private async Task<RunStateDocument> CreateFreshApplyRunAsync(
        int sourceNodeIndex,
        RunStateDocument sourceScanRun,
        CancellationToken ct)
    {
        var childConfig = CreateChildConfig(
            RunMode.ApplyRepairPlan,
            sourceNodeIndex,
            startEtag: null,
            ApplyExecutionMode.Automatic);

        var run = await _stateStore.CreateRunAsync(childConfig, 0L, null, ct).ConfigureAwait(false);
        run.SourceRepairPlanRunId = sourceScanRun.RunId;
        await _stateStore.SaveRunAsync(run, ct).ConfigureAwait(false);
        return run;
    }

    private AppConfig CreateChildConfig(
        RunMode runMode,
        int sourceNodeIndex,
        long? startEtag,
        ApplyExecutionMode applyExecutionMode)
    {
        return new AppConfig
        {
            DatabaseName = _config.DatabaseName,
            Nodes = _config.Nodes.Select(node => new NodeConfig
            {
                Label = node.Label,
                Url = node.Url
            }).ToList(),
            SourceNodeIndex = sourceNodeIndex,
            CertificateThumbprint = _config.CertificateThumbprint,
            CertificatePath = _config.CertificatePath,
            CertificatePassword = _config.CertificatePassword,
            Mode = _config.Mode,
            RunMode = runMode,
            ApplyExecutionMode = applyExecutionMode,
            LiveETagScanLaunchMode = _config.LiveETagScanLaunchMode,
            AllowInvalidServerCertificates = _config.AllowInvalidServerCertificates,
            StartEtag = startEtag,
            Throttle = new ThrottleConfig
            {
                PageSize = _config.Throttle.PageSize,
                DelayBetweenBatchesMs = _config.Throttle.DelayBetweenBatchesMs,
                MaxRetries = _config.Throttle.MaxRetries,
                RetryBaseDelayMs = _config.Throttle.RetryBaseDelayMs,
                ClusterLookupBatchSize = _config.Throttle.ClusterLookupBatchSize
            },
            StateStore = new StateStoreConfig
            {
                ServerUrl = _config.StateStore.ServerUrl,
                DatabaseName = _config.StateStore.DatabaseName,
                EnableDiagnostics = _config.StateStore.EnableDiagnostics
            }
        };
    }

    private static LiveETagProgressTotals CaptureProgressTotals(LiveETagClusterRecoverySummary summary)
        => new(
            summary.DocumentsInspected,
            summary.MismatchesFound,
            summary.RepairsPlanned,
            summary.RepairsAttempted,
            summary.RepairsPatchedOnWinner,
            summary.RepairsFailed);

    private static void ApplyScanOutcome(
        LiveETagClusterRecoverySummary summary,
        LiveETagNodeExecutionSummary nodeSummary,
        RunStateDocument scanRun)
    {
        nodeSummary.DocumentsInspected = scanRun.DocumentsInspected;
        nodeSummary.MismatchesFound = scanRun.MismatchesFound;
        nodeSummary.RepairsPlanned = scanRun.RepairsPlanned;

        summary.DocumentsInspected += scanRun.DocumentsInspected;
        summary.MismatchesFound += scanRun.MismatchesFound;
        summary.RepairsPlanned += scanRun.RepairsPlanned;
    }

    private static void ApplyApplyOutcome(
        LiveETagClusterRecoverySummary summary,
        LiveETagNodeExecutionSummary nodeSummary,
        RunStateDocument applyRun)
    {
        nodeSummary.RepairsAttempted = applyRun.RepairsAttempted;
        nodeSummary.RepairsPatchedOnWinner = applyRun.RepairsPatchedOnWinner;
        nodeSummary.RepairsFailed = applyRun.RepairsFailed;

        summary.RepairsAttempted += applyRun.RepairsAttempted;
        summary.RepairsPatchedOnWinner += applyRun.RepairsPatchedOnWinner;
        summary.RepairsFailed += applyRun.RepairsFailed;
    }

    private static IndexedRunProgressUpdate ComposeScanProgressUpdate(
        LiveETagProgressTotals progressBase,
        string nodeLabel,
        IndexedRunProgressUpdate update)
    {
        return update with
        {
            CurrentNodeLabel = nodeLabel,
            DocumentsInspected = progressBase.DocumentsInspected + update.DocumentsInspected,
            UniqueVersionsCompared = progressBase.DocumentsInspected + update.UniqueVersionsCompared,
            MismatchesFound = progressBase.MismatchesFound + update.MismatchesFound,
            RepairsPlanned = progressBase.RepairsPlanned + update.RepairsPlanned,
            RepairsAttempted = progressBase.RepairsAttempted,
            RepairsPatchedOnWinner = progressBase.RepairsPatchedOnWinner,
            RepairsFailed = progressBase.RepairsFailed
        };
    }

    private static IndexedRunProgressUpdate ComposeApplyProgressUpdate(
        LiveETagProgressTotals progressBase,
        string nodeLabel,
        IndexedRunProgressUpdate update)
    {
        return update with
        {
            CurrentNodeLabel = nodeLabel,
            DocumentsInspected = progressBase.DocumentsInspected,
            UniqueVersionsCompared = progressBase.DocumentsInspected,
            MismatchesFound = progressBase.MismatchesFound,
            RepairsPlanned = progressBase.RepairsPlanned,
            RepairsAttempted = progressBase.RepairsAttempted + update.RepairsAttempted,
            RepairsPatchedOnWinner = progressBase.RepairsPatchedOnWinner + update.RepairsPatchedOnWinner,
            RepairsFailed = progressBase.RepairsFailed + update.RepairsFailed,
            RepairsCompleted = update.RepairsCompleted
        };
    }

    private void PublishCoordinatorProgress(
        LiveETagClusterRecoverySummary summary,
        string phase,
        string? currentNodeLabel,
        string? detail)
    {
        ProgressUpdated?.Invoke(new IndexedRunProgressUpdate(
            Phase: phase,
            CurrentNodeLabel: currentNodeLabel,
            Detail: detail,
            CurrentNodeStreamedSnapshots: null,
            CurrentNodeTotalDocuments: null,
            SnapshotDocumentsImported: 0,
            SnapshotDocumentsSkipped: 0,
            SnapshotBulkInsertRestarts: 0,
            CandidateDocumentsFound: summary.MismatchesFound,
            CandidateDocumentsProcessed: summary.MismatchesFound,
            CandidateDocumentsExcludedBySkippedSnapshots: 0,
            DocumentsInspected: summary.DocumentsInspected,
            UniqueVersionsCompared: summary.DocumentsInspected,
            MismatchesFound: summary.MismatchesFound,
            RepairsPlanned: summary.RepairsPlanned,
            RepairsAttempted: summary.RepairsAttempted,
            RepairsPatchedOnWinner: summary.RepairsPatchedOnWinner,
            RepairsFailed: summary.RepairsFailed));
    }

    private static string DescribeNodeWork(LiveETagNodeWorkItem work)
        => work.Action switch
        {
            LiveETagNodeExecutionAction.StartScan => $"starting fresh scan from etag {work.StartEtag:N0}",
            LiveETagNodeExecutionAction.ResumeScan => $"resuming scan run {work.ScanRun?.RunId} from etag {work.ScanRun?.SafeRestartEtag ?? work.StartEtag:N0}",
            LiveETagNodeExecutionAction.StartApply => $"starting apply for scan run {work.ScanRun?.RunId}",
            LiveETagNodeExecutionAction.ResumeApply => $"resuming apply run {work.ApplyRun?.RunId} for scan run {work.ScanRun?.RunId}",
            _ => "preparing node work"
        };

    private readonly record struct LiveETagProgressTotals(
        long DocumentsInspected,
        long MismatchesFound,
        long RepairsPlanned,
        long RepairsAttempted,
        long RepairsPatchedOnWinner,
        long RepairsFailed);
}