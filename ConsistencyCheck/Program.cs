using ConsistencyCheck;
using Spectre.Console;

SchedulerLaunchOptions launchOptions;
try
{
    launchOptions = SchedulerLaunchOptions.Parse(args);
}
catch (InvalidOperationException ex)
{
    AnsiConsole.MarkupLine($"[red]Invalid command line: {Markup.Escape(ex.Message)}[/]");
    Environment.ExitCode = 1;
    return;
}

AnsiConsole.Write(new FigletText("ConsistencyCheck").Color(Color.CornflowerBlue));
AnsiConsole.MarkupLine("[grey]RavenDB Cluster Consistency Checker  v2.0[/]");
AnsiConsole.MarkupLine("[grey]Snapshot + local-index analysis with local RavenDB state storage.[/]");
AnsiConsole.WriteLine();

using var cts = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) =>
{
    e.Cancel = true;
    AnsiConsole.MarkupLine("\n[yellow]Interrupt received — exiting after the current in-flight work finishes…[/]");
    cts.Cancel();
};

var outputDir = Path.Combine(AppContext.BaseDirectory, "output");
Directory.CreateDirectory(outputDir);

var healthStatus = new HealthStatusReporter(launchOptions);
HealthEndpointServer? healthEndpoint = null;
var progressStore = new ProgressStore(outputDir);
RunMode? activeRunMode = null;
LiveETagScanLaunchMode activeLiveETagLaunchMode = LiveETagScanLaunchMode.SingleNodeManual;
LiveETagClusterExecutionMode activeLiveETagClusterExecutionMode = LiveETagClusterExecutionMode.SinglePass;

try
{
    if (launchOptions.IsSchedulerProfile)
    {
        healthStatus.MarkStarting();
        healthEndpoint = HealthEndpointServer.Create(launchOptions, healthStatus);
        await healthEndpoint.StartAsync(cts.Token).ConfigureAwait(false);
        AnsiConsole.MarkupLine(
            $"[green]Health endpoint listening on[/] [cyan]{Markup.Escape(launchOptions.HealthBindUrl + launchOptions.HealthPath)}[/]");
        AnsiConsole.WriteLine();
    }

    var resolved = await ResolveRunContextAsync(progressStore, launchOptions, healthStatus, cts.Token);
    await using var stateStore = resolved.StateStore;

    var config = resolved.Config;
    healthStatus.MarkConfigured(config);
    activeRunMode = config.RunMode;
    activeLiveETagLaunchMode = config.LiveETagScanLaunchMode;
    activeLiveETagClusterExecutionMode = config.LiveETagClusterExecutionMode;
    var isLiveEtagClusterCoordinator =
        config.RunMode == RunMode.LiveETagScan &&
        config.LiveETagScanLaunchMode == LiveETagScanLaunchMode.AllNodesAutomatic;
    var isLiveEtagRecurringCoordinator =
        isLiveEtagClusterCoordinator &&
        config.LiveETagClusterExecutionMode == LiveETagClusterExecutionMode.Recurring;
    var run = resolved.Run;
    var semanticsSnapshot = run?.ChangeVectorSemanticsSnapshot;

    using var certificateValidationScope = RunModePolicies.RequiresCustomerClusterConnectivity(config.RunMode)
        ? RavenCertificateValidationScope.Create(
            config.Nodes.Select(node => node.Url),
            config.AllowInvalidServerCertificates)
        : null;

    if (!resolved.StartupConnectivityValidated && RunModePolicies.RequiresStartupConnectivityValidation(config.RunMode))
        await RuntimeConnectivityValidator.ValidateAsync(config, cts.Token);

    var startupTitle = config.RunMode switch
    {
        RunMode.ImportSnapshots => "[bold green]Importing snapshots[/]",
        RunMode.DownloadSnapshotsToCache => "[bold green]Downloading snapshots to local cache[/]",
        RunMode.ImportCachedSnapshotsToStateStore => "[bold green]Importing cached snapshots into local RavenDB[/]",
        RunMode.ApplyRepairPlan => "[bold green]Applying saved repair plan[/]",
        RunMode.LiveETagScan => isLiveEtagClusterCoordinator
            ? isLiveEtagRecurringCoordinator
                ? "[bold green]Live ETag recurring scan + recovery across all nodes[/]"
                : "[bold green]Live ETag scan + recovery across all nodes[/]"
            : "[bold green]Live ETag scan — building repair plan[/]",
        RunMode.SnapshotCrossCheck => "[bold green][[TEMP]] Snapshot cross-check — building repair plan[/]",
        RunMode.MismatchDecisionFixup => "[bold green][[TEMP2]] Mismatch decision fixup[/]",
        _ => "[bold green]Analyzing imported snapshots[/]"
    };
    var traversalText = config.RunMode switch
    {
        RunMode.ImportSnapshots =>
            "Customer-cluster metadata query streams directly into the local RavenDB snapshot collection",
        RunMode.DownloadSnapshotsToCache =>
            "Customer-cluster metadata-only paging into the local segmented snapshot cache",
        RunMode.ImportCachedSnapshotsToStateStore =>
            "Local segmented snapshot cache -> local RavenDB snapshot collection",
        RunMode.ScanOnly =>
            "Imported snapshots + local candidate index + local snapshot evaluation (report only)",
        RunMode.DryRunRepair =>
            "Imported snapshots + local candidate index + local snapshot evaluation (collect repair plan)",
        RunMode.ScanAndRepair =>
            "Imported snapshots + local candidate index + live revalidation (repair enabled)",
        RunMode.ApplyRepairPlan =>
            "Saved repair-plan execution with live per-document revalidation before every patch",
        RunMode.LiveETagScan =>
            isLiveEtagClusterCoordinator
                ? isLiveEtagRecurringCoordinator
                    ? $"Sequential live ETag scan per node → immediate apply for that node → next node → wait {config.LiveETagRecurringIntervalMinutes.GetValueOrDefault():N0} minutes after the full cycle → repeat"
                    : "Sequential live ETag scan per node → immediate apply for that node → next node"
                : "Live ETag stream from source node → metadata from all nodes → repair plan (no snapshot import needed)",
        RunMode.SnapshotCrossCheck =>
            "[TEMP] Enumerate all imported snapshots node by node → fetch live metadata → repair plan (bypasses index)",
        RunMode.MismatchDecisionFixup =>
            "[TEMP2] Load all MismatchDocuments from a SnapshotCrossCheck run → restore SkippedAlreadyPlanned → PatchPlannedDryRun",
        _ => "Imported snapshots analysis"
    };

    var repairedDocCount = config.RunMode == RunMode.SnapshotCrossCheck
        ? (await stateStore.LoadSuccessfullyRepairedDocumentIdsAsync(cts.Token).ConfigureAwait(false)).Count
        : 0;

    AnsiConsole.WriteLine();
    AnsiConsole.Write(new Rule(startupTitle).RuleStyle("green"));
    AnsiConsole.MarkupLine($"  Database        : [cyan]{Markup.Escape(config.DatabaseName)}[/]");
    AnsiConsole.MarkupLine($"  Run mode        : [cyan]{Markup.Escape(DescribeRunMode(config.RunMode))}[/]");
    AnsiConsole.MarkupLine($"  Traversal       : [cyan]{Markup.Escape(traversalText)}[/]");
    AnsiConsole.MarkupLine($"  Nodes           : [cyan]{string.Join(", ", config.Nodes.Select(node => node.Label))}[/]");
    AnsiConsole.MarkupLine($"  Page size       : [cyan]{config.Throttle.PageSize}[/]   Lookup batch: [cyan]{config.Throttle.ClusterLookupBatchSize}[/]");
    AnsiConsole.MarkupLine($"  Delay           : [cyan]{config.Throttle.DelayBetweenBatchesMs}[/] ms   Retries: [cyan]{config.Throttle.MaxRetries}[/]");
    AnsiConsole.MarkupLine(
        $"  Server certs    : {(config.AllowInvalidServerCertificates ? "[yellow]allow invalid (dev/test only)[/]" : "[green]strict[/]")}");
    if (semanticsSnapshot != null)
    {
        AnsiConsole.MarkupLine(
            $"  Explicit unused : [cyan]{Markup.Escape(DescribeIdList(semanticsSnapshot.ExplicitUnusedDatabaseIds))}[/]");
        AnsiConsole.MarkupLine(
            $"  Potential unused: {(semanticsSnapshot.PotentialUnusedDatabaseIds.Count == 0
                ? "[green]none[/]"
                : $"[yellow]{Markup.Escape(DescribeIdList(semanticsSnapshot.PotentialUnusedDatabaseIds))}[/] [grey](warning only)[/]")}");
    }
    AnsiConsole.MarkupLine($"  State store     : [cyan]{Markup.Escape(config.StateStore.ServerUrl)}[/] / [cyan]{Markup.Escape(config.StateStore.DatabaseName)}[/]");
    if (run != null)
        AnsiConsole.MarkupLine($"  Run ID          : [cyan]{Markup.Escape(run.RunId)}[/]");
    if (config.RunMode is RunMode.DownloadSnapshotsToCache or RunMode.ImportCachedSnapshotsToStateStore &&
        TryGetSelectedImportNode(config) is { } selectedImportNode)
    {
        var selectedLabel = config.RunMode is RunMode.ImportSnapshots or RunMode.DownloadSnapshotsToCache
            ? "Download node"
            : "Import node";
        AnsiConsole.MarkupLine($"  {selectedLabel,-15}: [cyan]{Markup.Escape(selectedImportNode.Label)}[/]  ({Markup.Escape(selectedImportNode.Url)})");
    }
    if (config.RunMode == RunMode.ImportCachedSnapshotsToStateStore && run != null && !string.IsNullOrWhiteSpace(run.SourceSnapshotCacheRunId))
        AnsiConsole.MarkupLine($"  Source cache    : [cyan]{Markup.Escape(run.SourceSnapshotCacheRunId)}[/]");
    if (config.RunMode == RunMode.ApplyRepairPlan && run != null && !string.IsNullOrWhiteSpace(run.SourceRepairPlanRunId))
        AnsiConsole.MarkupLine($"  Source plan     : [cyan]{Markup.Escape(run.SourceRepairPlanRunId)}[/]");
    if (config.RunMode == RunMode.ApplyRepairPlan)
    {
        var applyModeText = config.ApplyExecutionMode == ApplyExecutionMode.Automatic
            ? "Automatic"
            : "Interactive per document";
        AnsiConsole.MarkupLine($"  Apply mode      : [cyan]{Markup.Escape(applyModeText)}[/]");
    }
    if (config.RunMode is RunMode.ScanOnly or RunMode.DryRunRepair or RunMode.ScanAndRepair &&
        run != null &&
        !string.IsNullOrWhiteSpace(run.SourceSnapshotRunId))
    {
        AnsiConsole.MarkupLine($"  Source snapshot : [cyan]{Markup.Escape(run.SourceSnapshotRunId)}[/]");
    }
    if (config.RunMode == RunMode.LiveETagScan)
    {
        if (isLiveEtagClusterCoordinator)
        {
            var liveCycleText = isLiveEtagRecurringCoordinator
                ? $"scan one node -> apply -> next node -> wait {config.LiveETagRecurringIntervalMinutes.GetValueOrDefault():N0} min -> repeat"
                : "scan one node -> apply -> next node";
            AnsiConsole.MarkupLine($"  Live cycle      : [cyan]{Markup.Escape(liveCycleText)}[/]");
            if (isLiveEtagRecurringCoordinator)
            {
                AnsiConsole.MarkupLine(
                    $"  Cycle delay     : [cyan]{config.LiveETagRecurringIntervalMinutes.GetValueOrDefault():N0}[/] min after a full all-node cycle");
            }
        }
        else
        {
            var sourceNode = config.Nodes[config.SourceNodeIndex];
            AnsiConsole.MarkupLine($"  Source node     : [cyan]{Markup.Escape(sourceNode.Label)}[/]  ({Markup.Escape(sourceNode.Url)})");
        }
    }
    if (config.RunMode == RunMode.SnapshotCrossCheck)
        AnsiConsole.MarkupLine($"  Already repaired: [cyan]{repairedDocCount:N0}[/] docs (will be skipped)");
    if (config.RunMode == RunMode.MismatchDecisionFixup && run != null && !string.IsNullOrWhiteSpace(run.SourceSnapshotRunId))
        AnsiConsole.MarkupLine($"  Target run      : [cyan]{Markup.Escape(run.SourceSnapshotRunId)}[/]");
    AnsiConsole.WriteLine();

    RunStateDocument? finalRun = run;
    LiveETagClusterRecoverySummary? liveEtagClusterSummary = null;
    var importProgressMode = config.RunMode is RunMode.ImportSnapshots or RunMode.DownloadSnapshotsToCache or RunMode.ImportCachedSnapshotsToStateStore;
    var automaticApplyMode = config.RunMode == RunMode.ApplyRepairPlan &&
                             config.ApplyExecutionMode == ApplyExecutionMode.Automatic;
    var interactiveApplyMode = config.RunMode == RunMode.ApplyRepairPlan &&
                               config.ApplyExecutionMode == ApplyExecutionMode.InteractivePerDocument;
    if (interactiveApplyMode)
    {
        await using var executor = new RepairPlanExecutor(config, stateStore);
        var autoModeActivated = false;
        executor.ProgressUpdated += update =>
        {
            if (update.RepairsCompleted is not { } completed)
                return;
            if (!autoModeActivated)
            {
                autoModeActivated = true;
                AnsiConsole.WriteLine();
            }
            Console.Write(
                $"\rautomatic: applied {completed:N0} / {update.RepairsPlanned:N0}" +
                $"  patched {update.RepairsPatchedOnWinner:N0}" +
                $"  failed {update.RepairsFailed:N0}   ");
        };
        finalRun = await executor.RunAsync(run!, cts.Token).ConfigureAwait(false);
        if (autoModeActivated)
            AnsiConsole.WriteLine();
    }
    else
    {
        ProgressColumn[] progressColumns = importProgressMode
            ? [
                new SpinnerColumn(),
                new TaskDescriptionColumn { Alignment = Justify.Left },
                new ProgressBarColumn(),
                new PercentageColumn(),
                new RemainingTimeColumn(),
                new TransferSpeedColumn(),
                new ElapsedTimeColumn()
            ]
            : automaticApplyMode
            ? [
                new SpinnerColumn(),
                new TaskDescriptionColumn { Alignment = Justify.Left },
                new ProgressBarColumn(),
                new PercentageColumn(),
                new RemainingTimeColumn(),
                new ElapsedTimeColumn()
            ]
            : [
                new SpinnerColumn(),
                new TaskDescriptionColumn { Alignment = Justify.Left },
                new ElapsedTimeColumn()
            ];

        await AnsiConsole.Progress()
            .AutoClear(false)
            .HideCompleted(false)
            .Columns(progressColumns)
            .StartAsync(async context =>
            {
            var progressLock = new object();
            var summaryTask = context.AddTask("Running…");
            summaryTask.IsIndeterminate(true);
            var importNodesForUi = importProgressMode && TryGetSelectedImportNode(config) is { } importNode
                ? [importNode]
                : config.Nodes;
            var nodeStreamedSnapshotCounts = importProgressMode
                ? importNodesForUi.ToDictionary(node => node.Label, _ => 0L, StringComparer.OrdinalIgnoreCase)
                : new Dictionary<string, long>(StringComparer.OrdinalIgnoreCase);
            var nodeTotalSnapshotCounts = importProgressMode
                ? importNodesForUi.ToDictionary(node => node.Label, _ => 0L, StringComparer.OrdinalIgnoreCase)
                : new Dictionary<string, long>(StringComparer.OrdinalIgnoreCase);
            var nodeTasks = importProgressMode
                ? importNodesForUi.ToDictionary(
                    node => node.Label,
                    node =>
                    {
                        var nodeTask = context.AddTask($"[grey]{Markup.Escape(node.Label)}[/]  waiting to start");
                        nodeTask.IsIndeterminate(true);
                        return nodeTask;
                    },
                    StringComparer.OrdinalIgnoreCase)
                : new Dictionary<string, ProgressTask>(StringComparer.OrdinalIgnoreCase);
            long crossCheckRateStartTicks = 0L;
            long crossCheckRateStartInspected = 0L;

            static void UpdateProgressTask(ProgressTask task, long currentValue, long? totalValue)
            {
                if (totalValue is not > 0)
                    return;

                var maxValue = Math.Max(1d, totalValue.Value);
                if (task.IsIndeterminate)
                {
                    task.MaxValue = maxValue;
                    task.IsIndeterminate = false;
                    if (!task.IsStarted)
                        task.StartTask();
                }
                else if (Math.Abs(task.MaxValue - maxValue) > double.Epsilon)
                {
                    task.MaxValue = maxValue;
                }

                task.Value = Math.Clamp(currentValue, 0L, totalValue.Value);
            }

            static bool ShouldAppendImportNodeDetail(IndexedRunProgressUpdate update)
            {
                if (string.IsNullOrWhiteSpace(update.Detail))
                    return false;

                if (update.CurrentNodeStreamedSnapshots is null)
                    return true;

                return update.Detail.StartsWith("streamed ", StringComparison.OrdinalIgnoreCase) == false;
            }

            void OnProgress(IndexedRunProgressUpdate update)
            {
                healthStatus.RecordProgress(update);

                lock (progressLock)
                {
                    var phaseText = $"[grey]{Markup.Escape(update.Phase)}[/]";
                    var nodeText = string.IsNullOrWhiteSpace(update.CurrentNodeLabel)
                        ? string.Empty
                        : $"  node [cyan]{Markup.Escape(update.CurrentNodeLabel)}[/]";
                    var detailText = string.IsNullOrWhiteSpace(update.Detail)
                        ? string.Empty
                        : $"  {Markup.Escape(update.Detail)}";

                    if (importProgressMode &&
                        !string.IsNullOrWhiteSpace(update.CurrentNodeLabel) &&
                        update.CurrentNodeStreamedSnapshots is long currentNodeStreamedSnapshots)
                    {
                        nodeStreamedSnapshotCounts[update.CurrentNodeLabel] = currentNodeStreamedSnapshots;
                    }

                    if (importProgressMode &&
                        !string.IsNullOrWhiteSpace(update.CurrentNodeLabel) &&
                        update.CurrentNodeTotalDocuments is long currentNodeTotalDocuments)
                    {
                        nodeTotalSnapshotCounts[update.CurrentNodeLabel] = currentNodeTotalDocuments;
                    }

                    var crossCheckRateText = string.Empty;
                    if (config.RunMode == RunMode.SnapshotCrossCheck && update.DocumentsInspected > 0)
                    {
                        if (crossCheckRateStartTicks == 0L)
                        {
                            crossCheckRateStartTicks = System.Diagnostics.Stopwatch.GetTimestamp();
                            crossCheckRateStartInspected = update.DocumentsInspected;
                        }
                        var elapsedSec = (double)(System.Diagnostics.Stopwatch.GetTimestamp() - crossCheckRateStartTicks)
                            / System.Diagnostics.Stopwatch.Frequency;
                        var rate = elapsedSec > 0.5
                            ? (update.DocumentsInspected - crossCheckRateStartInspected) / elapsedSec
                            : 0;
                        if (rate > 0)
                            crossCheckRateText = $"  [grey]{rate:N0}/s[/]";
                    }

                    var repairText = config.RunMode switch
                    {
                        RunMode.ScanAndRepair =>
                            $"  repairs [green]{update.RepairsPatchedOnWinner:N0}[/]/[yellow]{update.RepairsAttempted:N0}[/]/[red]{update.RepairsFailed:N0}[/]",
                        RunMode.DryRunRepair =>
                            $"  planned [yellow]{update.RepairsPlanned:N0}[/]",
                        RunMode.ApplyRepairPlan =>
                            $"  applied [green]{update.RepairsPatchedOnWinner:N0}[/]/[yellow]{update.RepairsAttempted:N0}[/]/[red]{update.RepairsFailed:N0}[/]",
                        RunMode.LiveETagScan =>
                            isLiveEtagClusterCoordinator
                                ? $"  inspected [cyan]{update.DocumentsInspected:N0}[/]  mismatches [red]{update.MismatchesFound:N0}[/]  planned [yellow]{update.RepairsPlanned:N0}[/]  applied [green]{update.RepairsPatchedOnWinner:N0}[/]/[yellow]{update.RepairsAttempted:N0}[/]/[red]{update.RepairsFailed:N0}[/]"
                                : $"  inspected [cyan]{update.DocumentsInspected:N0}[/]  mismatches [red]{update.MismatchesFound:N0}[/]  planned [yellow]{update.RepairsPlanned:N0}[/]",
                        RunMode.SnapshotCrossCheck =>
                            $"  inspected [cyan]{update.DocumentsInspected:N0}[/]  mismatches [red]{update.MismatchesFound:N0}[/]  planned [yellow]{update.RepairsPlanned:N0}[/]{crossCheckRateText}",
                        RunMode.MismatchDecisionFixup =>
                            $"  inspected [cyan]{update.DocumentsInspected:N0}[/]  fixed [yellow]{update.MismatchesFound:N0}[/]",
                        _ => string.Empty
                    };

                    var summaryNodeText = importProgressMode ? string.Empty : nodeText;
                    var summaryDetailText = detailText;
                    if (importProgressMode)
                    {
                        var totalStreamedAcrossNodes = nodeStreamedSnapshotCounts.Values.Sum();
                        var totalExpectedAcrossNodes = nodeTotalSnapshotCounts.Values.Sum();
                        if (totalExpectedAcrossNodes > 0)
                        {
                            UpdateProgressTask(summaryTask, totalStreamedAcrossNodes, totalExpectedAcrossNodes);
                            summaryDetailText = nodeStreamedSnapshotCounts.Count == 1
                                ? $"  streamed {totalStreamedAcrossNodes:N0}/{totalExpectedAcrossNodes:N0} snapshot rows from current node"
                                : $"  streamed total {totalStreamedAcrossNodes:N0}/{totalExpectedAcrossNodes:N0} snapshot rows across {nodeStreamedSnapshotCounts.Count:N0} nodes";
                        }
                        else if (string.IsNullOrWhiteSpace(update.Detail))
                        {
                            summaryDetailText = "  waiting for per-node document totals";
                        }
                    }
                    else if (automaticApplyMode && update.RepairsCompleted is { } completed)
                    {
                        UpdateProgressTask(summaryTask, completed, update.RepairsPlanned);
                    }

                    summaryTask.Description = importProgressMode
                        ? $"{phaseText}{summaryDetailText}  restarts [yellow]{update.SnapshotBulkInsertRestarts:N0}[/]  skipped [red]{update.SnapshotDocumentsSkipped:N0}[/]"
                        : automaticApplyMode
                        ? $"{phaseText}{summaryNodeText}{repairText}"
                        : $"{phaseText}{summaryNodeText}{summaryDetailText}  snapshots [cyan]{update.SnapshotDocumentsImported:N0}[/]  restarts [yellow]{update.SnapshotBulkInsertRestarts:N0}[/]  skipped [red]{update.SnapshotDocumentsSkipped:N0}[/]  candidates [cyan]{update.CandidateDocumentsProcessed:N0}[/]/[cyan]{update.CandidateDocumentsFound:N0}[/]  mismatches [red]{update.MismatchesFound:N0}[/]{repairText}";

                    if (!string.IsNullOrWhiteSpace(update.CurrentNodeLabel) &&
                        nodeTasks.TryGetValue(update.CurrentNodeLabel, out var nodeTask))
                    {
                        if (importProgressMode)
                        {
                            var nodeStreamed = nodeStreamedSnapshotCounts.GetValueOrDefault(update.CurrentNodeLabel, 0);
                            var nodeTotal = nodeTotalSnapshotCounts.GetValueOrDefault(update.CurrentNodeLabel, 0);
                            UpdateProgressTask(nodeTask, nodeStreamed, nodeTotal > 0 ? nodeTotal : null);

                            var nodeProgressText = nodeTotal > 0
                                ? $"  streamed {nodeStreamed:N0}/{nodeTotal:N0} snapshot rows"
                                : nodeStreamed > 0
                                    ? $"  streamed {nodeStreamed:N0} snapshot rows"
                                    : string.Empty;
                            var nodeDetailText = ShouldAppendImportNodeDetail(update)
                                ? detailText
                                : string.Empty;
                            nodeTask.Description =
                                $"[cyan]{Markup.Escape(update.CurrentNodeLabel)}[/]  {phaseText}{nodeProgressText}{nodeDetailText}";
                        }
                        else
                        {
                            nodeTask.Description =
                                $"[cyan]{Markup.Escape(update.CurrentNodeLabel)}[/]  {phaseText}{detailText}";
                        }
                    }
                }
            }

            if (config.RunMode == RunMode.ApplyRepairPlan)
            {
                await using var executor = new RepairPlanExecutor(config, stateStore);
                executor.ProgressUpdated += OnProgress;
                finalRun = await executor.RunAsync(run!, cts.Token).ConfigureAwait(false);
            }
            else if (config.RunMode == RunMode.LiveETagScan && isLiveEtagClusterCoordinator)
            {
                healthStatus.MarkCoordinatorStarting(config);
                var coordinator = new LiveETagClusterRecoveryCoordinator(config, stateStore);
                coordinator.ProgressUpdated += OnProgress;
                liveEtagClusterSummary = await coordinator.RunAsync(cts.Token).ConfigureAwait(false);
            }
            else if (config.RunMode == RunMode.LiveETagScan)
            {
                await using var scanner = new LiveETagScanPlanner(config, stateStore);
                scanner.ProgressUpdated += OnProgress;
                finalRun = await scanner.RunAsync(run!, cts.Token).ConfigureAwait(false);
            }
            else if (config.RunMode == RunMode.SnapshotCrossCheck)
            {
                await using var planner = new SnapshotCrossCheckPlanner(config, stateStore);
                planner.ProgressUpdated += OnProgress;
                finalRun = await planner.RunAsync(run!, cts.Token).ConfigureAwait(false);
            }
            else if (config.RunMode == RunMode.MismatchDecisionFixup)
            {
                await using var fixup = new MismatchDecisionFixupPlanner(stateStore);
                fixup.ProgressUpdated += OnProgress;
                finalRun = await fixup.RunAsync(run!, cts.Token).ConfigureAwait(false);
            }
            else
            {
                await using var checker = new IndexedConsistencyChecker(config, stateStore);
                checker.ProgressUpdated += OnProgress;
                finalRun = await checker.RunAsync(run!, cts.Token).ConfigureAwait(false);
            }

            summaryTask.Value = summaryTask.MaxValue;
            foreach (var nodeTask in nodeTasks.Values)
                nodeTask.Value = nodeTask.MaxValue;
            }).ConfigureAwait(false);
    }

    AnsiConsole.WriteLine();
    if (isLiveEtagClusterCoordinator)
    {
        WriteLiveEtagClusterRecoverySummary(
            config,
            liveEtagClusterSummary ?? throw new InvalidOperationException("Live ETag cluster recovery completed without a summary."));
    }
    else
    {
        var completedRun = finalRun ?? throw new InvalidOperationException("The selected run completed without a final run document.");
        var statusMarkup = completedRun.IsComplete
            ? "[bold green]Complete[/]"
            : "[bold yellow]Interrupted / stopped early[/]";
        var primarySnapshotLabel = config.RunMode switch
        {
            RunMode.DownloadSnapshotsToCache => "Cache rows downloaded",
            _ => "Snapshot docs imported"
        };
        var primarySnapshotValue = config.RunMode switch
        {
            RunMode.DownloadSnapshotsToCache => completedRun.SnapshotCacheNodes.Sum(node => node.DownloadedRows),
            _ => completedRun.SnapshotDocumentsImported
        };
        var completedNodeLabel = config.RunMode switch
        {
            RunMode.ImportSnapshots => "Import nodes completed",
            RunMode.DownloadSnapshotsToCache => "Cache nodes completed",
            RunMode.ImportCachedSnapshotsToStateStore => "Import nodes completed",
            _ => "Cluster nodes"
        };
        var completedNodeValue = config.RunMode switch
        {
            RunMode.ImportSnapshots => $"{completedRun.SnapshotImportNodes.Count(node => node.IsImportComplete):N0}/{config.Nodes.Count:N0}",
            RunMode.DownloadSnapshotsToCache => $"{completedRun.SnapshotCacheNodes.Count(node => node.IsDownloadComplete):N0}/{config.Nodes.Count:N0}",
            RunMode.ImportCachedSnapshotsToStateStore => $"{completedRun.SnapshotImportNodes.Count(node => node.IsImportComplete):N0}/{config.Nodes.Count:N0}",
            _ => $"{config.Nodes.Count:N0}"
        };

        var isAnalysisSummary = completedRun.RunMode is RunMode.ScanOnly or RunMode.DryRunRepair or RunMode.LiveETagScan or RunMode.SnapshotCrossCheck or RunMode.MismatchDecisionFixup;
        var summaryBody = isAnalysisSummary
            ? $"[grey]{primarySnapshotLabel,-25}:[/] [bold]{primarySnapshotValue:N0}[/]\n" +
              $"[grey]{completedNodeLabel,-25}:[/] [bold]{completedNodeValue}[/]\n" +
              $"[grey]Unhealthy documents      :[/] [bold red]{completedRun.MismatchesFound:N0}[/]\n" +
              $"[grey]Automatic repair docs    :[/] [bold yellow]{completedRun.RepairsPlanned:N0}[/]\n" +
              $"[grey]Manual review docs       :[/] [bold magenta]{completedRun.ManualReviewDocumentsFound:N0}[/]\n" +
              $"[grey]Run status               :[/] {statusMarkup}\n" +
              $"{(string.IsNullOrWhiteSpace(completedRun.SourceRepairPlanRunId) ? string.Empty : $"[grey]Source plan run          :[/] [cyan]{Markup.Escape(completedRun.SourceRepairPlanRunId)}[/]\n")}" +
              $"{(string.IsNullOrWhiteSpace(completedRun.SourceSnapshotCacheRunId) ? string.Empty : $"[grey]Source cache run         :[/] [cyan]{Markup.Escape(completedRun.SourceSnapshotCacheRunId)}[/]\n")}" +
              $"{(string.IsNullOrWhiteSpace(completedRun.SourceSnapshotRunId) ? string.Empty : $"[grey]Source snapshot run      :[/] [cyan]{Markup.Escape(completedRun.SourceSnapshotRunId)}[/]\n")}" +
              $"[grey]State store              :[/] [cyan]{Markup.Escape(config.StateStore.ServerUrl)}[/] / [cyan]{Markup.Escape(config.StateStore.DatabaseName)}[/]\n" +
              $"[grey]Run ID                   :[/] [cyan]{Markup.Escape(completedRun.RunId)}[/]"
            : $"[grey]{primarySnapshotLabel,-25}:[/] [bold]{primarySnapshotValue:N0}[/]\n" +
              $"[grey]{completedNodeLabel,-25}:[/] [bold]{completedNodeValue}[/]\n" +
              $"[grey]Snapshot docs skipped     :[/] [bold red]{completedRun.SnapshotDocumentsSkipped:N0}[/]\n" +
              $"[grey]Bulk-insert restarts     :[/] [bold yellow]{completedRun.SnapshotBulkInsertRestarts:N0}[/]\n" +
              $"[grey]Candidates found         :[/] [bold]{completedRun.CandidateDocumentsFound:N0}[/]\n" +
              $"[grey]Candidates processed     :[/] [bold]{completedRun.CandidateDocumentsProcessed:N0}[/]\n" +
              $"[grey]Candidates excluded skip :[/] [bold yellow]{completedRun.CandidateDocumentsExcludedBySkippedSnapshots:N0}[/]\n" +
              $"[grey]Documents inspected      :[/] [bold]{completedRun.DocumentsInspected:N0}[/]\n" +
              $"[grey]Unique versions compared :[/] [bold]{completedRun.UniqueVersionsCompared:N0}[/]\n" +
              $"[grey]Mismatches found         :[/] [bold red]{completedRun.MismatchesFound:N0}[/]\n" +
              $"[grey]Repairs planned          :[/] [bold yellow]{completedRun.RepairsPlanned:N0}[/]\n" +
              $"[grey]Repairs attempted        :[/] [bold]{completedRun.RepairsAttempted:N0}[/]\n" +
              $"[grey]Repairs patched winner   :[/] [bold green]{completedRun.RepairsPatchedOnWinner:N0}[/]\n" +
              $"[grey]Repairs failed           :[/] [bold red]{completedRun.RepairsFailed:N0}[/]\n" +
              $"[grey]Run status               :[/] {statusMarkup}\n" +
              $"{(string.IsNullOrWhiteSpace(completedRun.SourceRepairPlanRunId) ? string.Empty : $"[grey]Source plan run          :[/] [cyan]{Markup.Escape(completedRun.SourceRepairPlanRunId)}[/]\n")}" +
              $"{(string.IsNullOrWhiteSpace(completedRun.SourceSnapshotCacheRunId) ? string.Empty : $"[grey]Source cache run         :[/] [cyan]{Markup.Escape(completedRun.SourceSnapshotCacheRunId)}[/]\n")}" +
              $"{(string.IsNullOrWhiteSpace(completedRun.SourceSnapshotRunId) ? string.Empty : $"[grey]Source snapshot run      :[/] [cyan]{Markup.Escape(completedRun.SourceSnapshotRunId)}[/]\n")}" +
              $"[grey]State store              :[/] [cyan]{Markup.Escape(config.StateStore.ServerUrl)}[/] / [cyan]{Markup.Escape(config.StateStore.DatabaseName)}[/]\n" +
              $"[grey]Run ID                   :[/] [cyan]{Markup.Escape(completedRun.RunId)}[/]";

        var summary = new Panel(summaryBody)
            .Header("[bold]Run Summary[/]")
            .Border(BoxBorder.Rounded)
            .Padding(1, 0);

        AnsiConsole.Write(summary);
    }
}
catch (OperationCanceledException)
{
    AnsiConsole.WriteLine();
    AnsiConsole.MarkupLine(
        activeRunMode == RunMode.ApplyRepairPlan
            ? "[yellow]Run interrupted. Restart the tool and choose the existing apply-run to continue from its saved checkpoint.[/]"
            : activeRunMode == RunMode.LiveETagScan &&
              activeLiveETagLaunchMode == LiveETagScanLaunchMode.AllNodesAutomatic &&
              activeLiveETagClusterExecutionMode == LiveETagClusterExecutionMode.Recurring
                ? "[yellow]Run interrupted. Restart the tool, choose Live ETag scan -> all nodes automatically -> recurring cycle, and the coordinator will resume the pending per-node scan/apply cycle before continuing recurring passes.[/]"
            : activeRunMode == RunMode.LiveETagScan && activeLiveETagLaunchMode == LiveETagScanLaunchMode.AllNodesAutomatic
                ? "[yellow]Run interrupted. Restart the tool, choose Live ETag scan -> all nodes automatically, and the coordinator will resume the pending per-node scan/apply cycle.[/]"
            : activeRunMode == RunMode.LiveETagScan
                ? "[yellow]Run interrupted. Restart the tool and choose the existing live ETag scan run to resume from its saved ETag position.[/]"
            : activeRunMode == RunMode.SnapshotCrossCheck
                ? "[yellow]Run interrupted. Restart the tool and choose the existing snapshot cross-check run to resume from its saved position.[/]"
            : activeRunMode == RunMode.MismatchDecisionFixup
                ? "[yellow]Run interrupted. Restart the tool and run the mismatch decision fixup again — it is idempotent.[/]"
            : activeRunMode == RunMode.ImportSnapshots
                ? "[yellow]Run interrupted. Restart the tool and choose the existing snapshot-import run to continue over the current local snapshot set, or start fresh.[/]"
            : activeRunMode == RunMode.DownloadSnapshotsToCache
                ? "[yellow]Run interrupted. Restart the tool and choose the existing cache-download run to continue from its saved checkpoint, or discard it and start fresh.[/]"
                : activeRunMode == RunMode.ImportCachedSnapshotsToStateStore
                    ? "[yellow]Run interrupted. Restart the tool and choose the existing cache-import run to continue from its saved checkpoint, or start a fresh import from the active cache set.[/]"
                : "[yellow]Run interrupted. Restart the tool and choose the existing analysis-run to continue without re-importing snapshots.[/]");
}
catch (Exception ex)
{
    healthStatus.MarkFatal(ex);
    Environment.ExitCode = 1;
    AnsiConsole.WriteLine();
    AnsiConsole.MarkupLine($"[red]Unexpected error: {Markup.Escape(ex.Message)}[/]");
    AnsiConsole.WriteException(ex, ExceptionFormats.ShortenPaths | ExceptionFormats.ShowLinks);
}
finally
{
    if (healthEndpoint != null)
        await healthEndpoint.DisposeAsync().ConfigureAwait(false);
}

return;

static async Task<ResolvedRunContext> ResolveRunContextAsync(
    ProgressStore progressStore,
    SchedulerLaunchOptions launchOptions,
    HealthStatusReporter healthStatus,
    CancellationToken ct)
{
    if (launchOptions.IsSchedulerProfile)
        return await ResolveSchedulerRunContextAsync(progressStore, launchOptions, healthStatus, ct).ConfigureAwait(false);

    while (true)
    {
        var config = await LoadOrConfigureConfigAsync(progressStore, ct).ConfigureAwait(false);
        var stateStore = new StateStore(config.StateStore);

        try
        {
            await stateStore.EnsureDatabaseExistsAsync(ct).ConfigureAwait(false);
            await stateStore.EnsureLocalIndexesAsync(ct).ConfigureAwait(false);

            var selection = await SelectRunAsync(config, stateStore, ct).ConfigureAwait(false);
            if (selection.Reconfigure)
            {
                await stateStore.DisposeAsync().ConfigureAwait(false);
                continue;
            }

            if (selection.Cancelled)
                throw new OperationCanceledException("Run selection was cancelled.");

            if (selection.UseLiveETagClusterCoordinator)
                return new ResolvedRunContext(config, stateStore, null);

            var run = selection.Run!;
            if (RunModePolicies.RequiresSemanticsSnapshot(run.RunMode))
            {
                var semanticsSnapshot = await ChangeVectorSemantics.EnsureSnapshotAsync(
                        run,
                        token => ResolveSemanticsSnapshotAsync(config, stateStore, run, token),
                        (savedRun, token) => stateStore.SaveRunAsync(savedRun, token),
                        ct)
                    .ConfigureAwait(false);

                await stateStore.StoreDiagnosticsAsync(
                        [ChangeVectorSemantics.CreateDiagnostic(run.RunId, semanticsSnapshot)],
                        ct)
                    .ConfigureAwait(false);
            }

            return new ResolvedRunContext(config, stateStore, run);
        }
        catch
        {
            await stateStore.DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }
}

static async Task<ResolvedRunContext> ResolveSchedulerRunContextAsync(
    ProgressStore progressStore,
    SchedulerLaunchOptions launchOptions,
    HealthStatusReporter healthStatus,
    CancellationToken ct)
{
    var config = LoadSchedulerConfig(progressStore, launchOptions);
    healthStatus.MarkConfigured(config);

    return await StartupRetryPolicy.ExecuteAsync(
            token => CreateSchedulerRunContextAttemptAsync(config, token),
            launchOptions,
            healthStatus,
            attempt =>
            {
                var message =
                    $"Startup dependency unavailable (attempt {attempt.Attempt:N0}); retrying in {attempt.Delay.TotalSeconds:N0}s. Error: {attempt.Exception.Message}";
                FailoverLog.Append(message);
                AnsiConsole.MarkupLine($"[yellow]{Markup.Escape(message)}[/]");
            },
            ct)
        .ConfigureAwait(false);
}

static AppConfig LoadSchedulerConfig(ProgressStore progressStore, SchedulerLaunchOptions launchOptions)
{
    if (!progressStore.ConfigExists)
    {
        throw new InvalidOperationException(
            "Scheduler profile requires an existing complete output/config.json. Run the interactive wizard once before installing the scheduled task.");
    }

    var config = progressStore.LoadConfig();
    if (!ConfigWizard.IsConfigComplete(config))
    {
        throw new InvalidOperationException(
            "Scheduler profile requires a complete output/config.json. Run the interactive wizard once to fill missing configuration.");
    }

    SchedulerLaunchProfile.ApplyToConfig(config, launchOptions);
    return config;
}

static async Task<ResolvedRunContext> CreateSchedulerRunContextAttemptAsync(
    AppConfig config,
    CancellationToken ct)
{
    var stateStore = new StateStore(config.StateStore);
    try
    {
        await stateStore.EnsureDatabaseExistsAsync(ct).ConfigureAwait(false);
        await stateStore.EnsureLocalIndexesAsync(ct).ConfigureAwait(false);

        using var certificateValidationScope = RunModePolicies.RequiresCustomerClusterConnectivity(config.RunMode)
            ? RavenCertificateValidationScope.Create(
                config.Nodes.Select(node => node.Url),
                config.AllowInvalidServerCertificates)
            : null;

        if (RunModePolicies.RequiresStartupConnectivityValidation(config.RunMode))
            await RuntimeConnectivityValidator.ValidateAsync(config, ct).ConfigureAwait(false);

        return new ResolvedRunContext(config, stateStore, null, StartupConnectivityValidated: true);
    }
    catch
    {
        await stateStore.DisposeAsync().ConfigureAwait(false);
        throw;
    }
}

static async Task<AppConfig> LoadOrConfigureConfigAsync(ProgressStore progressStore, CancellationToken ct)
{
    if (!progressStore.ConfigExists)
    {
        AnsiConsole.MarkupLine("[yellow]No configuration found. Launching setup wizard…[/]");
        AnsiConsole.WriteLine();
        return await new ConfigWizard(progressStore).RunAsync(ct).ConfigureAwait(false);
    }

    try
    {
        var config = progressStore.LoadConfig();

        if (!ConfigWizard.IsConfigComplete(config))
        {
            AnsiConsole.MarkupLine("[yellow]Pre-filled configuration found — please provide the missing details.[/]");
            AnsiConsole.WriteLine();
            return await new ConfigWizard(progressStore).CompleteConfigAsync(config, ct).ConfigureAwait(false);
        }

        AnsiConsole.MarkupLine("[green]Loaded existing configuration.[/]");
        AnsiConsole.WriteLine();
        return await new ConfigWizard(progressStore).ChooseRunModeForLaunchAsync(config, ct).ConfigureAwait(false);
    }
    catch (InvalidOperationException ex)
    {
        AnsiConsole.MarkupLine($"[red]Failed to load configuration: {Markup.Escape(ex.Message)}[/]");
        AnsiConsole.MarkupLine("[yellow]Starting the setup wizard to reconfigure…[/]");
        AnsiConsole.WriteLine();
        return await new ConfigWizard(progressStore).RunAsync(ct).ConfigureAwait(false);
    }
}

static async Task<RunSelection> SelectRunAsync(
    AppConfig config,
    StateStore stateStore,
    CancellationToken ct)
{
    if (config.RunMode == RunMode.ApplyRepairPlan)
        return await SelectApplyRepairPlanRunAsync(config, stateStore, ct).ConfigureAwait(false);

    if (config.RunMode == RunMode.ImportSnapshots)
        return await SelectDirectSnapshotImportRunAsync(config, stateStore, ct).ConfigureAwait(false);

    if (config.RunMode == RunMode.DownloadSnapshotsToCache)
        return await SelectSnapshotCacheDownloadRunAsync(config, stateStore, ct).ConfigureAwait(false);

    if (config.RunMode == RunMode.ImportCachedSnapshotsToStateStore)
        return await SelectCachedSnapshotImportRunAsync(config, stateStore, ct).ConfigureAwait(false);

    if (config.RunMode == RunMode.LiveETagScan)
        return await SelectLiveETagScanRunAsync(config, stateStore, ct).ConfigureAwait(false);

    if (config.RunMode == RunMode.SnapshotCrossCheck)
        return await SelectSnapshotCrossCheckRunAsync(config, stateStore, ct).ConfigureAwait(false);

    if (config.RunMode == RunMode.MismatchDecisionFixup)
        return await SelectMismatchDecisionFixupRunAsync(config, stateStore, ct).ConfigureAwait(false);

    return await SelectAnalysisRunAsync(config, stateStore, ct).ConfigureAwait(false);
}

static async Task<RunSelection> SelectApplyRepairPlanRunAsync(
    AppConfig config,
    StateStore stateStore,
    CancellationToken ct)
{
    var sourcePlanRunId = await SelectRepairPlanRunIdAsync(config, stateStore, ct).ConfigureAwait(false);
    var incompleteRuns = await stateStore
        .LoadIncompleteApplyRepairRunsAsync(config.DatabaseName, sourcePlanRunId, take: 10, ct)
        .ConfigureAwait(false);

    if (incompleteRuns.Count == 0)
    {
        var freshRun = await CreateFreshRunAsync(config, stateStore, ct).ConfigureAwait(false);
        freshRun.SourceRepairPlanRunId = sourcePlanRunId;
        await stateStore.SaveRunAsync(freshRun, ct).ConfigureAwait(false);
        return new RunSelection(freshRun, false);
    }

    var resumeRunId = await SelectApplyRunToResumeAsync(sourcePlanRunId, incompleteRuns, ct).ConfigureAwait(false);
    if (resumeRunId == null)
        return new RunSelection(null, false, Cancelled: true);

    var existingRun = await stateStore.LoadRunAsync(resumeRunId, ct).ConfigureAwait(false)
                      ?? throw new InvalidOperationException(
                          $"Apply repair run '{resumeRunId}' was selected for resume, but the run document could not be loaded.");

    if (!string.Equals(existingRun.SourceRepairPlanRunId, sourcePlanRunId, StringComparison.Ordinal))
    {
        throw new InvalidOperationException(
            $"Apply repair run '{resumeRunId}' points to source plan '{existingRun.SourceRepairPlanRunId}', expected '{sourcePlanRunId}'.");
    }

    return new RunSelection(existingRun, false);
}

static async Task<RunSelection> SelectDirectSnapshotImportRunAsync(
    AppConfig config,
    StateStore stateStore,
    CancellationToken ct)
{
    config.SelectedImportNodeUrl = null;
    var head = await stateStore.LoadRunHeadAsync(config.DatabaseName, ct).ConfigureAwait(false);
    var incompleteRuns = await stateStore
        .LoadIncompleteSnapshotImportRunsAsync(config.DatabaseName, take: 10, ct)
        .ConfigureAwait(false);

    if (incompleteRuns.Count > 0)
    {
        return await SelectDirectSnapshotImportRunToResumeAsync(
                config,
                stateStore,
                incompleteRuns,
                allowStartFresh: true,
                replacementTargetSnapshotRunId: head?.ActiveSnapshotRunId,
                ct)
            .ConfigureAwait(false);
    }

    if (!string.IsNullOrWhiteSpace(head?.ActiveSnapshotRunId))
    {
        var choice = await new SelectionPrompt<string>()
            .Title(
                $"[bold]An active imported snapshot set already exists for[/] [cyan]{Markup.Escape(config.DatabaseName)}[/]\n" +
                $"[grey]Active imported snapshot run:[/] [cyan]{Markup.Escape(head.ActiveSnapshotRunId)}[/]\n" +
                "[yellow]Starting a fresh snapshot import will replace it after all configured nodes finish streaming.[/]")
            .AddChoices(
                "Start fresh snapshot import  [grey]- replace the active imported snapshot set[/]",
                "Cancel")
            .ShowAsync(AnsiConsole.Console, ct)
            .ConfigureAwait(false);

        if (string.Equals(choice, "Cancel", StringComparison.Ordinal))
            return new RunSelection(null, false, Cancelled: true);
    }

    var freshRun = await CreateFreshRunAsync(config, stateStore, ct).ConfigureAwait(false);
    return new RunSelection(freshRun, false);
}

static async Task<RunStateDocument> CreateFreshRunAsync(
    AppConfig config,
    StateStore stateStore,
    CancellationToken ct)
{
    ChangeVectorSemanticsSnapshot? freshSemanticsSnapshot = null;
    if (RunModePolicies.RequiresFreshSemanticsFromCluster(config.RunMode))
        freshSemanticsSnapshot = await ChangeVectorSemanticsResolver.ResolveAsync(config, ct).ConfigureAwait(false);

    return await stateStore.CreateRunAsync(config, 0, freshSemanticsSnapshot, ct).ConfigureAwait(false);
}

static async Task<ChangeVectorSemanticsSnapshot> ResolveSemanticsSnapshotAsync(
    AppConfig config,
    StateStore stateStore,
    RunStateDocument run,
    CancellationToken ct)
{
    if (RunModePolicies.RequiresFreshSemanticsFromCluster(run.RunMode))
        return await ChangeVectorSemanticsResolver.ResolveAsync(config, ct).ConfigureAwait(false);

    if (RunModePolicies.UsesLocalSnapshotEvaluation(run.RunMode))
    {
        if (string.IsNullOrWhiteSpace(run.SourceSnapshotRunId))
            throw new InvalidOperationException($"Offline analysis run '{run.RunId}' is missing SourceSnapshotRunId.");

        var sourceSnapshotRun = await stateStore.LoadRunAsync(run.SourceSnapshotRunId, ct).ConfigureAwait(false)
                               ?? throw new InvalidOperationException(
                                   $"Source snapshot run '{run.SourceSnapshotRunId}' could not be loaded for offline analysis run '{run.RunId}'.");

        if (sourceSnapshotRun.ChangeVectorSemanticsSnapshot == null)
        {
            throw new InvalidOperationException(
                $"Source snapshot run '{run.SourceSnapshotRunId}' is missing the persisted change-vector semantics snapshot required for offline analysis.");
        }

        return ChangeVectorSemantics.CloneSnapshot(sourceSnapshotRun.ChangeVectorSemanticsSnapshot);
    }

    throw new InvalidOperationException(
        $"Run mode '{run.RunMode}' requested a change-vector semantics snapshot, but no resolution strategy is defined.");
}

static async Task<RunSelection> SelectSnapshotCacheDownloadRunAsync(
    AppConfig config,
    StateStore stateStore,
    CancellationToken ct)
{
    var head = await stateStore.LoadRunHeadAsync(config.DatabaseName, ct).ConfigureAwait(false);
    if (!string.IsNullOrWhiteSpace(head?.PendingSnapshotCacheRunId))
    {
        var pendingRun = await stateStore.LoadRunAsync(head.PendingSnapshotCacheRunId!, ct).ConfigureAwait(false);
        if (pendingRun != null && ScanExecutionState.CanResumeSnapshotCacheDownload(pendingRun))
        {
            var selection = await SelectSnapshotCacheDownloadRunToResumeAsync(
                    config,
                    stateStore,
                    [ToSnapshotCacheDownloadRunInfo(pendingRun)],
                    allowStartFresh: true,
                    replacementTargetCacheRunId: head.ActiveSnapshotCacheRunId,
                    ct)
                .ConfigureAwait(false);

            if (selection.Run != null)
                await ChooseProcessingNodeForLaunchAsync(config, selection.Run, ct).ConfigureAwait(false);

            return selection;
        }
    }

    var incompleteRuns = await stateStore
        .LoadIncompleteSnapshotCacheDownloadRunsAsync(config.DatabaseName, take: 10, ct)
        .ConfigureAwait(false);
    if (incompleteRuns.Count > 0)
    {
        var selection = await SelectSnapshotCacheDownloadRunToResumeAsync(
                config,
                stateStore,
                incompleteRuns,
                allowStartFresh: true,
                replacementTargetCacheRunId: head?.ActiveSnapshotCacheRunId,
                ct)
            .ConfigureAwait(false);

        if (selection.Run != null)
            await ChooseProcessingNodeForLaunchAsync(config, selection.Run, ct).ConfigureAwait(false);

        return selection;
    }

    if (!string.IsNullOrWhiteSpace(head?.ActiveSnapshotCacheRunId))
    {
        var choice = await new SelectionPrompt<string>()
            .Title(
                $"[bold]An active snapshot cache set already exists for[/] [cyan]{Markup.Escape(config.DatabaseName)}[/]\n" +
                $"[grey]Active cache run:[/] [cyan]{Markup.Escape(head.ActiveSnapshotCacheRunId)}[/]\n" +
                "[yellow]Starting a new cache download will replace it after all configured nodes finish downloading.[/]")
            .AddChoices(
                "Start fresh cache download  [grey]- replace the active cached snapshot set[/]",
                "Cancel")
            .ShowAsync(AnsiConsole.Console, ct)
            .ConfigureAwait(false);

        if (string.Equals(choice, "Cancel", StringComparison.Ordinal))
            return new RunSelection(null, false, Cancelled: true);
    }

    var freshRun = await CreateFreshRunAsync(config, stateStore, ct).ConfigureAwait(false);
    await ChooseProcessingNodeForLaunchAsync(config, freshRun, ct).ConfigureAwait(false);
    return new RunSelection(freshRun, false);
}

static async Task<RunSelection> SelectCachedSnapshotImportRunAsync(
    AppConfig config,
    StateStore stateStore,
    CancellationToken ct)
{
    var head = await stateStore.LoadRunHeadAsync(config.DatabaseName, ct).ConfigureAwait(false);
    if (!string.IsNullOrWhiteSpace(head?.PendingSnapshotCacheRunId))
    {
        throw new InvalidOperationException(
            $"A snapshot cache download run ('{head.PendingSnapshotCacheRunId}') is still pending for database '{config.DatabaseName}'. Finish or discard it by running '{DescribeRunMode(RunMode.DownloadSnapshotsToCache)}' first.");
    }

    if (string.IsNullOrWhiteSpace(head?.ActiveSnapshotCacheRunId))
    {
        throw new InvalidOperationException(
            $"No completed snapshot cache set was found for database '{config.DatabaseName}'. Run '{DescribeRunMode(RunMode.DownloadSnapshotsToCache)}' first.");
    }

    var activeSnapshotCacheRunId = head.ActiveSnapshotCacheRunId!;
    var incompleteRuns = await stateStore
        .LoadIncompleteCachedSnapshotImportRunsAsync(config.DatabaseName, take: 10, ct)
        .ConfigureAwait(false);

    var resumableRuns = incompleteRuns
        .Where(run => string.Equals(run.SourceSnapshotCacheRunId, activeSnapshotCacheRunId, StringComparison.Ordinal))
        .ToList();
    var staleRuns = incompleteRuns
        .Where(run => string.Equals(run.SourceSnapshotCacheRunId, activeSnapshotCacheRunId, StringComparison.Ordinal) == false)
        .ToList();

    if (resumableRuns.Count > 0)
    {
        var selection = await SelectCachedSnapshotImportRunToResumeAsync(
                config,
                stateStore,
                activeSnapshotCacheRunId,
                resumableRuns,
                staleRuns.Count,
                ct)
            .ConfigureAwait(false);

        if (selection.Run != null)
            await ChooseProcessingNodeForLaunchAsync(config, selection.Run, ct).ConfigureAwait(false);

        return selection;
    }

    if (staleRuns.Count > 0)
    {
        var selected = await new SelectionPrompt<string>()
            .Title(
                $"[bold]Unfinished cache-import runs were found for older cache sets[/]\n" +
                $"[grey]Current active cache run:[/] [cyan]{Markup.Escape(activeSnapshotCacheRunId)}[/]\n" +
                $"[yellow]{staleRuns.Count:N0} unfinished cache-import run(s) refer to a replaced cache set and cannot be resumed.[/]")
            .AddChoices(
                "Start fresh cache import from the active cache set",
                "Cancel")
            .ShowAsync(AnsiConsole.Console, ct)
            .ConfigureAwait(false);

        if (string.Equals(selected, "Cancel", StringComparison.Ordinal))
            return new RunSelection(null, false, Cancelled: true);
    }

    if (!string.IsNullOrWhiteSpace(head.ActiveSnapshotRunId))
    {
        var choice = await new SelectionPrompt<string>()
            .Title(
                $"[bold]An active imported snapshot set already exists for[/] [cyan]{Markup.Escape(config.DatabaseName)}[/]\n" +
                $"[grey]Active imported snapshot run:[/] [cyan]{Markup.Escape(head.ActiveSnapshotRunId)}[/]\n" +
                "[yellow]Starting a fresh cache import will replace it after all cached nodes are imported.[/]")
            .AddChoices(
                "Start fresh cache import  [grey]- replace the imported snapshot set[/]",
                "Cancel")
            .ShowAsync(AnsiConsole.Console, ct)
            .ConfigureAwait(false);

        if (string.Equals(choice, "Cancel", StringComparison.Ordinal))
            return new RunSelection(null, false, Cancelled: true);
    }

    var freshRun = await CreateFreshRunAsync(config, stateStore, ct).ConfigureAwait(false);
    freshRun.SourceSnapshotCacheRunId = activeSnapshotCacheRunId;
    await stateStore.SaveRunAsync(freshRun, ct).ConfigureAwait(false);
    await ChooseProcessingNodeForLaunchAsync(config, freshRun, ct).ConfigureAwait(false);
    return new RunSelection(freshRun, false);
}

static async Task<RunSelection> SelectLiveETagScanRunAsync(
    AppConfig config,
    StateStore stateStore,
    CancellationToken ct)
{
    config.LiveETagScanLaunchMode = await ConfigWizard.AskForLiveETagScanLaunchModeAsync(ct).ConfigureAwait(false);
    config.LiveETagClusterExecutionMode = LiveETagClusterExecutionMode.SinglePass;
    config.LiveETagRecurringIntervalMinutes = null;
    config.StartEtag = null;

    if (config.LiveETagScanLaunchMode == LiveETagScanLaunchMode.AllNodesAutomatic)
    {
        config.LiveETagClusterExecutionMode = await ConfigWizard.AskForLiveETagClusterExecutionModeAsync(ct).ConfigureAwait(false);
        if (config.LiveETagClusterExecutionMode == LiveETagClusterExecutionMode.Recurring)
        {
            config.LiveETagRecurringIntervalMinutes = await ConfigWizard.AskForLiveETagRecurringIntervalMinutesAsync(ct).ConfigureAwait(false);
        }

        config.SourceNodeIndex = 0;
        return new RunSelection(null, false, Cancelled: false, UseLiveETagClusterCoordinator: true);
    }

    var incompleteRuns = await stateStore
        .LoadIncompleteAnalysisRunsAsync(config.DatabaseName, RunMode.LiveETagScan, take: 10, ct)
        .ConfigureAwait(false);

    if (incompleteRuns.Count == 1)
    {
        var r = incompleteRuns[0];
        var resumeLabel =
            $"Resume live ETag scan {r.RunId} (Recommended)  [grey]- last saved {DescribeTimestamp(r.LastSavedAt)}, inspected {r.CandidateDocumentsProcessed:N0} docs, mismatches {r.MismatchesFound:N0}, repairs planned {r.RepairsPlanned:N0}[/]";
        var selected = await new SelectionPrompt<string>()
            .Title("[bold]An unfinished live ETag scan run exists[/]")
            .AddChoices(
                resumeLabel,
                "Start fresh live ETag scan",
                "Cancel")
            .ShowAsync(AnsiConsole.Console, ct)
            .ConfigureAwait(false);

        if (string.Equals(selected, "Cancel", StringComparison.Ordinal))
            return new RunSelection(null, false, Cancelled: true);

        if (string.Equals(selected, resumeLabel, StringComparison.Ordinal))
        {
            var existingRun = await stateStore.LoadRunAsync(r.RunId, ct).ConfigureAwait(false)
                              ?? throw new InvalidOperationException(
                                  $"Live ETag scan run '{r.RunId}' was selected for resume, but the run document could not be loaded.");
            if (existingRun.ConfigSnapshot is { } savedConfig)
                config.SourceNodeIndex = savedConfig.SourceNodeIndex;
            return new RunSelection(existingRun, false);
        }
    }
    else if (incompleteRuns.Count > 1)
    {
        var choices = incompleteRuns
            .Select(r => new AnalysisRunResumeChoice(r, StartFresh: false, Cancelled: false))
            .Append(new AnalysisRunResumeChoice(null, StartFresh: true, Cancelled: false))
            .Append(new AnalysisRunResumeChoice(null, StartFresh: false, Cancelled: true))
            .ToList();

        var picked = await new SelectionPrompt<AnalysisRunResumeChoice>()
            .Title("[bold]Select the live ETag scan run to continue[/]")
            .UseConverter(choice =>
            {
                if (choice.Cancelled)
                    return "Cancel";

                if (choice.StartFresh)
                    return "Start fresh live ETag scan";

                var r = choice.Run!;
                return $"{r.RunId}  [grey]- last saved {DescribeTimestamp(r.LastSavedAt)}, inspected {r.CandidateDocumentsProcessed:N0} docs, mismatches {r.MismatchesFound:N0}[/]";
            })
            .AddChoices(choices)
            .ShowAsync(AnsiConsole.Console, ct)
            .ConfigureAwait(false);

        if (picked.Cancelled)
            return new RunSelection(null, false, Cancelled: true);

        if (!picked.StartFresh)
        {
            var existingRun = await stateStore.LoadRunAsync(picked.Run!.RunId, ct).ConfigureAwait(false)
                              ?? throw new InvalidOperationException(
                                  $"Live ETag scan run '{picked.Run.RunId}' was selected for resume, but the run document could not be loaded.");
            if (existingRun.ConfigSnapshot is { } savedConfig)
                config.SourceNodeIndex = savedConfig.SourceNodeIndex;
            return new RunSelection(existingRun, false);
        }
    }

    // Fresh run — ask for source node and start ETag before creating the run
    var (sourceNodeIndex, liveStartEtag) = await ConfigWizard.AskForLiveETagScanOptionsAsync(config.Nodes, ct).ConfigureAwait(false);
    config.SourceNodeIndex = sourceNodeIndex;
    config.StartEtag = liveStartEtag;

    ChangeVectorSemanticsSnapshot? freshSemanticsSnapshot = null;
    if (RunModePolicies.RequiresFreshSemanticsFromCluster(config.RunMode))
        freshSemanticsSnapshot = await ChangeVectorSemanticsResolver.ResolveAsync(config, ct).ConfigureAwait(false);

    var startEtag = config.StartEtag ?? 0L;
    var freshRun = await stateStore.CreateRunAsync(config, startEtag, freshSemanticsSnapshot, ct).ConfigureAwait(false);
    freshRun.SafeRestartEtag = startEtag;
    await stateStore.SaveRunAsync(freshRun, ct).ConfigureAwait(false);
    return new RunSelection(freshRun, false);
}

static async Task<RunSelection> SelectSnapshotCrossCheckRunAsync(
    AppConfig config,
    StateStore stateStore,
    CancellationToken ct)
{
    var incompleteRuns = await stateStore
        .LoadIncompleteAnalysisRunsAsync(config.DatabaseName, RunMode.SnapshotCrossCheck, take: 10, ct)
        .ConfigureAwait(false);

    if (incompleteRuns.Count == 1)
    {
        var r = incompleteRuns[0];
        var resumeLabel =
            $"Resume snapshot cross-check {r.RunId} (Recommended)  [grey]- last saved {DescribeTimestamp(r.LastSavedAt)}, inspected {r.CandidateDocumentsProcessed:N0} docs, mismatches {r.MismatchesFound:N0}, repairs planned {r.RepairsPlanned:N0}[/]";
        var selected = await new SelectionPrompt<string>()
            .Title("[bold]An unfinished snapshot cross-check run exists[/]")
            .AddChoices(resumeLabel, "Start fresh snapshot cross-check", "Cancel")
            .ShowAsync(AnsiConsole.Console, ct)
            .ConfigureAwait(false);

        if (string.Equals(selected, "Cancel", StringComparison.Ordinal))
            return new RunSelection(null, false, Cancelled: true);

        if (string.Equals(selected, resumeLabel, StringComparison.Ordinal))
        {
            var existingRun = await stateStore.LoadRunAsync(r.RunId, ct).ConfigureAwait(false)
                              ?? throw new InvalidOperationException(
                                  $"Snapshot cross-check run '{r.RunId}' was selected for resume, but the run document could not be loaded.");
            return new RunSelection(existingRun, false);
        }
    }
    else if (incompleteRuns.Count > 1)
    {
        var choices = incompleteRuns
            .Select(r => new AnalysisRunResumeChoice(r, StartFresh: false, Cancelled: false))
            .Append(new AnalysisRunResumeChoice(null, StartFresh: true, Cancelled: false))
            .Append(new AnalysisRunResumeChoice(null, StartFresh: false, Cancelled: true))
            .ToList();

        var picked = await new SelectionPrompt<AnalysisRunResumeChoice>()
            .Title("[bold]Select the snapshot cross-check run to continue[/]")
            .UseConverter(choice =>
            {
                if (choice.Cancelled) return "Cancel";
                if (choice.StartFresh) return "Start fresh snapshot cross-check";
                var r = choice.Run!;
                return $"{r.RunId}  [grey]- last saved {DescribeTimestamp(r.LastSavedAt)}, inspected {r.CandidateDocumentsProcessed:N0} docs, mismatches {r.MismatchesFound:N0}[/]";
            })
            .AddChoices(choices)
            .ShowAsync(AnsiConsole.Console, ct)
            .ConfigureAwait(false);

        if (picked.Cancelled)
            return new RunSelection(null, false, Cancelled: true);

        if (!picked.StartFresh)
        {
            var existingRun = await stateStore.LoadRunAsync(picked.Run!.RunId, ct).ConfigureAwait(false)
                              ?? throw new InvalidOperationException(
                                  $"Snapshot cross-check run '{picked.Run.RunId}' was selected for resume, but the run document could not be loaded.");
            return new RunSelection(existingRun, false);
        }
    }

    // Fresh run
    ChangeVectorSemanticsSnapshot? freshSemanticsSnapshot = null;
    if (RunModePolicies.RequiresFreshSemanticsFromCluster(config.RunMode))
        freshSemanticsSnapshot = await ChangeVectorSemanticsResolver.ResolveAsync(config, ct).ConfigureAwait(false);

    var freshRun = await stateStore.CreateRunAsync(config, 0L, freshSemanticsSnapshot, ct).ConfigureAwait(false);
    freshRun.SafeRestartEtag = 0L;  // start at node index 0
    freshRun.SourceSnapshotRunId = null;  // start from beginning of first node
    await stateStore.SaveRunAsync(freshRun, ct).ConfigureAwait(false);
    return new RunSelection(freshRun, false);
}

static async Task<RunSelection> SelectMismatchDecisionFixupRunAsync(
    AppConfig config,
    StateStore stateStore,
    CancellationToken ct)
{
    var crossCheckRuns = await stateStore
        .LoadSnapshotCrossCheckRunsForFixupAsync(config.DatabaseName, take: 20, ct)
        .ConfigureAwait(false);

    if (crossCheckRuns.Count == 0)
    {
        AnsiConsole.MarkupLine("[red]No SnapshotCrossCheck runs found in the state store. Nothing to fix.[/]");
        return new RunSelection(null, false, Cancelled: true);
    }

    var choices = crossCheckRuns
        .Select(r => new AnalysisRunResumeChoice(r, StartFresh: false, Cancelled: false))
        .Append(new AnalysisRunResumeChoice(null, StartFresh: false, Cancelled: true))
        .ToList();

    var picked = await new SelectionPrompt<AnalysisRunResumeChoice>()
        .Title("[bold]Select the SnapshotCrossCheck run to fix[/]  [grey](SkippedAlreadyPlanned → PatchPlannedDryRun)[/]")
        .UseConverter(choice =>
        {
            if (choice.Cancelled) return "Cancel";
            var r = choice.Run!;
            var status = r.MismatchesFound > 0
                ? $"mismatches {r.MismatchesFound:N0}  planned {r.RepairsPlanned:N0}"
                : "no mismatches recorded";
            return $"{r.RunId}  [grey]- {(r.LastSavedAt.HasValue ? DescribeTimestamp(r.LastSavedAt) : "started " + DescribeTimestamp(r.StartedAt))}, inspected {r.CandidateDocumentsProcessed:N0} docs, {status}[/]";
        })
        .AddChoices(choices)
        .ShowAsync(AnsiConsole.Console, ct)
        .ConfigureAwait(false);

    if (picked.Cancelled)
        return new RunSelection(null, false, Cancelled: true);

    var targetRunId = picked.Run!.RunId;
    var freshRun = await stateStore.CreateRunAsync(config, 0L, null, ct).ConfigureAwait(false);
    freshRun.SourceSnapshotRunId = targetRunId;
    await stateStore.SaveRunAsync(freshRun, ct).ConfigureAwait(false);
    return new RunSelection(freshRun, false);
}

static async Task<RunSelection> SelectAnalysisRunAsync(
    AppConfig config,
    StateStore stateStore,
    CancellationToken ct)
{
    var head = await stateStore.LoadRunHeadAsync(config.DatabaseName, ct).ConfigureAwait(false);
    if (!string.IsNullOrWhiteSpace(head?.PendingSnapshotRunId))
    {
        throw new InvalidOperationException(
            $"A snapshot import run ('{head.PendingSnapshotRunId}') is still pending for database '{config.DatabaseName}'. Finish it or discard it by running '{DescribeRunMode(RunMode.ImportSnapshots)}' first.");
    }

    if (string.IsNullOrWhiteSpace(head?.ActiveSnapshotRunId))
    {
        throw new InvalidOperationException(
            $"No completed imported snapshot set was found for database '{config.DatabaseName}'. Run '{DescribeRunMode(RunMode.ImportSnapshots)}' first.");
    }

    var activeSnapshotRunId = head.ActiveSnapshotRunId!;
    var incompleteRuns = await stateStore
        .LoadIncompleteAnalysisRunsAsync(config.DatabaseName, config.RunMode, take: 10, ct)
        .ConfigureAwait(false);

    var resumableRuns = incompleteRuns
        .Where(run => string.Equals(run.SourceSnapshotRunId, activeSnapshotRunId, StringComparison.Ordinal))
        .ToList();
    var staleRuns = incompleteRuns
        .Where(run => string.Equals(run.SourceSnapshotRunId, activeSnapshotRunId, StringComparison.Ordinal) == false)
        .ToList();

    if (resumableRuns.Count > 0)
    {
        return await SelectAnalysisRunToResumeAsync(
                config,
                stateStore,
                activeSnapshotRunId,
                resumableRuns,
                staleRuns.Count,
                ct)
            .ConfigureAwait(false);
    }

    if (staleRuns.Count > 0)
    {
        var selected = await new SelectionPrompt<string>()
            .Title(
                $"[bold]Unfinished analysis runs were found for older snapshot sets[/]\n" +
                $"[grey]Current active snapshot run:[/] [cyan]{Markup.Escape(activeSnapshotRunId)}[/]\n" +
                $"[yellow]{staleRuns.Count:N0} unfinished analysis run(s) refer to a replaced snapshot set and cannot be resumed.[/]")
            .AddChoices(
                "Start fresh analysis from the active snapshot set",
                "Cancel")
            .ShowAsync(AnsiConsole.Console, ct)
            .ConfigureAwait(false);

        if (string.Equals(selected, "Cancel", StringComparison.Ordinal))
            return new RunSelection(null, false, Cancelled: true);
    }

    var freshRun = await CreateFreshRunAsync(config, stateStore, ct).ConfigureAwait(false);
    freshRun.SourceSnapshotRunId = activeSnapshotRunId;
    var activeSnapshotRun = await stateStore.LoadRunAsync(activeSnapshotRunId, ct).ConfigureAwait(false);
    freshRun.SourceSnapshotCacheRunId = activeSnapshotRun?.SourceSnapshotCacheRunId;
    await stateStore.SaveRunAsync(freshRun, ct).ConfigureAwait(false);
    return new RunSelection(freshRun, false);
}

static async Task<RunSelection> SelectDirectSnapshotImportRunToResumeAsync(
    AppConfig config,
    StateStore stateStore,
    IReadOnlyList<ScanResumeRunInfo> incompleteRuns,
    bool allowStartFresh,
    string? replacementTargetSnapshotRunId,
    CancellationToken ct)
{
    var choices = incompleteRuns
        .Select(run => new SnapshotImportResumeChoice(run, StartFresh: false, Cancelled: false))
        .Concat(allowStartFresh ? [new SnapshotImportResumeChoice(null, StartFresh: true, Cancelled: false)] : [])
        .Append(new SnapshotImportResumeChoice(null, StartFresh: false, Cancelled: true))
        .ToList();

    var title =
        $"[bold]An unfinished snapshot-import run exists for[/] [cyan]{Markup.Escape(config.DatabaseName)}[/]" +
        (string.IsNullOrWhiteSpace(replacementTargetSnapshotRunId)
            ? string.Empty
            : $"\n[yellow]Starting fresh will replace active imported snapshot set[/] [cyan]{Markup.Escape(replacementTargetSnapshotRunId)}[/]");

    var selected = await new SelectionPrompt<SnapshotImportResumeChoice>()
        .Title(title)
        .UseConverter(choice =>
        {
            if (choice.Cancelled)
                return "Cancel";

            if (choice.StartFresh)
                return "Discard pending snapshot import and start fresh  [grey]- rebuild the imported snapshot set from all configured nodes[/]";

            var run = choice.Run!;
            return $"Resume snapshot import {run.RunId}  [grey]- last saved {DescribeTimestamp(run.LastSavedAt)}, imported {run.SnapshotDocumentsImported:N0} docs, phase {DescribeScanPhase(run.CurrentPhase)}[/]";
        })
        .AddChoices(choices)
        .ShowAsync(AnsiConsole.Console, ct)
        .ConfigureAwait(false);

    if (selected.Cancelled)
        return new RunSelection(null, false, Cancelled: true);

    if (selected.StartFresh)
    {
        var freshRun = await CreateFreshRunAsync(config, stateStore, ct).ConfigureAwait(false);
        return new RunSelection(freshRun, false);
    }

    var resumeRunId = selected.Run!.RunId;
    var existingRun = await stateStore.LoadRunAsync(resumeRunId, ct).ConfigureAwait(false)
                      ?? throw new InvalidOperationException(
                          $"Snapshot-import run '{resumeRunId}' was selected for resume, but the run document could not be loaded.");

    if (!string.Equals(existingRun.CustomerDatabaseName, config.DatabaseName, StringComparison.OrdinalIgnoreCase))
    {
        throw new InvalidOperationException(
            $"Snapshot-import run '{resumeRunId}' targets database '{existingRun.CustomerDatabaseName}', expected '{config.DatabaseName}'.");
    }

    if (existingRun.RunMode != RunMode.ImportSnapshots)
    {
        throw new InvalidOperationException(
            $"Snapshot-import run '{resumeRunId}' uses mode '{existingRun.RunMode}', which cannot be resumed as '{RunMode.ImportSnapshots}'.");
    }

    return new RunSelection(existingRun, false);
}

static async Task<RunSelection> SelectSnapshotCacheDownloadRunToResumeAsync(
    AppConfig config,
    StateStore stateStore,
    IReadOnlyList<SnapshotCacheDownloadRunInfo> incompleteRuns,
    bool allowStartFresh,
    string? replacementTargetCacheRunId,
    CancellationToken ct)
{
    var choices = incompleteRuns
        .Select(run => new SnapshotCacheDownloadResumeChoice(run, StartFresh: false, Cancelled: false))
        .Concat(allowStartFresh ? [new SnapshotCacheDownloadResumeChoice(null, StartFresh: true, Cancelled: false)] : [])
        .Append(new SnapshotCacheDownloadResumeChoice(null, StartFresh: false, Cancelled: true))
        .ToList();

    var title =
        $"[bold]An unfinished snapshot-cache download run exists for[/] [cyan]{Markup.Escape(config.DatabaseName)}[/]" +
        (string.IsNullOrWhiteSpace(replacementTargetCacheRunId)
            ? string.Empty
            : $"\n[yellow]Starting fresh will replace active cache set[/] [cyan]{Markup.Escape(replacementTargetCacheRunId)}[/]");

    var selected = await new SelectionPrompt<SnapshotCacheDownloadResumeChoice>()
        .Title(title)
        .UseConverter(choice =>
        {
            if (choice.Cancelled)
                return "Cancel";

            if (choice.StartFresh)
                return "Discard pending cache download and start fresh  [grey]- start a new cache run and ignore the old on-disk cache set[/]";

            var run = choice.Run!;
            return $"Resume cache download {run.RunId}  [grey]- last saved {DescribeTimestamp(run.LastSavedAt)}, downloaded {run.DownloadedRows:N0} rows, completed nodes {run.CompletedNodes:N0}/{config.Nodes.Count:N0}, phase {DescribeScanPhase(run.CurrentPhase)}[/]";
        })
        .AddChoices(choices)
        .ShowAsync(AnsiConsole.Console, ct)
        .ConfigureAwait(false);

    if (selected.Cancelled)
        return new RunSelection(null, false, Cancelled: true);

    if (selected.StartFresh)
    {
        var freshRun = await CreateFreshRunAsync(config, stateStore, ct).ConfigureAwait(false);
        return new RunSelection(freshRun, false);
    }

    var resumeRunId = selected.Run!.RunId;
    var existingRun = await stateStore.LoadRunAsync(resumeRunId, ct).ConfigureAwait(false)
                      ?? throw new InvalidOperationException(
                          $"Snapshot-cache download run '{resumeRunId}' was selected for resume, but the run document could not be loaded.");

    if (!string.Equals(existingRun.CustomerDatabaseName, config.DatabaseName, StringComparison.OrdinalIgnoreCase))
    {
        throw new InvalidOperationException(
            $"Snapshot-cache download run '{resumeRunId}' targets database '{existingRun.CustomerDatabaseName}', expected '{config.DatabaseName}'.");
    }

    if (existingRun.RunMode != RunMode.DownloadSnapshotsToCache)
    {
        throw new InvalidOperationException(
            $"Snapshot-cache download run '{resumeRunId}' uses mode '{existingRun.RunMode}', which cannot be resumed as '{RunMode.DownloadSnapshotsToCache}'.");
    }
    return new RunSelection(existingRun, false);
}

static async Task<RunSelection> SelectCachedSnapshotImportRunToResumeAsync(
    AppConfig config,
    StateStore stateStore,
    string activeSnapshotCacheRunId,
    IReadOnlyList<CachedSnapshotImportRunInfo> resumableRuns,
    int staleRunCount,
    CancellationToken ct)
{
    if (resumableRuns.Count == 1)
    {
        var resumeLabel =
            $"Resume cache import run {resumableRuns[0].RunId} (Recommended)  [grey]- last saved {DescribeTimestamp(resumableRuns[0].LastSavedAt)}, phase {DescribeScanPhase(resumableRuns[0].CurrentPhase)}, imported {resumableRuns[0].SnapshotDocumentsImported:N0}, restarts {resumableRuns[0].SnapshotBulkInsertRestarts:N0}, skipped {resumableRuns[0].SnapshotDocumentsSkipped:N0}[/]";
        var selected = await new SelectionPrompt<string>()
            .Title(
                $"[bold]An unfinished cache-import run exists for cache set[/] [cyan]{Markup.Escape(activeSnapshotCacheRunId)}[/]" +
                (staleRunCount == 0
                    ? string.Empty
                    : $"\n[yellow]{staleRunCount:N0} unfinished cache-import run(s) refer to older cache sets and cannot be resumed.[/]"))
            .AddChoices(
                resumeLabel,
                "Start fresh cache import from the active cache set",
                "Cancel")
            .ShowAsync(AnsiConsole.Console, ct)
            .ConfigureAwait(false);

        if (string.Equals(selected, "Cancel", StringComparison.Ordinal))
            return new RunSelection(null, false, Cancelled: true);

        if (string.Equals(selected, resumeLabel, StringComparison.Ordinal))
        {
            var existingRun = await stateStore.LoadRunAsync(resumableRuns[0].RunId, ct).ConfigureAwait(false)
                              ?? throw new InvalidOperationException(
                                  $"Cache-import run '{resumableRuns[0].RunId}' was selected for resume, but the run document could not be loaded.");
            return new RunSelection(existingRun, false);
        }
    }
    else
    {
        var choices = resumableRuns
            .Select(run => new CachedSnapshotImportResumeChoice(run, StartFresh: false, Cancelled: false))
            .Append(new CachedSnapshotImportResumeChoice(null, StartFresh: true, Cancelled: false))
            .Append(new CachedSnapshotImportResumeChoice(null, StartFresh: false, Cancelled: true))
            .ToList();

        var picked = await new SelectionPrompt<CachedSnapshotImportResumeChoice>()
            .Title(
                $"[bold]Select the cache-import run to continue for cache set[/] [cyan]{Markup.Escape(activeSnapshotCacheRunId)}[/]" +
                (staleRunCount == 0
                    ? string.Empty
                    : $"\n[yellow]{staleRunCount:N0} unfinished cache-import run(s) refer to older cache sets and cannot be resumed.[/]"))
            .UseConverter(choice =>
            {
                if (choice.Cancelled)
                    return "Cancel";

                if (choice.StartFresh)
                    return "Start fresh cache import from the active cache set";

                var run = choice.Run!;
                return $"{run.RunId}  [grey]- last saved {DescribeTimestamp(run.LastSavedAt)}, phase {DescribeScanPhase(run.CurrentPhase)}, imported {run.SnapshotDocumentsImported:N0}, restarts {run.SnapshotBulkInsertRestarts:N0}, skipped {run.SnapshotDocumentsSkipped:N0}[/]";
            })
            .AddChoices(choices)
            .ShowAsync(AnsiConsole.Console, ct)
            .ConfigureAwait(false);

        if (picked.Cancelled)
            return new RunSelection(null, false, Cancelled: true);

        if (picked.StartFresh == false)
        {
            var existingRun = await stateStore.LoadRunAsync(picked.Run!.RunId, ct).ConfigureAwait(false)
                              ?? throw new InvalidOperationException(
                                  $"Cache-import run '{picked.Run.RunId}' was selected for resume, but the run document could not be loaded.");
            return new RunSelection(existingRun, false);
        }
    }

    var freshRun = await CreateFreshRunAsync(config, stateStore, ct).ConfigureAwait(false);
    freshRun.SourceSnapshotCacheRunId = activeSnapshotCacheRunId;
    await stateStore.SaveRunAsync(freshRun, ct).ConfigureAwait(false);
    return new RunSelection(freshRun, false);
}

static async Task<RunSelection> SelectAnalysisRunToResumeAsync(
    AppConfig config,
    StateStore stateStore,
    string activeSnapshotRunId,
    IReadOnlyList<AnalysisRunInfo> resumableRuns,
    int staleRunCount,
    CancellationToken ct)
{
    if (resumableRuns.Count == 1)
    {
        var resumeLabel =
            $"Resume analysis run {resumableRuns[0].RunId} (Recommended)  [grey]- last saved {DescribeTimestamp(resumableRuns[0].LastSavedAt)}, phase {DescribeScanPhase(resumableRuns[0].CurrentPhase)}, processed {resumableRuns[0].CandidateDocumentsProcessed:N0}/{resumableRuns[0].CandidateDocumentsFound:N0} candidates[/]";
        var selected = await new SelectionPrompt<string>()
            .Title(
                $"[bold]An unfinished analysis run exists for snapshot set[/] [cyan]{Markup.Escape(activeSnapshotRunId)}[/]" +
                (staleRunCount == 0
                    ? string.Empty
                    : $"\n[yellow]{staleRunCount:N0} unfinished analysis run(s) refer to older snapshot sets and cannot be resumed.[/]"))
            .AddChoices(
                resumeLabel,
                "Start fresh analysis from the active snapshot set",
                "Cancel")
            .ShowAsync(AnsiConsole.Console, ct)
            .ConfigureAwait(false);

        if (string.Equals(selected, "Cancel", StringComparison.Ordinal))
            return new RunSelection(null, false, Cancelled: true);

        if (string.Equals(selected, resumeLabel, StringComparison.Ordinal))
        {
            var existingRun = await stateStore.LoadRunAsync(resumableRuns[0].RunId, ct).ConfigureAwait(false)
                              ?? throw new InvalidOperationException(
                                  $"Analysis run '{resumableRuns[0].RunId}' was selected for resume, but the run document could not be loaded.");
            return new RunSelection(existingRun, false);
        }
    }
    else
    {
        var choices = resumableRuns
            .Select(run => new AnalysisRunResumeChoice(run, StartFresh: false, Cancelled: false))
            .Append(new AnalysisRunResumeChoice(null, StartFresh: true, Cancelled: false))
            .Append(new AnalysisRunResumeChoice(null, StartFresh: false, Cancelled: true))
            .ToList();

        var picked = await new SelectionPrompt<AnalysisRunResumeChoice>()
            .Title(
                $"[bold]Select the analysis run to continue for snapshot set[/] [cyan]{Markup.Escape(activeSnapshotRunId)}[/]" +
                (staleRunCount == 0
                    ? string.Empty
                    : $"\n[yellow]{staleRunCount:N0} unfinished analysis run(s) refer to older snapshot sets and cannot be resumed.[/]"))
            .UseConverter(choice =>
            {
                if (choice.Cancelled)
                    return "Cancel";

                if (choice.StartFresh)
                    return "Start fresh analysis from the active snapshot set";

                var run = choice.Run!;
                return $"{run.RunId}  [grey]- last saved {DescribeTimestamp(run.LastSavedAt)}, phase {DescribeScanPhase(run.CurrentPhase)}, processed {run.CandidateDocumentsProcessed:N0}/{run.CandidateDocumentsFound:N0} candidates, mismatches {run.MismatchesFound:N0}[/]";
            })
            .AddChoices(choices)
            .ShowAsync(AnsiConsole.Console, ct)
            .ConfigureAwait(false);

        if (picked.Cancelled)
            return new RunSelection(null, false, Cancelled: true);

        if (picked.StartFresh == false)
        {
            var existingRun = await stateStore.LoadRunAsync(picked.Run!.RunId, ct).ConfigureAwait(false)
                              ?? throw new InvalidOperationException(
                                  $"Analysis run '{picked.Run.RunId}' was selected for resume, but the run document could not be loaded.");
            return new RunSelection(existingRun, false);
        }
    }

    var freshRun = await CreateFreshRunAsync(config, stateStore, ct).ConfigureAwait(false);
    freshRun.SourceSnapshotRunId = activeSnapshotRunId;
    var activeSnapshotRun = await stateStore.LoadRunAsync(activeSnapshotRunId, ct).ConfigureAwait(false);
    freshRun.SourceSnapshotCacheRunId = activeSnapshotRun?.SourceSnapshotCacheRunId;
    await stateStore.SaveRunAsync(freshRun, ct).ConfigureAwait(false);
    return new RunSelection(freshRun, false);
}

static async Task<string> SelectRepairPlanRunIdAsync(
    AppConfig config,
    StateStore stateStore,
    CancellationToken ct)
{
    var planRuns = await stateStore.LoadCompletedRepairPlanRunsAsync(config.DatabaseName, take: 10, ct).ConfigureAwait(false);
    if (planRuns.Count == 0)
    {
        throw new InvalidOperationException(
            $"No completed repair-plan runs were found for database '{config.DatabaseName}'. Run '{RunMode.DryRunRepair}' or '{RunMode.LiveETagScan}' first.");
    }

    if (planRuns.Count == 1)
    {
        if (string.IsNullOrWhiteSpace(planRuns[0].RunId))
            throw new InvalidOperationException("The only available repair plan has an empty run id and cannot be executed.");

        return planRuns[0].RunId;
    }

    var selected = await new SelectionPrompt<RepairPlanRunInfo>()
        .Title("[bold]Select the saved repair plan to execute[/]")
        .UseConverter(plan =>
            $"{plan.RunId}  [grey]- completed {plan.CompletedAt?.ToString("u") ?? "n/a"}, mismatches {plan.MismatchesFound.GetValueOrDefault():N0}, planned repairs {plan.RepairsPlanned.GetValueOrDefault():N0}[/]")
        .AddChoices(planRuns)
        .ShowAsync(AnsiConsole.Console, ct).ConfigureAwait(false);

    if (string.IsNullOrWhiteSpace(selected.RunId))
        throw new InvalidOperationException("The selected repair plan has an empty run id and cannot be executed.");

    return selected.RunId;
}

static async Task<string?> SelectApplyRunToResumeAsync(
    string sourcePlanRunId,
    IReadOnlyList<ApplyRepairRunInfo> incompleteRuns,
    CancellationToken ct)
{
    if (incompleteRuns.Count == 1)
    {
        var resumeText =
            $"Resume apply run {incompleteRuns[0].RunId}  [grey]- last saved {DescribeTimestamp(incompleteRuns[0].LastSavedAt)}, patched {incompleteRuns[0].RepairsPatchedOnWinner.GetValueOrDefault():N0}, failed {incompleteRuns[0].RepairsFailed.GetValueOrDefault():N0}[/]";
        var selected = await new SelectionPrompt<string>()
            .Title($"[bold]An unfinished apply-run already exists for saved plan[/] [cyan]{Markup.Escape(sourcePlanRunId)}[/]")
            .AddChoices(resumeText, "Cancel")
            .ShowAsync(AnsiConsole.Console, ct)
            .ConfigureAwait(false);

        return string.Equals(selected, "Cancel", StringComparison.Ordinal)
            ? null
            : incompleteRuns[0].RunId;
    }

    var choices = incompleteRuns
        .Select(run => new ApplyRunResumeChoice(run, Cancelled: false))
        .Append(new ApplyRunResumeChoice(null, Cancelled: true))
        .ToList();

    var picked = await new SelectionPrompt<ApplyRunResumeChoice>()
        .Title($"[bold]Multiple unfinished apply-runs exist for saved plan[/] [cyan]{Markup.Escape(sourcePlanRunId)}[/]")
        .UseConverter(choice => choice.Cancelled
            ? "Cancel"
            : $"{choice.Run!.RunId}  [grey]- last saved {DescribeTimestamp(choice.Run.LastSavedAt)}, patched {choice.Run.RepairsPatchedOnWinner.GetValueOrDefault():N0}, failed {choice.Run.RepairsFailed.GetValueOrDefault():N0}[/]")
        .AddChoices(choices)
        .ShowAsync(AnsiConsole.Console, ct)
        .ConfigureAwait(false);

    return picked.Cancelled ? null : picked.Run!.RunId;
}

static string DescribeRunMode(RunMode runMode) => runMode switch
{
    RunMode.ImportSnapshots => "Import snapshots only",
    RunMode.DownloadSnapshotsToCache => "Download snapshots to local cache",
    RunMode.ImportCachedSnapshotsToStateStore => "Import cached snapshots into local RavenDB",
    RunMode.ScanOnly => "Report mismatches from imported snapshots",
    RunMode.DryRunRepair => "Collect repair plan from imported snapshots",
    RunMode.ApplyRepairPlan => "Apply saved repair plan",
    RunMode.ScanAndRepair => "Analyze imported snapshots and repair",
    RunMode.LiveETagScan => "Live ETag scan — build repair plan directly",
    RunMode.SnapshotCrossCheck => "[TEMP] Snapshot cross-check — build repair plan from snapshots",
    RunMode.MismatchDecisionFixup => "[TEMP2] Fix mismatch decisions — restore SkippedAlreadyPlanned to PatchPlannedDryRun",
    _ => runMode.ToString()
};

static string DescribeIdList(IReadOnlyCollection<string> ids)
{
    if (ids.Count == 0)
        return "none";

    return string.Join(", ", ids);
}

static string DescribeTimestamp(DateTimeOffset? timestamp)
    => timestamp is null || timestamp.Value == default
        ? "n/a"
        : timestamp.Value.ToString("u");

static string DescribeScanPhase(ScanExecutionPhase phase) => phase switch
{
    ScanExecutionPhase.SnapshotCacheDownload => "snapshot cache download",
    ScanExecutionPhase.SnapshotImport => "snapshot import",
    ScanExecutionPhase.WaitingForSnapshotIndex => "waiting for snapshot index",
    ScanExecutionPhase.CandidateProcessing => "candidate processing",
    ScanExecutionPhase.Completed => "completed",
    _ => "not started"
};

static void WriteLiveEtagClusterRecoverySummary(
    AppConfig config,
    LiveETagClusterRecoverySummary summary)
{
    var statusMarkup = summary.IsComplete
        ? "[bold green]Complete[/]"
        : "[bold yellow]Interrupted / stopped early[/]";
    var executionModeLine = config.LiveETagClusterExecutionMode == LiveETagClusterExecutionMode.Recurring
        ? $"[grey]Execution mode            :[/] [bold]Recurring[/] [grey](wait {config.LiveETagRecurringIntervalMinutes.GetValueOrDefault():N0} min after each completed full cycle)[/]\n"
        : "[grey]Execution mode            :[/] [bold]Single pass[/]\n";

    var nodeLines = summary.NodeSummaries.Count == 0
        ? "[grey]No node cycles were executed.[/]"
        : string.Join(
            "\n",
            summary.NodeSummaries.Select(node =>
            {
                var applyText = node.ApplySkipped
                    ? "[grey]apply skipped[/]"
                    : node.ApplyRunId == null
                        ? "[grey]apply n/a[/]"
                        : $"apply [cyan]{Markup.Escape(node.ApplyRunId)}[/]";

                return $"[grey]{Markup.Escape(node.NodeLabel),-12}[/]  scan [cyan]{Markup.Escape(node.ScanRunId ?? "n/a")}[/]  {applyText}  start etag [cyan]{node.StartEtag:N0}[/]  inspected [bold]{node.DocumentsInspected:N0}[/]  planned [bold yellow]{node.RepairsPlanned:N0}[/]  applied [bold green]{node.RepairsPatchedOnWinner:N0}[/]/[bold]{node.RepairsAttempted:N0}[/]/[bold red]{node.RepairsFailed:N0}[/]";
            }));

    var summaryBody =
        executionModeLine +
        $"[grey]Node cycles completed     :[/] [bold]{summary.NodesCompleted:N0}/{summary.TotalNodes:N0}[/]\n" +
        $"[grey]Documents inspected      :[/] [bold]{summary.DocumentsInspected:N0}[/]\n" +
        $"[grey]Mismatches found         :[/] [bold red]{summary.MismatchesFound:N0}[/]\n" +
        $"[grey]Repairs planned          :[/] [bold yellow]{summary.RepairsPlanned:N0}[/]\n" +
        $"[grey]Repairs attempted        :[/] [bold]{summary.RepairsAttempted:N0}[/]\n" +
        $"[grey]Repairs patched winner   :[/] [bold green]{summary.RepairsPatchedOnWinner:N0}[/]\n" +
        $"[grey]Repairs failed           :[/] [bold red]{summary.RepairsFailed:N0}[/]\n" +
        $"[grey]Run status               :[/] {statusMarkup}\n" +
        $"[grey]State store              :[/] [cyan]{Markup.Escape(config.StateStore.ServerUrl)}[/] / [cyan]{Markup.Escape(config.StateStore.DatabaseName)}[/]\n" +
        "[grey]Node details             :[/]\n" +
        nodeLines;

    var panel = new Panel(summaryBody)
        .Header("[bold]Run Summary[/]")
        .Border(BoxBorder.Rounded)
        .Padding(1, 0);

    AnsiConsole.Write(panel);
}

static NodeConfig? TryGetSelectedImportNode(AppConfig config)
    => string.IsNullOrWhiteSpace(config.SelectedImportNodeUrl)
        ? null
        : config.Nodes.FirstOrDefault(node =>
            string.Equals(node.Url, config.SelectedImportNodeUrl, StringComparison.OrdinalIgnoreCase));

static async Task ChooseProcessingNodeForLaunchAsync(
    AppConfig config,
    RunStateDocument run,
    CancellationToken ct)
{
    if (config.RunMode == RunMode.ImportSnapshots)
    {
        config.SelectedImportNodeUrl = null;
        return;
    }

    if (config.RunMode is not RunMode.DownloadSnapshotsToCache and not RunMode.ImportCachedSnapshotsToStateStore)
        return;

    var nodeStatesByUrl = config.RunMode == RunMode.DownloadSnapshotsToCache
        ? run.SnapshotCacheNodes.ToDictionary(
            state => state.NodeUrl,
            state => !state.IsDownloadComplete,
            StringComparer.OrdinalIgnoreCase)
        : run.SnapshotImportNodes.ToDictionary(
            state => state.NodeUrl,
            state => !state.IsImportComplete,
            StringComparer.OrdinalIgnoreCase);

    var pendingNodes = config.Nodes
        .Where(node => nodeStatesByUrl.TryGetValue(node.Url, out var isPending) == false || isPending)
        .ToList();

    if (pendingNodes.Count == 0)
        throw new InvalidOperationException(
            config.RunMode == RunMode.DownloadSnapshotsToCache
                ? "All configured nodes are already marked as downloaded for this pending cache run."
                : "All configured nodes are already marked as imported for this pending cache-import run.");

    if (pendingNodes.Count == 1)
    {
        config.SelectedImportNodeUrl = pendingNodes[0].Url;
        return;
    }

    var completedNodes = config.Nodes
        .Where(node => nodeStatesByUrl.TryGetValue(node.Url, out var isPending) && isPending == false)
        .Select(node => node.Label)
        .ToArray();

    var stageTitle = config.RunMode == RunMode.DownloadSnapshotsToCache
        ? "Select the node to download in this launch"
        : "Select the cached node to import in this launch";
    var pendingVerb = config.RunMode == RunMode.DownloadSnapshotsToCache
        ? "download this node now"
        : "import this cached node now";
    var resumeVerb = config.RunMode == RunMode.DownloadSnapshotsToCache
        ? "resume the cached download for this node"
        : "resume the cached import for this node";

    var selectedNode = await new SelectionPrompt<NodeConfig>()
        .Title(
            $"[bold]{stageTitle}[/]\n" +
            (completedNodes.Length == 0
                ? "[grey]This launch will process exactly one node.[/]"
                : $"[grey]Already completed:[/] [cyan]{Markup.Escape(string.Join(", ", completedNodes))}[/]\n[grey]This launch will process exactly one remaining node.[/]"))
        .UseConverter(node =>
        {
            var status = nodeStatesByUrl.TryGetValue(node.Url, out var isPending) && isPending == false
                ? "[green]completed[/]"
                : run.RunMode is RunMode.ImportCachedSnapshotsToStateStore && run.SnapshotImportNodes.Any(state =>
                    string.Equals(state.NodeUrl, node.Url, StringComparison.OrdinalIgnoreCase) &&
                    (state.ImportedRows > 0 || state.LastConfirmedSnapshotId != null))
                    ? resumeVerb
                    : run.RunMode == RunMode.DownloadSnapshotsToCache && run.SnapshotCacheNodes.Any(state =>
                        string.Equals(state.NodeUrl, node.Url, StringComparison.OrdinalIgnoreCase) &&
                        (state.DownloadedRows > 0 || string.IsNullOrWhiteSpace(state.LastDownloadedDocumentId) == false))
                        ? resumeVerb
                        : pendingVerb;

            return $"{node.Label}  [grey]- {status}[/]";
        })
        .AddChoices(pendingNodes)
        .ShowAsync(AnsiConsole.Console, ct)
        .ConfigureAwait(false);

    config.SelectedImportNodeUrl = selectedNode.Url;
}

static SnapshotCacheDownloadRunInfo ToSnapshotCacheDownloadRunInfo(RunStateDocument run) => new()
{
    RunId = string.IsNullOrWhiteSpace(run.RunId) ? run.Id : run.RunId,
    RunMode = run.RunMode,
    StartedAt = run.StartedAt,
    LastSavedAt = run.LastSavedAt == default ? null : run.LastSavedAt,
    CurrentPhase = run.CurrentPhase,
    DownloadedRows = run.SnapshotCacheNodes.Sum(node => node.DownloadedRows),
    CompletedNodes = run.SnapshotCacheNodes.Count(node => node.IsDownloadComplete)
};

file sealed record ResolvedRunContext(
    AppConfig Config,
    StateStore StateStore,
    RunStateDocument? Run,
    bool StartupConnectivityValidated = false);

file sealed record RunSelection(
    RunStateDocument? Run,
    bool Reconfigure,
    bool Cancelled = false,
    bool UseLiveETagClusterCoordinator = false);
file sealed record ApplyRunResumeChoice(ApplyRepairRunInfo? Run, bool Cancelled);
file sealed record AnalysisRunResumeChoice(AnalysisRunInfo? Run, bool StartFresh, bool Cancelled);
file sealed record CachedSnapshotImportResumeChoice(CachedSnapshotImportRunInfo? Run, bool StartFresh, bool Cancelled);
file sealed record SnapshotImportResumeChoice(ScanResumeRunInfo? Run, bool StartFresh, bool Cancelled);
file sealed record SnapshotCacheDownloadResumeChoice(SnapshotCacheDownloadRunInfo? Run, bool StartFresh, bool Cancelled);
file sealed record LiveETagScanResumeChoice(AnalysisRunInfo? Run, bool StartFresh, bool Cancelled);
