using ConsistencyCheck;
using Spectre.Console;

AnsiConsole.Write(new FigletText("ConsistencyCheck").Color(Color.CornflowerBlue));
AnsiConsole.MarkupLine("[grey]RavenDB Cluster Consistency Checker  v2.0[/]");
AnsiConsole.MarkupLine("[grey]Symmetric full-pass scan with local RavenDB state storage.[/]");
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

var progressStore = new ProgressStore(outputDir);

try
{
    var resolved = await ResolveRunContextAsync(progressStore, cts.Token);
    await using var stateStore = resolved.StateStore;

    var config = resolved.Config;
    var run = resolved.Run;
    var semanticsSnapshot = run.ChangeVectorSemanticsSnapshot
        ?? throw new InvalidOperationException("Run is missing the change-vector semantics snapshot.");

    using var certificateValidationScope = RavenCertificateValidationScope.Create(
        config.Nodes.Select(node => node.Url),
        config.AllowInvalidServerCertificates);

    await RuntimeConnectivityValidator.ValidateAsync(config, cts.Token);

    AnsiConsole.WriteLine();
    AnsiConsole.Write(new Rule("[bold green]Starting symmetric scan[/]").RuleStyle("green"));
    AnsiConsole.MarkupLine($"  Database        : [cyan]{Markup.Escape(config.DatabaseName)}[/]");
    AnsiConsole.MarkupLine($"  Run mode        : [cyan]{DescribeRunMode(config.RunMode)}[/]");
    AnsiConsole.MarkupLine($"  Traversal       : [cyan]Full pass on every node[/]");
    AnsiConsole.MarkupLine($"  Nodes           : [cyan]{string.Join(", ", config.Nodes.Select(node => node.Label))}[/]");
    AnsiConsole.MarkupLine($"  Page size       : [cyan]{config.Throttle.PageSize}[/]   Lookup batch: [cyan]{config.Throttle.ClusterLookupBatchSize}[/]");
    AnsiConsole.MarkupLine($"  Delay           : [cyan]{config.Throttle.DelayBetweenBatchesMs}[/] ms   Retries: [cyan]{config.Throttle.MaxRetries}[/]");
    AnsiConsole.MarkupLine(
        $"  Server certs    : {(config.AllowInvalidServerCertificates ? "[yellow]allow invalid (dev/test only)[/]" : "[green]strict[/]")}");
    AnsiConsole.MarkupLine(
        $"  Explicit unused : [cyan]{Markup.Escape(DescribeIdList(semanticsSnapshot.ExplicitUnusedDatabaseIds))}[/]");
    AnsiConsole.MarkupLine(
        $"  Potential unused: {(semanticsSnapshot.PotentialUnusedDatabaseIds.Count == 0
            ? "[green]none[/]"
            : $"[yellow]{Markup.Escape(DescribeIdList(semanticsSnapshot.PotentialUnusedDatabaseIds))}[/] [grey](warning only)[/]")}");
    AnsiConsole.MarkupLine($"  State store     : [cyan]{Markup.Escape(config.StateStore.ServerUrl)}[/] / [cyan]{Markup.Escape(config.StateStore.DatabaseName)}[/]");
    AnsiConsole.MarkupLine($"  Run ID          : [cyan]{Markup.Escape(run.RunId)}[/]");
    AnsiConsole.WriteLine();

    RunStateDocument finalRun = run;
    await using var checker = new ConsistencyChecker(config, stateStore);

    await AnsiConsole.Progress()
        .AutoClear(false)
        .HideCompleted(false)
        .Columns(
            new SpinnerColumn(),
            new TaskDescriptionColumn { Alignment = Justify.Left },
            new ElapsedTimeColumn())
        .StartAsync(async context =>
        {
            var task = context.AddTask("Scanning…");
            task.IsIndeterminate(true);

            checker.ProgressUpdated += update =>
            {
                var nodeText = string.IsNullOrWhiteSpace(update.CurrentNodeLabel)
                    ? "finishing"
                    : $"node [cyan]{Markup.Escape(update.CurrentNodeLabel)}[/]";

                var repairText = config.RunMode switch
                {
                    RunMode.ScanAndRepair =>
                        $"  repairs [green]{update.RepairsPatchedOnWinner:N0}[/]/[yellow]{update.RepairsAttempted:N0}[/]/[red]{update.RepairsFailed:N0}[/]",
                    RunMode.DryRunRepair =>
                        $"  planned [yellow]{update.RepairsPlanned:N0}[/]",
                    _ => string.Empty
                };

                task.Description =
                    $"{nodeText}  docs [cyan]{update.DocumentsInspected:N0}[/]  versions [cyan]{update.UniqueVersionsCompared:N0}[/]  mismatches [red]{update.MismatchesFound:N0}[/]{repairText}";
            };

            finalRun = await checker.RunAsync(run, cts.Token).ConfigureAwait(false);
            task.Value = task.MaxValue;
        }).ConfigureAwait(false);

    AnsiConsole.WriteLine();
    var statusMarkup = finalRun.IsComplete
        ? "[bold green]Complete[/]"
        : "[bold yellow]Interrupted / stopped early[/]";

    var summary = new Panel(
            $"[grey]Documents inspected      :[/] [bold]{finalRun.DocumentsInspected:N0}[/]\n" +
            $"[grey]Unique versions compared :[/] [bold]{finalRun.UniqueVersionsCompared:N0}[/]\n" +
            $"[grey]Mismatches found         :[/] [bold red]{finalRun.MismatchesFound:N0}[/]\n" +
            $"[grey]Repairs planned         :[/] [bold yellow]{finalRun.RepairsPlanned:N0}[/]\n" +
            $"[grey]Repairs attempted       :[/] [bold]{finalRun.RepairsAttempted:N0}[/]\n" +
            $"[grey]Repairs patched winner  :[/] [bold green]{finalRun.RepairsPatchedOnWinner:N0}[/]\n" +
            $"[grey]Repairs failed          :[/] [bold red]{finalRun.RepairsFailed:N0}[/]\n" +
            $"[grey]Run status              :[/] {statusMarkup}\n" +
            $"[grey]Safe restart ETag       :[/] [cyan]{finalRun.SafeRestartEtag?.ToString("N0") ?? "n/a"}[/]\n" +
            $"[grey]State store             :[/] [cyan]{Markup.Escape(config.StateStore.ServerUrl)}[/] / [cyan]{Markup.Escape(config.StateStore.DatabaseName)}[/]\n" +
            $"[grey]Run ID                  :[/] [cyan]{Markup.Escape(finalRun.RunId)}[/]")
        .Header("[bold]Run Summary[/]")
        .Border(BoxBorder.Rounded)
        .Padding(1, 0);

    AnsiConsole.Write(summary);
}
catch (OperationCanceledException)
{
    AnsiConsole.WriteLine();
    AnsiConsole.MarkupLine("[yellow]Run interrupted. Exact per-node cursors remain saved in the local RavenDB state store.[/]");
}
catch (Exception ex)
{
    AnsiConsole.WriteLine();
    AnsiConsole.MarkupLine($"[red]Unexpected error: {Markup.Escape(ex.Message)}[/]");
    AnsiConsole.WriteException(ex, ExceptionFormats.ShortenPaths | ExceptionFormats.ShowLinks);
}

return;

static async Task<ResolvedRunContext> ResolveRunContextAsync(ProgressStore progressStore, CancellationToken ct)
{
    while (true)
    {
        var config = await LoadOrConfigureConfigAsync(progressStore, ct).ConfigureAwait(false);
        var stateStore = new StateStore(config.StateStore);

        try
        {
            await stateStore.EnsureDatabaseExistsAsync(ct).ConfigureAwait(false);

            var selection = await SelectRunAsync(config, progressStore, stateStore, ct).ConfigureAwait(false);
            if (selection.Reconfigure)
            {
                await stateStore.DisposeAsync().ConfigureAwait(false);
                continue;
            }

            var run = selection.Run!;
            var semanticsSnapshot = await ChangeVectorSemantics.EnsureSnapshotAsync(
                    run,
                    token => ChangeVectorSemanticsResolver.ResolveAsync(config, token),
                    (savedRun, token) => stateStore.SaveRunAsync(savedRun, token),
                    ct)
                .ConfigureAwait(false);

            await stateStore.StoreDiagnosticsAsync(
                    [ChangeVectorSemantics.CreateDiagnostic(run.RunId, semanticsSnapshot)],
                    ct)
                .ConfigureAwait(false);

            return new ResolvedRunContext(config, stateStore, run);
        }
        catch
        {
            await stateStore.DisposeAsync().ConfigureAwait(false);
            throw;
        }
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
        ConfigWizard.DisplayConfigSummary(config);
        AnsiConsole.WriteLine();
        return config;
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
    ProgressStore progressStore,
    StateStore stateStore,
    CancellationToken ct)
{
    var runHead = await stateStore.LoadRunHeadAsync(config.DatabaseName, ct).ConfigureAwait(false);
    RunStateDocument? latestRun = null;
    if (!string.IsNullOrWhiteSpace(runHead?.LatestRunId))
        latestRun = await stateStore.LoadRunAsync(runHead.LatestRunId, ct).ConfigureAwait(false);

    if (latestRun != null && !latestRun.IsComplete)
    {
        var choices = new List<string>
        {
            $"Resume interrupted run  [grey]— exact per-node cursors, {latestRun.DocumentsInspected:N0} docs, {latestRun.MismatchesFound:N0} mismatches[/]"
        };

        if (runHead?.LastCompletedSafeRestartEtag is long completedFrontier)
            choices.Add($"Start from last completed frontier  [grey]— ETag {completedFrontier:N0}[/]");

        choices.Add(GetConfiguredStartLabel(config));
        choices.Add("Start from a specific ETag");
        choices.Add("Reconfigure (run wizard again)");

        var choice = await new SelectionPrompt<string>()
            .Title("[bold]A previous run exists. What would you like to do?[/]")
            .AddChoices(choices)
            .ShowAsync(AnsiConsole.Console, ct).ConfigureAwait(false);

        if (choice.StartsWith("Resume interrupted run", StringComparison.Ordinal))
            return new RunSelection(latestRun, false);

        if (choice.StartsWith("Start from last completed frontier", StringComparison.Ordinal) &&
            runHead?.LastCompletedSafeRestartEtag is long restartEtag)
        {
            var semanticsSnapshot = await ChangeVectorSemanticsResolver.ResolveAsync(config, ct).ConfigureAwait(false);
            var run = await stateStore.CreateRunAsync(config, restartEtag, semanticsSnapshot, ct).ConfigureAwait(false);
            return new RunSelection(run, false);
        }

        if (choice == GetConfiguredStartLabel(config))
        {
            var semanticsSnapshot = await ChangeVectorSemanticsResolver.ResolveAsync(config, ct).ConfigureAwait(false);
            var run = await stateStore.CreateRunAsync(config, config.StartEtag ?? 0, semanticsSnapshot, ct).ConfigureAwait(false);
            return new RunSelection(run, false);
        }

        if (choice.StartsWith("Start from a specific ETag", StringComparison.Ordinal))
        {
            var startEtag = await AskForCustomStartEtagAsync(ct).ConfigureAwait(false);
            var semanticsSnapshot = await ChangeVectorSemanticsResolver.ResolveAsync(config, ct).ConfigureAwait(false);
            var run = await stateStore.CreateRunAsync(config, startEtag, semanticsSnapshot, ct).ConfigureAwait(false);
            return new RunSelection(run, false);
        }

        return new RunSelection(null, true);
    }

    if (runHead?.LastCompletedSafeRestartEtag is long safeRestartEtag)
    {
        var choice = await new SelectionPrompt<string>()
            .Title("[bold]How would you like to start the next run?[/]")
            .AddChoices(
                $"Start from last completed frontier  [grey]— ETag {safeRestartEtag:N0}[/]",
                GetConfiguredStartLabel(config),
                "Start from a specific ETag",
                "Reconfigure (run wizard again)")
            .ShowAsync(AnsiConsole.Console, ct).ConfigureAwait(false);

        if (choice.StartsWith("Start from last completed frontier", StringComparison.Ordinal))
        {
            var semanticsSnapshot = await ChangeVectorSemanticsResolver.ResolveAsync(config, ct).ConfigureAwait(false);
            var run = await stateStore.CreateRunAsync(config, safeRestartEtag, semanticsSnapshot, ct).ConfigureAwait(false);
            return new RunSelection(run, false);
        }

        if (choice == GetConfiguredStartLabel(config))
        {
            var semanticsSnapshot = await ChangeVectorSemanticsResolver.ResolveAsync(config, ct).ConfigureAwait(false);
            var run = await stateStore.CreateRunAsync(config, config.StartEtag ?? 0, semanticsSnapshot, ct).ConfigureAwait(false);
            return new RunSelection(run, false);
        }

        if (choice.StartsWith("Start from a specific ETag", StringComparison.Ordinal))
        {
            var startEtag = await AskForCustomStartEtagAsync(ct).ConfigureAwait(false);
            var semanticsSnapshot = await ChangeVectorSemanticsResolver.ResolveAsync(config, ct).ConfigureAwait(false);
            var run = await stateStore.CreateRunAsync(config, startEtag, semanticsSnapshot, ct).ConfigureAwait(false);
            return new RunSelection(run, false);
        }

        return new RunSelection(null, true);
    }

    var freshSemanticsSnapshot = await ChangeVectorSemanticsResolver.ResolveAsync(config, ct).ConfigureAwait(false);
    var freshRun = await stateStore.CreateRunAsync(config, config.StartEtag ?? 0, freshSemanticsSnapshot, ct).ConfigureAwait(false);
    return new RunSelection(freshRun, false);
}

static string GetConfiguredStartLabel(AppConfig config)
{
    return config.StartEtag switch
    {
        null => "Start from configured start ETag  [grey]— not specified[/]",
        0 => "Start from configured start ETag  [grey]— ETag 0[/]",
        _ => $"Start from configured start ETag  [grey]— ETag {config.StartEtag.Value:N0}[/]"
    };
}

static async Task<long> AskForCustomStartEtagAsync(CancellationToken ct)
{
    return await new TextPrompt<long>("  Enter the [cyan]starting ETag[/]:")
        .Validate(value => value >= 0
            ? ValidationResult.Success()
            : ValidationResult.Error("[red]ETag must be >= 0.[/]"))
        .ShowAsync(AnsiConsole.Console, ct).ConfigureAwait(false);
}

static string DescribeRunMode(RunMode runMode) => runMode switch
{
    RunMode.ScanOnly => "Scan only",
    RunMode.DryRunRepair => "Dry-run repair",
    RunMode.ScanAndRepair => "Scan and repair",
    _ => runMode.ToString()
};

static string DescribeIdList(IReadOnlyCollection<string> ids)
{
    if (ids.Count == 0)
        return "none";

    return string.Join(", ", ids);
}

file sealed record ResolvedRunContext(AppConfig Config, StateStore StateStore, RunStateDocument Run);

file sealed record RunSelection(RunStateDocument? Run, bool Reconfigure);
