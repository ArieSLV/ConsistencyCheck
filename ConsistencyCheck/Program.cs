using ConsistencyCheck;
using Spectre.Console;

// ─────────────────────────────────────────────────────────────────────────────
// Banner
// ─────────────────────────────────────────────────────────────────────────────

AnsiConsole.Write(new FigletText("ConsistencyCheck").Color(Color.CornflowerBlue));
AnsiConsole.MarkupLine("[grey]RavenDB Cluster Consistency Checker  v1.0[/]");
AnsiConsole.MarkupLine("[grey]Compares document change vectors across all cluster nodes.[/]");
AnsiConsole.WriteLine();

// ─────────────────────────────────────────────────────────────────────────────
// Graceful CTRL+C
// Cancellation is signalled here; the current batch will finish before the
// application exits so that progress is never lost.
// ─────────────────────────────────────────────────────────────────────────────

using var cts = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) =>
{
    // Cancel the token instead of terminating immediately.
    e.Cancel = true;
    AnsiConsole.MarkupLine(
        "\n[yellow]Interrupt received — finishing current batch and saving progress…[/]");
    cts.Cancel();
};

// ─────────────────────────────────────────────────────────────────────────────
// Output directory  (./output/ next to the executable)
// ─────────────────────────────────────────────────────────────────────────────

var outputDir = Path.Combine(AppContext.BaseDirectory, "output");
Directory.CreateDirectory(outputDir);

var progressStore = new ProgressStore(outputDir);

// ─────────────────────────────────────────────────────────────────────────────
// Configuration: load saved config or run the setup wizard
// ─────────────────────────────────────────────────────────────────────────────

AppConfig config;
ProgressState progress;

if (progressStore.ConfigExists)
{
    // ── Load and display existing configuration ───────────────────────────────
    try
    {
        config = progressStore.LoadConfig();
    }
    catch (InvalidOperationException ex)
    {
        AnsiConsole.MarkupLine($"[red]Failed to load configuration: {Markup.Escape(ex.Message)}[/]");
        AnsiConsole.MarkupLine("[yellow]Starting the setup wizard to reconfigure…[/]");
        AnsiConsole.WriteLine();

        config = await new ConfigWizard(progressStore).RunAsync(cts.Token);
        progressStore.DeleteProgress();
        progress = new ProgressState();
        goto RunScan;
    }

    // ── Pre-filled config: ask only for missing fields ────────────────────────
    if (!ConfigWizard.IsConfigComplete(config))
    {
        AnsiConsole.MarkupLine(
            "[yellow]Pre-filled configuration found — please provide the missing details.[/]");
        AnsiConsole.WriteLine();
        config = await new ConfigWizard(progressStore).CompleteConfigAsync(config, cts.Token);
        progressStore.DeleteProgress();
        progress = new ProgressState();
        goto RunScan;
    }

    AnsiConsole.MarkupLine("[green]Loaded existing configuration.[/]");
    AnsiConsole.WriteLine();
    ConfigWizard.DisplayConfigSummary(config);
    AnsiConsole.WriteLine();

    // ── Resume / restart / reconfigure prompt ─────────────────────────────────
    if (progressStore.ProgressExists)
    {
        progress = progressStore.LoadProgress();

        if (!progress.IsComplete)
        {
            var resumeLabel = $"Resume from ETag {progress.LastProcessedEtag ?? 0:N0}" +
                              $"  ({progress.DocumentsInspected:N0} docs already inspected," +
                              $" {progress.MismatchesFound:N0} mismatches found)";

            var choice = AnsiConsole.Prompt(
                new SelectionPrompt<string>()
                    .Title("[bold]A previous scan is in progress. What would you like to do?[/]")
                    .AddChoices(resumeLabel, "Restart scan from the beginning", "Reconfigure (run wizard again)"));

            if (choice.StartsWith("Restart"))
            {
                progressStore.DeleteProgress();
                progress = new ProgressState();
            }
            else if (choice.StartsWith("Reconfigure"))
            {
                AnsiConsole.WriteLine();
                config = await new ConfigWizard(progressStore).RunAsync(cts.Token);
                progressStore.DeleteProgress();
                progress = new ProgressState();
            }
            // else: resume — progress is already loaded, use it as-is
        }
        else
        {
            // Previous scan completed normally.
            AnsiConsole.MarkupLine("[green]Previous scan completed.[/] Starting a fresh scan.");
            progressStore.DeleteProgress();
            progress = new ProgressState();
        }
    }
    else
    {
        progress = new ProgressState();
    }
}
else
{
    // First run — no config.json found.
    AnsiConsole.MarkupLine("[yellow]No configuration found. Launching setup wizard…[/]");
    AnsiConsole.WriteLine();
    config   = await new ConfigWizard(progressStore).RunAsync(cts.Token);
    progress = new ProgressState();
}

RunScan:

// ─────────────────────────────────────────────────────────────────────────────
// Scan header
// ─────────────────────────────────────────────────────────────────────────────

AnsiConsole.WriteLine();
AnsiConsole.Write(new Rule("[bold green]Starting consistency scan[/]").RuleStyle("green"));
AnsiConsole.MarkupLine($"  Database  : [cyan]{config.DatabaseName}[/]");
AnsiConsole.MarkupLine($"  Mode      : [cyan]{config.Mode}[/]");
AnsiConsole.MarkupLine($"  Source    : [cyan]{config.SourceNode.Label}[/]  ({config.SourceNode.Url})");
AnsiConsole.MarkupLine($"  Targets   : [cyan]{string.Join(", ", config.TargetNodes.Select(n => n.Label))}[/]");
AnsiConsole.MarkupLine($"  Page size : [cyan]{config.Throttle.PageSize}[/] docs   Delay: [cyan]{config.Throttle.DelayBetweenBatchesMs}[/] ms");
if (progress.LastProcessedEtag.HasValue)
    AnsiConsole.MarkupLine($"  Resuming from ETag [cyan]{progress.LastProcessedEtag}[/]");
AnsiConsole.WriteLine();

// ─────────────────────────────────────────────────────────────────────────────
// Run the checker
// ─────────────────────────────────────────────────────────────────────────────

ProgressState finalState = progress;

await using var writer  = new ResultsWriter(outputDir);
await using var checker = new ConsistencyChecker(config, progressStore, writer);

// Fetch the current highest ETag for the progress bar (best-effort; null = indeterminate).
var maxEtag = await checker.FetchMaxEtagAsync(cts.Token);

try
{
    await AnsiConsole.Progress()
        .AutoClear(false)
        .HideCompleted(false)
        .Columns(
            new SpinnerColumn(),
            new TaskDescriptionColumn { Alignment = Justify.Left },
            new ProgressBarColumn(),
            new PercentageColumn(),
            new ElapsedTimeColumn(),
            new RemainingTimeColumn())
        .StartAsync(async ctx =>
        {
            var task = ctx.AddTask("Scanning…", maxValue: (double)(maxEtag ?? 100));
            if (!maxEtag.HasValue)
                task.IsIndeterminate(true);
            else if (progress.LastProcessedEtag.HasValue)
                task.Value = progress.LastProcessedEtag.Value; // seed resume position

            checker.BatchCompleted += async (inspected, mismatches, etag) =>
            {
                // Live description: running doc count and mismatch count.
                var mismatchText = mismatches > 0
                    ? $"[red]{mismatches:N0} mismatches[/]"
                    : "[green]0 mismatches[/]";
                task.Description = $"[cyan]{inspected:N0}[/] docs  {mismatchText}";

                // If the DB grew beyond our snapshot, re-fetch the ceiling once.
                if (maxEtag.HasValue && etag > maxEtag.Value)
                {
                    var refreshed = await checker.FetchMaxEtagAsync(CancellationToken.None);
                    if (refreshed.HasValue && refreshed.Value > task.MaxValue)
                        task.MaxValue = refreshed.Value;
                    maxEtag = refreshed ?? maxEtag;
                }

                task.Value = maxEtag.HasValue
                    ? Math.Min((double)etag, task.MaxValue)
                    : task.Value + 1; // indeterminate: just tick forward
            };

            finalState = await checker.RunAsync(progress, cts.Token);

            // Ensure the bar reaches 100 % on a clean finish.
            if (finalState.IsComplete)
                task.Value = task.MaxValue;
        });
}
catch (OperationCanceledException)
{
    // CTRL+C — progress has already been saved inside RunAsync.
    AnsiConsole.WriteLine();
    AnsiConsole.MarkupLine("[yellow]Scan interrupted — progress saved.[/]");
}
catch (Exception ex)
{
    AnsiConsole.WriteLine();
    AnsiConsole.MarkupLine($"[red]Unexpected error: {Markup.Escape(ex.Message)}[/]");
    AnsiConsole.WriteException(ex, ExceptionFormats.ShortenPaths | ExceptionFormats.ShowLinks);
}

// ─────────────────────────────────────────────────────────────────────────────
// Final summary
// ─────────────────────────────────────────────────────────────────────────────

AnsiConsole.WriteLine();

var statusMarkup = finalState.IsComplete
    ? "[bold green]Complete[/]"
    : "[bold yellow]Interrupted / stopped early[/]";

var mismatchSummary = finalState.MismatchesFound > 0
    ? $"[bold red]{finalState.MismatchesFound:N0}[/]"
    : "[bold green]0 — cluster is consistent![/]";

var summaryPanel = new Panel(
    $"[grey]Documents inspected :[/] [bold]{finalState.DocumentsInspected:N0}[/]\n" +
    $"[grey]Mismatches found    :[/] {mismatchSummary}\n" +
    $"[grey]Scan status         :[/] {statusMarkup}\n" +
    $"[grey]Output directory    :[/] [cyan]{outputDir}[/]")
    .Header("[bold]Scan Summary[/]")
    .Border(BoxBorder.Rounded)
    .Padding(1, 0);

AnsiConsole.Write(summaryPanel);

if (finalState.MismatchesFound > 0)
{
    AnsiConsole.WriteLine();
    AnsiConsole.MarkupLine(
        $"[yellow]Mismatch details have been written to [cyan]mismatches_*.csv[/] " +
        $"in the output directory.[/]");
}
