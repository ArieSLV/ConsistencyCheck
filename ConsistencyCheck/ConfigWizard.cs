using System.Security.Cryptography.X509Certificates;
using Raven.Client.Documents;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations;
using Spectre.Console;

namespace ConsistencyCheck;

/// <summary>
/// Interactive configuration wizard that guides the user through cluster setup,
/// validates connectivity to every node, and saves the resulting
/// <see cref="AppConfig"/> to disk via <see cref="ProgressStore"/>.
/// </summary>
/// <remarks>
/// The wizard is shown on the very first run (no <c>config.json</c> found) and
/// whenever the user chooses "Reconfigure" at the resume prompt.
/// All prompts are rendered with <c>Spectre.Console</c> for a clean, interactive
/// terminal experience.
/// </remarks>
public sealed class ConfigWizard
{
    private readonly ProgressStore _store;

    /// <summary>
    /// Initialises the wizard backed by the given <paramref name="store"/>.
    /// </summary>
    /// <param name="store">
    /// The persistence store used to save the finished configuration.
    /// </param>
    public ConfigWizard(ProgressStore store)
    {
        _store = store;
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Public entry point
    // ─────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// Runs the full interactive wizard, persists the result via
    /// <see cref="ProgressStore.SaveConfig"/>, and returns the completed
    /// <see cref="AppConfig"/>.
    /// </summary>
    /// <param name="ct">Cancellation token for CTRL+C support.</param>
    /// <returns>The validated and saved application configuration.</returns>
    public async Task<AppConfig> RunAsync(CancellationToken ct)
    {
        AnsiConsole.MarkupLine("[bold yellow]Configuration Wizard[/]");
        AnsiConsole.MarkupLine("[grey]Answer each prompt to configure the consistency checker.[/]");
        AnsiConsole.MarkupLine("[grey]Press CTRL+C at any time to abort.[/]");
        AnsiConsole.WriteLine();

        // ── Step 1: Database name ─────────────────────────────────────────────
        PrintStep(1, 7, "Database name");
        var databaseName = await new TextPrompt<string>("  RavenDB [cyan]database name[/]:")
            .Validate(s => string.IsNullOrWhiteSpace(s)
                ? ValidationResult.Error("[red]Database name cannot be empty.[/]")
                : ValidationResult.Success())
            .ShowAsync(AnsiConsole.Console, ct).ConfigureAwait(false);

        // ── Step 2: Node count ────────────────────────────────────────────────
        AnsiConsole.WriteLine();
        PrintStep(2, 7, "Number of cluster nodes");
        var nodeCount = await new SelectionPrompt<int>()
            .Title("  How many [cyan]nodes[/] does the cluster have?")
            .AddChoices(2, 3, 4, 5)
            .UseConverter(n => n == 3 ? $"3 nodes [grey](typical)[/]" : $"{n} nodes")
            .ShowAsync(AnsiConsole.Console, ct).ConfigureAwait(false);

        // ── Step 3: Node details ──────────────────────────────────────────────
        AnsiConsole.WriteLine();
        PrintStep(3, 7, "Node URLs and labels");
        var nodes = new List<NodeConfig>();
        for (var i = 0; i < nodeCount; i++)
        {
            AnsiConsole.MarkupLine($"  [bold]Node {i + 1} of {nodeCount}[/]");

            var defaultLabel = $"Node {(char)('A' + i)}";
            var label = await new TextPrompt<string>($"    Label [grey](e.g. {defaultLabel})[/]:")
                .DefaultValue(defaultLabel)
                .AllowEmpty()
                .ShowAsync(AnsiConsole.Console, ct).ConfigureAwait(false);

            var url = await new TextPrompt<string>($"    URL   [grey](e.g. https://node-a:8080)[/]:")
                .Validate(s =>
                {
                    if (!Uri.TryCreate(s, UriKind.Absolute, out var uri))
                        return ValidationResult.Error("[red]Not a valid URI.[/]");
                    if (uri.Scheme != "http" && uri.Scheme != "https")
                        return ValidationResult.Error("[red]Scheme must be http or https.[/]");
                    return ValidationResult.Success();
                })
                .ShowAsync(AnsiConsole.Console, ct).ConfigureAwait(false);

            // Normalise: strip trailing slash
            nodes.Add(new NodeConfig
            {
                Label = string.IsNullOrWhiteSpace(label) ? defaultLabel : label,
                Url   = url.TrimEnd('/')
            });
        }

        // ── Step 4: Source node ───────────────────────────────────────────────
        AnsiConsole.WriteLine();
        PrintStep(4, 7, "Source node (iteration origin)");
        AnsiConsole.MarkupLine("  [grey]Documents are iterated by ETag from this node.[/]");
        AnsiConsole.MarkupLine("  [grey]All other nodes are checked against the source.[/]");

        var sourceChoice = await new SelectionPrompt<string>()
            .Title("  Select the [cyan]source node[/]:")
            .AddChoices(nodes.Select(n => $"{n.Label}  ({n.Url})"))
            .ShowAsync(AnsiConsole.Console, ct).ConfigureAwait(false);

        var sourceIndex = nodes.FindIndex(n => sourceChoice.StartsWith(n.Label));

        // ── Step 5: Certificate ───────────────────────────────────────────────
        AnsiConsole.WriteLine();
        PrintStep(5, 7, "Client certificate");

        var (certPath, certPassword) = AskForCertificate(ct);

        // ── Step 6: Scan mode ─────────────────────────────────────────────────
        AnsiConsole.WriteLine();
        PrintStep(6, 7, "Scan mode");

        const string optionFirst  = "First mismatch  [grey]— stop as soon as one inconsistency is found (fast triage)[/]";
        const string optionAll    = "All mismatches  [grey]— scan the entire database (comprehensive report)[/]";

        var modeChoice = await new SelectionPrompt<string>()
            .Title("  Select [cyan]scan mode[/]:")
            .AddChoices(optionFirst, optionAll)
            .ShowAsync(AnsiConsole.Console, ct).ConfigureAwait(false);

        var mode = modeChoice.StartsWith("First") ? CheckMode.FirstMismatch : CheckMode.AllMismatches;

        // ── Step 7: Throttle ──────────────────────────────────────────────────
        AnsiConsole.WriteLine();
        PrintStep(7, 7, "Throttling (production safety)");
        AnsiConsole.MarkupLine("  Recommended defaults: PageSize=128, Delay=200 ms, Retries=3");

        var useDefaults = await new ConfirmationPrompt("  Use recommended throttle [cyan]defaults[/]?")
            { DefaultValue = true }
            .ShowAsync(AnsiConsole.Console, ct).ConfigureAwait(false);

        ThrottleConfig throttle;
        if (useDefaults)
        {
            throttle = new ThrottleConfig();
        }
        else
        {
            var pageSize = await new TextPrompt<int>("    Page size [grey](documents per batch, 1-1024)[/]:")
                .DefaultValue(128)
                .Validate(v => v is >= 1 and <= 1024
                    ? ValidationResult.Success()
                    : ValidationResult.Error("[red]Must be between 1 and 1024.[/]"))
                .ShowAsync(AnsiConsole.Console, ct).ConfigureAwait(false);

            var delayMs = await new TextPrompt<int>("    Delay between batches [grey](ms, 0-60000)[/]:")
                .DefaultValue(200)
                .Validate(v => v is >= 0 and <= 60_000
                    ? ValidationResult.Success()
                    : ValidationResult.Error("[red]Must be between 0 and 60000.[/]"))
                .ShowAsync(AnsiConsole.Console, ct).ConfigureAwait(false);

            var retries = await new TextPrompt<int>("    Max retries on transient errors [grey](1-10)[/]:")
                .DefaultValue(3)
                .Validate(v => v is >= 1 and <= 10
                    ? ValidationResult.Success()
                    : ValidationResult.Error("[red]Must be between 1 and 10.[/]"))
                .ShowAsync(AnsiConsole.Console, ct).ConfigureAwait(false);

            throttle = new ThrottleConfig
            {
                PageSize             = pageSize,
                DelayBetweenBatchesMs = delayMs,
                MaxRetries           = retries,
                RetryBaseDelayMs     = 500
            };
        }

        // ── Assemble config ───────────────────────────────────────────────────
        var config = new AppConfig
        {
            DatabaseName      = databaseName,
            Nodes             = nodes,
            SourceNodeIndex   = sourceIndex,
            CertificatePath   = certPath,
            CertificatePassword = certPassword,
            Mode              = mode,
            Throttle          = throttle
        };

        // ── Step 8: Connectivity test ─────────────────────────────────────────
        AnsiConsole.WriteLine();
        AnsiConsole.MarkupLine("[bold]Testing connectivity to all nodes...[/]");

        var allOk = await TestConnectivityAsync(config, ct).ConfigureAwait(false);

        if (!allOk)
        {
            AnsiConsole.WriteLine();
            var proceed = await new ConfirmationPrompt(
                    "  [yellow]One or more nodes failed the connectivity test.[/] Continue anyway?")
                { DefaultValue = false }
                .ShowAsync(AnsiConsole.Console, ct).ConfigureAwait(false);

            if (!proceed)
            {
                AnsiConsole.MarkupLine("[red]Setup cancelled.[/]");
                throw new OperationCanceledException("User cancelled after connectivity failure.");
            }
        }

        // ── Step 9: Save and display summary ──────────────────────────────────
        _store.SaveConfig(config);

        AnsiConsole.WriteLine();
        DisplayConfigSummary(config);
        AnsiConsole.MarkupLine("[green]Configuration saved.[/]");
        AnsiConsole.WriteLine();

        return config;
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Completion wizard (pre-filled config support)
    // ─────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// Returns <c>true</c> when the configuration is fully specified and the scan can
    /// start without additional wizard prompts.
    /// </summary>
    /// <remarks>
    /// A <c>null</c> <see cref="AppConfig.CertificatePath"/> signals that a support
    /// engineer pre-filled the config but intentionally left the certificate for the
    /// customer to supply. An empty string means "no certificate required."
    /// </remarks>
    internal static bool IsConfigComplete(AppConfig config)
        => config.CertificatePath != null;

    /// <summary>
    /// Completes a partially pre-filled <see cref="AppConfig"/> by asking only for
    /// fields that were intentionally left unspecified (currently: the certificate).
    /// Used when a support engineer ships <c>config.json</c> with cluster details
    /// already filled in but without the customer's certificate path.
    /// </summary>
    /// <param name="config">
    /// The partial configuration loaded from disk. Modified in-place and saved.
    /// </param>
    /// <param name="ct">Cancellation token for CTRL+C support.</param>
    /// <returns>The completed and saved configuration.</returns>
    public async Task<AppConfig> CompleteConfigAsync(AppConfig config, CancellationToken ct)
    {
        AnsiConsole.MarkupLine("[bold yellow]Configuration Completion[/]");
        AnsiConsole.MarkupLine(
            "[grey]A pre-filled configuration was found. " +
            "Please supply the missing details below.[/]");
        AnsiConsole.WriteLine();

        DisplayConfigSummary(config);
        AnsiConsole.WriteLine();

        if (config.CertificatePath == null)
        {
            AnsiConsole.MarkupLine("[bold cyan]Certificate[/] — [bold]Client certificate[/]");
            AnsiConsole.WriteLine();
            var (certPath, certPassword) = AskForCertificate(ct);
            config.CertificatePath   = certPath;
            config.CertificatePassword = certPassword;
        }

        // ── Connectivity test ─────────────────────────────────────────────────
        AnsiConsole.WriteLine();
        AnsiConsole.MarkupLine("[bold]Testing connectivity to all nodes...[/]");

        var allOk = await TestConnectivityAsync(config, ct).ConfigureAwait(false);

        if (!allOk)
        {
            AnsiConsole.WriteLine();
            var proceed = await new ConfirmationPrompt(
                    "  [yellow]One or more nodes failed the connectivity test.[/] Continue anyway?")
                { DefaultValue = false }
                .ShowAsync(AnsiConsole.Console, ct).ConfigureAwait(false);

            if (!proceed)
            {
                AnsiConsole.MarkupLine("[red]Setup cancelled.[/]");
                throw new OperationCanceledException("User cancelled after connectivity failure.");
            }
        }

        _store.SaveConfig(config);

        AnsiConsole.WriteLine();
        DisplayConfigSummary(config);
        AnsiConsole.MarkupLine("[green]Configuration saved.[/]");
        AnsiConsole.WriteLine();

        return config;
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Private helpers
    // ─────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// Interactively asks whether the cluster needs a certificate, and if so collects
    /// the file path (with automatic quote stripping and native cursor movement via
    /// <see cref="Console.ReadLine"/>) and the certificate password.
    /// </summary>
    /// <returns>
    /// A tuple of (<c>certPath</c>, <c>certPassword</c>).
    /// <c>certPath</c> is <see cref="string.Empty"/> when no certificate is required.
    /// </returns>
    private static (string certPath, string certPassword) AskForCertificate(CancellationToken ct)
    {
        ct.ThrowIfCancellationRequested();
        var needsCert = AnsiConsole.Confirm(
            "  Does the cluster require a [cyan]client certificate[/]?");

        if (!needsCert)
            return (string.Empty, string.Empty);

        AnsiConsole.MarkupLine(
            "  [grey]Tip: you may paste the path with surrounding quotes — " +
            "they will be stripped automatically.[/]");

        string certPath;
        while (true)
        {
            AnsiConsole.Markup("    Path to certificate [grey](.pfx / .p12)[/]: ");
            var input = (Console.ReadLine() ?? string.Empty).Trim('"');
            ct.ThrowIfCancellationRequested();

            if (string.IsNullOrWhiteSpace(input))
            {
                AnsiConsole.MarkupLine("[red]    Path cannot be empty.[/]");
                continue;
            }

            if (!File.Exists(input))
            {
                AnsiConsole.MarkupLine($"[red]    File not found: {input}[/]");
                continue;
            }

            certPath = input;
            break;
        }

        var certPassword = AnsiConsole.Prompt(
            new TextPrompt<string>("    Certificate password [grey](leave blank if none)[/]:")
                .AllowEmpty()
                .Secret());

        return (certPath, certPassword);
    }

    /// <summary>
    /// Performs a lightweight connectivity test against every configured node by
    /// creating a temporary <see cref="IDocumentStore"/> (with
    /// <c>DisableTopologyUpdates=true</c>) and executing an empty
    /// <see cref="GetDocumentsCommand"/> as a ping.
    /// </summary>
    /// <returns>
    /// <c>true</c> if every node responded successfully; <c>false</c> if any node failed.
    /// </returns>
    private static async Task<bool> TestConnectivityAsync(AppConfig config, CancellationToken ct)
    {
        X509Certificate2? cert = LoadCertificate(config);

        var results = new bool[config.Nodes.Count];

        await AnsiConsole.Progress()
            .AutoClear(false)
            .Columns(
                new TaskDescriptionColumn(),
                new SpinnerColumn(),
                new ElapsedTimeColumn())
            .StartAsync(async ctx =>
            {
                var tasks = config.Nodes.Select((node, i) =>
                {
                    var progressTask = ctx.AddTask($"  {node.Label} ({node.Url})");
                    return TestNodeAsync(node, config.DatabaseName, cert, progressTask, ct)
                        .ContinueWith(t =>
                        {
                            results[i] = t.IsCompletedSuccessfully && t.Result;
                        }, TaskScheduler.Default);
                }).ToList();

                await Task.WhenAll(tasks).ConfigureAwait(false);
            }).ConfigureAwait(false);

        cert?.Dispose();

        // Print per-node result
        for (var i = 0; i < config.Nodes.Count; i++)
        {
            var node   = config.Nodes[i];
            var status = results[i] ? "[green]✓ OK[/]" : "[red]✗ FAILED[/]";
            AnsiConsole.MarkupLine($"  {status}  {node.Label} ({node.Url})");
        }

        return results.All(r => r);
    }

    /// <summary>
    /// Tests connectivity to a single node. Returns <c>true</c> on success.
    /// </summary>
    private static async Task<bool> TestNodeAsync(
        NodeConfig node,
        string database,
        X509Certificate2? cert,
        ProgressTask progressTask,
        CancellationToken ct)
    {
        try
        {
            using var store = BuildStore(node.Url, database, cert);

            // GetStatisticsOperation is a lightweight GET to /databases/{db}/stats —
            // the lightest SDK call that confirms the node is alive and the database exists.
            await store.Maintenance.SendAsync(new GetStatisticsOperation(), ct)
                       .ConfigureAwait(false);

            progressTask.Value = 100;
            progressTask.StopTask();
            return true;
        }
        catch (Exception ex)
        {
            // Escape the message: RavenDB exceptions can contain square-bracket tokens
            // (e.g. "[TIOAdapter]") that Spectre.Console would misinterpret as markup.
            var safeMsg = Markup.Escape(ex.Message.Split('\n')[0]);
            progressTask.Description = $"  [red]{Markup.Escape(node.Label)}: {safeMsg}[/]";
            progressTask.Value = 100;
            progressTask.StopTask();
            return false;
        }
    }

    /// <summary>
    /// Creates and initialises a pinned <see cref="IDocumentStore"/> for a single node.
    /// <c>DisableTopologyUpdates=true</c> is critical: it prevents the client from
    /// overwriting the pinned URL with the full cluster topology, which would cause
    /// requests to be routed to unintended nodes.
    /// </summary>
    internal static IDocumentStore BuildStore(string url, string database, X509Certificate2? cert)
    {
        return new DocumentStore
        {
            Urls        = [url],
            Database    = database,
            Certificate = cert,
            Conventions = new DocumentConventions
            {
                DisableTopologyUpdates = true
            }
        }.Initialize();
    }

    /// <summary>
    /// Loads the client certificate from the paths specified in
    /// <paramref name="config"/>. Returns <c>null</c> if no certificate is configured.
    /// </summary>
    internal static X509Certificate2? LoadCertificate(AppConfig config)
    {
        if (string.IsNullOrEmpty(config.CertificatePath))
            return null;

        // Exportable: marks the private key as exportable so Windows SChannel (SCHANNEL.dll)
        // can access the key material during TLS client-auth handshakes. Without this flag,
        // the key is imported as non-exportable and TLS may fail with
        // "m_safeCertContext is an invalid handle" or "No credentials are available".
        const X509KeyStorageFlags certFlags = X509KeyStorageFlags.Exportable;

        return string.IsNullOrEmpty(config.CertificatePassword)
            ? new X509Certificate2(config.CertificatePath, (string?)null, certFlags)
            : new X509Certificate2(config.CertificatePath, config.CertificatePassword, certFlags);
    }

    /// <summary>Prints a numbered step header to the console.</summary>
    private static void PrintStep(int step, int total, string title) =>
        AnsiConsole.MarkupLine($"[bold cyan]Step {step}/{total}[/] — [bold]{title}[/]");

    /// <summary>Renders a summary table of the completed configuration.</summary>
    internal static void DisplayConfigSummary(AppConfig config)
    {
        var table = new Table()
            .Border(TableBorder.Rounded)
            .AddColumn("[grey]Setting[/]")
            .AddColumn("[grey]Value[/]");

        table.AddRow("Database",    $"[cyan]{config.DatabaseName}[/]");
        table.AddRow("Scan mode",   $"[cyan]{config.Mode}[/]");
        table.AddRow("Source node", $"[cyan]{config.SourceNode.Label}[/]  ({config.SourceNode.Url})");

        var targets = config.TargetNodes.ToList();
        for (var i = 0; i < targets.Count; i++)
            table.AddRow(i == 0 ? "Target node(s)" : string.Empty,
                $"[cyan]{targets[i].Label}[/]  ({targets[i].Url})");

        table.AddRow("Certificate",
            config.CertificatePath == null    ? "[yellow]not specified[/]" :
            config.CertificatePath.Length == 0 ? "[grey]none (open cluster)[/]" :
                                                 $"[cyan]{Path.GetFileName(config.CertificatePath)}[/]");

        table.AddRow("Page size",   $"[cyan]{config.Throttle.PageSize}[/] docs/batch");
        table.AddRow("Batch delay", $"[cyan]{config.Throttle.DelayBetweenBatchesMs}[/] ms");
        table.AddRow("Max retries", $"[cyan]{config.Throttle.MaxRetries}[/]");

        AnsiConsole.Write(table);
    }
}