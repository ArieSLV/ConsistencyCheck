using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using Raven.Client.Documents;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations;
using Spectre.Console;

namespace ConsistencyCheck;

/// <summary>
/// Interactive configuration wizard for the consistency checker.
/// </summary>
public sealed class ConfigWizard
{
    private readonly ProgressStore _store;

    public ConfigWizard(ProgressStore store)
    {
        _store = store;
    }

    public async Task<AppConfig> RunAsync(CancellationToken ct)
    {
        AnsiConsole.MarkupLine("[bold yellow]Configuration Wizard[/]");
        AnsiConsole.MarkupLine("[grey]Answer each prompt to configure the scan / repair tool.[/]");
        AnsiConsole.MarkupLine("[grey]Press CTRL+C at any time to abort.[/]");
        AnsiConsole.WriteLine();

        PrintStep(1, 8, "Database name");
        var databaseName = await new TextPrompt<string>("  RavenDB [cyan]database name[/]:")
            .Validate(value => string.IsNullOrWhiteSpace(value)
                ? ValidationResult.Error("[red]Database name cannot be empty.[/]")
                : ValidationResult.Success())
            .ShowAsync(AnsiConsole.Console, ct).ConfigureAwait(false);

        AnsiConsole.WriteLine();
        PrintStep(2, 8, "Number of cluster nodes");
        var nodeCount = await new SelectionPrompt<int>()
            .Title("  How many [cyan]nodes[/] does the cluster have?")
            .AddChoices(2, 3, 4, 5)
            .UseConverter(n => n == 3 ? "3 nodes [grey](typical)[/]" : $"{n} nodes")
            .ShowAsync(AnsiConsole.Console, ct).ConfigureAwait(false);

        AnsiConsole.WriteLine();
        PrintStep(3, 8, "Node URLs and labels");
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
                .Validate(value =>
                {
                    if (!Uri.TryCreate(value, UriKind.Absolute, out var uri))
                        return ValidationResult.Error("[red]Not a valid URI.[/]");
                    if (uri.Scheme != "http" && uri.Scheme != "https")
                        return ValidationResult.Error("[red]Scheme must be http or https.[/]");
                    return ValidationResult.Success();
                })
                .ShowAsync(AnsiConsole.Console, ct).ConfigureAwait(false);

            nodes.Add(new NodeConfig
            {
                Label = string.IsNullOrWhiteSpace(label) ? defaultLabel : label,
                Url = url.TrimEnd('/')
            });
        }

        AnsiConsole.WriteLine();
        PrintStep(4, 8, "Client certificate");
        var (certPath, certPassword) = AskForCertificate(ct);

        AnsiConsole.WriteLine();
        PrintStep(5, 8, "TLS validation");
        var allowInvalidServerCertificates = await AskForServerCertificateValidationAsync(nodes, ct).ConfigureAwait(false);

        AnsiConsole.WriteLine();
        PrintStep(6, 8, "Run mode");
        var (runMode, scanMode) = await AskForRunModeAsync(ct).ConfigureAwait(false);
        var applyExecutionMode = runMode == RunMode.ApplyRepairPlan
            ? await AskForApplyExecutionModeAsync(ct).ConfigureAwait(false)
            : ApplyExecutionMode.InteractivePerDocument;

        AnsiConsole.WriteLine();
        PrintStep(7, 8, "Throttling");
        var throttle = await AskForThrottleAsync(ct).ConfigureAwait(false);

        AnsiConsole.WriteLine();
        PrintStep(8, 8, "Local state storage");
        var stateStore = await AskForStateStoreAsync(ct).ConfigureAwait(false);

        var config = new AppConfig
        {
            DatabaseName = databaseName,
            Nodes = nodes,
            SourceNodeIndex = 0,
            CertificatePath = certPath,
            CertificatePassword = certPassword,
            AllowInvalidServerCertificates = allowInvalidServerCertificates,
            RunMode = runMode,
            ApplyExecutionMode = applyExecutionMode,
            Mode = scanMode,
            StartEtag = null,
            Throttle = throttle,
            StateStore = stateStore
        };

        AnsiConsole.WriteLine();
        AnsiConsole.MarkupLine("[bold]Testing connectivity to all cluster nodes...[/]");

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

    internal static bool IsConfigComplete(AppConfig config)
        => !NeedsCertificatePrompt(config)
           && !NeedsTlsValidationPrompt(config)
           && !NeedsRunModePrompt(config)
           && !NeedsThrottlePrompt(config)
           && !NeedsStateStorePrompt(config);

    public async Task<AppConfig> CompleteConfigAsync(AppConfig config, CancellationToken ct)
    {
        config.Throttle ??= new ThrottleConfig();
        config.StateStore ??= new StateStoreConfig();
        if (config.SourceNodeIndex < 0 || config.SourceNodeIndex >= config.Nodes.Count)
            config.SourceNodeIndex = 0;

        AnsiConsole.MarkupLine("[bold yellow]Configuration Completion[/]");
        AnsiConsole.MarkupLine("[grey]A pre-filled configuration was found. Please supply the missing details below.[/]");
        AnsiConsole.WriteLine();

        DisplayConfigSummary(config);
        AnsiConsole.WriteLine();

        if (NeedsCertificatePrompt(config))
        {
            AnsiConsole.MarkupLine("[bold cyan]Certificate[/] — [bold]Client certificate[/]");
            AnsiConsole.WriteLine();
            var (certPath, certPassword) = AskForCertificate(ct);
            config.CertificateThumbprint = null;
            config.CertificatePath = certPath;
            config.CertificatePassword = certPassword;
            config.MissingProperties.Remove("Certificate");
        }

        if (NeedsTlsValidationPrompt(config))
        {
            AnsiConsole.WriteLine();
            AnsiConsole.MarkupLine("[bold cyan]TLS validation[/] — [bold]Server certificate validation[/]");
            AnsiConsole.WriteLine();
            config.AllowInvalidServerCertificates =
                await AskForServerCertificateValidationAsync(config.Nodes, ct).ConfigureAwait(false);
            config.MissingProperties.Remove(nameof(AppConfig.AllowInvalidServerCertificates));
        }

        if (NeedsRunModePrompt(config))
        {
            AnsiConsole.WriteLine();
            AnsiConsole.MarkupLine("[bold cyan]Run mode[/] — [bold]Scan / repair behavior[/]");
            AnsiConsole.WriteLine();
            var (runMode, scanMode) = await AskForRunModeAsync(ct).ConfigureAwait(false);
            config.RunMode = runMode;
            config.Mode = scanMode;
            config.ApplyExecutionMode = runMode == RunMode.ApplyRepairPlan
                ? await AskForApplyExecutionModeAsync(ct).ConfigureAwait(false)
                : ApplyExecutionMode.InteractivePerDocument;
            config.MissingProperties.Remove(nameof(AppConfig.RunMode));
            config.MissingProperties.Remove(nameof(AppConfig.Mode));
        }

        if (NeedsThrottlePrompt(config))
        {
            AnsiConsole.WriteLine();
            AnsiConsole.MarkupLine("[bold cyan]Throttling[/] — [bold]Batch and retry settings[/]");
            AnsiConsole.WriteLine();
            config.Throttle = await AskForThrottleAsync(ct).ConfigureAwait(false);
            config.MissingProperties.Remove(nameof(AppConfig.Throttle));
        }

        if (NeedsStateStorePrompt(config))
        {
            AnsiConsole.WriteLine();
            AnsiConsole.MarkupLine("[bold cyan]Local state storage[/] — [bold]RavenDB state database[/]");
            AnsiConsole.WriteLine();
            config.StateStore = await AskForStateStoreAsync(ct).ConfigureAwait(false);
            config.MissingProperties.Remove(nameof(AppConfig.StateStore));
        }

        AnsiConsole.WriteLine();
        AnsiConsole.MarkupLine("[bold]Testing connectivity to all cluster nodes...[/]");

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

    public async Task<AppConfig> ChooseRunModeForLaunchAsync(AppConfig config, CancellationToken ct)
    {
        config.Throttle ??= new ThrottleConfig();
        config.StateStore ??= new StateStoreConfig();

        AnsiConsole.MarkupLine("[bold yellow]Launch Mode[/]");
        AnsiConsole.MarkupLine("[grey]Choose how this run should behave. This selection applies only to the current launch.[/]");
        AnsiConsole.WriteLine();

        DisplayConfigSummary(config);
        AnsiConsole.WriteLine();

        var (runMode, scanMode) = await AskForRunModeAsync(ct).ConfigureAwait(false);
        config.RunMode = runMode;
        config.Mode = scanMode;
        config.ApplyExecutionMode = runMode == RunMode.ApplyRepairPlan
            ? await AskForApplyExecutionModeAsync(ct).ConfigureAwait(false)
            : ApplyExecutionMode.InteractivePerDocument;

        AnsiConsole.WriteLine();
        AnsiConsole.MarkupLine("[bold]This launch will use:[/]");
        DisplayConfigSummary(config);
        AnsiConsole.WriteLine();

        return config;
    }

    private static async Task<(RunMode RunMode, CheckMode CheckMode)> AskForRunModeAsync(CancellationToken ct)
    {
        const string importSnapshotsOption =
            "Import snapshots only  [grey]- stream all configured nodes directly into the local RavenDB snapshot set[/]";
        const string scanOnlyOption =
            "Report mismatches from imported snapshots  [grey]- wait for the local index and persist findings only[/]";
        const string dryRunRepairOption =
            "Collect repair plan from imported snapshots  [grey]- analyze imported snapshots and persist exact repair actions[/]";
        const string applyRepairPlanOption =
            "Apply saved repair plan  [grey]- execute a previously collected plan without rescanning[/]";
        const string scanAndRepairOption =
            "Analyze imported snapshots and repair  [grey]- analyze imported snapshots and touch the winner copy[/]";
        const string liveETagScanOption =
            "Live ETag scan  [grey]- stream IDs from one node by ETag, fetch live metadata, build repair plan directly[/]";
        const string snapshotCrossCheckOption =
            "[[TEMP]] Snapshot cross-check  [grey]- enumerate all imported snapshots, fetch live metadata, build repair plan (bypasses index)[/]";
        const string mismatchDecisionFixupOption =
            "[[TEMP2]] Fix mismatch decisions  [grey]- restore SkippedAlreadyPlanned mismatches to PatchPlannedDryRun in a SnapshotCrossCheck run[/]";

        var runModeChoice = await new SelectionPrompt<string>()
            .Title("  Select [cyan]run mode[/]:")
            .AddChoices(importSnapshotsOption, scanOnlyOption, dryRunRepairOption, applyRepairPlanOption, scanAndRepairOption, liveETagScanOption, snapshotCrossCheckOption, mismatchDecisionFixupOption)
            .ShowAsync(AnsiConsole.Console, ct).ConfigureAwait(false);

        var runMode = runModeChoice switch
        {
            importSnapshotsOption => RunMode.ImportSnapshots,
            dryRunRepairOption => RunMode.DryRunRepair,
            applyRepairPlanOption => RunMode.ApplyRepairPlan,
            scanAndRepairOption => RunMode.ScanAndRepair,
            liveETagScanOption => RunMode.LiveETagScan,
            snapshotCrossCheckOption => RunMode.SnapshotCrossCheck,
            mismatchDecisionFixupOption => RunMode.MismatchDecisionFixup,
            _ => RunMode.ScanOnly
        };

        if (runMode is RunMode.ImportSnapshots or RunMode.ScanAndRepair or RunMode.DryRunRepair or RunMode.ApplyRepairPlan or RunMode.LiveETagScan or RunMode.SnapshotCrossCheck or RunMode.MismatchDecisionFixup)
            return (runMode, CheckMode.AllMismatches);

        const string firstMismatch =
            "First mismatch  [grey]— stop as soon as one inconsistency is found[/]";
        const string allMismatches =
            "All mismatches  [grey]— scan the entire dataset and collect every finding[/]";

        var scanModeChoice = await new SelectionPrompt<string>()
            .Title("  Select [cyan]scan termination strategy[/]:")
            .AddChoices(firstMismatch, allMismatches)
            .ShowAsync(AnsiConsole.Console, ct).ConfigureAwait(false);

        return (
            runMode,
            scanModeChoice == firstMismatch ? CheckMode.FirstMismatch : CheckMode.AllMismatches);
    }

    private static async Task<ApplyExecutionMode> AskForApplyExecutionModeAsync(CancellationToken ct)
    {
        const string interactiveOption =
            "Interactive per document  [grey]- live-check every document, show node CVs, and wait for confirmation[/]";
        const string automaticOption =
            "Automatic  [grey]- still live-check every document, but continue without per-document prompts[/]";

        var choice = await new SelectionPrompt<string>()
            .Title("  Select [cyan]apply execution mode[/]:")
            .AddChoices(interactiveOption, automaticOption)
            .ShowAsync(AnsiConsole.Console, ct)
            .ConfigureAwait(false);

        return choice == automaticOption
            ? ApplyExecutionMode.Automatic
            : ApplyExecutionMode.InteractivePerDocument;
    }

    internal static async Task<(int SourceNodeIndex, long? StartEtag)> AskForLiveETagScanOptionsAsync(
        List<NodeConfig> nodes,
        CancellationToken ct)
    {
        AnsiConsole.WriteLine();
        AnsiConsole.MarkupLine("  [grey]Live ETag scan options[/]");

        var indexedNodes = nodes.Select((node, i) => (Index: i, Node: node)).ToList();
        var chosen = await new SelectionPrompt<(int Index, NodeConfig Node)>()
            .Title("  Select [cyan]source node[/] (document IDs will be iterated from this node by ETag):")
            .UseConverter(c => $"{c.Node.Label}  [grey]({c.Node.Url})[/]")
            .AddChoices(indexedNodes)
            .ShowAsync(AnsiConsole.Console, ct).ConfigureAwait(false);

        var sourceNodeIndex = chosen.Index;

        var startEtagText = await new TextPrompt<string>("  Start [cyan]ETag[/] [grey](0 = from the beginning, or paste the ETag to continue from)[/]:")
            .DefaultValue("0")
            .Validate(value =>
            {
                if (!long.TryParse(value, out var etag) || etag < 0)
                    return ValidationResult.Error("[red]ETag must be a non-negative integer.[/]");
                return ValidationResult.Success();
            })
            .ShowAsync(AnsiConsole.Console, ct).ConfigureAwait(false);

        long.TryParse(startEtagText, out var startEtag);

        return (sourceNodeIndex, startEtag == 0 ? null : startEtag);
    }

    private static async Task<ThrottleConfig> AskForThrottleAsync(CancellationToken ct)
    {
        AnsiConsole.MarkupLine("  Recommended defaults: PageSize=128, LookupBatchSize=512, Delay=200 ms, Retries=3");

        var useDefaults = await new ConfirmationPrompt("  Use recommended throttle [cyan]defaults[/]?")
            { DefaultValue = true }
            .ShowAsync(AnsiConsole.Console, ct).ConfigureAwait(false);

        if (useDefaults)
            return new ThrottleConfig();

        var pageSize = await new TextPrompt<int>("    Page size [grey](items per node page, 1-1024)[/]:")
            .DefaultValue(128)
            .Validate(value => value is >= 1 and <= 1024
                ? ValidationResult.Success()
                : ValidationResult.Error("[red]Must be between 1 and 1024.[/]"))
            .ShowAsync(AnsiConsole.Console, ct).ConfigureAwait(false);

        var clusterLookupBatchSize = await new TextPrompt<int>("    Cluster lookup batch size [grey](unique IDs, 1-4096)[/]:")
            .DefaultValue(512)
            .Validate(value => value is >= 1 and <= 4096
                ? ValidationResult.Success()
                : ValidationResult.Error("[red]Must be between 1 and 4096.[/]"))
            .ShowAsync(AnsiConsole.Console, ct).ConfigureAwait(false);

        var delayMs = await new TextPrompt<int>("    Delay between pages [grey](ms, 0-60000)[/]:")
            .DefaultValue(200)
            .Validate(value => value is >= 0 and <= 60_000
                ? ValidationResult.Success()
                : ValidationResult.Error("[red]Must be between 0 and 60000.[/]"))
            .ShowAsync(AnsiConsole.Console, ct).ConfigureAwait(false);

        var retries = await new TextPrompt<int>("    Max retries on transient errors [grey](1-10)[/]:")
            .DefaultValue(3)
            .Validate(value => value is >= 1 and <= 10
                ? ValidationResult.Success()
                : ValidationResult.Error("[red]Must be between 1 and 10.[/]"))
            .ShowAsync(AnsiConsole.Console, ct).ConfigureAwait(false);

        return new ThrottleConfig
        {
            PageSize = pageSize,
            ClusterLookupBatchSize = clusterLookupBatchSize,
            DelayBetweenBatchesMs = delayMs,
            MaxRetries = retries,
            RetryBaseDelayMs = 500
        };
    }

    private static async Task<bool> AskForServerCertificateValidationAsync(
        IReadOnlyCollection<NodeConfig> nodes,
        CancellationToken ct)
    {
        if (nodes.Any(node => node.Url.StartsWith("https://", StringComparison.OrdinalIgnoreCase)) == false)
        {
            AnsiConsole.MarkupLine("  [grey]Cluster node URLs use HTTP, so no TLS override is needed.[/]");
            return false;
        }

        return await new ConfirmationPrompt(
                "  Allow [yellow]invalid or self-signed server certificates[/] for cluster nodes? [grey](local test clusters only; unsafe for production)[/]")
            { DefaultValue = false }
            .ShowAsync(AnsiConsole.Console, ct)
            .ConfigureAwait(false);
    }

    private static async Task<StateStoreConfig> AskForStateStoreAsync(CancellationToken ct)
    {
        var defaults = new StateStoreConfig();

        AnsiConsole.MarkupLine(
            $"  Default local RavenDB state store: [cyan]{defaults.ServerUrl}[/] / [cyan]{defaults.DatabaseName}[/]");

        var useDefaults = await new ConfirmationPrompt("  Use the [cyan]default local RavenDB[/] state store?")
            { DefaultValue = true }
            .ShowAsync(AnsiConsole.Console, ct).ConfigureAwait(false);

        if (useDefaults)
        {
            var diagnosticsEnabled = await new ConfirmationPrompt(
                    "  Persist [cyan]structured diagnostics[/] to the state database?")
                { DefaultValue = defaults.EnableDiagnostics }
                .ShowAsync(AnsiConsole.Console, ct).ConfigureAwait(false);

            defaults.EnableDiagnostics = diagnosticsEnabled;
            return defaults;
        }

        var serverUrl = await new TextPrompt<string>("    State store server URL [grey](http/https)[/]:")
            .DefaultValue(defaults.ServerUrl)
            .Validate(value =>
            {
                if (!Uri.TryCreate(value, UriKind.Absolute, out var uri))
                    return ValidationResult.Error("[red]Not a valid URI.[/]");
                if (uri.Scheme != "http" && uri.Scheme != "https")
                    return ValidationResult.Error("[red]Scheme must be http or https.[/]");
                return ValidationResult.Success();
            })
            .ShowAsync(AnsiConsole.Console, ct).ConfigureAwait(false);

        var databaseName = await new TextPrompt<string>("    State database name[/]:")
            .DefaultValue(defaults.DatabaseName)
            .Validate(value => string.IsNullOrWhiteSpace(value)
                ? ValidationResult.Error("[red]Database name cannot be empty.[/]")
                : ValidationResult.Success())
            .ShowAsync(AnsiConsole.Console, ct).ConfigureAwait(false);

        var enableDiagnostics = await new ConfirmationPrompt(
                "    Persist [cyan]structured diagnostics[/] to the state database?")
            { DefaultValue = defaults.EnableDiagnostics }
            .ShowAsync(AnsiConsole.Console, ct).ConfigureAwait(false);

        return new StateStoreConfig
        {
            ServerUrl = serverUrl.TrimEnd('/'),
            DatabaseName = databaseName,
            EnableDiagnostics = enableDiagnostics
        };
    }

    private static (string certPath, string certPassword) AskForCertificate(CancellationToken ct)
    {
        ct.ThrowIfCancellationRequested();

        var needsCert = AnsiConsole.Confirm(
            "  Does the cluster require a [cyan]client certificate[/]?");

        if (!needsCert)
            return (string.Empty, string.Empty);

        AnsiConsole.MarkupLine(
            "  [grey]Tip: you may paste the path with surrounding quotes — they will be stripped automatically.[/]");

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
                AnsiConsole.MarkupLine($"[red]    File not found: {Markup.Escape(input)}[/]");
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

    private static async Task<bool> TestConnectivityAsync(AppConfig config, CancellationToken ct)
    {
        var results = new bool[config.Nodes.Count];
        var certificate = LoadCertificate(config);
        using var certificateValidationScope = RavenCertificateValidationScope.Create(
            config.Nodes.Select(node => node.Url),
            config.AllowInvalidServerCertificates);

        await AnsiConsole.Progress()
            .AutoClear(false)
            .Columns(
                new TaskDescriptionColumn(),
                new SpinnerColumn(),
                new ElapsedTimeColumn())
            .StartAsync(async ctx =>
            {
                var tasks = config.Nodes.Select((node, index) =>
                {
                    var progressTask = ctx.AddTask($"  {node.Label} ({node.Url})");
                    return TestNodeAsync(node, config, certificate, progressTask, ct)
                        .ContinueWith(task =>
                        {
                            results[index] = task.IsCompletedSuccessfully && task.Result;
                        }, TaskScheduler.Default);
                }).ToList();

                await Task.WhenAll(tasks).ConfigureAwait(false);
            }).ConfigureAwait(false);

        for (var i = 0; i < config.Nodes.Count; i++)
        {
            var status = results[i] ? "[green]✓ OK[/]" : "[red]✗ FAILED[/]";
            AnsiConsole.MarkupLine($"  {status}  {Markup.Escape(config.Nodes[i].Label)} ({Markup.Escape(config.Nodes[i].Url)})");
        }

        return results.All(result => result);
    }

    private static async Task<bool> TestNodeAsync(
        NodeConfig node,
        AppConfig config,
        X509Certificate2? certificate,
        ProgressTask progressTask,
        CancellationToken ct)
    {
        try
        {
            using var store = BuildStore(node.Url, config.DatabaseName, certificate);
            await store.Maintenance.SendAsync(new GetStatisticsOperation(), ct).ConfigureAwait(false);

            progressTask.Value = 100;
            progressTask.StopTask();
            return true;
        }
        catch (Exception ex)
        {
            var safeMessage = Markup.Escape(ex.Message.Split('\n')[0]);
            progressTask.Description = $"  [red]{Markup.Escape(node.Label)}: {safeMessage}[/]";
            progressTask.Value = 100;
            progressTask.StopTask();
            return false;
        }
    }

    internal static IDocumentStore BuildStore(string url, string database, X509Certificate2? cert)
    {
        return new DocumentStore
        {
            Urls = [url],
            Database = database,
            Certificate = cert,
            Conventions = new DocumentConventions
            {
                DisableTopologyUpdates = true,
                DisposeCertificate = false,
                CreateHttpClient = handler =>
                {
                    handler.ClientCertificateOptions = ClientCertificateOption.Manual;
                    return new HttpClient(handler, disposeHandler: true);
                }
            }
        }.Initialize();
    }

    internal static X509Certificate2? LoadCertificate(AppConfig config)
    {
        if (!string.IsNullOrWhiteSpace(config.CertificateThumbprint))
            return LoadCertificateFromCurrentUserStore(config.CertificateThumbprint);

        if (string.IsNullOrEmpty(config.CertificatePath))
            return null;

        var password = string.IsNullOrEmpty(config.CertificatePassword)
            ? null
            : config.CertificatePassword;

        var rawBytes = File.ReadAllBytes(config.CertificatePath);
        const X509KeyStorageFlags baseFlags =
            X509KeyStorageFlags.Exportable | X509KeyStorageFlags.PersistKeySet;

        try
        {
            return EnsurePrivateKey(new X509Certificate2(
                rawBytes,
                password,
                baseFlags | X509KeyStorageFlags.MachineKeySet));
        }
        catch (CryptographicException)
        {
            return EnsurePrivateKey(new X509Certificate2(
                rawBytes,
                password,
                baseFlags | X509KeyStorageFlags.UserKeySet));
        }

        static X509Certificate2 EnsurePrivateKey(X509Certificate2 certificate)
        {
            _ = certificate.HasPrivateKey;
            return certificate;
        }

        static X509Certificate2 LoadCertificateFromCurrentUserStore(string thumbprint)
        {
            var normalized = thumbprint.Replace(" ", string.Empty).ToUpperInvariant();

            using var store = new X509Store(StoreName.My, StoreLocation.CurrentUser);
            store.Open(OpenFlags.ReadOnly | OpenFlags.OpenExistingOnly);

            var match = store.Certificates
                .Find(X509FindType.FindByThumbprint, normalized, validOnly: false)
                .OfType<X509Certificate2>()
                .FirstOrDefault(certificate => certificate.HasPrivateKey);

            if (match == null)
            {
                throw new InvalidOperationException(
                    $"Client certificate with thumbprint '{normalized}' was not found in CurrentUser\\My or has no private key.");
            }

            _ = match.HasPrivateKey;
            return match;
        }
    }

    private static bool HasCertificateConfiguration(AppConfig config)
        => config.CertificatePath != null || !string.IsNullOrWhiteSpace(config.CertificateThumbprint);

    private static bool NeedsCertificatePrompt(AppConfig config)
        => !HasCertificateConfiguration(config) || config.MissingProperties.Contains("Certificate");

    private static bool NeedsTlsValidationPrompt(AppConfig config)
        => config.MissingProperties.Contains(nameof(AppConfig.AllowInvalidServerCertificates)) &&
           config.Nodes.Any(node => node.Url.StartsWith("https://", StringComparison.OrdinalIgnoreCase));

    private static bool NeedsRunModePrompt(AppConfig config)
        => config.MissingProperties.Contains(nameof(AppConfig.RunMode)) ||
           config.MissingProperties.Contains(nameof(AppConfig.Mode));

    private static bool NeedsThrottlePrompt(AppConfig config)
        => config.MissingProperties.Contains(nameof(AppConfig.Throttle));

    private static bool NeedsStateStorePrompt(AppConfig config)
        => config.MissingProperties.Contains(nameof(AppConfig.StateStore));

    private static void PrintStep(int step, int total, string title) =>
        AnsiConsole.MarkupLine($"[bold cyan]Step {step}/{total}[/] — [bold]{title}[/]");

    internal static void DisplayConfigSummary(AppConfig config)
    {
        var table = new Table()
            .Border(TableBorder.Rounded)
            .AddColumn("[grey]Setting[/]")
            .AddColumn("[grey]Value[/]");

        table.AddRow("Database", $"[cyan]{Markup.Escape(config.DatabaseName)}[/]");
        table.AddRow(
            "Run mode",
            config.MissingProperties.Contains(nameof(AppConfig.RunMode))
                ? "[yellow]not specified[/]"
                : config.RunMode switch
                {
                    RunMode.ImportSnapshots => "[cyan]Import snapshots only[/]",
                    RunMode.DownloadSnapshotsToCache => "[cyan]Download snapshots to local cache[/]",
                    RunMode.ImportCachedSnapshotsToStateStore => "[cyan]Import cached snapshots into local RavenDB[/]",
                    RunMode.ScanOnly => "[cyan]Report mismatches from imported snapshots[/]",
                    RunMode.DryRunRepair => "[cyan]Collect repair plan from imported snapshots[/]",
                    RunMode.ApplyRepairPlan => "[cyan]Apply saved repair plan[/]",
                    RunMode.ScanAndRepair => "[cyan]Analyze imported snapshots and repair[/]",
                    RunMode.LiveETagScan => "[cyan]Live ETag scan — build repair plan directly[/]",
                    RunMode.SnapshotCrossCheck => "[cyan][[TEMP]] Snapshot cross-check — fetch live metadata for all snapshots[/]",
                    RunMode.MismatchDecisionFixup => "[cyan][[TEMP2]] Fix mismatch decisions — restore SkippedAlreadyPlanned to PatchPlannedDryRun[/]",
                    _ => $"[cyan]{config.RunMode}[/]"
                });
        table.AddRow(
            "Scan mode",
            config.MissingProperties.Contains(nameof(AppConfig.Mode))
                ? "[yellow]not specified[/]"
                : config.RunMode switch
                {
                    RunMode.ImportSnapshots => "[grey]Not applicable (direct import stage only)[/]",
                    RunMode.DownloadSnapshotsToCache => "[grey]Not applicable (download stage only)[/]",
                    RunMode.ImportCachedSnapshotsToStateStore => "[grey]Not applicable (import stage only)[/]",
                    RunMode.ScanAndRepair => "[grey]Analyze imported snapshots and repair[/]",
                    RunMode.DryRunRepair => "[grey]Analyze imported snapshots and collect repair plan[/]",
                    RunMode.ApplyRepairPlan => "[grey]Saved plan execution (no re-scan)[/]",
                    RunMode.LiveETagScan => "[grey]Live scan (no imported snapshots needed)[/]",
                    RunMode.SnapshotCrossCheck => "[grey][[TEMP]] Cross-check snapshots with live cluster metadata[/]",
                    RunMode.MismatchDecisionFixup => "[grey][[TEMP2]] Offline fixup only (no cluster access)[/]",
                    _ => config.Mode == CheckMode.FirstMismatch
                        ? "[cyan]First mismatch[/]"
                        : "[cyan]All mismatches[/]"
                });
        if (config.RunMode == RunMode.ApplyRepairPlan)
        {
            table.AddRow(
                "Apply mode",
                config.ApplyExecutionMode == ApplyExecutionMode.Automatic
                    ? "[yellow]Automatic[/]"
                    : "[green]Interactive per document[/]");
        }

        for (var i = 0; i < config.Nodes.Count; i++)
        {
            var node = config.Nodes[i];
            table.AddRow(
                i == 0 ? "Cluster nodes" : string.Empty,
                $"[cyan]{Markup.Escape(node.Label)}[/]  ({Markup.Escape(node.Url)})");
        }

        table.AddRow(
            "Certificate",
            GetCertificateSummary(config));
        table.AddRow(
            "Server cert validation",
            NeedsTlsValidationPrompt(config)
                ? "[yellow]not specified[/]"
                : config.AllowInvalidServerCertificates
                ? "[yellow]allow invalid (dev/test only)[/]"
                : "[green]strict[/]");

        table.AddRow(
            "Page size",
            NeedsThrottlePrompt(config)
                ? "[yellow]not specified[/]"
                : $"[cyan]{config.Throttle.PageSize}[/] items");
        table.AddRow(
            "Lookup batch",
            NeedsThrottlePrompt(config)
                ? "[yellow]not specified[/]"
                : $"[cyan]{config.Throttle.ClusterLookupBatchSize}[/] unique IDs");
        table.AddRow(
            "Page delay",
            NeedsThrottlePrompt(config)
                ? "[yellow]not specified[/]"
                : $"[cyan]{config.Throttle.DelayBetweenBatchesMs}[/] ms");
        table.AddRow(
            "Max retries",
            NeedsThrottlePrompt(config)
                ? "[yellow]not specified[/]"
                : $"[cyan]{config.Throttle.MaxRetries}[/]");
        table.AddRow(
            "State store",
            NeedsStateStorePrompt(config)
                ? "[yellow]not specified[/]"
                : $"[cyan]{Markup.Escape(config.StateStore.ServerUrl)}[/] / [cyan]{Markup.Escape(config.StateStore.DatabaseName)}[/]");
        table.AddRow(
            "Diagnostics",
            NeedsStateStorePrompt(config)
                ? "[yellow]not specified[/]"
                : config.StateStore.EnableDiagnostics ? "[green]enabled[/]" : "[grey]disabled[/]");

        AnsiConsole.Write(table);
    }

    private static string GetCertificateSummary(AppConfig config)
    {
        if (!string.IsNullOrWhiteSpace(config.CertificateThumbprint))
            return $"[cyan]CurrentUser\\\\My[/]  [grey]{Markup.Escape(config.CertificateThumbprint)}[/]";

        return config.CertificatePath == null
            ? "[yellow]not specified[/]"
            : config.CertificatePath.Length == 0
                ? "[grey]none (open cluster)[/]"
                : $"[cyan]{Markup.Escape(Path.GetFileName(config.CertificatePath))}[/]";
    }
}
