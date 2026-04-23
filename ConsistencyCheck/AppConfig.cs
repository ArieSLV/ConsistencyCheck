using System.Text.Json.Serialization;

namespace ConsistencyCheck;

// ─────────────────────────────────────────────────────────────────────────────
// Enumerations
// ─────────────────────────────────────────────────────────────────────────────

/// <summary>
/// Defines the strategy used to terminate a consistency scan.
/// </summary>
public enum CheckMode
{
    /// <summary>
    /// Stop immediately after the first mismatch or missing document is detected.
    /// Useful for a fast triage check: "is the cluster inconsistent at all?"
    /// </summary>
    FirstMismatch,

    /// <summary>
    /// Scan the entire database and collect every mismatch or missing document.
    /// Produces a comprehensive report suitable for remediation planning.
    /// </summary>
    AllMismatches
}

/// <summary>
/// Defines the top-level behavior of the tool.
/// </summary>
public enum RunMode
{
    /// <summary>
    /// Legacy direct-import mode kept only for backward compatibility with older
    /// persisted configs and runs. New launches should use
    /// <see cref="DownloadSnapshotsToCache"/> and
    /// <see cref="ImportCachedSnapshotsToStateStore"/> instead.
    /// </summary>
    ImportSnapshots,

    /// <summary>
    /// Download metadata-only snapshots from one selected customer node into the local
    /// segmented file cache without touching the local RavenDB snapshot collection.
    /// </summary>
    DownloadSnapshotsToCache,

    /// <summary>
    /// Import one selected cached node snapshot stream from the local file cache into the
    /// local RavenDB state store.
    /// </summary>
    ImportCachedSnapshotsToStateStore,

    /// <summary>
    /// Analyze an already imported snapshot set and persist detected inconsistencies
    /// without modifying the customer cluster.
    /// </summary>
    ScanOnly,

    /// <summary>
    /// Analyze an already imported snapshot set and build the exact repair plan that
    /// would be executed, but never send patch operations to the customer cluster.
    /// </summary>
    DryRunRepair,

    /// <summary>
    /// Execute a previously collected repair plan without re-scanning the cluster.
    /// The plan must already exist in the local state store.
    /// </summary>
    ApplyRepairPlan,

    /// <summary>
    /// Analyze an already imported snapshot set and immediately try to repair
    /// resolvable documents by touching the winner copy so replication re-fans
    /// the document out.
    /// </summary>
    ScanAndRepair,

    /// <summary>
    /// Stream document IDs from one node in ETag order, fetch metadata live from all
    /// nodes, and build a repair plan directly — without importing snapshots first.
    /// Useful for catching new inconsistencies that appeared after the last snapshot run.
    /// </summary>
    LiveETagScan,

    /// <summary>
    /// [TEMPORARY] Enumerate all already-imported snapshots node by node (A→B→C),
    /// fetch live metadata from the cluster for each document, and build a repair plan.
    /// Skips documents that were successfully repaired in any previous run.
    /// Bypasses the <c>NodeDocumentSnapshots/ProblemDocuments</c> index entirely.
    /// </summary>
    SnapshotCrossCheck,

    /// <summary>
    /// [TEMPORARY] Fix MismatchDocuments where RepairDecision was incorrectly set to
    /// "SkippedAlreadyPlanned" due to duplicate cross-node encounters in a SnapshotCrossCheck run.
    /// Restores affected documents to "PatchPlannedDryRun" so they appear in the repair plan.
    /// </summary>
    MismatchDecisionFixup
}

/// <summary>
/// Controls how a saved repair plan is executed.
/// </summary>
public enum ApplyExecutionMode
{
    /// <summary>
    /// Show one document at a time, wait for operator confirmation, and only switch to
    /// automatic execution when explicitly requested.
    /// </summary>
    InteractivePerDocument,

    /// <summary>
    /// Apply the saved plan without per-document confirmation prompts.
    /// </summary>
    Automatic
}

/// <summary>
/// Controls how <see cref="RunMode.LiveETagScan"/> is launched for the current run.
/// </summary>
public enum LiveETagScanLaunchMode
{
    /// <summary>
    /// Ask the operator for one source node and an optional manual start ETag.
    /// </summary>
    SingleNodeManual,

    /// <summary>
    /// Iterate all configured nodes sequentially. For each node, scan first, then
    /// immediately apply that node's repair plan before moving to the next node.
    /// </summary>
    AllNodesAutomatic
}

/// <summary>
/// Controls whether automatic all-node Live ETag recovery stops after one full pass
/// or keeps repeating with a delay between completed passes.
/// </summary>
public enum LiveETagClusterExecutionMode
{
    /// <summary>
    /// Run exactly one all-node scan -> apply cycle and then exit.
    /// </summary>
    SinglePass,

    /// <summary>
    /// After each completed all-node cycle, wait for the configured launch-time delay
    /// and then start the next cycle.
    /// </summary>
    Recurring
}

/// <summary>
/// Tracks the current execution phase of a scan-style run so interrupted imports
/// can be resumed deterministically.
/// </summary>
public enum ScanExecutionPhase
{
    None,
    SnapshotCacheDownload,
    SnapshotImport,
    WaitingForSnapshotIndex,
    CandidateProcessing,
    Completed
}

// ─────────────────────────────────────────────────────────────────────────────
// Configuration models
// ─────────────────────────────────────────────────────────────────────────────

/// <summary>
/// Connection details for a single RavenDB cluster node.
/// </summary>
public sealed class NodeConfig
{
    /// <summary>
    /// Human-readable label used in logs and reports, e.g. "Node A".
    /// Helps distinguish nodes without memorizing URLs.
    /// </summary>
    public string Label { get; set; } = string.Empty;

    /// <summary>
    /// Full URL including scheme, host and port.
    /// Example: <c>https://a.myserver.ravendb.cloud:443</c>
    /// </summary>
    public string Url { get; set; } = string.Empty;
}

/// <summary>
/// Throttling parameters that limit the tool's impact on the live production cluster.
/// All values are configurable through the setup wizard or by editing <c>config.json</c>.
/// </summary>
public sealed class ThrottleConfig
{
    /// <summary>
    /// Number of document metadata records fetched from the source node per iteration
    /// batch. Smaller values reduce memory pressure and server I/O per request;
    /// larger values reduce round-trips. Valid range: 1–1 024. Default: 128.
    /// </summary>
    public int PageSize { get; set; } = 128;

    /// <summary>
    /// Milliseconds to sleep between consecutive batches. Provides a breathing window
    /// that prevents the tool from monopolising disk I/O on the server.
    /// Set to <c>0</c> to disable inter-batch delay (not recommended on production).
    /// Valid range: 0–60 000. Default: 200 ms.
    /// </summary>
    public int DelayBetweenBatchesMs { get; set; } = 200;

    /// <summary>
    /// Maximum number of retry attempts for a single HTTP or RavenDB SDK call before
    /// the error is considered permanent and the document is logged as unresolvable.
    /// Valid range: 1–10. Default: 3.
    /// </summary>
    public int MaxRetries { get; set; } = 3;

    /// <summary>
    /// Base delay in milliseconds for the exponential-backoff retry strategy.
    /// Actual delay for attempt <em>n</em> is approximately
    /// <c>RetryBaseDelayMs × 2^(n-1)</c> plus a small random jitter.
    /// Valid range: 100–10 000. Default: 500 ms.
    /// </summary>
    public int RetryBaseDelayMs { get; set; } = 500;

    /// <summary>
    /// Maximum number of unique document IDs accumulated before the checker performs a
    /// batched metadata-only fan-out read across all cluster nodes.
    /// </summary>
    public int ClusterLookupBatchSize { get; set; } = 512;
}

/// <summary>
/// Configuration for the local RavenDB database used as the tool's single durable state
/// store.
/// </summary>
public sealed class StateStoreConfig
{
    /// <summary>
    /// RavenDB server URL hosting the local state database.
    /// </summary>
    public string ServerUrl { get; set; } = "http://127.0.0.1:8080";

    /// <summary>
    /// Database name that stores runs, mismatches, repairs, diagnostics and dedup state.
    /// </summary>
    public string DatabaseName { get; set; } = "ConsistencyCheck.State";

    /// <summary>
    /// When <c>true</c>, structured diagnostics are persisted to the state store.
    /// </summary>
    public bool EnableDiagnostics { get; set; } = true;
}

/// <summary>
/// Complete application configuration. Persisted to <c>output/config.json</c> after the
/// setup wizard completes and reloaded on subsequent runs to skip re-configuration.
/// </summary>
/// <remarks>
/// <para>
/// <strong>Certificate security note:</strong> <see cref="CertificatePassword"/> is stored
/// in plain text inside <c>config.json</c>. Ensure the output directory is accessible only
/// to the user running the tool (e.g. <c>chmod 700</c> on Linux / restricted ACLs on
/// Windows).
/// </para>
/// </remarks>
public sealed class AppConfig
{
    [JsonIgnore]
    public HashSet<string> MissingProperties { get; set; } = new(StringComparer.OrdinalIgnoreCase);

    /// <summary>Name of the RavenDB database to check. Must exist on every node.</summary>
    public string DatabaseName { get; set; } = string.Empty;

    /// <summary>
    /// All cluster nodes participating in the check. The list must contain at least two
    /// entries. The node at index <see cref="SourceNodeIndex"/> is iterated by ETag;
    /// all remaining nodes are comparison targets.
    /// </summary>
    public List<NodeConfig> Nodes { get; set; } = new();

    /// <summary>
    /// Zero-based index into <see cref="Nodes"/> that identifies the <em>source node</em>.
    /// The source node is iterated sequentially by ETag using its raw HTTP API.
    /// Every other node in the list becomes a comparison target.
    /// </summary>
    public int SourceNodeIndex { get; set; } = 0;

    /// <summary>
    /// Thumbprint of the client certificate in <c>CurrentUser\My</c>.
    /// When specified, the application prefers loading the certificate from the
    /// Windows certificate store instead of reading a <c>.pfx</c> file directly.
    /// </summary>
    public string? CertificateThumbprint { get; set; } = null;

    /// <summary>
    /// Absolute path to the shared client certificate file (.pfx / .p12).
    /// RavenDB clusters typically use a single cluster-wide client certificate.
    /// Used only when <see cref="CertificateThumbprint"/> is not specified.
    /// </summary>
    /// <remarks>
    /// Three distinct states:
    /// <list type="bullet">
    ///   <item><description><c>null</c> — not yet specified; the setup wizard will ask for it.
    ///   A support engineer can pre-fill <c>config.json</c> and leave this field absent (or
    ///   set it to <c>null</c>) so the customer is prompted only for the certificate.</description></item>
    ///   <item><description><c>""</c> (empty string) — no certificate required (open / HTTP cluster).</description></item>
    ///   <item><description>Any other value — path to the <c>.pfx</c> / <c>.p12</c> file.</description></item>
    /// </list>
    /// </remarks>
    public string? CertificatePath { get; set; } = null;

    /// <summary>
    /// Password for the certificate file. May be empty if the certificate has no password.
    /// <c>null</c> when <see cref="CertificatePath"/> has not yet been specified.
    /// Stored in plain text — see security note on <see cref="AppConfig"/>.
    /// </summary>
    public string? CertificatePassword { get; set; } = null;

    /// <summary>Scan termination strategy. See <see cref="CheckMode"/> for details.</summary>
    [JsonConverter(typeof(JsonStringEnumConverter))]
    public CheckMode Mode { get; set; } = CheckMode.AllMismatches;

    /// <summary>
    /// Top-level application mode. <see cref="RunMode.DownloadSnapshotsToCache"/>
    /// downloads metadata snapshots into the local segmented file cache;
    /// <see cref="RunMode.ImportCachedSnapshotsToStateStore"/> imports one cached node
    /// into the local RavenDB snapshot collection; <see cref="RunMode.ScanOnly"/>,
    /// <see cref="RunMode.DryRunRepair"/> and <see cref="RunMode.ScanAndRepair"/>
    /// analyze an already imported snapshot set; <see cref="RunMode.ApplyRepairPlan"/>
    /// executes a previously collected repair plan.
    /// </summary>
    [JsonConverter(typeof(JsonStringEnumConverter))]
    public RunMode RunMode { get; set; } = RunMode.ScanOnly;

    /// <summary>
    /// Controls whether <see cref="RunMode.ApplyRepairPlan"/> runs interactively one
    /// document at a time or continues automatically.
    /// </summary>
    [JsonConverter(typeof(JsonStringEnumConverter))]
    public ApplyExecutionMode ApplyExecutionMode { get; set; } = ApplyExecutionMode.InteractivePerDocument;

    /// <summary>
    /// Launch-only selection for <see cref="RunMode.LiveETagScan"/>.
    /// Single-node mode preserves the existing manual workflow; all-node mode runs a
    /// strict scan -> apply -> next-node cycle without per-node operator input.
    /// </summary>
    [JsonConverter(typeof(JsonStringEnumConverter))]
    public LiveETagScanLaunchMode LiveETagScanLaunchMode { get; set; } = LiveETagScanLaunchMode.SingleNodeManual;

    /// <summary>
    /// Launch-only execution style for automatic all-node Live ETag processing.
    /// Not persisted to <c>config.json</c>.
    /// </summary>
    [JsonIgnore]
    public LiveETagClusterExecutionMode LiveETagClusterExecutionMode { get; set; } = LiveETagClusterExecutionMode.SinglePass;

    /// <summary>
    /// Launch-only delay between completed automatic all-node Live ETag cycles.
    /// Used only when <see cref="LiveETagClusterExecutionMode"/> is <see cref="ConsistencyCheck.LiveETagClusterExecutionMode.Recurring"/>.
    /// Not persisted to <c>config.json</c>.
    /// </summary>
    [JsonIgnore]
    public int? LiveETagRecurringIntervalMinutes { get; set; }

    /// <summary>Throttling parameters. See <see cref="ThrottleConfig"/> for details.</summary>
    public ThrottleConfig Throttle { get; set; } = new();

    /// <summary>
    /// Location of the local RavenDB database that persists the tool's runtime state.
    /// </summary>
    public StateStoreConfig StateStore { get; set; } = new();

    /// <summary>
    /// When <c>true</c>, server certificate validation errors are ignored for RavenDB
    /// client connections. Intended only for local development / test clusters.
    /// </summary>
    public bool AllowInvalidServerCertificates { get; set; }

    /// <summary>
    /// Legacy field kept for backward compatibility with older config files.
    /// The current snapshot-index pipeline does not use it.
    /// </summary>
    public long? StartEtag { get; set; } = null;

    /// <summary>Returns the <see cref="NodeConfig"/> designated as the source node.</summary>
    [JsonIgnore]
    public NodeConfig SourceNode => Nodes[SourceNodeIndex];

    /// <summary>
    /// Ephemeral launch-only selection for snapshot cache download / cached snapshot
    /// import modes: when set, only this node is processed during the current launch.
    /// </summary>
    [JsonIgnore]
    public string? SelectedImportNodeUrl { get; set; }

    /// <summary>
    /// Returns all nodes that are comparison targets (i.e., every node except the source).
    /// </summary>
    [JsonIgnore]
    public IEnumerable<NodeConfig> TargetNodes =>
        Nodes.Where((_, i) => i != SourceNodeIndex);
}

// ─────────────────────────────────────────────────────────────────────────────
// Runtime state models
// ─────────────────────────────────────────────────────────────────────────────

/// <summary>
/// Persistent scan progress state. Saved to <c>output/progress.json</c> after every
/// successfully processed batch. Enables crash recovery: on restart the scan resumes
/// from the last successfully persisted ETag position.
/// </summary>
public sealed class ProgressState
{
    /// <summary>
    /// The ETag value that will be used as the starting cursor on the next batch fetch.
    /// <c>null</c> means the scan has not yet started (will begin from ETag 0).
    /// This value advances after each batch so a restart never re-processes already-seen
    /// documents.
    /// </summary>
    public long? LastProcessedEtag { get; set; }

    /// <summary>
    /// Cumulative count of documents inspected across all batches, including the current
    /// run and any previous runs that were resumed.
    /// </summary>
    public long DocumentsInspected { get; set; }

    /// <summary>
    /// Cumulative count of mismatches (both <c>CV_MISMATCH</c> and <c>MISSING</c>) found
    /// across all runs.
    /// </summary>
    public long MismatchesFound { get; set; }

    /// <summary>UTC timestamp when the very first run of this scan was started.</summary>
    public DateTimeOffset StartedAt { get; set; } = DateTimeOffset.UtcNow;

    /// <summary>UTC timestamp when progress was last saved to disk.</summary>
    public DateTimeOffset LastSavedAt { get; set; } = DateTimeOffset.UtcNow;

    /// <summary>
    /// <c>true</c> when the scan reached the end of the document stream normally
    /// (i.e., was not interrupted or stopped by a <see cref="CheckMode.FirstMismatch"/>
    /// early exit). A completed scan resets on the next run instead of offering a resume.
    /// </summary>
    public bool IsComplete { get; set; }
}

// ─────────────────────────────────────────────────────────────────────────────
// Result models
// ─────────────────────────────────────────────────────────────────────────────

/// <summary>
/// Represents a single consistency mismatch record written to the CSV output file.
/// Each row describes one document that differs (or is missing) between the source node
/// and one specific target node.
/// </summary>
public sealed class MismatchRecord
{
    /// <summary>RavenDB document identifier, e.g. <c>orders/1-A</c>.</summary>
    public string Id { get; set; } = string.Empty;

    /// <summary>
    /// Type of inconsistency detected:
    /// <list type="bullet">
    ///   <item><description><c>CV_MISMATCH</c> — the document exists on both nodes but
    ///   their change vectors differ, indicating one node has not yet applied all
    ///   replication updates.</description></item>
    ///   <item><description><c>MISSING</c> — the document exists on the source node but is
    ///   absent on the target node entirely.</description></item>
    /// </list>
    /// </summary>
    public string MismatchType { get; set; } = string.Empty;

    /// <summary>URL of the source (iteration) node.</summary>
    public string SourceNode { get; set; } = string.Empty;

    /// <summary>URL of the target node where the mismatch was detected.</summary>
    public string TargetNode { get; set; } = string.Empty;

    /// <summary>Change vector as reported by the source node for this document.</summary>
    public string SourceCV { get; set; } = string.Empty;

    /// <summary>
    /// Change vector as reported by the target node.
    /// Empty string when <see cref="MismatchType"/> is <c>MISSING</c>.
    /// </summary>
    public string TargetCV { get; set; } = string.Empty;

    /// <summary>
    /// The <c>@last-modified</c> metadata value from the source node (ISO 8601).
    /// Helps determine how recent the document change was.
    /// </summary>
    public string SourceLastModified { get; set; } = string.Empty;

    /// <summary>
    /// The <c>@last-modified</c> metadata value from the target node (ISO 8601).
    /// Empty string when <see cref="MismatchType"/> is <c>MISSING</c>.
    /// </summary>
    public string TargetLastModified { get; set; } = string.Empty;

    /// <summary>UTC timestamp when this mismatch was detected.</summary>
    public DateTimeOffset DetectedAt { get; set; } = DateTimeOffset.UtcNow;
}

/// <summary>
/// Per-node checkpoint information for a single run.
/// </summary>
public sealed class NodeCursorState
{
    public string NodeUrl { get; set; } = string.Empty;
    public long NextEtag { get; set; }
    public bool IsExhausted { get; set; }
    public long? LastPageFirstEtag { get; set; }
    public long? LastPageLastEtag { get; set; }
    public int LastPageCount { get; set; }
    public DateTimeOffset? LastPageFetchedAt { get; set; }
}

/// <summary>
/// The latest known run pointer for a customer database.
/// </summary>
public sealed class RunHeadDocument
{
    public string Id { get; set; } = string.Empty;
    public string CustomerDatabaseName { get; set; } = string.Empty;
    public string? LatestRunId { get; set; }
    public string? LastCompletedRunId { get; set; }
    public long? LastCompletedSafeRestartEtag { get; set; }
    public string? ActiveSnapshotCacheRunId { get; set; }
    public DateTimeOffset? ActiveSnapshotCacheCompletedAt { get; set; }
    public string? PendingSnapshotCacheRunId { get; set; }
    public DateTimeOffset? PendingSnapshotCacheStartedAt { get; set; }
    public string? ActiveSnapshotRunId { get; set; }
    public DateTimeOffset? ActiveSnapshotCompletedAt { get; set; }
    public string? PendingSnapshotRunId { get; set; }
    public DateTimeOffset? PendingSnapshotStartedAt { get; set; }
    public DateTimeOffset UpdatedAt { get; set; } = DateTimeOffset.UtcNow;
}

/// <summary>
/// Durable state of a single scan / repair run.
/// </summary>
public sealed class RunStateDocument
{
    public string Id { get; set; } = string.Empty;
    public string RunId { get; set; } = string.Empty;
    public string CustomerDatabaseName { get; set; } = string.Empty;
    public List<NodeConfig> ClusterNodes { get; set; } = [];
    [JsonConverter(typeof(JsonStringEnumConverter))]
    public RunMode RunMode { get; set; }
    [JsonConverter(typeof(JsonStringEnumConverter))]
    public CheckMode ScanMode { get; set; }
    public DateTimeOffset StartedAt { get; set; } = DateTimeOffset.UtcNow;
    public DateTimeOffset LastSavedAt { get; set; } = DateTimeOffset.UtcNow;
    public DateTimeOffset? CompletedAt { get; set; }
    public bool IsComplete { get; set; }
    [JsonConverter(typeof(JsonStringEnumConverter))]
    public ScanExecutionPhase CurrentPhase { get; set; }
    public List<SnapshotCacheNodeState> SnapshotCacheNodes { get; set; } = [];
    public bool SnapshotImportCompleted { get; set; }
    public List<SnapshotImportNodeState> SnapshotImportNodes { get; set; } = [];
    public List<NodeCursorState> NodeCursors { get; set; } = [];
    public long SnapshotDocumentsImported { get; set; }
    public long SnapshotDocumentsSkipped { get; set; }
    public long SnapshotBulkInsertRestarts { get; set; }
    public long CandidateDocumentsFound { get; set; }
    public long CandidateDocumentsProcessed { get; set; }
    public long CandidateDocumentsExcludedBySkippedSnapshots { get; set; }
    public long DocumentsInspected { get; set; }
    public long UniqueVersionsCompared { get; set; }
    public long MismatchesFound { get; set; }
    public long ManualReviewDocumentsFound { get; set; }
    public long RepairsPlanned { get; set; }
    public long RepairsAttempted { get; set; }
    public long RepairsPatchedOnWinner { get; set; }
    public long RepairsFailed { get; set; }
    public long? SafeRestartEtag { get; set; }
    public string? SourceRepairPlanRunId { get; set; }
    public string? SourceSnapshotCacheRunId { get; set; }
    public string? SourceSnapshotRunId { get; set; }
    public ChangeVectorSemanticsSnapshot? ChangeVectorSemanticsSnapshot { get; set; }
    public AppConfig? ConfigSnapshot { get; set; }
}

/// <summary>
/// Durable per-node snapshot cache download progress.
/// </summary>
public sealed class SnapshotCacheNodeState
{
    public string NodeUrl { get; set; } = string.Empty;
    public string NodeLabel { get; set; } = string.Empty;
    public string NodeAlias { get; set; } = string.Empty;
    public bool IsDownloadComplete { get; set; }
    public DateTimeOffset? DownloadCompletedAt { get; set; }
    public string? LastDownloadedDocumentId { get; set; }
    public long DownloadedRows { get; set; }
    public int CompletedSegmentCount { get; set; }
    public long CompressedBytesWritten { get; set; }
    public int CurrentSegmentId { get; set; } = 1;
    public long CurrentSegmentCommittedBytes { get; set; }
    public long CurrentSegmentCommittedRows { get; set; }
    public DateTimeOffset? LastUpdatedAt { get; set; }
}

/// <summary>
/// Durable per-node cached snapshot import progress, including resilient local bulk-insert
/// restarts and the exact local cache cursor.
/// </summary>
public sealed class SnapshotImportNodeState
{
    public string NodeUrl { get; set; } = string.Empty;
    public string NodeLabel { get; set; } = string.Empty;
    public bool IsImportComplete { get; set; }
    public DateTimeOffset? ImportCompletedAt { get; set; }
    public int CurrentSegmentId { get; set; } = 1;
    public int CurrentPageNumber { get; set; } = 1;
    public int CurrentRowOffsetInPage { get; set; }
    public long ImportedRows { get; set; }
    public long SnapshotsCommitted { get; set; }
    public long SnapshotsSkipped { get; set; }
    public long BulkInsertRestartCount { get; set; }
    public string? LastConfirmedSnapshotId { get; set; }
    public string? LastSkippedSnapshotId { get; set; }
    public string? LastError { get; set; }
    public DateTimeOffset? LastUpdatedAt { get; set; }
}

/// <summary>
/// Cluster-level snapshot of the change-vector comparison semantics used for one run.
/// This keeps winner selection deterministic even if the cluster metadata changes later.
/// </summary>
public sealed class ChangeVectorSemanticsSnapshot
{
    private HashSet<string>? _explicitUnusedDatabaseIdSet;

    public List<ChangeVectorSemanticsNodeInfo> Nodes { get; set; } = [];
    public List<string> ExplicitUnusedDatabaseIds { get; set; } = [];
    public List<string> PotentialUnusedDatabaseIds { get; set; } = [];

    [JsonIgnore]
    public IReadOnlySet<string> ExplicitUnusedDatabaseIdSet =>
        _explicitUnusedDatabaseIdSet ??= ExplicitUnusedDatabaseIds
            .Where(static id => string.IsNullOrWhiteSpace(id) == false)
            .ToHashSet(StringComparer.Ordinal);
}

/// <summary>
/// Per-node database metadata captured for change-vector comparison semantics.
/// </summary>
public sealed class ChangeVectorSemanticsNodeInfo
{
    public string NodeUrl { get; set; } = string.Empty;
    public string Label { get; set; } = string.Empty;
    public string? DatabaseId { get; set; }
    public string? DatabaseChangeVector { get; set; }
}

/// <summary>
/// Snapshot of a document's current live state on one node.
/// </summary>
public sealed class NodeObservedState
{
    public string NodeUrl { get; set; } = string.Empty;
    public bool Present { get; set; }
    public string? Collection { get; set; }
    public string? ChangeVector { get; set; }
    public string? LastModified { get; set; }
}

/// <summary>
/// Cluster-wide mismatch observation stored in the local state database.
/// </summary>
public sealed class MismatchDocument
{
    public string Id { get; set; } = string.Empty;
    public string RunId { get; set; } = string.Empty;
    public string DocumentId { get; set; } = string.Empty;
    public string? Collection { get; set; }
    public string MismatchType { get; set; } = string.Empty;
    public string? WinnerNode { get; set; }
    public string? WinnerCV { get; set; }
    public List<NodeObservedState> ObservedState { get; set; } = [];
    public string RepairDecision { get; set; } = string.Empty;
    public string? CurrentRepairDecision { get; set; }
    public string? LastRepairRunId { get; set; }
    public DateTimeOffset? LastRepairUpdatedAt { get; set; }
    public DateTimeOffset DetectedAt { get; set; } = DateTimeOffset.UtcNow;
}

/// <summary>
/// Repair attempt or repair decision persisted in the local state database.
/// </summary>
public sealed class RepairDocument
{
    public string Id { get; set; } = string.Empty;
    public string RunId { get; set; } = string.Empty;
    public List<string> DocumentIds { get; set; } = [];
    public string? Collection { get; set; }
    public string WinnerNode { get; set; } = string.Empty;
    public string? WinnerCV { get; set; }
    public List<string> AffectedNodes { get; set; } = [];
    public long? PatchOperationId { get; set; }
    public string? SourceRepairPlanId { get; set; }
    public string? SourceRepairPlanRunId { get; set; }
    public string RepairStatus { get; set; } = string.Empty;
    public long? DocumentsAttempted { get; set; }
    public long? DocumentsPatchedOnWinner { get; set; }
    public long? DocumentsFailed { get; set; }
    public DateTimeOffset CompletedAt { get; set; } = DateTimeOffset.UtcNow;
    public string? Error { get; set; }
}

public sealed class BlockedApplyExecutionCountIndexEntry
{
    public string RunId { get; set; } = string.Empty;
    public string? SourceRepairPlanRunId { get; set; }
    public int Count { get; set; }
}

/// <summary>
/// Per-run guard preventing the same document from being planned or applied more than once.
/// This is an idempotency barrier for one run, not a durable repair history record.
/// </summary>
public sealed class RepairActionGuardDocument
{
    public string Id { get; set; } = string.Empty;
    public string RunId { get; set; } = string.Empty;
    public string DocumentId { get; set; } = string.Empty;
    public string WinnerNode { get; set; } = string.Empty;
    public string? WinnerCV { get; set; }
    public long? PatchOperationId { get; set; }
    public DateTimeOffset RecordedAt { get; set; } = DateTimeOffset.UtcNow;
}

/// <summary>
/// Live metadata snapshot captured during <see cref="RunMode.ApplyRepairPlan"/> when the
/// current cluster state no longer matches the imported snapshot that produced the saved
/// repair plan.
/// </summary>
public sealed class RepairStateChangedDocument
{
    public string Id { get; set; } = string.Empty;
    public string ApplyRunId { get; set; } = string.Empty;
    public string SourceRepairPlanRunId { get; set; } = string.Empty;
    public string? SourceSnapshotRunId { get; set; }
    public string DocumentId { get; set; } = string.Empty;
    public string? SourceMismatchType { get; set; }
    public string? SourceWinnerNode { get; set; }
    public string? SourceWinnerCV { get; set; }
    public List<NodeObservedState> SourceObservedState { get; set; } = [];
    public bool LiveIsConsistent { get; set; }
    public string? LiveMismatchType { get; set; }
    public string? LiveWinnerNode { get; set; }
    public string? LiveWinnerCV { get; set; }
    public List<NodeObservedState> LiveObservedState { get; set; } = [];
    public string Reason { get; set; } = string.Empty;
    public DateTimeOffset RecordedAt { get; set; } = DateTimeOffset.UtcNow;
}

/// <summary>
/// Snapshot row that had to be skipped after retry because the local bulk-insert sink
/// could not get it durably accepted by the local state store.
/// </summary>
public sealed class SnapshotImportSkippedDocument
{
    public string Id { get; set; } = string.Empty;
    public string RunId { get; set; } = string.Empty;
    public string OriginalDocumentId { get; set; } = string.Empty;
    public string SnapshotDocumentId { get; set; } = string.Empty;
    public string FromNode { get; set; } = string.Empty;
    public string NodeUrl { get; set; } = string.Empty;
    public int FailureCount { get; set; }
    public string? LastConfirmedSnapshotIdBeforeSkip { get; set; }
    public string? Error { get; set; }
    public DateTimeOffset SkippedAt { get; set; } = DateTimeOffset.UtcNow;
}

/// <summary>
/// Snapshot row whose original document id contained characters that are unsafe for the
/// RavenDB bulk-insert document-id serialization path, so a normalized physical snapshot
/// id was used instead.
/// </summary>
public sealed class UnsafeSnapshotIdDocument
{
    public string Id { get; set; } = string.Empty;
    public string RunId { get; set; } = string.Empty;
    public string OriginalDocumentId { get; set; } = string.Empty;
    public string NormalizedSnapshotDocumentId { get; set; } = string.Empty;
    public string FromNode { get; set; } = string.Empty;
    public string NodeUrl { get; set; } = string.Empty;
    public string NormalizationReason { get; set; } = string.Empty;
    public DateTimeOffset DetectedAt { get; set; } = DateTimeOffset.UtcNow;
}

/// <summary>
/// Structured diagnostic event persisted to the local state database.
/// </summary>
public sealed class DiagnosticDocument
{
    public string Id { get; set; } = string.Empty;
    public string RunId { get; set; } = string.Empty;
    public string Kind { get; set; } = string.Empty;
    public string? NodeUrl { get; set; }
    public long? RequestedStartEtag { get; set; }
    public long? FirstItemEtag { get; set; }
    public long? LastItemEtag { get; set; }
    public int? PageCount { get; set; }
    public int? VersionsSkippedByDedup { get; set; }
    public int? NewVersionsQueued { get; set; }
    public int? UniqueIdsInLookupBatch { get; set; }
    public int? MismatchesInBatch { get; set; }
    public int? RepairsTriggered { get; set; }
    public string? Message { get; set; }
    public string? JsonPayload { get; set; }
    public DateTimeOffset Timestamp { get; set; } = DateTimeOffset.UtcNow;
}

/// <summary>
/// RavenDB document that owns one dedup bucket attachment.
/// </summary>
public sealed class DedupBucketDocument
{
    public string Id { get; set; } = string.Empty;
    public string RunId { get; set; } = string.Empty;
    public string Prefix { get; set; } = string.Empty;
    public int EntryCount { get; set; }
    public DateTimeOffset LastUpdatedAt { get; set; } = DateTimeOffset.UtcNow;
    public string AttachmentName { get; set; } = "hashes.bin";
}
