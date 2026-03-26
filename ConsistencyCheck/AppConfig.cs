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
    /// Detect inconsistencies and persist the findings, but do not modify the cluster.
    /// </summary>
    ScanOnly,

    /// <summary>
    /// Detect inconsistencies and build the exact repair plan that would be executed,
    /// but never send patch operations to the customer cluster.
    /// </summary>
    DryRunRepair,

    /// <summary>
    /// Detect inconsistencies and immediately try to repair resolvable documents by
    /// touching the winner copy so replication re-fans the document out.
    /// </summary>
    ScanAndRepair
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
    /// Top-level application mode. <see cref="RunMode.ScanOnly"/> only reports
    /// inconsistencies, <see cref="RunMode.DryRunRepair"/> records what would be repaired
    /// without mutating the cluster, and <see cref="RunMode.ScanAndRepair"/> performs
    /// winner-side patch-by-query operations.
    /// </summary>
    [JsonConverter(typeof(JsonStringEnumConverter))]
    public RunMode RunMode { get; set; } = RunMode.ScanOnly;

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
    /// ETag value from which to begin scanning when starting a fresh run.
    /// </summary>
    /// <remarks>
    /// Three distinct states:
    /// <list type="bullet">
    ///   <item><description><c>null</c> — not yet specified; the setup/completion wizard will ask for it.</description></item>
    ///   <item><description><c>0</c> — scan from the very beginning.</description></item>
    ///   <item><description>Any positive value — skip older documents and start from that ETag.</description></item>
    /// </list>
    /// </remarks>
    public long? StartEtag { get; set; } = null;

    /// <summary>Returns the <see cref="NodeConfig"/> designated as the source node.</summary>
    [JsonIgnore]
    public NodeConfig SourceNode => Nodes[SourceNodeIndex];

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
    public List<NodeCursorState> NodeCursors { get; set; } = [];
    public long DocumentsInspected { get; set; }
    public long UniqueVersionsCompared { get; set; }
    public long MismatchesFound { get; set; }
    public long RepairsPlanned { get; set; }
    public long RepairsAttempted { get; set; }
    public long RepairsPatchedOnWinner { get; set; }
    public long RepairsFailed { get; set; }
    public long? SafeRestartEtag { get; set; }
    public ChangeVectorSemanticsSnapshot? ChangeVectorSemanticsSnapshot { get; set; }
    public AppConfig? ConfigSnapshot { get; set; }
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
    public string PatchQuery { get; set; } = string.Empty;
    public long? PatchOperationId { get; set; }
    public string RepairStatus { get; set; } = string.Empty;
    public DateTimeOffset CompletedAt { get; set; } = DateTimeOffset.UtcNow;
    public string? Error { get; set; }
}

/// <summary>
/// Repair guard preventing the same document from being patched more than once per run.
/// </summary>
public sealed class RepairGuardDocument
{
    public string Id { get; set; } = string.Empty;
    public string RunId { get; set; } = string.Empty;
    public string DocumentId { get; set; } = string.Empty;
    public string WinnerNode { get; set; } = string.Empty;
    public string? WinnerCV { get; set; }
    public long? PatchOperationId { get; set; }
    public DateTimeOffset PatchedAt { get; set; } = DateTimeOffset.UtcNow;
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
