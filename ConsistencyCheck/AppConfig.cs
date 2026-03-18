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
    /// Absolute path to the shared client certificate file (.pfx / .p12).
    /// RavenDB clusters typically use a single cluster-wide client certificate.
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

    /// <summary>Throttling parameters. See <see cref="ThrottleConfig"/> for details.</summary>
    public ThrottleConfig Throttle { get; set; } = new();

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

    /// <summary>UTC timestamp when this mismatch was detected.</summary>
    public DateTimeOffset DetectedAt { get; set; } = DateTimeOffset.UtcNow;
}
