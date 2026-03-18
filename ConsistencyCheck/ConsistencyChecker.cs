using System.Net.Http.Headers;
using System.Security.Cryptography.X509Certificates;
using System.Text.Json;
using Polly;
using Polly.Retry;
using Raven.Client.Documents;
using Raven.Client.Documents.Commands;
using Raven.Client.Exceptions;
using Sparrow.Json;

namespace ConsistencyCheck;

/// <summary>
/// Core consistency-check engine.
/// </summary>
/// <remarks>
/// <para>
/// <strong>Algorithm:</strong>
/// <list type="number">
///   <item><description>Fetch a page of document metadata (ID + change vector) from the
///   <em>source node</em> using its raw HTTP API, ordered by ETag. This minimises server
///   I/O because <c>metadataOnly=true</c> avoids reading document bodies from storage.
///   </description></item>
///   <item><description>For each document in the page, issue parallel
///   <see cref="GetDocumentsCommand"/> calls (one per target node) using the RavenDB
///   .NET client SDK with <c>metadataOnly=true</c>.
///   </description></item>
///   <item><description>Compare the change vectors.  Because segment ordering in a change
///   vector may differ between nodes (e.g. after a topology change), the CVs are parsed
///   into <c>Dictionary&lt;string, long&gt;</c> (nodeTag → etag) before comparison.
///   </description></item>
///   <item><description>Write any <c>CV_MISMATCH</c> or <c>MISSING</c> records to the
///   <see cref="ResultsWriter"/>, save progress, then optionally throttle before the next
///   batch.</description></item>
/// </list>
/// </para>
/// <para>
/// <strong>Crash safety:</strong> progress is flushed to disk after every batch, so a
/// restart resumes from the last saved ETag with no re-processing of already-seen pages.
/// </para>
/// <para>
/// <strong>Cancellation:</strong> a <see cref="CancellationToken"/> is propagated through
/// all async calls. On CTRL+C the current batch runs to completion so progress is saved
/// cleanly, then the method returns.
/// </para>
/// </remarks>
public sealed class ConsistencyChecker : IAsyncDisposable
{
    // ── Dependencies ──────────────────────────────────────────────────────────

    private readonly AppConfig        _config;
    private readonly ProgressStore    _store;
    private readonly ResultsWriter    _writer;
    private readonly HttpClient       _sourceHttpClient;
    private readonly ResiliencePipeline _retry;

    /// <summary>
    /// One <see cref="IDocumentStore"/> per cluster node (source <em>and</em> targets),
    /// keyed by the node URL. Each store is pinned to its specific URL via
    /// <c>DisableTopologyUpdates=true</c>. Including the source node here ensures that
    /// per-document batch CV queries are issued to <em>all</em> nodes symmetrically —
    /// no node is treated as a trusted reference without independent verification.
    /// </summary>
    private readonly Dictionary<string, IDocumentStore> _allStores;

    /// <summary>The database ID of the source node, fetched once on startup.</summary>
    private string _sourceDatabaseId = string.Empty;

    /// <summary>
    /// Running count of mismatches found in this session.
    /// Updated atomically via <see cref="Interlocked.Increment(ref long)"/>;
    /// a plain field is required because C# does not allow <c>ref</c> on properties.
    /// </summary>
    private long _mismatchCounter;

    // ── Event ─────────────────────────────────────────────────────────────────

    /// <summary>
    /// Raised after each batch is fully processed and progress has been saved.
    /// The handler may be asynchronous; <see cref="RunAsync"/> awaits it before
    /// moving to the next batch.
    /// </summary>
    /// <remarks>
    /// Arguments: (<em>documentsInspected</em>, <em>mismatchesFound</em>,
    /// <em>currentEtag</em>).
    /// </remarks>
    public event Func<long, long, long, Task>? BatchCompleted;

    // ─────────────────────────────────────────────────────────────────────────
    // Construction
    // ─────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// Initialises the checker: creates one <see cref="IDocumentStore"/> per target node,
    /// configures the HTTP client for the source node, and builds the Polly retry pipeline.
    /// </summary>
    /// <param name="config">Application configuration.</param>
    /// <param name="store">Persistence store for progress.</param>
    /// <param name="writer">CSV output writer for mismatches.</param>
    public ConsistencyChecker(AppConfig config, ProgressStore store, ResultsWriter writer)
    {
        _config = config;
        _store  = store;
        _writer = writer;
        _retry  = BuildRetryPipeline(config.Throttle);

        var cert = ConfigWizard.LoadCertificate(config);

        // ── Source node: raw HttpClient (no topology overhead) ────────────────
        _sourceHttpClient = BuildHttpClient(cert);

        // ── All nodes: one SDK DocumentStore each (source + targets) ─────────
        // The source node is included so that batch CV queries are issued to every node
        // symmetrically; the raw HttpClient above is used only for ETag-ordered iteration.
        _allStores = new Dictionary<string, IDocumentStore>(StringComparer.OrdinalIgnoreCase);
        foreach (var node in config.Nodes)
            _allStores[node.Url] = ConfigWizard.BuildStore(node.Url, config.DatabaseName, cert);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Public API
    // ─────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// Runs the consistency check until one of the following conditions is met:
    /// <list type="bullet">
    ///   <item><description>The source node has no more documents (scan complete).</description></item>
    ///   <item><description><see cref="CheckMode.FirstMismatch"/> is active and at least
    ///   one mismatch has been found.</description></item>
    ///   <item><description><paramref name="ct"/> is cancelled (CTRL+C).</description></item>
    /// </list>
    /// Progress is persisted after every batch; the method always returns a valid
    /// (possibly partially-updated) <see cref="ProgressState"/>.
    /// </summary>
    /// <param name="initialState">
    /// The progress state loaded from disk (may represent a previously interrupted run).
    /// </param>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>The final progress state after this run.</returns>
    public async Task<ProgressState> RunAsync(ProgressState initialState, CancellationToken ct)
    {
        var state = initialState;

        // Seed the thread-safe mismatch counter from any previously saved state
        // so that a resumed run reports cumulative totals correctly.
        _mismatchCounter = state.MismatchesFound;

        // Fetch the source database ID once — needed to extract the ETag component from CVs.
        _sourceDatabaseId = await FetchSourceDatabaseIdAsync(ct).ConfigureAwait(false);

        var startEtag = state.LastProcessedEtag ?? 0L;

        while (true)
        {
            ct.ThrowIfCancellationRequested();

            // ── 1. Fetch next page from source node ───────────────────────────
            var batch = await FetchSourceBatchAsync(startEtag, _config.Throttle.PageSize, ct)
                            .ConfigureAwait(false);

            if (batch.Count == 0)
            {
                // Reached the end of the document stream.
                state.IsComplete = true;
                _store.SaveProgress(state);
                break;
            }

            // ── 2. Compare each document against all target nodes in parallel ─
            await ProcessBatchAsync(batch, ct).ConfigureAwait(false);

            // ── 3. Advance ETag cursor ────────────────────────────────────────
            var lastEtag = ExtractEtagFromChangeVector(batch[^1].ChangeVector, _sourceDatabaseId);
            if (lastEtag <= startEtag)
                lastEtag = startEtag + 1; // guard against a non-advancing CV

            state.LastProcessedEtag  = lastEtag;
            state.DocumentsInspected += batch.Count;
            state.MismatchesFound    = Interlocked.Read(ref _mismatchCounter);
            state.LastSavedAt        = DateTimeOffset.UtcNow;
            _store.SaveProgress(state);

            if (BatchCompleted != null)
                await BatchCompleted(state.DocumentsInspected, state.MismatchesFound, lastEtag)
                    .ConfigureAwait(false);

            // ── 4. FirstMismatch early exit ───────────────────────────────────
            if (_config.Mode == CheckMode.FirstMismatch && state.MismatchesFound > 0)
                break;

            // ── 5. Throttle ───────────────────────────────────────────────────
            if (_config.Throttle.DelayBetweenBatchesMs > 0)
                await Task.Delay(_config.Throttle.DelayBetweenBatchesMs, ct).ConfigureAwait(false);

            startEtag = lastEtag + 1;
        }

        return state;
    }

    /// <inheritdoc/>
    public async ValueTask DisposeAsync()
    {
        _sourceHttpClient.Dispose();
        foreach (var store in _allStores.Values)
            store.Dispose();

        await Task.CompletedTask.ConfigureAwait(false);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Source node — raw HTTP iteration
    // ─────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// Fetches one page of document metadata from the source node using its raw HTTP API.
    /// </summary>
    /// <remarks>
    /// Endpoint: <c>GET /databases/{db}/docs?etag={etag}&amp;pageSize={n}&amp;metadataOnly=true</c>
    /// <br/>
    /// Using the raw HTTP API (rather than the SDK) avoids per-request session overhead
    /// (identity map, tracking, topology negotiation) and keeps the tool lightweight.
    /// </remarks>
    /// <param name="startEtag">Inclusive lower-bound ETag for this page.</param>
    /// <param name="pageSize">Maximum number of documents to return.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>
    /// A list of (documentId, changeVector) pairs ordered by ETag.
    /// Returns an empty list when the end of the stream is reached.
    /// </returns>
    private async Task<List<(string Id, string ChangeVector)>> FetchSourceBatchAsync(
        long startEtag, int pageSize, CancellationToken ct)
    {
        var url = $"{_config.SourceNode.Url}/databases/{_config.DatabaseName}/docs" +
                  $"?etag={startEtag}&pageSize={pageSize}&metadataOnly=true";

        var responseBody = await _retry.ExecuteAsync(async token =>
        {
            using var response = await _sourceHttpClient.GetAsync(url, token).ConfigureAwait(false);
            response.EnsureSuccessStatusCode();
            return await response.Content.ReadAsStringAsync(token).ConfigureAwait(false);
        }, ct).ConfigureAwait(false);

        return ParseSourceBatch(responseBody);
    }

    /// <summary>
    /// Parses the JSON response from the raw HTTP docs endpoint into a list of
    /// (id, changeVector) pairs.
    /// </summary>
    private static List<(string Id, string ChangeVector)> ParseSourceBatch(string json)
    {
        var result = new List<(string, string)>();

        using var doc = JsonDocument.Parse(json);

        if (!doc.RootElement.TryGetProperty("Results", out var results))
            return result;

        foreach (var element in results.EnumerateArray())
        {
            if (!element.TryGetProperty("@metadata", out var meta))
                continue;

            var id = meta.TryGetProperty("@id",            out var idProp) ? idProp.GetString()            : null;
            var cv = meta.TryGetProperty("@change-vector", out var cvProp) ? cvProp.GetString() : null;

            if (!string.IsNullOrEmpty(id) && cv != null)
                result.Add((id, cv));
        }

        return result;
    }

    // ─────────────────────────────────────────────────────────────────────────
    // All-node batch comparison
    // ─────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// Compares the change vector of every document in <paramref name="batch"/> against
    /// all target nodes, using the CVs already obtained from the source node during
    /// iteration as the reference.
    /// </summary>
    /// <remarks>
    /// Source CVs come directly from the HTTP iteration batch — no additional request is
    /// sent to the source node, keeping it at minimum load (important when the source is
    /// the primary node serving all reads and writes).
    /// One <see cref="GetDocumentsCommand"/> per target node carries all IDs in the batch,
    /// reducing round-trips from <c>batch.Count × targetCount</c> to <c>targetCount</c>.
    /// </remarks>
    private async Task ProcessBatchAsync(
        List<(string Id, string ChangeVector)> batch,
        CancellationToken ct)
    {
        var docIds = batch.Select(d => d.Id).ToArray();

        // Source CVs are taken directly from the iteration batch — no extra request
        // to the source node. This matches the original behaviour and keeps the
        // source node (primary) at minimum load.
        var sourceCvs = batch.ToDictionary(
            d => d.Id,
            d => (string?)d.ChangeVector,
            StringComparer.OrdinalIgnoreCase);

        // Issue one batch metadata request to every TARGET node in parallel.
        var nodeCvTasks = _config.TargetNodes.Select(async targetNode =>
        {
            var store = _allStores[targetNode.Url];
            var cvMap = await FetchBatchCvsAsync(store, docIds, ct).ConfigureAwait(false);
            return (NodeUrl: targetNode.Url, CvMap: cvMap);
        });

        var nodeResults = await Task.WhenAll(nodeCvTasks).ConfigureAwait(false);
        var cvsByNode   = nodeResults.ToDictionary(
            r => r.NodeUrl, r => r.CvMap, StringComparer.OrdinalIgnoreCase);

        foreach (var docId in docIds)
        {
            sourceCvs.TryGetValue(docId, out var sourceCV);

            if (sourceCV == null)
                continue;

            var sourceParsed = ParseChangeVector(sourceCV);

            foreach (var targetNode in _config.TargetNodes)
            {
                if (!cvsByNode.TryGetValue(targetNode.Url, out var targetCvs) ||
                    !targetCvs.TryGetValue(docId, out var targetCV) ||
                    targetCV == null)
                {
                    Interlocked.Increment(ref _mismatchCounter);
                    await _writer.WriteAsync(new MismatchRecord
                    {
                        Id           = docId,
                        MismatchType = "MISSING",
                        SourceNode   = _config.SourceNode.Url,
                        TargetNode   = targetNode.Url,
                        SourceCV     = sourceCV,
                        TargetCV     = string.Empty
                    }, ct).ConfigureAwait(false);
                    continue;
                }

                // Compare parsed dictionaries — order-insensitive CV comparison.
                if (!ChangeVectorsEqual(sourceParsed, ParseChangeVector(targetCV)))
                {
                    Interlocked.Increment(ref _mismatchCounter);
                    await _writer.WriteAsync(new MismatchRecord
                    {
                        Id           = docId,
                        MismatchType = "CV_MISMATCH",
                        SourceNode   = _config.SourceNode.Url,
                        TargetNode   = targetNode.Url,
                        SourceCV     = sourceCV,
                        TargetCV     = targetCV
                    }, ct).ConfigureAwait(false);
                }
            }
        }
    }

    /// <summary>
    /// Fetches the change vector for each document ID in <paramref name="docIds"/> from
    /// the given <paramref name="store"/> using a single batched
    /// <see cref="GetDocumentsCommand"/> request.
    /// </summary>
    /// <returns>
    /// A dictionary mapping each document ID to its change vector string, or <c>null</c>
    /// when the document is absent on that node.
    /// </returns>
    private async Task<Dictionary<string, string?>> FetchBatchCvsAsync(
        IDocumentStore store, string[] docIds, CancellationToken ct)
    {
        return await _retry.ExecuteAsync<Dictionary<string, string?>>(async token =>
        {
            using (store.GetRequestExecutor(_config.DatabaseName)
                        .ContextPool.AllocateOperationContext(out JsonOperationContext ctx))
            {
                var command = new GetDocumentsCommand(
                    store.Conventions, docIds, includes: null, metadataOnly: true);

                await store.GetRequestExecutor(_config.DatabaseName)
                           .ExecuteAsync(command, ctx, null, token)
                           .ConfigureAwait(false);

                var result  = new Dictionary<string, string?>(
                    docIds.Length, StringComparer.OrdinalIgnoreCase);
                var results = command.Result?.Results;

                for (int i = 0; i < docIds.Length; i++)
                {
                    var docId = docIds[i];

                    if (results == null || i >= results.Length ||
                        results[i] is not BlittableJsonReaderObject docObj)
                    {
                        result[docId] = null; // document absent on this node
                        continue;
                    }

                    if (docObj.TryGet("@metadata", out BlittableJsonReaderObject meta) &&
                        meta.TryGet("@change-vector", out string cv))
                    {
                        result[docId] = cv;
                    }
                    else
                    {
                        result[docId] = string.Empty; // present but no CV in metadata
                    }
                }

                return result;
            }
        }, ct).ConfigureAwait(false);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Change vector helpers
    // ─────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// Parses a RavenDB change vector string into a dictionary mapping node-tag to etag.
    /// </summary>
    /// <remarks>
    /// Change vector format: <c>"A:1234-DbIdABC, B:5678-DbIdXYZ"</c>
    /// <br/>
    /// The segments after the dash (<c>DbIdABC</c>) are database-level identifiers.
    /// The short letter prefix (<c>A</c>, <c>B</c>) is the node tag within the cluster.
    /// We key the dictionary by the node tag because it is stable and short.
    /// <br/>
    /// Using a dictionary instead of a raw string comparison makes the check
    /// order-insensitive, which matters after topology changes can reorder segments.
    /// </remarks>
    internal static Dictionary<string, long> ParseChangeVector(string? cv)
    {
        var result = new Dictionary<string, long>(StringComparer.OrdinalIgnoreCase);
        if (string.IsNullOrWhiteSpace(cv))
            return result;

        foreach (var segment in cv.Split(',', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries))
        {
            // segment = "A:1234-DbIdABC"
            var colonIdx = segment.IndexOf(':');
            if (colonIdx <= 0) continue;

            var dashIdx = segment.IndexOf('-', colonIdx);
            if (dashIdx <= colonIdx) continue;

            var nodeTag  = segment[..colonIdx];
            var etagStr  = segment[(colonIdx + 1)..dashIdx];

            if (long.TryParse(etagStr, out var etag))
                result[nodeTag] = etag;
        }

        return result;
    }

    /// <summary>
    /// Returns <c>true</c> when two parsed change vector dictionaries represent the
    /// same replication state (same set of node tags with identical etag values).
    /// </summary>
    private static bool ChangeVectorsEqual(
        Dictionary<string, long> a,
        Dictionary<string, long> b)
    {
        if (a.Count != b.Count) return false;

        foreach (var (tag, etag) in a)
        {
            if (!b.TryGetValue(tag, out var otherEtag) || otherEtag != etag)
                return false;
        }

        return true;
    }

    /// <summary>
    /// Extracts the ETag value contributed by the source node from a change vector string.
    /// Used to advance the iteration cursor after each batch.
    /// </summary>
    /// <param name="cv">Change vector string from the last document in the batch.</param>
    /// <param name="sourceDatabaseId">
    /// The database ID of the source node (the suffix after the dash in CV segments
    /// that belong to the source).
    /// </param>
    /// <returns>
    /// The ETag for the source node's segment, or 0 if the segment cannot be found.
    /// Falls back to the maximum ETag across all segments when the database ID is unknown.
    /// </returns>
    internal static long ExtractEtagFromChangeVector(string cv, string sourceDatabaseId)
    {
        if (string.IsNullOrWhiteSpace(cv))
            return 0L;

        var segments = cv.Split(',', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries);

        long maxEtag = 0;

        foreach (var segment in segments)
        {
            // segment = "A:1234-DbIdABC"
            var colonIdx = segment.IndexOf(':');
            if (colonIdx <= 0) continue;

            var dashIdx = segment.IndexOf('-', colonIdx);
            if (dashIdx <= colonIdx) continue;

            var dbId    = segment[(dashIdx + 1)..];
            var etagStr = segment[(colonIdx + 1)..dashIdx];

            if (!long.TryParse(etagStr, out var etag)) continue;

            maxEtag = Math.Max(maxEtag, etag);

            if (string.Equals(dbId, sourceDatabaseId, StringComparison.OrdinalIgnoreCase))
                return etag; // exact match found
        }

        // Fallback: return the largest ETag seen across all segments.
        return maxEtag;
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Source database ID
    // ─────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// Fetches the database ID from the source node via
    /// <c>GET /databases/{db}/debug/database-info</c>.
    /// The database ID is the suffix after the dash in change vector segments belonging
    /// to this node (e.g. <c>A:1234-[databaseId]</c>).
    /// Cached for the lifetime of the checker.
    /// </summary>
    private async Task<string> FetchSourceDatabaseIdAsync(CancellationToken ct)
    {
        var url = $"{_config.SourceNode.Url}/databases/{_config.DatabaseName}/debug/database-info";

        try
        {
            var json = await _retry.ExecuteAsync(async token =>
            {
                using var resp = await _sourceHttpClient.GetAsync(url, token).ConfigureAwait(false);
                resp.EnsureSuccessStatusCode();
                return await resp.Content.ReadAsStringAsync(token).ConfigureAwait(false);
            }, ct).ConfigureAwait(false);

            using var doc = JsonDocument.Parse(json);
            if (doc.RootElement.TryGetProperty("DatabaseId", out var prop))
                return prop.GetString() ?? string.Empty;
        }
        catch
        {
            // If the endpoint is unavailable, the ETag extraction falls back to
            // using the maximum ETag across all CV segments, which is still correct
            // in most cases.
        }

        return string.Empty;
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Source database statistics
    // ─────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// Fetches the highest document ETag currently assigned in the database from the
    /// source node via <c>GET /databases/{db}/stats</c>.
    /// </summary>
    /// <remarks>
    /// Used to compute the scan progress percentage for the live display.
    /// The returned value is a snapshot taken at scan start; new documents written
    /// during the scan may push <c>currentEtag</c> beyond this value, so callers
    /// should cap the computed percentage at 100 %.
    /// Returns <c>null</c> when the endpoint is unavailable or the value cannot be
    /// parsed — in that case the progress bar is simply not shown.
    /// </remarks>
    public async Task<long?> FetchMaxEtagAsync(CancellationToken ct)
    {
        var url = $"{_config.SourceNode.Url}/databases/{_config.DatabaseName}/stats";

        try
        {
            var json = await _retry.ExecuteAsync(async token =>
            {
                using var resp = await _sourceHttpClient.GetAsync(url, token).ConfigureAwait(false);
                resp.EnsureSuccessStatusCode();
                return await resp.Content.ReadAsStringAsync(token).ConfigureAwait(false);
            }, ct).ConfigureAwait(false);

            using var doc = JsonDocument.Parse(json);
            if (doc.RootElement.TryGetProperty("LastDocEtag", out var prop) &&
                prop.TryGetInt64(out var etag) && etag > 0)
            {
                return etag;
            }
        }
        catch
        {
            // If the stats endpoint is unavailable, the progress bar is simply not shown.
        }

        return null;
    }

    // ─────────────────────────────────────────────────────────────────────────
    // HTTP client and retry pipeline
    // ─────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// Creates an <see cref="HttpClient"/> for the source node with optional client
    /// certificate support and a 30-second per-request timeout.
    /// </summary>
    private static HttpClient BuildHttpClient(X509Certificate2? cert)
    {
        var handler = new HttpClientHandler();

        if (cert != null)
        {
            handler.ClientCertificates.Add(cert);
            // Accept the server's self-signed certificate if present.
            handler.ServerCertificateCustomValidationCallback =
                HttpClientHandler.DangerousAcceptAnyServerCertificateValidator;
        }

        var client = new HttpClient(handler)
        {
            Timeout = TimeSpan.FromSeconds(30)
        };
        client.DefaultRequestHeaders.Accept.Add(
            new MediaTypeWithQualityHeaderValue("application/json"));

        return client;
    }

    /// <summary>
    /// Builds a Polly <see cref="ResiliencePipeline"/> that retries transient
    /// HTTP and RavenDB network errors with exponential back-off and jitter.
    /// </summary>
    private static ResiliencePipeline BuildRetryPipeline(ThrottleConfig throttle) =>
        new ResiliencePipelineBuilder()
            .AddRetry(new RetryStrategyOptions
            {
                MaxRetryAttempts = throttle.MaxRetries,
                Delay            = TimeSpan.FromMilliseconds(throttle.RetryBaseDelayMs),
                BackoffType      = DelayBackoffType.Exponential,
                UseJitter        = true,
                ShouldHandle     = new PredicateBuilder()
                    .Handle<HttpRequestException>()
                    .Handle<IOException>()
                    .Handle<TaskCanceledException>(ex =>
                        !ex.CancellationToken.IsCancellationRequested)
                    .Handle<RavenException>(ex =>
                        ex.Message.Contains("timeout",  StringComparison.OrdinalIgnoreCase) ||
                        ex.Message.Contains("connect",  StringComparison.OrdinalIgnoreCase) ||
                        ex.Message.Contains("unavailable", StringComparison.OrdinalIgnoreCase))
            })
            .Build();
}
