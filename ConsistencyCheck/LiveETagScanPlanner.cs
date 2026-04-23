using System.Net.Http.Headers;
using System.Security.Cryptography.X509Certificates;
using System.Text.Json;
using Polly;
using Polly.Retry;
using Raven.Client.Documents;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Conventions;
using Raven.Client.Exceptions;
using Sparrow.Json;

namespace ConsistencyCheck;

/// <summary>
/// Streams document IDs from one node by ETag, fetches metadata live from all nodes,
/// evaluates consistency, and writes a repair plan directly — no snapshot import needed.
/// </summary>
internal sealed class LiveETagScanPlanner : IAsyncDisposable
{
    private readonly AppConfig _config;
    private readonly StateStore _stateStore;
    private readonly HttpClient _httpClient;
    private readonly X509Certificate2? _certificate;
    private readonly ResiliencePipeline _retry;
    private readonly Dictionary<string, IDocumentStore> _storesByNodeUrl;
    private readonly Dictionary<string, string> _labelsByNodeUrl;

    public LiveETagScanPlanner(AppConfig config, StateStore stateStore)
    {
        _config = config;
        _stateStore = stateStore;
        _certificate = ConfigWizard.LoadCertificate(config);
        var allowedHosts = config.Nodes
            .Select(node => Uri.TryCreate(node.Url, UriKind.Absolute, out var uri) ? uri.Host : null)
            .Where(static host => !string.IsNullOrWhiteSpace(host))
            .Select(static host => host!)
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToHashSet(StringComparer.OrdinalIgnoreCase);
        _httpClient = BuildHttpClient(_certificate, config.AllowInvalidServerCertificates, allowedHosts);
        _retry = BuildRetryPipeline(config.Throttle);
        _storesByNodeUrl = config.Nodes.ToDictionary(
            node => node.Url,
            node => (IDocumentStore)new DocumentStore
            {
                Urls = [node.Url],
                Database = config.DatabaseName,
                Certificate = _certificate,
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
            }.Initialize(),
            StringComparer.OrdinalIgnoreCase);
        _labelsByNodeUrl = config.Nodes.ToDictionary(
            node => node.Url,
            node => node.Label,
            StringComparer.OrdinalIgnoreCase);
    }

    public event Action<IndexedRunProgressUpdate>? ProgressUpdated;

    public async Task<RunStateDocument> RunAsync(RunStateDocument run, CancellationToken ct)
    {
        var sourceNode = _config.Nodes[_config.SourceNodeIndex];
        var nextEtag = run.SafeRestartEtag ?? _config.StartEtag ?? 0L;

        PublishProgress("starting live etag scan", sourceNode.Label, $"starting from etag {nextEtag:N0}", run);

        while (true)
        {
            ct.ThrowIfCancellationRequested();

            var items = await FetchNodePageAsync(sourceNode.Url, nextEtag, _config.Throttle.PageSize, ct).ConfigureAwait(false);

            if (items.Count == 0)
                break;

            var documentIds = new List<string>(items.Count);
            var sourceNodeStates = new Dictionary<string, NodeObservedState>(StringComparer.OrdinalIgnoreCase);
            var seenIds = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            var lastEtag = nextEtag;

            foreach (var item in items)
            {
                if (item.Etag > lastEtag)
                    lastEtag = item.Etag;

                if (ShouldIncludePageItem(item.Type, item.Flags) == false)
                    continue;

                if (!seenIds.Add(item.Id))
                    continue;

                documentIds.Add(item.Id);
                sourceNodeStates[item.Id] = new NodeObservedState
                {
                    NodeUrl = sourceNode.Url,
                    Present = true,
                    ChangeVector = item.ChangeVector,
                    Collection = item.Collection,
                    LastModified = item.LastModified
                };
            }

            nextEtag = lastEtag + 1;
            run.SafeRestartEtag = nextEtag;

            if (documentIds.Count > 0)
            {
                foreach (var chunk in documentIds.Chunk(_config.Throttle.ClusterLookupBatchSize))
                {
                    var result = await ProcessChunkAsync(run, chunk, sourceNodeStates, ct).ConfigureAwait(false);

                    run.DocumentsInspected += chunk.Length;
                    run.MismatchesFound += result.MismatchesFound;
                    run.RepairsPlanned += result.RepairsPlanned;

                    await _stateStore.PersistBatchAsync(
                            run,
                            result.Mismatches,
                            result.Repairs,
                            result.Guards,
                            result.Diagnostics,
                            ct)
                        .ConfigureAwait(false);
                }
            }
            else
            {
                await _stateStore.SaveRunAsync(run, ct).ConfigureAwait(false);
            }

            PublishProgress("scanning by etag", sourceNode.Label, $"next etag {nextEtag:N0}", run);

            if (_config.Throttle.DelayBetweenBatchesMs > 0)
                await Task.Delay(_config.Throttle.DelayBetweenBatchesMs, ct).ConfigureAwait(false);
        }

        run.IsComplete = true;
        run.CompletedAt = DateTimeOffset.UtcNow;

        await _stateStore.PersistBatchAsync(
                run,
                [],
                [],
                [],
                [
                    new DiagnosticDocument
                    {
                        Id = StateStore.CreateDiagnosticId(run.RunId),
                        RunId = run.RunId,
                        Kind = "LiveETagScanSummary",
                        Message =
                            $"Complete. DocumentsInspected={run.DocumentsInspected:N0}, MismatchesFound={run.MismatchesFound:N0}, RepairsPlanned={run.RepairsPlanned:N0}, FinalEtag={run.SafeRestartEtag}",
                        Timestamp = DateTimeOffset.UtcNow
                    }
                ],
                ct)
            .ConfigureAwait(false);

        PublishProgress("complete", null, "live etag scan complete", run);
        return run;
    }

    public async ValueTask DisposeAsync()
    {
        _httpClient.Dispose();
        foreach (var store in _storesByNodeUrl.Values)
            store.Dispose();

        // Do not dispose _certificate — it may be held in the global HttpClient cache.
        await Task.CompletedTask.ConfigureAwait(false);
    }

    private async Task<ChunkResult> ProcessChunkAsync(
        RunStateDocument run,
        string[] documentIds,
        IReadOnlyDictionary<string, NodeObservedState> sourceNodeStates,
        CancellationToken ct)
    {
        var snapshots = await FetchAllNodesMetadataAsync(documentIds, sourceNodeStates, ct).ConfigureAwait(false);

        var evaluations = new List<DocumentEvaluation>();
        foreach (var documentId in documentIds)
        {
            var evaluation = ConsistencyDecisionEngine.EvaluateDocument(
                documentId,
                snapshots[documentId],
                run.ChangeVectorSemanticsSnapshot);
            if (evaluation != null)
                evaluations.Add(evaluation);
        }

        var repairCandidates = new List<RepairCandidate>();
        foreach (var evaluation in evaluations)
        {
            if (string.Equals(evaluation.MismatchType, "AMBIGUOUS_CV", StringComparison.Ordinal))
            {
                evaluation.RepairDecision = "SkippedAmbiguousCv";
                continue;
            }

            if (string.Equals(evaluation.MismatchType, "COLLECTION_MISMATCH", StringComparison.Ordinal) ||
                string.IsNullOrWhiteSpace(evaluation.Collection) ||
                string.IsNullOrWhiteSpace(evaluation.WinnerNode))
            {
                evaluation.RepairDecision = "SkippedCollectionMismatch";
                continue;
            }

            repairCandidates.Add(new RepairCandidate(
                evaluation.DocumentId,
                evaluation.Collection!,
                evaluation.WinnerNode!,
                evaluation.WinnerCV,
                evaluation.AffectedNodes));
        }

        var guardedIds = repairCandidates.Count > 0
            ? await _stateStore.GetGuardedDocumentIdsAsync(
                    run.RunId,
                    repairCandidates.Select(c => c.DocumentId).ToArray(),
                    ct)
                .ConfigureAwait(false)
            : (IReadOnlySet<string>)new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        var mismatches = new List<MismatchDocument>();
        var repairs = new List<RepairDocument>();
        var guards = new List<RepairActionGuardDocument>();
        var repairsPlanned = 0L;

        foreach (var group in repairCandidates
                     .Where(c => !guardedIds.Contains(c.DocumentId))
                     .GroupBy(c => new RepairGroupKey(c.WinnerNode, c.Collection, c.WinnerCV)))
        {
            var candidates = group.ToList();
            var first = candidates[0];
            var groupDocIds = candidates.Select(c => c.DocumentId).ToArray();
            var affectedNodes = candidates
                .SelectMany(c => c.AffectedNodes)
                .Distinct(StringComparer.OrdinalIgnoreCase)
                .OrderBy(static n => n, StringComparer.OrdinalIgnoreCase)
                .ToList();

            repairs.Add(new RepairDocument
            {
                Id = StateStore.CreateRepairId(run.RunId),
                RunId = run.RunId,
                DocumentIds = groupDocIds.ToList(),
                Collection = first.Collection,
                WinnerNode = first.WinnerNode,
                WinnerCV = first.WinnerCV,
                AffectedNodes = affectedNodes,
                PatchOperationId = null,
                RepairStatus = "PatchPlannedDryRun",
                CompletedAt = DateTimeOffset.UtcNow
            });

            foreach (var candidate in candidates)
            {
                guards.Add(new RepairActionGuardDocument
                {
                    Id = StateStore.GetRepairActionGuardId(run.RunId, candidate.DocumentId),
                    RunId = run.RunId,
                    DocumentId = candidate.DocumentId,
                    WinnerNode = first.WinnerNode,
                    WinnerCV = first.WinnerCV,
                    PatchOperationId = null,
                    RecordedAt = DateTimeOffset.UtcNow
                });

                var eval = evaluations.First(e =>
                    string.Equals(e.DocumentId, candidate.DocumentId, StringComparison.OrdinalIgnoreCase));
                eval.RepairDecision = "PatchPlannedDryRun";
            }

            repairsPlanned += groupDocIds.Length;
        }

        // Mark guarded candidates as already planned
        foreach (var candidate in repairCandidates.Where(c => guardedIds.Contains(c.DocumentId)))
        {
            var eval = evaluations.First(e =>
                string.Equals(e.DocumentId, candidate.DocumentId, StringComparison.OrdinalIgnoreCase));
            if (string.IsNullOrWhiteSpace(eval.RepairDecision))
                eval.RepairDecision = "SkippedAlreadyPlanned";
        }

        foreach (var evaluation in evaluations)
        {
            // If the guard fired (document already seen in a previous ETag page of this run,
            // e.g. because it was modified mid-scan and got a new ETag), skip writing a new
            // MismatchDocument — the existing one already has the correct PatchPlannedDryRun
            // decision and writing SkippedAlreadyPlanned would silently overwrite it.
            if (string.Equals(evaluation.RepairDecision, "SkippedAlreadyPlanned", StringComparison.Ordinal))
                continue;

            mismatches.Add(new MismatchDocument
            {
                Id = StateStore.GetMismatchId(run.RunId, evaluation.DocumentId),
                RunId = run.RunId,
                DocumentId = evaluation.DocumentId,
                Collection = evaluation.Collection,
                MismatchType = evaluation.MismatchType,
                WinnerNode = evaluation.WinnerNode,
                WinnerCV = evaluation.WinnerCV,
                ObservedState = evaluation.ObservedState,
                RepairDecision = evaluation.RepairDecision,
                CurrentRepairDecision = evaluation.RepairDecision,
                DetectedAt = DateTimeOffset.UtcNow
            });
        }

        var diagnostic = new DiagnosticDocument
        {
            Id = StateStore.CreateDiagnosticId(run.RunId),
            RunId = run.RunId,
            Kind = "LiveETagScanBatch",
            UniqueIdsInLookupBatch = documentIds.Length,
            MismatchesInBatch = evaluations.Count,
            RepairsTriggered = (int)repairsPlanned,
            Message = $"UniqueIds={documentIds.Length}, Mismatches={evaluations.Count}, RepairsPlanned={repairsPlanned}",
            Timestamp = DateTimeOffset.UtcNow
        };

        return new ChunkResult(mismatches, repairs, guards, [diagnostic], evaluations.Count, repairsPlanned);
    }

    private async Task<Dictionary<string, List<NodeObservedState>>> FetchAllNodesMetadataAsync(
        string[] documentIds,
        IReadOnlyDictionary<string, NodeObservedState> sourceNodeStates,
        CancellationToken ct)
    {
        var sourceNodeUrl = _config.Nodes[_config.SourceNodeIndex].Url;

        var tasks = _config.Nodes
            .Where(node => !string.Equals(node.Url, sourceNodeUrl, StringComparison.OrdinalIgnoreCase))
            .Select(async node =>
            {
                var metadata = await FetchBatchMetadataAsync(_storesByNodeUrl[node.Url], node.Url, documentIds, ct).ConfigureAwait(false);
                return (NodeUrl: node.Url, Metadata: metadata);
            });

        var results = await Task.WhenAll(tasks).ConfigureAwait(false);
        var metadataByNode = results.ToDictionary(
            r => r.NodeUrl,
            r => r.Metadata,
            StringComparer.OrdinalIgnoreCase);

        var snapshots = documentIds.ToDictionary(
            id => id,
            _ => new List<NodeObservedState>(_config.Nodes.Count),
            StringComparer.OrdinalIgnoreCase);

        foreach (var node in _config.Nodes)
        {
            foreach (var documentId in documentIds)
            {
                NodeObservedState state;
                if (string.Equals(node.Url, sourceNodeUrl, StringComparison.OrdinalIgnoreCase))
                {
                    state = sourceNodeStates.TryGetValue(documentId, out var s)
                        ? s
                        : new NodeObservedState { NodeUrl = node.Url, Present = false };
                }
                else
                {
                    state = metadataByNode[node.Url][documentId];
                }
                snapshots[documentId].Add(state);
            }
        }

        return snapshots;
    }

    private async Task<Dictionary<string, NodeObservedState>> FetchBatchMetadataAsync(
        IDocumentStore store,
        string nodeUrl,
        string[] documentIds,
        CancellationToken ct)
    {
        return await _retry.ExecuteAsync(async token =>
        {
            using var contextScope = store.GetRequestExecutor(_config.DatabaseName)
                .ContextPool
                .AllocateOperationContext(out JsonOperationContext context);

            var command = new GetDocumentsCommand(
                store.Conventions,
                documentIds,
                includes: null,
                metadataOnly: true);

            await store.GetRequestExecutor(_config.DatabaseName)
                .ExecuteAsync(command, context, null, token)
                .ConfigureAwait(false);

            var results = documentIds.ToDictionary(
                documentId => documentId,
                _ => new NodeObservedState { NodeUrl = nodeUrl, Present = false },
                StringComparer.OrdinalIgnoreCase);

            var documents = command.Result?.Results;
            if (documents == null)
                return results;

            for (var i = 0; i < documents.Length; i++)
            {
                if (documents[i] is not BlittableJsonReaderObject document)
                    continue;

                if (!document.TryGet("@metadata", out BlittableJsonReaderObject metadata))
                    continue;

                if (!metadata.TryGet("@id", out string actualId) ||
                    string.IsNullOrWhiteSpace(actualId) ||
                    !results.TryGetValue(actualId, out var state))
                {
                    continue;
                }

                metadata.TryGet("@change-vector", out string changeVector);
                metadata.TryGet("@last-modified", out string lastModified);
                metadata.TryGet("@collection", out string collection);

                state.Present = true;
                state.ChangeVector = changeVector;
                state.LastModified = lastModified;
                state.Collection = collection;
            }

            return results;
        }, ct).ConfigureAwait(false);
    }

    private async Task<List<ETagItem>> FetchNodePageAsync(
        string nodeUrl,
        long startEtag,
        int pageSize,
        CancellationToken ct)
    {
        var url = $"{nodeUrl.TrimEnd('/')}/databases/{_config.DatabaseName}/debug/replication/all-items" +
                  $"?etag={startEtag}&pageSize={pageSize}&type=Document&format=json";

        var responseBody = await _retry.ExecuteAsync(async token =>
        {
            using var response = await _httpClient.GetAsync(url, token).ConfigureAwait(false);
            if (!response.IsSuccessStatusCode)
            {
                var errorBody = await response.Content.ReadAsStringAsync(token).ConfigureAwait(false);
                throw new HttpRequestException(
                    $"Request to {url} failed with {(int)response.StatusCode} ({response.ReasonPhrase}): {errorBody}");
            }

            return await response.Content.ReadAsStringAsync(token).ConfigureAwait(false);
        }, ct).ConfigureAwait(false);

        return ParseETagItems(responseBody);
    }

    private static List<ETagItem> ParseETagItems(string json)
    {
        var items = new List<ETagItem>();

        using var document = JsonDocument.Parse(json);
        if (!document.RootElement.TryGetProperty("Results", out var results))
            return items;

        foreach (var element in results.EnumerateArray())
        {
            var id = element.TryGetProperty("Id", out var idProp) ? idProp.GetString() : null;
            var type = element.TryGetProperty("Type", out var typeProp) ? typeProp.GetString() : null;
            var flags = element.TryGetProperty("Flags", out var flagsProp) ? flagsProp.GetString() : null;
            var changeVector = element.TryGetProperty("ChangeVector", out var cvProp) ? cvProp.GetString() : null;
            var collection = element.TryGetProperty("Collection", out var colProp) ? colProp.GetString() : null;
            var lastModified = element.TryGetProperty("LastModified", out var lmProp) ? lmProp.GetString() : null;

            if (string.IsNullOrWhiteSpace(id) ||
                string.IsNullOrWhiteSpace(type) ||
                !element.TryGetProperty("Etag", out var etagProp) ||
                !TryReadInt64(etagProp, out var etag))
            {
                continue;
            }

            items.Add(new ETagItem(id, etag, type, flags, changeVector, collection, lastModified));
        }

        return items;
    }

    private static bool TryReadInt64(JsonElement element, out long value)
    {
        if (element.ValueKind == JsonValueKind.Number && element.TryGetInt64(out value))
            return true;

        if (element.ValueKind == JsonValueKind.String)
        {
            var text = element.GetString();
            if (long.TryParse(text, out value))
                return true;
        }

        value = 0;
        return false;
    }

    internal static bool ShouldIncludePageItem(string type, string? flags)
    {
        if (!string.Equals(type, "Document", StringComparison.OrdinalIgnoreCase))
            return false;

        if (HasFlag(flags, "Conflicted"))
            return false;

        if (HasFlag(flags, "DeleteRevision"))
            return false;

        return true;
    }

    private static bool HasFlag(string? flags, string expectedFlag)
    {
        if (string.IsNullOrWhiteSpace(flags))
            return false;

        foreach (var flag in flags.Split(',', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries))
        {
            if (string.Equals(flag, expectedFlag, StringComparison.OrdinalIgnoreCase))
                return true;
        }

        return false;
    }

    private void PublishProgress(string phase, string? currentNodeLabel, string? detail, RunStateDocument run)
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
            CandidateDocumentsFound: run.MismatchesFound,
            CandidateDocumentsProcessed: run.MismatchesFound,
            CandidateDocumentsExcludedBySkippedSnapshots: 0,
            DocumentsInspected: run.DocumentsInspected,
            UniqueVersionsCompared: run.DocumentsInspected,
            MismatchesFound: run.MismatchesFound,
            RepairsPlanned: run.RepairsPlanned,
            RepairsAttempted: 0,
            RepairsPatchedOnWinner: 0,
            RepairsFailed: 0));
    }

    private static HttpClient BuildHttpClient(
        X509Certificate2? certificate,
        bool allowInvalidServerCertificates,
        IReadOnlySet<string> allowedHosts)
    {
        var handler = new HttpClientHandler();
        handler.ClientCertificateOptions = ClientCertificateOption.Manual;
        if (certificate != null)
            handler.ClientCertificates.Add(certificate);

        if (allowInvalidServerCertificates)
        {
            handler.ServerCertificateCustomValidationCallback = (request, _, _, _) =>
                request?.RequestUri is { Host: var host } &&
                allowedHosts.Contains(host);
        }

        var client = new HttpClient(handler)
        {
            Timeout = TimeSpan.FromSeconds(30)
        };
        client.DefaultRequestHeaders.Accept.Add(
            new MediaTypeWithQualityHeaderValue("application/json"));
        return client;
    }

    private static ResiliencePipeline BuildRetryPipeline(ThrottleConfig throttle)
        => new ResiliencePipelineBuilder()
            .AddRetry(new RetryStrategyOptions
            {
                MaxRetryAttempts = throttle.MaxRetries,
                Delay = TimeSpan.FromMilliseconds(throttle.RetryBaseDelayMs),
                BackoffType = DelayBackoffType.Exponential,
                UseJitter = true,
                ShouldHandle = new PredicateBuilder()
                    .Handle<HttpRequestException>()
                    .Handle<IOException>()
                    .Handle<TaskCanceledException>(exception =>
                        !exception.CancellationToken.IsCancellationRequested)
                    .Handle<RavenException>(exception =>
                        exception.Message.Contains("timeout", StringComparison.OrdinalIgnoreCase) ||
                        exception.Message.Contains("connect", StringComparison.OrdinalIgnoreCase) ||
                        exception.Message.Contains("unavailable", StringComparison.OrdinalIgnoreCase))
            })
            .Build();

    private sealed record ETagItem(
        string Id,
        long Etag,
        string Type,
        string? Flags,
        string? ChangeVector,
        string? Collection,
        string? LastModified);

    private sealed record RepairCandidate(
        string DocumentId,
        string Collection,
        string WinnerNode,
        string? WinnerCV,
        List<string> AffectedNodes);

    private sealed record RepairGroupKey(string WinnerNode, string Collection, string? WinnerCV);

    private sealed record ChunkResult(
        IReadOnlyList<MismatchDocument> Mismatches,
        IReadOnlyList<RepairDocument> Repairs,
        IReadOnlyList<RepairActionGuardDocument> Guards,
        IReadOnlyList<DiagnosticDocument> Diagnostics,
        long MismatchesFound,
        long RepairsPlanned);
}
