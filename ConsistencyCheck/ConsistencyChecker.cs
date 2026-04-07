using System.Net.Http.Headers;
using System.Security.Cryptography.X509Certificates;
using System.Text.Json;
using Polly;
using Polly.Retry;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Queries;
using Raven.Client.Exceptions;
using Sparrow.Json;

namespace ConsistencyCheck;

/// <summary>
/// Symmetric full-pass checker that iterates every node independently and persists all
/// durable state into the local RavenDB state store.
/// </summary>
public sealed class ConsistencyChecker : IAsyncDisposable
{
    private readonly AppConfig _config;
    private readonly StateStore _stateStore;
    private readonly HttpClient _httpClient;
    private readonly X509Certificate2? _certificate;
    private readonly ResiliencePipeline _retry;
    private readonly Dictionary<string, IDocumentStore> _storesByNodeUrl;
    private readonly Dictionary<string, string> _labelsByNodeUrl;

    public ConsistencyChecker(AppConfig config, StateStore stateStore)
    {
        _config = config;
        _stateStore = stateStore;
        _certificate = ConfigWizard.LoadCertificate(config);
        HashSet<string> allowedServerCertificateHosts = config.Nodes
            .Select(node => Uri.TryCreate(node.Url, UriKind.Absolute, out var uri) ? uri.Host : null)
            .Where(static host => string.IsNullOrWhiteSpace(host) == false)
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToHashSet(StringComparer.OrdinalIgnoreCase)!;
        _httpClient = BuildHttpClient(
            _certificate,
            config.AllowInvalidServerCertificates,
            allowedServerCertificateHosts);
        _retry = BuildRetryPipeline(config.Throttle);
        _storesByNodeUrl = config.Nodes.ToDictionary(
            node => node.Url,
            node => new DocumentStore
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

    public event Action<RunProgressUpdate>? ProgressUpdated;

    public async Task<RunStateDocument> RunAsync(RunStateDocument run, CancellationToken ct)
    {
        var orderedCursors = GetOrderedCursors(run);

        while (orderedCursors.Any(cursor => !cursor.IsExhausted))
        {
            foreach (var cursor in orderedCursors)
            {
                if (cursor.IsExhausted)
                    continue;

                ct.ThrowIfCancellationRequested();

                var page = await FetchNodePageAsync(
                        cursor.NodeUrl,
                        cursor.NextEtag,
                        _config.Throttle.PageSize,
                        ct)
                    .ConfigureAwait(false);

                if (page.Items.Count == 0)
                {
                    cursor.IsExhausted = true;
                    cursor.LastPageFirstEtag = null;
                    cursor.LastPageLastEtag = null;
                    cursor.LastPageCount = 0;
                    cursor.LastPageFetchedAt = DateTimeOffset.UtcNow;
                    run.SafeRestartEtag = ComputeSafeRestartEtag(run);

                    await _stateStore.PersistBatchAsync(
                            run,
                            [],
                            [],
                            [],
                            CreateDiagnostics(
                                CreatePageDiagnostic(
                                    run,
                                    page,
                                    0,
                                    0,
                                    0,
                                    0,
                                    0,
                                    0,
                                    0,
                                    "Node exhausted")),
                            ct)
                        .ConfigureAwait(false);

                    PublishProgress(run, cursor.NodeUrl);
                    continue;
                }

                var pageOutcome = await ProcessPageAsync(run, page, ct).ConfigureAwait(false);

                var lastItemEtag = page.Items[^1].Etag;
                cursor.NextEtag = lastItemEtag >= cursor.NextEtag
                    ? lastItemEtag + 1
                    : cursor.NextEtag + 1;
                cursor.LastPageFirstEtag = page.Items[0].Etag;
                cursor.LastPageLastEtag = lastItemEtag;
                cursor.LastPageCount = page.Items.Count;
                cursor.LastPageFetchedAt = DateTimeOffset.UtcNow;

                run.DocumentsInspected += pageOutcome.DocumentsInspected;
                run.UniqueVersionsCompared += pageOutcome.UniqueVersionsCompared;
                run.MismatchesFound += pageOutcome.MismatchesFound;
                run.RepairsPlanned += pageOutcome.RepairsPlanned;
                run.RepairsAttempted += pageOutcome.RepairsAttempted;
                run.RepairsPatchedOnWinner += pageOutcome.RepairsPatchedOnWinner;
                run.RepairsFailed += pageOutcome.RepairsFailed;
                run.SafeRestartEtag = ComputeSafeRestartEtag(run);

                await _stateStore.PersistBatchAsync(
                        run,
                        pageOutcome.Mismatches,
                        pageOutcome.Repairs,
                        pageOutcome.RepairGuards,
                        pageOutcome.Diagnostics,
                        ct)
                    .ConfigureAwait(false);

                if (pageOutcome.ProcessedVersionKeys.Count > 0)
                {
                    await _stateStore.Dedup.MarkProcessedAsync(run.RunId, pageOutcome.ProcessedVersionKeys, ct)
                        .ConfigureAwait(false);
                }

                PublishProgress(run, cursor.NodeUrl);

                if (run.RunMode == RunMode.ScanOnly &&
                    _config.Mode == CheckMode.FirstMismatch &&
                    run.MismatchesFound > 0)
                {
                    return run;
                }

                if (_config.Throttle.DelayBetweenBatchesMs > 0)
                    await Task.Delay(_config.Throttle.DelayBetweenBatchesMs, ct).ConfigureAwait(false);
            }
        }

        run.IsComplete = true;
        run.CompletedAt = DateTimeOffset.UtcNow;
        run.SafeRestartEtag = ComputeSafeRestartEtag(run);

        await _stateStore.PersistBatchAsync(
                run,
                [],
                [],
                [],
                CreateDiagnostics(new DiagnosticDocument
                {
                    Id = StateStore.CreateDiagnosticId(run.RunId),
                    RunId = run.RunId,
                    Kind = "RunSummary",
                    Message =
                        $"Complete. DocumentsInspected={run.DocumentsInspected}, UniqueVersionsCompared={run.UniqueVersionsCompared}, " +
                        $"MismatchesFound={run.MismatchesFound}, RepairsPlanned={run.RepairsPlanned}, RepairsAttempted={run.RepairsAttempted}, " +
                        $"RepairsPatchedOnWinner={run.RepairsPatchedOnWinner}, RepairsFailed={run.RepairsFailed}, " +
                        $"SafeRestartEtag={run.SafeRestartEtag}",
                    Timestamp = DateTimeOffset.UtcNow
                }),
                ct)
            .ConfigureAwait(false);

        PublishProgress(run, nodeUrl: null);
        return run;
    }

    public async ValueTask DisposeAsync()
    {
        _httpClient.Dispose();
        foreach (var store in _storesByNodeUrl.Values)
            store.Dispose();

        _certificate?.Dispose();
        await Task.CompletedTask.ConfigureAwait(false);
    }

    private async Task<PageOutcome> ProcessPageAsync(
        RunStateDocument run,
        IterationPage page,
        CancellationToken ct)
    {
        var pageDistinctHashes = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var workItems = new List<PageWorkItem>();
        var skippedNonDocumentType = 0;
        var skippedConflicted = 0;
        var skippedDuplicateWithinPage = 0;
        var semanticsSnapshot = run.ChangeVectorSemanticsSnapshot;

        foreach (var item in page.Items)
        {
            if (!string.Equals(item.Type, "Document", StringComparison.OrdinalIgnoreCase))
            {
                skippedNonDocumentType++;
                continue;
            }

            if (HasFlag(item.Flags, "Conflicted"))
            {
                skippedConflicted++;
                continue;
            }

            var normalizedCv = ConsistencyDecisionEngine.NormalizeChangeVector(
                item.ChangeVector,
                semanticsSnapshot?.ExplicitUnusedDatabaseIdSet);
            var versionKey = VersionKey.Create(item.Id, normalizedCv);
            if (!pageDistinctHashes.Add(versionKey.HashHex))
            {
                skippedDuplicateWithinPage++;
                continue;
            }

            workItems.Add(new PageWorkItem(item.Id, versionKey));
        }

        var knownHashes = await _stateStore.Dedup.GetKnownHashesAsync(
                run.RunId,
                workItems.Select(item => item.VersionKey).ToArray(),
                ct)
            .ConfigureAwait(false);

        var newVersionKeys = new List<VersionKey>();
        var lookupIds = new List<string>();
        var seenLookupIds = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var skippedByDedup = 0;

        foreach (var workItem in workItems)
        {
            if (knownHashes.Contains(workItem.VersionKey.HashHex))
            {
                skippedByDedup++;
                continue;
            }

            newVersionKeys.Add(workItem.VersionKey);
            if (seenLookupIds.Add(workItem.DocumentId))
                lookupIds.Add(workItem.DocumentId);
        }

        var mismatches = new List<MismatchDocument>();
        var repairs = new List<RepairDocument>();
        var repairGuards = new List<RepairActionGuardDocument>();
        var diagnostics = new List<DiagnosticDocument>();

        long mismatchCount = 0;
        long repairsPlanned = 0;
        long repairsAttempted = 0;
        long repairsPatchedOnWinner = 0;
        long repairsFailed = 0;

        foreach (var chunk in lookupIds.Chunk(_config.Throttle.ClusterLookupBatchSize))
        {
            var lookupOutcome = await ProcessLookupChunkAsync(run, page.NodeUrl, chunk, ct).ConfigureAwait(false);
            mismatches.AddRange(lookupOutcome.Mismatches);
            repairs.AddRange(lookupOutcome.Repairs);
            repairGuards.AddRange(lookupOutcome.RepairGuards);
            diagnostics.AddRange(lookupOutcome.Diagnostics);
            mismatchCount += lookupOutcome.MismatchesFound;
            repairsPlanned += lookupOutcome.RepairsPlanned;
            repairsAttempted += lookupOutcome.RepairsAttempted;
            repairsPatchedOnWinner += lookupOutcome.RepairsPatchedOnWinner;
            repairsFailed += lookupOutcome.RepairsFailed;
        }

        diagnostics.AddRange(CreateDiagnostics(CreatePageDiagnostic(
            run,
            page,
            workItems.Count,
            skippedByDedup,
            newVersionKeys.Count,
            lookupIds.Count,
            (int)mismatchCount,
            run.RunMode == RunMode.DryRunRepair ? (int)repairsPlanned : (int)repairsAttempted,
            skippedNonDocumentType + skippedConflicted + skippedDuplicateWithinPage,
            $"SkippedNonDocumentType={skippedNonDocumentType}, SkippedConflicted={skippedConflicted}, SkippedDuplicateWithinPage={skippedDuplicateWithinPage}")));

        return new PageOutcome(
            DocumentsInspected: workItems.Count,
            UniqueVersionsCompared: newVersionKeys.Count,
            MismatchesFound: mismatchCount,
            RepairsPlanned: repairsPlanned,
            RepairsAttempted: repairsAttempted,
            RepairsPatchedOnWinner: repairsPatchedOnWinner,
            RepairsFailed: repairsFailed,
            ProcessedVersionKeys: newVersionKeys,
            Mismatches: mismatches,
            Repairs: repairs,
            RepairGuards: repairGuards,
            Diagnostics: diagnostics);
    }

    private async Task<LookupOutcome> ProcessLookupChunkAsync(
        RunStateDocument run,
        string pageNodeUrl,
        IReadOnlyCollection<string> documentIds,
        CancellationToken ct)
    {
        if (documentIds.Count == 0)
            return new LookupOutcome([], [], [], [], 0, 0, 0, 0, 0);

        var snapshots = await FetchClusterSnapshotsAsync(documentIds.ToArray(), ct).ConfigureAwait(false);
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

        var mismatches = new List<MismatchDocument>();
        var repairs = new List<RepairDocument>();
        var repairGuards = new List<RepairActionGuardDocument>();
        var diagnostics = new List<DiagnosticDocument>();
        var repairCandidates = new List<RepairCandidate>();
        var supportsRepairPlanning = run.RunMode is RunMode.ScanAndRepair or RunMode.DryRunRepair;

        foreach (var evaluation in evaluations)
        {
            if (run.RunMode == RunMode.ScanOnly)
            {
                evaluation.RepairDecision = "ScanOnly_NoRepair";
                continue;
            }

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

        long repairsPlanned = 0;
        long repairsAttempted = 0;
        long repairsPatchedOnWinner = 0;
        long repairsFailed = 0;

        if (supportsRepairPlanning && repairCandidates.Count > 0)
        {
            var guardedIds = await _stateStore.GetGuardedDocumentIdsAsync(
                    run.RunId,
                    repairCandidates.Select(candidate => candidate.DocumentId).ToArray(),
                    ct)
                .ConfigureAwait(false);

            var candidatesToExecute = new List<RepairCandidate>();
            foreach (var candidate in repairCandidates)
            {
                if (guardedIds.Contains(candidate.DocumentId))
                {
                    continue;
                }

                candidatesToExecute.Add(candidate);
            }

            foreach (var group in candidatesToExecute
                         .GroupBy(candidate => new RepairGroupKey(candidate.WinnerNode, candidate.Collection, candidate.WinnerCV))
                         .SelectMany(group => group.Chunk(_config.Throttle.PageSize)))
            {
                var repairOutcome = await ExecuteRepairGroupAsync(run, group, evaluations, ct).ConfigureAwait(false);
                repairs.Add(repairOutcome.RepairDocument);
                repairGuards.AddRange(repairOutcome.RepairGuards);
                repairsPlanned += repairOutcome.RepairsPlanned;
                repairsAttempted += repairOutcome.RepairsAttempted;
                repairsPatchedOnWinner += repairOutcome.RepairsPatchedOnWinner;
                repairsFailed += repairOutcome.RepairsFailed;
            }
        }

        foreach (var evaluation in evaluations)
        {
            mismatches.Add(new MismatchDocument
            {
                Id = StateStore.GetMismatchId(run.RunId, evaluation.DocumentId),
                RunId = run.RunId,
                DocumentId = evaluation.DocumentId,
                Collection = evaluation.Collection,
                MismatchType = evaluation.MismatchType,
                WinnerNode = evaluation.WinnerNode,
                WinnerCV = evaluation.WinnerCV,
                ObservedState = evaluation.ObservedState
                    .Select(CloneObservedState)
                    .ToList(),
                RepairDecision = evaluation.RepairDecision,
                CurrentRepairDecision = evaluation.RepairDecision,
                DetectedAt = DateTimeOffset.UtcNow
            });
        }

        diagnostics.AddRange(CreateDiagnostics(new DiagnosticDocument
        {
            Id = StateStore.CreateDiagnosticId(run.RunId),
            RunId = run.RunId,
            Kind = "LookupBatch",
            NodeUrl = pageNodeUrl,
            UniqueIdsInLookupBatch = documentIds.Count,
            MismatchesInBatch = evaluations.Count,
            RepairsTriggered = run.RunMode == RunMode.DryRunRepair
                ? (int)repairsPlanned
                : (int)repairsAttempted,
            Message =
                run.RunMode == RunMode.DryRunRepair
                    ? $"UniqueIds={documentIds.Count}, Mismatches={evaluations.Count}, RepairsPlanned={repairsPlanned}"
                    : $"UniqueIds={documentIds.Count}, Mismatches={evaluations.Count}, RepairsAttempted={repairsAttempted}, RepairsFailed={repairsFailed}",
            Timestamp = DateTimeOffset.UtcNow
        }));

        return new LookupOutcome(
            mismatches,
            repairs,
            repairGuards,
            diagnostics,
            evaluations.Count,
            repairsPlanned,
            repairsAttempted,
            repairsPatchedOnWinner,
            repairsFailed);
    }

    private async Task<RepairGroupOutcome> ExecuteRepairGroupAsync(
        RunStateDocument run,
        IReadOnlyCollection<RepairCandidate> candidates,
        IReadOnlyCollection<DocumentEvaluation> evaluations,
        CancellationToken ct)
    {
        var first = candidates.First();
        var documentIds = candidates.Select(candidate => candidate.DocumentId).ToArray();
        var affectedNodes = candidates
            .SelectMany(candidate => candidate.AffectedNodes)
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .OrderBy(node => node, StringComparer.OrdinalIgnoreCase)
            .ToList();

        var query = RepairPatchQueryBuilder.BuildTouchWinnerPatchQuery(first.Collection);
        var indexQuery = new IndexQuery
        {
            Query = query,
            QueryParameters = new Parameters
            {
                ["ids"] = documentIds
            }
        };

        if (run.RunMode == RunMode.DryRunRepair)
        {
            var dryRunDocument = new RepairDocument
            {
                Id = StateStore.CreateRepairId(run.RunId),
                RunId = run.RunId,
                DocumentIds = documentIds.ToList(),
                Collection = first.Collection,
                WinnerNode = first.WinnerNode,
                WinnerCV = first.WinnerCV,
                AffectedNodes = affectedNodes,
                PatchOperationId = null,
                RepairStatus = "PatchPlannedDryRun",
                CompletedAt = DateTimeOffset.UtcNow
            };

            var dryRunGuards = documentIds
                .Select(documentId => new RepairActionGuardDocument
                {
                    Id = StateStore.GetRepairActionGuardId(run.RunId, documentId),
                    RunId = run.RunId,
                    DocumentId = documentId,
                    WinnerNode = first.WinnerNode,
                    WinnerCV = first.WinnerCV,
                    PatchOperationId = null,
                    RecordedAt = DateTimeOffset.UtcNow
                })
                .ToList();

            foreach (var candidate in candidates)
            {
                var evaluation = evaluations.First(e =>
                    string.Equals(e.DocumentId, candidate.DocumentId, StringComparison.OrdinalIgnoreCase));
                evaluation.RepairDecision = "PatchPlannedDryRun";
            }

            return new RepairGroupOutcome(
                dryRunDocument,
                dryRunGuards,
                documentIds.Length,
                0,
                0,
                0);
        }

        var repairDocument = new RepairDocument
        {
            Id = StateStore.CreateRepairId(run.RunId),
            RunId = run.RunId,
            DocumentIds = documentIds.ToList(),
            Collection = first.Collection,
            WinnerNode = first.WinnerNode,
            WinnerCV = first.WinnerCV,
            AffectedNodes = affectedNodes,
            RepairStatus = "PatchFailed",
            CompletedAt = DateTimeOffset.UtcNow
        };

        var guards = documentIds
            .Select(documentId => new RepairActionGuardDocument
            {
                Id = StateStore.GetRepairActionGuardId(run.RunId, documentId),
                RunId = run.RunId,
                DocumentId = documentId,
                WinnerNode = first.WinnerNode,
                WinnerCV = first.WinnerCV,
                RecordedAt = DateTimeOffset.UtcNow
            })
            .ToList();

        try
        {
            var operation = await _storesByNodeUrl[first.WinnerNode]
                .Operations
                .SendAsync(
                    new PatchByQueryOperation(indexQuery, new QueryOperationOptions { RetrieveDetails = true }),
                    token: ct)
                .ConfigureAwait(false);

            var result = await operation.WaitForCompletionAsync<BulkOperationResult>(ct).ConfigureAwait(false);
            repairDocument.CompletedAt = DateTimeOffset.UtcNow;

            var statusById = BuildPatchStatusById(result, documentIds);
            var patchedCount = 0L;
            var failedCount = 0L;

            foreach (var candidate in candidates)
            {
                var evaluation = evaluations.First(e =>
                    string.Equals(e.DocumentId, candidate.DocumentId, StringComparison.OrdinalIgnoreCase));

                if (!statusById.TryGetValue(candidate.DocumentId, out var status))
                {
                    evaluation.RepairDecision = result.Total > 0
                        ? "PatchCompletedOnWinner"
                        : "SkippedStateChanged";
                }
                else
                {
                    evaluation.RepairDecision = MapPatchStatusToDecision(status);
                }

                if (string.Equals(evaluation.RepairDecision, "PatchCompletedOnWinner", StringComparison.Ordinal))
                    patchedCount++;
                else
                    failedCount++;
            }

            repairDocument.RepairStatus = patchedCount > 0
                ? "PatchCompletedOnWinner"
                : "SkippedStateChanged";
            repairDocument.Error = failedCount > 0
                ? $"Non-patched documents in group: {failedCount} of {documentIds.Length}."
                : null;

            return new RepairGroupOutcome(
                repairDocument,
                guards,
                0,
                documentIds.Length,
                patchedCount,
                failedCount);
        }
        catch (Exception ex)
        {
            repairDocument.CompletedAt = DateTimeOffset.UtcNow;
            repairDocument.RepairStatus = "PatchFailed";
            repairDocument.Error = ex.Message;

            foreach (var candidate in candidates)
            {
                var evaluation = evaluations.First(e =>
                    string.Equals(e.DocumentId, candidate.DocumentId, StringComparison.OrdinalIgnoreCase));
                evaluation.RepairDecision = "PatchFailed";
            }

            return new RepairGroupOutcome(
                repairDocument,
                guards,
                0,
                documentIds.Length,
                0,
                documentIds.Length);
        }
    }

    private static Dictionary<string, PatchStatus> BuildPatchStatusById(
        BulkOperationResult result,
        IReadOnlyCollection<string> fallbackIds)
    {
        var statusById = new Dictionary<string, PatchStatus>(StringComparer.OrdinalIgnoreCase);
        foreach (var detail in result.Details.OfType<BulkOperationResult.PatchDetails>())
        {
            if (!string.IsNullOrWhiteSpace(detail.Id))
                statusById[detail.Id] = detail.Status;
        }

        if (statusById.Count == 0 && result.Total > 0)
        {
            foreach (var id in fallbackIds)
                statusById[id] = PatchStatus.Patched;
        }

        return statusById;
    }

    private static string MapPatchStatusToDecision(PatchStatus status)
        => status switch
        {
            PatchStatus.Patched => "PatchCompletedOnWinner",
            PatchStatus.DocumentDoesNotExist => "SkippedStateChanged",
            PatchStatus.NotModified => "PatchFailed",
            PatchStatus.Skipped => "PatchFailed",
            PatchStatus.Created => "PatchFailed",
            _ => "PatchFailed"
        };

    private static RepairDocument CreateRepairDecision(
        RunStateDocument run,
        DocumentEvaluation evaluation,
        string status,
        string? error)
    {
        return new RepairDocument
        {
            Id = StateStore.CreateRepairId(run.RunId),
            RunId = run.RunId,
            DocumentIds = [evaluation.DocumentId],
            Collection = evaluation.Collection,
            WinnerNode = evaluation.WinnerNode ?? string.Empty,
            WinnerCV = evaluation.WinnerCV,
            AffectedNodes = evaluation.AffectedNodes.ToList(),
            RepairStatus = status,
            CompletedAt = DateTimeOffset.UtcNow,
            Error = error
        };
    }

    private async Task<Dictionary<string, List<NodeObservedState>>> FetchClusterSnapshotsAsync(
        string[] documentIds,
        CancellationToken ct)
    {
        var tasks = _config.Nodes.Select(async node =>
        {
            var metadata = await FetchBatchMetadataAsync(
                    _storesByNodeUrl[node.Url],
                    node.Url,
                    documentIds,
                    ct)
                .ConfigureAwait(false);
            return (NodeUrl: node.Url, Metadata: metadata);
        });

        var results = await Task.WhenAll(tasks).ConfigureAwait(false);
        var metadataByNode = results.ToDictionary(
            result => result.NodeUrl,
            result => result.Metadata,
            StringComparer.OrdinalIgnoreCase);

        var snapshots = documentIds.ToDictionary(
            id => id,
            _ => new List<NodeObservedState>(_config.Nodes.Count),
            StringComparer.OrdinalIgnoreCase);

        foreach (var node in _config.Nodes)
        {
            var perNode = metadataByNode[node.Url];
            foreach (var documentId in documentIds)
                snapshots[documentId].Add(CloneObservedState(perNode[documentId]));
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
                _ => new NodeObservedState
                {
                    NodeUrl = nodeUrl,
                    Present = false
                },
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

    private async Task<IterationPage> FetchNodePageAsync(
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

        return new IterationPage(nodeUrl, startEtag, ParseIterationItems(responseBody));
    }

    private static List<IterationItem> ParseIterationItems(string json)
    {
        var items = new List<IterationItem>();

        using var document = JsonDocument.Parse(json);
        if (!document.RootElement.TryGetProperty("Results", out var results))
            return items;

        foreach (var element in results.EnumerateArray())
        {
            var id = element.TryGetProperty("Id", out var idProperty) ? idProperty.GetString() : null;
            var changeVector = element.TryGetProperty("ChangeVector", out var cvProperty) ? cvProperty.GetString() : null;
            var type = element.TryGetProperty("Type", out var typeProperty) ? typeProperty.GetString() : null;
            var flags = element.TryGetProperty("Flags", out var flagsProperty) ? flagsProperty.GetString() : null;
            var lastModified = element.TryGetProperty("LastModified", out var lmProperty) ? lmProperty.GetString() : null;

            if (string.IsNullOrWhiteSpace(id) ||
                string.IsNullOrWhiteSpace(changeVector) ||
                string.IsNullOrWhiteSpace(type) ||
                !element.TryGetProperty("Etag", out var etagProperty) ||
                !TryReadInt64(etagProperty, out var etag))
            {
                continue;
            }

            items.Add(new IterationItem(id, changeVector, lastModified, etag, type, flags));
        }

        return items;
    }

    private IReadOnlyList<NodeCursorState> GetOrderedCursors(RunStateDocument run)
    {
        var byUrl = run.NodeCursors.ToDictionary(cursor => cursor.NodeUrl, StringComparer.OrdinalIgnoreCase);
        return _config.Nodes
            .Select(node => byUrl.TryGetValue(node.Url, out var cursor)
                ? cursor
                : throw new InvalidOperationException($"Run state is missing cursor for node '{node.Url}'."))
            .ToList();
    }

    private static long ComputeSafeRestartEtag(RunStateDocument run)
        => run.NodeCursors.Count == 0 ? 0 : run.NodeCursors.Min(cursor => cursor.NextEtag);

    private void PublishProgress(RunStateDocument run, string? nodeUrl)
    {
        ProgressUpdated?.Invoke(new RunProgressUpdate(
            CurrentNodeLabel: nodeUrl != null && _labelsByNodeUrl.TryGetValue(nodeUrl, out var label)
                ? label
                : null,
            DocumentsInspected: run.DocumentsInspected,
            UniqueVersionsCompared: run.UniqueVersionsCompared,
            MismatchesFound: run.MismatchesFound,
            RepairsPlanned: run.RepairsPlanned,
            RepairsAttempted: run.RepairsAttempted,
            RepairsPatchedOnWinner: run.RepairsPatchedOnWinner,
            RepairsFailed: run.RepairsFailed,
            SafeRestartEtag: run.SafeRestartEtag));
    }

    private static NodeObservedState CloneObservedState(NodeObservedState state)
    {
        return new NodeObservedState
        {
            NodeUrl = state.NodeUrl,
            Present = state.Present,
            Collection = state.Collection,
            ChangeVector = state.ChangeVector,
            LastModified = state.LastModified
        };
    }

    private static IReadOnlyCollection<DiagnosticDocument> CreateDiagnostics(DiagnosticDocument diagnostic)
        => [diagnostic];

    private static DiagnosticDocument CreatePageDiagnostic(
        RunStateDocument run,
        IterationPage page,
        int documentsInspected,
        int versionsSkippedByDedup,
        int newVersionsQueued,
        int uniqueIdsInLookupBatch,
        int mismatchesInBatch,
        int repairsTriggered,
        int pageFilteredOut,
        string? message)
    {
        return new DiagnosticDocument
        {
            Id = StateStore.CreateDiagnosticId(run.RunId),
            RunId = run.RunId,
            Kind = "NodePage",
            NodeUrl = page.NodeUrl,
            RequestedStartEtag = page.RequestedStartEtag,
            FirstItemEtag = page.Items.Count > 0 ? page.Items[0].Etag : null,
            LastItemEtag = page.Items.Count > 0 ? page.Items[^1].Etag : null,
            PageCount = page.Items.Count,
            VersionsSkippedByDedup = versionsSkippedByDedup,
            NewVersionsQueued = newVersionsQueued,
            UniqueIdsInLookupBatch = uniqueIdsInLookupBatch,
            MismatchesInBatch = mismatchesInBatch,
            RepairsTriggered = repairsTriggered,
            Message = $"DocumentsInspected={documentsInspected}, PageFilteredOut={pageFilteredOut}. {message}",
            Timestamp = DateTimeOffset.UtcNow
        };
    }

    private static bool TryReadInt64(JsonElement value, out long result)
    {
        switch (value.ValueKind)
        {
            case JsonValueKind.Number:
                return value.TryGetInt64(out result);
            case JsonValueKind.String:
                return long.TryParse(value.GetString(), out result);
            default:
                result = 0;
                return false;
        }
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

    private sealed record IterationPage(
        string NodeUrl,
        long RequestedStartEtag,
        List<IterationItem> Items);

    private sealed record IterationItem(
        string Id,
        string ChangeVector,
        string? LastModified,
        long Etag,
        string Type,
        string? Flags);

    private sealed record PageWorkItem(string DocumentId, VersionKey VersionKey);

    private sealed record RepairCandidate(
        string DocumentId,
        string Collection,
        string WinnerNode,
        string? WinnerCV,
        List<string> AffectedNodes);

    private sealed record RepairGroupKey(
        string WinnerNode,
        string Collection,
        string? WinnerCV);

    private sealed record PageOutcome(
        long DocumentsInspected,
        long UniqueVersionsCompared,
        long MismatchesFound,
        long RepairsPlanned,
        long RepairsAttempted,
        long RepairsPatchedOnWinner,
        long RepairsFailed,
        List<VersionKey> ProcessedVersionKeys,
        List<MismatchDocument> Mismatches,
        List<RepairDocument> Repairs,
        List<RepairActionGuardDocument> RepairGuards,
        List<DiagnosticDocument> Diagnostics);

    private sealed record LookupOutcome(
        List<MismatchDocument> Mismatches,
        List<RepairDocument> Repairs,
        List<RepairActionGuardDocument> RepairGuards,
        List<DiagnosticDocument> Diagnostics,
        long MismatchesFound,
        long RepairsPlanned,
        long RepairsAttempted,
        long RepairsPatchedOnWinner,
        long RepairsFailed);

    private sealed record RepairGroupOutcome(
        RepairDocument RepairDocument,
        List<RepairActionGuardDocument> RepairGuards,
        long RepairsPlanned,
        long RepairsAttempted,
        long RepairsPatchedOnWinner,
        long RepairsFailed);
}

public sealed record RunProgressUpdate(
    string? CurrentNodeLabel,
    long DocumentsInspected,
    long UniqueVersionsCompared,
    long MismatchesFound,
    long RepairsPlanned,
    long RepairsAttempted,
    long RepairsPatchedOnWinner,
    long RepairsFailed,
    long? SafeRestartEtag);
