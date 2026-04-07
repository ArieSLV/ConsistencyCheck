using System.Security.Cryptography.X509Certificates;
using Polly;
using Polly.Retry;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Queries;
using Raven.Client.Exceptions;
using Spectre.Console;
using System.Reflection;
using Sparrow.Json;

namespace ConsistencyCheck;

internal sealed class RepairPlanExecutor : IAsyncDisposable
{
    private static readonly PropertyInfo OperationIdProperty = typeof(Operation)
        .GetProperty("Id", BindingFlags.Instance | BindingFlags.NonPublic)
        ?? throw new InvalidOperationException("Raven.Client Operation.Id property was not found.");

    private readonly AppConfig _config;
    private readonly StateStore _stateStore;
    private readonly X509Certificate2? _certificate;
    private readonly ResiliencePipeline _retry;
    private readonly Dictionary<string, IDocumentStore> _storesByNodeUrl;
    private readonly Dictionary<string, string> _labelsByNodeUrl;

    public RepairPlanExecutor(AppConfig config, StateStore stateStore)
    {
        _config = config;
        _stateStore = stateStore;
        _certificate = ConfigWizard.LoadCertificate(config);
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

    internal event Action<IndexedRunProgressUpdate>? ProgressUpdated;

    public async Task<RunStateDocument> RunAsync(RunStateDocument run, CancellationToken ct)
    {
        if (string.IsNullOrWhiteSpace(run.SourceRepairPlanRunId))
            throw new InvalidOperationException("Repair-plan execution run is missing SourceRepairPlanRunId.");

        var sourcePlanRunId = run.SourceRepairPlanRunId;
        PublishProgress("loading repair plan", null, $"loading saved plan {sourcePlanRunId}", run);

        var sourcePlanRun = await _stateStore.LoadRunAsync(sourcePlanRunId, ct).ConfigureAwait(false)
                            ?? throw new InvalidOperationException(
                                $"Repair-plan source run '{sourcePlanRunId}' could not be loaded.");
        var sourceSemanticsSnapshot = sourcePlanRun.ChangeVectorSemanticsSnapshot == null
            ? null
            : ChangeVectorSemantics.CloneSnapshot(sourcePlanRun.ChangeVectorSemanticsSnapshot);

        var groupedPlans = await _stateStore.LoadExecutableRepairPlanDocumentsAsync(sourcePlanRunId, ct).ConfigureAwait(false);
        var plans = RepairPlanExecutionPlanner.ExpandToSingleDocumentPlans(groupedPlans).ToList();
        var sourceMismatches = await _stateStore
            .LoadMismatchesByRunAndDocumentIdsAsync(
                sourcePlanRunId,
                plans.SelectMany(plan => plan.DocumentIds).Distinct(StringComparer.OrdinalIgnoreCase).ToArray(),
                ct)
            .ConfigureAwait(false);
        var sourceMismatchesById = sourceMismatches.ToDictionary(
            mismatch => mismatch.DocumentId,
            mismatch => mismatch,
            StringComparer.OrdinalIgnoreCase);
        var existingExecutions = await _stateStore
            .LoadApplyExecutionDocumentsAsync(run.RunId, sourcePlanRunId, ct)
            .ConfigureAwait(false);
        if (existingExecutions.Any(execution => execution.DocumentIds.Count != 1))
        {
            throw new InvalidOperationException(
                $"Apply run '{run.RunId}' contains legacy grouped execution checkpoints. Start a fresh apply run to use interactive per-document validation safely.");
        }

        var resumeState = ApplyRepairPlanExecution.Rebuild(existingExecutions);
        var automaticMode = _config.ApplyExecutionMode == ApplyExecutionMode.Automatic;

        run.RepairsPlanned = plans.Count;
        run.RepairsAttempted = resumeState.RepairsAttempted;
        run.RepairsPatchedOnWinner = resumeState.RepairsPatchedOnWinner;
        run.RepairsFailed = resumeState.RepairsFailed;

        await _stateStore.PersistBatchAsync(
                run,
                [],
                [],
                [],
                [
                    CreateDiagnostic(
                        run.RunId,
                        "RepairPlanLoaded",
                        $"Loaded {groupedPlans.Count:N0} saved repair groups and expanded them into {plans.Count:N0} per-document repair items from source run '{sourcePlanRunId}'."),
                    CreateDiagnostic(
                        run.RunId,
                        "RepairPlanResumeSelected",
                        existingExecutions.Count == 0
                            ? $"Starting fresh apply execution for source run '{sourcePlanRunId}'."
                            : $"Resuming apply run '{run.RunId}' for source run '{sourcePlanRunId}' from {existingExecutions.Count:N0} persisted execution checkpoint(s)."),
                    CreateDiagnostic(
                        run.RunId,
                        "RepairPlanResumeStateRebuilt",
                        $"CompletedItems={resumeState.CompletedItemCount:N0}, RepairsPlanned={run.RepairsPlanned:N0}, RepairsAttempted={run.RepairsAttempted:N0}, RepairsPatchedOnWinner={run.RepairsPatchedOnWinner:N0}, RepairsFailed={run.RepairsFailed:N0}, ApplyExecutionMode={_config.ApplyExecutionMode}.")
                ],
                ct)
            .ConfigureAwait(false);

        if (resumeState.BlockedExecutions.Count > 0)
        {
            var plansById = plans.ToDictionary(plan => plan.Id, StringComparer.OrdinalIgnoreCase);

            foreach (var blocked in resumeState.BlockedExecutions)
            {
                if (string.IsNullOrWhiteSpace(blocked.SourceRepairPlanId) ||
                    !plansById.TryGetValue(blocked.SourceRepairPlanId, out var blockedPlan))
                {
                    var blockedMessage =
                        $"Cannot resume apply run '{run.RunId}' safely. Source plan item '{blocked.SourceRepairPlanId}' is stuck in '{blocked.RepairStatus}' without a persisted PatchOperationId. DocumentIds={string.Join(", ", blocked.DocumentIds)}.";

                    await _stateStore.StoreDiagnosticsAsync(
                            [
                                CreateDiagnostic(
                                    run.RunId,
                                    "RepairPlanResumeBlocked",
                                    blockedMessage,
                                    blocked.WinnerNode)
                            ],
                            ct)
                        .ConfigureAwait(false);

                    throw new InvalidOperationException(
                        $"{blockedMessage} The process was interrupted in the non-atomic window between local checkpoint and remote operation confirmation.");
                }

                PublishProgress(
                    "reconciling blocked item",
                    ResolveWinnerLabel(blockedPlan.WinnerNode),
                    $"{blockedPlan.DocumentIds.Count:N0} docs",
                    run);

                var recoveredOutcome = await RecoverBlockedDispatchAsync(run, blockedPlan, blocked, ct).ConfigureAwait(false);
                ApplyOutcome(run, recoveredOutcome);
                await PersistFinalOutcomeAsync(run, blockedPlan, recoveredOutcome, CancellationToken.None).ConfigureAwait(false);
            }

            existingExecutions = await _stateStore
                .LoadApplyExecutionDocumentsAsync(run.RunId, sourcePlanRunId, ct)
                .ConfigureAwait(false);
            resumeState = ApplyRepairPlanExecution.Rebuild(existingExecutions);
        }

        var processedItems = (long)resumeState.CompletedItemCount;

        PublishProgress(
            existingExecutions.Count == 0 ? "applying repair plan" : "resuming apply run",
            null,
            $"already completed {resumeState.CompletedItemCount:N0} of {plans.Count:N0} plan items",
            run,
            processedItems);

        if (!automaticMode)
            ReorderForPriorityDocuments(plans, resumeState);

        var operatorStopped = false;
        foreach (var plan in plans)
        {
            var documentId = plan.DocumentIds.Single();
            if (!sourceMismatchesById.TryGetValue(documentId, out var sourceMismatch))
            {
                throw new InvalidOperationException(
                    $"Saved repair-plan item '{plan.Id}' references '{documentId}', but the source mismatch document for run '{sourcePlanRunId}' could not be loaded.");
            }

            if (resumeState.ExecutionsBySourceRepairPlanId.TryGetValue(plan.Id, out var existingExecution))
            {
                if (ApplyRepairPlanExecution.IsTerminalStatus(existingExecution.RepairStatus))
                    continue;

                if (ApplyRepairPlanExecution.IsStartedState(existingExecution))
                {
                    PublishProgress(
                        "resuming apply run",
                        ResolveWinnerLabel(plan.WinnerNode),
                        "waiting for in-flight remote patch",
                        run,
                        processedItems);

                    var resumedOutcome = await ResumeStartedRepairPlanAsync(
                            run,
                            plan,
                            existingExecution,
                            resumedFromExistingCheckpoint: true,
                            CancellationToken.None)
                        .ConfigureAwait(false);

                    ApplyOutcome(run, resumedOutcome);
                    await PersistFinalOutcomeAsync(run, plan, resumedOutcome, CancellationToken.None).ConfigureAwait(false);
                    processedItems++;
                    PublishProgress("applying repair plan", ResolveWinnerLabel(plan.WinnerNode), null, run, processedItems);
                    continue;
                }

                throw new InvalidOperationException(
                    $"Apply run '{run.RunId}' contains an unsupported non-terminal execution state '{existingExecution.RepairStatus}' for source plan '{plan.Id}'.");
            }

            ct.ThrowIfCancellationRequested();

            var executionResult = await ExecuteValidatedRepairPlanAsync(
                    run,
                    plan,
                    sourceMismatch,
                    sourcePlanRun,
                    sourceSemanticsSnapshot,
                    automaticMode,
                    ct)
                .ConfigureAwait(false);

            if (executionResult.Outcome != null)
            {
                ApplyOutcome(run, executionResult.Outcome);
                await PersistFinalOutcomeAsync(run, plan, executionResult.Outcome, CancellationToken.None).ConfigureAwait(false);
                processedItems++;
                PublishProgress("applying repair plan", ResolveWinnerLabel(plan.WinnerNode), null, run, processedItems);
            }

            if (executionResult.EnableAutomaticMode)
                automaticMode = true;

            if (executionResult.StopRequested)
            {
                operatorStopped = true;
                break;
            }
        }

        if (!operatorStopped)
        {
            run.IsComplete = true;
            run.CompletedAt = DateTimeOffset.UtcNow;
        }

        await _stateStore.PersistBatchAsync(
                run,
                [],
                [],
                [],
                [
                    CreateDiagnostic(
                        run.RunId,
                        "RepairPlanExecutionSummary",
                        $"SourceRepairPlanRunId={sourcePlanRunId}, RepairsPlanned={run.RepairsPlanned}, RepairsAttempted={run.RepairsAttempted}, RepairsPatchedOnWinner={run.RepairsPatchedOnWinner}, RepairsFailed={run.RepairsFailed}, StoppedByOperator={operatorStopped}.")
                ],
                CancellationToken.None)
            .ConfigureAwait(false);

        PublishProgress(
            operatorStopped ? "stopped" : "complete",
            null,
            operatorStopped ? "apply run stopped by operator" : "saved repair plan applied",
            run,
            processedItems);
        return run;
    }

    public async ValueTask DisposeAsync()
    {
        foreach (var store in _storesByNodeUrl.Values)
            store.Dispose();

        // Do not dispose _certificate here. The certificate object is held by the global
        // DefaultRavenHttpClientFactory HttpClient cache (via HttpClientHandler.ClientCertificates).
        // Disposing it here would invalidate the cached HttpClient's CNG key handle, causing
        // "m_safeCertContext is an invalid handle" on the next SSL handshake in subsequent code.
        await Task.CompletedTask.ConfigureAwait(false);
    }

    private async Task<PerDocumentExecutionResult> ExecuteValidatedRepairPlanAsync(
        RunStateDocument run,
        RepairDocument sourcePlan,
        MismatchDocument sourceMismatch,
        RunStateDocument sourcePlanRun,
        ChangeVectorSemanticsSnapshot? sourceSemanticsSnapshot,
        bool automaticMode,
        CancellationToken ct)
    {
        var documentId = sourcePlan.DocumentIds.Single();
        var liveSnapshots = await FetchClusterSnapshotsAsync([documentId], ct).ConfigureAwait(false);
        var liveObservedState = liveSnapshots[documentId];
        var liveEvaluation = ConsistencyDecisionEngine.EvaluateDocument(
            documentId,
            liveObservedState,
            sourceSemanticsSnapshot);
        var snapshotStateChanged = RepairPlanExecutionPlanner.ObservedStatesMatch(
            sourceMismatch.ObservedState,
            liveObservedState,
            _config.Nodes,
            sourceSemanticsSnapshot) == false;

        var sameSavedWinner = LiveEvaluationMatchesSavedPlan(sourcePlan, liveEvaluation, sourceSemanticsSnapshot);
        var stateChangedDocument = snapshotStateChanged
            ? CreateRepairStateChangedDocument(
                run,
                sourcePlanRun,
                sourceMismatch,
                liveObservedState,
                liveEvaluation)
            : null;
        var preview = new RepairPreview(
            SourcePlan: sourcePlan,
            SourceMismatch: sourceMismatch,
            LiveObservedState: liveObservedState,
            LiveEvaluation: liveEvaluation,
            SnapshotStateChanged: snapshotStateChanged,
            PatchEligible: liveEvaluation != null && snapshotStateChanged == false && sameSavedWinner);

        if (!preview.PatchEligible)
        {
            var suppressedOutcome = CreateSkippedOutcome(
                run,
                sourcePlan,
                liveEvaluation == null ? "SkippedAlreadyConsistent" : "SkippedStateChanged",
                liveEvaluation == null
                    ? "Saved repair plan suppressed because the live cluster state is already consistent."
                    : "Saved repair plan suppressed because the live cluster state changed since the imported snapshot.",
                liveEvaluation == null
                    ? "RepairPlanSkippedAlreadyConsistent"
                    : "RepairPlanSkippedStateChanged",
                stateChangedDocument is null ? [] : [stateChangedDocument]);

            if (!automaticMode)
            {
                RenderRepairPreview(preview);
                var followUp = await AskForNonPatchContinuationAsync(ct).ConfigureAwait(false);
                return new PerDocumentExecutionResult(
                    Outcome: suppressedOutcome,
                    EnableAutomaticMode: followUp.EnableAutomaticMode,
                    StopRequested: followUp.StopRequested);
            }

            PublishProgress(
                liveEvaluation == null ? "skipping consistent document" : "skipping changed document",
                ResolveWinnerLabel(sourcePlan.WinnerNode),
                documentId,
                run);
            return new PerDocumentExecutionResult(
                Outcome: suppressedOutcome,
                EnableAutomaticMode: false,
                StopRequested: false);
        }

        if (!automaticMode)
        {
            RenderRepairPreview(preview);
            var decision = await AskForPatchDecisionAsync(ct).ConfigureAwait(false);
            if (decision.StopRequested)
            {
                return new PerDocumentExecutionResult(
                    Outcome: null,
                    EnableAutomaticMode: false,
                    StopRequested: true);
            }

            // Second live check: re-fetch all nodes right before dispatching the patch.
            // The user may have spent several minutes reviewing the preview; the cluster state
            // could have changed in the meantime.
            var preDispatchSnapshots = await FetchClusterSnapshotsAsync([documentId], ct).ConfigureAwait(false);
            var preDispatchObservedState = preDispatchSnapshots[documentId];
            var preDispatchEvaluation = ConsistencyDecisionEngine.EvaluateDocument(
                documentId, preDispatchObservedState, sourceSemanticsSnapshot);
            var preDispatchStateChanged = RepairPlanExecutionPlanner.ObservedStatesMatch(
                sourceMismatch.ObservedState, preDispatchObservedState, _config.Nodes, sourceSemanticsSnapshot) == false;
            var preDispatchSameSavedWinner = LiveEvaluationMatchesSavedPlan(
                sourcePlan, preDispatchEvaluation, sourceSemanticsSnapshot);

            if (preDispatchEvaluation == null || preDispatchStateChanged || !preDispatchSameSavedWinner)
            {
                var preDispatchStChangedDoc = preDispatchStateChanged
                    ? CreateRepairStateChangedDocument(
                        run, sourcePlanRun, sourceMismatch, preDispatchObservedState, preDispatchEvaluation)
                    : null;

                AnsiConsole.MarkupLine(preDispatchEvaluation == null
                    ? "[yellow]Pre-dispatch check: cluster is now consistent — patch suppressed.[/]"
                    : "[yellow]Pre-dispatch check: cluster state changed between preview and approval — patch suppressed.[/]");

                var suppressedOutcome = CreateSkippedOutcome(
                    run,
                    sourcePlan,
                    preDispatchEvaluation == null ? "SkippedAlreadyConsistent" : "SkippedStateChanged",
                    preDispatchEvaluation == null
                        ? "Pre-dispatch check: cluster is already consistent."
                        : "Pre-dispatch check: cluster state changed between preview and approval.",
                    preDispatchEvaluation == null
                        ? "RepairPlanSkippedAlreadyConsistent"
                        : "RepairPlanSkippedStateChanged",
                    preDispatchStChangedDoc is null ? [] : [preDispatchStChangedDoc]);

                return new PerDocumentExecutionResult(
                    Outcome: suppressedOutcome,
                    EnableAutomaticMode: decision.EnableAutomaticMode,
                    StopRequested: false);
            }

            var newOutcome = await ExecuteNewRepairPlanAsync(run, sourcePlan, ct).ConfigureAwait(false);
            return new PerDocumentExecutionResult(
                Outcome: newOutcome,
                EnableAutomaticMode: decision.EnableAutomaticMode,
                StopRequested: false);
        }

        PublishProgress(
            "applying repair plan",
            ResolveWinnerLabel(sourcePlan.WinnerNode),
            documentId,
            run);

        return new PerDocumentExecutionResult(
            Outcome: await ExecuteNewRepairPlanAsync(run, sourcePlan, ct).ConfigureAwait(false),
            EnableAutomaticMode: false,
            StopRequested: false);
    }

    private async Task<RepairExecutionOutcome> ExecuteNewRepairPlanAsync(
        RunStateDocument run,
        RepairDocument sourcePlan,
        CancellationToken ct)
    {
        var executionDocument = CreateExecutionDocument(
            run,
            sourcePlan,
            StateStore.CreateApplyRepairExecutionId(run.RunId, sourcePlan.Id));
        executionDocument.RepairStatus = ApplyRepairPlanExecution.PatchDispatching;
        executionDocument.CompletedAt = DateTimeOffset.UtcNow;

        await _stateStore.PersistBatchAsync(
                run,
                [],
                [executionDocument],
                [],
                [],
                ct)
            .ConfigureAwait(false);

        Operation operation;
        try
        {
            operation = await _storesByNodeUrl[sourcePlan.WinnerNode]
                .Operations
                .SendAsync(
                    new PatchByQueryOperation(
                        new IndexQuery
                        {
                            Query = RepairPatchQueryBuilder.BuildTouchWinnerPatchQuery(sourcePlan.Collection ?? string.Empty),
                            QueryParameters = new Parameters
                            {
                                ["ids"] = sourcePlan.DocumentIds.ToArray()
                            }
                        },
                        new QueryOperationOptions { RetrieveDetails = true }),
                    token: CancellationToken.None)
                .ConfigureAwait(false);

            executionDocument.PatchOperationId = GetOperationId(operation);
            executionDocument.RepairStatus = ApplyRepairPlanExecution.PatchStarted;
            executionDocument.Error = null;
            executionDocument.CompletedAt = DateTimeOffset.UtcNow;

            await _stateStore.PersistBatchAsync(
                    run,
                    [],
                    [executionDocument],
                    [],
                    [
                        CreateDiagnostic(
                            run.RunId,
                            "RepairPlanItemDispatchCheckpointSaved",
                            $"Saved dispatch checkpoint for source plan '{sourcePlan.Id}' with operation id {executionDocument.PatchOperationId}.",
                            sourcePlan.WinnerNode)
                    ],
                    CancellationToken.None)
                .ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            await _stateStore.StoreDiagnosticsAsync(
                    [
                        CreateDiagnostic(
                            run.RunId,
                            "RepairPlanResumeBlocked",
                            $"Saved plan '{sourcePlan.Id}' entered '{ApplyRepairPlanExecution.PatchDispatching}' but the remote operation confirmation was not durably recorded: {ex.Message}",
                            sourcePlan.WinnerNode)
                    ],
                    CancellationToken.None)
                .ConfigureAwait(false);

            throw new InvalidOperationException(
                $"Repair plan item '{sourcePlan.Id}' was interrupted before its remote operation id could be durably recorded. The next start will block automatic resume for this item until it is investigated.",
                ex);
        }

        return await ResumeStartedRepairPlanAsync(
                run,
                sourcePlan,
                executionDocument,
                resumedFromExistingCheckpoint: false,
                CancellationToken.None)
            .ConfigureAwait(false);
    }

    private async Task<RepairExecutionOutcome> RecoverBlockedDispatchAsync(
        RunStateDocument run,
        RepairDocument sourcePlan,
        RepairDocument executionDocument,
        CancellationToken ct)
    {
        var semanticsSnapshot = run.ChangeVectorSemanticsSnapshot;
        var expectedWinnerCv = ConsistencyDecisionEngine.NormalizeChangeVector(
            sourcePlan.WinnerCV,
            semanticsSnapshot?.ExplicitUnusedDatabaseIdSet);

        var snapshots = await FetchClusterSnapshotsAsync(sourcePlan.DocumentIds.ToArray(), ct).ConfigureAwait(false);
        var precomputedDocumentDecisions = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        var pendingDocumentIds = new List<string>();

        foreach (var documentId in sourcePlan.DocumentIds)
        {
            if (!snapshots.TryGetValue(documentId, out var snapshot))
            {
                precomputedDocumentDecisions[documentId] = "PatchFailed";
                continue;
            }

            var evaluation = ConsistencyDecisionEngine.EvaluateDocument(documentId, snapshot, semanticsSnapshot);
            if (evaluation == null)
            {
                var currentNormalizedCv = snapshot
                    .Where(state => state.Present)
                    .Select(state => ConsistencyDecisionEngine.NormalizeChangeVector(
                        state.ChangeVector,
                        semanticsSnapshot?.ExplicitUnusedDatabaseIdSet))
                    .Distinct(StringComparer.OrdinalIgnoreCase)
                    .SingleOrDefault() ?? string.Empty;

                precomputedDocumentDecisions[documentId] = string.Equals(
                    currentNormalizedCv,
                    expectedWinnerCv,
                    StringComparison.OrdinalIgnoreCase)
                    ? "PatchCompletedOnWinner"
                    : "SkippedStateChanged";
                continue;
            }

            var currentWinnerCv = ConsistencyDecisionEngine.NormalizeChangeVector(
                evaluation.WinnerCV,
                semanticsSnapshot?.ExplicitUnusedDatabaseIdSet);

            var sameWinner =
                string.Equals(evaluation.WinnerNode, sourcePlan.WinnerNode, StringComparison.OrdinalIgnoreCase) &&
                string.Equals(currentWinnerCv, expectedWinnerCv, StringComparison.OrdinalIgnoreCase) &&
                string.Equals(evaluation.Collection, sourcePlan.Collection, StringComparison.OrdinalIgnoreCase) &&
                string.Equals(evaluation.MismatchType, "AMBIGUOUS_CV", StringComparison.Ordinal) == false &&
                string.Equals(evaluation.MismatchType, "COLLECTION_MISMATCH", StringComparison.Ordinal) == false;

            if (sameWinner)
            {
                pendingDocumentIds.Add(documentId);
                continue;
            }

            precomputedDocumentDecisions[documentId] = "SkippedStateChanged";
        }

        if (pendingDocumentIds.Count == 0)
        {
            return FinalizeRecoveredStateWithoutRedispatch(
                run,
                sourcePlan,
                executionDocument,
                precomputedDocumentDecisions,
                [
                    CreateDiagnostic(
                        run.RunId,
                        "RepairPlanBlockedItemRecovered",
                        $"Recovered blocked saved plan '{sourcePlan.Id}' without redispatch. Current cluster state no longer requires replay for any of its {sourcePlan.DocumentIds.Count:N0} document(s).",
                        sourcePlan.WinnerNode)
                ]);
        }

        return await RedispatchRecoveredBlockedPlanAsync(
                run,
                sourcePlan,
                executionDocument,
                pendingDocumentIds,
                precomputedDocumentDecisions,
                CancellationToken.None)
            .ConfigureAwait(false);
    }

    private async Task<RepairExecutionOutcome> ResumeStartedRepairPlanAsync(
        RunStateDocument run,
        RepairDocument sourcePlan,
        RepairDocument executionDocument,
        bool resumedFromExistingCheckpoint,
        CancellationToken ct)
    {
        if (executionDocument.PatchOperationId == null)
        {
            throw new InvalidOperationException(
                $"Apply repair execution '{executionDocument.Id}' is marked as '{executionDocument.RepairStatus}' but has no PatchOperationId.");
        }

        var diagnostics = new List<DiagnosticDocument>();
        if (resumedFromExistingCheckpoint)
        {
            diagnostics.Add(CreateDiagnostic(
                run.RunId,
                "RepairPlanItemResumedFromOperation",
                $"Resuming saved plan '{sourcePlan.Id}' from persisted operation id {executionDocument.PatchOperationId}.",
                sourcePlan.WinnerNode));
        }

        var operationState = await WaitForTerminalOperationStateAsync(
                sourcePlan.WinnerNode,
                executionDocument.PatchOperationId.Value,
                ct)
            .ConfigureAwait(false);

        return FinalizeOperationState(
            run,
            sourcePlan,
            executionDocument,
            operationState,
            diagnostics);
    }

    private async Task<RepairExecutionOutcome> RedispatchRecoveredBlockedPlanAsync(
        RunStateDocument run,
        RepairDocument sourcePlan,
        RepairDocument executionDocument,
        IReadOnlyList<string> pendingDocumentIds,
        IReadOnlyDictionary<string, string> precomputedDocumentDecisions,
        CancellationToken ct)
    {
        executionDocument.RepairStatus = ApplyRepairPlanExecution.PatchDispatching;
        executionDocument.PatchOperationId = null;
        executionDocument.Error = null;
        executionDocument.CompletedAt = DateTimeOffset.UtcNow;

        await _stateStore.PersistBatchAsync(
                run,
                [],
                [executionDocument],
                [],
                [
                    CreateDiagnostic(
                        run.RunId,
                        "RepairPlanBlockedItemRecovered",
                        $"Blocked saved plan '{sourcePlan.Id}' still needs replay for {pendingDocumentIds.Count:N0} of {sourcePlan.DocumentIds.Count:N0} document(s); redispatching the pending subset.",
                        sourcePlan.WinnerNode)
                ],
                ct)
            .ConfigureAwait(false);

        Operation operation;
        try
        {
            operation = await _storesByNodeUrl[sourcePlan.WinnerNode]
                .Operations
                .SendAsync(
                    new PatchByQueryOperation(
                        new IndexQuery
                        {
                            Query = RepairPatchQueryBuilder.BuildTouchWinnerPatchQuery(sourcePlan.Collection ?? string.Empty),
                            QueryParameters = new Parameters
                            {
                                ["ids"] = pendingDocumentIds.ToArray()
                            }
                        },
                        new QueryOperationOptions { RetrieveDetails = true }),
                    token: CancellationToken.None)
                .ConfigureAwait(false);

            executionDocument.PatchOperationId = GetOperationId(operation);
            executionDocument.RepairStatus = ApplyRepairPlanExecution.PatchStarted;
            executionDocument.Error = null;
            executionDocument.CompletedAt = DateTimeOffset.UtcNow;

            await _stateStore.PersistBatchAsync(
                    run,
                    [],
                    [executionDocument],
                    [],
                    [
                        CreateDiagnostic(
                            run.RunId,
                            "RepairPlanItemDispatchCheckpointSaved",
                            $"Saved dispatch checkpoint for recovered blocked plan '{sourcePlan.Id}' with operation id {executionDocument.PatchOperationId}.",
                            sourcePlan.WinnerNode)
                    ],
                    CancellationToken.None)
                .ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            await _stateStore.StoreDiagnosticsAsync(
                    [
                        CreateDiagnostic(
                            run.RunId,
                            "RepairPlanResumeBlocked",
                            $"Recovered blocked plan '{sourcePlan.Id}' could not persist a new remote operation id after redispatch: {ex.Message}",
                            sourcePlan.WinnerNode)
                    ],
                    CancellationToken.None)
                .ConfigureAwait(false);

            throw new InvalidOperationException(
                $"Blocked repair plan item '{sourcePlan.Id}' required redispatch, but the new remote operation id could not be durably recorded. Automatic resume is blocked until this item is investigated.",
                ex);
        }

        var operationState = await WaitForTerminalOperationStateAsync(
                sourcePlan.WinnerNode,
                executionDocument.PatchOperationId.Value,
                CancellationToken.None)
            .ConfigureAwait(false);

        return FinalizeRecoveredOperationState(
            run,
            sourcePlan,
            executionDocument,
            operationState,
            pendingDocumentIds,
            precomputedDocumentDecisions,
            [
                CreateDiagnostic(
                    run.RunId,
                    "RepairPlanItemResumedFromOperation",
                    $"Recovered blocked saved plan '{sourcePlan.Id}' by redispatching {pendingDocumentIds.Count:N0} pending document(s) with operation id {executionDocument.PatchOperationId}.",
                    sourcePlan.WinnerNode)
            ]);
    }

    private RepairExecutionOutcome FinalizeOperationState(
        RunStateDocument run,
        RepairDocument sourcePlan,
        RepairDocument executionDocument,
        OperationState operationState,
        IReadOnlyCollection<DiagnosticDocument> startupDiagnostics)
    {
        var diagnostics = startupDiagnostics.ToList();
        var attempted = sourcePlan.DocumentIds.Count;
        long patched = 0;
        long failed = 0;
        var documentDecisions = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

        executionDocument.CompletedAt = DateTimeOffset.UtcNow;
        executionDocument.DocumentsAttempted = attempted;

        if (operationState.Status == OperationStatus.Completed)
        {
            if (operationState.Result is not BulkOperationResult result)
            {
                executionDocument.RepairStatus = "PatchFailed";
                executionDocument.Error =
                    $"Operation {executionDocument.PatchOperationId} completed without a BulkOperationResult payload.";
                executionDocument.DocumentsPatchedOnWinner = 0;
                executionDocument.DocumentsFailed = attempted;
                foreach (var documentId in sourcePlan.DocumentIds)
                    documentDecisions[documentId] = "PatchFailed";

                diagnostics.Add(CreateDiagnostic(
                    run.RunId,
                    "RepairPlanExecutionFailed",
                    $"Execution of saved plan '{sourcePlan.Id}' completed without a BulkOperationResult payload.",
                    sourcePlan.WinnerNode));
            }
            else
            {
                var statusById = BuildPatchStatusById(result, sourcePlan.DocumentIds);
                foreach (var documentId in sourcePlan.DocumentIds)
                {
                    string decision;
                    if (!statusById.TryGetValue(documentId, out var status))
                    {
                        if (result.Total > 0)
                        {
                            decision = "PatchCompletedOnWinner";
                            patched++;
                        }
                        else
                        {
                            decision = "PatchFailed";
                            failed++;
                        }

                        documentDecisions[documentId] = decision;

                        continue;
                    }

                    decision = MapPatchStatusToDecision(status);
                    documentDecisions[documentId] = decision;
                    if (string.Equals(decision, "PatchCompletedOnWinner", StringComparison.Ordinal))
                        patched++;
                    else
                        failed++;
                }

                executionDocument.RepairStatus = patched > 0
                    ? "PatchCompletedOnWinner"
                    : "SkippedStateChanged";
                executionDocument.Error = failed > 0
                    ? $"Non-patched documents in group: {failed} of {attempted}."
                    : null;
                executionDocument.DocumentsPatchedOnWinner = patched;
                executionDocument.DocumentsFailed = failed;

                diagnostics.Add(CreateDiagnostic(
                    run.RunId,
                    "RepairPlanExecution",
                    $"Executed saved plan '{sourcePlan.Id}' on winner '{sourcePlan.WinnerNode}'. Patched={patched}, Failed={failed}.",
                    sourcePlan.WinnerNode));
            }
        }
        else
        {
            executionDocument.RepairStatus = "PatchFailed";
            executionDocument.Error = operationState.Result?.ToString()
                                     ?? $"Operation {executionDocument.PatchOperationId} ended with status {operationState.Status}.";
            executionDocument.DocumentsPatchedOnWinner = 0;
            executionDocument.DocumentsFailed = attempted;
            failed = attempted;
            foreach (var documentId in sourcePlan.DocumentIds)
                documentDecisions[documentId] = "PatchFailed";

            diagnostics.Add(CreateDiagnostic(
                run.RunId,
                "RepairPlanExecutionFailed",
                $"Execution of saved plan '{sourcePlan.Id}' ended with status {operationState.Status}: {executionDocument.Error}",
                sourcePlan.WinnerNode));
        }

        var repairGuards = sourcePlan.DocumentIds
            .Select(documentId => new RepairActionGuardDocument
            {
                Id = StateStore.GetRepairActionGuardId(run.RunId, documentId),
                RunId = run.RunId,
                DocumentId = documentId,
                WinnerNode = sourcePlan.WinnerNode,
                WinnerCV = sourcePlan.WinnerCV,
                PatchOperationId = executionDocument.PatchOperationId,
                RecordedAt = DateTimeOffset.UtcNow
            })
            .ToList();

        return new RepairExecutionOutcome(
            executionDocument,
            repairGuards,
            attempted,
            executionDocument.DocumentsPatchedOnWinner ?? patched,
            executionDocument.DocumentsFailed ?? failed,
            documentDecisions,
            diagnostics,
            []);
    }

    private RepairExecutionOutcome FinalizeRecoveredStateWithoutRedispatch(
        RunStateDocument run,
        RepairDocument sourcePlan,
        RepairDocument executionDocument,
        IReadOnlyDictionary<string, string> documentDecisions,
        IReadOnlyCollection<DiagnosticDocument> startupDiagnostics)
    {
        var diagnostics = startupDiagnostics.ToList();
        var attempted = sourcePlan.DocumentIds.Count;
        var patched = documentDecisions.Values.Count(decision =>
            string.Equals(decision, "PatchCompletedOnWinner", StringComparison.Ordinal));
        var failed = attempted - patched;

        executionDocument.CompletedAt = DateTimeOffset.UtcNow;
        executionDocument.DocumentsAttempted = attempted;
        executionDocument.DocumentsPatchedOnWinner = patched;
        executionDocument.DocumentsFailed = failed;
        executionDocument.Error = failed > 0
            ? $"Recovered blocked plan without redispatch; {failed} document(s) no longer matched the saved repair target."
            : null;

        executionDocument.RepairStatus = patched > 0
            ? "PatchCompletedOnWinner"
            : documentDecisions.Values.All(decision => string.Equals(decision, "SkippedStateChanged", StringComparison.Ordinal))
                ? "SkippedStateChanged"
                : "PatchFailed";

        var repairGuards = sourcePlan.DocumentIds
            .Select(documentId => new RepairActionGuardDocument
            {
                Id = StateStore.GetRepairActionGuardId(run.RunId, documentId),
                RunId = run.RunId,
                DocumentId = documentId,
                WinnerNode = sourcePlan.WinnerNode,
                WinnerCV = sourcePlan.WinnerCV,
                PatchOperationId = executionDocument.PatchOperationId,
                RecordedAt = DateTimeOffset.UtcNow
            })
            .ToList();

        return new RepairExecutionOutcome(
            executionDocument,
            repairGuards,
            attempted,
            patched,
            failed,
            new Dictionary<string, string>(documentDecisions, StringComparer.OrdinalIgnoreCase),
            diagnostics,
            []);
    }

    private RepairExecutionOutcome FinalizeRecoveredOperationState(
        RunStateDocument run,
        RepairDocument sourcePlan,
        RepairDocument executionDocument,
        OperationState operationState,
        IReadOnlyCollection<string> redispatchedDocumentIds,
        IReadOnlyDictionary<string, string> precomputedDocumentDecisions,
        IReadOnlyCollection<DiagnosticDocument> startupDiagnostics)
    {
        var diagnostics = startupDiagnostics.ToList();
        var attempted = sourcePlan.DocumentIds.Count;
        var documentDecisions = new Dictionary<string, string>(precomputedDocumentDecisions, StringComparer.OrdinalIgnoreCase);

        executionDocument.CompletedAt = DateTimeOffset.UtcNow;
        executionDocument.DocumentsAttempted = attempted;

        if (operationState.Status == OperationStatus.Completed)
        {
            if (operationState.Result is not BulkOperationResult result)
            {
                foreach (var documentId in redispatchedDocumentIds)
                    documentDecisions[documentId] = "PatchFailed";

                diagnostics.Add(CreateDiagnostic(
                    run.RunId,
                    "RepairPlanExecutionFailed",
                    $"Recovered blocked saved plan '{sourcePlan.Id}' completed without a BulkOperationResult payload.",
                    sourcePlan.WinnerNode));
            }
            else
            {
                var statusById = BuildPatchStatusById(result, redispatchedDocumentIds);
                foreach (var documentId in redispatchedDocumentIds)
                {
                    if (!statusById.TryGetValue(documentId, out var status))
                    {
                        documentDecisions[documentId] = result.Total > 0
                            ? "PatchCompletedOnWinner"
                            : "PatchFailed";
                        continue;
                    }

                    documentDecisions[documentId] = MapPatchStatusToDecision(status);
                }

                diagnostics.Add(CreateDiagnostic(
                    run.RunId,
                    "RepairPlanExecution",
                    $"Recovered blocked saved plan '{sourcePlan.Id}' on winner '{sourcePlan.WinnerNode}'.",
                    sourcePlan.WinnerNode));
            }
        }
        else
        {
            foreach (var documentId in redispatchedDocumentIds)
                documentDecisions[documentId] = "PatchFailed";

            diagnostics.Add(CreateDiagnostic(
                run.RunId,
                "RepairPlanExecutionFailed",
                $"Recovered blocked saved plan '{sourcePlan.Id}' ended with status {operationState.Status}: {operationState.Result?.ToString()}",
                sourcePlan.WinnerNode));
        }

        foreach (var documentId in sourcePlan.DocumentIds)
        {
            if (!documentDecisions.ContainsKey(documentId))
                documentDecisions[documentId] = "PatchFailed";
        }

        var patched = documentDecisions.Values.Count(decision =>
            string.Equals(decision, "PatchCompletedOnWinner", StringComparison.Ordinal));
        var failed = attempted - patched;

        executionDocument.DocumentsPatchedOnWinner = patched;
        executionDocument.DocumentsFailed = failed;
        executionDocument.Error = failed > 0
            ? $"Non-patched documents in recovered group: {failed} of {attempted}."
            : null;
        executionDocument.RepairStatus = patched > 0
            ? "PatchCompletedOnWinner"
            : documentDecisions.Values.All(decision => string.Equals(decision, "SkippedStateChanged", StringComparison.Ordinal))
                ? "SkippedStateChanged"
                : "PatchFailed";

        var repairGuards = sourcePlan.DocumentIds
            .Select(documentId => new RepairActionGuardDocument
            {
                Id = StateStore.GetRepairActionGuardId(run.RunId, documentId),
                RunId = run.RunId,
                DocumentId = documentId,
                WinnerNode = sourcePlan.WinnerNode,
                WinnerCV = sourcePlan.WinnerCV,
                PatchOperationId = executionDocument.PatchOperationId,
                RecordedAt = DateTimeOffset.UtcNow
            })
            .ToList();

        return new RepairExecutionOutcome(
            executionDocument,
            repairGuards,
            attempted,
            patched,
            failed,
            documentDecisions,
            diagnostics,
            []);
    }

    private async Task<OperationState> WaitForTerminalOperationStateAsync(
        string winnerNode,
        long patchOperationId,
        CancellationToken ct)
    {
        if (_storesByNodeUrl.TryGetValue(winnerNode, out var store) == false)
        {
            throw new InvalidOperationException(
                $"Cannot resume patch operation {patchOperationId} because winner node '{winnerNode}' is not present in the configured node list.");
        }

        while (true)
        {
            var state = await store
                .Maintenance
                .SendAsync(new GetOperationStateOperation(patchOperationId), ct)
                .ConfigureAwait(false);

            if (state == null)
            {
                throw new InvalidOperationException(
                    $"RavenDB returned no operation state for patch operation {patchOperationId} on winner node '{winnerNode}'. Automatic resume cannot determine whether the operation completed, so this apply-run requires manual investigation.");
            }

            if (state.Status != OperationStatus.InProgress)
                return state;

            await Task.Delay(TimeSpan.FromMilliseconds(250), ct).ConfigureAwait(false);
        }
    }

    private async Task PersistFinalOutcomeAsync(
        RunStateDocument run,
        RepairDocument sourcePlan,
        RepairExecutionOutcome outcome,
        CancellationToken ct)
    {
        var updatedMismatches = await UpdateSourcePlanMismatchesAsync(run, sourcePlan, outcome.DocumentDecisionsById, ct)
            .ConfigureAwait(false);

        await _stateStore.PersistBatchAsync(
                run,
                updatedMismatches,
                [outcome.RepairDocument],
                outcome.RepairGuards,
                outcome.StateChangedDocuments,
                outcome.Diagnostics,
                ct)
            .ConfigureAwait(false);
    }

    private static RepairDocument CreateExecutionDocument(
        RunStateDocument run,
        RepairDocument sourcePlan,
        string id)
    {
        return new RepairDocument
        {
            Id = id,
            RunId = run.RunId,
            DocumentIds = sourcePlan.DocumentIds.ToList(),
            Collection = sourcePlan.Collection,
            WinnerNode = sourcePlan.WinnerNode,
            WinnerCV = sourcePlan.WinnerCV,
            AffectedNodes = sourcePlan.AffectedNodes.ToList(),
            PatchOperationId = sourcePlan.PatchOperationId,
            SourceRepairPlanId = sourcePlan.Id,
            SourceRepairPlanRunId = sourcePlan.RunId,
            RepairStatus = ApplyRepairPlanExecution.PatchDispatching,
            CompletedAt = DateTimeOffset.UtcNow
        };
    }

    private static void ApplyOutcome(RunStateDocument run, RepairExecutionOutcome outcome)
    {
        run.RepairsAttempted += outcome.RepairsAttempted;
        run.RepairsPatchedOnWinner += outcome.RepairsPatchedOnWinner;
        run.RepairsFailed += outcome.RepairsFailed;
    }

    private RepairExecutionOutcome CreateSkippedOutcome(
        RunStateDocument run,
        RepairDocument sourcePlan,
        string repairStatus,
        string message,
        string diagnosticKind,
        IReadOnlyCollection<RepairStateChangedDocument> stateChangedDocuments)
    {
        var executionDocument = CreateExecutionDocument(
            run,
            sourcePlan,
            StateStore.CreateApplyRepairExecutionId(run.RunId, sourcePlan.Id));
        executionDocument.RepairStatus = repairStatus;
        executionDocument.Error = message;
        executionDocument.DocumentsAttempted = 0;
        executionDocument.DocumentsPatchedOnWinner = 0;
        executionDocument.DocumentsFailed = 0;
        executionDocument.CompletedAt = DateTimeOffset.UtcNow;

        var documentId = sourcePlan.DocumentIds.Single();
        var repairGuards = new List<RepairActionGuardDocument>
        {
            new()
            {
                Id = StateStore.GetRepairActionGuardId(run.RunId, documentId),
                RunId = run.RunId,
                DocumentId = documentId,
                WinnerNode = sourcePlan.WinnerNode,
                WinnerCV = sourcePlan.WinnerCV,
                PatchOperationId = null,
                RecordedAt = DateTimeOffset.UtcNow
            }
        };

        return new RepairExecutionOutcome(
            executionDocument,
            repairGuards,
            RepairsAttempted: 0,
            RepairsPatchedOnWinner: 0,
            RepairsFailed: 0,
            new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
            {
                [documentId] = repairStatus
            },
            [
                CreateDiagnostic(
                    run.RunId,
                    diagnosticKind,
                    $"{message} DocumentId='{documentId}'.",
                    sourcePlan.WinnerNode)
            ],
            stateChangedDocuments.ToList());
    }

    private RepairStateChangedDocument CreateRepairStateChangedDocument(
        RunStateDocument applyRun,
        RunStateDocument sourcePlanRun,
        MismatchDocument sourceMismatch,
        IReadOnlyCollection<NodeObservedState> liveObservedState,
        DocumentEvaluation? liveEvaluation)
    {
        return new RepairStateChangedDocument
        {
            Id = StateStore.CreateRepairStateChangedId(applyRun.RunId, sourceMismatch.DocumentId),
            ApplyRunId = applyRun.RunId,
            SourceRepairPlanRunId = sourcePlanRun.RunId,
            SourceSnapshotRunId = sourcePlanRun.SourceSnapshotRunId,
            DocumentId = sourceMismatch.DocumentId,
            SourceMismatchType = sourceMismatch.MismatchType,
            SourceWinnerNode = sourceMismatch.WinnerNode,
            SourceWinnerCV = sourceMismatch.WinnerCV,
            SourceObservedState = sourceMismatch.ObservedState
                .Select(CloneObservedState)
                .ToList(),
            LiveIsConsistent = liveEvaluation == null,
            LiveMismatchType = liveEvaluation?.MismatchType,
            LiveWinnerNode = liveEvaluation?.WinnerNode,
            LiveWinnerCV = liveEvaluation?.WinnerCV,
            LiveObservedState = liveObservedState
                .Select(CloneObservedState)
                .ToList(),
            Reason = liveEvaluation == null ? "ConsistentNow" : "ChangedStillInconsistent",
            RecordedAt = DateTimeOffset.UtcNow
        };
    }

    private bool LiveEvaluationMatchesSavedPlan(
        RepairDocument sourcePlan,
        DocumentEvaluation? liveEvaluation,
        ChangeVectorSemanticsSnapshot? sourceSemanticsSnapshot)
    {
        if (liveEvaluation == null)
            return false;

        if (string.Equals(liveEvaluation.MismatchType, "AMBIGUOUS_CV", StringComparison.Ordinal) ||
            string.Equals(liveEvaluation.MismatchType, "COLLECTION_MISMATCH", StringComparison.Ordinal))
        {
            return false;
        }

        var normalizedLiveWinnerCv = ConsistencyDecisionEngine.NormalizeChangeVector(
            liveEvaluation.WinnerCV,
            sourceSemanticsSnapshot?.ExplicitUnusedDatabaseIdSet);
        var normalizedSavedWinnerCv = ConsistencyDecisionEngine.NormalizeChangeVector(
            sourcePlan.WinnerCV,
            sourceSemanticsSnapshot?.ExplicitUnusedDatabaseIdSet);

        return string.Equals(liveEvaluation.WinnerNode, sourcePlan.WinnerNode, StringComparison.OrdinalIgnoreCase) &&
               string.Equals(liveEvaluation.Collection, sourcePlan.Collection, StringComparison.OrdinalIgnoreCase) &&
               string.Equals(normalizedLiveWinnerCv, normalizedSavedWinnerCv, StringComparison.OrdinalIgnoreCase);
    }

    private void RenderRepairPreview(RepairPreview preview)
    {
        var sourceWinnerLabel = string.IsNullOrWhiteSpace(preview.SourcePlan.WinnerNode)
            ? "[yellow]unknown[/]"
            : $"[cyan]{Markup.Escape(ResolveWinnerLabel(preview.SourcePlan.WinnerNode))}[/]";
        var liveWinnerLabel = preview.LiveEvaluation == null
            ? "[green]cluster is consistent[/]"
            : $"[cyan]{Markup.Escape(ResolveWinnerLabel(preview.LiveEvaluation.WinnerNode ?? string.Empty))}[/]";
        var statusMarkup = preview.PatchEligible
            ? "[green]Eligible to apply saved repair[/]"
            : preview.LiveEvaluation == null
                ? "[yellow]Already consistent now; saved repair will be suppressed[/]"
                : "[yellow]State changed since snapshot; saved repair will be suppressed and recorded[/]";

        var headerMarkup = new Markup(string.Join(Environment.NewLine,
        [
            $"[grey]Document ID   :[/] [bold]{Markup.Escape(preview.SourceMismatch.DocumentId)}[/]",
            $"[grey]Saved winner :[/] {sourceWinnerLabel}",
            $"[grey]Live winner  :[/] {liveWinnerLabel}",
            $"[grey]Status       :[/] {statusMarkup}"
        ]));

        var savedCvTable = BuildChangeVectorTable(preview.SourceMismatch.ObservedState, preview.SourcePlan.WinnerNode);
        var liveCvTable = BuildChangeVectorTable(preview.LiveObservedState, preview.LiveEvaluation?.WinnerNode);

        AnsiConsole.Write(new Panel(new Rows(
                headerMarkup,
                new Text(""),
                new Markup("[grey]Saved state:[/]"), savedCvTable,
                new Text(""),
                new Markup("[grey]Live state:[/]"), liveCvTable))
            .Header("[bold]Repair Preview[/]")
            .Border(BoxBorder.Rounded)
            .BorderStyle(preview.PatchEligible ? Style.Parse("green") : Style.Parse("yellow")));
    }

    private Table BuildChangeVectorTable(IReadOnlyList<NodeObservedState> observedState, string? winnerNodeUrl)
    {
        var rawSegments = _config.Nodes.ToDictionary(
            node => node.Url,
            node =>
            {
                var state = observedState.FirstOrDefault(s =>
                    string.Equals(s.NodeUrl, node.Url, StringComparison.OrdinalIgnoreCase));
                return ParseRawCvSegments(state is { Present: true } ? state.ChangeVector : null);
            },
            StringComparer.OrdinalIgnoreCase);

        // Collect all unique database IDs and pick a display prefix from whichever node has it
        var displayPrefixByDbId = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var segments in rawSegments.Values)
            foreach (var (dbId, (prefix, _)) in segments)
                displayPrefixByDbId.TryAdd(dbId, prefix);

        var table = new Table()
            .Border(TableBorder.Simple)
            .BorderStyle(Style.Parse("grey"))
            .HideHeaders();

        // Column headers: blank segment column + one column per node
        table.AddColumn(new TableColumn(string.Empty).LeftAligned());
        foreach (var node in _config.Nodes)
        {
            var isWinner = string.Equals(node.Url, winnerNodeUrl, StringComparison.OrdinalIgnoreCase);
            var header = isWinner
                ? $"[green bold]{Markup.Escape(node.Label)}[/]"
                : $"[grey]{Markup.Escape(node.Label)}[/]";
            table.AddColumn(new TableColumn(header).RightAligned());
        }

        // Header row (since HideHeaders is set, add it manually as a styled row)
        var headerCells = new List<string> { "[grey]Segment[/]" };
        foreach (var node in _config.Nodes)
        {
            var isWinner = string.Equals(node.Url, winnerNodeUrl, StringComparison.OrdinalIgnoreCase);
            headerCells.Add(isWinner
                ? $"[green bold]{Markup.Escape(node.Label)} ✓[/]"
                : $"[grey]{Markup.Escape(node.Label)}[/]");
        }
        table.AddRow(headerCells.ToArray());

        // Sort segments: by prefix (A, B, C, RAFT, SINK…) then by dbId
        var sortedDbIds = displayPrefixByDbId.Keys
            .OrderBy(dbId => displayPrefixByDbId[dbId], StringComparer.Ordinal)
            .ThenBy(dbId => dbId, StringComparer.Ordinal)
            .ToList();

        foreach (var dbId in sortedDbIds)
        {
            var prefix = displayPrefixByDbId[dbId];
            var shortDbId = dbId.Length > 7 ? dbId[..7] : dbId;
            var segmentLabel = $"[grey]{Markup.Escape(prefix)}:{Markup.Escape(shortDbId)}…[/]";

            var etagByNode = _config.Nodes
                .Select(node => rawSegments[node.Url].TryGetValue(dbId, out var seg) ? (long?)seg.etag : null)
                .ToList();

            var distinctValues = etagByNode.Where(e => e.HasValue).Select(e => e!.Value).Distinct().ToList();
            var allSame = etagByNode.All(e => e.HasValue) && distinctValues.Count == 1;

            var cells = new List<string> { segmentLabel };
            foreach (var (node, etagOpt) in _config.Nodes.Zip(etagByNode))
            {
                if (!etagOpt.HasValue)
                {
                    cells.Add("[red]—[/]");
                    continue;
                }

                var etag = etagOpt.Value;
                var isWinnerNode = string.Equals(node.Url, winnerNodeUrl, StringComparison.OrdinalIgnoreCase);
                var formatted = etag.ToString("N0");

                cells.Add(allSame
                    ? $"[grey]{formatted}[/]"
                    : isWinnerNode
                        ? $"[green]{formatted}[/]"
                        : $"[red]{formatted}[/]");
            }

            table.AddRow(cells.ToArray());
        }

        return table;
    }

    private static Dictionary<string, (string prefix, long etag)> ParseRawCvSegments(string? changeVector)
    {
        var result = new Dictionary<string, (string prefix, long etag)>(StringComparer.Ordinal);
        if (string.IsNullOrWhiteSpace(changeVector))
            return result;

        foreach (var segment in changeVector.Split(',', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries))
        {
            var colonIndex = segment.IndexOf(':');
            if (colonIndex <= 0)
                continue;

            var dashIndex = segment.IndexOf('-', colonIndex);
            if (dashIndex <= colonIndex)
                continue;

            var prefix = segment[..colonIndex];
            var databaseId = segment[(dashIndex + 1)..];
            var etagText = segment[(colonIndex + 1)..dashIndex];

            if (!long.TryParse(etagText, out var etag))
                continue;

            if (result.TryGetValue(databaseId, out var existing))
                result[databaseId] = (existing.prefix, Math.Max(existing.etag, etag));
            else
                result[databaseId] = (prefix, etag);
        }

        return result;
    }

    private static void ReorderForPriorityDocuments(List<RepairDocument> plans, ApplyRepairPlanResumeState resumeState)
    {
        AnsiConsole.MarkupLine("[grey]Enter document IDs to process first (one per line, empty line to start):[/]");

        var insertPosition = 0;
        while (true)
        {
            var input = new TextPrompt<string>("[grey]  Priority document ID:[/]")
                .AllowEmpty()
                .Show(AnsiConsole.Console);

            if (string.IsNullOrWhiteSpace(input))
                break;

            var docId = input.Trim();
            var matchIndex = plans.FindIndex(p =>
                string.Equals(p.DocumentIds[0], docId, StringComparison.OrdinalIgnoreCase));

            if (matchIndex < 0)
            {
                AnsiConsole.MarkupLine($"  [yellow]Not found in repair plan: {Markup.Escape(docId)}[/]");
                continue;
            }

            var matched = plans[matchIndex];
            if (resumeState.ExecutionsBySourceRepairPlanId.TryGetValue(matched.Id, out var existingExec) &&
                ApplyRepairPlanExecution.IsTerminalStatus(existingExec.RepairStatus))
            {
                AnsiConsole.MarkupLine($"  [yellow]Already processed ({existingExec.RepairStatus}): {Markup.Escape(docId)}[/]");
                continue;
            }

            plans.RemoveAt(matchIndex);
            plans.Insert(insertPosition++, matched);
            AnsiConsole.MarkupLine($"  [green]Moved to position {insertPosition}: {Markup.Escape(docId)}[/]");
        }
    }

    private static async Task<InteractivePatchDecision> AskForPatchDecisionAsync(CancellationToken ct)
    {
        var choice = await new SelectionPrompt<string>()
            .Title("[bold]Choose how to proceed with this document[/]")
            .AddChoices(
                "Apply this document",
                "Apply this document and continue automatically",
                "Stop here")
            .ShowAsync(AnsiConsole.Console, ct)
            .ConfigureAwait(false);

        return choice switch
        {
            "Apply this document and continue automatically" => new InteractivePatchDecision(false, true),
            "Stop here" => new InteractivePatchDecision(true, false),
            _ => new InteractivePatchDecision(false, false)
        };
    }

    private static async Task<InteractiveContinueDecision> AskForNonPatchContinuationAsync(CancellationToken ct)
    {
        var choice = await new SelectionPrompt<string>()
            .Title("[bold]No patch will be sent for this document[/]")
            .AddChoices(
                "Continue to the next document",
                "Continue automatically from now on",
                "Stop here")
            .ShowAsync(AnsiConsole.Console, ct)
            .ConfigureAwait(false);

        return choice switch
        {
            "Continue automatically from now on" => new InteractiveContinueDecision(false, true),
            "Stop here" => new InteractiveContinueDecision(true, false),
            _ => new InteractiveContinueDecision(false, false)
        };
    }

    private string ResolveWinnerLabel(string winnerNode)
        => _labelsByNodeUrl.TryGetValue(winnerNode, out var winnerLabel)
            ? winnerLabel
            : winnerNode;

    private void PublishProgress(
        string phase,
        string? currentNodeLabel,
        string? detail,
        RunStateDocument run,
        long? itemsCompleted = null)
    {
        ProgressUpdated?.Invoke(new IndexedRunProgressUpdate(
            Phase: phase,
            CurrentNodeLabel: currentNodeLabel,
            Detail: detail,
            CurrentNodeStreamedSnapshots: null,
            CurrentNodeTotalDocuments: null,
            SnapshotDocumentsImported: run.SnapshotDocumentsImported,
            SnapshotDocumentsSkipped: run.SnapshotDocumentsSkipped,
            SnapshotBulkInsertRestarts: run.SnapshotBulkInsertRestarts,
            CandidateDocumentsFound: run.CandidateDocumentsFound,
            CandidateDocumentsProcessed: run.CandidateDocumentsProcessed,
            CandidateDocumentsExcludedBySkippedSnapshots: run.CandidateDocumentsExcludedBySkippedSnapshots,
            DocumentsInspected: run.DocumentsInspected,
            UniqueVersionsCompared: run.UniqueVersionsCompared,
            MismatchesFound: run.MismatchesFound,
            RepairsPlanned: run.RepairsPlanned,
            RepairsAttempted: run.RepairsAttempted,
            RepairsPatchedOnWinner: run.RepairsPatchedOnWinner,
            RepairsFailed: run.RepairsFailed,
            RepairsCompleted: itemsCompleted));
    }

    private static DiagnosticDocument CreateDiagnostic(
        string runId,
        string kind,
        string message,
        string? nodeUrl = null)
    {
        return new DiagnosticDocument
        {
            Id = StateStore.CreateDiagnosticId(runId),
            RunId = runId,
            Kind = kind,
            NodeUrl = nodeUrl,
            Message = message,
            Timestamp = DateTimeOffset.UtcNow
        };
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

    private static long GetOperationId(Operation operation)
        => (long)(OperationIdProperty.GetValue(operation)
                  ?? throw new InvalidOperationException("Raven.Client Operation.Id returned null."));

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

    private async Task<List<MismatchDocument>> UpdateSourcePlanMismatchesAsync(
        RunStateDocument applyRun,
        RepairDocument sourcePlan,
        IReadOnlyDictionary<string, string> documentDecisionsById,
        CancellationToken ct)
    {
        if (documentDecisionsById.Count == 0)
            return [];

        var sourcePlanRunId = sourcePlan.RunId;
        if (string.IsNullOrWhiteSpace(sourcePlanRunId))
            return [];

        var mismatches = await _stateStore
            .LoadMismatchesByRunAndDocumentIdsAsync(sourcePlanRunId, sourcePlan.DocumentIds, ct)
            .ConfigureAwait(false);

        var updatedAt = DateTimeOffset.UtcNow;
        foreach (var mismatch in mismatches)
        {
            if (!documentDecisionsById.TryGetValue(mismatch.DocumentId, out var decision))
                continue;

            mismatch.CurrentRepairDecision = decision;
            mismatch.LastRepairRunId = applyRun.RunId;
            mismatch.LastRepairUpdatedAt = updatedAt;
        }

        return mismatches.ToList();
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

    private sealed record RepairExecutionOutcome(
        RepairDocument RepairDocument,
        List<RepairActionGuardDocument> RepairGuards,
        long RepairsAttempted,
        long RepairsPatchedOnWinner,
        long RepairsFailed,
        IReadOnlyDictionary<string, string> DocumentDecisionsById,
        IReadOnlyCollection<DiagnosticDocument> Diagnostics,
        List<RepairStateChangedDocument> StateChangedDocuments);

    private sealed record RepairPreview(
        RepairDocument SourcePlan,
        MismatchDocument SourceMismatch,
        IReadOnlyList<NodeObservedState> LiveObservedState,
        DocumentEvaluation? LiveEvaluation,
        bool SnapshotStateChanged,
        bool PatchEligible);

    private sealed record InteractivePatchDecision(bool StopRequested, bool EnableAutomaticMode);

    private sealed record InteractiveContinueDecision(bool StopRequested, bool EnableAutomaticMode);

    private sealed record PerDocumentExecutionResult(
        RepairExecutionOutcome? Outcome,
        bool EnableAutomaticMode,
        bool StopRequested);
}
