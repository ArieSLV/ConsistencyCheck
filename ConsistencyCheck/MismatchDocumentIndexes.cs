using Raven.Client.Documents.Indexes;

namespace ConsistencyCheck;

internal sealed class AmbiguousDocumentIndexEntry
{
    public string RunId { get; set; } = string.Empty;
    public string DocumentId { get; set; } = string.Empty;
    public string? Collection { get; set; }
    public string MismatchType { get; set; } = string.Empty;
    public string RepairDecision { get; set; } = string.Empty;
    public string CurrentRepairDecision { get; set; } = string.Empty;
    public string? LastRepairRunId { get; set; }
    public DateTimeOffset? LastRepairUpdatedAt { get; set; }
    public DateTimeOffset DetectedAt { get; set; }
    public List<string> PresentNodes { get; set; } = [];
    public int PresentNodesCount { get; set; }
    public List<string> ObservedChangeVectors { get; set; } = [];
    public int ObservedChangeVectorsCount { get; set; }
}

internal sealed class RepairableDocumentIndexEntry
{
    public string RunId { get; set; } = string.Empty;
    public string DocumentId { get; set; } = string.Empty;
    public string? Collection { get; set; }
    public string MismatchType { get; set; } = string.Empty;
    public string WinnerNode { get; set; } = string.Empty;
    public string? WinnerCV { get; set; }
    public string RepairDecision { get; set; } = string.Empty;
    public string CurrentRepairDecision { get; set; } = string.Empty;
    public string? LastRepairRunId { get; set; }
    public DateTimeOffset? LastRepairUpdatedAt { get; set; }
    public DateTimeOffset DetectedAt { get; set; }
    public List<string> PresentNodes { get; set; } = [];
    public int PresentNodesCount { get; set; }
    public List<string> ObservedChangeVectors { get; set; } = [];
    public int ObservedChangeVectorsCount { get; set; }
}

internal sealed class MismatchDocumentLookupEntry
{
    public string Id { get; set; } = string.Empty;
    public string RunId { get; set; } = string.Empty;
    public string DocumentId { get; set; } = string.Empty;
}

internal sealed class UnhealthyDocumentIndexEntry
{
    public string RunId { get; set; } = string.Empty;
    public string DocumentId { get; set; } = string.Empty;
    public string? Collection { get; set; }
    public string MismatchType { get; set; } = string.Empty;
    public string RepairDecision { get; set; } = string.Empty;
    public string CurrentRepairDecision { get; set; } = string.Empty;
    public string? WinnerNode { get; set; }
    public string? WinnerCV { get; set; }
    public DateTimeOffset DetectedAt { get; set; }
}

internal static class MismatchDocumentIndexes
{
    public const string CollectionName = "MismatchDocuments";
    public const string ManualReviewDocumentsIndexName = "MismatchDocuments/ManualReviewDocuments";
    public const string AutomaticRepairPlanDocumentsIndexName = "MismatchDocuments/AutomaticRepairPlanDocuments";
    public const string UnhealthyDocumentsIndexName = "MismatchDocuments/UnhealthyDocuments";
    public const string ByRunAndDocumentIdIndexName = "MismatchDocuments/ByRunAndDocumentId";
    public const string ByRunIndexName = "MismatchDocuments/ByRun";
    public const string SkippedAmbiguousCvIndexName = "MismatchDocuments/SkippedAmbiguousCv";
    public const string SkippedCollectionMismatchIndexName = "MismatchDocuments/SkippedCollectionMismatch";
    public const string SkippedAlreadyPlannedIndexName = "MismatchDocuments/SkippedAlreadyPlanned";

    public static bool IsAmbiguous(MismatchDocument mismatch)
        => string.Equals(mismatch.MismatchType, "AMBIGUOUS_CV", StringComparison.Ordinal);

    public static bool RequiresManualReview(MismatchDocument mismatch)
        => IsAmbiguous(mismatch);

    public static bool HasAutomaticRepairPlan(MismatchDocument mismatch)
        => string.Equals(ResolveCurrentRepairDecision(mismatch), "PatchPlannedDryRun", StringComparison.Ordinal);

    public static bool IsRepairable(MismatchDocument mismatch)
    {
        if (IsAmbiguous(mismatch))
            return false;

        if (string.Equals(mismatch.MismatchType, "COLLECTION_MISMATCH", StringComparison.Ordinal))
            return false;

        if (string.IsNullOrWhiteSpace(mismatch.Collection))
            return false;

        if (string.IsNullOrWhiteSpace(mismatch.WinnerNode))
            return false;

        var currentDecision = ResolveCurrentRepairDecision(mismatch);
        return string.Equals(currentDecision, "PatchCompletedOnWinner", StringComparison.Ordinal) == false &&
               string.Equals(currentDecision, "SkippedStateChanged", StringComparison.Ordinal) == false &&
               string.Equals(currentDecision, "SkippedAlreadyPatchedThisRun", StringComparison.Ordinal) == false;
    }

    public static string ResolveCurrentRepairDecision(MismatchDocument mismatch)
        => string.IsNullOrWhiteSpace(mismatch.CurrentRepairDecision)
            ? mismatch.RepairDecision
            : mismatch.CurrentRepairDecision;

    public static IndexDefinition BuildManualReviewDocumentsIndexDefinition()
    {
        return new IndexDefinition
        {
            Name = ManualReviewDocumentsIndexName,
            Maps =
            {
                $$"""
                  from doc in docs.{{CollectionName}}
                  where doc.MismatchType == "AMBIGUOUS_CV"
                  select new
                  {
                      doc.RunId,
                      doc.DocumentId,
                      doc.Collection,
                      doc.MismatchType,
                      doc.RepairDecision,
                      CurrentRepairDecision = doc.CurrentRepairDecision != null && doc.CurrentRepairDecision != "" ? doc.CurrentRepairDecision : doc.RepairDecision,
                      doc.LastRepairRunId,
                      doc.LastRepairUpdatedAt,
                      doc.DetectedAt,
                      PresentNodes = doc.ObservedState.Where(x => x.Present).Select(x => x.NodeUrl).Distinct().ToArray(),
                      PresentNodesCount = doc.ObservedState.Count(x => x.Present),
                      ObservedChangeVectors = doc.ObservedState.Where(x => x.Present && x.ChangeVector != null && x.ChangeVector != "").Select(x => x.ChangeVector).Distinct().ToArray(),
                      ObservedChangeVectorsCount = doc.ObservedState.Where(x => x.Present && x.ChangeVector != null && x.ChangeVector != "").Select(x => x.ChangeVector).Distinct().Count()
                  }
                  """
            }
        };
    }

    public static IndexDefinition BuildAutomaticRepairPlanDocumentsIndexDefinition()
    {
        return new IndexDefinition
        {
            Name = AutomaticRepairPlanDocumentsIndexName,
            Maps =
            {
                $$"""
                  from doc in docs.{{CollectionName}}
                  let currentRepairDecision = doc.CurrentRepairDecision != null && doc.CurrentRepairDecision != "" ? doc.CurrentRepairDecision : doc.RepairDecision
                  where currentRepairDecision == "PatchPlannedDryRun"
                  select new
                  {
                      doc.RunId,
                      doc.DocumentId,
                      doc.Collection,
                      doc.MismatchType,
                      doc.WinnerNode,
                      doc.WinnerCV,
                      doc.RepairDecision,
                      CurrentRepairDecision = currentRepairDecision,
                      doc.LastRepairRunId,
                      doc.LastRepairUpdatedAt,
                      doc.DetectedAt,
                      PresentNodes = doc.ObservedState.Where(x => x.Present).Select(x => x.NodeUrl).Distinct().ToArray(),
                      PresentNodesCount = doc.ObservedState.Count(x => x.Present),
                      ObservedChangeVectors = doc.ObservedState.Where(x => x.Present && x.ChangeVector != null && x.ChangeVector != "").Select(x => x.ChangeVector).Distinct().ToArray(),
                      ObservedChangeVectorsCount = doc.ObservedState.Where(x => x.Present && x.ChangeVector != null && x.ChangeVector != "").Select(x => x.ChangeVector).Distinct().Count()
                  }
                  """
            }
        };
    }

    public static IndexDefinition BuildUnhealthyDocumentsIndexDefinition()
    {
        return new IndexDefinition
        {
            Name = UnhealthyDocumentsIndexName,
            Maps =
            {
                $$"""
                  from doc in docs.{{CollectionName}}
                  let currentRepairDecision = doc.CurrentRepairDecision != null && doc.CurrentRepairDecision != "" ? doc.CurrentRepairDecision : doc.RepairDecision
                  select new
                  {
                      doc.RunId,
                      doc.DocumentId,
                      doc.Collection,
                      doc.MismatchType,
                      doc.RepairDecision,
                      CurrentRepairDecision = currentRepairDecision,
                      doc.WinnerNode,
                      doc.WinnerCV,
                      doc.DetectedAt
                  }
                  """
            }
        };
    }

    public static IndexDefinition BuildByRunAndDocumentIdIndexDefinition()
    {
        return new IndexDefinition
        {
            Name = ByRunAndDocumentIdIndexName,
            Maps =
            {
                $$"""
                  from doc in docs.{{CollectionName}}
                  select new
                  {
                      Id = Id(doc),
                      doc.RunId,
                      doc.DocumentId
                  }
                  """
            }
        };
    }

    /// <summary>All mismatches for a run — overview of all 13 (or however many).</summary>
    public static IndexDefinition BuildByRunIndexDefinition()
    {
        return new IndexDefinition
        {
            Name = ByRunIndexName,
            Maps =
            {
                $$"""
                  from doc in docs.{{CollectionName}}
                  let currentRepairDecision = doc.CurrentRepairDecision != null && doc.CurrentRepairDecision != "" ? doc.CurrentRepairDecision : doc.RepairDecision
                  select new
                  {
                      doc.RunId,
                      doc.DocumentId,
                      doc.Collection,
                      doc.MismatchType,
                      CurrentRepairDecision = currentRepairDecision,
                      doc.WinnerNode,
                      doc.DetectedAt
                  }
                  """
            }
        };
    }

    /// <summary>Documents skipped because change vectors were ambiguous (no clear winner).</summary>
    public static IndexDefinition BuildSkippedAmbiguousCvIndexDefinition()
    {
        return new IndexDefinition
        {
            Name = SkippedAmbiguousCvIndexName,
            Maps =
            {
                $$"""
                  from doc in docs.{{CollectionName}}
                  let currentRepairDecision = doc.CurrentRepairDecision != null && doc.CurrentRepairDecision != "" ? doc.CurrentRepairDecision : doc.RepairDecision
                  where currentRepairDecision == "SkippedAmbiguousCv"
                  select new
                  {
                      doc.RunId,
                      doc.DocumentId,
                      doc.Collection,
                      doc.MismatchType,
                      ObservedChangeVectors = doc.ObservedState.Where(x => x.Present && x.ChangeVector != null && x.ChangeVector != "").Select(x => x.ChangeVector).Distinct().ToArray(),
                      ObservedChangeVectorsCount = doc.ObservedState.Where(x => x.Present && x.ChangeVector != null && x.ChangeVector != "").Select(x => x.ChangeVector).Distinct().Count(),
                      PresentNodes = doc.ObservedState.Where(x => x.Present).Select(x => x.NodeUrl).Distinct().ToArray(),
                      doc.DetectedAt
                  }
                  """
            }
        };
    }

    /// <summary>Documents skipped because of collection mismatch or missing collection/winner info.</summary>
    public static IndexDefinition BuildSkippedCollectionMismatchIndexDefinition()
    {
        return new IndexDefinition
        {
            Name = SkippedCollectionMismatchIndexName,
            Maps =
            {
                $$"""
                  from doc in docs.{{CollectionName}}
                  let currentRepairDecision = doc.CurrentRepairDecision != null && doc.CurrentRepairDecision != "" ? doc.CurrentRepairDecision : doc.RepairDecision
                  where currentRepairDecision == "SkippedCollectionMismatch"
                  select new
                  {
                      doc.RunId,
                      doc.DocumentId,
                      doc.Collection,
                      doc.MismatchType,
                      PresentNodes = doc.ObservedState.Where(x => x.Present).Select(x => x.NodeUrl).Distinct().ToArray(),
                      ObservedChangeVectors = doc.ObservedState.Where(x => x.Present && x.ChangeVector != null && x.ChangeVector != "").Select(x => x.ChangeVector).Distinct().ToArray(),
                      doc.DetectedAt
                  }
                  """
            }
        };
    }

    /// <summary>
    /// Documents skipped because a guard was already in place from an earlier page/node in the same run.
    /// These are victims of the deterministic-ID overwrite bug — see MismatchDecisionFixupPlanner.
    /// </summary>
    public static IndexDefinition BuildSkippedAlreadyPlannedIndexDefinition()
    {
        return new IndexDefinition
        {
            Name = SkippedAlreadyPlannedIndexName,
            Maps =
            {
                $$"""
                  from doc in docs.{{CollectionName}}
                  let currentRepairDecision = doc.CurrentRepairDecision != null && doc.CurrentRepairDecision != "" ? doc.CurrentRepairDecision : doc.RepairDecision
                  where currentRepairDecision == "SkippedAlreadyPlanned"
                  select new
                  {
                      doc.RunId,
                      doc.DocumentId,
                      doc.Collection,
                      doc.MismatchType,
                      doc.WinnerNode,
                      doc.WinnerCV,
                      doc.DetectedAt
                  }
                  """
            }
        };
    }
}

internal static class RunStateDocumentIndexes
{
    public const string CollectionName = "RunStateDocuments";
    public const string LiveETagScanRangesIndexName = "RunStateDocuments/LiveETagScanRanges";

    /// <summary>One row per LiveETagScan run: which node was scanned, from which etag to which etag.</summary>
    public static IndexDefinition BuildLiveETagScanRangesIndexDefinition()
    {
        return new IndexDefinition
        {
            Name = LiveETagScanRangesIndexName,
            Maps =
            {
                """
                from doc in docs.RunStateDocuments
                where doc.RunMode == "LiveETagScan"
                let idx = doc.ConfigSnapshot != null ? (int)doc.ConfigSnapshot.SourceNodeIndex : 0
                let nodes = doc.ConfigSnapshot != null ? doc.ConfigSnapshot.Nodes : null
                let sourceNode = nodes != null ? nodes[idx].Label : null
                select new
                {
                    doc.RunId,
                    doc.CustomerDatabaseName,
                    SourceNode = sourceNode,
                    StartEtag = doc.ConfigSnapshot != null && doc.ConfigSnapshot.StartEtag != null ? (long)doc.ConfigSnapshot.StartEtag : 0L,
                    EndEtag = doc.SafeRestartEtag != null ? doc.SafeRestartEtag - 1 : (long?)null,
                    doc.IsComplete,
                    doc.StartedAt,
                    doc.CompletedAt
                }
                """
            }
        };
    }
}

internal static class RepairDocumentIndexes
{
    public const string CollectionName = "RepairDocuments";
    public const string BlockedApplyExecutionCountsIndexName = "RepairDocuments/BlockedApplyExecutionCounts";

    public static IndexDefinition BuildBlockedApplyExecutionCountsIndexDefinition()
    {
        return new IndexDefinition
        {
            Name = BlockedApplyExecutionCountsIndexName,
            Maps =
            {
                $$"""
                  from doc in docs.{{CollectionName}}
                  where doc.RepairStatus == "PatchDispatching"
                     && doc.PatchOperationId == null
                     && doc.SourceRepairPlanId != null
                     && doc.SourceRepairPlanId != ""
                     && doc.SourceRepairPlanRunId != null
                     && doc.SourceRepairPlanRunId != ""
                  select new
                  {
                      doc.RunId,
                      doc.SourceRepairPlanRunId,
                      Count = 1
                  }
                  """
            },
            Reduce =
                """
                from result in results
                group result by new
                {
                    result.RunId,
                    result.SourceRepairPlanRunId
                }
                into g
                select new
                {
                    g.Key.RunId,
                    g.Key.SourceRepairPlanRunId,
                    Count = g.Sum(x => x.Count)
                }
                """
        };
    }
}
