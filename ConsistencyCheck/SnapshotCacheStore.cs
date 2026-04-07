using System.IO.Compression;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace ConsistencyCheck;

internal sealed class SnapshotCacheRow
{
    [JsonPropertyName("id")]
    public string Id { get; set; } = string.Empty;

    [JsonPropertyName("cv")]
    public string? ChangeVector { get; set; }

    [JsonPropertyName("c")]
    public string? Collection { get; set; }

    [JsonPropertyName("lm")]
    public DateTimeOffset? LastModified { get; set; }
}

internal sealed class SnapshotCachePageIndexEntry
{
    public int PageNumber { get; set; }
    public long CompressedByteOffset { get; set; }
    public int CompressedByteLength { get; set; }
    public long FirstRowNumber { get; set; }
    public int RowCount { get; set; }
    public string FirstDocumentId { get; set; } = string.Empty;
    public string LastDocumentId { get; set; } = string.Empty;
    public string ChecksumSha256 { get; set; } = string.Empty;
}

internal sealed class SnapshotCacheSegmentManifest
{
    public int SegmentId { get; set; }
    public string FileName { get; set; } = string.Empty;
    public string IndexFileName { get; set; } = string.Empty;
    public long CompressedBytes { get; set; }
    public long RowCount { get; set; }
    public List<SnapshotCachePageIndexEntry> Pages { get; set; } = [];
}

internal sealed class SnapshotCacheNodeManifest
{
    public string CacheRunId { get; set; } = string.Empty;
    public string DatabaseName { get; set; } = string.Empty;
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
    public List<SnapshotCacheSegmentManifest> Segments { get; set; } = [];
}

internal sealed class SnapshotCacheSetNodeSummary
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

internal sealed class SnapshotCacheSetManifest
{
    public string CacheRunId { get; set; } = string.Empty;
    public string DatabaseName { get; set; } = string.Empty;
    public DateTimeOffset CreatedAt { get; set; } = DateTimeOffset.UtcNow;
    public DateTimeOffset UpdatedAt { get; set; } = DateTimeOffset.UtcNow;
    public List<SnapshotCacheSetNodeSummary> Nodes { get; set; } = [];
}

internal sealed record SnapshotCacheImportCursor(
    int SegmentId,
    int PageNumber,
    int RowOffsetInPage,
    long ImportedRows)
{
    public static SnapshotCacheImportCursor Start => new(1, 1, 0, 0);

    public static SnapshotCacheImportCursor FromNodeState(SnapshotImportNodeState state)
    {
        var segmentId = state.CurrentSegmentId > 0 ? state.CurrentSegmentId : 1;
        var pageNumber = state.CurrentPageNumber > 0 ? state.CurrentPageNumber : 1;
        var rowOffset = state.CurrentRowOffsetInPage >= 0 ? state.CurrentRowOffsetInPage : 0;
        var importedRows = state.ImportedRows >= 0 ? state.ImportedRows : 0;
        return new SnapshotCacheImportCursor(segmentId, pageNumber, rowOffset, importedRows);
    }
}

internal sealed record SnapshotCacheReadPage(
    int SegmentId,
    int PageNumber,
    IReadOnlyList<SnapshotCacheRow> Rows,
    int? NextSegmentId,
    int? NextPageNumber);

internal sealed class SnapshotCacheStore
{
    internal const int DownloadPageSize = 8_192;
    internal const long TargetCompressedSegmentBytes = 512L * 1024 * 1024;

    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        WriteIndented = true,
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
        PropertyNameCaseInsensitive = true,
        Converters = { new JsonStringEnumConverter() }
    };

    private static readonly JsonSerializerOptions RowJsonOptions = new()
    {
        WriteIndented = false,
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
        PropertyNameCaseInsensitive = true
    };

    private readonly string _cacheRootDirectory;

    public SnapshotCacheStore(string outputDirectory)
    {
        _cacheRootDirectory = Path.Combine(outputDirectory, "cache");
        Directory.CreateDirectory(_cacheRootDirectory);
    }

    public string GetSetDirectory(string databaseName, string cacheRunId)
        => Path.Combine(_cacheRootDirectory, ToSafeDirectoryName(databaseName), cacheRunId);

    public async Task<SnapshotCacheSetManifest> LoadOrCreateSetManifestAsync(
        string databaseName,
        string cacheRunId,
        IReadOnlyCollection<NodeConfig> nodes,
        CancellationToken ct)
    {
        var manifestPath = GetSetManifestPath(databaseName, cacheRunId);
        var existing = await LoadJsonIfExistsAsync<SnapshotCacheSetManifest>(manifestPath, ct).ConfigureAwait(false);
        if (existing != null)
            return existing;

        var manifest = new SnapshotCacheSetManifest
        {
            CacheRunId = cacheRunId,
            DatabaseName = databaseName,
            Nodes = nodes
                .Select((node, index) => new SnapshotCacheSetNodeSummary
                {
                    NodeUrl = node.Url,
                    NodeLabel = node.Label,
                    NodeAlias = NodeDocumentSnapshots.GetNodeAlias(index)
                })
                .ToList()
        };

        await SaveSetManifestAsync(manifest, ct).ConfigureAwait(false);
        return manifest;
    }

    public async Task<SnapshotCacheNodeManifest> LoadOrCreateNodeManifestAsync(
        string databaseName,
        string cacheRunId,
        NodeConfig node,
        string nodeAlias,
        CancellationToken ct)
    {
        var manifestPath = GetNodeManifestPath(databaseName, cacheRunId, nodeAlias);
        var existing = await LoadJsonIfExistsAsync<SnapshotCacheNodeManifest>(manifestPath, ct).ConfigureAwait(false);
        if (existing != null)
            return existing;

        EnsureNodeDirectories(databaseName, cacheRunId, nodeAlias);

        var manifest = new SnapshotCacheNodeManifest
        {
            CacheRunId = cacheRunId,
            DatabaseName = databaseName,
            NodeUrl = node.Url,
            NodeLabel = node.Label,
            NodeAlias = nodeAlias,
            LastUpdatedAt = DateTimeOffset.UtcNow,
            Segments =
            [
                CreateEmptySegment(1)
            ]
        };

        await SaveNodeManifestAsync(manifest, ct).ConfigureAwait(false);
        return manifest;
    }

    public async Task<SnapshotCacheNodeManifest> LoadRequiredNodeManifestAsync(
        string databaseName,
        string cacheRunId,
        string nodeAlias,
        CancellationToken ct)
    {
        var manifestPath = GetNodeManifestPath(databaseName, cacheRunId, nodeAlias);
        return await LoadJsonIfExistsAsync<SnapshotCacheNodeManifest>(manifestPath, ct).ConfigureAwait(false)
               ?? throw new InvalidOperationException(
                   $"Snapshot cache node manifest was not found for node '{nodeAlias}' in cache run '{cacheRunId}'.");
    }

    public async Task PrepareNodeForResumeAsync(SnapshotCacheNodeManifest nodeManifest, CancellationToken ct)
    {
        EnsureNodeDirectories(nodeManifest.DatabaseName, nodeManifest.CacheRunId, nodeManifest.NodeAlias);
        EnsureSegmentManifest(nodeManifest, nodeManifest.CurrentSegmentId);

        foreach (var segment in nodeManifest.Segments)
            await SaveSegmentIndexAsync(nodeManifest.DatabaseName, nodeManifest.CacheRunId, nodeManifest.NodeAlias, segment, ct).ConfigureAwait(false);

        var currentSegment = EnsureSegmentManifest(nodeManifest, nodeManifest.CurrentSegmentId);
        var currentPath = GetSegmentPath(nodeManifest.DatabaseName, nodeManifest.CacheRunId, nodeManifest.NodeAlias, currentSegment.FileName);
        Directory.CreateDirectory(Path.GetDirectoryName(currentPath)!);

        await using var stream = new FileStream(
            currentPath,
            FileMode.OpenOrCreate,
            FileAccess.ReadWrite,
            FileShare.Read);

        if (stream.Length != nodeManifest.CurrentSegmentCommittedBytes)
            stream.SetLength(nodeManifest.CurrentSegmentCommittedBytes);

        stream.Flush(true);
    }

    public async Task AppendPageAsync(
        SnapshotCacheNodeManifest nodeManifest,
        IReadOnlyList<SnapshotCacheRow> rows,
        CancellationToken ct)
    {
        if (rows.Count == 0)
            return;

        var payload = SerializeRows(rows);
        var compressedPayload = Compress(payload);

        var currentSegment = EnsureSegmentManifest(nodeManifest, nodeManifest.CurrentSegmentId);
        if (nodeManifest.CurrentSegmentCommittedBytes > 0 &&
            nodeManifest.CurrentSegmentCommittedBytes + compressedPayload.Length > TargetCompressedSegmentBytes)
        {
            nodeManifest.CompletedSegmentCount = Math.Max(nodeManifest.CompletedSegmentCount, nodeManifest.CurrentSegmentId);
            nodeManifest.CurrentSegmentId++;
            nodeManifest.CurrentSegmentCommittedBytes = 0;
            nodeManifest.CurrentSegmentCommittedRows = 0;
            currentSegment = EnsureSegmentManifest(nodeManifest, nodeManifest.CurrentSegmentId);
        }

        var segmentPath = GetSegmentPath(nodeManifest.DatabaseName, nodeManifest.CacheRunId, nodeManifest.NodeAlias, currentSegment.FileName);
        Directory.CreateDirectory(Path.GetDirectoryName(segmentPath)!);

        var pageNumber = currentSegment.Pages.Count + 1;
        var pageOffset = nodeManifest.CurrentSegmentCommittedBytes;
        var firstRowNumber = currentSegment.RowCount + 1;

        await using (var stream = new FileStream(
                         segmentPath,
                         FileMode.OpenOrCreate,
                         FileAccess.ReadWrite,
                         FileShare.Read))
        {
            stream.SetLength(nodeManifest.CurrentSegmentCommittedBytes);
            stream.Position = nodeManifest.CurrentSegmentCommittedBytes;
            await stream.WriteAsync(compressedPayload, ct).ConfigureAwait(false);
            await stream.FlushAsync(ct).ConfigureAwait(false);
            stream.Flush(true);
        }

        var pageEntry = new SnapshotCachePageIndexEntry
        {
            PageNumber = pageNumber,
            CompressedByteOffset = pageOffset,
            CompressedByteLength = compressedPayload.Length,
            FirstRowNumber = firstRowNumber,
            RowCount = rows.Count,
            FirstDocumentId = rows[0].Id,
            LastDocumentId = rows[^1].Id,
            ChecksumSha256 = Convert.ToHexString(SHA256.HashData(payload))
        };

        currentSegment.Pages.Add(pageEntry);
        currentSegment.CompressedBytes += compressedPayload.Length;
        currentSegment.RowCount += rows.Count;

        nodeManifest.DownloadedRows += rows.Count;
        nodeManifest.LastDownloadedDocumentId = rows[^1].Id;
        nodeManifest.CurrentSegmentCommittedBytes += compressedPayload.Length;
        nodeManifest.CurrentSegmentCommittedRows += rows.Count;
        nodeManifest.CompressedBytesWritten += compressedPayload.Length;
        nodeManifest.LastUpdatedAt = DateTimeOffset.UtcNow;

        await SaveSegmentIndexAsync(nodeManifest.DatabaseName, nodeManifest.CacheRunId, nodeManifest.NodeAlias, currentSegment, ct).ConfigureAwait(false);
        await SaveNodeManifestAsync(nodeManifest, ct).ConfigureAwait(false);
    }

    public async Task MarkNodeDownloadCompleteAsync(
        SnapshotCacheNodeManifest nodeManifest,
        CancellationToken ct)
    {
        nodeManifest.IsDownloadComplete = true;
        nodeManifest.DownloadCompletedAt = DateTimeOffset.UtcNow;
        nodeManifest.CompletedSegmentCount = nodeManifest.Segments.Count;
        nodeManifest.LastUpdatedAt = DateTimeOffset.UtcNow;

        await SaveNodeManifestAsync(nodeManifest, ct).ConfigureAwait(false);
    }

    public async Task SaveSetManifestAsync(SnapshotCacheSetManifest manifest, CancellationToken ct)
    {
        manifest.UpdatedAt = DateTimeOffset.UtcNow;
        EnsureSetDirectory(manifest.DatabaseName, manifest.CacheRunId);
        await AtomicWriteJsonAsync(GetSetManifestPath(manifest.DatabaseName, manifest.CacheRunId), manifest, ct).ConfigureAwait(false);
    }

    public async Task SaveNodeManifestAsync(SnapshotCacheNodeManifest manifest, CancellationToken ct)
    {
        manifest.LastUpdatedAt = DateTimeOffset.UtcNow;
        EnsureNodeDirectories(manifest.DatabaseName, manifest.CacheRunId, manifest.NodeAlias);
        await AtomicWriteJsonAsync(GetNodeManifestPath(manifest.DatabaseName, manifest.CacheRunId, manifest.NodeAlias), manifest, ct).ConfigureAwait(false);
    }

    public async IAsyncEnumerable<SnapshotCacheReadPage> ReadPagesAsync(
        SnapshotCacheNodeManifest nodeManifest,
        SnapshotCacheImportCursor startCursor,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct)
    {
        var orderedPages = nodeManifest.Segments
            .OrderBy(segment => segment.SegmentId)
            .SelectMany(segment => segment.Pages.OrderBy(page => page.PageNumber)
                .Select(page => (Segment: segment, Page: page)))
            .ToList();

        for (var index = 0; index < orderedPages.Count; index++)
        {
            ct.ThrowIfCancellationRequested();

            var current = orderedPages[index];
            if (current.Segment.SegmentId < startCursor.SegmentId)
                continue;

            if (current.Segment.SegmentId == startCursor.SegmentId &&
                current.Page.PageNumber < startCursor.PageNumber)
            {
                continue;
            }

            var rows = await ReadPageRowsAsync(nodeManifest, current.Segment, current.Page, ct).ConfigureAwait(false);
            var next = index + 1 < orderedPages.Count ? orderedPages[index + 1] : ((SnapshotCacheSegmentManifest Segment, SnapshotCachePageIndexEntry Page)?)null;

            yield return new SnapshotCacheReadPage(
                current.Segment.SegmentId,
                current.Page.PageNumber,
                rows,
                next?.Segment.SegmentId,
                next?.Page.PageNumber);
        }
    }

    public SnapshotCacheSetManifest BuildSetManifest(
        string databaseName,
        string cacheRunId,
        IEnumerable<SnapshotCacheNodeState> nodeStates)
    {
        return new SnapshotCacheSetManifest
        {
            CacheRunId = cacheRunId,
            DatabaseName = databaseName,
            Nodes = nodeStates
                .OrderBy(state => state.NodeAlias, StringComparer.OrdinalIgnoreCase)
                .Select(state => new SnapshotCacheSetNodeSummary
                {
                    NodeUrl = state.NodeUrl,
                    NodeLabel = state.NodeLabel,
                    NodeAlias = state.NodeAlias,
                    IsDownloadComplete = state.IsDownloadComplete,
                    DownloadCompletedAt = state.DownloadCompletedAt,
                    LastDownloadedDocumentId = state.LastDownloadedDocumentId,
                    DownloadedRows = state.DownloadedRows,
                    CompletedSegmentCount = state.CompletedSegmentCount,
                    CompressedBytesWritten = state.CompressedBytesWritten,
                    CurrentSegmentId = state.CurrentSegmentId,
                    CurrentSegmentCommittedBytes = state.CurrentSegmentCommittedBytes,
                    CurrentSegmentCommittedRows = state.CurrentSegmentCommittedRows,
                    LastUpdatedAt = state.LastUpdatedAt
                })
                .ToList()
        };
    }

    private async Task<IReadOnlyList<SnapshotCacheRow>> ReadPageRowsAsync(
        SnapshotCacheNodeManifest nodeManifest,
        SnapshotCacheSegmentManifest segment,
        SnapshotCachePageIndexEntry page,
        CancellationToken ct)
    {
        var segmentPath = GetSegmentPath(nodeManifest.DatabaseName, nodeManifest.CacheRunId, nodeManifest.NodeAlias, segment.FileName);
        await using var stream = new FileStream(segmentPath, FileMode.Open, FileAccess.Read, FileShare.Read);
        stream.Position = page.CompressedByteOffset;
        var compressed = new byte[page.CompressedByteLength];

        var read = 0;
        while (read < compressed.Length)
        {
            var current = await stream.ReadAsync(compressed.AsMemory(read, compressed.Length - read), ct).ConfigureAwait(false);
            if (current == 0)
                throw new EndOfStreamException($"Unexpected end of segment file '{segmentPath}'.");

            read += current;
        }

        await using var compressedStream = new MemoryStream(compressed, writable: false);
        await using var gzip = new GZipStream(compressedStream, CompressionMode.Decompress);
        using var reader = new StreamReader(gzip, Encoding.UTF8, leaveOpen: false);

        var rows = new List<SnapshotCacheRow>(page.RowCount);
        while (reader.ReadLine() is { } line)
        {
            if (string.IsNullOrWhiteSpace(line))
                continue;

            rows.Add(JsonSerializer.Deserialize<SnapshotCacheRow>(line, RowJsonOptions)
                     ?? throw new InvalidOperationException("Snapshot cache row deserialized to null."));
        }

        return rows;
    }

    private async Task SaveSegmentIndexAsync(
        string databaseName,
        string cacheRunId,
        string nodeAlias,
        SnapshotCacheSegmentManifest segment,
        CancellationToken ct)
        => await AtomicWriteJsonAsync(
                GetSegmentIndexPath(databaseName, cacheRunId, nodeAlias, segment.IndexFileName),
                segment,
                ct)
            .ConfigureAwait(false);

    private static SnapshotCacheSegmentManifest CreateEmptySegment(int segmentId)
    {
        var filePrefix = segmentId.ToString("D6", System.Globalization.CultureInfo.InvariantCulture);
        return new SnapshotCacheSegmentManifest
        {
            SegmentId = segmentId,
            FileName = $"{filePrefix}.jsonl.gz",
            IndexFileName = $"{filePrefix}.index.json"
        };
    }

    private static SnapshotCacheSegmentManifest EnsureSegmentManifest(SnapshotCacheNodeManifest nodeManifest, int segmentId)
    {
        var existing = nodeManifest.Segments.FirstOrDefault(segment => segment.SegmentId == segmentId);
        if (existing != null)
            return existing;

        var created = CreateEmptySegment(segmentId);
        nodeManifest.Segments.Add(created);
        nodeManifest.Segments.Sort((left, right) => left.SegmentId.CompareTo(right.SegmentId));
        return created;
    }

    private static byte[] SerializeRows(IReadOnlyList<SnapshotCacheRow> rows)
    {
        using var stream = new MemoryStream();
        foreach (var row in rows)
        {
            JsonSerializer.Serialize(stream, row, RowJsonOptions);
            stream.WriteByte((byte)'\n');
        }

        return stream.ToArray();
    }

    private static byte[] Compress(byte[] payload)
    {
        using var output = new MemoryStream();
        using (var gzip = new GZipStream(output, CompressionLevel.Optimal, leaveOpen: true))
            gzip.Write(payload, 0, payload.Length);

        return output.ToArray();
    }

    private static async Task<T?> LoadJsonIfExistsAsync<T>(string path, CancellationToken ct)
    {
        if (File.Exists(path) == false)
            return default;

        await using var stream = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read);
        return await JsonSerializer.DeserializeAsync<T>(stream, JsonOptions, ct).ConfigureAwait(false);
    }

    private static async Task AtomicWriteJsonAsync(string targetPath, object value, CancellationToken ct)
    {
        Directory.CreateDirectory(Path.GetDirectoryName(targetPath)!);

        var tempPath = targetPath + ".tmp";
        await using (var stream = new FileStream(tempPath, FileMode.Create, FileAccess.Write, FileShare.None))
        {
            await JsonSerializer.SerializeAsync(stream, value, value.GetType(), JsonOptions, ct).ConfigureAwait(false);
            await stream.FlushAsync(ct).ConfigureAwait(false);
            stream.Flush(true);
        }

        File.Move(tempPath, targetPath, overwrite: true);
    }

    private void EnsureSetDirectory(string databaseName, string cacheRunId)
        => Directory.CreateDirectory(GetSetDirectory(databaseName, cacheRunId));

    private void EnsureNodeDirectories(string databaseName, string cacheRunId, string nodeAlias)
        => Directory.CreateDirectory(GetSegmentsDirectory(databaseName, cacheRunId, nodeAlias));

    private string GetSetManifestPath(string databaseName, string cacheRunId)
        => Path.Combine(GetSetDirectory(databaseName, cacheRunId), "set-manifest.json");

    private string GetNodeDirectory(string databaseName, string cacheRunId, string nodeAlias)
        => Path.Combine(GetSetDirectory(databaseName, cacheRunId), "nodes", nodeAlias);

    private string GetNodeManifestPath(string databaseName, string cacheRunId, string nodeAlias)
        => Path.Combine(GetNodeDirectory(databaseName, cacheRunId, nodeAlias), "node-manifest.json");

    private string GetSegmentsDirectory(string databaseName, string cacheRunId, string nodeAlias)
        => Path.Combine(GetNodeDirectory(databaseName, cacheRunId, nodeAlias), "segments");

    private string GetSegmentPath(string databaseName, string cacheRunId, string nodeAlias, string fileName)
        => Path.Combine(GetSegmentsDirectory(databaseName, cacheRunId, nodeAlias), fileName);

    private string GetSegmentIndexPath(string databaseName, string cacheRunId, string nodeAlias, string fileName)
        => Path.Combine(GetSegmentsDirectory(databaseName, cacheRunId, nodeAlias), fileName);

    private static string ToSafeDirectoryName(string value)
    {
        var invalid = Path.GetInvalidFileNameChars();
        var builder = new StringBuilder(value.Length);
        foreach (var ch in value)
            builder.Append(invalid.Contains(ch) ? '_' : ch);

        return builder.ToString();
    }
}
