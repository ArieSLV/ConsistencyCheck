using System.IO.Compression;
using System.Text;
using System.Text.Json;
using Xunit;

namespace ConsistencyCheck.Tests;

public sealed class SnapshotCacheStoreTests : IDisposable
{
    private readonly string _rootDirectory;

    public SnapshotCacheStoreTests()
    {
        _rootDirectory = Path.Combine(
            Environment.CurrentDirectory,
            "test-artifacts",
            nameof(SnapshotCacheStoreTests),
            Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_rootDirectory);
    }

    [Fact]
    public async Task AppendPageAsync_WritesSegmentAndIndexWithCompactRows()
    {
        var store = new SnapshotCacheStore(_rootDirectory);
        var node = new NodeConfig
        {
            Label = "Node A",
            Url = "https://localhost/A"
        };

        var nodeManifest = await store.LoadOrCreateNodeManifestAsync(
            "Customers",
            "runs/20260329-1200000000000",
            node,
            "A",
            CancellationToken.None);

        await store.AppendPageAsync(nodeManifest,
        [
            new SnapshotCacheRow
            {
                Id = "users/1-A",
                ChangeVector = "A:1-db",
                Collection = "Users",
                LastModified = DateTimeOffset.Parse("2026-03-29T08:00:00Z")
            },
            new SnapshotCacheRow
            {
                Id = "users/2-A",
                ChangeVector = "A:2-db",
                Collection = "Users",
                LastModified = DateTimeOffset.Parse("2026-03-29T08:01:00Z")
            }
        ], CancellationToken.None);

        var setDirectory = store.GetSetDirectory("Customers", "runs/20260329-1200000000000");
        var segmentPath = Path.Combine(setDirectory, "nodes", "A", "segments", "000001.jsonl.gz");
        var indexPath = Path.Combine(setDirectory, "nodes", "A", "segments", "000001.index.json");

        Assert.True(File.Exists(segmentPath));
        Assert.True(File.Exists(indexPath));

        var segmentIndex = JsonSerializer.Deserialize<SnapshotCacheSegmentManifest>(
            await File.ReadAllTextAsync(indexPath, CancellationToken.None));
        Assert.NotNull(segmentIndex);
        Assert.Single(segmentIndex!.Pages);
        Assert.Equal(2, segmentIndex.Pages[0].RowCount);
        Assert.Equal("users/1-A", segmentIndex.Pages[0].FirstDocumentId);
        Assert.Equal("users/2-A", segmentIndex.Pages[0].LastDocumentId);

        await using var file = new FileStream(segmentPath, FileMode.Open, FileAccess.Read, FileShare.Read);
        await using var gzip = new GZipStream(file, CompressionMode.Decompress);
        using var reader = new StreamReader(gzip, Encoding.UTF8);
        var payload = await reader.ReadToEndAsync(CancellationToken.None);

        Assert.Contains("\"id\":\"users/1-A\"", payload, StringComparison.Ordinal);
        Assert.Contains("\"cv\":\"A:1-db\"", payload, StringComparison.Ordinal);
        Assert.DoesNotContain("NodeUrl", payload, StringComparison.Ordinal);
        Assert.DoesNotContain("NodeLabel", payload, StringComparison.Ordinal);
        Assert.DoesNotContain("FromNode", payload, StringComparison.Ordinal);
    }

    [Fact]
    public async Task PrepareNodeForResumeAsync_TruncatesCurrentSegmentToCommittedBytes()
    {
        var store = new SnapshotCacheStore(_rootDirectory);
        var node = new NodeConfig
        {
            Label = "Node A",
            Url = "https://localhost/A"
        };

        var nodeManifest = await store.LoadOrCreateNodeManifestAsync(
            "Customers",
            "runs/20260329-1200000000001",
            node,
            "A",
            CancellationToken.None);

        await store.AppendPageAsync(nodeManifest,
        [
            new SnapshotCacheRow
            {
                Id = "users/1-A",
                ChangeVector = "A:1-db",
                Collection = "Users",
                LastModified = DateTimeOffset.Parse("2026-03-29T08:00:00Z")
            }
        ], CancellationToken.None);

        var setDirectory = store.GetSetDirectory("Customers", "runs/20260329-1200000000001");
        var segmentPath = Path.Combine(setDirectory, "nodes", "A", "segments", "000001.jsonl.gz");
        var committedLength = new FileInfo(segmentPath).Length;

        await File.AppendAllTextAsync(segmentPath, "partial-tail", CancellationToken.None);
        Assert.True(new FileInfo(segmentPath).Length > committedLength);

        await store.PrepareNodeForResumeAsync(nodeManifest, CancellationToken.None);

        Assert.Equal(committedLength, new FileInfo(segmentPath).Length);
    }

    [Fact]
    public async Task ReadPagesAsync_ReturnsPagesInOrder()
    {
        var store = new SnapshotCacheStore(_rootDirectory);
        var node = new NodeConfig
        {
            Label = "Node A",
            Url = "https://localhost/A"
        };

        var nodeManifest = await store.LoadOrCreateNodeManifestAsync(
            "Customers",
            "runs/20260329-1200000000002",
            node,
            "A",
            CancellationToken.None);

        await store.AppendPageAsync(nodeManifest,
        [
            new SnapshotCacheRow { Id = "users/1-A", ChangeVector = "A:1-db" },
            new SnapshotCacheRow { Id = "users/2-A", ChangeVector = "A:2-db" }
        ], CancellationToken.None);

        await store.AppendPageAsync(nodeManifest,
        [
            new SnapshotCacheRow { Id = "users/3-A", ChangeVector = "A:3-db" }
        ], CancellationToken.None);

        var pages = new List<SnapshotCacheReadPage>();
        await foreach (var page in store.ReadPagesAsync(nodeManifest, SnapshotCacheImportCursor.Start, CancellationToken.None))
            pages.Add(page);

        Assert.Equal(2, pages.Count);
        Assert.Equal(new[] { "users/1-A", "users/2-A" }, pages[0].Rows.Select(row => row.Id).ToArray());
        Assert.Equal(new[] { "users/3-A" }, pages[1].Rows.Select(row => row.Id).ToArray());
    }

    public void Dispose()
    {
        if (Directory.Exists(_rootDirectory))
            Directory.Delete(_rootDirectory, recursive: true);
    }
}
