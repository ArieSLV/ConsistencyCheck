using Raven.Client.Documents;
using Raven.Client.Documents.Operations.Attachments;

namespace ConsistencyCheck;

/// <summary>
/// Exact dedup store backed by RavenDB documents with binary attachments.
/// </summary>
public sealed class DedupBucketStore : IAsyncDisposable
{
    private const string AttachmentName = "hashes.bin";
    private const int HashBytes = 32;
    private const int MaxCachedBuckets = 256;

    private readonly IDocumentStore _store;
    private readonly Dictionary<string, CachedBucket> _cache = new(StringComparer.OrdinalIgnoreCase);
    private readonly LinkedList<string> _lru = new();
    private readonly Dictionary<string, LinkedListNode<string>> _lruNodes = new(StringComparer.OrdinalIgnoreCase);
    private readonly SemaphoreSlim _gate = new(1, 1);

    public DedupBucketStore(IDocumentStore store)
    {
        _store = store;
    }

    public async Task<HashSet<string>> GetKnownHashesAsync(
        string runId,
        IReadOnlyCollection<VersionKey> keys,
        CancellationToken ct)
    {
        var known = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        if (keys.Count == 0)
            return known;

        var byPrefix = keys.GroupBy(k => k.Prefix, StringComparer.OrdinalIgnoreCase);

        await _gate.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            foreach (var group in byPrefix)
            {
                var bucket = await GetBucketAsync(runId, group.Key, ct).ConfigureAwait(false);
                foreach (var key in group)
                {
                    if (bucket.Hashes.Contains(key.HashHex))
                        known.Add(key.HashHex);
                }
            }
        }
        finally
        {
            _gate.Release();
        }

        return known;
    }

    public async Task MarkProcessedAsync(
        string runId,
        IReadOnlyCollection<VersionKey> keys,
        CancellationToken ct)
    {
        if (keys.Count == 0)
            return;

        List<CachedBucket> dirtyBuckets = [];

        await _gate.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            foreach (var group in keys.GroupBy(k => k.Prefix, StringComparer.OrdinalIgnoreCase))
            {
                var bucket = await GetBucketAsync(runId, group.Key, ct).ConfigureAwait(false);
                var changed = false;
                foreach (var key in group)
                {
                    changed |= bucket.Hashes.Add(key.HashHex);
                }

                if (changed)
                {
                    bucket.Dirty = true;
                    bucket.Document.EntryCount = bucket.Hashes.Count;
                    bucket.Document.LastUpdatedAt = DateTimeOffset.UtcNow;
                    dirtyBuckets.Add(bucket);
                }
            }
        }
        finally
        {
            _gate.Release();
        }

        if (dirtyBuckets.Count > 0)
            await FlushAsync(dirtyBuckets, ct).ConfigureAwait(false);
    }

    public async ValueTask DisposeAsync()
    {
        await _gate.WaitAsync().ConfigureAwait(false);
        try
        {
            var dirty = _cache.Values.Where(x => x.Dirty).ToList();
            if (dirty.Count > 0)
                await FlushAsync(dirty, CancellationToken.None).ConfigureAwait(false);
        }
        finally
        {
            _gate.Release();
            _gate.Dispose();
        }
    }

    private async Task<CachedBucket> GetBucketAsync(string runId, string prefix, CancellationToken ct)
    {
        var cacheKey = $"{runId}|{prefix}";
        if (_cache.TryGetValue(cacheKey, out var cached))
        {
            Touch(cacheKey);
            return cached;
        }

        var docId = GetBucketId(runId, prefix);
        DedupBucketDocument? doc;
        byte[] bytes;

        using (var session = _store.OpenAsyncSession())
        {
            doc = await session.LoadAsync<DedupBucketDocument>(docId, ct).ConfigureAwait(false);

            bytes = [];
            if (doc != null)
            {
                using var attachment = await session.Advanced.Attachments.GetAsync(docId, AttachmentName, ct)
                    .ConfigureAwait(false);
                if (attachment != null)
                    bytes = await ReadAllBytesAsync(attachment, ct).ConfigureAwait(false);
            }
        }

        doc ??= new DedupBucketDocument
        {
            Id = docId,
            RunId = runId,
            Prefix = prefix,
            AttachmentName = AttachmentName
        };

        cached = new CachedBucket(doc, DeserializeHashes(bytes));
        _cache[cacheKey] = cached;
        Touch(cacheKey);
        EvictIfNeeded();
        return cached;
    }

    private async Task FlushAsync(IReadOnlyCollection<CachedBucket> dirtyBuckets, CancellationToken ct)
    {
        using var session = _store.OpenAsyncSession();
        var streams = new List<MemoryStream>(dirtyBuckets.Count);
        var bucketById = dirtyBuckets.ToDictionary(bucket => bucket.Document.Id, StringComparer.OrdinalIgnoreCase);
        var existingById = await session.LoadAsync<DedupBucketDocument>(bucketById.Keys.ToArray(), ct)
            .ConfigureAwait(false);

        try
        {
            foreach (var bucket in dirtyBuckets)
            {
                existingById.TryGetValue(bucket.Document.Id, out var existing);
                var document = existing ?? bucket.Document;
                document.RunId = bucket.Document.RunId;
                document.Prefix = bucket.Document.Prefix;
                document.EntryCount = bucket.Hashes.Count;
                document.LastUpdatedAt = DateTimeOffset.UtcNow;
                document.AttachmentName = AttachmentName;

                await session.StoreAsync(document, document.Id, ct).ConfigureAwait(false);

                var stream = new MemoryStream(SerializeHashes(bucket.Hashes), writable: false);
                streams.Add(stream);
                session.Advanced.Attachments.Store(document.Id, AttachmentName, stream, "application/octet-stream");
            }

            await session.SaveChangesAsync(ct).ConfigureAwait(false);

            foreach (var bucket in dirtyBuckets)
                bucket.Dirty = false;
        }
        finally
        {
            foreach (var stream in streams)
                await stream.DisposeAsync().ConfigureAwait(false);
        }
    }

    private static async Task<byte[]> ReadAllBytesAsync(AttachmentResult attachment, CancellationToken ct)
    {
        await using var memory = new MemoryStream();
        await attachment.Stream.CopyToAsync(memory, ct).ConfigureAwait(false);
        return memory.ToArray();
    }

    private static HashSet<string> DeserializeHashes(byte[] bytes)
    {
        var hashes = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        if (bytes.Length == 0)
            return hashes;

        for (var i = 0; i + HashBytes <= bytes.Length; i += HashBytes)
            hashes.Add(Convert.ToHexString(bytes, i, HashBytes));

        return hashes;
    }

    private static byte[] SerializeHashes(HashSet<string> hashes)
    {
        var ordered = hashes.OrderBy(x => x, StringComparer.OrdinalIgnoreCase).ToArray();
        var bytes = new byte[ordered.Length * HashBytes];

        for (var i = 0; i < ordered.Length; i++)
        {
            var slice = Convert.FromHexString(ordered[i]);
            Buffer.BlockCopy(slice, 0, bytes, i * HashBytes, HashBytes);
        }

        return bytes;
    }

    private static string GetBucketId(string runId, string prefix) => $"dedup-buckets/{runId}/{prefix}";

    private void Touch(string cacheKey)
    {
        if (_lruNodes.TryGetValue(cacheKey, out var existing))
            _lru.Remove(existing);

        var node = _lru.AddFirst(cacheKey);
        _lruNodes[cacheKey] = node;
    }

    private void EvictIfNeeded()
    {
        while (_cache.Count > MaxCachedBuckets && _lru.Last != null)
        {
            var key = _lru.Last.Value;
            var bucket = _cache[key];
            if (bucket.Dirty)
                return;

            _lru.RemoveLast();
            _lruNodes.Remove(key);
            _cache.Remove(key);
        }
    }

    private sealed class CachedBucket
    {
        public CachedBucket(DedupBucketDocument document, HashSet<string> hashes)
        {
            Document = document;
            Hashes = hashes;
        }

        public DedupBucketDocument Document { get; }
        public HashSet<string> Hashes { get; }
        public bool Dirty { get; set; }
    }
}