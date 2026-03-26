using System.Text;

namespace ConsistencyCheck;

/// <summary>
/// Lightweight append-only diagnostics writer for troubleshooting scan runs.
/// </summary>
public sealed class DiagnosticsWriter : IAsyncDisposable
{
    private readonly SemaphoreSlim _lock = new(1, 1);
    private readonly StreamWriter _writer;

    public string FilePath { get; }

    public DiagnosticsWriter(string filePath)
    {
        FilePath = filePath;
        Directory.CreateDirectory(Path.GetDirectoryName(filePath)!);

        _writer = new StreamWriter(
            new FileStream(filePath, FileMode.Create, FileAccess.Write, FileShare.ReadWrite),
            Encoding.UTF8)
        {
            AutoFlush = true
        };
    }

    public async Task WriteLineAsync(string message, CancellationToken ct = default)
    {
        await _lock.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            var line = $"[{DateTimeOffset.UtcNow:O}] {message}";
            await _writer.WriteLineAsync(line.AsMemory(), ct).ConfigureAwait(false);
        }
        finally
        {
            _lock.Release();
        }
    }

    public async ValueTask DisposeAsync()
    {
        await _lock.WaitAsync().ConfigureAwait(false);
        try
        {
            await _writer.FlushAsync().ConfigureAwait(false);
            await _writer.DisposeAsync().ConfigureAwait(false);
        }
        finally
        {
            _lock.Release();
            _lock.Dispose();
        }
    }
}
