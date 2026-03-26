using System.Text;

namespace ConsistencyCheck;

/// <summary>
/// Thread-safe, append-only CSV writer for <see cref="MismatchRecord"/> instances.
/// </summary>
/// <remarks>
/// <para>
/// <strong>File rotation:</strong> when the current output file reaches
/// <see cref="RotationSizeBytes"/> (100 MB), the writer closes it and opens the next
/// file in sequence: <c>mismatches_001.csv</c>, <c>mismatches_002.csv</c>, etc.
/// </para>
/// <para>
/// <strong>Resume safety:</strong> on construction the writer scans the output directory
/// for existing files and continues from the last one, so a resumed run appends to the
/// same files rather than creating duplicates with redundant headers.
/// </para>
/// <para>
/// If the latest file is temporarily locked by another process (for example, opened in
/// Excel), the writer automatically advances to the next numbered file instead of
/// aborting the scan.
/// </para>
/// <para>
/// <strong>Thread safety:</strong> a <see cref="SemaphoreSlim"/> serialises all writes.
/// The parallel fan-out in <see cref="ConsistencyChecker"/> may call
/// <see cref="WriteAsync"/> from multiple concurrent tasks simultaneously.
/// </para>
/// </remarks>
public sealed class ResultsWriter : IAsyncDisposable
{
    // ── Constants ─────────────────────────────────────────────────────────────

    /// <summary>Maximum size of a single CSV file before rotation. Default: 100 MB.</summary>
    public const long RotationSizeBytes = 100L * 1024 * 1024;

    private const string FilePattern = "mismatches_{0:D3}.csv";

    private static readonly string CsvHeader =
        "Id,MismatchType,SourceNode,TargetNode,SourceCV,TargetCV,SourceLastModified,TargetLastModified,DetectedAt";

    // ── State ─────────────────────────────────────────────────────────────────

    private readonly string _outputDirectory;
    private readonly SemaphoreSlim _lock = new(1, 1);

    private StreamWriter? _writer;
    private int _fileIndex;
    private long _currentFileSize;

    // ─────────────────────────────────────────────────────────────────────────
    // Construction
    // ─────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// Initialises a <see cref="ResultsWriter"/> for the given directory.
    /// Scans for existing <c>mismatches_NNN.csv</c> files to determine the starting
    /// file index so that a resumed run appends to the correct file.
    /// </summary>
    /// <param name="outputDirectory">Directory where CSV files will be written.</param>
    public ResultsWriter(string outputDirectory)
    {
        _outputDirectory = outputDirectory;
        _fileIndex       = DetectStartingFileIndex(outputDirectory);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Public API
    // ─────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// Asynchronously appends a <see cref="MismatchRecord"/> to the current CSV file.
    /// Rotates to a new file if the size threshold is reached before writing.
    /// This method is safe to call concurrently from multiple tasks.
    /// </summary>
    /// <param name="record">The mismatch record to persist.</param>
    /// <param name="ct">Cancellation token.</param>
    public async Task WriteAsync(MismatchRecord record, CancellationToken ct)
    {
        await _lock.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            await EnsureWriterOpenAsync(ct).ConfigureAwait(false);
            await RotateIfNeededAsync(ct).ConfigureAwait(false);

            var line = FormatCsvRow(record);
            await _writer!.WriteLineAsync(line.AsMemory(), ct).ConfigureAwait(false);
            await _writer.FlushAsync(ct).ConfigureAwait(false);

            _currentFileSize += Encoding.UTF8.GetByteCount(line) + Environment.NewLine.Length;
        }
        finally
        {
            _lock.Release();
        }
    }

    /// <summary>
    /// Flushes and closes the current CSV file.
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        await _lock.WaitAsync().ConfigureAwait(false);
        try
        {
            if (_writer != null)
            {
                await _writer.FlushAsync().ConfigureAwait(false);
                await _writer.DisposeAsync().ConfigureAwait(false);
                _writer = null;
            }
        }
        finally
        {
            _lock.Release();
            _lock.Dispose();
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Private helpers
    // ─────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// Opens the current output file for writing (appending if it already exists).
    /// Writes the CSV header if the file is new or empty.
    /// Caller must hold <see cref="_lock"/>.
    /// </summary>
    private async Task EnsureWriterOpenAsync(CancellationToken ct)
    {
        if (_writer != null)
            return;

        while (true)
        {
            var path      = GetFilePath(_fileIndex);
            var fileExists = File.Exists(path);
            var fileSize  = fileExists ? new FileInfo(path).Length : 0L;

            try
            {
                _writer          = OpenAppendWriter(path);
                _currentFileSize = fileSize;

                if (fileSize == 0)
                {
                    // New file — write the header row first.
                    await _writer.WriteLineAsync(CsvHeader.AsMemory(), ct).ConfigureAwait(false);
                    await _writer.FlushAsync(ct).ConfigureAwait(false);
                    _currentFileSize += Encoding.UTF8.GetByteCount(CsvHeader) + Environment.NewLine.Length;
                }

                return;
            }
            catch (IOException ex) when (IsFileLocked(ex))
            {
                // Someone else is holding the current file. Keep the scan running by
                // switching to the next numbered file.
                _fileIndex++;
            }
        }
    }

    /// <summary>
    /// Rotates to the next file if <see cref="_currentFileSize"/> has reached
    /// <see cref="RotationSizeBytes"/>. Caller must hold <see cref="_lock"/>.
    /// </summary>
    private async Task RotateIfNeededAsync(CancellationToken ct)
    {
        if (_writer == null || _currentFileSize < RotationSizeBytes)
            return;

        await _writer.FlushAsync(ct).ConfigureAwait(false);
        await _writer.DisposeAsync().ConfigureAwait(false);
        _writer = null;

        _fileIndex++;

        var newPath = GetFilePath(_fileIndex);
        _writer          = OpenCreateWriter(newPath);
        _currentFileSize = 0L;

        // Write header to the freshly rotated file.
        await _writer.WriteLineAsync(CsvHeader.AsMemory(), ct).ConfigureAwait(false);
        await _writer.FlushAsync(ct).ConfigureAwait(false);
        _currentFileSize += Encoding.UTF8.GetByteCount(CsvHeader) + Environment.NewLine.Length;
    }

    /// <summary>
    /// Formats a <see cref="MismatchRecord"/> as an RFC 4180-compliant CSV row.
    /// Fields that contain commas, double-quotes, or line breaks are wrapped in
    /// double-quotes with embedded quotes doubled.
    /// </summary>
    private static string FormatCsvRow(MismatchRecord r) =>
        string.Join(",",
            EscapeCsvField(r.Id),
            EscapeCsvField(r.MismatchType),
            EscapeCsvField(r.SourceNode),
            EscapeCsvField(r.TargetNode),
            EscapeCsvField(r.SourceCV),
            EscapeCsvField(r.TargetCV),
            EscapeCsvField(r.SourceLastModified),
            EscapeCsvField(r.TargetLastModified),
            EscapeCsvField(r.DetectedAt.ToString("o")));

    /// <summary>
    /// Escapes a single CSV field according to RFC 4180.
    /// </summary>
    private static string EscapeCsvField(string field)
    {
        if (string.IsNullOrEmpty(field))
            return string.Empty;

        // Quoting is required when the field contains a comma, double-quote, or newline.
        if (field.Contains(',') || field.Contains('"') ||
            field.Contains('\r') || field.Contains('\n'))
        {
            return $"\"{field.Replace("\"", "\"\"")}\"";
        }

        return field;
    }

    /// <summary>Returns the full path for the CSV file at the given 1-based index.</summary>
    private string GetFilePath(int index) =>
        Path.Combine(_outputDirectory, string.Format(FilePattern, index));

    private static StreamWriter OpenAppendWriter(string path) =>
        new(new FileStream(path, FileMode.Append, FileAccess.Write, FileShare.ReadWrite), Encoding.UTF8);

    private static StreamWriter OpenCreateWriter(string path) =>
        new(new FileStream(path, FileMode.Create, FileAccess.Write, FileShare.ReadWrite), Encoding.UTF8);

    private static bool IsFileLocked(IOException ex)
        => ex.HResult is unchecked((int)0x80070020) or unchecked((int)0x80070021);

    /// <summary>
    /// Scans <paramref name="directory"/> for existing <c>mismatches_NNN.csv</c> files
    /// and returns the index of the last one so that the writer can continue from where
    /// a previous run left off. Returns 1 if no files are found.
    /// </summary>
    private static int DetectStartingFileIndex(string directory)
    {
        if (!Directory.Exists(directory))
            return 1;

        var files = Directory.GetFiles(directory, "mismatches_???.csv");
        if (files.Length == 0)
            return 1;

        var maxIndex = 1;
        foreach (var file in files)
        {
            var name = Path.GetFileNameWithoutExtension(file); // "mismatches_001"
            var parts = name.Split('_');
            if (parts.Length > 0 && int.TryParse(parts[^1], out var idx))
                maxIndex = Math.Max(maxIndex, idx);
        }

        return maxIndex;
    }
}
