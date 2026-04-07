using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace ConsistencyCheck;

/// <summary>
/// Provides atomic load and save operations for <see cref="AppConfig"/> and
/// <see cref="ProgressState"/> using JSON serialisation.
/// </summary>
/// <remarks>
/// <para>
/// All writes use a <em>write-to-temp-then-rename</em> pattern:
/// data is serialised to a <c>.tmp</c> file first, then
/// <see cref="File.Move(string, string, bool)"/> atomically replaces the target file.
/// On Windows this calls <c>MoveFileExW</c> with <c>MOVEFILE_REPLACE_EXISTING</c>;
/// on POSIX systems it calls <c>rename(2)</c>. Both operations are atomic for
/// same-volume moves, guaranteeing that a crash never leaves a partially-written file.
/// </para>
/// </remarks>
public sealed class ProgressStore
{
    // ── JSON serialiser options shared across all operations ──────────────────

    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        WriteIndented = true,
        Converters = { new JsonStringEnumConverter() },
        PropertyNameCaseInsensitive = true
    };

    // ── File paths ────────────────────────────────────────────────────────────

    private readonly string _configPath;
    private readonly string _progressPath;

    // ─────────────────────────────────────────────────────────────────────────
    // Construction
    // ─────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// Initialises a <see cref="ProgressStore"/> that persists files inside
    /// <paramref name="outputDirectory"/>. The directory is created if it does not
    /// already exist.
    /// </summary>
    /// <param name="outputDirectory">
    /// Directory where <c>config.json</c> and <c>progress.json</c> are stored.
    /// </param>
    public ProgressStore(string outputDirectory)
    {
        Directory.CreateDirectory(outputDirectory);
        _configPath   = Path.Combine(outputDirectory, "config.json");
        _progressPath = Path.Combine(outputDirectory, "progress.json");
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Public properties
    // ─────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// <c>true</c> when <c>config.json</c> exists in the output directory.
    /// Use this to decide whether to run the setup wizard or load the saved config.
    /// </summary>
    public bool ConfigExists => File.Exists(_configPath);

    /// <summary>
    /// <c>true</c> when <c>progress.json</c> exists in the output directory.
    /// Use this to decide whether to offer the user a resume prompt.
    /// </summary>
    public bool ProgressExists => File.Exists(_progressPath);

    // ─────────────────────────────────────────────────────────────────────────
    // Config persistence
    // ─────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// Loads and deserialises <c>config.json</c>.
    /// </summary>
    /// <returns>The deserialised <see cref="AppConfig"/>.</returns>
    /// <exception cref="InvalidOperationException">
    /// Thrown when the file is missing, malformed, or fails basic validation.
    /// </exception>
    public AppConfig LoadConfig()
    {
        if (!File.Exists(_configPath))
            throw new InvalidOperationException(
                $"Configuration file not found: {_configPath}");

        var json = File.ReadAllText(_configPath, Encoding.UTF8);

        AppConfig config;
        try
        {
            config = JsonSerializer.Deserialize<AppConfig>(json, JsonOptions)
                     ?? throw new InvalidOperationException("Deserialised config was null.");
        }
        catch (JsonException ex)
        {
            throw new InvalidOperationException(
                $"Configuration file is malformed: {ex.Message}", ex);
        }

        foreach (var node in config.Nodes)
            node.Url = node.Url.TrimEnd('/');

        config.MissingProperties = DetectMissingProperties(json);
        config.CertificateThumbprint = string.IsNullOrWhiteSpace(config.CertificateThumbprint)
            ? null
            : config.CertificateThumbprint.Trim();

        config.Throttle ??= new ThrottleConfig();
        config.StateStore ??= new StateStoreConfig();

        if (config.SourceNodeIndex < 0 || config.SourceNodeIndex >= config.Nodes.Count)
            config.SourceNodeIndex = 0;

        ValidateConfig(config);
        return config;
    }

    /// <summary>
    /// Atomically saves <paramref name="config"/> to <c>config.json</c>.
    /// </summary>
    /// <param name="config">The configuration to persist.</param>
    public void SaveConfig(AppConfig config)
    {
        ValidateConfig(config);
        config.MissingProperties.Clear();
        AtomicWrite(_configPath, config);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Progress persistence
    // ─────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// Loads and deserialises <c>progress.json</c>.
    /// Returns a fresh <see cref="ProgressState"/> (starting from the beginning)
    /// if the file does not exist.
    /// </summary>
    /// <returns>The current or a new <see cref="ProgressState"/>.</returns>
    public ProgressState LoadProgress()
    {
        if (!File.Exists(_progressPath))
            return new ProgressState();

        var json = File.ReadAllText(_progressPath, Encoding.UTF8);

        try
        {
            return JsonSerializer.Deserialize<ProgressState>(json, JsonOptions)
                   ?? new ProgressState();
        }
        catch (JsonException)
        {
            // Corrupted progress file — start fresh rather than crashing.
            return new ProgressState();
        }
    }

    /// <summary>
    /// Atomically saves <paramref name="state"/> to <c>progress.json</c>.
    /// Called after every successfully processed batch.
    /// </summary>
    /// <param name="state">The current progress state to persist.</param>
    public void SaveProgress(ProgressState state)
    {
        state.LastSavedAt = DateTimeOffset.UtcNow;
        AtomicWrite(_progressPath, state);
    }

    /// <summary>
    /// Deletes <c>progress.json</c> to prepare for a fresh scan.
    /// Does nothing if the file does not exist.
    /// </summary>
    public void DeleteProgress()
    {
        if (File.Exists(_progressPath))
            File.Delete(_progressPath);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Private helpers
    // ─────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// Serialises <paramref name="value"/> to a temporary file next to
    /// <paramref name="targetPath"/>, then renames it over the target in one
    /// atomic OS operation.
    /// </summary>
    private static void AtomicWrite(string targetPath, object value)
    {
        var tmpPath = targetPath + ".tmp";
        var json    = JsonSerializer.Serialize(value, JsonOptions);

        File.WriteAllText(tmpPath, json, Encoding.UTF8);

        // File.Move with overwrite:true is atomic for same-volume operations
        // on both Windows (MoveFileExW + MOVEFILE_REPLACE_EXISTING) and POSIX (rename(2)).
        File.Move(tmpPath, targetPath, overwrite: true);
    }

    /// <summary>
    /// Validates the supplied <see cref="AppConfig"/> and throws
    /// <see cref="InvalidOperationException"/> for any rule violation.
    /// </summary>
    private static void ValidateConfig(AppConfig config)
    {
        if (string.IsNullOrWhiteSpace(config.DatabaseName))
            throw new InvalidOperationException("DatabaseName must not be empty.");

        if (config.Nodes == null || config.Nodes.Count < 2)
            throw new InvalidOperationException(
                "At least two nodes must be configured (one source and one target).");

        foreach (var node in config.Nodes)
        {
            if (string.IsNullOrWhiteSpace(node.Url))
                throw new InvalidOperationException(
                    $"Node '{node.Label}' has an empty URL.");

            if (!Uri.TryCreate(node.Url, UriKind.Absolute, out var uri) ||
                (uri.Scheme != "http" && uri.Scheme != "https"))
                throw new InvalidOperationException(
                    $"Node '{node.Label}' URL is not a valid http/https URI: {node.Url}");
        }

        if (!string.IsNullOrEmpty(config.CertificatePath) &&
            !File.Exists(config.CertificatePath))
            throw new InvalidOperationException(
                $"Certificate file not found: {config.CertificatePath}");

        if (config.CertificateThumbprint != null &&
            string.IsNullOrWhiteSpace(config.CertificateThumbprint))
            throw new InvalidOperationException("CertificateThumbprint must not be empty when specified.");

        if (config.StartEtag is < 0)
            throw new InvalidOperationException("StartEtag must be >= 0.");

        if (config.Throttle.PageSize is < 1 or > 1024)
            throw new InvalidOperationException("Throttle.PageSize must be between 1 and 1024.");

        if (config.Throttle.DelayBetweenBatchesMs is < 0 or > 60_000)
            throw new InvalidOperationException(
                "Throttle.DelayBetweenBatchesMs must be between 0 and 60000.");

        if (config.Throttle.MaxRetries is < 1 or > 10)
            throw new InvalidOperationException("Throttle.MaxRetries must be between 1 and 10.");

        if (config.Throttle.RetryBaseDelayMs is < 100 or > 10_000)
            throw new InvalidOperationException(
                "Throttle.RetryBaseDelayMs must be between 100 and 10000.");

        if (config.Throttle.ClusterLookupBatchSize < 1)
            throw new InvalidOperationException(
                "Throttle.ClusterLookupBatchSize must be >= 1.");

        if (string.IsNullOrWhiteSpace(config.StateStore.ServerUrl))
            throw new InvalidOperationException("StateStore.ServerUrl must not be empty.");

        if (!Uri.TryCreate(config.StateStore.ServerUrl, UriKind.Absolute, out var stateStoreUri) ||
            (stateStoreUri.Scheme != "http" && stateStoreUri.Scheme != "https"))
            throw new InvalidOperationException(
                $"StateStore.ServerUrl is not a valid http/https URI: {config.StateStore.ServerUrl}");

        if (string.IsNullOrWhiteSpace(config.StateStore.DatabaseName))
            throw new InvalidOperationException("StateStore.DatabaseName must not be empty.");
    }

    private static HashSet<string> DetectMissingProperties(string json)
    {
        using var document = JsonDocument.Parse(json);
        if (document.RootElement.ValueKind != JsonValueKind.Object)
            return new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        var missing = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var root = document.RootElement;
        var rootProperties = GetPropertyNames(root);

        if (!rootProperties.Contains(nameof(AppConfig.RunMode)))
            missing.Add(nameof(AppConfig.RunMode));

        if (!rootProperties.Contains(nameof(AppConfig.Mode)))
            missing.Add(nameof(AppConfig.Mode));

        if (!rootProperties.Contains(nameof(AppConfig.AllowInvalidServerCertificates)))
            missing.Add(nameof(AppConfig.AllowInvalidServerCertificates));

        var hasCertificatePath = rootProperties.Contains(nameof(AppConfig.CertificatePath));
        var hasCertificateThumbprint = rootProperties.Contains(nameof(AppConfig.CertificateThumbprint));
        if (!hasCertificatePath && !hasCertificateThumbprint)
        {
            missing.Add("Certificate");
        }
        else if (hasCertificatePath &&
                 TryGetProperty(root, nameof(AppConfig.CertificatePath), out var certificatePathElement) &&
                 certificatePathElement.ValueKind == JsonValueKind.String &&
                 string.IsNullOrEmpty(certificatePathElement.GetString()) == false &&
                 !rootProperties.Contains(nameof(AppConfig.CertificatePassword)))
        {
            missing.Add("Certificate");
        }

        if (!HasAllProperties(root, nameof(AppConfig.Throttle),
                nameof(ThrottleConfig.PageSize),
                nameof(ThrottleConfig.DelayBetweenBatchesMs),
                nameof(ThrottleConfig.MaxRetries),
                nameof(ThrottleConfig.RetryBaseDelayMs),
                nameof(ThrottleConfig.ClusterLookupBatchSize)))
        {
            missing.Add(nameof(AppConfig.Throttle));
        }

        if (!HasAllProperties(root, nameof(AppConfig.StateStore),
                nameof(StateStoreConfig.ServerUrl),
                nameof(StateStoreConfig.DatabaseName),
                nameof(StateStoreConfig.EnableDiagnostics)))
        {
            missing.Add(nameof(AppConfig.StateStore));
        }

        return missing;
    }

    private static bool HasAllProperties(JsonElement root, string objectPropertyName, params string[] requiredProperties)
    {
        if (!TryGetProperty(root, objectPropertyName, out var objectElement) ||
            objectElement.ValueKind != JsonValueKind.Object)
        {
            return false;
        }

        var properties = GetPropertyNames(objectElement);
        return requiredProperties.All(properties.Contains);
    }

    private static HashSet<string> GetPropertyNames(JsonElement element)
    {
        var properties = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var property in element.EnumerateObject())
            properties.Add(property.Name);

        return properties;
    }

    private static bool TryGetProperty(JsonElement element, string propertyName, out JsonElement value)
    {
        foreach (var property in element.EnumerateObject())
        {
            if (string.Equals(property.Name, propertyName, StringComparison.OrdinalIgnoreCase))
            {
                value = property.Value;
                return true;
            }
        }

        value = default;
        return false;
    }
}
