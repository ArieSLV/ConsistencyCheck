# RavenDB Consistency Check

A console tool for detecting replication inconsistencies across nodes in a RavenDB cluster.
It iterates over every document on a designated source node and compares each document's
change vector against all other nodes. Any document that is missing or has a diverging
change vector is written to a CSV report.

---

## How it works

1. Fetches document metadata (ID + change vector) from the **source node** in ETag order via raw HTTP — metadata-only, no document bodies are transferred.
2. For each batch, issues a parallel `GetDocumentsCommand` to every **target node** and compares change vectors.
3. Writes `CV_MISMATCH` or `MISSING` records to a CSV file in the `output/` directory.
4. Saves progress after every batch so the scan can be resumed if interrupted.

---

## Prerequisites

No prerequisites — the distributed build is self-contained and includes the .NET runtime.

---

## Quick Start

1. Download and unzip the release package.
2. Run `ConsistencyCheck.exe`.
3. The interactive setup wizard will guide you through configuration on first launch.
4. Results are written to `output/consistency_log_XXXX.csv` next to the executable.

---

## Configuration

There are two ways to configure the tool:

### Option 1 — Interactive wizard

Run the executable without any config file. The wizard will prompt for all required
settings and save them to `output/config.json`. On subsequent runs the saved config
is loaded automatically.

### Option 2 — Pre-filled `config.json`

Create `output/config.json` next to the executable before the first run.
You can pre-fill any subset of fields — the wizard will only prompt for the ones
that are missing or incomplete.

**Full config.json reference:**

```json
{
  "DatabaseName": "MyDatabase",
  "Nodes": [
    { "Label": "Node A", "Url": "https://a.example.ravendb.cloud:443" },
    { "Label": "Node B", "Url": "https://b.example.ravendb.cloud:443" },
    { "Label": "Node C", "Url": "https://c.example.ravendb.cloud:443" }
  ],
  "SourceNodeIndex": 0,
  "CertificatePath": "C:\\path\\to\\client.certificate.pfx",
  "CertificatePassword": "",
  "Mode": "FirstMismatch",
  "Throttle": {
    "PageSize": 128,
    "DelayBetweenBatchesMs": 200,
    "MaxRetries": 3,
    "RetryBaseDelayMs": 500
  }
}
```

| Field | Description |
|-------|-------------|
| `DatabaseName` | Name of the RavenDB database to check. Must exist on every node. |
| `Nodes` | Array of cluster nodes. Requires at least two entries. Each node has a `Label` (display name) and `Url` (full URL with port). |
| `SourceNodeIndex` | Zero-based index of the node used as the iteration source. All other nodes become comparison targets. Set this to the primary node (the one handling writes) to minimise its load. |
| `CertificatePath` | Path to the client certificate `.pfx` / `.p12` file. Set to `""` for open (non-authenticated) clusters. Omit or set to `null` to have the wizard ask for it. |
| `CertificatePassword` | Password for the certificate file. Can be empty if the certificate has no password. |
| `Mode` | `FirstMismatch` — stop at the first detected inconsistency (fast triage). `AllMismatches` — scan the full database and collect every inconsistency. |
| `Throttle.PageSize` | Documents fetched per batch (1–1024, default: 128). |
| `Throttle.DelayBetweenBatchesMs` | Pause between batches in milliseconds (0–60000, default: 200). Increase to reduce cluster load. |
| `Throttle.MaxRetries` | Retry attempts for failed HTTP/SDK calls (1–10, default: 3). |
| `Throttle.RetryBaseDelayMs` | Base delay for exponential-backoff retries in milliseconds (100–10000, default: 500). |

---

## Output

Results are written to `output/consistency_log_XXXX.csv` (auto-numbered, rotates at 100 MB).

| Column | Description |
|--------|-------------|
| `Id` | RavenDB document identifier, e.g. `orders/1-A`. |
| `MismatchType` | `CV_MISMATCH` — change vectors differ between nodes. `MISSING` — document exists on the source but is absent on the target. |
| `SourceNode` | URL of the source (iteration) node. |
| `TargetNode` | URL of the target node where the inconsistency was detected. |
| `SourceCV` | Change vector as seen on the source node. |
| `TargetCV` | Change vector as seen on the target node. Empty for `MISSING` records. |
| `DetectedAt` | UTC timestamp when the inconsistency was recorded. |

---

## Resuming an interrupted scan

Progress is saved to `output/progress.json` after every batch. If the scan is interrupted
(Ctrl+C, crash, etc.), simply run the executable again — it will offer to resume from
where it left off.

---

## Throttling

By default the tool uses a 200 ms pause between batches (`DelayBetweenBatchesMs: 200`)
and a batch size of 128 documents. These values are conservative and suitable for
production clusters.

To reduce cluster impact further: increase `DelayBetweenBatchesMs` or decrease `PageSize`.
To speed up the scan on an idle cluster: set `DelayBetweenBatchesMs: 0` and increase `PageSize` up to 1024.

The tool sends read requests only to **target nodes** during comparison; the source node
receives only the lightweight ETag-ordered iteration requests.
