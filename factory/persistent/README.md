# Persistent Factory

A generic persistent factory for buildomat that invokes external commands to
execute workloads on persistent infrastructure. Unlike VM-based factories that
spin up ephemeral instances per job, this factory runs on a long-lived host and
delegates job execution to a configurable command, keeping all domain-specific
knowledge out of the factory itself.

## Use Cases

This factory is suitable for any environment where jobs run on infrastructure
that persists across jobs rather than being provisioned per job:

- **Hardware CI** — test rigs with USB probes, JTAG adapters, or other physical
  connections (e.g., Gimlet, SP, sidecar boards)
- **Dedicated servers** — bare-metal machines that can't run ephemeral VMs
- **Lab instruments** — shared equipment requiring host-local access (FPGA
  boards, network switches, power controllers)
- **Platform-specific builds** — macOS or other hosts where ephemeral VM
  tooling is unavailable or impractical
- **GPU/accelerator rigs** — machines with specialized hardware passed through
  to a single job at a time

The factory is agnostic to the workload. All domain-specific logic lives in the
external command, so the same factory binary serves any of the above by changing
the configuration.

## Configuration

The factory reads a TOML configuration file specified with `-f`:

```toml
[general]
# Base URL of the buildomat server.
baseurl = "http://buildomat-server:9979"

[factory]
# Bearer token for factory API authentication.
token = "FACTORY_TOKEN"

[execution]
# External command to invoke for each job.
command = "/usr/local/bin/sp-runner"
# Arguments passed to the command (optional).
args = ["ci-job"]
# Base directory for per-job subdirectories. Each job gets its own
# directory tree: {job_dir}/{job_id}/{work,artifacts,output}/
job_dir = "/var/lib/buildomat-factory/jobs"

# Each [target.*] section declares a target this factory can accept.
# The factory advertises these to the server when requesting leases.
[target.hwci-grapefruit]
[target.hwci-gimlet]
```

## External Command Interface

### Environment Variables

The factory sets these environment variables before invoking the command:

| Variable | Description |
|----------|-------------|
| `BUILDOMAT_JOB_ID` | Unique job identifier |
| `BUILDOMAT_TARGET` | Target name from the job (matches a `[target.*]` key) |
| `BUILDOMAT_WORKER_ID` | Worker identifier for this job |
| `BUILDOMAT_WORK_DIR` | Working directory (also the process cwd) |
| `BUILDOMAT_ARTIFACT_DIR` | Input artifacts location |
| `BUILDOMAT_OUTPUT_DIR` | Where to place output files for upload |

### Directory Layout

```
{job_dir}/{job_id}/
  work/        # cwd for the command
  artifacts/   # input artifacts (read-only by convention)
  output/      # output files to upload
```

### Exit Code

- **0** — job succeeded
- **non-zero** — job failed (exit code is logged, `worker_job_complete` called
  with `failed=true`)
- **signal** — treated as failure (e.g., killed by OOM or timeout)

## stdout/stderr Streaming

The factory captures stdout and stderr from the external command and streams
them to the server as events via `worker_job_append`. Lines are read
asynchronously and batched (up to 100 events per call). Both streams drain
completely before the factory proceeds to output upload and job completion.

Each event includes:
- `stream` — `"stdout"` or `"stderr"`
- `payload` — the line content (no trailing newline)
- `time` — UTC timestamp

## Input Artifacts

Jobs can depend on outputs from prior jobs. The factory downloads all input
artifacts before invoking the command:

1. After bootstrapping as a worker, the factory calls `worker_ping` to get the
   job's input list.
2. Each input is downloaded via `worker_job_input_download` to a temporary
   `.partial` file, then atomically renamed on success.
3. Files are placed in `BUILDOMAT_ARTIFACT_DIR` preserving the original path
   structure (e.g., `sub/dir/file.bin` creates intermediate directories).

The command finds inputs ready to use when it starts.

## Output Rules

Output rules control which files from `BUILDOMAT_OUTPUT_DIR` get uploaded after
the command completes. Rules come from the job definition (returned in
`worker_ping`), not from the factory config.

Each rule has four fields:

| Field | Type | Description |
|-------|------|-------------|
| `rule` | glob pattern | Matched against relative paths in the output dir |
| `ignore` | bool | If true, matching files are excluded from upload |
| `require_match` | bool | If true, at least one file must match or upload fails |
| `size_change_ok` | bool | If true, files that disappear or change size mid-upload are warnings, not errors |

### Processing Order

1. Rules are separated into include patterns and ignore patterns.
2. Include patterns are globbed against the output directory.
3. Files matching any ignore pattern are excluded.
4. Remaining files are uploaded.
5. After upload, all `require_match` patterns are checked — if any matched zero
   files, the upload fails with an error.

### Example Rules

```
# Upload all .log files; at least one must exist.
rule = "*.log", require_match = true

# Upload everything else.
rule = "*"

# But skip temporary files.
rule = "*.tmp", ignore = true
```

If no output rules are provided, all files in the output directory are uploaded.

## Diagnostic Scripts

The server can provide diagnostic scripts via `factory_metadata` in the
`worker_ping` response. Two hooks are supported:

### Pre-job diagnostic (`pre_job_diagnostic_script`)

Runs **before** the external command. Its output is streamed as events with
stream names `diag.pre.stdout` and `diag.pre.stderr`. If the script fails
(non-zero exit), a warning is logged but the job still proceeds.

### Post-job diagnostic (`post_job_diagnostic_script`)

Runs **after** the command completes and outputs are uploaded. Its output is
streamed as `diag.post.stdout` and `diag.post.stderr`.

If a post-job script is configured, the factory calls
`worker_diagnostics_enable` during setup so the server holds the worker open.
After the script runs, `worker_diagnostics_complete` is called with
`hold=true` if the script exited non-zero (allowing manual investigation) or
`hold=false` on success.

Both scripts run as `bash` in the job's work directory.

## Error Handling

- **Command failure** — output upload is still attempted so logs and diagnostics
  are preserved. `worker_job_complete(failed=true)` is then sent.
- **Recycle** — if the server sets `recycle=true` on the worker, the factory
  kills the command and destroys the worker without reporting completion.
- **Upload errors** — logged as warnings; the job still completes. Individual
  file failures don't block other uploads.
- **Cleanup** — the per-job directory is removed after the worker is destroyed,
  regardless of outcome.

## Job Lifecycle

1. Factory calls `factory_lease` with its configured targets.
2. Server returns a lease with `{job, target}`.
3. Factory calls `factory_worker_create`, then `factory_worker_associate`.
4. Factory generates a worker token, calls `worker_bootstrap`.
5. Factory calls `worker_ping` to get job info (inputs, output rules, metadata).
6. Input artifacts are downloaded to `{job_dir}/{job_id}/artifacts/`.
7. If `factory_metadata` includes diagnostic scripts, `worker_diagnostics_enable`
   is called and the pre-job script runs.
8. The external command is spawned with piped stdout/stderr; an event streamer
   task begins reading lines.
9. On each `factory_loop` iteration, the factory polls `factory_worker_get` and
   checks if the child process has exited.
10. When the command exits, the event streamer is awaited to flush remaining
    output.
11. Output files are uploaded per the output rules.
12. `worker_job_complete` is called with the pass/fail result.
13. Post-job diagnostic script runs (if configured).
14. `factory_worker_destroy` cleans up the worker.
15. The per-job directory is removed.

## Integration Testing

The `persistent-runner-stub` crate (`factory/persistent-runner-stub/`) provides
a test binary that exercises all factory features through the external command
interface. It supports multiple modes (`--mode success|fail|slow|crash|
no-output|large-output`) to test different lifecycle paths. See its source for
the full interface contract and `factory/persistent/tests/stub-config.toml` for
an example factory configuration that uses it.

## Development Roadmap

This factory is being built incrementally.  Each commit adds one coherent piece
of functionality.  All items below will be checked off as the corresponding
code lands.

- [x] Configuration, signal handling, and main loop skeleton
- [ ] Factory loop and worker operations (lease, spawn, complete)
- [ ] Unit and integration tests
- [ ] End-to-end tests with runner stub
