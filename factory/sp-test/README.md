# SP-Test Factory

A buildomat factory for hardware-in-the-loop (HIL) testing of SP/ROT firmware on physical testbeds.

## Overview

The SP-Test factory enables automated CI testing of Hubris SP and ROT firmware on real hardware. It integrates with buildomat to:

1. Poll for jobs targeting SP/ROT hardware tests
2. Download artifacts from buildomat storage
3. Dispatch tests to sp-runner service via HTTP API
4. Upload results back to buildomat

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                    BUILDOMAT SERVER                                 │
│  - Queues jobs by target (e.g., "sp-grapefruit")                    │
│  - Stores artifacts from upstream build jobs                        │
└─────────────────────────────────────────────────────────────────────┘
                              ▲
                              │ HTTP polling (factory protocol)
                              ▼
┌─────────────────────────────────────────────────────────────────────┐
│            TESTBED HOST (e.g., voidstar.lan)                        │
│                                                                     │
│  ┌───────────────────────────────────────────────────────────────┐  │
│  │              SP-TEST FACTORY (this crate)                     │  │
│  │                                                               │  │
│  │  - Polls buildomat-server for jobs                            │  │
│  │  - Downloads artifacts to local input_dir                     │  │
│  │  - Dispatches to sp-runner service via HTTP                   │  │
│  │  - Uploads results from output_dir                            │  │
│  └───────────────────────────────────────────────────────────────┘  │
│                              │                                      │
│                              │ HTTP API (localhost)                 │
│                              ▼                                      │
│  ┌───────────────────────────────────────────────────────────────┐  │
│  │              SP-RUNNER SERVICE                                │  │
│  │                                                               │  │
│  │  - Receives job requests with local paths                     │  │
│  │  - Manages testbed state and allocation                       │  │
│  │  - Runs sp-test as subprocess                                 │  │
│  │  - Writes results to output_dir                               │  │
│  └───────────────────────────────────────────────────────────────┘  │
│                              │                                      │
│                              │ subprocess                           │
│                              ▼                                      │
│  ┌───────────────────────────────────────────────────────────────┐  │
│  │              SP-TEST                                          │  │
│  │  - Hardware communication via MGS protocol                    │  │
│  │  - Firmware update and validation                             │  │
│  └───────────────────────────────────────────────────────────────┘  │
│                              │                                      │
│                              ▼                                      │
│                      [ Hardware Testbed ]                           │
└─────────────────────────────────────────────────────────────────────┘
```

## Design Principles

1. **Factory owns buildomat protocol** - All buildomat communication (polling, artifact download, result upload) happens in the factory.

2. **sp-runner owns test execution** - sp-runner manages testbed state and runs tests. It has no knowledge of buildomat.

3. **Shared filesystem** - Factory and sp-runner run on the same host, using local paths for artifact exchange.

4. **HTTP dispatch** - Factory communicates with sp-runner via HTTP API, enabling clean separation and easy debugging.

## Quick Start

### Prerequisites

1. **sp-runner service running** on the testbed host:
   ```bash
   sp-runner -c ~/Oxide/ci/config.toml buildomat service --listen 127.0.0.1:9090
   ```

2. **Buildomat server** accessible from the testbed host

3. **Baseline firmware** cached on the testbed host

### Start the Factory

```bash
# Build
cargo build -p buildomat-factory-sp-test

# Run
buildomat-factory-sp-test \
    -f /path/to/config.toml \
    -d /path/to/data.sqlite3
```

## Configuration

```toml
[general]
baseurl = "http://buildomat-server:8000"

[factory]
token = "factory-api-token"

[testbed.grapefruit-7f495641]
sp_type = "grapefruit"
targets = ["sp-grapefruit"]
baseline = "v16"
sp_runner_url = "http://localhost:9090"  # HTTP dispatch mode
enabled = true
```

### Configuration Options

| Field | Description |
|-------|-------------|
| `baseurl` | Buildomat server URL |
| `token` | Factory API authentication token |
| `sp_type` | Hardware type (grapefruit, gimlet, etc.) |
| `targets` | Buildomat target names this testbed handles |
| `baseline` | Default baseline version (e.g., "v16") |
| `sp_runner_url` | sp-runner service URL for HTTP dispatch |
| `enabled` | Whether this testbed is available |

## Job Execution Flow

1. **Factory polls** buildomat-server for pending jobs
2. **Factory receives lease** for a job targeting configured testbed
3. **Factory downloads artifacts** from buildomat storage to `input_dir`
4. **Factory dispatches job** via HTTP:
   ```
   POST http://localhost:9090/job
   {
     "testbed": "grapefruit-7f495641",
     "input_dir": "/tmp/buildomat/instance-123/input",
     "output_dir": "/tmp/buildomat/instance-123/output",
     "baseline": "v16",
     "test_type": "update-rollback"
   }
   ```
5. **Factory polls for completion**:
   ```
   GET http://localhost:9090/job/{job_id}/status
   ```
6. **Factory uploads results** from `output_dir` to buildomat storage
7. **Factory reports completion** to buildomat-server

## Components

| File | Description |
|------|-------------|
| `main.rs` | Entry point, CLI parsing, initialization |
| `config.rs` | TOML configuration with `sp_runner_url` support |
| `testbed.rs` | Testbed manager, capability tracking |
| `worker.rs` | Main polling loop, job lifecycle |
| `executor.rs` | HTTP dispatch to sp-runner service |
| `db/` | SQLite persistence for instance state |

## Testing Status

**Verified working (2025-12-03):**
- Factory polling buildomat-server
- Artifact download via worker protocol
- HTTP dispatch to sp-runner service
- Job status polling
- Result upload via worker protocol
- End-to-end test on illumos: 42 tests, 61 seconds

## Deployment

For detailed deployment instructions, see:
- [BUILDOMAT-FACTORY-SETUP.md](https://github.com/oxidecomputer/sp-tools/blob/main/doc/ci-integration/BUILDOMAT-FACTORY-SETUP.md) in sp-tools
- [buildomat-integration-testing.md](https://github.com/oxidecomputer/sp-tools/blob/main/doc/ci-integration/buildomat-integration-testing.md) for testing

### Deployment Scripts (in sp-tools)

```bash
# Sync and build on illumos host
./scripts/rsync-and-install-to-illumos voidstar.lan --buildomat

# Start services on remote host
./scripts/buildomat-remote-start.sh --reset
```

## Troubleshooting

### Factory not receiving jobs

1. Check factory is registered with buildomat-server
2. Verify target names match job configuration
3. Check factory logs for polling errors

### Tests failing

1. Check sp-runner service is running: `curl http://localhost:9090/health`
2. Check testbed is available: `curl http://localhost:9090/testbeds`
3. Review sp-runner logs for test execution errors

### Testbed stuck in busy state

Recover via sp-runner API:
```bash
curl -X POST http://localhost:9090/testbed/grapefruit-7f495641/recover \
  -H 'Content-Type: application/json' -d '{"force": false}'
```

## Related Documentation

- **sp-tools**: https://github.com/oxidecomputer/sp-tools
- **sp-runner CLI**: `sp-runner --help` or sp-tools CLI-REFERENCE.md
- **Buildomat**: https://github.com/oxidecomputer/buildomat
