# SP-Test Factory

A buildomat factory for hardware-in-the-loop (HIL) testing of SP/ROT firmware on physical testbeds.

## Overview

The SP-Test factory enables automated CI testing of Hubris SP and ROT firmware on real hardware. It integrates with buildomat to:

1. Poll for jobs targeting SP/ROT hardware tests
2. Allocate available testbeds to jobs
3. Execute tests via [`sp-runner`](https://github.com/oxidecomputer/sp-tools) on testbed hardware
4. Report results back to buildomat

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                    BUILDOMAT SERVER                                 │
│  - Queues jobs by target (e.g., "sp-grapefruit")                    │
│  - Stores artifacts from upstream build jobs                        │
└─────────────────────────────────────────────────────────────────────┘
                              ▲
                              │ HTTP polling (every 7 seconds)
                              ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    SP-TEST FACTORY (this crate)                     │
│                                                                     │
│  Responsibilities:                                                  │
│  - Poll buildomat-server for jobs matching configured targets       │
│  - Track testbed availability via local database                    │
│  - Create workers and associate with testbed instances              │
│  - Start buildomat-agent to execute jobs                            │
│  - Clean up after job completion                                    │
│                                                                     │
│  Does NOT know:                                                     │
│  - How to run tests (that's sp-runner's job)                        │
│  - Test orchestration logic                                         │
│  - Firmware update protocols                                        │
└─────────────────────────────────────────────────────────────────────┘
                              │
                              │ Spawns buildomat-agent per job
                              ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    BUILDOMAT AGENT                                  │
│  - Downloads job artifacts from server                              │
│  - Executes job script (which invokes sp-runner)                    │
│  - Uploads results back to server                                   │
└─────────────────────────────────────────────────────────────────────┘
                              │
                              │ Job script invokes
                              ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    SP-RUNNER                                        │
│  - Loads testbed configuration                                      │
│  - Runs sp-test against hardware                                    │
│  - Writes results to output directory                               │
└─────────────────────────────────────────────────────────────────────┘
```

## Design Principles

1. **Factory owns coordination** - All testbed allocation and status tracking happens in the factory, not in sp-runner.

2. **sp-runner stays simple** - sp-runner has no knowledge of buildomat, job scheduling, or testbed allocation. It just runs tests on hardware it's pointed at.

3. **File-based testbed config** - Testbed configuration comes from a local TOML file. This can later be replaced with an Inventron backend if dynamic allocation is needed.

4. **Local-first development** - The factory supports local execution (no SSH) for development, with SSH support planned for production.

## Components

### `main.rs`
Entry point. Parses CLI arguments, loads configuration, initializes database, and starts the factory worker task.

### `config.rs`
TOML configuration parsing. Defines testbed and target configuration structures.

### `testbed.rs`
Testbed manager abstraction. Tracks which testbeds are configured, their capabilities (which targets they can serve), and availability.

### `worker.rs`
The main factory polling loop:
- Phase 0: Check for completed agents, clean up
- Phase 1: Validate existing instances against server
- Phase 2: Clean up orphaned workers
- Phase 3: Request new work, create instances, start agents

### `executor.rs`
Agent lifecycle management. Handles starting the buildomat-agent process, monitoring its status, and cleanup.

### `db/`
SQLite database layer for instance state persistence. Tracks testbed assignments across factory restarts.

## Instance State Machine

```
Created ──────► Running ──────► Destroying ──────► Destroyed
    │              │                 ▲
    │              │                 │
    └──────────────┴─────────────────┘
         (on error or recycle)
```

- **Created**: Instance allocated, agent starting
- **Running**: Agent is online and executing job
- **Destroying**: Job complete or recycled, cleaning up
- **Destroyed**: Fully cleaned up (historical record)

## Configuration

See `doc/setup.md` for detailed configuration instructions.

Example configuration:

```toml
[general]
baseurl = "http://buildomat-server:8000"

[factory]
token = "factory-api-token"

[testbed.grapefruit-7f495641]
sp_type = "grapefruit"
targets = ["01KAYPTX7HMFSPHN2PAAXRHESC"]  # Target ID (ULID)
sp_runner_path = "sp-runner"
sp_runner_config = "~/Oxide/ci/config.toml"
baseline = "v16"
enabled = true

[target.sp-grapefruit]
```

**Note**: The buildomat API requires target IDs (ULIDs), not target names.

## Usage

```bash
# Build
cargo build -p buildomat-factory-sp-test

# Run
./target/debug/buildomat-factory-sp-test \
    -f /path/to/config.toml \
    -d /path/to/data.sqlite3
```

## Platform Requirements

The buildomat-agent requires **illumos** (specifically SMF - Service Management
Framework). The factory itself is cross-platform, but agent execution only works
on illumos hosts.

**Development options:**
1. **Local mode on illumos**: Factory and agent on same illumos host
2. **SSH mode**: Factory on any platform, SSH to illumos testbed host for agent execution

## Testing Status

The following has been verified:

1. **Factory polling**: Successfully connects to buildomat-server, requests leases
2. **Job acquisition**: Factory receives jobs targeting configured targets
3. **Worker creation**: Creates workers, instances, and database records
4. **Instance lifecycle**: State machine (Created → Running → Destroying → Destroyed)
5. **Cleanup**: Orphaned workers and completed instances cleaned up properly

**Pending verification** (requires illumos testbed):
- Agent execution completing successfully
- sp-runner invocation via job script
- Results uploaded to buildomat-server

## Future Work

- **SSH execution mode**: Run agents on remote testbed hosts via SSH (in progress)
- **Target name resolution**: Look up target IDs by name at startup
- **Inventron integration**: Dynamic testbed discovery and allocation
- **Health monitoring**: Detect and recover from stuck agents
