# SP-Test Factory Setup Guide

This guide describes how to set up and configure the SP-Test factory for buildomat integration.

## Prerequisites

1. **Buildomat Server** - A running buildomat-server instance
2. **Factory Token** - Admin-generated factory authentication token
3. **Testbed Hardware** - At least one configured SP/ROT testbed
4. **sp-runner** - Installed and configured
   [`sp-runner`](https://github.com/oxidecomputer/sp-tools) with testbed access

## Directory Structure

```
$FACTORY_BASE/
├── config.toml      # Factory configuration
├── data.sqlite3     # Instance tracking database (auto-created)
└── work/            # Agent working directories (auto-created)
```

## Configuration

### Step 1: Create Configuration Directory

```bash
export FACTORY_BASE=~/Oxide/ci/factory
mkdir -p "$FACTORY_BASE"
```

### Step 2: Generate Configuration

Use the setup script to generate initial configuration:

```bash
# From buildomat repo
cd ~/Oxide/src/buildomat/sp-tools-integration

# Generate config interactively
./factory/sp-test/scripts/setup.sh --base-dir "$FACTORY_BASE"

# Or generate with specific values
./factory/sp-test/scripts/setup.sh \
    --base-dir "$FACTORY_BASE" \
    --server-url "http://localhost:8000" \
    --token-file ~/secrets/factory-token
```

### Step 3: Configure Testbeds

Edit `$FACTORY_BASE/config.toml` to add your testbeds:

```toml
[testbed.grapefruit-7f495641]
sp_type = "grapefruit"
targets = ["sp-grapefruit"]
# Omit 'host' for local execution, or specify for SSH:
# host = "testbed-host.local"
sp_runner_path = "sp-runner"
sp_runner_config = "~/Oxide/ci/config.toml"
baseline = "v16"
enabled = true
```

## Configuration Reference

### `[general]` Section

| Field | Required | Description |
|-------|----------|-------------|
| `baseurl` | Yes | Buildomat server URL (e.g., `http://localhost:8000`) |

### `[factory]` Section

| Field | Required | Description |
|-------|----------|-------------|
| `token` | Yes | Factory authentication token from buildomat admin |

### `[testbed.<name>]` Section

| Field | Required | Default | Description |
|-------|----------|---------|-------------|
| `sp_type` | Yes | - | Hardware type: `gimlet`, `grapefruit`, `sidecar`, `psc` |
| `targets` | Yes | - | List of buildomat targets this testbed serves |
| `host` | No | (local) | SSH host for remote execution |
| `sp_runner_path` | No | `sp-runner` | Path to sp-runner binary |
| `sp_runner_config` | No | `~/Oxide/ci/config.toml` | Path to sp-runner config |
| `baseline` | No | `v16` | Baseline firmware version |
| `enabled` | No | `true` | Enable/disable testbed for CI |

### `[target.<name>]` Section

Define buildomat targets that jobs can request. Currently no target-specific
configuration is needed, but the section must exist for each target referenced
by testbeds.

## Running the Factory

```bash
# Build the factory
cd ~/Oxide/src/buildomat/sp-tools-integration
cargo build -p buildomat-factory-sp-test

# Run with configuration
./target/debug/buildomat-factory-sp-test \
    -f "$FACTORY_BASE/config.toml" \
    -d "$FACTORY_BASE/data.sqlite3"
```

## Environment Variables

| Variable | Description |
|----------|-------------|
| `BUILDOMAT_AGENT_PATH` | Path to buildomat-agent binary |
| `BUILDOMAT_WORK_DIR` | Base directory for agent work directories |

## Obtaining a Factory Token

Contact the buildomat administrator to obtain a factory token. The token is
generated on the server side and grants the factory permission to:

- Poll for available jobs
- Create and manage workers
- Associate workers with jobs

## Troubleshooting

### Factory not receiving jobs

1. Verify the factory token is correct
2. Check that targets in config match job target requirements
3. Ensure at least one testbed is enabled and not in use

### Agent fails to start

1. Check `BUILDOMAT_AGENT_PATH` points to valid agent binary
2. Verify work directory is writable
3. Check agent logs in `$BUILDOMAT_WORK_DIR/<instance>/`

### Testbed not available

1. Verify testbed is `enabled = true` in config
2. Check no other instance is using the testbed
3. Review factory logs for testbed-specific errors
