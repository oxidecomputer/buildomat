#!/bin/bash
# SP-Test Factory Setup Script
#
# Generates initial configuration for the SP-Test factory.
# See doc/setup.md for detailed documentation.

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

info() { echo -e "${GREEN}INFO:${NC} $*"; }
warn() { echo -e "${YELLOW}WARN:${NC} $*"; }
error() { echo -e "${RED}ERROR:${NC} $*" >&2; }
prompt() { echo -e "${BLUE}$*${NC}"; }

usage() {
    cat <<EOF
Usage: $(basename "$0") [OPTIONS]

Generate SP-Test factory configuration.

Options:
    --base-dir DIR      Base directory for factory (default: ~/Oxide/ci/factory)
    --server-url URL    Buildomat server URL (default: http://localhost:8000)
    --token-file FILE   File containing factory token (will prompt if not provided)
    --sp-runner PATH    Path to sp-runner binary (default: sp-runner)
    --sp-config PATH    Path to sp-runner config (default: ~/Oxide/ci/config.toml)
    --help              Show this help message

Example:
    $(basename "$0") --base-dir ~/Oxide/ci/factory --server-url http://localhost:8000
EOF
    exit 1
}

# Default values
BASE_DIR="${HOME}/Oxide/ci/factory"
SERVER_URL="http://localhost:8000"
TOKEN_FILE=""
SP_RUNNER_PATH="sp-runner"
SP_RUNNER_CONFIG="${HOME}/Oxide/ci/config.toml"

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --base-dir)
            BASE_DIR="$2"
            shift 2
            ;;
        --server-url)
            SERVER_URL="$2"
            shift 2
            ;;
        --token-file)
            TOKEN_FILE="$2"
            shift 2
            ;;
        --sp-runner)
            SP_RUNNER_PATH="$2"
            shift 2
            ;;
        --sp-config)
            SP_RUNNER_CONFIG="$2"
            shift 2
            ;;
        --help)
            usage
            ;;
        *)
            error "Unknown option: $1"
            usage
            ;;
    esac
done

info "SP-Test Factory Setup"
info "====================="
echo ""

# Create base directory
info "Creating directory structure..."
mkdir -p "$BASE_DIR"
mkdir -p "$BASE_DIR/work"

# Get factory token
if [[ -n "$TOKEN_FILE" ]] && [[ -f "$TOKEN_FILE" ]]; then
    FACTORY_TOKEN=$(cat "$TOKEN_FILE")
    info "Read factory token from $TOKEN_FILE"
elif [[ -n "$FACTORY_TOKEN" ]]; then
    info "Using FACTORY_TOKEN from environment"
else
    prompt "Enter factory token (from buildomat admin):"
    read -r FACTORY_TOKEN
    if [[ -z "$FACTORY_TOKEN" ]]; then
        error "Factory token is required"
        exit 1
    fi
fi

# Validate token format (basic check)
if [[ ${#FACTORY_TOKEN} -lt 16 ]]; then
    warn "Factory token seems short - verify it's correct"
fi

# Check for existing config
CONFIG_FILE="$BASE_DIR/config.toml"
if [[ -f "$CONFIG_FILE" ]]; then
    warn "Configuration file already exists: $CONFIG_FILE"
    prompt "Overwrite? [y/N]"
    read -r REPLY
    if [[ ! "$REPLY" =~ ^[Yy]$ ]]; then
        info "Keeping existing configuration"
        exit 0
    fi
fi

# Generate configuration
info "Generating configuration..."
cat > "$CONFIG_FILE" <<EOF
# SP-Test Factory Configuration
# Generated: $(date -Iseconds)
# See doc/setup.md for documentation

[general]
baseurl = "$SERVER_URL"

[factory]
token = "$FACTORY_TOKEN"

# Testbed definitions
# Add your testbeds below. Example:
#
# [testbed.grapefruit-7f495641]
# sp_type = "grapefruit"
# targets = ["sp-grapefruit"]
# # host = "testbed-host.local"  # Omit for local execution
# sp_runner_path = "$SP_RUNNER_PATH"
# sp_runner_config = "$SP_RUNNER_CONFIG"
# baseline = "v16"
# enabled = true

# Target definitions
# Add targets that your testbeds can serve:
#
# [target.sp-grapefruit]
# [target.sp-gimlet]
EOF

info "Configuration written to: $CONFIG_FILE"

# Set permissions (token should not be world-readable)
chmod 600 "$CONFIG_FILE"
info "Set permissions to 600 (owner read/write only)"

# Check for sp-runner config
if [[ -f "$SP_RUNNER_CONFIG" ]]; then
    info "sp-runner config found: $SP_RUNNER_CONFIG"
else
    warn "sp-runner config not found: $SP_RUNNER_CONFIG"
    warn "You may need to update sp_runner_config in testbed definitions"
fi

# Check for testbed discovery
DISCOVERED_DIR="${SP_RUNNER_CONFIG%/*}/testbeds/discovered"
if [[ -d "$DISCOVERED_DIR" ]]; then
    TESTBEDS=$(ls "$DISCOVERED_DIR"/*.toml 2>/dev/null | wc -l)
    if [[ $TESTBEDS -gt 0 ]]; then
        info "Found $TESTBEDS discovered testbed(s) in $DISCOVERED_DIR"
        echo ""
        prompt "Discovered testbeds:"
        for f in "$DISCOVERED_DIR"/*.toml; do
            basename "$f" .toml
        done
        echo ""
        info "Add these testbeds to $CONFIG_FILE to enable them"
    fi
fi

echo ""
info "Setup complete!"
echo ""
info "Next steps:"
info "  1. Edit $CONFIG_FILE to add testbed definitions"
info "  2. Run the factory:"
info "     ./target/debug/buildomat-factory-sp-test \\"
info "         -f $CONFIG_FILE \\"
info "         -d $BASE_DIR/data.sqlite3"
