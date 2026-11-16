#!/usr/bin/env bash
set -euo pipefail
IFS=$'\n\t'

BLUE='\033[1;34m'
GREEN='\033[1;32m'
NC='\033[0m'
INFO="${BLUE}ℹ${NC}"
OK="${GREEN}✓${NC}"

if [ -f "/etc/uv/uv.toml" ]; then
    echo -e "${INFO} Detected system-level /etc/uv/uv.toml; configuring PyPI default index via environment..."
    rm -f uv.toml
    printf 'export UV_DEFAULT_INDEX=https://pypi.org/simple\n' > .envrc
    if [ -f "/etc/npmrc" ]; then
        printf 'export npm_config_registry=https://registry.npmjs.org\n' >> .envrc
    fi
    echo -e "${OK} Wrote registry overrides to .envrc (run 'direnv allow' or source .envrc in direct shells)."
elif [ -f "/etc/npmrc" ]; then
    echo -e "${INFO} Detected system-level /etc/npmrc; configuring npm registry via environment..."
    printf 'export npm_config_registry=https://registry.npmjs.org\n' > .envrc
    echo -e "${OK} Wrote npm_config_registry to .envrc."
else
    echo -e "${INFO} No system-level /etc/uv/uv.toml or /etc/npmrc detected. Skipping registry overrides."
fi
