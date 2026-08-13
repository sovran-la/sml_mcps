#!/bin/bash
# ci-local.sh - Run the same checks that CI runs

set -e  # Exit on first error

CARGO="${CARGO:-cargo}"
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${YELLOW}=== Running CI checks locally ===${NC}\n"

# Format
echo -e "${YELLOW}[1/6] Formatting code...${NC}"
$CARGO fmt --all
echo -e "${GREEN}✓ Formatted${NC}\n"

# Clippy
echo -e "${YELLOW}[2/6] Running clippy...${NC}"
if $CARGO clippy --all-targets --features hosted -- -D warnings; then
    echo -e "${GREEN}✓ Clippy passed${NC}\n"
else
    echo -e "${RED}✗ Clippy failed${NC}"
    exit 1
fi

# Clippy with the cli feature - it is not in `hosted`, so it needs its own pass
echo -e "${YELLOW}[3/6] Running clippy (--features cli)...${NC}"
if $CARGO clippy --all-targets --features cli -- -D warnings; then
    echo -e "${GREEN}✓ Clippy passed (--features cli)${NC}\n"
else
    echo -e "${RED}✗ Clippy failed (--features cli)${NC}"
    exit 1
fi

# Tests without features
echo -e "${YELLOW}[4/6] Running tests (no features)...${NC}"
if $CARGO test; then
    echo -e "${GREEN}✓ Tests passed (no features)${NC}\n"
else
    echo -e "${RED}✗ Tests failed (no features)${NC}"
    exit 1
fi

# Tests with hosted feature
echo -e "${YELLOW}[5/6] Running tests (--features hosted)...${NC}"
if $CARGO test --features hosted; then
    echo -e "${GREEN}✓ Tests passed (--features hosted)${NC}\n"
else
    echo -e "${RED}✗ Tests failed (--features hosted)${NC}"
    exit 1
fi

# Tests with the cli feature
echo -e "${YELLOW}[6/6] Running tests (--features cli)...${NC}"
if $CARGO test --features cli; then
    echo -e "${GREEN}✓ Tests passed (--features cli)${NC}\n"
else
    echo -e "${RED}✗ Tests failed (--features cli)${NC}"
    exit 1
fi

echo -e "${GREEN}=== All CI checks passed! ===${NC}"
