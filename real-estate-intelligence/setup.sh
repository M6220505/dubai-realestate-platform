#!/usr/bin/env bash
# One-command setup for Dubai Real Estate Intelligence Platform.
# Run from inside the real-estate-intelligence/ directory.
#
# Usage:
#   bash setup.sh
#   source .venv/bin/activate
#   python3 -m pipeline.weekly_pipeline --dry-run

set -euo pipefail

echo "Dubai Real Estate Intelligence Platform — Setup"
echo "================================================"

# Check Python 3
if ! command -v python3 &>/dev/null; then
    echo ""
    echo "ERROR: python3 not found."
    echo "Install Python 3.9+ first:"
    echo "  macOS:           brew install python3"
    echo "  Ubuntu/Debian:   sudo apt install python3 python3-pip python3-venv"
    exit 1
fi

PY_VERSION=$(python3 -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')
echo "Python version: $PY_VERSION"

# Create virtual environment
echo ""
echo "Creating virtual environment (.venv)..."
python3 -m venv .venv

# Activate it
source .venv/bin/activate

# Install dependencies
echo "Installing dependencies..."
pip3 install --upgrade pip --quiet
pip3 install -r requirements.txt

echo ""
echo "================================================"
echo "Setup complete!"
echo ""
echo "Next steps:"
echo ""
echo "  1. Activate the virtual environment:"
echo "     source .venv/bin/activate"
echo ""
echo "  2. Verify install (dry run — no network requests):"
echo "     python3 -m pipeline.weekly_pipeline --dry-run"
echo ""
echo "  3. Run the test suite:"
echo "     python3 -m pytest tests/ -v"
echo ""
echo "  4. Run the full weekly pipeline:"
echo "     python3 -m pipeline.weekly_pipeline --max-pages 2"
echo "================================================"
