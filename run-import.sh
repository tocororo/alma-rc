#!/bin/bash
# run-import.sh - Wrapper script for importing DSpace data to InvenioRDM

set -e

# Default project directory
PROJECT_DIR="${PROJECT_DIR:-/app}"

# Change to project directory
cd "$PROJECT_DIR"

# Activate virtual environment if exists
if [ -f .venv/bin/activate ]; then
    source .venv/bin/activate
fi

# Run the import script with all arguments
python -m alma_rc.insert_data.import "$@"