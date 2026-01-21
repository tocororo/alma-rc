#!/bin/bash
# docker-entrypoint.sh

set -e

# Activate virtual environment if using Poetry virtualenvs
if [ -f /app/.venv/bin/activate ]; then
    source /app/.venv/bin/activate
fi

# Run the command
exec "$@"