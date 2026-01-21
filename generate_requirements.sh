#!/bin/bash
# generate_requirements.sh

# Generate requirements.txt from pyproject.toml
poetry export --without-hashes --without-urls -f requirements.txt -o requirements.txt

# Also generate dev requirements
poetry export --without-hashes --without-urls --with dev -f requirements.txt -o requirements-dev.txt