#!/bin/bash
# quick-start.sh

set -e

# Configuration
CSV_PATH="$1"
OUTPUT_DIR="${2:-./invenio_records}"
SAMPLE="${3:-10}"
LOG_LEVEL="${4:-INFO}"

if [ -z "$CSV_PATH" ]; then
    echo "Usage: $0 <csv_file_path> [output_dir] [sample_size] [log_level]"
    echo "Example: $0 /data/dspace_export.csv /data/output 100 DEBUG"
    exit 1
fi

# Check if CSV file exists
if [ ! -f "$CSV_PATH" ]; then
    echo "Error: CSV file not found at $CSV_PATH"
    exit 1
fi

# Create output directory
mkdir -p "$OUTPUT_DIR"

echo "Starting DSpace to Invenio migration..."
echo "CSV file: $CSV_PATH"
echo "Output directory: $OUTPUT_DIR"
echo "Sample size: $SAMPLE"
echo "Log level: $LOG_LEVEL"

# Build and run with Podman
podman build -t alma-rc-import .

# Run the import
podman run --rm \
    --volume "$(dirname "$CSV_PATH"):/csv:Z" \
    --volume "$OUTPUT_DIR:/output:Z" \
    alma-rc-import \
    run-import "/csv/$(basename "$CSV_PATH")" \
    --output-dir /output \
    --sample "$SAMPLE" \
    --log-level "$LOG_LEVEL"

echo "Migration completed!"
echo "Output saved to: $OUTPUT_DIR"