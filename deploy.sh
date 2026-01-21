#!/bin/bash
# deploy.sh - Podman deployment script

set -e

# Configuration
CONTAINER_NAME="alma-rc"
IMAGE_NAME="alma-rc:latest"
CODE_DIR="/alma/rc"
DATA_DIR="/alma/rc/data"
CSV_FILE="${DATA_DIR}/all-rc.csv"  # Update with your actual CSV file

# Create data directory if it doesn't exist
mkdir -p "${DATA_DIR}"

# Build the image
echo "Building Docker image..."
podman build -t ${IMAGE_NAME} .

# Remove existing container if it exists
if podman container exists ${CONTAINER_NAME}; then
    echo "Removing existing container..."
    podman container rm ${CONTAINER_NAME} --force 
fi

# Run the container with volume mount
echo "Starting container..."
podman run -d \
    --name ${CONTAINER_NAME} \
    --volume ${DATA_DIR}:/data:Z \
    --volume ${CODE_DIR}/.env:/app/.env:Z \
    ${IMAGE_NAME} \
    sleep infinity

echo "Container ${CONTAINER_NAME} started successfully."
echo "Data directory: ${DATA_DIR}"
echo ""
echo "To run the import script:"
echo "  podman exec -it ${CONTAINER_NAME} run-import /data/all-rc.csv --output-dir /data --sample 1"
echo ""
echo "To check logs:"
echo "  podman logs ${CONTAINER_NAME}"
echo ""
echo "To stop the container:"
echo "  podman stop ${CONTAINER_NAME}"