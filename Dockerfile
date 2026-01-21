# Dockerfile
FROM docker.uclv.cu/python:3.12-slim-bookworm

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# Install system dependencies
RUN apt-get update && apt-get install -y \
    curl \
    wget \
    xmlsec1 \
    libxml2-dev \
    libxslt1-dev \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Create app directory
WORKDIR /app

# Copy requirements file first (for better caching)
COPY requirements.txt ./

# Install Python dependencies
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Copy project files
COPY pyproject.toml README.md ./
COPY alma_rc/ ./alma_rc/

# Create directories for data
RUN mkdir -p /data/bitstreams /data/invenio_records /data/harvest

# Create entrypoint script
COPY docker-entrypoint.sh /usr/local/bin/
RUN chmod +x /usr/local/bin/docker-entrypoint.sh

# Create wrapper script for import.py
COPY run-import.sh /usr/local/bin/run-import
RUN chmod +x /usr/local/bin/run-import

# Set volume for output data
VOLUME ["/data"]

# Set entrypoint
ENTRYPOINT ["docker-entrypoint.sh"]

# Default command
CMD ["run-import", "--help"]