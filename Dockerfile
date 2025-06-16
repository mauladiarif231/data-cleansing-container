# Use Python 3.11 slim as base image
FROM python:3.8

# Set working directory
WORKDIR /app

# Install system dependencies including development tools
RUN apt-get update && apt-get install -y \
    gcc \
    python3-dev \
    libpq-dev \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for better caching
COPY requirements.txt .

# Install Python dependencies with better error handling
RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Verify critical packages are installed
RUN python -c "import sqlalchemy; print('SQLAlchemy version:', sqlalchemy.__version__)" && \
    python -c "import pandas; print('Pandas version:', pandas.__version__)" && \
    python -c "import psycopg2; print('sycopg2 installed successfully')" && \
    echo "✓ All required packages installed successfully"

# Copy application files
COPY main.py .
COPY ddl.sql .

# Create necessary directories with correct permissions
RUN mkdir -p /source /target /app/logs && \
    chmod -R 775 /source /target /app/logs

# Add a non-root user for security
RUN useradd -m -u 1001 appuser && \
    chown -R appuser:appuser /app /source /target /app/logs
USER appuser

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app

# Default command
# If you want running in local(docker without run task 3), please uncomment this line
# CMD ["python", "main.py"]