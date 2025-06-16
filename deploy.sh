#!/bin/bash
set -e

echo "Stopping and removing Docker Compose services with volumes..."
docker-compose -f docker-compose.yaml down -v

echo "Building Docker Compose services..."
docker-compose -f docker-compose.yaml up --build -d

echo "Waiting for services to be ready..."

echo "Checking service health..."
if ! docker-compose ps | grep -q "healthy"; then
    echo "Error: One or more services are not healthy. Check logs."
    docker-compose logs
    exit 1
fi

echo "Removing old data-cleaner image..."
docker rmi data-cleaner:latest 2>/dev/null || true

# Build ulang image dengan no-cache
echo "Building data-cleaner image with no-cache..."
docker build --no-cache -t data-cleaner:latest .

echo "Deployment completed! Check Airflow UI at http://localhost:8080 (use admin/admin to log in)."
echo "View logs if needed: docker-compose logs"