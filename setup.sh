#!/bin/bash
set -e

echo "=== Loading environment variables ==="
if [ ! -f variables.env ]; then
    echo "ERROR: variables.env file not found!"
    exit 1
fi

# Export variables from variables.env
export $(grep -v '^#' variables.env | xargs)

echo "=== Creating necessary directories ==="
if [ ! -d pgdata ]; then
    echo "Creating pgdata directory..."
    mkdir -p pgdata
    chmod 700 pgdata
fi

echo "=== Pulling Docker images (if necessary) ==="
docker compose --env-file variables.env pull

echo "=== Starting all containers ==="
docker compose --env-file variables.env up -d

echo ""
echo "=== Checking container status ==="
docker compose ps

echo ""
echo "=== Services Information ==="
echo "TimescaleDB: localhost:5432"
echo "  User: ${POSTGRES_USER}"
echo "  Database: ${POSTGRES_DB}"
echo ""
echo "RabbitMQ AMQP: localhost:5672"
echo "RabbitMQ Management UI: http://localhost:15672"
echo "  User: ${RABBITMQ_DEFAULT_USER}"
echo ""

echo "=== Displaying logs (Ctrl+C to exit) ==="
docker compose logs -f


