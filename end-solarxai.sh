#!/bin/bash

# SolarX AI Docker Environment End Script
# Removes all containers, volumes, and cleans up the entire environment

set -e

echo "Ending SolarX AI Kafka Environment..."
echo "This will remove all containers, volumes, and data!"
echo ""

# Ask for confirmation
read -p "Are you sure you want to remove everything? This cannot be undone. (yes/no): " -r
echo
if [[ ! $REPLY =~ ^[Yy][Ee][Ss]$ ]]; then
    echo "Cancelled. Environment remains intact."
    exit 1
fi

echo "Removing all containers and volumes..."
echo ""

# Stop local MLflow UI if running
MLFLOW_PID_FILE=".mlflow_ui.pid"
if [ -f "$MLFLOW_PID_FILE" ]; then
    kill $(cat "$MLFLOW_PID_FILE") 2>/dev/null || true
    rm -f "$MLFLOW_PID_FILE"
    echo "MLflow UI stopped"
fi

# First, stop all running services
echo "Stopping all services..."
docker-compose down --volumes 2>/dev/null || true

# Ensure one-off db-seeder container is removed if it exists
docker rm -f solarxai-db-seeder 2>/dev/null || true

echo "Removing stopped containers..."
docker container prune -f 2>/dev/null || true

echo "Removing unused volumes..."
docker volume prune -f 2>/dev/null || true

# Clean up local kafka logs
echo "Cleaning up local Kafka logs..."
if [ -d "data/kafka-logs-1" ]; then
    rm -rf data/kafka-logs-1
    echo "Removed data/kafka-logs-1"
fi

if [ -d "data/kafka-logs-2" ]; then
    rm -rf data/kafka-logs-2
    echo "Removed data/kafka-logs-2"
fi

if [ -d "data/kafka-logs-3" ]; then
    rm -rf data/kafka-logs-3
    echo "Removed data/kafka-logs-3"
fi

# Clean up simulator output parquet files
echo "Cleaning up simulator output parquet files..."
if [ -d "simulator/output" ]; then
    sudo find simulator/output -type f -name "*.parquet" -delete
    echo "Removed all .parquet files from simulator/output/"
fi

echo ""
echo "SolarX AI environment has been completely removed!"
echo ""
echo "Remaining images (can be manually removed if needed):"
docker images | grep -E "kafka|flink|solarxai" || echo "No SolarX AI related images found"
echo ""
echo "To start a fresh environment, run: ./start-solarxai.sh"
echo ""
