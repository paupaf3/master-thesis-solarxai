#!/bin/bash

# SolarX AI Docker Environment Startup Script

set -e

echo "Starting SolarX AI Kafka Environment..."

# Function to wait for service to be ready
wait_for_service() {
    local service_name=$1
    local port=$2
    local host=${3:-localhost}
    
    echo "Waiting for $service_name to be ready..."
    until nc -z $host $port; do
        echo "   ... waiting for $service_name ($host:$port)"
        sleep 2
    done
    echo "$service_name is ready!"
}

# Start Kafka infrastructure first
echo "Starting Kafka Controllers and Brokers..."
docker-compose up -d controller-1 controller-2 controller-3

# Wait for controllers to be ready
wait_for_service "controller-1" 9093
wait_for_service "controller-2" 9094
wait_for_service "controller-3" 9095


docker-compose up -d broker-1 broker-2 broker-3

# Wait for brokers to be ready
wait_for_service "broker-1" 9092
wait_for_service "broker-2" 9192  
wait_for_service "broker-3" 9292

# Wait additional time for brokers to fully initialize and form the cluster
echo "Waiting for Kafka brokers to fully initialize and form the cluster..."
sleep 20

echo "Kafka cluster is ready!"

# Create Kafka topics
echo "Creating Kafka topics..."
./scripts/create-kafka-topics.sh

echo "Topics created successfully!"

# Start PostgreSQL and seed dimension tables
echo ""
echo "Starting PostgreSQL..."
docker-compose up -d postgres

# Wait for PostgreSQL to be ready
wait_for_service "PostgreSQL" 5432

echo "Running database seeder (db-seeder)..."
docker-compose run --rm db-seeder
echo "Database seeding complete!"

# Start Flink cluster
echo ""
echo "Starting Flink cluster..."
docker-compose up -d flink-jobmanager

# Wait for JobManager to be ready
wait_for_service "Flink JobManager" 8081

docker-compose up -d flink-taskmanager-1 flink-taskmanager-2 flink-taskmanager-3

# Wait for TaskManagers to register
echo "Waiting for TaskManagers to register with JobManager..."
sleep 20

echo "Flink cluster is ready!"
echo "  Flink Web UI: http://localhost:8081"

# Start Flink consumer jobs
echo ""
echo "Submitting Flink jobs (unified mode)..."

# Submit unified consumer job (handles all Kafka topics)
docker exec -d flink-jobmanager flink run -py /opt/flink/jobs/unified_consumer.py

# Submit unified bronze to silver processing job
docker exec -d flink-jobmanager flink run -py /opt/flink/jobs/unified_bronze_to_silver.py

# Submit unified silver to gold processing job
docker exec -d flink-jobmanager flink run -py /opt/flink/jobs/unified_silver_to_gold.py

echo "Flink jobs submitted (3 unified jobs instead of 13 individual jobs)!"

# Start MLflow UI locally (reads from local mlruns/)
echo ""
echo "Starting MLflow UI locally..."
MLFLOW_PID_FILE=".mlflow_ui.pid"
if [ -f "$MLFLOW_PID_FILE" ] && kill -0 $(cat "$MLFLOW_PID_FILE") 2>/dev/null; then
    echo "MLflow UI already running (PID $(cat $MLFLOW_PID_FILE))"
else
    mlflow ui --port 5000 --backend-store-uri sqlite:///mlflow/experiments/mlflow.db &
    echo $! > "$MLFLOW_PID_FILE"
    echo "MLflow UI started (PID $(cat $MLFLOW_PID_FILE))"
fi
echo "  MLflow UI: http://localhost:5000"

# Start simulators based on arguments or default set
if [ "$1" = "default" ]; then
    echo "Starting default simulator..."
    docker-compose up -d pv-simulator-default
elif [ "$1" = "fast" ]; then
    echo "Starting fast simulator..."
    docker-compose up -d pv-simulator-fast
elif [ "$1" = "high-failure" ]; then
    echo "Starting high-failure simulator..."
    docker-compose up -d pv-simulator-high-failure
elif [ "$1" = "high-failure-fast" ]; then
    echo "Starting high-failure fast simulator..."
    docker-compose up -d pv-simulator-high-failure-fast
else
    echo "Starting default simulator..."
    docker-compose up -d pv-simulator-default
fi

# Start Inference Engine
echo ""
echo "Starting Inference Engine..."
docker-compose up -d inference-anomaly inference-forecasting
echo "Inference Engine started (anomaly + forecasting)."

# Start Streamlit dashboard
echo ""
echo "Starting Streamlit dashboard..."
docker-compose up -d streamlit-dashboard

# Wait for Streamlit to be ready
wait_for_service "Streamlit dashboard" 8501

echo "Streamlit dashboard is ready!"

echo ""
echo "SolarX AI environment is ready!"
echo ""
echo "Monitor logs with: docker-compose logs -f [service-name]"
echo "View running services: docker-compose ps"
echo "Stop environment: docker-compose down"
echo ""
echo "Kafka brokers available at:"
echo "  - localhost:9092 (broker-1)"
echo "  - localhost:9192 (broker-2)"
echo "  - localhost:9292 (broker-3)"
echo ""
echo "Flink Web UI available at:"
echo "  - http://localhost:8081"
echo ""
echo "Streamlit Dashboard available at:"
echo "  - http://localhost:8501"
echo ""
echo "MLflow Tracking Server available at:"
echo "  - http://localhost:5000"
echo ""
echo "Flink Commands:"
echo "  - List jobs: docker exec flink-jobmanager flink list"
echo "  - Run a job: docker exec flink-jobmanager flink run -py /opt/flink/jobs/your_job.py"
echo "  - Cancel a job: docker exec flink-jobmanager flink cancel <job_id>"
echo "  - View logs: docker logs -f flink-jobmanager"
echo ""
echo "PostgreSQL Database:"
echo "  - Host: localhost"
echo "  - Port: 5432"
echo "PgAdmin:"
echo "Execute with docker-compose up -d pgadmin"
echo "Access PgAdmin at:"
echo "  - http://localhost:5050"
echo ""