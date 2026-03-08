#!/bin/bash

# Kafka Topics Creation Script for SolarX AI

set -e

# Function to create a topic if it doesn't exist
create_topic() {
    local topic_name=$1
    local partitions=${2:-3}
    local replication_factor=${3:-2}
    
    echo "Creating topic: $topic_name (partitions: $partitions, replication-factor: $replication_factor)"
    
    # Retry logic for topic creation in case brokers are still initializing
    local max_retries=10
    local retry_count=0
    
    while [ $retry_count -lt $max_retries ]; do
        # Use the internal cluster network addresses for better reliability
        # Topics are cluster-wide, so connecting to any broker in the cluster is sufficient
        if docker exec broker-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server broker-1:29092,broker-2:29192,broker-3:29292 \
            --create \
            --topic $topic_name \
            --partitions $partitions \
            --replication-factor $replication_factor \
            --if-not-exists \
            --config retention.ms=604800000 \
            --config segment.ms=86400000; then
            echo "Topic $topic_name created successfully!"
            return 0
        else
            retry_count=$((retry_count + 1))
            if [ $retry_count -lt $max_retries ]; then
                echo "   ... retrying topic creation in 5 seconds (attempt $retry_count/$max_retries)"
                sleep 5
            fi
        fi
    done
    
    echo "ERROR: Failed to create topic $topic_name after $max_retries attempts"
    return 1
}

# Function to list all topics
list_topics() {
    echo "Listing all topics from the cluster:"
    # Connect to multiple brokers for redundancy
    docker exec broker-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server broker-1:29092,broker-2:29192,broker-3:29292 --list
}

# Function to describe topics and show replication details
describe_topics() {
    echo "Topic replication details:"
    echo "========================="
    docker exec broker-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server broker-1:29092,broker-2:29192,broker-3:29292 --describe
}

echo "Creating Kafka topics for SolarX AI..."

# Create topics for different data streams
echo "Creating data stream topics..."

# Meteorological data topic
create_topic "on_site_meteo_station" 3 2

# Inverter data topic
create_topic "inverter" 3 2

# POI meter topic  
create_topic "poi_meter" 3 2

# System status topic
create_topic "system_status" 3 2

# Inference trigger topic (Flink B2S notifies inference engine of new silver data)
create_topic "inference-trigger" 3 2

echo ""
echo "Topics created successfully!"
echo ""

# List all topics to verify creation
list_topics

echo ""

# Show detailed replication information
describe_topics

echo ""
echo "Topic creation completed!"