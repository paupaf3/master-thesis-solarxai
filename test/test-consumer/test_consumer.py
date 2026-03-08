#!/usr/bin/env python3
import json
import sys
import time
import argparse
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any
from kafka import KafkaConsumer
import threading
import signal


class TestConsumer:
    """Test consumer that saves all messages from Kafka topics to files."""
    
    def __init__(self, brokers: List[str], output_dir: str, topics: List[str]):
        self.brokers = brokers
        self.output_dir = Path(output_dir)
        self.topics = topics
        self.running = True
        self.consumers = {}
        self.file_handles = {}
        
        # Create output directory if it doesn't exist
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
        print(f"Test Consumer initialized:")
        print(f"  Brokers: {brokers}")
        print(f"  Output directory: {output_dir}")
        print(f"  Topics to consume: {topics}")
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully."""
        print(f"\nReceived signal {signum}. Shutting down gracefully...")
        self.stop()
    
    def _create_consumer(self, topic: str) -> KafkaConsumer:
        """Create a Kafka consumer for a specific topic."""
        try:
            consumer = KafkaConsumer(
                topic,
                bootstrap_servers=self.brokers,
                auto_offset_reset='earliest',  # Start from beginning
                enable_auto_commit=True,
                group_id=f'test_consumer_group_{topic}',
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                consumer_timeout_ms=1000,  # Timeout after 1 second of no messages
                session_timeout_ms=30000,
                heartbeat_interval_ms=3000,
                request_timeout_ms=40000,
                api_version=(0, 10, 1),
            )
            print(f"Created consumer for topic: {topic}")
            return consumer
        except Exception as e:
            print(f"Error creating consumer for topic {topic}: {e}")
            raise
    
    def _get_output_file(self, topic: str):
        """Get or create output file handle for a topic."""
        if topic not in self.file_handles:
            filename = self.output_dir / f"{topic}_messages.jsonl"
            self.file_handles[topic] = open(filename, 'a', encoding='utf-8')
            print(f"Created output file: {filename}")
        return self.file_handles[topic]
    
    def _save_message(self, topic: str, message: Dict[Any, Any]):
        """Save a message to the appropriate topic file."""
        try:
            # Add metadata
            enriched_message = {
                'timestamp': datetime.utcnow().isoformat(),
                'topic': topic,
                'partition': message.get('partition', 'unknown'),
                'offset': message.get('offset', 'unknown'),
                'data': message
            }
            
            # Write to file
            file_handle = self._get_output_file(topic)
            file_handle.write(json.dumps(enriched_message) + '\n')
            file_handle.flush()  # Ensure data is written immediately
            
            print(f"[{topic}] Saved message at {enriched_message['timestamp']}")
            
        except Exception as e:
            print(f"Error saving message for topic {topic}: {e}")
    
    def _consume_topic(self, topic: str):
        """Consume messages from a specific topic."""
        print(f"Starting consumer thread for topic: {topic}")
        
        try:
            consumer = self._create_consumer(topic)
            self.consumers[topic] = consumer
            
            while self.running:
                try:
                    # Poll for messages
                    message_batch = consumer.poll(timeout_ms=1000)
                    
                    if not message_batch:
                        continue
                    
                    for topic_partition, messages in message_batch.items():
                        for message in messages:
                            if not self.running:
                                break
                                
                            # Create message info with metadata
                            message_info = {
                                'partition': topic_partition.partition,
                                'offset': message.offset,
                                'key': message.key.decode('utf-8') if message.key else None,
                                'value': message.value,
                                'timestamp': message.timestamp,
                                'timestamp_type': message.timestamp_type
                            }
                            
                            self._save_message(topic, message_info)
                
                except Exception as e:
                    if self.running:
                        print(f"Error consuming from topic {topic}: {e}")
                        time.sleep(5)  # Wait before retrying
                    
        except Exception as e:
            print(f"Fatal error in consumer thread for topic {topic}: {e}")
        finally:
            if topic in self.consumers:
                self.consumers[topic].close()
                print(f"Closed consumer for topic: {topic}")
    
    def start(self):
        """Start consuming from all topics."""
        print("Starting test consumer...")
        
        # Create a thread for each topic
        threads = []
        for topic in self.topics:
            thread = threading.Thread(target=self._consume_topic, args=(topic,))
            thread.daemon = True
            thread.start()
            threads.append(thread)
            time.sleep(1)  # Small delay between starting consumers
        
        print(f"Started {len(threads)} consumer threads")
        
        try:
            # Keep main thread alive
            while self.running:
                time.sleep(1)
        except KeyboardInterrupt:
            print("\nKeyboard interrupt received")
        finally:
            self.stop()
            
            # Wait for threads to finish
            print("Waiting for consumer threads to finish...")
            for thread in threads:
                thread.join(timeout=5)
            
            print("All consumer threads finished")
    
    def stop(self):
        """Stop all consumers and close files."""
        print("Stopping test consumer...")
        self.running = False
        
        # Close all consumers
        for topic, consumer in self.consumers.items():
            try:
                consumer.close()
                print(f"Closed consumer for topic: {topic}")
            except Exception as e:
                print(f"Error closing consumer for topic {topic}: {e}")
        
        # Close all file handles
        for topic, file_handle in self.file_handles.items():
            try:
                file_handle.close()
                print(f"Closed file for topic: {topic}")
            except Exception as e:
                print(f"Error closing file for topic {topic}: {e}")
        
        print("Test consumer stopped")


def get_available_topics(brokers: List[str]) -> List[str]:
    """Get list of available topics from Kafka cluster."""
    try:
        consumer = KafkaConsumer(
            bootstrap_servers=brokers,
            consumer_timeout_ms=5000,
            api_version=(0, 10, 1),
        )
        topics = consumer.topics()
        consumer.close()
        return list(topics)
    except Exception as e:
        print(f"Error getting topics from Kafka: {e}")
        # Return default topics if connection fails
        return [
            'on_site_meteo_station',
            'inverter', 
            'poi_meter',
            'system_alerts',
            'processed_data'
        ]


def main():
    """Main function."""
    parser = argparse.ArgumentParser(
        description='Test Kafka Consumer for SolarXAI',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    
    parser.add_argument(
        '--brokers',
        default='localhost:9092,localhost:9192,localhost:9292',
        help='Comma-separated list of Kafka brokers (default: localhost:9092,localhost:9192,localhost:9292)'
    )
    
    parser.add_argument(
        '--output-dir',
        default='./output',
        help='Directory to save message files (default: ./output)'
    )
    
    parser.add_argument(
        '--topics',
        help='Comma-separated list of topics to consume (default: auto-discover all topics)'
    )
    
    parser.add_argument(
        '--list-topics',
        action='store_true',
        help='List available topics and exit'
    )
    
    args = parser.parse_args()
    
    # Parse brokers
    brokers = [broker.strip() for broker in args.brokers.split(',')]
    
    # Get topics
    if args.topics:
        topics = [topic.strip() for topic in args.topics.split(',')]
    else:
        print("Auto-discovering topics...")
        topics = get_available_topics(brokers)
    
    # List topics if requested
    if args.list_topics:
        print("Available topics:")
        for topic in topics:
            print(f"  - {topic}")
        return
    
    if not topics:
        print("No topics found or specified. Exiting.")
        sys.exit(1)
    
    # Create and start consumer
    try:
        consumer = TestConsumer(brokers, args.output_dir, topics)
        consumer.start()
    except KeyboardInterrupt:
        print("\nExiting...")
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()