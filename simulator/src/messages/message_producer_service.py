import json
import os
from confluent_kafka import Producer


class MessageProducerService:
    
    def __init__(self, guid=None):
        # Get broker addresses from environment variable, fallback to localhost
        kafka_brokers = os.environ.get('KAFKA_BROKERS', 'localhost:9092')
        
        self._producer = Producer({
            'bootstrap.servers': kafka_brokers,
            'acks': 'all',
        })
        self.guid = guid
        broker_list = [broker.strip() for broker in kafka_brokers.split(',')]
        print(f"MessageProducerService initialized with brokers: {broker_list}")
        

    def send_message(self, topic: str, message: dict):
        try:
            # Add GUID to the message
            message_with_guid = message.copy()
            message_with_guid['guid'] = self.guid
            
            print(f"Sending message to topic {topic}: {message_with_guid}")
            
            # Send message to the topic
            self._producer.produce(
                topic=topic,
                value=json.dumps(message_with_guid).encode('utf-8')
            )
            
            # Wait for any pending messages to be delivered
            self._producer.flush()
            print(f"Message sent to topic {topic}")
            
            return 0
        except Exception as e:
            print(f"Error sending message to topic {topic}: {e}")
            raise e
    
    def close(self):
        """Close the producer connection"""
        if self._producer:
            self._producer.flush()
