from confluent_kafka import Consumer, TopicPartition
from typing import Dict, Any, List
import json
import time
from dagster import AssetExecutionContext
from etl_pipeline.utils.constants import STOCK_PRICE_TOPIC, KAFKA_BATCH_SIZE, KAFKA_BATCH_TIMEOUT,KAFKA_BOOTSTRAP_SERVERS
import logging

logging.basicConfig(level=logging.INFO)
class KafkaClient:
    def __init__(
        self,
        context: AssetExecutionContext=None,
        bootstrap_servers: str = KAFKA_BOOTSTRAP_SERVERS,
        group_id: str = 'dagster',
        batch_size: int = KAFKA_BATCH_SIZE,
        batch_timeout: float = KAFKA_BATCH_TIMEOUT,
        topic: str = STOCK_PRICE_TOPIC
    ):
        self.context = context
        self.config = {
            'bootstrap.servers': bootstrap_servers,
            'group.id': group_id,
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False,
            'session.timeout.ms': 10000,
            'max.poll.interval.ms': 300000
        }
        self.batch_size = batch_size
        self.batch_timeout = batch_timeout
        self.topic = topic
        self.consumer = None
        self.current_offsets = {}  # Track offsets per partition

    def connect(self):
        self.consumer = Consumer(self.config)
        self.consumer.subscribe([self.topic])

    def consume_batch(self) -> List[Dict[str, Any]]:
        if not self.consumer:
            self.connect()
            
        messages = []
        start_time = time.time()
        
        # Reset offset tracking for new batch
        self.current_offsets = {}
        
        while len(messages) < self.batch_size and (time.time() - start_time) < self.batch_timeout:
            batch = self.consumer.consume(num_messages=self.batch_size - len(messages), timeout=1.0)
            
            for msg in batch:
                if msg is None or msg.error():
                    continue
                    
                try:
                    value = json.loads(msg.value().decode('utf-8'))
                    messages.append(value)
                    
                    # Track the latest offset for each partition
                    tp = TopicPartition(msg.topic(), msg.partition(), msg.offset() + 1)
                    self.current_offsets[msg.partition()] = tp
                    
                except Exception as e:
                    self.context.log.error(f"Error processing message: {e}")
                    continue
                    
        return messages
    
    def commit(self):
        """Commit consumed offsets"""
        if not self.consumer or not self.current_offsets:
            return
            
        try:
            # Commit stored offsets
            offsets = list(self.current_offsets.values())
            self.consumer.commit(offsets=offsets)
            self.context.log.info(f"Successfully committed offsets: {offsets}")
        except Exception as e:
            self.context.log.info(f"Error committing offsets: {e}")
    
    def close(self):
        if self.consumer:
            try:
                self.commit()  # Final commit before closing
            finally:
                self.context.log.info("Closing Kafka consumer")
                self.consumer.close()