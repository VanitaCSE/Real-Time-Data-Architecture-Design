"""
🌊 Data Ingestion Layer - AWS Kinesis Producer
Real-time data ingestion for pricing events
Maps to: AWS Kinesis Data Streams
"""

import json
import time
import logging
from datetime import datetime
from typing import Dict, Any, Optional
import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)

class KinesisProducer:
    """
    AWS Kinesis Data Streams Producer
    Handles real-time data ingestion from multiple sources
    """
    
    def __init__(self, stream_name: str = 'pricing-events', region: str = 'us-east-1'):
        self.stream_name = stream_name
        self.region = region
        self.kinesis_client = boto3.client('kinesis', region_name=region)
        self.producer_stats = {
            'records_sent': 0,
            'records_failed': 0,
            'bytes_sent': 0,
            'start_time': datetime.now()
        }
    
    def send_pricing_event(self, event_data: Dict[str, Any]) -> bool:
        """
        Send pricing event to Kinesis stream
        Maps to: Kinesis PutRecord API
        """
        try:
            # Add metadata
            enriched_event = {
                **event_data,
                'ingestion_timestamp': datetime.now().isoformat(),
                'source': 'kinesis_producer',
                'partition_key': f"demand_{event_data.get('demand', 0)}"
            }
            
            # Serialize event
            payload = json.dumps(enriched_event)
            
            # Send to Kinesis
            response = self.kinesis_client.put_record(
                StreamName=self.stream_name,
                Data=payload,
                PartitionKey=enriched_event['partition_key']
            )
            
            # Update stats
            self.producer_stats['records_sent'] += 1
            self.producer_stats['bytes_sent'] += len(payload)
            
            logger.info(f"Event sent to Kinesis - SequenceNumber: {response['SequenceNumber']}")
            return True
            
        except ClientError as e:
            self.producer_stats['records_failed'] += 1
            logger.error(f"Failed to send event to Kinesis: {e}")
            return False
    
    def send_batch_events(self, events: list) -> Dict[str, int]:
        """
        Send multiple events in batch
        Maps to: Kinesis PutRecords API
        """
        results = {'success': 0, 'failed': 0}
        
        # Prepare batch records
        records = []
        for event in events:
            enriched_event = {
                **event,
                'ingestion_timestamp': datetime.now().isoformat(),
                'source': 'kinesis_producer',
                'partition_key': f"demand_{event.get('demand', 0)}"
            }
            
            records.append({
                'Data': json.dumps(enriched_event),
                'PartitionKey': enriched_event['partition_key']
            })
        
        # Send batch
        try:
            response = self.kinesis_client.put_records(
                StreamName=self.stream_name,
                Records=records
            )
            
            # Process results
            for record_result in response['Records']:
                if 'ErrorCode' in record_result:
                    results['failed'] += 1
                else:
                    results['success'] += 1
            
            # Update stats
            self.producer_stats['records_sent'] += results['success']
            self.producer_stats['records_failed'] += results['failed']
            
            logger.info(f"Batch sent - Success: {results['success']}, Failed: {results['failed']}")
            
        except ClientError as e:
            logger.error(f"Batch send failed: {e}")
            results['failed'] = len(events)
        
        return results
    
    def get_producer_stats(self) -> Dict[str, Any]:
        """Get producer statistics"""
        runtime = datetime.now() - self.producer_stats['start_time']
        
        return {
            'records_sent': self.producer_stats['records_sent'],
            'records_failed': self.producer_stats['records_failed'],
            'bytes_sent': self.producer_stats['bytes_sent'],
            'success_rate': (
                self.producer_stats['records_sent'] / 
                max(self.producer_stats['records_sent'] + self.producer_stats['records_failed'], 1)
            ) * 100,
            'runtime_seconds': int(runtime.total_seconds()),
            'throughput_per_second': round(
                self.producer_stats['records_sent'] / max(runtime.total_seconds(), 1), 2
            )
        }

# Usage example for local development
class LocalKinesisSimulator:
    """
    Local simulator for Kinesis during development
    Replaces AWS Kinesis with local file-based system
    """
    
    def __init__(self, stream_name: str = 'local-pricing-events'):
        self.stream_name = stream_name
        self.events_file = f"{stream_name}.jsonl"
        self.producer_stats = {
            'records_sent': 0,
            'records_failed': 0,
            'start_time': datetime.now()
        }
    
    def send_pricing_event(self, event_data: Dict[str, Any]) -> bool:
        """Simulate sending event to local file"""
        try:
            enriched_event = {
                **event_data,
                'ingestion_timestamp': datetime.now().isoformat(),
                'source': 'local_simulator'
            }
            
            with open(self.events_file, 'a') as f:
                f.write(json.dumps(enriched_event) + '\n')
            
            self.producer_stats['records_sent'] += 1
            logger.info(f"Event sent to local stream - Event ID: {event_data.get('event_id')}")
            return True
            
        except Exception as e:
            self.producer_stats['records_failed'] += 1
            logger.error(f"Failed to send event to local stream: {e}")
            return False

# Factory function
def create_producer(use_local: bool = True, stream_name: str = 'pricing-events') -> object:
    """
    Factory function to create appropriate producer
    """
    if use_local:
        return LocalKinesisSimulator(stream_name)
    else:
        return KinesisProducer(stream_name)

if __name__ == "__main__":
    # Example usage
    producer = create_producer(use_local=True)
    
    # Send sample event
    sample_event = {
        'event_id': 1,
        'demand': 75,
        'timestamp': datetime.now().isoformat(),
        'source': 'api_gateway'
    }
    
    success = producer.send_pricing_event(sample_event)
    print(f"Event sent successfully: {success}")
    print(f"Producer stats: {producer.get_producer_stats()}")
