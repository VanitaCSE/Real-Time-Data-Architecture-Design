"""
🏞️ Storage Layer - AWS S3 Data Lake
Data lake for raw and processed pricing data
Maps to: AWS S3 + S3 Glacier
"""

import json
import logging
import boto3
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
from dataclasses import dataclass
from pathlib import Path
import gzip
import io

logger = logging.getLogger(__name__)

@dataclass
class S3Object:
    """S3 object representation"""
    bucket: str
    key: str
    size: int
    last_modified: datetime
    etag: str
    storage_class: str = 'STANDARD'

class S3DataLake:
    """
    AWS S3 Data Lake for pricing data storage
    Handles raw data, processed data, and archival
    Maps to: AWS S3 + S3 Glacier
    """
    
    def __init__(self, bucket_name: str = 'pricing-data-lake', region: str = 'us-east-1'):
        self.bucket_name = bucket_name
        self.region = region
        self.s3_client = None  # boto3.client('s3', region_name=region)
        self.use_local = True  # For local development
        
        # Data lake structure
        self.folders = {
            'raw': 'raw/events/',
            'processed': 'processed/events/',
            'analytics': 'analytics/aggregates/',
            'ml_models': 'ml/models/',
            'logs': 'logs/application/',
            'archive': 'archive/historical/'
        }
        
        self.storage_stats = {
            'objects_stored': 0,
            'bytes_stored': 0,
            'uploads_failed': 0,
            'start_time': datetime.now()
        }
    
    def store_raw_event(self, event_data: Dict[str, Any]) -> bool:
        """
        Store raw event data in S3
        Maps to: S3 PutObject for raw data ingestion
        """
        try:
            # Generate S3 key based on date
            timestamp = datetime.fromisoformat(event_data['timestamp'])
            s3_key = f"{self.folders['raw']}{timestamp.strftime('year=%Y/month=%m/day=%d/hour=%H')}/{event_data['event_id']}.json"
            
            # Prepare data
            data_bytes = json.dumps(event_data, default=str).encode('utf-8')
            
            if self.use_local:
                return self._store_local(s3_key, data_bytes, 'raw')
            else:
                return self._store_s3(s3_key, data_bytes, 'STANDARD')
                
        except Exception as e:
            self.storage_stats['uploads_failed'] += 1
            logger.error(f"Failed to store raw event: {e}")
            return False
    
    def store_processed_event(self, processed_event: Dict[str, Any]) -> bool:
        """
        Store processed event data in S3
        Maps to: S3 PutObject for processed data
        """
        try:
            # Generate S3 key with partitioning
            timestamp = datetime.fromisoformat(processed_event['processed_at'])
            s3_key = f"{self.folders['processed']}{timestamp.strftime('year=%Y/month=%m/day=%D')}/{processed_event['event_id']}.json"
            
            # Prepare data with compression
            data_bytes = json.dumps(processed_event, default=str).encode('utf-8')
            compressed_data = gzip.compress(data_bytes)
            
            if self.use_local:
                return self._store_local(s3_key, compressed_data, 'processed', compressed=True)
            else:
                return self._store_s3(s3_key, compressed_data, 'STANDARD', content_encoding='gzip')
                
        except Exception as e:
            self.storage_stats['uploads_failed'] += 1
            logger.error(f"Failed to store processed event: {e}")
            return False
    
    def store_batch_events(self, events: List[Dict[str, Any]], data_type: str = 'processed') -> Dict[str, int]:
        """
        Store batch of events in S3
        Maps to: S3 PutObject for batch uploads
        """
        results = {'success': 0, 'failed': 0}
        
        # Group events by hour for efficient storage
        events_by_hour = {}
        
        for event in events:
            timestamp = datetime.fromisoformat(event.get('timestamp', event.get('processed_at')))
            hour_key = timestamp.strftime('year=%Y/month=%m/day=%d/hour=%H')
            
            if hour_key not in events_by_hour:
                events_by_hour[hour_key] = []
            events_by_hour[hour_key].append(event)
        
        # Store each hour batch
        for hour_key, hour_events in events_by_hour.items():
            try:
                # Create batch file
                batch_data = {
                    'batch_info': {
                        'hour': hour_key,
                        'event_count': len(hour_events),
                        'created_at': datetime.now().isoformat()
                    },
                    'events': hour_events
                }
                
                # Generate S3 key
                s3_key = f"{self.folders[data_type]}{hour_key}/batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
                
                # Compress batch data
                data_bytes = json.dumps(batch_data, default=str).encode('utf-8')
                compressed_data = gzip.compress(data_bytes)
                
                if self.use_local:
                    success = self._store_local(s3_key, compressed_data, data_type, compressed=True)
                else:
                    success = self._store_s3(s3_key, compressed_data, 'STANDARD', content_encoding='gzip')
                
                if success:
                    results['success'] += len(hour_events)
                else:
                    results['failed'] += len(hour_events)
                    
            except Exception as e:
                logger.error(f"Failed to store batch for {hour_key}: {e}")
                results['failed'] += len(hour_events)
        
        logger.info(f"Batch storage complete - Success: {results['success']}, Failed: {results['failed']}")
        return results
    
    def store_analytics_data(self, analytics_data: Dict[str, Any], analytics_type: str) -> bool:
        """
        Store analytics aggregates in S3
        Maps to: S3 for analytics data storage
        """
        try:
            # Generate S3 key
            timestamp = datetime.now()
            s3_key = f"{self.folders['analytics']}{analytics_type}/{timestamp.strftime('year=%Y/month=%m/day=%D')}/{analytics_type}_{timestamp.strftime('%Y%m%d_%H%M%S')}.json"
            
            # Prepare data
            data_bytes = json.dumps(analytics_data, default=str).encode('utf-8')
            
            if self.use_local:
                return self._store_local(s3_key, data_bytes, 'analytics')
            else:
                return self._store_s3(s3_key, data_bytes, 'STANDARD')
                
        except Exception as e:
            logger.error(f"Failed to store analytics data: {e}")
            return False
    
    def store_ml_model(self, model_data: bytes, model_version: str) -> bool:
        """
        Store ML model artifacts in S3
        Maps to: S3 for ML model storage
        """
        try:
            # Generate S3 key for model
            s3_key = f"{self.folders['ml_models']}{model_version}/model.pkl"
            
            if self.use_local:
                return self._store_local(s3_key, model_data, 'ml_models')
            else:
                return self._store_s3(s3_key, model_data, 'STANDARD')
                
        except Exception as e:
            logger.error(f"Failed to store ML model: {e}")
            return False
    
    def archive_old_data(self, days_old: int = 90) -> Dict[str, int]:
        """
        Archive old data to Glacier
        Maps to: S3 lifecycle policy + Glacier
        """
        try:
            cutoff_date = datetime.now() - timedelta(days=days_old)
            archived_count = 0
            
            if self.use_local:
                # Simulate archival by moving files
                archived_count = self._archive_local_data(cutoff_date)
            else:
                # In production, use S3 lifecycle policies
                archived_count = self._archive_s3_data(cutoff_date)
            
            logger.info(f"Archived {archived_count} objects older than {days_old} days")
            return {'archived': archived_count}
            
        except Exception as e:
            logger.error(f"Failed to archive data: {e}")
            return {'archived': 0, 'error': str(e)}
    
    def query_events(self, start_date: datetime, end_date: datetime, 
                    data_type: str = 'processed') -> List[Dict[str, Any]]:
        """
        Query events from S3 data lake
        Maps to: S3 Select + Athena
        """
        try:
            events = []
            
            if self.use_local:
                events = self._query_local_events(start_date, end_date, data_type)
            else:
                # In production, use S3 Select or Athena
                events = self._query_s3_events(start_date, end_date, data_type)
            
            logger.info(f"Queried {len(events)} events from {data_type} data")
            return events
            
        except Exception as e:
            logger.error(f"Failed to query events: {e}")
            return []
    
    def get_storage_stats(self) -> Dict[str, Any]:
        """Get storage statistics"""
        runtime = datetime.now() - self.storage_stats['start_time']
        
        return {
            'objects_stored': self.storage_stats['objects_stored'],
            'bytes_stored': self.storage_stats['bytes_stored'],
            'uploads_failed': self.storage_stats['uploads_failed'],
            'success_rate': (
                self.storage_stats['objects_stored'] / 
                max(self.storage_stats['objects_stored'] + self.storage_stats['uploads_failed'], 1)
            ) * 100,
            'runtime_seconds': int(runtime.total_seconds()),
            'storage_classes': {
                'standard': self.storage_stats['bytes_stored'],
                'glacier': 0  # Would be tracked in production
            }
        }
    
    def _store_local(self, s3_key: str, data: bytes, data_type: str, compressed: bool = False) -> bool:
        """Store data locally (for development)"""
        try:
            # Create local directory structure
            local_path = Path('local_s3') / s3_key
            local_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Write data
            mode = 'wb' if compressed else 'w'
            with open(local_path, mode) as f:
                if compressed:
                    f.write(data)
                else:
                    f.write(data.decode('utf-8'))
            
            # Update stats
            self.storage_stats['objects_stored'] += 1
            self.storage_stats['bytes_stored'] += len(data)
            
            logger.debug(f"Stored locally: {s3_key} ({len(data)} bytes)")
            return True
            
        except Exception as e:
            logger.error(f"Failed to store locally: {e}")
            return False
    
    def _store_s3(self, s3_key: str, data: bytes, storage_class: str, 
                  content_encoding: Optional[str] = None) -> bool:
        """Store data in S3 (production)"""
        try:
            # In production, this would use boto3
            put_params = {
                'Bucket': self.bucket_name,
                'Key': s3_key,
                'Body': data,
                'StorageClass': storage_class
            }
            
            if content_encoding:
                put_params['ContentEncoding'] = content_encoding
            
            # self.s3_client.put_object(**put_params)
            
            # Update stats
            self.storage_stats['objects_stored'] += 1
            self.storage_stats['bytes_stored'] += len(data)
            
            logger.info(f"Stored in S3: {s3_key} ({len(data)} bytes)")
            return True
            
        except Exception as e:
            logger.error(f"Failed to store in S3: {e}")
            return False
    
    def _query_local_events(self, start_date: datetime, end_date: datetime, 
                           data_type: str) -> List[Dict[str, Any]]:
        """Query local events (for development)"""
        events = []
        base_path = Path('local_s3') / self.folders[data_type]
        
        if not base_path.exists():
            return events
        
        # Walk through directory structure
        for file_path in base_path.rglob('*.json'):
            try:
                # Parse date from file path
                file_date_str = file_path.parent.name
                if '=' in file_date_str:
                    file_date = datetime.strptime(file_date_str.split('=')[1], '%Y%m%d')
                    
                    if start_date <= file_date <= end_date:
                        with open(file_path, 'r') as f:
                            if file_path.name.startswith('batch_'):
                                # Batch file
                                batch_data = json.load(f)
                                events.extend(batch_data.get('events', []))
                            else:
                                # Single event file
                                event_data = json.load(f)
                                events.append(event_data)
                                
            except Exception as e:
                logger.warning(f"Failed to read {file_path}: {e}")
        
        return events
    
    def _archive_local_data(self, cutoff_date: datetime) -> int:
        """Archive local data (simulation)"""
        archived_count = 0
        archive_path = Path('local_s3') / self.folders['archive']
        
        for data_type in ['raw', 'processed']:
            source_path = Path('local_s3') / self.folders[data_type]
            
            if source_path.exists():
                for file_path in source_path.rglob('*.json'):
                    try:
                        file_mtime = datetime.fromtimestamp(file_path.stat().st_mtime)
                        
                        if file_mtime < cutoff_date:
                            # Move to archive
                            relative_path = file_path.relative_to(source_path)
                            archive_file_path = archive_path / data_type / relative_path
                            archive_file_path.parent.mkdir(parents=True, exist_ok=True)
                            
                            file_path.rename(archive_file_path)
                            archived_count += 1
                            
                    except Exception as e:
                        logger.warning(f"Failed to archive {file_path}: {e}")
        
        return archived_count

# Data partitioning utility
class DataPartitioner:
    """
    Utility for efficient data partitioning in S3
    Maps to: S3 partitioning strategy
    """
    
    @staticmethod
    def generate_partition_key(timestamp: datetime, partition_type: str = 'hour') -> str:
        """Generate partition key based on timestamp"""
        if partition_type == 'hour':
            return f"year={timestamp.year}/month={timestamp.month:02d}/day={timestamp.day:02d}/hour={timestamp.hour:02d}"
        elif partition_type == 'day':
            return f"year={timestamp.year}/month={timestamp.month:02d}/day={timestamp.day:02d}"
        elif partition_type == 'month':
            return f"year={timestamp.year}/month={timestamp.month:02d}"
        else:
            return f"year={timestamp.year}"
    
    @staticmethod
    def get_optimal_partition_size(data_volume_gb: float) -> str:
        """Get optimal partition size based on data volume"""
        if data_volume_gb < 1:
            return 'hour'  # For small datasets
        elif data_volume_gb < 100:
            return 'day'   # For medium datasets
        else:
            return 'month' # For large datasets

# Test function
def test_s3_data_lake():
    """Test S3 data lake locally"""
    data_lake = S3DataLake()
    
    # Store sample events
    sample_events = []
    for i in range(5):
        event = {
            'event_id': i + 1,
            'demand': 50 + (i % 20),
            'price': 100 + (i * 2),
            'timestamp': (datetime.now() - timedelta(hours=i)).isoformat(),
            'processed_at': (datetime.now() - timedelta(hours=i)).isoformat(),
            'pricing_tier': 'Medium',
            'pricing_method': 'ml'
        }
        sample_events.append(event)
    
    # Store events
    for event in sample_events:
        success = data_lake.store_processed_event(event)
        print(f"Stored event {event['event_id']}: {success}")
    
    # Store batch
    batch_results = data_lake.store_batch_events(sample_events)
    print(f"Batch results: {batch_results}")
    
    # Query events
    queried_events = data_lake.query_events(
        start_date=datetime.now() - timedelta(hours=6),
        end_date=datetime.now()
    )
    print(f"Queried {len(queried_events)} events")
    
    # Get stats
    stats = data_lake.get_storage_stats()
    print(f"Storage stats: {json.dumps(stats, indent=2, default=str)}")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    test_s3_data_lake()
