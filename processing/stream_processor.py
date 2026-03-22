"""
⚡ Processing Layer - AWS Lambda Stream Processor
Real-time stream processing with window analytics
Maps to: AWS Lambda + Kinesis Data Analytics
"""

import json
import logging
import time
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
from dataclasses import dataclass, field
from collections import deque
import statistics

logger = logging.getLogger(__name__)

@dataclass
class WindowStats:
    """Window statistics for stream processing"""
    moving_avg: float = 50.0
    window_min: float = 50.0
    window_max: float = 50.0
    volatility: float = 0.0
    trend_strength: float = 0.0
    window_size: int = 0
    last_updated: datetime = field(default_factory=datetime.now)

@dataclass
class ProcessedEvent:
    """Processed event structure"""
    event_id: int
    demand: int
    price: float
    pricing_tier: str
    pricing_method: str
    timestamp: str
    processed_at: str
    window_stats: WindowStats
    metadata: Dict[str, Any] = field(default_factory=dict)

class StreamProcessor:
    """
    AWS Lambda stream processor for real-time pricing events
    Handles sliding window analytics and ML integration
    """
    
    def __init__(self, window_size: int = 10):
        self.window_size = window_size
        self.demand_window = deque(maxlen=window_size)
        self.window_stats = WindowStats()
        self.processing_stats = {
            'events_processed': 0,
            'events_failed': 0,
            'processing_times': [],
            'start_time': datetime.now()
        }
        
        # Import ML predictor (will be created separately)
        try:
            from ml_predictor import MLPredictor
            self.ml_predictor = MLPredictor()
        except ImportError:
            logger.warning("ML predictor not available, using rule-based pricing")
            self.ml_predictor = None
    
    def process_kinesis_record(self, record: Dict[str, Any]) -> Optional[ProcessedEvent]:
        """
        Process single Kinesis record
        Maps to: Lambda function for Kinesis stream processing
        """
        start_time = time.time()
        
        try:
            # Parse Kinesis record
            event_data = json.loads(record['data'])
            
            # Update sliding window
            self._update_demand_window(event_data['demand'])
            
            # Process event with pricing
            processed_event = self._process_event(event_data)
            
            # Update stats
            processing_time = (time.time() - start_time) * 1000
            self.processing_stats['processing_times'].append(processing_time)
            self.processing_stats['events_processed'] += 1
            
            # Keep only last 1000 processing times
            if len(self.processing_stats['processing_times']) > 1000:
                self.processing_stats['processing_times'] = self.processing_stats['processing_times'][-1000:]
            
            logger.info(f"Processed event #{processed_event.event_id} - "
                       f"Price: ${processed_event.price} ({processed_event.pricing_method.upper()}) "
                       f"in {processing_time:.2f}ms")
            
            return processed_event
            
        except Exception as e:
            self.processing_stats['events_failed'] += 1
            logger.error(f"Failed to process record: {e}")
            return None
    
    def process_batch_records(self, records: List[Dict[str, Any]]) -> List[ProcessedEvent]:
        """
        Process batch of Kinesis records
        Maps to: Lambda batch processing
        """
        processed_events = []
        
        for record in records:
            processed_event = self.process_kinesis_record(record)
            if processed_event:
                processed_events.append(processed_event)
        
        logger.info(f"Batch processing complete - Success: {len(processed_events)}, Failed: {len(records) - len(processed_events)}")
        return processed_events
    
    def _update_demand_window(self, new_demand: int) -> None:
        """
        Update sliding window with new demand value
        Maps to: Kinesis Data Analytics window functions
        """
        # Add new demand to window
        self.demand_window.append(new_demand)
        
        # Calculate window statistics
        if len(self.demand_window) >= 3:  # Need at least 3 points for meaningful stats
            self._calculate_window_stats()
        
        self.window_stats.last_updated = datetime.now()
        logger.debug(f"Window updated: {len(self.demand_window)} points, avg: {self.window_stats.moving_avg:.1f}")
    
    def _calculate_window_stats(self) -> None:
        """
        Calculate comprehensive window statistics
        Maps to: Streaming analytics aggregations
        """
        if not self.demand_window:
            return
        
        # Basic statistics
        self.window_stats.moving_avg = statistics.mean(self.demand_window)
        self.window_stats.window_min = min(self.demand_window)
        self.window_stats.window_max = max(self.demand_window)
        self.window_stats.window_size = len(self.demand_window)
        
        # Advanced statistics
        if len(self.demand_window) >= 2:
            self.window_stats.volatility = statistics.stdev(self.demand_window) / max(self.window_stats.moving_avg, 1)
        
        # Trend analysis (linear regression slope)
        if len(self.demand_window) >= 3:
            self.window_stats.trend_strength = self._calculate_trend_strength()
    
    def _calculate_trend_strength(self) -> float:
        """
        Calculate trend strength using linear regression
        Maps to: Kinesis Data Analytics advanced analytics
        """
        n = len(self.demand_window)
        x_values = list(range(n))
        y_values = list(self.demand_window)
        
        # Calculate slope (trend strength)
        x_mean = sum(x_values) / n
        y_mean = sum(y_values) / n
        
        numerator = sum((x_values[i] - x_mean) * (y_values[i] - y_mean) for i in range(n))
        denominator = sum((x_values[i] - x_mean) ** 2 for i in range(n))
        
        if denominator != 0:
            return numerator / denominator
        else:
            return 0
    
    def _process_event(self, event_data: Dict[str, Any]) -> ProcessedEvent:
        """
        Process single event with pricing calculation
        Maps to: Lambda business logic
        """
        # Get pricing using ML or rule-based
        pricing_result = self._calculate_pricing(event_data['demand'])
        
        # Create processed event
        processed_event = ProcessedEvent(
            event_id=event_data['event_id'],
            demand=event_data['demand'],
            price=pricing_result['price'],
            pricing_tier=pricing_result['pricing_tier'],
            pricing_method=pricing_result['pricing_method'],
            timestamp=event_data['timestamp'],
            processed_at=datetime.now().isoformat(),
            window_stats=WindowStats(
                moving_avg=self.window_stats.moving_avg,
                window_min=self.window_stats.window_min,
                window_max=self.window_stats.window_max,
                volatility=self.window_stats.volatility,
                trend_strength=self.window_stats.trend_strength,
                window_size=self.window_stats.window_size,
                last_updated=self.window_stats.last_updated
            ),
            metadata={
                'source': event_data.get('source', 'unknown'),
                'ingestion_timestamp': event_data.get('ingestion_timestamp'),
                'api_gateway_request_id': event_data.get('api_gateway_request_id')
            }
        )
        
        return processed_event
    
    def _calculate_pricing(self, demand: int) -> Dict[str, Any]:
        """
        Calculate pricing using ML or rule-based logic
        Maps to: ML model inference or business rules
        """
        # Try ML prediction first
        if self.ml_predictor and self.ml_predictor.is_available():
            try:
                ml_result = self.ml_predictor.predict_price(
                    demand=demand,
                    moving_avg=self.window_stats.moving_avg,
                    volatility=self.window_stats.volatility,
                    trend_strength=self.window_stats.trend_strength
                )
                
                return {
                    'price': ml_result['predicted_price'],
                    'pricing_tier': ml_result['pricing_tier'],
                    'pricing_method': 'ml'
                }
                
            except Exception as e:
                logger.warning(f"ML prediction failed, using rule-based: {e}")
        
        # Fallback to rule-based pricing
        return self._rule_based_pricing(demand)
    
    def _rule_based_pricing(self, demand: int) -> Dict[str, Any]:
        """
        Rule-based pricing fallback
        Maps to: Traditional business logic
        """
        base_price = 100.0
        
        # Traditional tier-based pricing
        if demand <= 33:
            tier_multiplier = 1.0
            tier = 'Low'
        elif demand <= 66:
            tier_multiplier = 1.2
            tier = 'Medium'
        else:
            tier_multiplier = 1.5
            tier = 'High'
        
        # Window-based adjustments
        if self.window_stats.window_size >= 5:
            avg_adjustment = (self.window_stats.moving_avg - 50) * 0.002
            tier_multiplier += avg_adjustment
        
        # Volatility adjustment
        if self.window_stats.volatility > 0.2:
            tier_multiplier += 0.05
        
        # Trend adjustment
        if self.window_stats.trend_strength > 1:
            tier_multiplier += 0.03
        
        final_price = round(base_price * tier_multiplier, 2)
        
        return {
            'price': final_price,
            'pricing_tier': tier,
            'pricing_method': 'rule_based'
        }
    
    def get_processing_stats(self) -> Dict[str, Any]:
        """Get processing statistics"""
        runtime = datetime.now() - self.processing_stats['start_time']
        
        # Calculate performance metrics
        if self.processing_stats['processing_times']:
            avg_time = statistics.mean(self.processing_stats['processing_times'])
            p95_time = sorted(self.processing_stats['processing_times'])[int(len(self.processing_stats['processing_times']) * 0.95)]
        else:
            avg_time = p95_time = 0
        
        return {
            'events_processed': self.processing_stats['events_processed'],
            'events_failed': self.processing_stats['events_failed'],
            'success_rate': (
                self.processing_stats['events_processed'] / 
                max(self.processing_stats['events_processed'] + self.processing_stats['events_failed'], 1)
            ) * 100,
            'runtime_seconds': int(runtime.total_seconds()),
            'events_per_second': round(
                self.processing_stats['events_processed'] / max(runtime.total_seconds(), 1), 2
            ),
            'performance': {
                'avg_processing_time_ms': round(avg_time, 2),
                'p95_processing_time_ms': round(p95_time, 2),
                'min_processing_time_ms': min(self.processing_stats['processing_times']) if self.processing_stats['processing_times'] else 0,
                'max_processing_time_ms': max(self.processing_stats['processing_times']) if self.processing_stats['processing_times'] else 0
            },
            'window_stats': {
                'current_window_size': len(self.demand_window),
                'max_window_size': self.window_size,
                'moving_avg': round(self.window_stats.moving_avg, 2),
                'volatility': round(self.window_stats.volatility, 3),
                'trend_strength': round(self.window_stats.trend_strength, 2)
            }
        }

# AWS Lambda handler function
def lambda_handler(event, context):
    """
    AWS Lambda handler for Kinesis stream processing
    Maps to: Lambda function entry point
    """
    logger.info(f"Lambda function invoked with {len(event.get('Records', []))} records")
    
    # Initialize processor
    processor = StreamProcessor()
    
    # Process records
    processed_events = []
    for record in event.get('Records', []):
        if record.get('eventSource') == 'aws:kinesis':
            processed_event = processor.process_kinesis_record(record)
            if processed_event:
                processed_events.append(processed_event)
    
    # Send to storage layer (would be S3/DynamoDB in production)
    # For now, just log the results
    logger.info(f"Successfully processed {len(processed_events)} events")
    
    # Return response
    return {
        'statusCode': 200,
        'body': json.dumps({
            'processed_events': len(processed_events),
            'stats': processor.get_processing_stats()
        })
    }

# Local testing function
def test_stream_processor():
    """Test stream processor locally"""
    processor = StreamProcessor()
    
    # Simulate incoming records
    test_records = [
        {
            'data': json.dumps({
                'event_id': i,
                'demand': 50 + (i % 20),
                'timestamp': datetime.now().isoformat(),
                'source': 'test'
            })
        }
        for i in range(15)
    ]
    
    # Process records
    for record in test_records:
        processed_event = processor.process_kinesis_record(record)
        if processed_event:
            print(f"Processed: {processed_event}")
    
    # Print stats
    stats = processor.get_processing_stats()
    print(f"Processing Stats: {json.dumps(stats, indent=2, default=str)}")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    test_stream_processor()
