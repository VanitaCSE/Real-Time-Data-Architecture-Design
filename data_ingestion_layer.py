"""
STEP 1: Data Ingestion Layer - Real Streaming Simulation
Mimics Apache Kafka using Python queues and threading
"""

import time
import threading
import queue
import random
import json
from datetime import datetime
from typing import Dict, Any
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataProducer:
    """
    Data Producer - Simulates real-time data generation
    Like Kafka producers sending events to topics
    """
    
    def __init__(self, event_queue: queue.Queue, production_interval: float = 1.0):
        self.event_queue = event_queue
        self.production_interval = production_interval
        self.is_running = False
        self.event_counter = 0
        self.producer_thread = None
        
        # Demand simulation parameters (realistic patterns)
        self.base_demand = 50
        self.demand_volatility = 20
        self.trend_factor = 0.1
        
    def generate_demand_event(self) -> Dict[str, Any]:
        """
        Generate realistic demand event with patterns
        Simulates real-world demand fluctuations
        """
        self.event_counter += 1
        
        # Simulate demand with trend and volatility
        trend_component = self.event_counter * self.trend_factor
        random_component = random.gauss(0, self.demand_volatility)
        seasonal_component = 10 * math.sin(self.event_counter * 0.1)
        
        demand = max(10, self.base_demand + trend_component + random_component + seasonal_component)
        
        event = {
            'event_id': self.event_counter,
            'timestamp': datetime.now().isoformat(),
            'demand': round(demand, 2),
            'source': 'data_producer',
            'event_type': 'demand_update'
        }
        
        return event
    
    def start_production(self):
        """Start continuous event production"""
        if self.is_running:
            logger.warning("Producer is already running")
            return
            
        self.is_running = True
        self.producer_thread = threading.Thread(target=self._production_loop, daemon=True)
        self.producer_thread.start()
        logger.info("🚀 Data Producer started - generating events every 1 second")
        
    def stop_production(self):
        """Stop event production"""
        self.is_running = False
        if self.producer_thread:
            self.producer_thread.join(timeout=2)
        logger.info("⏹️ Data Producer stopped")
        
    def _production_loop(self):
        """Main production loop - runs every second"""
        while self.is_running:
            try:
                # Generate event
                event = self.generate_demand_event()
                
                # Send to queue (like Kafka topic)
                self.event_queue.put(event)
                
                logger.info(f"📦 Produced event #{event['event_id']} - Demand: {event['demand']}")
                
                # Wait for next interval
                time.sleep(self.production_interval)
                
            except Exception as e:
                logger.error(f"❌ Error in production loop: {e}")
                break

class StreamProcessor:
    """
    Stream Processor - Simulates real-time stream processing
    Like Apache Spark Streaming or Apache Flink
    """
    
    def __init__(self, input_queue: queue.Queue, output_queue: queue.Queue):
        self.input_queue = input_queue
        self.output_queue = output_queue
        self.is_running = False
        self.processor_thread = None
        
        # Processing state
        self.processed_events = []
        self.window_size = 10  # Last 10 events for moving average
        
    def start_processing(self):
        """Start stream processing"""
        if self.is_running:
            logger.warning("Processor is already running")
            return
            
        self.is_running = True
        self.processor_thread = threading.Thread(target=self._processing_loop, daemon=True)
        self.processor_thread.start()
        logger.info("⚡ Stream Processor started - processing events in real-time")
        
    def stop_processing(self):
        """Stop stream processing"""
        self.is_running = False
        if self.processor_thread:
            self.processor_thread.join(timeout=2)
        logger.info("⏹️ Stream Processor stopped")
        
    def _processing_loop(self):
        """Main processing loop - processes events as they arrive"""
        while self.is_running:
            try:
                # Get event from queue (blocking)
                event = self.input_queue.get(timeout=1)
                
                # Process event
                processed_event = self._process_event(event)
                
                # Send to output queue
                self.output_queue.put(processed_event)
                
                logger.info(f"⚙️ Processed event #{processed_event['event_id']}")
                
            except queue.Empty:
                continue  # No events available
            except Exception as e:
                logger.error(f"❌ Error in processing loop: {e}")
                
    def _process_event(self, event: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process individual event with real-time analytics
        Calculates moving averages, trends, etc.
        """
        # Add to processed events
        self.processed_events.append(event)
        
        # Keep only last N events for window calculations
        if len(self.processed_events) > self.window_size:
            self.processed_events = self.processed_events[-self.window_size:]
        
        # Calculate moving average
        moving_avg = sum(e['demand'] for e in self.processed_events) / len(self.processed_events)
        
        # Calculate trend
        if len(self.processed_events) >= 2:
            recent_avg = sum(e['demand'] for e in self.processed_events[-3:]) / min(3, len(self.processed_events))
            older_avg = sum(e['demand'] for e in self.processed_events[:3]) / min(3, len(self.processed_events))
            
            if recent_avg > older_avg * 1.05:
                trend = 'increasing'
            elif recent_avg < older_avg * 0.95:
                trend = 'decreasing'
            else:
                trend = 'stable'
        else:
            trend = 'insufficient_data'
        
        # Add processing metadata
        processed_event.update({
            'processed_at': datetime.now().isoformat(),
            'moving_average': round(moving_avg, 2),
            'trend': trend,
            'window_size': len(self.processed_events),
            'processing_latency_ms': self._calculate_latency(event['timestamp'])
        })
        
        return processed_event
    
    def _calculate_latency(self, event_timestamp: str) -> float:
        """Calculate processing latency in milliseconds"""
        try:
            event_time = datetime.fromisoformat(event_timestamp.replace('Z', '+00:00'))
            current_time = datetime.now()
            latency = (current_time - event_time).total_seconds() * 1000
            return round(latency, 2)
        except:
            return 0.0

# Initialize streaming components
def create_streaming_pipeline():
    """
    Create complete streaming pipeline
    Producer -> Queue -> Processor -> Queue
    """
    # Create queues (like Kafka topics)
    raw_events_queue = queue.Queue(maxsize=1000)  # Raw events
    processed_events_queue = queue.Queue(maxsize=1000)  # Processed events
    
    # Create components
    producer = DataProducer(raw_events_queue)
    processor = StreamProcessor(raw_events_queue, processed_events_queue)
    
    return {
        'producer': producer,
        'processor': processor,
        'raw_queue': raw_events_queue,
        'processed_queue': processed_events_queue
    }

# Example usage
if __name__ == "__main__":
    # Create streaming pipeline
    pipeline = create_streaming_pipeline()
    
    # Start components
    pipeline['producer'].start_production()
    pipeline['processor'].start_processing()
    
    # Consume processed events
    try:
        while True:
            event = pipeline['processed_queue'].get()
            print(f"📊 Consumed: {json.dumps(event, indent=2)}")
    except KeyboardInterrupt:
        print("\n🛑 Shutting down...")
        pipeline['producer'].stop_production()
        pipeline['processor'].stop_processing()
