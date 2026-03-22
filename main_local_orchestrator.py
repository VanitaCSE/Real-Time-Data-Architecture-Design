"""
🎯 Local Orchestrator - No AWS Dependencies Required
Complete ML pricing system using only local components
Perfect for development and testing without cloud dependencies
"""

import json
import logging
import time
from datetime import datetime
from typing import Dict, Any, List, Optional
from dataclasses import dataclass
import threading
import random
import statistics
from collections import deque
import os

logger = logging.getLogger(__name__)

@dataclass
class SystemStatus:
    """System status overview"""
    ingestion_layer: bool = False
    processing_layer: bool = False
    storage_layer: bool = False
    serving_layer: bool = False
    monitoring_layer: bool = False
    ml_model_ready: bool = False
    overall_health: str = 'initializing'

class LocalDataProducer:
    """Local data producer (simulates Kinesis)"""
    
    def __init__(self):
        self.events = []
        self.stats = {'sent': 0, 'failed': 0}
    
    def send_event(self, event_data):
        """Simulate sending event"""
        try:
            self.events.append(event_data)
            self.stats['sent'] += 1
            return True
        except:
            self.stats['failed'] += 1
            return False

class LocalMLPredictor:
    """Local ML predictor (simulates SageMaker)"""
    
    def __init__(self):
        self.is_trained = False
        self.model = None
        self._train_model()
    
    def _train_model(self):
        """Train simple ML model"""
        # Simulate training with rule-based logic
        self.is_trained = True
        logger.info("Local ML model trained successfully")
    
    def predict_price(self, demand, moving_avg=50, volatility=0, trend_strength=0):
        """Predict price using simple rules"""
        base_price = 100.0
        
        # Demand-based pricing
        if demand <= 33:
            multiplier = 1.0
            tier = 'Low'
        elif demand <= 66:
            multiplier = 1.2
            tier = 'Medium'
        else:
            multiplier = 1.5
            tier = 'High'
        
        # Add some ML-like complexity
        ml_adjustment = (trend_strength * 2) + (volatility * 10)
        price = base_price * multiplier + ml_adjustment
        
        return {
            'predicted_price': round(price, 2),
            'pricing_tier': tier,
            'confidence': 85.0 + random.uniform(-5, 5)
        }

class LocalStreamProcessor:
    """Local stream processor (simulates Lambda)"""
    
    def __init__(self):
        self.demand_window = deque(maxlen=10)
        self.stats = {
            'events_processed': 0,
            'events_failed': 0,
            'ml_predictions': 0,
            'processing_times': []
        }
        self.ml_predictor = LocalMLPredictor()
    
    def process_event(self, event_data):
        """Process single event"""
        start_time = time.time()
        
        try:
            # Update window
            self.demand_window.append(event_data['demand'])
            
            # Calculate window stats
            if len(self.demand_window) >= 3:
                moving_avg = statistics.mean(self.demand_window)
                volatility = statistics.stdev(self.demand_window) / max(moving_avg, 1)
            else:
                moving_avg = event_data['demand']
                volatility = 0
            
            # Get ML prediction
            ml_result = self.ml_predictor.predict_price(
                demand=event_data['demand'],
                moving_avg=moving_avg,
                volatility=volatility,
                trend_strength=random.uniform(-1, 1)
            )
            
            # Create processed event
            processed_event = {
                'event_id': event_data['event_id'],
                'demand': event_data['demand'],
                'price': ml_result['predicted_price'],
                'pricing_tier': ml_result['pricing_tier'],
                'pricing_method': 'ml',
                'timestamp': event_data['timestamp'],
                'processed_at': datetime.now().isoformat(),
                'moving_avg': round(moving_avg, 2),
                'volatility': round(volatility, 3),
                'confidence': ml_result['confidence']
            }
            
            # Update stats
            processing_time = (time.time() - start_time) * 1000
            self.stats['processing_times'].append(processing_time)
            self.stats['events_processed'] += 1
            self.stats['ml_predictions'] += 1
            
            # Keep only last 100 times
            if len(self.stats['processing_times']) > 100:
                self.stats['processing_times'] = self.stats['processing_times'][-100:]
            
            logger.info(f"Processed event #{processed_event['event_id']} - Price: ${processed_event['price']} (ML)")
            
            return processed_event
            
        except Exception as e:
            self.stats['events_failed'] += 1
            logger.error(f"Processing failed: {e}")
            return None

class LocalStorage:
    """Local storage (simulates S3/DynamoDB)"""
    
    def __init__(self):
        self.events = []
        self.stats = {'stored': 0, 'failed': 0}
    
    def store_event(self, event_data):
        """Store event locally"""
        try:
            self.events.append(event_data)
            self.stats['stored'] += 1
            return True
        except:
            self.stats['failed'] += 1
            return False

class LocalCache:
    """Local cache (simulates Redis)"""
    
    def __init__(self):
        self.cache = {}
        self.stats = {'hits': 0, 'misses': 0, 'sets': 0}
    
    def get(self, key):
        """Get from cache"""
        if key in self.cache:
            self.stats['hits'] += 1
            return self.cache[key]
        self.stats['misses'] += 1
        return None
    
    def set(self, key, value, ttl_seconds=300):
        """Set in cache"""
        self.cache[key] = value
        self.stats['sets'] += 1
        return True

class LocalMonitor:
    """Local monitoring (simulates CloudWatch)"""
    
    def __init__(self):
        self.metrics = []
        self.alerts = []
        self.stats = {'metrics_published': 0, 'alerts_triggered': 0}
    
    def publish_metric(self, name, value, unit='Count'):
        """Publish metric"""
        metric = {
            'name': name,
            'value': value,
            'unit': unit,
            'timestamp': datetime.now().isoformat()
        }
        self.metrics.append(metric)
        self.stats['metrics_published'] += 1
        
        # Check for alerts
        if name == 'ErrorRate' and value > 5:
            self.alerts.append(f"High error rate: {value}%")
            self.stats['alerts_triggered'] += 1

class LocalOrchestrator:
    """
    Local orchestrator - No AWS dependencies
    Complete ML pricing system using only local components
    """
    
    def __init__(self):
        self.system_status = SystemStatus()
        self.start_time = datetime.now()
        self.is_running = False
        
        # Initialize local components
        self._initialize_local_system()
        
        logger.info("Local Orchestrator initialized successfully")
    
    def _initialize_local_system(self):
        """Initialize all local components"""
        try:
            # Data Ingestion
            self.data_producer = LocalDataProducer()
            self.system_status.ingestion_layer = True
            
            # Processing Layer
            self.stream_processor = LocalStreamProcessor()
            self.system_status.processing_layer = True
            
            # Storage Layer
            self.storage = LocalStorage()
            self.system_status.storage_layer = True
            
            # Serving Layer
            self.cache = LocalCache()
            self.system_status.serving_layer = True
            
            # Monitoring Layer
            self.monitor = LocalMonitor()
            self.system_status.monitoring_layer = True
            
            # ML Model
            self.system_status.ml_model_ready = True
            
            # Update overall health
            self.system_status.overall_health = 'healthy'
            
            logger.info("All local components initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize local system: {e}")
            self.system_status.overall_health = 'failed'
            raise
    
    def process_pricing_request(self, request_data):
        """Process complete pricing request"""
        request_id = request_data.get('request_id', f"req_{int(time.time())}")
        start_time = time.time()
        
        try:
            logger.info(f"[{request_id}] Processing pricing request...")
            
            # Step 1: Data Ingestion
            event_data = {
                'event_id': int(time.time()),
                'demand': request_data['demand'],
                'timestamp': request_data.get('timestamp', datetime.now().isoformat()),
                'source': request_data.get('source', 'api')
            }
            
            # Step 2: Processing
            processed_event = self.stream_processor.process_event(event_data)
            
            if not processed_event:
                raise Exception("Processing failed")
            
            # Step 3: Storage
            self.storage.store_event(processed_event)
            
            # Step 4: Caching
            cache_key = f"pricing_{processed_event['event_id']}"
            self.cache.set(cache_key, processed_event)
            
            # Step 5: Monitoring
            processing_time_ms = (time.time() - start_time) * 1000
            self.monitor.publish_metric('ProcessingTime', processing_time_ms, 'Milliseconds')
            self.monitor.publish_metric('MLPredictions', 1, 'Count')
            
            logger.info(f"[{request_id}] Request processed successfully in {processing_time_ms:.2f}ms")
            
            return {
                'request_id': request_id,
                'status': 'success',
                'data': {
                    'event_id': processed_event['event_id'],
                    'demand': processed_event['demand'],
                    'price': processed_event['price'],
                    'pricing_tier': processed_event['pricing_tier'],
                    'pricing_method': processed_event['pricing_method'],
                    'confidence': processed_event['confidence']
                },
                'processing_time_ms': round(processing_time_ms, 2),
                'stages_completed': ['ingestion', 'processing', 'storage', 'serving', 'monitoring']
            }
            
        except Exception as e:
            logger.error(f"[{request_id}] Processing failed: {e}")
            self.monitor.publish_metric('ErrorRate', 1, 'Percent')
            
            return {
                'request_id': request_id,
                'status': 'failed',
                'error': str(e),
                'processing_time_ms': round((time.time() - start_time) * 1000, 2)
            }
    
    def get_system_health(self):
        """Get system health status"""
        try:
            return {
                'system_status': self.system_status.__dict__,
                'uptime_seconds': int((datetime.now() - self.start_time).total_seconds()),
                'components': {
                    'data_producer': self.data_producer.stats,
                    'stream_processor': self.stream_processor.stats,
                    'storage': self.storage.stats,
                    'cache': self.cache.stats,
                    'monitor': self.monitor.stats
                },
                'timestamp': datetime.now().isoformat()
            }
        except Exception as e:
            return {
                'system_status': {'overall_health': 'error'},
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }

# Flask API for local system
from flask import Flask, request, jsonify

app = Flask(__name__)
orchestrator = None

def initialize_orchestrator():
    global orchestrator
    if orchestrator is None:
        orchestrator = LocalOrchestrator()

# Initialize orchestrator on first request
@app.before_request
def ensure_orchestrator():
    if orchestrator is None:
        initialize_orchestrator()

@app.route('/v1/system/pricing', methods=['POST'])
def process_pricing_request():
    """Process pricing request"""
    try:
        request_data = request.get_json()
        if not request_data:
            return jsonify({
                'status': 'error',
                'message': 'No JSON data provided'
            }), 400
        
        request_data['request_id'] = request.headers.get('X-Request-ID')
        
        result = orchestrator.process_pricing_request(request_data)
        
        status_code = 200 if result['status'] == 'success' else 500
        return jsonify(result), status_code
        
    except Exception as e:
        logger.error(f"API error: {e}")
        return jsonify({
            'status': 'error',
            'message': 'Internal server error',
            'error': str(e)
        }), 500

@app.route('/v1/system/health', methods=['GET'])
def get_system_health():
    """Get system health"""
    try:
        health = orchestrator.get_system_health()
        return jsonify(health)
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': 'Failed to get system health',
            'error': str(e)
        }), 500

@app.route('/v1/system/demo', methods=['GET'])
def run_demo():
    """Run a demo with multiple requests"""
    try:
        demo_results = []
        
        for i in range(5):
            request_data = {
                'demand': random.randint(20, 80),
                'source': f'demo_request_{i+1}'
            }
            
            result = orchestrator.process_pricing_request(request_data)
            demo_results.append(result)
            
            time.sleep(0.1)  # Small delay between requests
        
        return jsonify({
            'status': 'success',
            'message': 'Demo completed successfully',
            'results': demo_results,
            'system_health': orchestrator.get_system_health()
        })
        
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': 'Demo failed',
            'error': str(e)
        }), 500

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    try:
        logger.info("Starting Local ML Pricing System")
        print("\n🚀 Local ML Pricing System Starting...")
        print("📍 API will be available at: http://localhost:5004")
        print("🏥 Health check: http://localhost:5004/v1/system/health")
        print("🧪 Demo: http://localhost:5004/v1/system/demo")
        print("💡 No AWS dependencies required!\n")
        
        app.run(debug=True, host='0.0.0.0', port=5004)
        
    except KeyboardInterrupt:
        logger.info("Shutting down Local ML Pricing System...")
    except Exception as e:
        logger.error(f"System startup failed: {e}")
