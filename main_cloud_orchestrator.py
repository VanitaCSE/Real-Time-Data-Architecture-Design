"""
🎯 Cloud Architecture Orchestrator
Main entry point for the cloud-native ML pricing system
Coordinates all layers: Ingestion → Processing → Storage → Serving
"""

import json
import logging
import asyncio
import time
from datetime import datetime
from typing import Dict, Any, List, Optional
from dataclasses import dataclass
import threading

# Import all layers
from data_ingestion.kinesis_producer import create_producer
from data_ingestion.api_gateway_handler import APIGatewayHandler
from processing.stream_processor import StreamProcessor
from processing.ml_predictor import MLPredictor
from storage.s3_data_lake import S3DataLake
from serving.api_service import APIService, CacheManager
from monitoring.cloudwatch_metrics import CloudWatchMonitor, SystemMetricsCollector

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

class CloudOrchestrator:
    """
    Main orchestrator for the cloud-native ML pricing system
    Coordinates all architectural layers
    """
    
    def __init__(self, use_local: bool = True):
        self.use_local = use_local
        self.system_status = SystemStatus()
        self.start_time = datetime.now()
        
        # Initialize all layers
        self._initialize_layers()
        
        # Background tasks
        self.background_threads = []
        self.is_running = False
        
        logger.info("Cloud Orchestrator initialized")
    
    def _initialize_layers(self) -> None:
        """Initialize all architectural layers"""
        try:
            # 1. Data Ingestion Layer
            logger.info("Initializing Data Ingestion Layer...")
            self.kinesis_producer = create_producer(use_local=self.use_local)
            self.api_gateway = APIGatewayHandler(use_local=self.use_local)
            self.system_status.ingestion_layer = True
            
            # 2. Processing Layer
            logger.info("Initializing Processing Layer...")
            self.stream_processor = StreamProcessor()
            self.ml_predictor = MLPredictor()
            self.system_status.processing_layer = True
            
            # 3. Storage Layer
            logger.info("Initializing Storage Layer...")
            self.s3_data_lake = S3DataLake()
            self.system_status.storage_layer = True
            
            # 4. Serving Layer
            logger.info("Initializing Serving Layer...")
            self.cache_manager = CacheManager(use_local=self.use_local)
            self.api_service = APIService(self.cache_manager)
            self.system_status.serving_layer = True
            
            # 5. Monitoring Layer
            logger.info("Initializing Monitoring Layer...")
            self.cloudwatch_monitor = CloudWatchMonitor(use_local=self.use_local)
            self.metrics_collector = SystemMetricsCollector(self.cloudwatch_monitor)
            self.system_status.monitoring_layer = True
            
            # Initialize ML model if needed
            if not self.ml_predictor.is_available():
                logger.info("Training ML model...")
                training_data = self.ml_predictor.generate_training_data(1000)
                self.ml_predictor.train_model(training_data)
                self.system_status.ml_model_ready = True
            else:
                self.system_status.ml_model_ready = True
            
            # Update overall health
            self._update_system_health()
            
            logger.info("All layers initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize layers: {e}")
            self.system_status.overall_health = 'failed'
            raise
    
    def start_system(self) -> None:
        """Start the complete cloud system"""
        try:
            logger.info("Starting Cloud ML Pricing System...")
            self.is_running = True
            
            # Setup monitoring
            self.cloudwatch_monitor.setup_alarms()
            self.cloudwatch_monitor.create_custom_dashboard()
            
            # Start background tasks
            self._start_background_tasks()
            
            logger.info("Cloud ML Pricing System started successfully")
            
        except Exception as e:
            logger.error(f"Failed to start system: {e}")
            self.is_running = False
            raise
    
    def stop_system(self) -> None:
        """Stop the cloud system gracefully"""
        try:
            logger.info("Stopping Cloud ML Pricing System...")
            self.is_running = False
            
            # Stop background threads
            for thread in self.background_threads:
                if thread.is_alive():
                    thread.join(timeout=5)
            
            logger.info("Cloud ML Pricing System stopped")
            
        except Exception as e:
            logger.error(f"Error stopping system: {e}")
    
    def process_pricing_request(self, request_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process complete pricing request through all layers
        Maps to: End-to-end request flow
        """
        request_id = request_data.get('request_id', f"req_{int(time.time())}")
        start_time = time.time()
        
        try:
            # Layer 1: Data Ingestion - API Gateway
            logger.info(f"[{request_id}] Processing pricing request...")
            api_response = self.api_gateway.handle_pricing_request(request_data)
            
            if api_response.status != 'success':
                return {
                    'request_id': request_id,
                    'status': 'failed',
                    'error': api_response.error,
                    'stage': 'ingestion',
                    'processing_time_ms': (time.time() - start_time) * 1000
                }
            
            # Layer 2: Processing - Stream Processing + ML
            # Simulate stream processing
            event_data = {
                'event_id': api_response.data['event_id'],
                'demand': request_data['demand'],
                'timestamp': request_data.get('timestamp', datetime.now().isoformat()),
                'source': 'api_gateway'
            }
            
            processed_event = self.stream_processor._process_event(event_data)
            
            # Layer 3: Storage - S3 Data Lake
            self.s3_data_lake.store_processed_event(processed_event.__dict__)
            
            # Layer 4: Serving - Cache the result
            cache_key = f"pricing_result_{processed_event.event_id}"
            self.cache_manager.set(cache_key, processed_event.__dict__, ttl_seconds=300)
            
            # Layer 5: Monitoring - Collect metrics
            processing_time_ms = (time.time() - start_time) * 1000
            
            # Publish metrics
            from monitoring.cloudwatch_metrics import MetricData
            latency_metric = MetricData(
                namespace='MLPricingSystem',
                metric_name='EndToEndLatency',
                value=processing_time_ms,
                unit='Milliseconds',
                dimensions={'RequestType': 'Pricing', 'Environment': 'production'}
            )
            self.cloudwatch_monitor.publish_metric(latency_metric)
            
            logger.info(f"[{request_id}] Request processed successfully in {processing_time_ms:.2f}ms")
            
            return {
                'request_id': request_id,
                'status': 'success',
                'data': {
                    'event_id': processed_event.event_id,
                    'demand': processed_event.demand,
                    'price': processed_event.price,
                    'pricing_tier': processed_event.pricing_tier,
                    'pricing_method': processed_event.pricing_method,
                    'window_stats': processed_event.window_stats.__dict__
                },
                'processing_time_ms': processing_time_ms,
                'stages_completed': ['ingestion', 'processing', 'storage', 'serving', 'monitoring']
            }
            
        except Exception as e:
            logger.error(f"[{request_id}] Processing failed: {e}")
            return {
                'request_id': request_id,
                'status': 'failed',
                'error': str(e),
                'processing_time_ms': (time.time() - start_time) * 1000
            }
    
    def get_system_health(self) -> Dict[str, Any]:
        """Get comprehensive system health status"""
        try:
            # Collect stats from all layers
            ingestion_stats = self.api_gateway.get_api_stats()
            processing_stats = self.stream_processor.get_processing_stats()
            storage_stats = self.s3_data_lake.get_storage_stats()
            serving_stats = self.api_service.get_api_stats().data if self.api_service.get_api_stats().status == 'success' else {}
            cache_stats = self.cache_manager.get_stats()
            monitoring_stats = self.cloudwatch_monitor.get_monitoring_stats()
            
            # ML model info
            ml_model_info = self.ml_predictor.get_model_info()
            
            # Calculate overall health
            self._update_system_health()
            
            return {
                'system_status': self.system_status.__dict__,
                'uptime_seconds': int((datetime.now() - self.start_time).total_seconds()),
                'layers': {
                    'ingestion': {
                        'status': 'healthy' if self.system_status.ingestion_layer else 'unhealthy',
                        'stats': ingestion_stats
                    },
                    'processing': {
                        'status': 'healthy' if self.system_status.processing_layer else 'unhealthy',
                        'stats': processing_stats
                    },
                    'storage': {
                        'status': 'healthy' if self.system_status.storage_layer else 'unhealthy',
                        'stats': storage_stats
                    },
                    'serving': {
                        'status': 'healthy' if self.system_status.serving_layer else 'unhealthy',
                        'stats': serving_stats
                    },
                    'monitoring': {
                        'status': 'healthy' if self.system_status.monitoring_layer else 'unhealthy',
                        'stats': monitoring_stats
                    }
                },
                'ml_model': {
                    'status': 'ready' if self.system_status.ml_model_ready else 'not_ready',
                    'info': ml_model_info
                },
                'cache': {
                    'status': 'healthy',
                    'stats': cache_stats
                },
                'timestamp': datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Failed to get system health: {e}")
            return {
                'system_status': {'overall_health': 'error'},
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }
    
    def _start_background_tasks(self) -> None:
        """Start background monitoring and maintenance tasks"""
        
        def metrics_collection_task():
            """Background task for metrics collection"""
            while self.is_running:
                try:
                    # Collect metrics from all layers
                    api_stats = self.api_gateway.get_api_stats()
                    processing_stats = self.stream_processor.get_processing_stats()
                    cache_stats = self.cache_manager.get_stats()
                    
                    # Publish metrics
                    self.metrics_collector.collect_api_metrics(api_stats)
                    self.metrics_collector.collect_processing_metrics(processing_stats)
                    self.metrics_collector.collect_cache_metrics(cache_stats)
                    
                    if self.system_status.ml_model_ready:
                        ml_stats = self.ml_predictor.get_model_info()
                        self.metrics_collector.collect_ml_metrics(ml_stats)
                    
                    time.sleep(60)  # Collect every minute
                    
                except Exception as e:
                    logger.error(f"Metrics collection error: {e}")
                    time.sleep(60)
        
        def health_check_task():
            """Background task for health monitoring"""
            while self.is_running:
                try:
                    self._update_system_health()
                    
                    # Publish health metric
                    from monitoring.cloudwatch_metrics import MetricData
                    health_metric = MetricData(
                        namespace='MLPricingSystem',
                        metric_name='SystemHealth',
                        value=1 if self.system_status.overall_health == 'healthy' else 0,
                        unit='None',
                        dimensions={'Component': 'Overall', 'Environment': 'production'}
                    )
                    self.cloudwatch_monitor.publish_metric(health_metric)
                    
                    time.sleep(30)  # Check every 30 seconds
                    
                except Exception as e:
                    logger.error(f"Health check error: {e}")
                    time.sleep(30)
        
        # Start background threads
        metrics_thread = threading.Thread(target=metrics_collection_task, daemon=True)
        health_thread = threading.Thread(target=health_check_task, daemon=True)
        
        metrics_thread.start()
        health_thread.start()
        
        self.background_threads.extend([metrics_thread, health_thread])
        
        logger.info("Background tasks started")
    
    def _update_system_health(self) -> None:
        """Update overall system health status"""
        layer_statuses = [
            self.system_status.ingestion_layer,
            self.system_status.processing_layer,
            self.system_status.storage_layer,
            self.system_status.serving_layer,
            self.system_status.monitoring_layer
        ]
        
        if all(layer_statuses) and self.system_status.ml_model_ready:
            self.system_status.overall_health = 'healthy'
        elif any(layer_statuses):
            self.system_status.overall_health = 'degraded'
        else:
            self.system_status.overall_health = 'unhealthy'

# Flask API for the complete system
from flask import Flask, request, jsonify

app = Flask(__name__)
orchestrator = None

@app.before_first_request
def initialize_orchestrator():
    """Initialize orchestrator before first request"""
    global orchestrator
    if orchestrator is None:
        orchestrator = CloudOrchestrator(use_local=True)
        orchestrator.start_system()

@app.route('/v1/system/pricing', methods=['POST'])
def process_pricing_request():
    """Process pricing request through complete system"""
    try:
        request_data = request.get_json()
        if not request_data:
            return jsonify({
                'status': 'error',
                'message': 'No JSON data provided'
            }), 400
        
        # Add request metadata
        request_data['request_id'] = request.headers.get('X-Request-ID')
        request_data['client_ip'] = request.remote_addr
        
        # Process through all layers
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
    """Get complete system health"""
    try:
        health = orchestrator.get_system_health()
        return jsonify(health)
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': 'Failed to get system health',
            'error': str(e)
        }), 500

@app.route('/v1/system/status', methods=['GET'])
def get_system_status():
    """Get system status overview"""
    try:
        if orchestrator:
            status = {
                'system_running': orchestrator.is_running,
                'uptime_seconds': int((datetime.now() - orchestrator.start_time).total_seconds()),
                'system_status': orchestrator.system_status.__dict__
            }
        else:
            status = {
                'system_running': False,
                'system_status': 'initializing'
            }
        
        return jsonify(status)
    except Exception as e:
        return jsonify({
            'status': 'error',
            'error': str(e)
        }), 500

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    try:
        logger.info("Starting Cloud ML Pricing System Orchestrator")
        app.run(debug=True, host='0.0.0.0', port=5003)
    except KeyboardInterrupt:
        logger.info("Shutting down Cloud ML Pricing System...")
        if orchestrator:
            orchestrator.stop_system()
    except Exception as e:
        logger.error(f"System startup failed: {e}")
        if orchestrator:
            orchestrator.stop_system()
