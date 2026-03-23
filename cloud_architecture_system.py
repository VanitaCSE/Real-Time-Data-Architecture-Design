"""
STEP 6: Cloud Architecture Ready - Complete Real-Time System
Integrates all layers: Data Ingestion → Processing → Storage → Serving
Maps to AWS cloud services for production deployment
"""

import time
import threading
import queue
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, Optional
import json
import signal
import sys

# Import all layers
from data_ingestion_layer import create_streaming_pipeline
from data_storage_layer import DataStorageLayer
from realtime_processing_layer import RealTimeProcessor
from serving_layer import APIServingLayer
from ml_pricing_layer import MLPricingModel, MLModelManager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class CloudArchitectureSystem:
    """
    Complete Cloud Architecture System
    Orchestrates all layers for production-ready real-time processing
    """
    
    def __init__(self):
        logger.info("🚀 Initializing Cloud Architecture System...")
        
        # System state
        self.is_running = False
        self.start_time = datetime.now()
        
        # Initialize all layers
        self._initialize_layers()
        
        # Performance tracking
        self.system_metrics = {
            'total_events_processed': 0,
            'total_api_requests': 0,
            'system_errors': 0,
            'last_health_check': None
        }
        
        # Background threads
        self.monitoring_thread = None
        self.health_check_thread = None
        
        logger.info("✅ Cloud Architecture System initialized")
        
    def _initialize_layers(self):
        """Initialize all architecture layers"""
        
        # 1. Data Ingestion Layer (Kinesis equivalent)
        logger.info("📥 Initializing Data Ingestion Layer...")
        self.streaming_pipeline = create_streaming_pipeline()
        
        # 2. Data Storage Layer (S3/RDS equivalent)
        logger.info("💾 Initializing Data Storage Layer...")
        self.storage_layer = DataStorageLayer()
        
        # 3. Real-Time Processing Layer (Spark Streaming equivalent)
        logger.info("⚡ Initializing Real-Time Processing Layer...")
        self.processor = RealTimeProcessor(
            self.streaming_pipeline['raw_queue'],
            self.streaming_pipeline['processed_queue'],
            self.storage_layer
        )
        
        # 4. ML Pricing Layer (SageMaker equivalent)
        logger.info("🤖 Initializing ML Pricing Layer...")
        self.ml_model = MLPricingModel(model_type="random_forest")
        self.ml_manager = MLModelManager()
        self.ml_manager.add_model("primary", self.ml_model)
        
        # 5. Serving Layer (API Gateway equivalent)
        logger.info("🌐 Initializing Serving Layer...")
        self.serving_layer = APIServingLayer(
            self.storage_layer,
            self.streaming_pipeline['processed_queue']
        )
        
        logger.info("✅ All layers initialized successfully")
        
    def start_system(self):
        """Start the complete system"""
        if self.is_running:
            logger.warning("⚠️ System is already running")
            return
            
        logger.info("🚀 Starting Cloud Architecture System...")
        
        try:
            # Start data ingestion
            self.streaming_pipeline['producer'].start_production()
            logger.info("📥 Data ingestion started")
            
            # Start real-time processing
            self.processor.start_processing()
            logger.info("⚡ Real-time processing started")
            
            # Start background monitoring
            self._start_monitoring()
            
            # Set up graceful shutdown
            signal.signal(signal.SIGINT, self._signal_handler)
            signal.signal(signal.SIGTERM, self._signal_handler)
            
            self.is_running = True
            logger.info("✅ Cloud Architecture System started successfully")
            
            # Print system information
            self._print_system_info()
            
        except Exception as e:
            logger.error(f"❌ Failed to start system: {e}")
            self.stop_system()
            raise
            
    def stop_system(self):
        """Stop the complete system"""
        if not self.is_running:
            return
            
        logger.info("🛑 Stopping Cloud Architecture System...")
        
        try:
            # Stop streaming components
            self.streaming_pipeline['producer'].stop_production()
            self.processor.stop_processing()
            
            # Stop monitoring threads
            self._stop_monitoring()
            
            # Close storage layer
            self.storage_layer.close()
            
            self.is_running = False
            logger.info("✅ Cloud Architecture System stopped")
            
        except Exception as e:
            logger.error(f"❌ Error stopping system: {e}")
            
    def _start_monitoring(self):
        """Start background monitoring threads"""
        
        # System metrics monitoring
        self.monitoring_thread = threading.Thread(
            target=self._monitoring_loop, daemon=True
        )
        self.monitoring_thread.start()
        
        # Health check monitoring
        self.health_check_thread = threading.Thread(
            target=self._health_check_loop, daemon=True
        )
        self.health_check_thread.start()
        
        logger.info("📊 Background monitoring started")
        
    def _stop_monitoring(self):
        """Stop background monitoring threads"""
        # Threads are daemon, so they'll stop automatically
        logger.info("📊 Background monitoring stopped")
        
    def _monitoring_loop(self):
        """Main monitoring loop"""
        while self.is_running:
            try:
                # Collect system metrics
                self._collect_system_metrics()
                
                # Store metrics in database
                metrics = {
                    'api_latency_ms': self.serving_layer.get_api_performance_stats()['avg_response_time_ms'],
                    'requests_per_minute': self.serving_layer.get_api_performance_stats()['requests_per_minute'],
                    'price_calculation_time_ms': self._get_avg_ml_prediction_time(),
                    'storage_efficiency_percent': self.storage_layer.get_database_stats()['storage_efficiency'],
                    'queue_depth': self.streaming_pipeline['processed_queue'].qsize(),
                    'memory_usage_mb': self._estimate_memory_usage()
                }
                
                self.storage_layer.store_performance_metrics(metrics)
                
                # Sleep for 30 seconds
                time.sleep(30)
                
            except Exception as e:
                logger.error(f"❌ Monitoring error: {e}")
                self.system_metrics['system_errors'] += 1
                time.sleep(30)
                
    def _health_check_loop(self):
        """Health check monitoring loop"""
        while self.is_running:
            try:
                # Perform health check
                health_status = self._perform_health_check()
                self.system_metrics['last_health_check'] = datetime.now().isoformat()
                
                # Log health status
                if health_status['overall_status'] != 'healthy':
                    logger.warning(f"⚠️ Health check warning: {health_status}")
                    
                # Sleep for 60 seconds
                time.sleep(60)
                
            except Exception as e:
                logger.error(f"❌ Health check error: {e}")
                time.sleep(60)
                
    def _collect_system_metrics(self):
        """Collect comprehensive system metrics"""
        try:
            # Processing metrics
            processing_stats = self.processor.get_processing_stats()
            self.system_metrics['total_events_processed'] = processing_stats['processed_events']
            
            # API metrics
            api_stats = self.serving_layer.get_api_performance_stats()
            self.system_metrics['total_api_requests'] = api_stats['total_requests']
            
            # Database metrics
            db_stats = self.storage_layer.get_database_stats()
            
            # Log summary
            logger.info(f"📊 System Status - Events: {processing_stats['processed_events']}, "
                       f"API Requests: {api_stats['total_requests']}, "
                       f"DB Records: {db_stats['total_pricing_events']}, "
                       f"Queue Depth: {processing_stats['queue_depth']}")
                       
        except Exception as e:
            logger.error(f"❌ Metrics collection error: {e}")
            
    def _perform_health_check(self) -> Dict[str, Any]:
        """Perform comprehensive health check"""
        health_status = {
            'timestamp': datetime.now().isoformat(),
            'overall_status': 'healthy',
            'components': {}
        }
        
        # Check producer health
        producer_healthy = self.streaming_pipeline['producer'].is_running
        health_status['components']['producer'] = {
            'status': 'healthy' if producer_healthy else 'unhealthy',
            'message': 'Running' if producer_healthy else 'Stopped'
        }
        
        # Check processor health
        processor_healthy = self.processor.is_running
        processing_stats = self.processor.get_processing_stats()
        health_status['components']['processor'] = {
            'status': 'healthy' if processor_healthy else 'unhealthy',
            'message': f"Running (processed: {processing_stats['processed_events']})" if processor_healthy else 'Stopped'
        }
        
        # Check storage health
        try:
            db_stats = self.storage_layer.get_database_stats()
            storage_healthy = db_stats['total_pricing_events'] >= 0
            health_status['components']['storage'] = {
                'status': 'healthy' if storage_healthy else 'unhealthy',
                'message': f"OK ({db_stats['total_pricing_events']} records)"
            }
        except:
            health_status['components']['storage'] = {
                'status': 'unhealthy',
                'message': 'Database error'
            }
            
        # Check ML model health
        ml_healthy = self.ml_model.is_trained
        health_status['components']['ml_model'] = {
            'status': 'healthy' if ml_healthy else 'training',
            'message': 'Trained' if ml_healthy else 'Training in progress'
        }
        
        # Check queue health
        queue_depth = self.streaming_pipeline['processed_queue'].qsize()
        queue_healthy = queue_depth < 900  # Less than 90% full
        health_status['components']['queues'] = {
            'status': 'healthy' if queue_healthy else 'warning',
            'message': f"Depth: {queue_depth}/1000"
        }
        
        # Determine overall status
        unhealthy_components = [
            name for name, comp in health_status['components'].items()
            if comp['status'] == 'unhealthy'
        ]
        
        if unhealthy_components:
            health_status['overall_status'] = 'unhealthy'
            health_status['issues'] = unhealthy_components
        elif any(comp['status'] == 'warning' for comp in health_status['components'].values()):
            health_status['overall_status'] = 'warning'
            
        return health_status
        
    def _get_avg_ml_prediction_time(self) -> float:
        """Get average ML prediction time"""
        # This would be tracked in the ML model in a real implementation
        return 5.0  # Placeholder
        
    def _estimate_memory_usage(self) -> float:
        """Estimate system memory usage in MB"""
        # This would use psutil in a real implementation
        return 150.0  # Placeholder
        
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals"""
        logger.info(f"🛑 Received signal {signum}, shutting down gracefully...")
        self.stop_system()
        sys.exit(0)
        
    def _print_system_info(self):
        """Print system information"""
        logger.info("=" * 60)
        logger.info("🏗️  CLOUD ARCHITECTURE SYSTEM")
        logger.info("=" * 60)
        logger.info("📥 Data Ingestion:  Kafka Simulation (Python Queues)")
        logger.info("⚡  Processing:     Spark Streaming Equivalent")
        logger.info("🤖 ML Pricing:      SageMaker Equivalent")
        logger.info("💾 Storage:         SQLite (S3/RDS Equivalent)")
        logger.info("🌐 Serving:         API Gateway Equivalent")
        logger.info("📊 Monitoring:      CloudWatch Equivalent")
        logger.info("=" * 60)
        logger.info("🌐 API Endpoints:")
        logger.info("   GET /v1/price          - Latest pricing data")
        logger.info("   GET /v1/price/history  - Historical data")
        logger.info("   GET /v1/system/metrics - System metrics")
        logger.info("   GET /v1/health         - Health check")
        logger.info("=" * 60)
        logger.info("☁️  AWS Cloud Mapping:")
        logger.info("   Kinesis  → Data Ingestion Layer")
        logger.info("   Lambda   → Processing Layer")
        logger.info("   SageMaker→ ML Pricing Layer")
        logger.info("   S3/RDS   → Storage Layer")
        logger.info("   API GW   → Serving Layer")
        logger.info("   CloudWatch→ Monitoring")
        logger.info("=" * 60)
        
    def get_system_status(self) -> Dict[str, Any]:
        """Get comprehensive system status"""
        return {
            'system_info': {
                'is_running': self.is_running,
                'start_time': self.start_time.isoformat(),
                'uptime_seconds': int((datetime.now() - self.start_time).total_seconds()),
                'version': 'v1.0.0'
            },
            'metrics': self.system_metrics,
            'processing_stats': self.processor.get_processing_stats(),
            'api_performance': self.serving_layer.get_api_performance_stats(),
            'database_stats': self.storage_layer.get_database_stats(),
            'ml_model_info': self.ml_model.get_model_info(),
            'cache_stats': self.serving_layer.cache.get_stats(),
            'health_status': self._perform_health_check()
        }

# Flask API Integration
class CloudArchitectureAPI:
    """
    Flask API for the Cloud Architecture System
    Provides REST endpoints for all system functionality
    """
    
    def __init__(self, system: CloudArchitectureSystem):
        self.system = system
        self.app = None
        self._setup_flask()
        
    def _setup_flask(self):
        """Setup Flask application with all endpoints"""
        try:
            from flask import Flask, jsonify, request
            
            self.app = Flask(__name__)
            
            # Main pricing endpoint
            @self.app.route('/v1/price', methods=['GET'])
            def get_latest_price():
                try:
                    include_analytics = request.args.get('analytics', 'true').lower() == 'true'
                    return jsonify(self.system.serving_layer.get_latest_price(include_analytics))
                except Exception as e:
                    return jsonify({
                        'status': 'error',
                        'message': str(e),
                        'timestamp': datetime.now().isoformat()
                    }), 500
                    
            # Price history endpoint
            @self.app.route('/v1/price/history', methods=['GET'])
            def get_price_history():
                try:
                    limit = int(request.args.get('limit', 100))
                    window_minutes = int(request.args.get('window', 60))
                    return jsonify(self.system.serving_layer.get_price_history(limit, window_minutes))
                except Exception as e:
                    return jsonify({
                        'status': 'error',
                        'message': str(e),
                        'timestamp': datetime.now().isoformat()
                    }), 500
                    
            # System metrics endpoint
            @self.app.route('/v1/system/metrics', methods=['GET'])
            def get_system_metrics():
                try:
                    return jsonify(self.system.serving_layer.get_system_metrics())
                except Exception as e:
                    return jsonify({
                        'status': 'error',
                        'message': str(e),
                        'timestamp': datetime.now().isoformat()
                    }), 500
                    
            # Health check endpoint
            @self.app.route('/v1/health', methods=['GET'])
            def health_check():
                try:
                    health_status = self.system._perform_health_check()
                    return jsonify({
                        'status': 'success',
                        'data': health_status,
                        'timestamp': datetime.now().isoformat()
                    })
                except Exception as e:
                    return jsonify({
                        'status': 'error',
                        'message': str(e),
                        'timestamp': datetime.now().isoformat()
                    }), 500
                    
            # ML model info endpoint
            @self.app.route('/v1/ml/model', methods=['GET'])
            def get_ml_model_info():
                try:
                    return jsonify({
                        'status': 'success',
                        'data': self.system.ml_model.get_model_info(),
                        'timestamp': datetime.now().isoformat()
                    })
                except Exception as e:
                    return jsonify({
                        'status': 'error',
                        'message': str(e),
                        'timestamp': datetime.now().isoformat()
                    }), 500
                    
            # System status endpoint
            @self.app.route('/v1/system/status', methods=['GET'])
            def get_system_status():
                try:
                    return jsonify({
                        'status': 'success',
                        'data': self.system.get_system_status(),
                        'timestamp': datetime.now().isoformat()
                    })
                except Exception as e:
                    return jsonify({
                        'status': 'error',
                        'message': str(e),
                        'timestamp': datetime.now().isoformat()
                    }), 500
                    
            logger.info("🌐 Flask API endpoints configured")
            
        except ImportError:
            logger.error("❌ Flask not available. Install with: pip install flask")
            raise
            
    def run(self, host='127.0.0.1', port=5000, debug=False):
        """Run the Flask API server"""
        if self.app:
            logger.info(f"🌐 Starting API server on http://{host}:{port}")
            self.app.run(host=host, port=port, debug=debug)
        else:
            logger.error("❌ Flask app not initialized")

# Main execution
def main():
    """Main execution function"""
    logger.info("🚀 Starting Cloud Architecture System...")
    
    # Initialize system
    system = CloudArchitectureSystem()
    
    # Start system
    system.start_system()
    
    # Setup API
    api = CloudArchitectureAPI(system)
    
    try:
        # Run API server (blocking)
        api.run(host='0.0.0.0', port=5000, debug=False)
    except KeyboardInterrupt:
        logger.info("🛑 Received keyboard interrupt...")
    finally:
        # Cleanup
        system.stop_system()
        logger.info("👋 Cloud Architecture System shutdown complete")

if __name__ == "__main__":
    main()

# Cloud Architecture Documentation
"""
CLOUD ARCHITECTURE MAPPING:

Local Component               → AWS Service
─────────────────────────────────────────────────
Data Ingestion Layer         → Amazon Kinesis Data Streams
Real-Time Processing Layer   → AWS Lambda / AWS Glue
ML Pricing Layer             → Amazon SageMaker
Data Storage Layer           → Amazon S3 / Amazon RDS
API Serving Layer            → Amazon API Gateway
Caching System               → Amazon ElastiCache (Redis)
Monitoring & Metrics        → Amazon CloudWatch
Load Balancing               → Application Load Balancer
Security                     → AWS IAM / Cognito

DEPLOYMENT ARCHITECTURE:

1. Data Flow:
   External Sources → Kinesis → Lambda → SageMaker → S3/RDS → API Gateway

2. Processing Pipeline:
   - Real-time: Kinesis → Lambda → SageMaker → ElastiCache
   - Batch: S3 → Glue → SageMaker → S3

3. API Layer:
   - API Gateway → Lambda → ElastiCache/RDS → Response

4. Monitoring:
   - CloudWatch Logs + Metrics
   - X-Ray for tracing
   - CloudWatch Alarms

SCALING CONSIDERATIONS:

1. Horizontal Scaling:
   - Multiple Lambda instances
   - Kinesis shard scaling
   - RDS read replicas

2. Performance:
   - ElastiCache for sub-millisecond response
   - CDN for static assets
   - Global infrastructure

3. Reliability:
   - Multi-AZ deployments
   - Auto-scaling groups
   - Disaster recovery

4. Security:
   - VPC isolation
   - Encryption at rest and in transit
   - IAM roles and policies
"""
