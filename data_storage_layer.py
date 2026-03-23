"""
STEP 2: Data Storage Layer - SQLite Database with Cloud Scaling
Handles persistent storage of demand and pricing data
"""

import sqlite3
import json
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
import logging
import threading
import os

logger = logging.getLogger(__name__)

class DataStorageLayer:
    """
    Data Storage Layer - Manages persistent data storage
    SQLite for local, with explanations for cloud scaling
    """
    
    def __init__(self, db_path: str = "realtime_pricing.db"):
        self.db_path = db_path
        self.connection = None
        self.lock = threading.Lock()
        self.init_database()
        
    def init_database(self):
        """Initialize database with optimized schema"""
        try:
            with self.lock:
                self.connection = sqlite3.connect(self.db_path, check_same_thread=False)
                self.connection.row_factory = sqlite3.Row  # Enable dict-like access
                
                # Enable WAL mode for better concurrent access
                self.connection.execute("PRAGMA journal_mode=WAL")
                self.connection.execute("PRAGMA synchronous=NORMAL")
                self.connection.execute("PRAGMA cache_size=10000")
                
                self._create_tables()
                self._create_indexes()
                
                logger.info(f"✅ Database initialized: {self.db_path}")
                
        except Exception as e:
            logger.error(f"❌ Database initialization failed: {e}")
            raise
            
    def _create_tables(self):
        """Create optimized tables for real-time data"""
        
        # Main pricing events table
        self.connection.execute("""
            CREATE TABLE IF NOT EXISTS pricing_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_id INTEGER UNIQUE NOT NULL,
                timestamp TEXT NOT NULL,
                demand REAL NOT NULL,
                price REAL NOT NULL,
                pricing_tier TEXT NOT NULL,
                moving_average REAL,
                trend TEXT,
                source TEXT DEFAULT 'streaming_system',
                processed_at TEXT NOT NULL,
                processing_latency_ms REAL,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # ML model predictions table
        self.connection.execute("""
            CREATE TABLE IF NOT EXISTS ml_predictions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_id INTEGER,
                prediction_timestamp TEXT NOT NULL,
                demand_input REAL NOT NULL,
                predicted_price REAL NOT NULL,
                model_version TEXT,
                confidence_score REAL,
                actual_price REAL,
                prediction_error REAL,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (event_id) REFERENCES pricing_events (event_id)
            )
        """)
        
        # Performance metrics table
        self.connection.execute("""
            CREATE TABLE IF NOT EXISTS performance_metrics (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                metric_timestamp TEXT NOT NULL,
                api_latency_ms REAL,
                requests_per_minute INTEGER,
                price_calculation_time_ms REAL,
                storage_efficiency_percent REAL,
                queue_depth INTEGER,
                memory_usage_mb REAL,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # System health table
        self.connection.execute("""
            CREATE TABLE IF NOT EXISTS system_health (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                health_check_timestamp TEXT NOT NULL,
                producer_status TEXT,
                processor_status TEXT,
                database_size_mb REAL,
                total_events INTEGER,
                events_last_hour INTEGER,
                avg_processing_latency_ms REAL,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        self.connection.commit()
        logger.info("📊 Database tables created")
        
    def _create_indexes(self):
        """Create performance indexes for real-time queries"""
        indexes = [
            "CREATE INDEX IF NOT EXISTS idx_pricing_events_timestamp ON pricing_events(timestamp)",
            "CREATE INDEX IF NOT EXISTS idx_pricing_events_event_id ON pricing_events(event_id)",
            "CREATE INDEX IF NOT EXISTS idx_pricing_events_demand ON pricing_events(demand)",
            "CREATE INDEX IF NOT EXISTS idx_ml_predictions_timestamp ON ml_predictions(prediction_timestamp)",
            "CREATE INDEX IF NOT EXISTS idx_performance_metrics_timestamp ON performance_metrics(metric_timestamp)",
            "CREATE INDEX IF NOT EXISTS idx_system_health_timestamp ON system_health(health_check_timestamp)"
        ]
        
        for index_sql in indexes:
            self.connection.execute(index_sql)
            
        self.connection.commit()
        logger.info("🔍 Database indexes created")
        
    def store_pricing_event(self, event_data: Dict[str, Any]) -> bool:
        """
        Store pricing event with all analytics
        Returns True if successful, False otherwise
        """
        try:
            with self.lock:
                cursor = self.connection.cursor()
                cursor.execute("""
                    INSERT OR REPLACE INTO pricing_events 
                    (event_id, timestamp, demand, price, pricing_tier, 
                     moving_average, trend, source, processed_at, processing_latency_ms)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    event_data['event_id'],
                    event_data['timestamp'],
                    event_data['demand'],
                    event_data['price'],
                    event_data['pricing_tier'],
                    event_data.get('moving_average'),
                    event_data.get('trend'),
                    event_data.get('source', 'streaming_system'),
                    event_data.get('processed_at'),
                    event_data.get('processing_latency_ms')
                ))
                
                self.connection.commit()
                logger.debug(f"💾 Stored event #{event_data['event_id']}")
                return True
                
        except Exception as e:
            logger.error(f"❌ Failed to store event: {e}")
            return False
            
    def store_ml_prediction(self, prediction_data: Dict[str, Any]) -> bool:
        """Store ML prediction with tracking"""
        try:
            with self.lock:
                cursor = self.connection.cursor()
                cursor.execute("""
                    INSERT INTO ml_predictions 
                    (event_id, prediction_timestamp, demand_input, predicted_price,
                     model_version, confidence_score, actual_price, prediction_error)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    prediction_data['event_id'],
                    prediction_data['prediction_timestamp'],
                    prediction_data['demand_input'],
                    prediction_data['predicted_price'],
                    prediction_data.get('model_version', 'v1.0'),
                    prediction_data.get('confidence_score'),
                    prediction_data.get('actual_price'),
                    prediction_data.get('prediction_error')
                ))
                
                self.connection.commit()
                logger.debug(f"🤖 Stored ML prediction for event #{prediction_data['event_id']}")
                return True
                
        except Exception as e:
            logger.error(f"❌ Failed to store ML prediction: {e}")
            return False
            
    def store_performance_metrics(self, metrics: Dict[str, Any]) -> bool:
        """Store system performance metrics"""
        try:
            with self.lock:
                cursor = self.connection.cursor()
                cursor.execute("""
                    INSERT INTO performance_metrics 
                    (metric_timestamp, api_latency_ms, requests_per_minute,
                     price_calculation_time_ms, storage_efficiency_percent,
                     queue_depth, memory_usage_mb)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (
                    datetime.now().isoformat(),
                    metrics.get('api_latency_ms'),
                    metrics.get('requests_per_minute'),
                    metrics.get('price_calculation_time_ms'),
                    metrics.get('storage_efficiency_percent'),
                    metrics.get('queue_depth'),
                    metrics.get('memory_usage_mb')
                ))
                
                self.connection.commit()
                logger.debug("📈 Performance metrics stored")
                return True
                
        except Exception as e:
            logger.error(f"❌ Failed to store metrics: {e}")
            return False
            
    def get_latest_pricing_data(self, limit: int = 100) -> List[Dict[str, Any]]:
        """Get latest pricing data for API serving"""
        try:
            cursor = self.connection.cursor()
            cursor.execute("""
                SELECT * FROM pricing_events 
                ORDER BY timestamp DESC 
                LIMIT ?
            """, (limit,))
            
            rows = cursor.fetchall()
            return [dict(row) for row in rows]
            
        except Exception as e:
            logger.error(f"❌ Failed to get latest data: {e}")
            return []
            
    def get_demand_analytics(self, window_minutes: int = 60) -> Dict[str, Any]:
        """Get demand analytics for specified time window"""
        try:
            cursor = self.connection.cursor()
            cutoff_time = (datetime.now() - timedelta(minutes=window_minutes)).isoformat()
            
            cursor.execute("""
                SELECT 
                    COUNT(*) as total_events,
                    AVG(demand) as avg_demand,
                    MIN(demand) as min_demand,
                    MAX(demand) as max_demand,
                    AVG(price) as avg_price,
                    MIN(price) as min_price,
                    MAX(price) as max_price,
                    AVG(processing_latency_ms) as avg_latency
                FROM pricing_events 
                WHERE timestamp > ?
            """, (cutoff_time,))
            
            row = cursor.fetchone()
            if row:
                return {
                    'total_events': row['total_events'],
                    'avg_demand': round(row['avg_demand'], 2) if row['avg_demand'] else 0,
                    'min_demand': row['min_demand'] or 0,
                    'max_demand': row['max_demand'] or 0,
                    'avg_price': round(row['avg_price'], 2) if row['avg_price'] else 0,
                    'min_price': row['min_price'] or 0,
                    'max_price': row['max_price'] or 0,
                    'avg_latency_ms': round(row['avg_latency'], 2) if row['avg_latency'] else 0
                }
            return {}
            
        except Exception as e:
            logger.error(f"❌ Failed to get analytics: {e}")
            return {}
            
    def get_ml_model_performance(self, limit: int = 1000) -> Dict[str, Any]:
        """Get ML model performance metrics"""
        try:
            cursor = self.connection.cursor()
            cursor.execute("""
                SELECT 
                    COUNT(*) as total_predictions,
                    AVG(confidence_score) as avg_confidence,
                    AVG(ABS(prediction_error)) as avg_error,
                    model_version
                FROM ml_predictions 
                WHERE actual_price IS NOT NULL
                ORDER BY prediction_timestamp DESC
                LIMIT ?
            """, (limit,))
            
            row = cursor.fetchone()
            if row:
                return {
                    'total_predictions': row['total_predictions'],
                    'avg_confidence': round(row['avg_confidence'], 3) if row['avg_confidence'] else 0,
                    'avg_prediction_error': round(row['avg_error'], 2) if row['avg_error'] else 0,
                    'model_version': row['model_version']
                }
            return {}
            
        except Exception as e:
            logger.error(f"❌ Failed to get ML performance: {e}")
            return {}
            
    def get_database_stats(self) -> Dict[str, Any]:
        """Get database statistics for monitoring"""
        try:
            cursor = self.connection.cursor()
            
            # Table sizes
            cursor.execute("SELECT COUNT(*) FROM pricing_events")
            pricing_events = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM ml_predictions")
            ml_predictions = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM performance_metrics")
            performance_metrics = cursor.fetchone()[0]
            
            # Database file size
            db_size = os.path.getsize(self.db_path) / (1024 * 1024)  # MB
            
            # Recent activity
            cutoff_time = (datetime.now() - timedelta(hours=1)).isoformat()
            cursor.execute("SELECT COUNT(*) FROM pricing_events WHERE timestamp > ?", (cutoff_time,))
            recent_events = cursor.fetchone()[0]
            
            return {
                'database_path': self.db_path,
                'database_size_mb': round(db_size, 2),
                'total_pricing_events': pricing_events,
                'total_ml_predictions': ml_predictions,
                'total_performance_metrics': performance_metrics,
                'events_last_hour': recent_events,
                'storage_efficiency': round((pricing_events / max(pricing_events + ml_predictions, 1)) * 100, 2)
            }
            
        except Exception as e:
            logger.error(f"❌ Failed to get database stats: {e}")
            return {}
            
    def cleanup_old_data(self, days_to_keep: int = 7):
        """Clean up old data to manage database size"""
        try:
            with self.lock:
                cursor = self.connection.cursor()
                cutoff_date = (datetime.now() - timedelta(days=days_to_keep)).isoformat()
                
                # Delete old pricing events
                cursor.execute("DELETE FROM pricing_events WHERE timestamp < ?", (cutoff_date,))
                pricing_deleted = cursor.rowcount
                
                # Delete old ML predictions
                cursor.execute("DELETE FROM ml_predictions WHERE prediction_timestamp < ?", (cutoff_date,))
                ml_deleted = cursor.rowcount
                
                # Delete old performance metrics
                cursor.execute("DELETE FROM performance_metrics WHERE metric_timestamp < ?", (cutoff_date,))
                metrics_deleted = cursor.rowcount
                
                # Vacuum database
                self.connection.execute("VACUUM")
                self.connection.commit()
                
                logger.info(f"🧹 Cleaned up data: {pricing_deleted} pricing, {ml_deleted} ML, {metrics_deleted} metrics")
                
        except Exception as e:
            logger.error(f"❌ Failed to cleanup old data: {e}")
            
    def close(self):
        """Close database connection"""
        if self.connection:
            self.connection.close()
            logger.info("📁 Database connection closed")

# Cloud Scaling Explanation
"""
CLOUD SCALING STRATEGY:

1. Amazon S3 / BigQuery for Data Lake:
   - Store raw events in S3 for infinite scale
   - Use BigQuery for analytics on historical data
   - Partition by date for efficient querying

2. Amazon RDS / Aurora for Transactional Data:
   - Replace SQLite with PostgreSQL/MySQL
   - Multi-AZ deployment for high availability
   - Read replicas for API serving

3. Amazon DynamoDB for Real-time Data:
   - Fast key-value access for latest prices
   - Auto-scaling throughput
   - TTL for automatic data expiration

4. Data Pipeline:
   - AWS Kinesis Data Streams for real-time ingestion
   - AWS Lambda for stream processing
   - AWS Glue for batch ETL jobs
   - Amazon Redshift for business intelligence
"""

# Initialize global storage instance
storage_layer = DataStorageLayer()
