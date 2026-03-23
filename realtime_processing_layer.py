"""
STEP 3: Real-Time Processing Layer - Advanced Stream Analytics
Mimics Apache Spark Streaming and Apache Flink processing
"""

import time
import threading
import queue
import statistics
import math
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
import logging
from collections import deque
import json

logger = logging.getLogger(__name__)

class RealTimeProcessor:
    """
    Advanced Real-Time Stream Processor
    Implements windowing, aggregations, and complex analytics
    """
    
    def __init__(self, input_queue: queue.Queue, output_queue: queue.Queue, storage_layer):
        self.input_queue = input_queue
        self.output_queue = output_queue
        self.storage_layer = storage_layer
        
        # Processing configuration
        self.is_running = False
        self.processor_thread = None
        
        # Window configurations
        self.window_sizes = {
            'short': 5,      # 5 events - immediate trends
            'medium': 10,    # 10 events - moving averages
            'long': 50       # 50 events - stable patterns
        }
        
        # Data windows for analytics
        self.demand_windows = {
            'short': deque(maxlen=self.window_sizes['short']),
            'medium': deque(maxlen=self.window_sizes['medium']),
            'long': deque(maxlen=self.window_sizes['long'])
        }
        
        self.price_windows = {
            'short': deque(maxlen=self.window_sizes['short']),
            'medium': deque(maxlen=self.window_sizes['medium']),
            'long': deque(maxlen=self.window_sizes['long'])
        }
        
        # Analytics state
        self.processed_count = 0
        self.error_count = 0
        self.last_processing_time = None
        
        # Performance tracking
        self.processing_times = deque(maxlen=100)
        self.throughput_tracker = deque(maxlen=60)  # Last 60 seconds
        
    def start_processing(self):
        """Start the real-time processing engine"""
        if self.is_running:
            logger.warning("Processor is already running")
            return
            
        self.is_running = True
        self.processor_thread = threading.Thread(target=self._processing_loop, daemon=True)
        self.processor_thread.start()
        logger.info("🚀 Real-Time Processor started with advanced analytics")
        
    def stop_processing(self):
        """Stop the processing engine"""
        self.is_running = False
        if self.processor_thread:
            self.processor_thread.join(timeout=5)
        logger.info("⏹️ Real-Time Processor stopped")
        
    def _processing_loop(self):
        """Main processing loop with advanced analytics"""
        while self.is_running:
            try:
                start_time = time.time()
                
                # Get event from queue
                event = self.input_queue.get(timeout=1)
                
                # Process with advanced analytics
                processed_event = self._process_with_analytics(event)
                
                # Send to output queue
                self.output_queue.put(processed_event)
                
                # Store in database
                self.storage_layer.store_pricing_event(processed_event)
                
                # Track performance
                processing_time = (time.time() - start_time) * 1000
                self.processing_times.append(processing_time)
                self.processed_count += 1
                
                # Track throughput
                current_time = datetime.now()
                self.throughput_tracker.append(current_time)
                
                logger.debug(f"⚡ Processed event #{processed_event['event_id']} in {processing_time:.2f}ms")
                
            except queue.Empty:
                continue
            except Exception as e:
                self.error_count += 1
                logger.error(f"❌ Processing error: {e}")
                
    def _process_with_analytics(self, event: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process event with comprehensive real-time analytics
        """
        processing_start = datetime.now()
        
        # Calculate price using advanced pricing logic
        price_data = self._calculate_advanced_price(event)
        
        # Update windows
        self._update_windows(event['demand'], price_data['price'])
        
        # Calculate comprehensive analytics
        analytics = self._calculate_analytics()
        
        # Determine pricing tier
        pricing_tier = self._determine_pricing_tier(price_data['price'])
        
        # Create processed event
        processed_event = {
            **event,
            **price_data,
            **analytics,
            'pricing_tier': pricing_tier,
            'processed_at': processing_start.isoformat(),
            'processing_latency_ms': self._calculate_latency(event['timestamp']),
            'processed_count': self.processed_count,
            'error_rate_percent': round((self.error_count / max(self.processed_count, 1)) * 100, 2)
        }
        
        self.last_processing_time = processing_start
        return processed_event
        
    def _calculate_advanced_price(self, event: Dict[str, Any]) -> Dict[str, Any]:
        """
        Advanced price calculation with multiple factors
        """
        demand = event['demand']
        
        # Base pricing formula with elasticity
        base_price = 100.0
        demand_elasticity = 0.8
        price = base_price + (demand * demand_elasticity)
        
        # Apply moving average smoothing
        if len(self.demand_windows['medium']) >= 3:
            avg_demand = statistics.mean(self.demand_windows['medium'])
            smoothing_factor = 0.3
            price = price * (1 - smoothing_factor) + (base_price + avg_demand * demand_elasticity) * smoothing_factor
        
        # Apply trend adjustment
        trend_factor = self._calculate_trend_factor()
        price *= trend_factor
        
        # Apply volatility adjustment
        volatility_factor = self._calculate_volatility_factor()
        price *= volatility_factor
        
        # Ensure price bounds
        price = max(50.0, min(250.0, price))
        
        return {
            'price': round(price, 2),
            'base_price': base_price,
            'demand_elasticity': demand_elasticity,
            'trend_factor': round(trend_factor, 3),
            'volatility_factor': round(volatility_factor, 3),
            'pricing_method': 'advanced_analytics'
        }
        
    def _update_windows(self, demand: float, price: float):
        """Update sliding windows for analytics"""
        for window_type in self.demand_windows:
            self.demand_windows[window_type].append(demand)
            
        for window_type in self.price_windows:
            self.price_windows[window_type].append(price)
            
    def _calculate_analytics(self) -> Dict[str, Any]:
        """
        Calculate comprehensive real-time analytics
        """
        analytics = {}
        
        # Moving averages for different windows
        for window_type, window in self.demand_windows.items():
            if window:
                analytics[f'demand_ma_{window_type}'] = round(statistics.mean(window), 2)
                analytics[f'demand_volatility_{window_type}'] = round(statistics.stdev(window) if len(window) > 1 else 0, 2)
                
        for window_type, window in self.price_windows.items():
            if window:
                analytics[f'price_ma_{window_type}'] = round(statistics.mean(window), 2)
                analytics[f'price_volatility_{window_type}'] = round(statistics.stdev(window) if len(window) > 1 else 0, 2)
        
        # Trend analysis
        analytics.update(self._calculate_trends())
        
        # Demand patterns
        analytics.update(self._analyze_demand_patterns())
        
        # Price elasticity
        analytics['price_elasticity_current'] = self._calculate_price_elasticity()
        
        # Anomaly detection
        analytics['is_anomaly'] = self._detect_anomaly()
        
        return analytics
        
    def _calculate_trends(self) -> Dict[str, Any]:
        """Calculate various trend indicators"""
        trends = {}
        
        for window_type, window in self.demand_windows.items():
            if len(window) >= 3:
                # Linear regression for trend
                x = list(range(len(window)))
                y = list(window)
                
                n = len(window)
                sum_x = sum(x)
                sum_y = sum(y)
                sum_xy = sum(x[i] * y[i] for i in range(n))
                sum_x2 = sum(x[i] ** 2 for i in range(n))
                
                # Calculate slope (trend)
                if n * sum_x2 - sum_x ** 2 != 0:
                    slope = (n * sum_xy - sum_x * sum_y) / (n * sum_x2 - sum_x ** 2)
                    trends[f'demand_trend_{window_type}'] = round(slope, 3)
                    
                    # Determine trend direction
                    if slope > 0.5:
                        trends[f'demand_direction_{window_type}'] = 'increasing'
                    elif slope < -0.5:
                        trends[f'demand_direction_{window_type}'] = 'decreasing'
                    else:
                        trends[f'demand_direction_{window_type}'] = 'stable'
                        
        return trends
        
    def _analyze_demand_patterns(self) -> Dict[str, Any]:
        """Analyze demand patterns and seasonality"""
        patterns = {}
        
        if len(self.demand_windows['medium']) >= 5:
            recent_demand = list(self.demand_windows['medium'])
            
            # Demand momentum (rate of change)
            if len(recent_demand) >= 2:
                momentum = recent_demand[-1] - recent_demand[-2]
                patterns['demand_momentum'] = round(momentum, 2)
                
            # Demand acceleration (change in momentum)
            if len(recent_demand) >= 3:
                acceleration = (recent_demand[-1] - recent_demand[-2]) - (recent_demand[-2] - recent_demand[-3])
                patterns['demand_acceleration'] = round(acceleration, 2)
                
            # Demand range
            patterns['demand_range'] = round(max(recent_demand) - min(recent_demand), 2)
            patterns['demand_coefficient_of_variation'] = round(
                statistics.stdev(recent_demand) / statistics.mean(recent_demand) * 100, 2
            ) if len(recent_demand) > 1 else 0
            
        return patterns
        
    def _calculate_trend_factor(self) -> float:
        """Calculate trend adjustment factor for pricing"""
        if len(self.demand_windows['medium']) >= 5:
            recent_avg = statistics.mean(list(self.demand_windows['medium'])[-3:])
            older_avg = statistics.mean(list(self.demand_windows['medium'])[:3])
            
            if older_avg > 0:
                trend_ratio = recent_avg / older_avg
                # Clamp trend factor to reasonable range
                return max(0.8, min(1.2, trend_ratio))
                
        return 1.0
        
    def _calculate_volatility_factor(self) -> float:
        """Calculate volatility adjustment factor for pricing"""
        if len(self.demand_windows['medium']) >= 3:
            volatility = statistics.stdev(self.demand_windows['medium'])
            mean_demand = statistics.mean(self.demand_windows['medium'])
            
            if mean_demand > 0:
                cv = volatility / mean_demand  # Coefficient of variation
                # Higher volatility -> higher prices (risk premium)
                volatility_factor = 1.0 + (cv * 0.1)
                return max(0.9, min(1.3, volatility_factor))
                
        return 1.0
        
    def _calculate_price_elasticity(self) -> float:
        """Calculate current price elasticity of demand"""
        if (len(self.demand_windows['medium']) >= 3 and 
            len(self.price_windows['medium']) >= 3):
            
            demand_changes = []
            price_changes = []
            
            demand_list = list(self.demand_windows['medium'])
            price_list = list(self.price_windows['medium'])
            
            for i in range(1, len(demand_list)):
                if demand_list[i-1] > 0 and price_list[i-1] > 0:
                    demand_pct_change = (demand_list[i] - demand_list[i-1]) / demand_list[i-1]
                    price_pct_change = (price_list[i] - price_list[i-1]) / price_list[i-1]
                    
                    if price_pct_change != 0:
                        elasticity = demand_pct_change / price_pct_change
                        demand_changes.append(elasticity)
                        
            if demand_changes:
                return round(statistics.mean(demand_changes), 3)
                
        return -0.8  # Default elasticity
        
    def _detect_anomaly(self) -> bool:
        """Detect anomalous demand patterns"""
        if len(self.demand_windows['medium']) >= 5:
            recent_demand = list(self.demand_windows['medium'])[-1]
            historical_demand = list(self.demand_windows['medium'])[:-1]
            
            if historical_demand:
                mean_demand = statistics.mean(historical_demand)
                std_demand = statistics.stdev(historical_demand)
                
                # Flag as anomaly if beyond 2 standard deviations
                if std_demand > 0:
                    z_score = abs(recent_demand - mean_demand) / std_demand
                    return z_score > 2.0
                    
        return False
        
    def _determine_pricing_tier(self, price: float) -> str:
        """Determine pricing tier based on price"""
        if price < 80:
            return 'Low'
        elif price < 150:
            return 'Medium'
        elif price < 200:
            return 'High'
        else:
            return 'Premium'
            
    def _calculate_latency(self, event_timestamp: str) -> float:
        """Calculate processing latency in milliseconds"""
        try:
            event_time = datetime.fromisoformat(event_timestamp.replace('Z', '+00:00'))
            current_time = datetime.now()
            latency = (current_time - event_time).total_seconds() * 1000
            return round(latency, 2)
        except:
            return 0.0
            
    def get_processing_stats(self) -> Dict[str, Any]:
        """Get real-time processing statistics"""
        current_time = datetime.now()
        
        # Calculate throughput (events per second)
        recent_events = [t for t in self.throughput_tracker 
                        if (current_time - t).total_seconds() <= 60]
        throughput = len(recent_events) / 60.0 if recent_events else 0
        
        # Calculate average processing time
        avg_processing_time = statistics.mean(self.processing_times) if self.processing_times else 0
        
        # Calculate p95 processing time
        if self.processing_times:
            sorted_times = sorted(self.processing_times)
            p95_index = int(len(sorted_times) * 0.95)
            p95_processing_time = sorted_times[min(p95_index, len(sorted_times)-1)]
        else:
            p95_processing_time = 0
            
        return {
            'processed_events': self.processed_count,
            'error_count': self.error_count,
            'error_rate_percent': round((self.error_count / max(self.processed_count, 1)) * 100, 2),
            'throughput_events_per_second': round(throughput, 2),
            'avg_processing_time_ms': round(avg_processing_time, 2),
            'p95_processing_time_ms': round(p95_processing_time, 2),
            'last_processing_time': self.last_processing_time.isoformat() if self.last_processing_time else None,
            'queue_depth': self.input_queue.qsize(),
            'window_sizes': self.window_sizes,
            'current_window_sizes': {
                'short_demand': len(self.demand_windows['short']),
                'medium_demand': len(self.demand_windows['medium']),
                'long_demand': len(self.demand_windows['long'])
            }
        }

# Stream Processing Architecture Explanation
"""
STREAM PROCESSING ARCHITECTURE:

1. Apache Spark Streaming Equivalent:
   - Micro-batch processing with windowing
   - Stateful transformations (moving averages)
   - Join operations (demand + price analytics)
   - Aggregations and time-series operations

2. Apache Flink Equivalent:
   - Event-at-a-time processing
   - Window operators (tumbling, sliding)
   - State management for analytics
   - Low-latency processing

3. Real-Time Analytics:
   - Moving averages (5, 10, 50 event windows)
   - Trend detection with linear regression
   - Volatility calculations
   - Anomaly detection (statistical outliers)
   - Price elasticity calculations

4. Performance Optimizations:
   - Deque data structures for O(1) operations
   - Sliding windows for memory efficiency
   - Parallel processing capabilities
   - Backpressure handling via queue sizes
"""
