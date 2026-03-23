"""
STEP 4: Serving Layer - Production API with Caching and Monitoring
High-performance API layer with Redis-like caching and comprehensive logging
"""

import time
import json
import threading
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List
import logging
from collections import defaultdict, deque
import hashlib
import pickle

logger = logging.getLogger(__name__)

class MemoryCache:
    """
    In-memory cache simulating Redis behavior
    Supports TTL, LRU eviction, and cache statistics
    """
    
    def __init__(self, max_size: int = 1000, default_ttl: int = 30):
        self.max_size = max_size
        self.default_ttl = default_ttl
        self.cache = {}
        self.access_times = {}
        self.expiry_times = {}
        self.lock = threading.RLock()
        
        # Cache statistics
        self.hits = 0
        self.misses = 0
        self.evictions = 0
        
    def get(self, key: str) -> Optional[Any]:
        """Get value from cache with TTL check"""
        with self.lock:
            # Check if key exists
            if key not in self.cache:
                self.misses += 1
                return None
                
            # Check TTL
            if key in self.expiry_times:
                if datetime.now() > self.expiry_times[key]:
                    self._remove_key(key)
                    self.misses += 1
                    return None
                    
            # Update access time for LRU
            self.access_times[key] = datetime.now()
            self.hits += 1
            
            logger.debug(f"🎯 Cache HIT for key: {key}")
            return self.cache[key]
            
    def set(self, key: str, value: Any, ttl: Optional[int] = None) -> bool:
        """Set value in cache with optional TTL"""
        with self.lock:
            try:
                # Evict if necessary
                if len(self.cache) >= self.max_size and key not in self.cache:
                    self._evict_lru()
                    
                # Set value
                self.cache[key] = value
                self.access_times[key] = datetime.now()
                
                # Set expiry
                if ttl:
                    self.expiry_times[key] = datetime.now() + timedelta(seconds=ttl)
                elif self.default_ttl:
                    self.expiry_times[key] = datetime.now() + timedelta(seconds=self.default_ttl)
                    
                logger.debug(f"💾 Cache SET for key: {key}")
                return True
                
            except Exception as e:
                logger.error(f"❌ Cache set error: {e}")
                return False
                
    def delete(self, key: str) -> bool:
        """Delete key from cache"""
        with self.lock:
            return self._remove_key(key)
            
    def clear(self):
        """Clear all cache entries"""
        with self.lock:
            self.cache.clear()
            self.access_times.clear()
            self.expiry_times.clear()
            logger.info("🧹 Cache cleared")
            
    def _remove_key(self, key: str) -> bool:
        """Remove key and all its metadata"""
        removed = False
        if key in self.cache:
            del self.cache[key]
            removed = True
        if key in self.access_times:
            del self.access_times[key]
        if key in self.expiry_times:
            del self.expiry_times[key]
        return removed
        
    def _evict_lru(self):
        """Evict least recently used item"""
        if not self.access_times:
            return
            
        # Find LRU key
        lru_key = min(self.access_times.keys(), key=lambda k: self.access_times[k])
        self._remove_key(lru_key)
        self.evictions += 1
        logger.debug(f"🗑️ Evicted LRU key: {lru_key}")
        
    def get_stats(self) -> Dict[str, Any]:
        """Get cache performance statistics"""
        total_requests = self.hits + self.misses
        hit_rate = (self.hits / total_requests * 100) if total_requests > 0 else 0
        
        return {
            'cache_size': len(self.cache),
            'max_size': self.max_size,
            'hits': self.hits,
            'misses': self.misses,
            'evictions': self.evictions,
            'hit_rate_percent': round(hit_rate, 2),
            'memory_usage_mb': round(self._estimate_memory_usage(), 2)
        }
        
    def _estimate_memory_usage(self) -> float:
        """Estimate memory usage in MB"""
        try:
            return len(pickle.dumps(self.cache)) / (1024 * 1024)
        except:
            return 0.0

class RateLimiter:
    """
    Rate limiter for API protection
    Implements token bucket algorithm
    """
    
    def __init__(self, requests_per_minute: int = 60):
        self.requests_per_minute = requests_per_minute
        self.tokens = requests_per_minute
        self.last_refill = datetime.now()
        self.lock = threading.Lock()
        
    def is_allowed(self) -> bool:
        """Check if request is allowed"""
        with self.lock:
            now = datetime.now()
            
            # Refill tokens based on time passed
            time_passed = (now - self.last_refill).total_seconds()
            tokens_to_add = time_passed * (self.requests_per_minute / 60)
            self.tokens = min(self.requests_per_minute, self.tokens + tokens_to_add)
            self.last_refill = now
            
            # Check if request allowed
            if self.tokens >= 1:
                self.tokens -= 1
                return True
            return False

class APIServingLayer:
    """
    Production API Serving Layer
    Provides high-performance endpoints with caching and monitoring
    """
    
    def __init__(self, storage_layer, processed_queue):
        self.storage_layer = storage_layer
        self.processed_queue = processed_queue
        
        # Initialize components
        self.cache = MemoryCache(max_size=1000, default_ttl=30)
        self.rate_limiter = RateLimiter(requests_per_minute=120)
        
        # Performance tracking
        self.request_times = deque(maxlen=1000)
        self.request_counts = defaultdict(int)
        self.error_counts = defaultdict(int)
        
        # API metrics
        self.total_requests = 0
        self.total_errors = 0
        self.start_time = datetime.now()
        
    def create_api_response(self, data: Any, status: str = "success", 
                          message: str = "OK", status_code: int = 200,
                          metadata: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Create standardized API response
        """
        response = {
            "status": status,
            "message": message,
            "data": data,
            "timestamp": datetime.now().isoformat(),
            "version": "v1"
        }
        
        if metadata:
            response["metadata"] = metadata
            
        return response
        
    def get_latest_price(self, include_analytics: bool = True) -> Dict[str, Any]:
        """
        Get latest price with caching
        Main serving endpoint for price data
        """
        start_time = time.time()
        cache_key = f"latest_price_{include_analytics}"
        
        try:
            # Check rate limit
            if not self.rate_limiter.is_allowed():
                return self.create_api_response(
                    None, "error", "Rate limit exceeded", 429
                )
            
            # Check cache first
            cached_data = self.cache.get(cache_key)
            if cached_data:
                processing_time = (time.time() - start_time) * 1000
                return self._add_performance_metadata(
                    cached_data, processing_time, cache_hit=True
                )
            
            # Get fresh data
            latest_data = self._get_fresh_price_data(include_analytics)
            
            if not latest_data:
                return self.create_api_response(
                    None, "error", "No pricing data available", 503
                )
            
            # Structure response
            response_data = self._structure_price_response(latest_data, include_analytics)
            
            # Cache the response
            self.cache.set(cache_key, response_data, ttl=10)  # 10 second TTL
            
            processing_time = (time.time() - start_time) * 1000
            return self._add_performance_metadata(
                self.create_api_response(response_data), processing_time, cache_hit=False
            )
            
        except Exception as e:
            logger.error(f"❌ Price API error: {e}")
            self.total_errors += 1
            processing_time = (time.time() - start_time) * 1000
            return self._add_performance_metadata(
                self.create_api_response(None, "error", str(e), 500), 
                processing_time, cache_hit=False
            )
            
    def get_price_history(self, limit: int = 100, window_minutes: int = 60) -> Dict[str, Any]:
        """
        Get price history with analytics
        """
        start_time = time.time()
        cache_key = f"price_history_{limit}_{window_minutes}"
        
        try:
            # Check cache
            cached_data = self.cache.get(cache_key)
            if cached_data:
                processing_time = (time.time() - start_time) * 1000
                return self._add_performance_metadata(
                    cached_data, processing_time, cache_hit=True
                )
            
            # Get data from storage
            history_data = self.storage_layer.get_latest_pricing_data(limit)
            
            if not history_data:
                return self.create_api_response(
                    [], "success", "No historical data available"
                )
            
            # Add analytics
            analytics = self.storage_layer.get_demand_analytics(window_minutes)
            
            response_data = {
                "history": history_data,
                "analytics": analytics,
                "summary": {
                    "total_events": len(history_data),
                    "time_window_minutes": window_minutes,
                    "data_retention_days": 7
                }
            }
            
            # Cache response
            self.cache.set(cache_key, response_data, ttl=60)  # 1 minute TTL
            
            processing_time = (time.time() - start_time) * 1000
            return self._add_performance_metadata(
                self.create_api_response(response_data), processing_time, cache_hit=False
            )
            
        except Exception as e:
            logger.error(f"❌ History API error: {e}")
            processing_time = (time.time() - start_time) * 1000
            return self._add_performance_metadata(
                self.create_api_response(None, "error", str(e), 500), 
                processing_time, cache_hit=False
            )
            
    def get_system_metrics(self) -> Dict[str, Any]:
        """
        Get comprehensive system metrics
        """
        start_time = time.time()
        
        try:
            # Get database stats
            db_stats = self.storage_layer.get_database_stats()
            
            # Get cache stats
            cache_stats = self.cache.get_stats()
            
            # Get API performance stats
            api_stats = self._get_api_performance_stats()
            
            # Get queue stats
            queue_stats = {
                "processed_queue_depth": self.processed_queue.qsize(),
                "queue_utilization_percent": round(
                    (self.processed_queue.qsize() / 1000) * 100, 2
                )
            }
            
            # Calculate system health
            system_health = self._calculate_system_health(db_stats, cache_stats, api_stats)
            
            response_data = {
                "database": db_stats,
                "cache": cache_stats,
                "api_performance": api_stats,
                "queues": queue_stats,
                "system_health": system_health,
                "uptime_seconds": int((datetime.now() - self.start_time).total_seconds())
            }
            
            processing_time = (time.time() - start_time) * 1000
            return self._add_performance_metadata(
                self.create_api_response(response_data), processing_time, cache_hit=False
            )
            
        except Exception as e:
            logger.error(f"❌ Metrics API error: {e}")
            processing_time = (time.time() - start_time) * 1000
            return self._add_performance_metadata(
                self.create_api_response(None, "error", str(e), 500), 
                processing_time, cache_hit=False
            )
            
    def _get_fresh_price_data(self, include_analytics: bool) -> Optional[Dict[str, Any]]:
        """Get fresh price data from queue or storage"""
        # Try to get from processed queue first (most recent)
        try:
            if not self.processed_queue.empty():
                latest_data = self.processed_queue.queue[-1]
                return latest_data
        except:
            pass
            
        # Fallback to storage
        latest_data = self.storage_layer.get_latest_pricing_data(limit=1)
        return latest_data[0] if latest_data else None
        
    def _structure_price_response(self, data: Dict[str, Any], include_analytics: bool) -> Dict[str, Any]:
        """Structure price response for production API"""
        response = {
            "pricing": {
                "current_price": data.get("price", 0),
                "currency": "USD",
                "pricing_tier": data.get("pricing_tier", "Unknown"),
                "base_price": data.get("base_price", 100),
                "trend_factor": data.get("trend_factor", 1.0),
                "volatility_factor": data.get("volatility_factor", 1.0)
            },
            "demand": {
                "current_demand": data.get("demand", 0),
                "moving_average": data.get("demand_ma_medium", data.get("demand", 0)),
                "trend": data.get("demand_direction_medium", "stable"),
                "momentum": data.get("demand_momentum", 0),
                "volatility": data.get("demand_volatility_medium", 0)
            },
            "metadata": {
                "event_id": data.get("event_id", 0),
                "timestamp": data.get("timestamp", datetime.now().isoformat()),
                "processed_at": data.get("processed_at", datetime.now().isoformat()),
                "source": data.get("source", "streaming_system"),
                "pricing_method": data.get("pricing_method", "advanced_analytics"),
                "processing_latency_ms": data.get("processing_latency_ms", 0)
            }
        }
        
        if include_analytics:
            response["analytics"] = {
                "price_elasticity": data.get("price_elasticity_current", -0.8),
                "demand_range": data.get("demand_range", 0),
                "coefficient_of_variation": data.get("demand_coefficient_of_variation", 0),
                "is_anomaly": data.get("is_anomaly", False),
                "window_analytics": {
                    "short_window": {
                        "size": data.get("window_sizes", {}).get("short", 5),
                        "demand_ma": data.get("demand_ma_short", 0),
                        "price_ma": data.get("price_ma_short", 0)
                    },
                    "medium_window": {
                        "size": data.get("window_sizes", {}).get("medium", 10),
                        "demand_ma": data.get("demand_ma_medium", 0),
                        "price_ma": data.get("price_ma_medium", 0)
                    },
                    "long_window": {
                        "size": data.get("window_sizes", {}).get("long", 50),
                        "demand_ma": data.get("demand_ma_long", 0),
                        "price_ma": data.get("price_ma_long", 0)
                    }
                }
            }
            
        return response
        
    def _get_api_performance_stats(self) -> Dict[str, Any]:
        """Get API performance statistics"""
        now = datetime.now()
        uptime = (now - self.start_time).total_seconds()
        
        # Calculate requests per minute
        recent_requests = [t for t in self.request_times 
                          if (now - t).total_seconds() <= 60]
        requests_per_minute = len(recent_requests)
        
        # Calculate average response time
        avg_response_time = sum(self.request_times) / len(self.request_times) if self.request_times else 0
        
        # Calculate p95 response time
        if self.request_times:
            sorted_times = sorted(self.request_times)
            p95_index = int(len(sorted_times) * 0.95)
            p95_response_time = sorted_times[min(p95_index, len(sorted_times)-1)]
        else:
            p95_response_time = 0
            
        return {
            "total_requests": self.total_requests,
            "total_errors": self.total_errors,
            "error_rate_percent": round((self.total_errors / max(self.total_requests, 1)) * 100, 2),
            "requests_per_minute": requests_per_minute,
            "avg_response_time_ms": round(avg_response_time, 2),
            "p95_response_time_ms": round(p95_response_time, 2),
            "uptime_seconds": int(uptime)
        }
        
    def _calculate_system_health(self, db_stats: Dict, cache_stats: Dict, 
                               api_stats: Dict) -> Dict[str, Any]:
        """Calculate overall system health score"""
        health_score = 100
        
        # Database health (30% weight)
        if db_stats.get('total_pricing_events', 0) == 0:
            health_score -= 30
        elif db_stats.get('events_last_hour', 0) < 10:
            health_score -= 15
            
        # Cache health (20% weight)
        cache_hit_rate = cache_stats.get('hit_rate_percent', 0)
        if cache_hit_rate < 50:
            health_score -= 20
        elif cache_hit_rate < 70:
            health_score -= 10
            
        # API health (30% weight)
        error_rate = api_stats.get('error_rate_percent', 0)
        if error_rate > 10:
            health_score -= 30
        elif error_rate > 5:
            health_score -= 15
        elif error_rate > 1:
            health_score -= 5
            
        # Response time health (20% weight)
        avg_response_time = api_stats.get('avg_response_time_ms', 0)
        if avg_response_time > 1000:
            health_score -= 20
        elif avg_response_time > 500:
            health_score -= 10
        elif avg_response_time > 200:
            health_score -= 5
            
        health_score = max(0, health_score)
        
        if health_score >= 90:
            status = "excellent"
        elif health_score >= 75:
            status = "good"
        elif health_score >= 60:
            status = "fair"
        else:
            status = "poor"
            
        return {
            "health_score": health_score,
            "status": status,
            "components": {
                "database": "healthy" if db_stats.get('total_pricing_events', 0) > 0 else "unhealthy",
                "cache": f"hit_rate_{cache_hit_rate:.0f}%",
                "api": f"error_rate_{error_rate:.1f}%"
            }
        }
        
    def _add_performance_metadata(self, response: Dict[str, Any], 
                                 processing_time: float, cache_hit: bool) -> Dict[str, Any]:
        """Add performance metadata to response"""
        self.total_requests += 1
        self.request_times.append(processing_time)
        
        if response.get("status") == "error":
            self.total_errors += 1
            
        if "metadata" not in response:
            response["metadata"] = {}
            
        response["metadata"].update({
            "response_time_ms": round(processing_time, 2),
            "cache_hit": cache_hit,
            "request_id": hashlib.md5(
                f"{datetime.now().isoformat()}{processing_time}".encode()
            ).hexdigest()[:8]
        })
        
        return response

# Serving Layer Architecture Explanation
"""
SERVING LAYER ARCHITECTURE:

1. Redis-like Caching:
   - TTL (Time To Live) support
   - LRU (Least Recently Used) eviction
   - Cache hit/miss statistics
   - Memory usage monitoring

2. Rate Limiting:
   - Token bucket algorithm
   - Configurable request limits
   - Protection against abuse

3. API Performance:
   - Response time tracking
   - Error rate monitoring
   - Request throughput metrics
   - P95 response time calculation

4. Production Features:
   - Standardized API responses
   - Comprehensive error handling
   - Request IDs for tracing
   - Health checks and monitoring

5. Cloud Scaling:
   - Amazon API Gateway for API management
   - Amazon ElastiCache for Redis caching
   - AWS CloudWatch for monitoring
   - AWS X-Ray for request tracing
"""
