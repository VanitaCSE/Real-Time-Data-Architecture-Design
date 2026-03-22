"""
🌐 Serving Layer - API Service
High-performance API service for pricing data
Maps to: AWS API Gateway + Lambda + ElastiCache
"""

import json
import logging
import time
import redis
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
from dataclasses import dataclass, field
from functools import wraps
import statistics

logger = logging.getLogger(__name__)

@dataclass
class APIResponse:
    """Standard API response structure"""
    status: str
    message: str
    data: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    timestamp: str = field(default_factory=lambda: datetime.now().isoformat())
    request_id: Optional[str] = None
    cached: bool = False

@dataclass
class PricingData:
    """Pricing data structure"""
    current_price: float
    currency: str = 'USD'
    pricing_tier: str = 'Medium'
    demand: int = 50
    moving_average: float = 50.0
    volatility: float = 0.0
    trend_strength: float = 0.0
    pricing_method: str = 'ml'
    last_updated: str = field(default_factory=lambda: datetime.now().isoformat())

class CacheManager:
    """
    Redis cache manager for high-performance API
    Maps to: AWS ElastiCache Redis
    """
    
    def __init__(self, host: str = 'localhost', port: int = 6379, db: int = 0, use_local: bool = True):
        self.use_local = use_local
        self.redis_client = None
        
        if not use_local:
            try:
                self.redis_client = redis.Redis(host=host, port=port, db=db, decode_responses=True)
                self.redis_client.ping()
                logger.info("Connected to Redis cache")
            except Exception as e:
                logger.warning(f"Redis connection failed, using local cache: {e}")
                self.use_local = True
        
        # Local cache fallback
        self.local_cache = {}
        self.cache_stats = {
            'hits': 0,
            'misses': 0,
            'sets': 0,
            'start_time': datetime.now()
        }
    
    def get(self, key: str) -> Optional[Any]:
        """Get value from cache"""
        try:
            if self.redis_client and not self.use_local:
                value = self.redis_client.get(key)
                if value:
                    self.cache_stats['hits'] += 1
                    return json.loads(value)
            else:
                # Local cache with TTL
                if key in self.local_cache:
                    cache_item = self.local_cache[key]
                    if datetime.now() < cache_item['expires']:
                        self.cache_stats['hits'] += 1
                        return cache_item['value']
                    else:
                        del self.local_cache[key]
            
            self.cache_stats['misses'] += 1
            return None
            
        except Exception as e:
            logger.error(f"Cache get error: {e}")
            self.cache_stats['misses'] += 1
            return None
    
    def set(self, key: str, value: Any, ttl_seconds: int = 300) -> bool:
        """Set value in cache"""
        try:
            serialized_value = json.dumps(value, default=str)
            
            if self.redis_client and not self.use_local:
                success = self.redis_client.setex(key, ttl_seconds, serialized_value)
                if success:
                    self.cache_stats['sets'] += 1
                    return True
            else:
                # Local cache with TTL
                self.local_cache[key] = {
                    'value': value,
                    'expires': datetime.now() + timedelta(seconds=ttl_seconds)
                }
                self.cache_stats['sets'] += 1
                return True
            
            return False
            
        except Exception as e:
            logger.error(f"Cache set error: {e}")
            return False
    
    def delete(self, key: str) -> bool:
        """Delete key from cache"""
        try:
            if self.redis_client and not self.use_local:
                return bool(self.redis_client.delete(key))
            else:
                if key in self.local_cache:
                    del self.local_cache[key]
                    return True
                return False
                
        except Exception as e:
            logger.error(f"Cache delete error: {e}")
            return False
    
    def get_stats(self) -> Dict[str, Any]:
        """Get cache statistics"""
        runtime = datetime.now() - self.cache_stats['start_time']
        total_requests = self.cache_stats['hits'] + self.cache_stats['misses']
        
        return {
            'hits': self.cache_stats['hits'],
            'misses': self.cache_stats['misses'],
            'sets': self.cache_stats['sets'],
            'hit_rate': (self.cache_stats['hits'] / max(total_requests, 1)) * 100,
            'runtime_seconds': int(runtime.total_seconds()),
            'cache_size': len(self.local_cache) if self.use_local else 'N/A'
        }

class APIService:
    """
    High-performance API service for pricing data
    Maps to: API Gateway + Lambda architecture
    """
    
    def __init__(self, cache_manager: Optional[CacheManager] = None):
        self.cache_manager = cache_manager or CacheManager(use_local=True)
        self.api_stats = {
            'total_requests': 0,
            'successful_requests': 0,
            'failed_requests': 0,
            'response_times': [],
            'start_time': datetime.now()
        }
        
        # Mock data source (would be DynamoDB in production)
        self.data_source = MockDataSource()
    
    def get_current_pricing(self, request_id: Optional[str] = None) -> APIResponse:
        """
        Get current pricing data with caching
        Maps to: API Gateway → Lambda → ElastiCache → DynamoDB
        """
        start_time = time.time()
        self.api_stats['total_requests'] += 1
        
        try:
            # Try cache first
            cache_key = 'current_pricing'
            cached_data = self.cache_manager.get(cache_key)
            
            if cached_data:
                response_time = (time.time() - start_time) * 1000
                self._update_response_time(response_time)
                
                return APIResponse(
                    status='success',
                    message='Pricing data retrieved from cache',
                    data=cached_data,
                    request_id=request_id,
                    cached=True
                )
            
            # Cache miss - get fresh data
            pricing_data = self.data_source.get_latest_pricing()
            
            if not pricing_data:
                self.api_stats['failed_requests'] += 1
                return APIResponse(
                    status='error',
                    message='No pricing data available',
                    error='Service temporarily unavailable',
                    request_id=request_id
                )
            
            # Cache the result
            self.cache_manager.set(cache_key, pricing_data.__dict__, ttl_seconds=60)
            
            # Prepare response data
            response_data = {
                'pricing': {
                    'current_price': pricing_data.current_price,
                    'currency': pricing_data.currency,
                    'pricing_tier': pricing_data.pricing_tier
                },
                'demand': {
                    'current_demand': pricing_data.demand,
                    'moving_average': pricing_data.moving_average,
                    'trend': 'stable'  # Would be calculated from data
                },
                'analytics': {
                    'volatility': pricing_data.volatility,
                    'trend_strength': pricing_data.trend_strength,
                    'pricing_method': pricing_data.pricing_method
                },
                'metadata': {
                    'last_updated': pricing_data.last_updated,
                    'data_source': 'production'
                }
            }
            
            response_time = (time.time() - start_time) * 1000
            self._update_response_time(response_time)
            self.api_stats['successful_requests'] += 1
            
            return APIResponse(
                status='success',
                message='Pricing data retrieved successfully',
                data=response_data,
                request_id=request_id,
                cached=False
            )
            
        except Exception as e:
            self.api_stats['failed_requests'] += 1
            logger.error(f"Error getting current pricing: {e}")
            return APIResponse(
                status='error',
                message='Internal server error',
                error=str(e),
                request_id=request_id
            )
    
    def get_pricing_history(self, hours: int = 24, request_id: Optional[str] = None) -> APIResponse:
        """
        Get pricing history with caching
        Maps to: API Gateway → Lambda → S3/DynamoDB
        """
        start_time = time.time()
        self.api_stats['total_requests'] += 1
        
        try:
            # Check cache
            cache_key = f'pricing_history_{hours}h'
            cached_data = self.cache_manager.get(cache_key)
            
            if cached_data:
                response_time = (time.time() - start_time) * 1000
                self._update_response_time(response_time)
                
                return APIResponse(
                    status='success',
                    message='Pricing history retrieved from cache',
                    data=cached_data,
                    request_id=request_id,
                    cached=True
                )
            
            # Get fresh data
            history_data = self.data_source.get_pricing_history(hours)
            
            if not history_data:
                return APIResponse(
                    status='error',
                    message='No historical data available',
                    error='No data found for specified period',
                    request_id=request_id
                )
            
            # Cache the result (longer TTL for historical data)
            self.cache_manager.set(cache_key, history_data, ttl_seconds=1800)
            
            response_time = (time.time() - start_time) * 1000
            self._update_response_time(response_time)
            self.api_stats['successful_requests'] += 1
            
            return APIResponse(
                status='success',
                message='Pricing history retrieved successfully',
                data=history_data,
                request_id=request_id,
                cached=False
            )
            
        except Exception as e:
            self.api_stats['failed_requests'] += 1
            logger.error(f"Error getting pricing history: {e}")
            return APIResponse(
                status='error',
                message='Internal server error',
                error=str(e),
                request_id=request_id
            )
    
    def get_pricing_analytics(self, request_id: Optional[str] = None) -> APIResponse:
        """
        Get pricing analytics with caching
        Maps to: API Gateway → Lambda → Analytics Service
        """
        start_time = time.time()
        self.api_stats['total_requests'] += 1
        
        try:
            # Check cache
            cache_key = 'pricing_analytics'
            cached_data = self.cache_manager.get(cache_key)
            
            if cached_data:
                response_time = (time.time() - start_time) * 1000
                self._update_response_time(response_time)
                
                return APIResponse(
                    status='success',
                    message='Analytics data retrieved from cache',
                    data=cached_data,
                    request_id=request_id,
                    cached=True
                )
            
            # Get fresh analytics
            analytics_data = self.data_source.get_pricing_analytics()
            
            # Cache the result
            self.cache_manager.set(cache_key, analytics_data, ttl_seconds=300)
            
            response_time = (time.time() - start_time) * 1000
            self._update_response_time(response_time)
            self.api_stats['successful_requests'] += 1
            
            return APIResponse(
                status='success',
                message='Analytics data retrieved successfully',
                data=analytics_data,
                request_id=request_id,
                cached=False
            )
            
        except Exception as e:
            self.api_stats['failed_requests'] += 1
            logger.error(f"Error getting analytics: {e}")
            return APIResponse(
                status='error',
                message='Internal server error',
                error=str(e),
                request_id=request_id
            )
    
    def invalidate_cache(self, pattern: str = '*') -> APIResponse:
        """
        Invalidate cache entries
        Maps to: Cache management API
        """
        try:
            # In production, this would use Redis pattern matching
            # For local cache, we'll clear everything
            if self.cache_manager.use_local:
                self.cache_manager.local_cache.clear()
            else:
                # Redis pattern deletion would go here
                pass
            
            return APIResponse(
                status='success',
                message=f'Cache invalidated for pattern: {pattern}',
                data={'pattern': pattern}
            )
            
        except Exception as e:
            logger.error(f"Error invalidating cache: {e}")
            return APIResponse(
                status='error',
                message='Failed to invalidate cache',
                error=str(e)
            )
    
    def get_api_stats(self) -> APIResponse:
        """Get API performance statistics"""
        try:
            runtime = datetime.now() - self.api_stats['start_time']
            
            # Calculate performance metrics
            if self.api_stats['response_times']:
                avg_time = statistics.mean(self.api_stats['response_times'])
                p95_time = sorted(self.api_stats['response_times'])[int(len(self.api_stats['response_times']) * 0.95)]
                p99_time = sorted(self.api_stats['response_times'])[int(len(self.api_stats['response_times']) * 0.99)]
            else:
                avg_time = p95_time = p99_time = 0
            
            stats_data = {
                'requests': {
                    'total': self.api_stats['total_requests'],
                    'successful': self.api_stats['successful_requests'],
                    'failed': self.api_stats['failed_requests'],
                    'success_rate': (self.api_stats['successful_requests'] / max(self.api_stats['total_requests'], 1)) * 100
                },
                'performance': {
                    'avg_response_time_ms': round(avg_time, 2),
                    'p95_response_time_ms': round(p95_time, 2),
                    'p99_response_time_ms': round(p99_time, 2),
                    'min_response_time_ms': min(self.api_stats['response_times']) if self.api_stats['response_times'] else 0,
                    'max_response_time_ms': max(self.api_stats['response_times']) if self.api_stats['response_times'] else 0
                },
                'runtime': {
                    'start_time': self.api_stats['start_time'].isoformat(),
                    'runtime_seconds': int(runtime.total_seconds())
                },
                'cache_stats': self.cache_manager.get_stats()
            }
            
            return APIResponse(
                status='success',
                message='API statistics retrieved successfully',
                data=stats_data
            )
            
        except Exception as e:
            logger.error(f"Error getting API stats: {e}")
            return APIResponse(
                status='error',
                message='Failed to retrieve statistics',
                error=str(e)
            )
    
    def _update_response_time(self, response_time_ms: float) -> None:
        """Update response time statistics"""
        self.api_stats['response_times'].append(response_time_ms)
        
        # Keep only last 1000 response times
        if len(self.api_stats['response_times']) > 1000:
            self.api_stats['response_times'] = self.api_stats['response_times'][-1000:]

class MockDataSource:
    """
    Mock data source for development
    Maps to: DynamoDB in production
    """
    
    def __init__(self):
        self.current_pricing = PricingData(
            current_price=125.50,
            demand=75,
            moving_average=72.3,
            volatility=0.15,
            trend_strength=0.8,
            pricing_method='ml'
        )
    
    def get_latest_pricing(self) -> Optional[PricingData]:
        """Get latest pricing data"""
        # Simulate real-time updates
        import random
        self.current_pricing.current_price += random.uniform(-2, 2)
        self.current_pricing.demand = max(0, min(100, self.current_pricing.demand + random.randint(-5, 5)))
        self.current_pricing.last_updated = datetime.now().isoformat()
        
        return self.current_pricing
    
    def get_pricing_history(self, hours: int) -> Optional[Dict[str, Any]]:
        """Get pricing history"""
        history = []
        base_time = datetime.now() - timedelta(hours=hours)
        
        for i in range(hours * 4):  # 15-minute intervals
            timestamp = base_time + timedelta(minutes=i * 15)
            price = 100 + (i % 20) + (i * 0.1)
            
            history.append({
                'timestamp': timestamp.isoformat(),
                'price': round(price, 2),
                'demand': 50 + (i % 30),
                'pricing_tier': 'Medium' if price < 120 else 'High'
            })
        
        return {
            'period_hours': hours,
            'data_points': len(history),
            'history': history
        }
    
    def get_pricing_analytics(self) -> Optional[Dict[str, Any]]:
        """Get pricing analytics"""
        return {
            'summary': {
                'avg_price': 118.75,
                'min_price': 95.50,
                'max_price': 145.25,
                'price_volatility': 0.12
            },
            'tiers': {
                'Low': {'count': 145, 'avg_price': 105.20},
                'Medium': {'count': 298, 'avg_price': 118.75},
                'High': {'count': 57, 'avg_price': 142.30}
            },
            'trends': {
                'price_trend': 'increasing',
                'demand_trend': 'stable',
                'volatility_trend': 'decreasing'
            }
        }

# Flask API implementation
from flask import Flask, request, jsonify

app = Flask(__name__)
api_service = APIService()

@app.route('/v1/pricing/current', methods=['GET'])
def get_current_pricing():
    """Get current pricing"""
    request_id = request.headers.get('X-Request-ID')
    response = api_service.get_current_pricing(request_id)
    
    status_code = 200 if response.status == 'success' else 500
    return jsonify({
        'status': response.status,
        'message': response.message,
        'data': response.data,
        'error': response.error,
        'timestamp': response.timestamp,
        'request_id': response.request_id,
        'cached': response.cached
    }), status_code

@app.route('/v1/pricing/history', methods=['GET'])
def get_pricing_history():
    """Get pricing history"""
    hours = request.args.get('hours', 24, type=int)
    request_id = request.headers.get('X-Request-ID')
    
    response = api_service.get_pricing_history(hours, request_id)
    
    status_code = 200 if response.status == 'success' else 500
    return jsonify({
        'status': response.status,
        'message': response.message,
        'data': response.data,
        'error': response.error,
        'timestamp': response.timestamp,
        'request_id': response.request_id,
        'cached': response.cached
    }), status_code

@app.route('/v1/pricing/analytics', methods=['GET'])
def get_pricing_analytics():
    """Get pricing analytics"""
    request_id = request.headers.get('X-Request-ID')
    
    response = api_service.get_pricing_analytics(request_id)
    
    status_code = 200 if response.status == 'success' else 500
    return jsonify({
        'status': response.status,
        'message': response.message,
        'data': response.data,
        'error': response.error,
        'timestamp': response.timestamp,
        'request_id': response.request_id,
        'cached': response.cached
    }), status_code

@app.route('/v1/cache/invalidate', methods=['POST'])
def invalidate_cache():
    """Invalidate cache"""
    pattern = request.json.get('pattern', '*') if request.json else '*'
    
    response = api_service.invalidate_cache(pattern)
    
    status_code = 200 if response.status == 'success' else 500
    return jsonify({
        'status': response.status,
        'message': response.message,
        'data': response.data,
        'error': response.error,
        'timestamp': response.timestamp
    }), status_code

@app.route('/v1/stats', methods=['GET'])
def get_api_stats():
    """Get API statistics"""
    response = api_service.get_api_stats()
    
    status_code = 200 if response.status == 'success' else 500
    return jsonify({
        'status': response.status,
        'message': response.message,
        'data': response.data,
        'error': response.error,
        'timestamp': response.timestamp
    }), status_code

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    app.run(debug=True, host='0.0.0.0', port=5002)  # Different port for serving layer
