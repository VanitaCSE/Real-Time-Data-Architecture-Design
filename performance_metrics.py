"""
STEP 7: Performance Metrics Dashboard
Enhanced dashboard with comprehensive metrics and business insights
"""

import time
import json
from datetime import datetime, timedelta
from typing import Dict, Any, List
import logging

logger = logging.getLogger(__name__)

class PerformanceMetricsCollector:
    """
    Collects and analyzes performance metrics for business decisions
    """
    
    def __init__(self, storage_layer, serving_layer, processor, ml_model):
        self.storage_layer = storage_layer
        self.serving_layer = serving_layer
        self.processor = processor
        self.ml_model = ml_model
        
        # Metrics cache
        self.metrics_cache = {}
        self.last_update = None
        
    def collect_comprehensive_metrics(self) -> Dict[str, Any]:
        """Collect all performance metrics for dashboard"""
        try:
            current_time = datetime.now()
            
            # API Performance Metrics
            api_metrics = self._collect_api_metrics()
            
            # System Performance Metrics
            system_metrics = self._collect_system_metrics()
            
            # Business Metrics
            business_metrics = self._collect_business_metrics()
            
            # ML Model Metrics
            ml_metrics = self._collect_ml_metrics()
            
            # Data Quality Metrics
            data_quality_metrics = self._collect_data_quality_metrics()
            
            # Real-time Analytics
            realtime_analytics = self._collect_realtime_analytics()
            
            # Combine all metrics
            comprehensive_metrics = {
                'timestamp': current_time.isoformat(),
                'api_performance': api_metrics,
                'system_performance': system_metrics,
                'business_metrics': business_metrics,
                'ml_model_metrics': ml_metrics,
                'data_quality_metrics': data_quality_metrics,
                'realtime_analytics': realtime_analytics
            }
            
            # Cache metrics
            self.metrics_cache = comprehensive_metrics
            self.last_update = current_time
            
            return comprehensive_metrics
            
        except Exception as e:
            logger.error(f"❌ Metrics collection failed: {e}")
            return self._get_fallback_metrics()
            
    def _collect_api_metrics(self) -> Dict[str, Any]:
        """Collect API performance metrics"""
        api_stats = self.serving_layer.get_api_performance_stats()
        cache_stats = self.serving_layer.cache.get_stats()
        
        return {
            'total_requests': api_stats['total_requests'],
            'requests_per_minute': api_stats['requests_per_minute'],
            'avg_response_time_ms': api_stats['avg_response_time_ms'],
            'p95_response_time_ms': api_stats['p95_response_time_ms'],
            'error_rate_percent': api_stats['error_rate_percent'],
            'cache_hit_rate_percent': cache_stats['hit_rate_percent'],
            'uptime_seconds': api_stats['uptime_seconds'],
            'api_health_score': self._calculate_api_health_score(api_stats, cache_stats)
        }
        
    def _collect_system_metrics(self) -> Dict[str, Any]:
        """Collect system performance metrics"""
        processing_stats = self.processor.get_processing_stats()
        db_stats = self.storage_layer.get_database_stats()
        
        return {
            'events_processed_total': processing_stats['processed_events'],
            'processing_throughput_eps': processing_stats['throughput_events_per_second'],
            'avg_processing_latency_ms': processing_stats['avg_processing_time_ms'],
            'p95_processing_latency_ms': processing_stats['p95_processing_time_ms'],
            'queue_depth': processing_stats['queue_depth'],
            'error_rate_percent': processing_stats['error_rate_percent'],
            'database_size_mb': db_stats['database_size_mb'],
            'storage_efficiency_percent': db_stats['storage_efficiency'],
            'system_health_score': self._calculate_system_health_score(processing_stats, db_stats)
        }
        
    def _collect_business_metrics(self) -> Dict[str, Any]:
        """Collect business-focused metrics"""
        # Get recent pricing data
        recent_data = self.storage_layer.get_latest_pricing_data(limit=100)
        
        if not recent_data:
            return self._get_empty_business_metrics()
            
        # Calculate business metrics
        prices = [float(item['price']) for item in recent_data if item.get('price')]
        demands = [float(item['demand']) for item in recent_data if item.get('demand')]
        
        if not prices or not demands:
            return self._get_empty_business_metrics()
            
        # Revenue calculations
        avg_price = sum(prices) / len(prices)
        avg_demand = sum(demands) / len(demands)
        estimated_revenue_per_event = avg_price * avg_demand
        
        # Price optimization metrics
        price_volatility = (max(prices) - min(prices)) / avg_price * 100
        demand_volatility = (max(demands) - min(demands)) / avg_demand * 100
        
        # Pricing tier distribution
        tier_distribution = {}
        for item in recent_data:
            tier = item.get('pricing_tier', 'Unknown')
            tier_distribution[tier] = tier_distribution.get(tier, 0) + 1
            
        total_events = len(tier_distribution)
        tier_percentages = {
            tier: (count / total_events * 100) 
            for tier, count in tier_distribution.items()
        }
        
        return {
            'avg_price_usd': round(avg_price, 2),
            'avg_demand_units': round(avg_demand, 2),
            'estimated_revenue_per_event': round(estimated_revenue_per_event, 2),
            'price_volatility_percent': round(price_volatility, 2),
            'demand_volatility_percent': round(demand_volatility, 2),
            'pricing_tier_distribution': tier_percentages,
            'total_events_analyzed': len(recent_data),
            'business_efficiency_score': self._calculate_business_efficiency_score(
                price_volatility, demand_volatility, tier_percentages
            )
        }
        
    def _collect_ml_metrics(self) -> Dict[str, Any]:
        """Collect ML model performance metrics"""
        model_info = self.ml_model.get_model_info()
        
        # Get ML performance from storage
        ml_performance = self.storage_layer.get_ml_model_performance()
        
        return {
            'model_type': model_info['model_type'],
            'model_version': model_info['model_version'],
            'is_trained': model_info['isained'],
            'training_samples': model_info['training_samples'],
            'total_predictions': model_info['prediction_count'],
            'model_accuracy_r2': model_info['metrics'].get('r2_score', 0),
            'model_rmse': model_info['metrics'].get('rmse', 0),
            'model_mae': model_info['metrics'].get('mae', 0),
            'avg_prediction_error': ml_performance.get('avg_prediction_error', 0),
            'avg_confidence_score': ml_performance.get('avg_confidence', 0),
            'ml_performance_score': self._calculate_ml_performance_score(model_info, ml_performance)
        }
        
    def _collect_data_quality_metrics(self) -> Dict[str, Any]:
        """Collect data quality metrics"""
        db_stats = self.storage_layer.get_database_stats()
        
        # Get recent data for quality analysis
        recent_data = self.storage_layer.get_latest_pricing_data(limit=1000)
        
        if not recent_data:
            return self._get_empty_data_quality_metrics()
            
        # Data completeness
        total_fields = ['demand', 'price', 'pricing_tier', 'timestamp', 'processed_at']
        completeness_scores = {}
        
        for field in total_fields:
            complete_count = sum(1 for item in recent_data if item.get(field) is not None)
            completeness_scores[field] = (complete_count / len(recent_data)) * 100
            
        overall_completeness = sum(completeness_scores.values()) / len(completeness_scores)
        
        # Data freshness
        latest_timestamp = recent_data[0].get('timestamp')
        if latest_timestamp:
            try:
                latest_time = datetime.fromisoformat(latest_timestamp.replace('Z', '+00:00'))
                data_age_seconds = (datetime.now() - latest_time).total_seconds()
                data_freshness_score = max(0, 100 - (data_age_seconds / 60))  # Decay over minutes
            except:
                data_freshness_score = 50
        else:
            data_freshness_score = 0
            
        # Data consistency
        price_consistency = self._check_price_consistency(recent_data)
        demand_consistency = self._check_demand_consistency(recent_data)
        
        return {
            'total_records': db_stats['total_pricing_events'],
            'data_completeness_percent': round(overall_completeness, 2),
            'field_completeness': completeness_scores,
            'data_freshness_score': round(data_freshness_score, 2),
            'price_consistency_score': price_consistency,
            'demand_consistency_score': demand_consistency,
            'data_quality_score': round((overall_completeness + data_freshness_score + price_consistency + demand_consistency) / 4, 2)
        }
        
    def _collect_realtime_analytics(self) -> Dict[str, Any]:
        """Collect real-time analytics"""
        # Get latest event
        latest_data = self.storage_layer.get_latest_pricing_data(limit=1)
        
        if not latest_data:
            return self._get_empty_realtime_analytics()
            
        latest_event = latest_data[0]
        
        # Calculate real-time trends
        last_hour_data = self.storage_layer.get_demand_analytics(window_minutes=60)
        
        return {
            'current_demand': latest_event.get('demand', 0),
            'current_price_usd': latest_event.get('price', 0),
            'current_pricing_tier': latest_event.get('pricing_tier', 'Unknown'),
            'current_trend': latest_event.get('trend', 'unknown'),
            'processing_latency_ms': latest_event.get('processing_latency_ms', 0),
            'last_event_timestamp': latest_event.get('timestamp'),
            'events_last_hour': last_hour_data.get('total_events', 0),
            'avg_demand_last_hour': last_hour_data.get('avg_demand', 0),
            'avg_price_last_hour': last_hour_data.get('avg_price', 0),
            'price_range_last_hour': {
                'min': last_hour_data.get('min_price', 0),
                'max': last_hour_data.get('max_price', 0)
            },
            'demand_range_last_hour': {
                'min': last_hour_data.get('min_demand', 0),
                'max': last_hour_data.get('max_demand', 0)
            }
        }
        
    def _calculate_api_health_score(self, api_stats: Dict, cache_stats: Dict) -> float:
        """Calculate API health score (0-100)"""
        score = 100
        
        # Response time impact (30%)
        avg_response_time = api_stats.get('avg_response_time_ms', 0)
        if avg_response_time > 1000:
            score -= 30
        elif avg_response_time > 500:
            score -= 20
        elif avg_response_time > 200:
            score -= 10
            
        # Error rate impact (30%)
        error_rate = api_stats.get('error_rate_percent', 0)
        if error_rate > 10:
            score -= 30
        elif error_rate > 5:
            score -= 20
        elif error_rate > 1:
            score -= 10
            
        # Cache hit rate impact (20%)
        cache_hit_rate = cache_stats.get('hit_rate_percent', 0)
        if cache_hit_rate < 50:
            score -= 20
        elif cache_hit_rate < 70:
            score -= 10
            
        # Request rate impact (20%)
        requests_per_minute = api_stats.get('requests_per_minute', 0)
        if requests_per_minute < 1:
            score -= 20
            
        return max(0, score)
        
    def _calculate_system_health_score(self, processing_stats: Dict, db_stats: Dict) -> float:
        """Calculate system health score (0-100)"""
        score = 100
        
        # Processing throughput impact (40%)
        throughput = processing_stats.get('throughput_events_per_second', 0)
        if throughput < 0.5:
            score -= 40
        elif throughput < 0.8:
            score -= 20
            
        # Error rate impact (30%)
        error_rate = processing_stats.get('error_rate_percent', 0)
        if error_rate > 5:
            score -= 30
        elif error_rate > 2:
            score -= 15
            
        # Queue depth impact (20%)
        queue_depth = processing_stats.get('queue_depth', 0)
        if queue_depth > 800:
            score -= 20
        elif queue_depth > 500:
            score -= 10
            
        # Database efficiency impact (10%)
        storage_efficiency = db_stats.get('storage_efficiency', 0)
        if storage_efficiency < 80:
            score -= 10
            
        return max(0, score)
        
    def _calculate_business_efficiency_score(self, price_volatility: float, 
                                         demand_volatility: float, 
                                         tier_distribution: Dict) -> float:
        """Calculate business efficiency score (0-100)"""
        score = 100
        
        # Price volatility impact (40%)
        if price_volatility > 30:
            score -= 40
        elif price_volatility > 20:
            score -= 20
        elif price_volatility > 10:
            score -= 10
            
        # Demand volatility impact (30%)
        if demand_volatility > 40:
            score -= 30
        elif demand_volatility > 25:
            score -= 15
            
        # Tier distribution impact (30%)
        # Ideal distribution: Balanced across tiers
        medium_tier_percentage = tier_distribution.get('Medium', 0)
        if medium_tier_percentage < 30 or medium_tier_percentage > 70:
            score -= 30
        elif medium_tier_percentage < 40 or medium_tier_percentage > 60:
            score -= 15
            
        return max(0, score)
        
    def _calculate_ml_performance_score(self, model_info: Dict, ml_performance: Dict) -> float:
        """Calculate ML performance score (0-100)"""
        score = 100
        
        # Model accuracy impact (50%)
        r2_score = model_info['metrics'].get('r2_score', 0)
        if r2_score < 0.5:
            score -= 50
        elif r2_score < 0.7:
            score -= 30
        elif r2_score < 0.9:
            score -= 15
            
        # Prediction error impact (30%)
        avg_error = ml_performance.get('avg_prediction_error', 100)
        if avg_error > 20:
            score -= 30
        elif avg_error > 10:
            score -= 15
        elif avg_error > 5:
            score -= 5
            
        # Confidence score impact (20%)
        confidence = ml_performance.get('avg_confidence', 0)
        if confidence < 0.5:
            score -= 20
        elif confidence < 0.7:
            score -= 10
        elif confidence < 0.9:
            score -= 5
            
        return max(0, score)
        
    def _check_price_consistency(self, data: List[Dict]) -> float:
        """Check price consistency score"""
        if len(data) < 10:
            return 50
            
        prices = [float(item['price']) for item in data if item.get('price')]
        if len(prices) < 10:
            return 50
            
        # Check for outliers
        avg_price = sum(prices) / len(prices)
        outliers = sum(1 for price in prices if abs(price - avg_price) > avg_price * 0.5)
        
        consistency_score = max(0, 100 - (outliers / len(prices) * 100))
        return round(consistency_score, 2)
        
    def _check_demand_consistency(self, data: List[Dict]) -> float:
        """Check demand consistency score"""
        if len(data) < 10:
            return 50
            
        demands = [float(item['demand']) for item in data if item.get('demand')]
        if len(demands) < 10:
            return 50
            
        # Check for outliers
        avg_demand = sum(demands) / len(demands)
        outliers = sum(1 for demand in demands if abs(demand - avg_demand) > avg_demand * 0.5)
        
        consistency_score = max(0, 100 - (outliers / len(demands) * 100))
        return round(consistency_score, 2)
        
    def _get_fallback_metrics(self) -> Dict[str, Any]:
        """Get fallback metrics when collection fails"""
        return {
            'timestamp': datetime.now().isoformat(),
            'api_performance': {'status': 'error'},
            'system_performance': {'status': 'error'},
            'business_metrics': {'status': 'error'},
            'ml_model_metrics': {'status': 'error'},
            'data_quality_metrics': {'status': 'error'},
            'realtime_analytics': {'status': 'error'}
        }
        
    def _get_empty_business_metrics(self) -> Dict[str, Any]:
        """Get empty business metrics when no data available"""
        return {
            'avg_price_usd': 0,
            'avg_demand_units': 0,
            'estimated_revenue_per_event': 0,
            'price_volatility_percent': 0,
            'demand_volatility_percent': 0,
            'pricing_tier_distribution': {},
            'total_events_analyzed': 0,
            'business_efficiency_score': 0
        }
        
    def _get_empty_data_quality_metrics(self) -> Dict[str, Any]:
        """Get empty data quality metrics when no data available"""
        return {
            'total_records': 0,
            'data_completeness_percent': 0,
            'field_completeness': {},
            'data_freshness_score': 0,
            'price_consistency_score': 0,
            'demand_consistency_score': 0,
            'data_quality_score': 0
        }
        
    def _get_empty_realtime_analytics(self) -> Dict[str, Any]:
        """Get empty realtime analytics when no data available"""
        return {
            'current_demand': 0,
            'current_price_usd': 0,
            'current_pricing_tier': 'Unknown',
            'current_trend': 'unknown',
            'processing_latency_ms': 0,
            'last_event_timestamp': None,
            'events_last_hour': 0,
            'avg_demand_last_hour': 0,
            'avg_price_last_hour': 0,
            'price_range_last_hour': {'min': 0, 'max': 0},
            'demand_range_last_hour': {'min': 0, 'max': 0}
        }

# Business Insights Generator
class BusinessInsightsGenerator:
    """
    Generates business insights from metrics
    """
    
    def __init__(self):
        self.insights_cache = {}
        
    def generate_insights(self, metrics: Dict[str, Any]) -> Dict[str, Any]:
        """Generate business insights from metrics"""
        insights = {
            'timestamp': datetime.now().isoformat(),
            'recommendations': [],
            'alerts': [],
            'opportunities': [],
            'performance_summary': {}
        }
        
        # API Performance Insights
        self._analyze_api_performance(metrics.get('api_performance', {}), insights)
        
        # Business Metrics Insights
        self._analyze_business_metrics(metrics.get('business_metrics', {}), insights)
        
        # ML Model Insights
        self._analyze_ml_metrics(metrics.get('ml_model_metrics', {}), insights)
        
        # Data Quality Insights
        self._analyze_data_quality(metrics.get('data_quality_metrics', {}), insights)
        
        # Overall Performance Summary
        insights['performance_summary'] = self._create_performance_summary(metrics)
        
        return insights
        
    def _analyze_api_performance(self, api_metrics: Dict, insights: Dict):
        """Analyze API performance and generate insights"""
        response_time = api_metrics.get('avg_response_time_ms', 0)
        error_rate = api_metrics.get('error_rate_percent', 0)
        cache_hit_rate = api_metrics.get('cache_hit_rate_percent', 0)
        
        # Alerts
        if response_time > 500:
            insights['alerts'].append({
                'type': 'performance',
                'severity': 'high',
                'message': f'API response time is {response_time}ms (threshold: 500ms)',
                'recommendation': 'Consider scaling up resources or optimizing queries'
            })
            
        if error_rate > 5:
            insights['alerts'].append({
                'type': 'reliability',
                'severity': 'high',
                'message': f'API error rate is {error_rate}% (threshold: 5%)',
                'recommendation': 'Investigate error causes and implement better error handling'
            })
            
        # Opportunities
        if cache_hit_rate < 70:
            insights['opportunities'].append({
                'type': 'optimization',
                'message': f'Cache hit rate is {cache_hit_rate}% - improving this could reduce response times',
                'potential_impact': '20-30% response time reduction'
            })
            
    def _analyze_business_metrics(self, business_metrics: Dict, insights: Dict):
        """Analyze business metrics and generate insights"""
        price_volatility = business_metrics.get('price_volatility_percent', 0)
        demand_volatility = business_metrics.get('demand_volatility_percent', 0)
        revenue_per_event = business_metrics.get('estimated_revenue_per_event', 0)
        
        # Alerts
        if price_volatility > 25:
            insights['alerts'].append({
                'type': 'business',
                'severity': 'medium',
                'message': f'Price volatility is high at {price_volatility}%',
                'recommendation': 'Consider implementing price stabilization mechanisms'
            })
            
        # Opportunities
        if demand_volatility > 30:
            insights['opportunities'].append({
                'type': 'business',
                'message': 'High demand volatility indicates opportunity for dynamic pricing',
                'potential_impact': '5-15% revenue increase'
            })
            
        # Recommendations
        if revenue_per_event < 5000:
            insights['recommendations'].append({
                'type': 'business',
                'message': 'Consider strategies to increase revenue per event',
                'suggestions': ['Bundle pricing', 'Premium tiers', 'Value-added services']
            })
            
    def _analyze_ml_metrics(self, ml_metrics: Dict, insights: Dict):
        """Analyze ML model metrics and generate insights"""
        accuracy = ml_metrics.get('model_accuracy_r2', 0)
        prediction_error = ml_metrics.get('avg_prediction_error', 100)
        
        # Alerts
        if accuracy < 0.7:
            insights['alerts'].append({
                'type': 'ml',
                'severity': 'medium',
                'message': f'ML model accuracy is {accuracy:.3f} (threshold: 0.7)',
                'recommendation': 'Retrain model with more data or try different algorithms'
            })
            
        # Opportunities
        if prediction_error > 10:
            insights['opportunities'].append({
                'type': 'ml',
                'message': 'Reducing prediction error could improve pricing accuracy',
                'potential_impact': '2-5% revenue improvement'
            })
            
    def _analyze_data_quality(self, data_quality: Dict, insights: Dict):
        """Analyze data quality and generate insights"""
        completeness = data_quality.get('data_completeness_percent', 0)
        freshness = data_quality.get('data_freshness_score', 0)
        
        # Alerts
        if completeness < 90:
            insights['alerts'].append({
                'type': 'data',
                'severity': 'medium',
                'message': f'Data completeness is {completeness}% (threshold: 90%)',
                'recommendation': 'Investigate missing data sources and improve data collection'
            })
            
        if freshness < 70:
            insights['alerts'].append({
                'type': 'data',
                'severity': 'high',
                'message': f'Data freshness score is {freshness}% - data may be stale',
                'recommendation': 'Check data pipeline for delays'
            })
            
    def _create_performance_summary(self, metrics: Dict) -> Dict[str, Any]:
        """Create overall performance summary"""
        api_health = metrics.get('api_performance', {}).get('api_health_score', 0)
        system_health = metrics.get('system_performance', {}).get('system_health_score', 0)
        business_efficiency = metrics.get('business_metrics', {}).get('business_efficiency_score', 0)
        ml_performance = metrics.get('ml_model_metrics', {}).get('ml_performance_score', 0)
        
        overall_score = (api_health + system_health + business_efficiency + ml_performance) / 4
        
        if overall_score >= 85:
            grade = 'A'
            status = 'Excellent'
        elif overall_score >= 70:
            grade = 'B'
            status = 'Good'
        elif overall_score >= 55:
            grade = 'C'
            status = 'Fair'
        else:
            grade = 'D'
            status = 'Needs Improvement'
            
        return {
            'overall_score': round(overall_score, 2),
            'grade': grade,
            'status': status,
            'component_scores': {
                'api_health': api_health,
                'system_health': system_health,
                'business_efficiency': business_efficiency,
                'ml_performance': ml_performance
            }
        }

# Initialize metrics collector
metrics_collector = None
insights_generator = BusinessInsightsGenerator()

def initialize_metrics_collector(storage_layer, serving_layer, processor, ml_model):
    """Initialize the metrics collector"""
    global metrics_collector
    metrics_collector = PerformanceMetricsCollector(
        storage_layer, serving_layer, processor, ml_model
    )
    logger.info("📊 Performance metrics collector initialized")
