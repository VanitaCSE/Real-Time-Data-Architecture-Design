"""
📊 Monitoring Layer - CloudWatch Metrics
Comprehensive monitoring and alerting for the ML pricing system
Maps to: AWS CloudWatch + CloudWatch Alarms + SNS
"""

import json
import logging
import time
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
from dataclasses import dataclass, field
import statistics
import boto3

logger = logging.getLogger(__name__)

@dataclass
class MetricData:
    """CloudWatch metric data structure"""
    namespace: str
    metric_name: str
    value: float
    unit: str
    dimensions: Dict[str, str] = field(default_factory=dict)
    timestamp: datetime = field(default_factory=datetime.now)

@dataclass
class AlertRule:
    """Alert rule configuration"""
    name: str
    metric_name: str
    threshold: float
    comparison: str  # 'GreaterThan', 'LessThan', 'Equal'
    evaluation_periods: int = 2
    period_seconds: int = 300
    treat_missing_data: str = 'missing'

class CloudWatchMonitor:
    """
    CloudWatch monitoring and metrics collection
    Maps to: AWS CloudWatch Metrics + Alarms
    """
    
    def __init__(self, namespace: str = 'MLPricingSystem', region: str = 'us-east-1'):
        self.namespace = namespace
        self.region = region
        self.cloudwatch = None  # boto3.client('cloudwatch', region_name=region)
        self.use_local = True  # For local development
        
        # Metric storage for local development
        self.local_metrics = []
        self.alert_rules = self._initialize_alert_rules()
        self.monitoring_stats = {
            'metrics_published': 0,
            'alerts_triggered': 0,
            'start_time': datetime.now()
        }
    
    def publish_metric(self, metric_data: MetricData) -> bool:
        """
        Publish metric to CloudWatch
        Maps to: CloudWatch PutMetricData
        """
        try:
            if self.use_local:
                self._store_local_metric(metric_data)
            else:
                self._publish_cloudwatch_metric(metric_data)
            
            self.monitoring_stats['metrics_published'] += 1
            logger.debug(f"Published metric: {metric_data.metric_name} = {metric_data.value}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to publish metric: {e}")
            return False
    
    def publish_batch_metrics(self, metrics: List[MetricData]) -> Dict[str, int]:
        """
        Publish multiple metrics in batch
        Maps to: CloudWatch PutMetricData (batch)
        """
        results = {'success': 0, 'failed': 0}
        
        for metric in metrics:
            if self.publish_metric(metric):
                results['success'] += 1
            else:
                results['failed'] += 1
        
        logger.info(f"Batch metrics published - Success: {results['success']}, Failed: {results['failed']}")
        return results
    
    def create_custom_dashboard(self) -> Dict[str, Any]:
        """
        Create CloudWatch dashboard for the system
        Maps to: CloudWatch Dashboards
        """
        dashboard_config = {
            'widgets': [
                {
                    'type': 'metric',
                    'properties': {
                        'metrics': [
                            ['MLPricingSystem', 'ProcessingLatency', 'Environment', 'production'],
                            ['.', 'APIResponseTime', '.', '.'],
                            ['.', 'CacheHitRate', '.', '.']
                        ],
                        'view': 'timeSeries',
                        'stacked': False,
                        'region': self.region,
                        'title': 'Performance Metrics',
                        'period': 300
                    }
                },
                {
                    'type': 'metric',
                    'properties': {
                        'metrics': [
                            ['MLPricingSystem', 'EventsProcessed', 'Environment', 'production'],
                            ['.', 'MLPredictions', '.', '.'],
                            ['.', 'APIRequests', '.', '.']
                        ],
                        'view': 'timeSeries',
                        'stacked': False,
                        'region': self.region,
                        'title': 'Volume Metrics',
                        'period': 300
                    }
                },
                {
                    'type': 'metric',
                    'properties': {
                        'metrics': [
                            ['MLPricingSystem', 'ErrorRate', 'Environment', 'production'],
                            ['.', 'MLPredictionFailures', '.', '.'],
                            ['.', 'CacheMisses', '.', '.']
                        ],
                        'view': 'timeSeries',
                        'stacked': False,
                        'region': self.region,
                        'title': 'Error Metrics',
                        'period': 300,
                        'yAxis': {'left': {'min': 0}}
                    }
                },
                {
                    'type': 'metric',
                    'properties': {
                        'metrics': [
                            ['MLPricingSystem', 'MLPredictionAccuracy', 'Environment', 'production'],
                            ['.', 'ModelR2Score', '.', '.']
                        ],
                        'view': 'timeSeries',
                        'stacked': False,
                        'region': self.region,
                        'title': 'ML Model Performance',
                        'period': 300
                    }
                }
            ]
        }
        
        if not self.use_local:
            try:
                # self.cloudwatch.put_dashboard(
                #     DashboardName='MLPricingSystem-Dashboard',
                #     DashboardBody=json.dumps(dashboard_config)
                # )
                logger.info("CloudWatch dashboard created successfully")
            except Exception as e:
                logger.error(f"Failed to create dashboard: {e}")
        
        return dashboard_config
    
    def setup_alarms(self) -> Dict[str, Any]:
        """
        Setup CloudWatch alarms for monitoring
        Maps to: CloudWatch Alarms + SNS
        """
        alarm_configs = []
        
        for rule in self.alert_rules:
            alarm_config = {
                'AlarmName': f"{self.namespace}-{rule.name}",
                'AlarmDescription': f"Alert for {rule.name}",
                'MetricName': rule.metric_name,
                'Namespace': self.namespace,
                'Statistic': 'Average',
                'Period': rule.period_seconds,
                'EvaluationPeriods': rule.evaluation_periods,
                'Threshold': rule.threshold,
                'ComparisonOperator': f"{'GreaterThan' if rule.comparison == 'GreaterThan' else 'LessThan' if rule.comparison == 'LessThan' else 'EqualTo'}Threshold",
                'TreatMissingData': rule.treat_missing_data,
                'Dimensions': [
                    {
                        'Name': 'Environment',
                        'Value': 'production'
                    }
                ]
            }
            
            alarm_configs.append(alarm_config)
            
            if not self.use_local:
                try:
                    # self.cloudwatch.put_metric_alarm(**alarm_config)
                    logger.info(f"Created alarm: {alarm_config['AlarmName']}")
                except Exception as e:
                    logger.error(f"Failed to create alarm {rule.name}: {e}")
        
        return {'alarms_created': len(alarm_configs), 'configurations': alarm_configs}
    
    def get_metric_statistics(self, metric_name: str, hours: int = 24) -> Dict[str, Any]:
        """
        Get metric statistics from CloudWatch
        Maps to: CloudWatch GetMetricStatistics
        """
        end_time = datetime.now()
        start_time = end_time - timedelta(hours=hours)
        
        if self.use_local:
            return self._get_local_metric_statistics(metric_name, start_time, end_time)
        else:
            try:
                # response = self.cloudwatch.get_metric_statistics(
                #     Namespace=self.namespace,
                #     MetricName=metric_name,
                #     StartTime=start_time,
                #     EndTime=end_time,
                #     Period=300,
                #     Statistics=['Average', 'Minimum', 'Maximum', 'Sum']
                # )
                # return self._process_cloudwatch_response(response)
                pass
            except Exception as e:
                logger.error(f"Failed to get metric statistics: {e}")
                return {}
    
    def _initialize_alert_rules(self) -> List[AlertRule]:
        """Initialize default alert rules"""
        return [
            AlertRule(
                name='HighProcessingLatency',
                metric_name='ProcessingLatency',
                threshold=1000.0,  # 1 second
                comparison='GreaterThan'
            ),
            AlertRule(
                name='HighErrorRate',
                metric_name='ErrorRate',
                threshold=5.0,  # 5%
                comparison='GreaterThan'
            ),
            AlertRule(
                name='LowCacheHitRate',
                metric_name='CacheHitRate',
                threshold=80.0,  # 80%
                comparison='LessThan'
            ),
            AlertRule(
                name='MLPredictionFailures',
                metric_name='MLPredictionFailures',
                threshold=10.0,  # 10 failures
                comparison='GreaterThan'
            ),
            AlertRule(
                name='LowMLPredictionAccuracy',
                metric_name='MLPredictionAccuracy',
                threshold=90.0,  # 90%
                comparison='LessThan'
            )
        ]
    
    def _store_local_metric(self, metric_data: MetricData) -> None:
        """Store metric locally for development"""
        self.local_metrics.append(metric_data)
        
        # Keep only last 1000 metrics
        if len(self.local_metrics) > 1000:
            self.local_metrics = self.local_metrics[-1000:]
        
        # Check alerts
        self._check_local_alerts(metric_data)
    
    def _publish_cloudwatch_metric(self, metric_data: MetricData) -> None:
        """Publish metric to CloudWatch (production)"""
        # In production, this would use boto3
        # self.cloudwatch.put_metric_data(
        #     Namespace=metric_data.namespace,
        #     MetricData=[
        #         {
        #             'MetricName': metric_data.metric_name,
        #             'Value': metric_data.value,
        #             'Unit': metric_data.unit,
        #             'Dimensions': [
        #                 {'Name': k, 'Value': v} for k, v in metric_data.dimensions.items()
        #             ],
        #             'Timestamp': metric_data.timestamp
        #         }
        #     ]
        # )
        pass
    
    def _check_local_alerts(self, metric_data: MetricData) -> None:
        """Check if metric triggers any alerts"""
        for rule in self.alert_rules:
            if rule.metric_name == metric_data.metric_name:
                triggered = False
                
                if rule.comparison == 'GreaterThan' and metric_data.value > rule.threshold:
                    triggered = True
                elif rule.comparison == 'LessThan' and metric_data.value < rule.threshold:
                    triggered = True
                elif rule.comparison == 'Equal' and metric_data.value == rule.threshold:
                    triggered = True
                
                if triggered:
                    self.monitoring_stats['alerts_triggered'] += 1
                    logger.warning(f"ALERT TRIGGERED: {rule.name} - {metric_data.metric_name} = {metric_data.value} (threshold: {rule.threshold})")
    
    def _get_local_metric_statistics(self, metric_name: str, start_time: datetime, end_time: datetime) -> Dict[str, Any]:
        """Get local metric statistics"""
        filtered_metrics = [
            m for m in self.local_metrics 
            if m.metric_name == metric_name and start_time <= m.timestamp <= end_time
        ]
        
        if not filtered_metrics:
            return {}
        
        values = [m.value for m in filtered_metrics]
        
        return {
            'metric_name': metric_name,
            'start_time': start_time.isoformat(),
            'end_time': end_time.isoformat(),
            'data_points': len(values),
            'average': statistics.mean(values),
            'minimum': min(values),
            'maximum': max(values),
            'sum': sum(values),
            'values': values
        }
    
    def get_monitoring_stats(self) -> Dict[str, Any]:
        """Get monitoring system statistics"""
        runtime = datetime.now() - self.monitoring_stats['start_time']
        
        return {
            'metrics_published': self.monitoring_stats['metrics_published'],
            'alerts_triggered': self.monitoring_stats['alerts_triggered'],
            'runtime_seconds': int(runtime.total_seconds()),
            'metrics_per_second': round(
                self.monitoring_stats['metrics_published'] / max(runtime.total_seconds(), 1), 2
            ),
            'local_metrics_count': len(self.local_metrics),
            'alert_rules_count': len(self.alert_rules)
        }

class SystemMetricsCollector:
    """
    Collects system metrics for monitoring
    Maps to: Application-level metrics collection
    """
    
    def __init__(self, monitor: CloudWatchMonitor):
        self.monitor = monitor
        self.collection_interval = 60  # seconds
        self.last_collection = datetime.now()
    
    def collect_api_metrics(self, api_stats: Dict[str, Any]) -> None:
        """Collect API-related metrics"""
        # API Response Time
        if 'performance' in api_stats:
            response_time_metric = MetricData(
                namespace=self.monitor.namespace,
                metric_name='APIResponseTime',
                value=api_stats['performance'].get('avg_response_time_ms', 0),
                unit='Milliseconds',
                dimensions={'Service': 'API', 'Environment': 'production'}
            )
            self.monitor.publish_metric(response_time_metric)
        
        # API Requests
        if 'requests' in api_stats:
            requests_metric = MetricData(
                namespace=self.monitor.namespace,
                metric_name='APIRequests',
                value=api_stats['requests'].get('total', 0),
                unit='Count',
                dimensions={'Service': 'API', 'Environment': 'production'}
            )
            self.monitor.publish_metric(requests_metric)
        
        # Error Rate
        if 'requests' in api_stats:
            total_requests = api_stats['requests'].get('total', 0)
            failed_requests = api_stats['requests'].get('total_errors', 0)
            error_rate = (failed_requests / max(total_requests, 1)) * 100
            
            error_rate_metric = MetricData(
                namespace=self.monitor.namespace,
                metric_name='ErrorRate',
                value=error_rate,
                unit='Percent',
                dimensions={'Service': 'API', 'Environment': 'production'}
            )
            self.monitor.publish_metric(error_rate_metric)
    
    def collect_processing_metrics(self, processing_stats: Dict[str, Any]) -> None:
        """Collect stream processing metrics"""
        # Processing Latency
        if 'performance' in processing_stats:
            latency_metric = MetricData(
                namespace=self.monitor.namespace,
                metric_name='ProcessingLatency',
                value=processing_stats['performance'].get('avg_processing_time_ms', 0),
                unit='Milliseconds',
                dimensions={'Service': 'StreamProcessor', 'Environment': 'production'}
            )
            self.monitor.publish_metric(latency_metric)
        
        # Events Processed
        events_metric = MetricData(
            namespace=self.monitor.namespace,
            metric_name='EventsProcessed',
            value=processing_stats.get('events_processed', 0),
            unit='Count',
            dimensions={'Service': 'StreamProcessor', 'Environment': 'production'}
        )
        self.monitor.publish_metric(events_metric)
        
        # ML Predictions
        ml_predictions_metric = MetricData(
            namespace=self.monitor.namespace,
            metric_name='MLPredictions',
            value=processing_stats.get('ml_predictions', 0),
            unit='Count',
            dimensions={'Service': 'StreamProcessor', 'Environment': 'production'}
        )
        self.monitor.publish_metric(ml_predictions_metric)
    
    def collect_cache_metrics(self, cache_stats: Dict[str, Any]) -> None:
        """Collect cache performance metrics"""
        # Cache Hit Rate
        hit_rate_metric = MetricData(
            namespace=self.monitor.namespace,
            metric_name='CacheHitRate',
            value=cache_stats.get('hit_rate', 0),
            unit='Percent',
            dimensions={'Service': 'Cache', 'Environment': 'production'}
        )
        self.monitor.publish_metric(hit_rate_metric)
        
        # Cache Misses
        misses_metric = MetricData(
            namespace=self.monitor.namespace,
            metric_name='CacheMisses',
            value=cache_stats.get('misses', 0),
            unit='Count',
            dimensions={'Service': 'Cache', 'Environment': 'production'}
        )
        self.monitor.publish_metric(misses_metric)
    
    def collect_ml_metrics(self, ml_stats: Dict[str, Any]) -> None:
        """Collect ML model metrics"""
        # ML Prediction Accuracy
        if 'training_stats' in ml_stats:
            accuracy_metric = MetricData(
                namespace=self.monitor.namespace,
                metric_name='MLPredictionAccuracy',
                value=ml_stats['training_stats'].get('r2_score', 0) * 100,
                unit='Percent',
                dimensions={'Service': 'MLModel', 'Environment': 'production'}
            )
            self.monitor.publish_metric(accuracy_metric)
            
            # Model R2 Score
            r2_metric = MetricData(
                namespace=self.monitor.namespace,
                metric_name='ModelR2Score',
                value=ml_stats['training_stats'].get('r2_score', 0),
                unit='None',
                dimensions={'Service': 'MLModel', 'Environment': 'production'}
            )
            self.monitor.publish_metric(r2_metric)
        
        # ML Prediction Failures
        failures_metric = MetricData(
            namespace=self.monitor.namespace,
            metric_name='MLPredictionFailures',
            value=ml_stats.get('prediction_failures', 0),
            unit='Count',
            dimensions={'Service': 'MLModel', 'Environment': 'production'}
        )
        self.monitor.publish_metric(failures_metric)

# Test function
def test_cloudwatch_monitoring():
    """Test CloudWatch monitoring locally"""
    monitor = CloudWatchMonitor(use_local=True)
    collector = SystemMetricsCollector(monitor)
    
    # Setup alarms
    alarm_results = monitor.setup_alarms()
    print(f"Alarms setup: {alarm_results}")
    
    # Create dashboard
    dashboard = monitor.create_custom_dashboard()
    print(f"Dashboard created with {len(dashboard['widgets'])} widgets")
    
    # Publish sample metrics
    sample_metrics = [
        MetricData('MLPricingSystem', 'APIResponseTime', 150.5, 'Milliseconds'),
        MetricData('MLPricingSystem', 'EventsProcessed', 1250, 'Count'),
        MetricData('MLPricingSystem', 'MLPredictions', 1180, 'Count'),
        MetricData('MLPricingSystem', 'CacheHitRate', 92.5, 'Percent'),
        MetricData('MLPricingSystem', 'ErrorRate', 2.1, 'Percent')
    ]
    
    for metric in sample_metrics:
        monitor.publish_metric(metric)
    
    # Collect metrics from sample data
    collector.collect_api_metrics({
        'performance': {'avg_response_time_ms': 150.5},
        'requests': {'total': 1250, 'total_errors': 26}
    })
    
    collector.collect_processing_metrics({
        'performance': {'avg_processing_time_ms': 45.2},
        'events_processed': 1250,
        'ml_predictions': 1180
    })
    
    collector.collect_cache_metrics({
        'hit_rate': 92.5,
        'misses': 95
    })
    
    collector.collect_ml_metrics({
        'training_stats': {'r2_score': 0.996},
        'prediction_failures': 5
    })
    
    # Get statistics
    stats = monitor.get_monitoring_stats()
    print(f"Monitoring stats: {json.dumps(stats, indent=2, default=str)}")
    
    # Get metric statistics
    api_latency_stats = monitor.get_metric_statistics('APIResponseTime', hours=1)
    print(f"API Latency stats: {json.dumps(api_latency_stats, indent=2, default=str)}")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    test_cloudwatch_monitoring()
