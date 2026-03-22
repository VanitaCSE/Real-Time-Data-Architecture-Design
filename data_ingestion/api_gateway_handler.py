"""
🚪 Data Ingestion Layer - AWS API Gateway Handler
RESTful API endpoints for pricing system
Maps to: AWS API Gateway + Lambda
"""

import json
import logging
from datetime import datetime
from typing import Dict, Any, Optional
from dataclasses import dataclass
from .kinesis_producer import create_producer

logger = logging.getLogger(__name__)

@dataclass
class APIRequest:
    """API request data structure"""
    demand: int
    source: str = 'api_gateway'
    timestamp: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None

@dataclass
class APIResponse:
    """API response data structure"""
    status: str
    message: str
    data: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    timestamp: str = None
    
    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now().isoformat()

class APIGatewayHandler:
    """
    AWS API Gateway handler for pricing events
    Validates, transforms, and forwards requests to Kinesis
    """
    
    def __init__(self, use_local: bool = True):
        self.kinesis_producer = create_producer(use_local=use_local)
        self.request_validator = RequestValidator()
        self.request_stats = {
            'total_requests': 0,
            'valid_requests': 0,
            'invalid_requests': 0,
            'start_time': datetime.now()
        }
    
    def handle_pricing_request(self, request_data: Dict[str, Any]) -> APIResponse:
        """
        Handle pricing request from API Gateway
        Maps to: API Gateway → Lambda → Kinesis
        """
        self.request_stats['total_requests'] += 1
        
        try:
            # Validate request
            validation_result = self.request_validator.validate_pricing_request(request_data)
            
            if not validation_result['is_valid']:
                self.request_stats['invalid_requests'] += 1
                return APIResponse(
                    status='error',
                    message='Validation failed',
                    error=validation_result['error']
                )
            
            # Create API request object
            api_request = APIRequest(
                demand=request_data['demand'],
                source=request_data.get('source', 'api_gateway'),
                timestamp=request_data.get('timestamp'),
                metadata=request_data.get('metadata')
            )
            
            # Transform to event format
            event_data = {
                'event_id': self._generate_event_id(),
                'demand': api_request.demand,
                'timestamp': api_request.timestamp or datetime.now().isoformat(),
                'source': api_request.source,
                'metadata': api_request.metadata or {},
                'api_gateway_request_id': request_data.get('request_id'),
                'client_ip': request_data.get('client_ip'),
                'user_agent': request_data.get('user_agent')
            }
            
            # Send to Kinesis
            kinesis_success = self.kinesis_producer.send_pricing_event(event_data)
            
            if kinesis_success:
                self.request_stats['valid_requests'] += 1
                return APIResponse(
                    status='success',
                    message='Pricing request accepted for processing',
                    data={
                        'event_id': event_data['event_id'],
                        'demand': event_data['demand'],
                        'status': 'queued_for_processing'
                    }
                )
            else:
                self.request_stats['invalid_requests'] += 1
                return APIResponse(
                    status='error',
                    message='Failed to queue pricing request',
                    error='Kinesis ingestion failed'
                )
                
        except Exception as e:
            self.request_stats['invalid_requests'] += 1
            logger.error(f"Error handling pricing request: {e}")
            return APIResponse(
                status='error',
                message='Internal server error',
                error=str(e)
            )
    
    def handle_batch_pricing_requests(self, requests_data: list) -> APIResponse:
        """
        Handle batch pricing requests
        Maps to: API Gateway Batch Request → Lambda → Kinesis
        """
        try:
            # Validate all requests
            valid_events = []
            invalid_count = 0
            
            for i, request_data in enumerate(requests_data):
                validation_result = self.request_validator.validate_pricing_request(request_data)
                
                if validation_result['is_valid']:
                    event_data = {
                        'event_id': self._generate_event_id(),
                        'demand': request_data['demand'],
                        'timestamp': request_data.get('timestamp', datetime.now().isoformat()),
                        'source': request_data.get('source', 'api_gateway_batch'),
                        'metadata': request_data.get('metadata', {}),
                        'batch_index': i
                    }
                    valid_events.append(event_data)
                else:
                    invalid_count += 1
            
            # Send batch to Kinesis
            if valid_events:
                batch_results = self.kinesis_producer.send_batch_events(valid_events)
                self.request_stats['valid_requests'] += batch_results['success']
                self.request_stats['invalid_requests'] += batch_results['failed'] + invalid_count
            else:
                self.request_stats['invalid_requests'] += invalid_count
            
            return APIResponse(
                status='success',
                message=f'Batch processed - Valid: {len(valid_events)}, Invalid: {invalid_count}',
                data={
                    'valid_events': len(valid_events),
                    'invalid_events': invalid_count,
                    'batch_results': batch_results if valid_events else None
                }
            )
            
        except Exception as e:
            logger.error(f"Error handling batch request: {e}")
            return APIResponse(
                status='error',
                message='Batch processing failed',
                error=str(e)
            )
    
    def get_api_stats(self) -> Dict[str, Any]:
        """Get API handler statistics"""
        runtime = datetime.now() - self.request_stats['start_time']
        
        return {
            'total_requests': self.request_stats['total_requests'],
            'valid_requests': self.request_stats['valid_requests'],
            'invalid_requests': self.request_stats['invalid_requests'],
            'validation_rate': (
                self.request_stats['valid_requests'] / 
                max(self.request_stats['total_requests'], 1)
            ) * 100,
            'runtime_seconds': int(runtime.total_seconds()),
            'requests_per_second': round(
                self.request_stats['total_requests'] / max(runtime.total_seconds(), 1), 2
            ),
            'kinesis_stats': self.kinesis_producer.get_producer_stats()
        }
    
    def _generate_event_id(self) -> int:
        """Generate unique event ID"""
        return int(datetime.now().timestamp() * 1000)

class RequestValidator:
    """
    Request validation for API Gateway
    Ensures data quality and security
    """
    
    def __init__(self):
        self.validation_rules = {
            'demand': {'min': 0, 'max': 100, 'required': True},
            'source': {'max_length': 50, 'required': False},
            'timestamp': {'format': 'iso_datetime', 'required': False}
        }
    
    def validate_pricing_request(self, request_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Validate pricing request data
        Maps to: API Gateway Request Validation
        """
        errors = []
        
        # Check required fields
        if 'demand' not in request_data:
            errors.append('Missing required field: demand')
        else:
            demand = request_data['demand']
            if not isinstance(demand, (int, float)):
                errors.append('Demand must be a number')
            elif demand < 0 or demand > 100:
                errors.append('Demand must be between 0 and 100')
        
        # Validate optional fields
        if 'source' in request_data:
            source = request_data['source']
            if not isinstance(source, str) or len(source) > 50:
                errors.append('Source must be a string with max 50 characters')
        
        # Validate timestamp format if provided
        if 'timestamp' in request_data:
            timestamp = request_data['timestamp']
            if not self._is_valid_iso_datetime(timestamp):
                errors.append('Timestamp must be in ISO datetime format')
        
        # Check for additional security
        if self._contains_suspicious_content(request_data):
            errors.append('Request contains suspicious content')
        
        return {
            'is_valid': len(errors) == 0,
            'errors': errors,
            'error': errors[0] if errors else None
        }
    
    def _is_valid_iso_datetime(self, timestamp: str) -> bool:
        """Validate ISO datetime format"""
        try:
            datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
            return True
        except ValueError:
            return False
    
    def _contains_suspicious_content(self, request_data: Dict[str, Any]) -> bool:
        """Check for suspicious content in request"""
        suspicious_patterns = ['<script>', 'javascript:', 'eval(', 'exec(']
        
        for key, value in request_data.items():
            if isinstance(value, str):
                for pattern in suspicious_patterns:
                    if pattern.lower() in value.lower():
                        return True
        
        return False

# Flask API implementation for local development
from flask import Flask, request, jsonify

app = Flask(__name__)
api_handler = APIGatewayHandler(use_local=True)

@app.route('/v1/pricing/request', methods=['POST'])
def handle_pricing_request():
    """Handle single pricing request"""
    try:
        request_data = request.get_json()
        if not request_data:
            return jsonify({
                'status': 'error',
                'message': 'No JSON data provided'
            }), 400
        
        # Add request metadata
        request_data.update({
            'request_id': request.headers.get('X-Request-ID'),
            'client_ip': request.remote_addr,
            'user_agent': request.headers.get('User-Agent')
        })
        
        response = api_handler.handle_pricing_request(request_data)
        
        status_code = 200 if response.status == 'success' else 400
        return jsonify({
            'status': response.status,
            'message': response.message,
            'data': response.data,
            'error': response.error,
            'timestamp': response.timestamp
        }), status_code
        
    except Exception as e:
        logger.error(f"API error: {e}")
        return jsonify({
            'status': 'error',
            'message': 'Internal server error',
            'error': str(e)
        }), 500

@app.route('/v1/pricing/batch', methods=['POST'])
def handle_batch_request():
    """Handle batch pricing requests"""
    try:
        requests_data = request.get_json()
        if not requests_data or not isinstance(requests_data, list):
            return jsonify({
                'status': 'error',
                'message': 'JSON array required'
            }), 400
        
        # Add request metadata to each request
        for req_data in requests_data:
            req_data.update({
                'request_id': request.headers.get('X-Request-ID'),
                'client_ip': request.remote_addr,
                'user_agent': request.headers.get('User-Agent')
            })
        
        response = api_handler.handle_batch_pricing_requests(requests_data)
        
        status_code = 200 if response.status == 'success' else 400
        return jsonify({
            'status': response.status,
            'message': response.message,
            'data': response.data,
            'timestamp': response.timestamp
        }), status_code
        
    except Exception as e:
        logger.error(f"Batch API error: {e}")
        return jsonify({
            'status': 'error',
            'message': 'Internal server error',
            'error': str(e)
        }), 500

@app.route('/v1/stats', methods=['GET'])
def get_api_stats():
    """Get API statistics"""
    try:
        stats = api_handler.get_api_stats()
        return jsonify({
            'status': 'success',
            'data': stats,
            'timestamp': datetime.now().isoformat()
        })
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': 'Failed to get stats',
            'error': str(e)
        }), 500

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    app.run(debug=True, host='0.0.0.0', port=5001)  # Different port to avoid conflicts
