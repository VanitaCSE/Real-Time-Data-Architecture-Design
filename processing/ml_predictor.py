"""
🤖 Processing Layer - ML Predictor
Machine learning model for price prediction
Maps to: AWS SageMaker + Lambda
"""

import json
import logging
import pickle
import os
from datetime import datetime
from typing import Dict, Any, Optional
import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestRegressor
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import train_test_split
from sklearn.metrics import r2_score, mean_absolute_error

logger = logging.getLogger(__name__)

class MLPredictor:
    """
    Machine Learning predictor for dynamic pricing
    Maps to: AWS SageMaker endpoint
    """
    
    def __init__(self, model_path: str = 'pricing_model.pkl', scaler_path: str = 'pricing_scaler.pkl'):
        self.model_path = model_path
        self.scaler_path = scaler_path
        self.model = None
        self.scaler = None
        self.is_trained = False
        self.model_info = {
            'model_type': 'random_forest',
            'status': 'not_initialized',
            'training_stats': {},
            'last_updated': None
        }
        
        # Try to load existing model
        self._load_model()
    
    def _load_model(self) -> bool:
        """
        Load trained model from file
        Maps to: SageMaker model loading
        """
        try:
            if os.path.exists(self.model_path) and os.path.exists(self.scaler_path):
                with open(self.model_path, 'rb') as f:
                    self.model = pickle.load(f)
                
                with open(self.scaler_path, 'rb') as f:
                    self.scaler = pickle.load(f)
                
                self.is_trained = True
                self.model_info['status'] = 'loaded'
                self.model_info['last_updated'] = datetime.now().isoformat()
                
                logger.info(f"ML model loaded from {self.model_path}")
                return True
            else:
                logger.info("No existing model found, will need training")
                return False
                
        except Exception as e:
            logger.error(f"Failed to load model: {e}")
            return False
    
    def predict_price(self, demand: int, moving_avg: float = 50.0, 
                     volatility: float = 0.0, trend_strength: float = 0.0,
                     timestamp: Optional[str] = None) -> Dict[str, Any]:
        """
        Predict price using ML model
        Maps to: SageMaker endpoint invocation
        """
        if not self.is_trained or self.model is None:
            raise ValueError("Model not trained or loaded")
        
        try:
            # Prepare features
            features = self._prepare_features(demand, moving_avg, volatility, trend_strength, timestamp)
            
            # Scale features
            features_scaled = self.scaler.transform([features])
            
            # Make prediction
            predicted_price = self.model.predict(features_scaled)[0]
            
            # Determine pricing tier
            if predicted_price <= 110:
                tier = 'Low'
            elif predicted_price <= 140:
                tier = 'Medium'
            else:
                tier = 'High'
            
            logger.debug(f"ML prediction: demand={demand}, price=${predicted_price:.2f}, tier={tier}")
            
            return {
                'predicted_price': round(predicted_price, 2),
                'pricing_tier': tier,
                'features_used': {
                    'demand': demand,
                    'moving_avg': moving_avg,
                    'volatility': volatility,
                    'trend_strength': trend_strength
                },
                'confidence': self._calculate_prediction_confidence(features_scaled),
                'model_type': self.model_info['model_type']
            }
            
        except Exception as e:
            logger.error(f"ML prediction failed: {e}")
            raise
    
    def _prepare_features(self, demand: int, moving_avg: float, volatility: float, 
                         trend_strength: float, timestamp: Optional[str] = None) -> list:
        """
        Prepare features for ML prediction
        Maps to: Feature engineering pipeline
        """
        # Parse timestamp for temporal features
        if timestamp:
            try:
                dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                hour_of_day = dt.hour
                day_of_week = dt.weekday()
            except ValueError:
                hour_of_day = datetime.now().hour
                day_of_week = datetime.now().weekday()
        else:
            hour_of_day = datetime.now().hour
            day_of_week = datetime.now().weekday()
        
        # Create feature vector
        features = [
            demand,                    # Current demand
            moving_avg,                # Moving average
            volatility,                # Volatility
            trend_strength,            # Trend strength
            hour_of_day,              # Hour of day (0-23)
            day_of_week,              # Day of week (0-6)
            demand - moving_avg,       # Demand deviation from average
            abs(trend_strength),      # Absolute trend strength
            volatility * demand,       # Demand-weighted volatility
            max(0, demand - 50),      # Demand above baseline
            max(0, 50 - demand)       # Demand below baseline
        ]
        
        return features
    
    def _calculate_prediction_confidence(self, features_scaled) -> float:
        """
        Calculate prediction confidence based on feature variance
        Maps to: Model uncertainty estimation
        """
        try:
            # Get predictions from all trees in Random Forest
            tree_predictions = [tree.predict(features_scaled)[0] for tree in self.model.estimators_]
            
            # Calculate standard deviation as confidence measure
            std_dev = np.std(tree_predictions)
            
            # Convert to confidence score (lower std = higher confidence)
            confidence = max(0, min(100, 100 - (std_dev * 10)))
            
            return round(confidence, 2)
            
        except Exception:
            return 75.0  # Default confidence
    
    def train_model(self, training_data: list) -> Dict[str, Any]:
        """
        Train ML model with provided data
        Maps to: SageMaker training job
        """
        try:
            logger.info(f"Starting ML model training with {len(training_data)} samples")
            
            # Prepare training data
            X, y = self._prepare_training_data(training_data)
            
            if len(X) < 10:
                raise ValueError("Insufficient training data (minimum 10 samples required)")
            
            # Split data
            X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
            
            # Initialize and train model
            self.model = RandomForestRegressor(
                n_estimators=100,
                max_depth=10,
                min_samples_split=5,
                min_samples_leaf=2,
                random_state=42
            )
            
            self.model.fit(X_train, y_train)
            
            # Initialize and fit scaler
            self.scaler = StandardScaler()
            self.scaler.fit(X_train)
            
            # Evaluate model
            y_pred = self.model.predict(X_test)
            r2 = r2_score(y_test, y_pred)
            mae = mean_absolute_error(y_test, y_pred)
            
            # Update model info
            self.is_trained = True
            self.model_info.update({
                'status': 'trained',
                'training_stats': {
                    'samples_trained': len(X_train),
                    'samples_tested': len(X_test),
                    'r2_score': round(r2, 4),
                    'mean_absolute_error': round(mae, 4),
                    'feature_importance': dict(zip(self._get_feature_names(), self.model.feature_importances_))
                },
                'last_updated': datetime.now().isoformat()
            })
            
            # Save model
            self._save_model()
            
            logger.info(f"Model training completed - R²: {r2:.4f}, MAE: {mae:.4f}")
            
            return self.model_info['training_stats']
            
        except Exception as e:
            logger.error(f"Model training failed: {e}")
            raise
    
    def _prepare_training_data(self, training_data: list) -> tuple:
        """
        Prepare training data from historical events
        Maps to: Data preprocessing pipeline
        """
        features = []
        prices = []
        
        for data_point in training_data:
            # Extract features
            feature_vector = self._prepare_features(
                demand=data_point['demand'],
                moving_avg=data_point.get('moving_avg', 50.0),
                volatility=data_point.get('volatility', 0.0),
                trend_strength=data_point.get('trend_strength', 0.0),
                timestamp=data_point.get('timestamp')
            )
            
            features.append(feature_vector)
            prices.append(data_point['price'])
        
        return np.array(features), np.array(prices)
    
    def _get_feature_names(self) -> list:
        """Get feature names for model interpretation"""
        return [
            'demand', 'moving_avg', 'volatility', 'trend_strength',
            'hour_of_day', 'day_of_week', 'demand_deviation',
            'abs_trend_strength', 'demand_weighted_volatility',
            'demand_above_baseline', 'demand_below_baseline'
        ]
    
    def _save_model(self) -> None:
        """
        Save trained model to file
        Maps to: SageMaker model artifact storage
        """
        try:
            with open(self.model_path, 'wb') as f:
                pickle.dump(self.model, f)
            
            with open(self.scaler_path, 'wb') as f:
                pickle.dump(self.scaler, f)
            
            logger.info(f"Model saved to {self.model_path}")
            
        except Exception as e:
            logger.error(f"Failed to save model: {e}")
    
    def is_available(self) -> bool:
        """Check if ML predictor is available"""
        return self.is_trained and self.model is not None
    
    def get_model_info(self) -> Dict[str, Any]:
        """Get model information"""
        return self.model_info.copy()
    
    def generate_training_data(self, num_samples: int = 1000) -> list:
        """
        Generate simulated training data
        Maps to: Historical data preparation
        """
        training_data = []
        
        for i in range(num_samples):
            # Generate realistic demand pattern
            base_demand = 50
            demand_variation = np.sin(i / 50) * 20  # Seasonal pattern
            random_noise = np.random.normal(0, 5)     # Random variation
            demand = max(0, min(100, base_demand + demand_variation + random_noise))
            
            # Generate window statistics
            moving_avg = demand + np.random.normal(0, 2)
            volatility = abs(np.random.normal(0, 0.1))
            trend_strength = np.random.normal(0, 0.5)
            
            # Calculate price based on demand and features
            base_price = 100.0
            
            # Demand-based pricing
            if demand <= 33:
                demand_multiplier = 1.0
            elif demand <= 66:
                demand_multiplier = 1.2
            else:
                demand_multiplier = 1.5
            
            # Feature-based adjustments
            trend_adjustment = trend_strength * 2
            volatility_adjustment = volatility * 10
            
            price = base_price * demand_multiplier + trend_adjustment + volatility_adjustment
            price = round(max(50, min(200, price)), 2)
            
            # Create timestamp
            timestamp = (datetime.now() - timedelta(hours=num_samples - i)).isoformat()
            
            training_data.append({
                'demand': int(demand),
                'price': price,
                'moving_avg': round(moving_avg, 2),
                'volatility': round(volatility, 4),
                'trend_strength': round(trend_strength, 3),
                'timestamp': timestamp
            })
        
        return training_data

# AWS SageMaker endpoint wrapper
class SageMakerPredictor:
    """
    Wrapper for AWS SageMaker endpoint
    Maps to: Production SageMaker deployment
    """
    
    def __init__(self, endpoint_name: str = 'pricing-predictor-endpoint'):
        self.endpoint_name = endpoint_name
        self.runtime_client = None  # boto3.client('sagemaker-runtime')
        
    def predict(self, features: list) -> float:
        """
        Invoke SageMaker endpoint for prediction
        Maps to: SageMaker runtime invocation
        """
        # In production, this would call SageMaker endpoint
        # For now, use local predictor
        predictor = MLPredictor()
        if not predictor.is_available():
            # Train with sample data if not available
            training_data = predictor.generate_training_data(500)
            predictor.train_model(training_data)
        
        result = predictor.predict_price(
            demand=features[0],
            moving_avg=features[1],
            volatility=features[2],
            trend_strength=features[3]
        )
        
        return result['predicted_price']

# Test function
def test_ml_predictor():
    """Test ML predictor locally"""
    predictor = MLPredictor()
    
    # Train model if not available
    if not predictor.is_available():
        print("Training new model...")
        training_data = predictor.generate_training_data(1000)
        training_stats = predictor.train_model(training_data)
        print(f"Training completed: {training_stats}")
    
    # Test predictions
    test_cases = [
        {'demand': 25, 'moving_avg': 45, 'volatility': 0.1, 'trend_strength': -0.5},
        {'demand': 50, 'moving_avg': 50, 'volatility': 0.05, 'trend_strength': 0.0},
        {'demand': 75, 'moving_avg': 55, 'volatility': 0.2, 'trend_strength': 0.8}
    ]
    
    for test_case in test_cases:
        result = predictor.predict_price(**test_case)
        print(f"Test case: {test_case}")
        print(f"Prediction: ${result['predicted_price']} ({result['pricing_tier']}) - Confidence: {result['confidence']}%")
        print()

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    test_ml_predictor()
