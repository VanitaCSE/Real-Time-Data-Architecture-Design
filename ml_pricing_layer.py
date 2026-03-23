"""
STEP 5: ML-Based Pricing System
Machine Learning model for real-time price prediction with training and inference
"""

import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.metrics import mean_squared_error, r2_score, mean_absolute_error
import pickle
import threading
import time
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional, Tuple
import logging
from collections import deque
import json

logger = logging.getLogger(__name__)

class MLPricingModel:
    """
    Machine Learning Pricing Model
    Supports multiple algorithms with real-time training and inference
    """
    
    def __init__(self, model_type: str = "random_forest"):
        self.model_type = model_type
        self.model = None
        self.scaler = StandardScaler()
        self.is_trained = False
        
        # Model metadata
        self.model_version = "v1.0"
        self.training_timestamp = None
        self.feature_names = []
        self.model_metrics = {}
        
        # Training data storage
        self.training_data = deque(maxlen=10000)  # Last 10k events for training
        self.min_training_samples = 50
        
        # Performance tracking
        self.prediction_count = 0
        self.prediction_errors = deque(maxlen=100)
        
        # Initialize model
        self._initialize_model()
        
    def _initialize_model(self):
        """Initialize the ML model based on type"""
        if self.model_type == "random_forest":
            self.model = RandomForestRegressor(
                n_estimators=100,
                max_depth=10,
                random_state=42,
                n_jobs=-1
            )
        elif self.model_type == "gradient_boosting":
            self.model = GradientBoostingRegressor(
                n_estimators=100,
                learning_rate=0.1,
                max_depth=6,
                random_state=42
            )
        elif self.model_type == "linear_regression":
            self.model = LinearRegression()
        else:
            raise ValueError(f"Unsupported model type: {self.model_type}")
            
        logger.info(f"🤖 Initialized {self.model_type} model")
        
    def add_training_sample(self, demand: float, price: float, features: Dict[str, Any] = None):
        """
        Add a training sample from real-time data
        """
        sample = {
            'timestamp': datetime.now().isoformat(),
            'demand': demand,
            'price': price,
            'features': features or {}
        }
        
        self.training_data.append(sample)
        
        # Auto-train if we have enough samples
        if len(self.training_data) >= self.min_training_samples and len(self.training_data) % 20 == 0:
            threading.Thread(target=self._auto_train, daemon=True).start()
            
    def _auto_train(self):
        """Automatic training in background thread"""
        try:
            logger.info("🔄 Starting auto-training...")
            success = self.train_model()
            if success:
                logger.info("✅ Auto-training completed successfully")
            else:
                logger.warning("⚠️ Auto-training failed")
        except Exception as e:
            logger.error(f"❌ Auto-training error: {e}")
            
    def train_model(self, force_retrain: bool = False) -> bool:
        """
        Train the ML model with collected data
        """
        try:
            if len(self.training_data) < self.min_training_samples:
                logger.warning(f"⚠️ Insufficient training data: {len(self.training_data)} < {self.min_training_samples}")
                return False
                
            # Convert training data to DataFrame
            df = pd.DataFrame(list(self.training_data))
            
            # Prepare features
            X, y = self._prepare_features(df)
            
            if X is None or len(X) == 0:
                logger.error("❌ Failed to prepare features")
                return False
                
            # Split data
            X_train, X_test, y_train, y_test = train_test_split(
                X, y, test_size=0.2, random_state=42
            )
            
            # Scale features
            X_train_scaled = self.scaler.fit_transform(X_train)
            X_test_scaled = self.scaler.transform(X_test)
            
            # Train model
            self.model.fit(X_train_scaled, y_train)
            
            # Evaluate model
            y_pred = self.model.predict(X_test_scaled)
            
            # Calculate metrics
            self.model_metrics = {
                'mse': mean_squared_error(y_test, y_pred),
                'rmse': np.sqrt(mean_squared_error(y_test, y_pred)),
                'mae': mean_absolute_error(y_test, y_pred),
                'r2_score': r2_score(y_test, y_pred),
                'training_samples': len(X_train),
                'validation_samples': len(X_test)
            }
            
            # Cross-validation
            cv_scores = cross_val_score(
                self.model, X_train_scaled, y_train, 
                cv=5, scoring='r2'
            )
            self.model_metrics['cv_r2_mean'] = cv_scores.mean()
            self.model_metrics['cv_r2_std'] = cv_scores.std()
            
            # Update model metadata
            self.is_trained = True
            self.training_timestamp = datetime.now().isoformat()
            self.feature_names = list(X.columns)
            
            logger.info(f"✅ Model trained successfully - R²: {self.model_metrics['r2_score']:.4f}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Model training failed: {e}")
            return False
            
    def _prepare_features(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.Series]:
        """
        Prepare features for ML training
        """
        try:
            # Base features
            features = ['demand']
            
            # Time-based features
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df['hour'] = df['timestamp'].dt.hour
            df['day_of_week'] = df['timestamp'].dt.dayofweek
            features.extend(['hour', 'day_of_week'])
            
            # Demand-based features
            df['demand_squared'] = df['demand'] ** 2
            df['demand_log'] = np.log1p(df['demand'])
            features.extend(['demand_squared', 'demand_log'])
            
            # Rolling features (if enough data)
            if len(df) >= 5:
                df['demand_ma_3'] = df['demand'].rolling(window=3, min_periods=1).mean()
                df['demand_std_3'] = df['demand'].rolling(window=3, min_periods=1).std()
                df['demand_trend_3'] = df['demand'].rolling(window=3, min_periods=1).apply(
                    lambda x: np.polyfit(range(len(x)), x, 1)[0] if len(x) > 1 else 0
                )
                features.extend(['demand_ma_3', 'demand_std_3', 'demand_trend_3'])
                
            if len(df) >= 10:
                df['demand_ma_5'] = df['demand'].rolling(window=5, min_periods=1).mean()
                df['demand_std_5'] = df['demand'].rolling(window=5, min_periods=1).std()
                features.extend(['demand_ma_5', 'demand_std_5'])
                
            # Additional features from event data
            if 'features' in df.columns:
                feature_df = pd.json_normalize(df['features'])
                for col in feature_df.columns:
                    if col not in df.columns:
                        df[col] = feature_df[col]
                        features.append(col)
                        
            # Select available features
            available_features = [f for f in features if f in df.columns]
            
            # Handle missing values
            X = df[available_features].fillna(0)
            y = df['price']
            
            return X, y
            
        except Exception as e:
            logger.error(f"❌ Feature preparation failed: {e}")
            return None, None
            
    def predict_price(self, demand: float, features: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        Predict price using trained ML model
        """
        start_time = time.time()
        
        try:
            if not self.is_trained:
                return self._fallback_prediction(demand, "Model not trained")
                
            # Prepare features for prediction
            X = self._prepare_prediction_features(demand, features)
            
            if X is None:
                return self._fallback_prediction(demand, "Feature preparation failed")
                
            # Scale features
            X_scaled = self.scaler.transform(X)
            
            # Make prediction
            predicted_price = self.model.predict(X_scaled)[0]
            
            # Calculate confidence (based on historical performance)
            confidence = self._calculate_confidence(demand)
            
            # Ensure reasonable price bounds
            predicted_price = max(50.0, min(250.0, predicted_price))
            
            # Track prediction
            self.prediction_count += 1
            prediction_time = (time.time() - start_time) * 1000
            
            result = {
                'predicted_price': round(predicted_price, 2),
                'confidence_score': round(confidence, 3),
                'model_type': self.model_type,
                'model_version': self.model_version,
                'prediction_time_ms': round(prediction_time, 2),
                'features_used': list(X.columns),
                'is_ml_prediction': True
            }
            
            logger.debug(f"🎯 ML Prediction: {predicted_price:.2f} (confidence: {confidence:.3f})")
            return result
            
        except Exception as e:
            logger.error(f"❌ ML prediction failed: {e}")
            return self._fallback_prediction(demand, f"Prediction error: {str(e)}")
            
    def _prepare_prediction_features(self, demand: float, features: Dict[str, Any]) -> Optional[pd.DataFrame]:
        """Prepare features for single prediction"""
        try:
            # Create feature dictionary
            now = datetime.now()
            feature_dict = {
                'demand': demand,
                'hour': now.hour,
                'day_of_week': now.weekday(),
                'demand_squared': demand ** 2,
                'demand_log': np.log1p(demand)
            }
            
            # Add rolling features if available from training data
            if len(self.training_data) >= 3:
                recent_demands = [sample['demand'] for sample in list(self.training_data)[-3:]]
                feature_dict['demand_ma_3'] = np.mean(recent_demands)
                feature_dict['demand_std_3'] = np.std(recent_demands) if len(recent_demands) > 1 else 0
                feature_dict['demand_trend_3'] = np.polyfit(range(len(recent_demands)), recent_demands, 1)[0] if len(recent_demands) > 1 else 0
                
            if len(self.training_data) >= 5:
                recent_demands = [sample['demand'] for sample in list(self.training_data)[-5:]]
                feature_dict['demand_ma_5'] = np.mean(recent_demands)
                feature_dict['demand_std_5'] = np.std(recent_demands) if len(recent_demands) > 1 else 0
                
            # Add additional features
            if features:
                feature_dict.update(features)
                
            # Create DataFrame with correct feature order
            available_features = {k: v for k, v in feature_dict.items() if k in self.feature_names}
            
            if not available_features:
                logger.error("❌ No valid features for prediction")
                return None
                
            # Ensure all required features are present
            for feature in self.feature_names:
                if feature not in available_features:
                    available_features[feature] = 0
                    
            # Create DataFrame with correct column order
            X = pd.DataFrame([available_features], columns=self.feature_names)
            return X
            
        except Exception as e:
            logger.error(f"❌ Prediction feature preparation failed: {e}")
            return None
            
    def _calculate_confidence(self, demand: float) -> float:
        """Calculate prediction confidence based on model performance and data similarity"""
        if not self.is_trained:
            return 0.0
            
        # Base confidence from model R² score
        base_confidence = max(0.0, self.model_metrics.get('r2_score', 0.0))
        
        # Adjust based on data similarity
        if self.training_data:
            training_demands = [sample['demand'] for sample in self.training_data]
            demand_std = np.std(training_demands)
            
            if demand_std > 0:
                # Calculate how similar this demand is to training data
                demand_z_score = abs(demand - np.mean(training_demands)) / demand_std
                similarity_penalty = min(0.3, demand_z_score * 0.1)
                base_confidence *= (1 - similarity_penalty)
                
        # Adjust based on prediction error history
        if self.prediction_errors:
            avg_error = np.mean(self.prediction_errors)
            error_penalty = min(0.2, avg_error / 100.0)
            base_confidence *= (1 - error_penalty)
            
        return max(0.1, min(1.0, base_confidence))
        
    def _fallback_prediction(self, demand: float, reason: str) -> Dict[str, Any]:
        """Fallback prediction using rule-based approach"""
        # Simple rule-based pricing
        base_price = 100.0
        price = base_price + (demand * 0.8)
        price = max(50.0, min(250.0, price))
        
        return {
            'predicted_price': round(price, 2),
            'confidence_score': 0.5,
            'model_type': 'rule_based_fallback',
            'model_version': 'fallback',
            'prediction_time_ms': 1.0,
            'fallback_reason': reason,
            'features_used': ['demand'],
            'is_ml_prediction': False
        }
        
    def update_with_actual_price(self, demand: float, predicted_price: float, actual_price: float):
        """Update model with actual price for continuous learning"""
        error = abs(actual_price - predicted_price)
        self.prediction_errors.append(error)
        
        # Add to training data
        self.add_training_sample(demand, actual_price)
        
        logger.debug(f"📊 Updated with actual price - Error: {error:.2f}")
        
    def get_model_info(self) -> Dict[str, Any]:
        """Get comprehensive model information"""
        info = {
            'model_type': self.model_type,
            'model_version': self.model_version,
            'is_trained': self.is_trained,
            'training_timestamp': self.training_timestamp,
            'feature_names': self.feature_names,
            'training_samples': len(self.training_data),
            'prediction_count': self.prediction_count,
            'metrics': self.model_metrics
        }
        
        if self.prediction_errors:
            info['prediction_performance'] = {
                'avg_prediction_error': round(np.mean(self.prediction_errors), 2),
                'max_prediction_error': round(np.max(self.prediction_errors), 2),
                'min_prediction_error': round(np.min(self.prediction_errors), 2),
                'recent_predictions': len(self.prediction_errors)
            }
            
        return info
        
    def save_model(self, filepath: str) -> bool:
        """Save trained model to file"""
        try:
            model_data = {
                'model': self.model,
                'scaler': self.scaler,
                'model_type': self.model_type,
                'model_version': self.model_version,
                'feature_names': self.feature_names,
                'training_timestamp': self.training_timestamp,
                'model_metrics': self.model_metrics,
                'is_trained': self.is_trained
            }
            
            with open(filepath, 'wb') as f:
                pickle.dump(model_data, f)
                
            logger.info(f"💾 Model saved to {filepath}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to save model: {e}")
            return False
            
    def load_model(self, filepath: str) -> bool:
        """Load trained model from file"""
        try:
            with open(filepath, 'rb') as f:
                model_data = pickle.load(f)
                
            self.model = model_data['model']
            self.scaler = model_data['scaler']
            self.model_type = model_data['model_type']
            self.model_version = model_data['model_version']
            self.feature_names = model_data['feature_names']
            self.training_timestamp = model_data['training_timestamp']
            self.model_metrics = model_data['model_metrics']
            self.is_trained = model_data['is_trained']
            
            logger.info(f"📂 Model loaded from {filepath}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to load model: {e}")
            return False

class MLModelManager:
    """
    Manages multiple ML models with A/B testing and model selection
    """
    
    def __init__(self):
        self.models = {}
        self.active_model = None
        self.model_performance = {}
        
    def add_model(self, name: str, model: MLPricingModel):
        """Add a model to the manager"""
        self.models[name] = model
        
        # Set as active if first model
        if not self.active_model:
            self.active_model = name
            
        logger.info(f"➕ Added model: {name}")
        
    def set_active_model(self, name: str) -> bool:
        """Set the active model for predictions"""
        if name in self.models:
            self.active_model = name
            logger.info(f"🎯 Active model set to: {name}")
            return True
        return False
        
    def predict_with_active_model(self, demand: float, features: Dict[str, Any] = None) -> Dict[str, Any]:
        """Make prediction using active model"""
        if not self.active_model or self.active_model not in self.models:
            raise ValueError("No active model available")
            
        model = self.models[self.active_model]
        prediction = model.predict_price(demand, features)
        prediction['model_name'] = self.active_model
        
        return prediction
        
    def get_all_models_info(self) -> Dict[str, Any]:
        """Get information about all models"""
        return {
            name: model.get_model_info() 
            for name, model in self.models.items()
        }
        
    def compare_models(self, test_samples: List[Tuple[float, float]]) -> Dict[str, Any]:
        """Compare performance of all models"""
        results = {}
        
        for name, model in self.models.items():
            if not model.is_trained:
                continue
                
            errors = []
            for demand, actual_price in test_samples:
                prediction = model.predict_price(demand)
                predicted_price = prediction['predicted_price']
                error = abs(actual_price - predicted_price)
                errors.append(error)
                
            if errors:
                results[name] = {
                    'avg_error': np.mean(errors),
                    'max_error': np.max(errors),
                    'min_error': np.min(errors),
                    'std_error': np.std(errors)
                }
                
        return results

# ML Architecture Explanation
"""
MACHINE LEARNING ARCHITECTURE:

1. Model Types:
   - Random Forest: Ensemble method, robust to outliers
   - Gradient Boosting: Sequential ensemble, high accuracy
   - Linear Regression: Simple baseline, interpretable

2. Feature Engineering:
   - Time-based features (hour, day of week)
   - Demand transformations (log, squared)
   - Rolling statistics (moving averages, trends)
   - Volatility measures

3. Training Pipeline:
   - Automatic data collection from streaming
   - Feature preprocessing and scaling
   - Model training with cross-validation
   - Performance evaluation (R², MSE, MAE)

4. Real-time Inference:
   - Feature preparation for live data
   - Confidence scoring based on similarity
   - Fallback to rule-based pricing
   - Continuous learning with actual prices

5. Production Scaling:
   - Amazon SageMaker for model training
   - AWS Lambda for model inference
   - Amazon S3 for model storage
   - AWS CloudWatch for model monitoring
"""

# Initialize global ML model
ml_model = MLPricingModel(model_type="random_forest")
ml_manager = MLModelManager()
ml_manager.add_model("primary", ml_model)
