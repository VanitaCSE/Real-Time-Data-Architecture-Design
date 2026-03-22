"""
Machine Learning Pricing Model
Replaces rule-based pricing with regression model
"""

import numpy as np
import pandas as pd
from sklearn.linear_model import LinearRegression
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error, r2_score
import pickle
import os
import logging
from datetime import datetime
import sqlite3

logger = logging.getLogger(__name__)

class MLPricingModel:
    """
    Machine Learning model for dynamic pricing
    Uses regression to predict prices based on demand and features
    """
    
    def __init__(self, model_type='linear', model_path='pricing_model.pkl'):
        self.model_type = model_type
        self.model_path = model_path
        self.model = None
        self.is_trained = False
        self.feature_columns = ['demand', 'moving_avg', 'volatility', 'trend_strength', 'hour_of_day', 'day_of_week']
        self.training_stats = {}
        
        # Initialize model based on type
        if model_type == 'linear':
            self.model = LinearRegression()
        elif model_type == 'random_forest':
            self.model = RandomForestRegressor(n_estimators=100, random_state=42)
        else:
            raise ValueError(f"Unsupported model type: {model_type}")
    
    def generate_training_data(self, num_samples=10000):
        """
        Generate simulated historical training data
        Creates realistic demand-price relationships with patterns
        """
        logger.info(f"Generating {num_samples} training samples...")
        
        # Simulate time-based patterns
        dates = pd.date_range(start='2023-01-01', periods=num_samples, freq='H')
        
        # Generate demand with patterns
        demand_base = 50
        demand_patterns = []
        prices = []
        
        for i, date in enumerate(dates):
            # Add time-based patterns
            hour_factor = 0.3 * np.sin(2 * np.pi * date.hour / 24)  # Daily pattern
            day_factor = 0.2 * np.sin(2 * np.pi * date.dayofweek / 7)  # Weekly pattern
            
            # Add trend and seasonality
            trend = 0.001 * i  # Slight upward trend
            seasonal = 10 * np.sin(2 * np.pi * i / (24 * 30))  # Monthly seasonality
            
            # Generate demand with noise
            demand = demand_base + hour_factor * 20 + day_factor * 15 + trend + seasonal + np.random.normal(0, 5)
            demand = max(0, min(100, demand))  # Clamp to 0-100
            
            # Calculate moving average (simulate window)
            window_size = min(10, i + 1)
            if i >= 9:
                recent_demands = demand_patterns[-9:]
                moving_avg = np.mean(recent_demands + [demand])
                volatility = np.std(recent_demands + [demand])
                trend_strength = (demand - recent_demands[0]) / 10 if len(recent_demands) > 0 else 0
            else:
                moving_avg = demand
                volatility = 5
                trend_strength = 0
            
            # Generate price with complex relationship to demand
            # Non-linear pricing with multiple factors
            base_price = 100
            
            # Demand elasticity (non-linear)
            if demand < 30:
                demand_multiplier = 0.8 + 0.4 * (demand / 30)  # Low demand: gradual increase
            elif demand < 70:
                demand_multiplier = 1.2 + 0.3 * ((demand - 30) / 40)  # Medium demand: moderate increase
            else:
                demand_multiplier = 1.5 + 0.5 * ((demand - 70) / 30)  # High demand: steep increase
            
            # Volatility premium
            volatility_premium = volatility * 0.5
            
            # Trend bonus/penalty
            trend_adjustment = trend_strength * 2
            
            # Time-based adjustments
            hour_premium = 0.1 * np.sin(2 * np.pi * date.hour / 24)
            day_premium = 0.05 * np.sin(2 * np.pi * date.dayofweek / 7)
            
            price = base_price * demand_multiplier + volatility_premium + trend_adjustment + hour_premium + day_premium
            price = max(80, min(200, price))  # Clamp to reasonable range
            
            # Add some noise
            price += np.random.normal(0, 2)
            
            demand_patterns.append(demand)
            prices.append(price)
        
        # Create DataFrame
        training_data = pd.DataFrame({
            'timestamp': dates,
            'demand': demand_patterns,
            'price': prices,
            'hour_of_day': dates.hour,
            'day_of_week': dates.dayofweek,
            'moving_avg': [np.mean(demand_patterns[max(0, i-9):i+1]) for i in range(len(demand_patterns))],
            'volatility': [np.std(demand_patterns[max(0, i-9):i+1]) for i in range(len(demand_patterns))],
            'trend_strength': [(demand_patterns[i] - demand_patterns[max(0, i-10)]) / 10 if i >= 10 else 0 for i in range(len(demand_patterns))]
        })
        
        logger.info(f"Generated training data with shape: {training_data.shape}")
        return training_data
    
    def prepare_features(self, data):
        """
        Prepare features for ML model
        """
        features = data[self.feature_columns].copy()
        
        # Add interaction terms
        features['demand_moving_avg_ratio'] = features['demand'] / (features['moving_avg'] + 1e-6)
        features['demand_volatility_interaction'] = features['demand'] * features['volatility']
        
        # Add polynomial features for non-linear relationships
        features['demand_squared'] = features['demand'] ** 2
        features['moving_avg_squared'] = features['moving_avg'] ** 2
        
        return features
    
    def train(self, training_data=None, test_size=0.2):
        """
        Train the ML model on historical data
        """
        logger.info("Starting ML model training...")
        
        # Generate training data if not provided
        if training_data is None:
            training_data = self.generate_training_data()
        
        # Prepare features and target
        X = self.prepare_features(training_data)
        y = training_data['price']
        
        # Split data
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=test_size, random_state=42
        )
        
        # Train model
        logger.info(f"Training {self.model_type} model on {len(X_train)} samples...")
        self.model.fit(X_train, y_train)
        
        # Evaluate model
        y_pred = self.model.predict(X_test)
        mse = mean_squared_error(y_test, y_pred)
        r2 = r2_score(y_test, y_pred)
        rmse = np.sqrt(mse)
        
        # Store training statistics
        self.training_stats = {
            'model_type': self.model_type,
            'training_samples': len(X_train),
            'test_samples': len(X_test),
            'mse': mse,
            'rmse': rmse,
            'r2_score': r2,
            'feature_importance': {},
            'training_time': datetime.now().isoformat()
        }
        
        # Feature importance (if available)
        if hasattr(self.model, 'feature_importances_'):
            for i, feature in enumerate(X.columns):
                self.training_stats['feature_importance'][feature] = self.model.feature_importances_[i]
        elif hasattr(self.model, 'coef_'):
            for i, feature in enumerate(X.columns):
                self.training_stats['feature_importance'][feature] = self.model.coef_[i]
        
        self.is_trained = True
        
        logger.info(f"Model training completed. RMSE: {rmse:.2f}, R²: {r2:.3f}")
        logger.info(f"Feature importance: {self.training_stats['feature_importance']}")
        
        return self.training_stats
    
    def predict_price(self, demand, moving_avg=None, volatility=None, trend_strength=None, timestamp=None):
        """
        Predict price using trained ML model
        """
        if not self.is_trained:
            logger.warning("Model not trained, falling back to rule-based pricing")
            return self._fallback_pricing(demand)
        
        try:
            # Use current timestamp if not provided
            if timestamp is None:
                timestamp = datetime.now()
            elif isinstance(timestamp, str):
                timestamp = datetime.fromisoformat(timestamp)
            
            # Default values for features if not provided
            if moving_avg is None:
                moving_avg = demand
            if volatility is None:
                volatility = 5.0
            if trend_strength is None:
                trend_strength = 0.0
            
            # Prepare features
            features = pd.DataFrame({
                'demand': [demand],
                'moving_avg': [moving_avg],
                'volatility': [volatility],
                'trend_strength': [trend_strength],
                'hour_of_day': [timestamp.hour],
                'day_of_week': [timestamp.weekday()],
                'demand_moving_avg_ratio': [demand / (moving_avg + 1e-6)],
                'demand_volatility_interaction': [demand * volatility],
                'demand_squared': [demand ** 2],
                'moving_avg_squared': [moving_avg ** 2]
            })
            
            # Ensure all expected columns are present
            for col in self.feature_columns + ['demand_moving_avg_ratio', 'demand_volatility_interaction', 'demand_squared', 'moving_avg_squared']:
                if col not in features.columns:
                    features[col] = 0
            
            # Predict
            predicted_price = self.model.predict(features[self.feature_columns + ['demand_moving_avg_ratio', 'demand_volatility_interaction', 'demand_squared', 'moving_avg_squared']])[0]
            
            # Clamp to reasonable range
            predicted_price = max(80, min(200, predicted_price))
            
            logger.debug(f"ML prediction: demand={demand}, predicted_price={predicted_price:.2f}")
            
            return round(predicted_price, 2)
            
        except Exception as e:
            logger.error(f"ML prediction failed: {e}, falling back to rule-based")
            return self._fallback_pricing(demand)
    
    def _fallback_pricing(self, demand):
        """
        Fallback rule-based pricing if ML fails
        """
        base_price = 100.0
        
        if demand <= 33:
            multiplier = 1.0
        elif demand <= 66:
            multiplier = 1.2
        else:
            multiplier = 1.5
        
        return round(base_price * multiplier, 2)
    
    def save_model(self, path=None):
        """
        Save trained model to disk
        """
        if not self.is_trained:
            raise ValueError("Cannot save untrained model")
        
        save_path = path or self.model_path
        
        model_data = {
            'model': self.model,
            'model_type': self.model_type,
            'feature_columns': self.feature_columns,
            'training_stats': self.training_stats,
            'is_trained': self.is_trained
        }
        
        with open(save_path, 'wb') as f:
            pickle.dump(model_data, f)
        
        logger.info(f"Model saved to {save_path}")
    
    def load_model(self, path=None):
        """
        Load trained model from disk
        """
        load_path = path or self.model_path
        
        if not os.path.exists(load_path):
            logger.warning(f"Model file {load_path} not found, will need to train")
            return False
        
        try:
            with open(load_path, 'rb') as f:
                model_data = pickle.load(f)
            
            self.model = model_data['model']
            self.model_type = model_data['model_type']
            self.feature_columns = model_data['feature_columns']
            self.training_stats = model_data['training_stats']
            self.is_trained = model_data['is_trained']
            
            logger.info(f"Model loaded from {load_path}")
            logger.info(f"Model stats: R²={self.training_stats.get('r2_score', 'N/A'):.3f}")
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to load model: {e}")
            return False
    
    def get_model_info(self):
        """
        Get model information and statistics
        """
        if not self.is_trained:
            return {'status': 'not_trained', 'model_type': self.model_type}
        
        return {
            'status': 'trained',
            'model_type': self.model_type,
            'training_stats': self.training_stats,
            'feature_columns': self.feature_columns
        }
    
    def retrain_with_database_data(self, db_path='pricing_data.db'):
        """
        Retrain model using actual database data
        """
        if not os.path.exists(db_path):
            logger.warning(f"Database {db_path} not found, using simulated data")
            return self.train()
        
        try:
            # Load data from database
            conn = sqlite3.connect(db_path)
            query = """
                SELECT demand, price, timestamp, moving_avg, volatility, trend_strength
                FROM pricing_events 
                WHERE moving_avg IS NOT NULL 
                ORDER BY timestamp
            """
            df = pd.read_sql_query(query, conn)
            conn.close()
            
            if len(df) < 100:
                logger.warning(f"Insufficient data in database ({len(df)} records), using simulated data")
                return self.train()
            
            # Prepare timestamp features
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df['hour_of_day'] = df['timestamp'].dt.hour
            df['day_of_week'] = df['timestamp'].dt.weekday
            
            # Fill missing values
            df['moving_avg'].fillna(df['demand'], inplace=True)
            df['volatility'].fillna(5.0, inplace=True)
            df['trend_strength'].fillna(0.0, inplace=True)
            
            logger.info(f"Training with {len(df)} real database records")
            return self.train(df)
            
        except Exception as e:
            logger.error(f"Failed to train with database data: {e}, using simulated data")
            return self.train()

# Example usage and testing
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    # Create and train model
    model = MLPricingModel(model_type='random_forest')
    
    # Train with simulated data
    stats = model.train()
    print("Training completed:", stats)
    
    # Test predictions
    test_demands = [20, 40, 60, 80]
    for demand in test_demands:
        price = model.predict_price(demand, moving_avg=demand, volatility=5.0, trend_strength=0.0)
        print(f"Demand: {demand} -> Predicted Price: ${price}")
    
    # Save model
    model.save_model()
    print("Model saved successfully!")
