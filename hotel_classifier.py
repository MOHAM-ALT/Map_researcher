# machine_learning/hotel_classifier.py
import os
import json
import pickle
import logging
import numpy as np
import pandas as pd
from datetime import datetime
from sklearn.ensemble import RandomForestClassifier
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import Pipeline
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report, confusion_matrix
import re

class HotelClassifier:
    """Machine learning classifier for identifying hotels and daily rentals"""
    
    def __init__(self, db=None, config=None):
        """Initialize the HotelClassifier class"""
        self.db = db
        self.config = config
        self.logger = logging.getLogger("hotel_classifier")
        self.model = None
        self.text_vectorizer = None
        self.feature_names = None
        self.model_trained = False
        
    def train_model(self, training_data=None):
        """
        Train the hotel classification model
        
        Args:
            training_data: Optional training data, if not provided will load from database
            
        Returns:
            Dict with training results
        """
        self.logger.info("Training hotel classification model")
        
        try:
            # If training data not provided, load from database
            if training_data is None and self.db and hasattr(self.db, 'get_training_samples'):
                training_data = self.db.get_training_samples()
                self.logger.info(f"Loaded {len(training_data)} training samples from database")
            
            if not training_data or len(training_data) < 10:
                self.logger.warning("Insufficient training data")
                return {"error": "Insufficient training data"}
            
            # Prepare training data
            X, y = self._prepare_training_data(training_data)
            
            if len(X) < 10:
                self.logger.warning("Insufficient processed training data")
                return {"error": "Insufficient processed training data"}
            
            # Split data into training and testing sets
            X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
            
            # Train model
            self.logger.info("Training Random Forest classifier")
            
            self.model = RandomForestClassifier(
                n_estimators=100, 
                max_depth=None,
                min_samples_split=2,
                random_state=42
            )
            
            self.model.fit(X_train, y_train)
            
            # Evaluate model
            y_pred = self.model.predict(X_test)
            
            # Calculate metrics
            accuracy = (y_pred == y_test).mean()
            report = classification_report(y_test, y_pred, output_dict=True)
            
            self.logger.info(f"Model trained with accuracy: {accuracy:.4f}")
            
            # Get feature importance
            if hasattr(self.model, 'feature_importances_'):
                feature_importance = self.model.feature_importances_
                feature_importance_dict = {self.feature_names[i]: float(feature_importance[i]) 
                                          for i in range(len(self.feature_names))}
            else:
                feature_importance_dict = {}
            
            # Sort features by importance
            sorted_features = sorted(feature_importance_dict.items(), key=lambda x: x[1], reverse=True)
            top_features = sorted_features[:20]
            
            self.model_trained = True
            
            return {
                "status": "success",
                "accuracy": float(accuracy),
                "report": report,
                "top_features": top_features,
                "training_samples": len(y),
                "positive_samples": int(sum(y)),
                "negative_samples": int(len(y) - sum(y))
            }
            
        except Exception as e:
            self.logger.error(f"Error training model: {str(e)}")
            return {"error": f"Error training model: {str(e)}"}
    
    def _prepare_training_data(self, training_data):
        """
        Prepare training data for model training
        
        Args:
            training_data: List of training samples
            
        Returns:
            Tuple (X, y) of features and labels
        """
        # Convert to DataFrame for easier processing
        df = pd.DataFrame(training_data)
        
        # Ensure required columns exist
        required_columns = ['name', 'is_hotel']
        for col in required_columns:
            if col not in df.columns:
                self.logger.error(f"Required column '{col}' missing from training data")
                return np.array([]), np.array([])
        
        # Extract labels
        y = df['is_hotel'].astype(int).values
        
        # Process text features (name, description, etc.)
        text_features = []
        
        # Process name
        if 'name' in df.columns:
            df['name'] = df['name'].fillna('').astype(str)
            text_features.append('name')
        
        # Process description if available
        if 'description' in df.columns:
            df['description'] = df['description'].fillna('').astype(str)
            text_features.append('description')
        
        # Process address if available
        if 'address' in df.columns:
            df['address'] = df['address'].fillna('').astype(str)
            text_features.append('address')
        
        # Process reviews if available
        if 'reviews' in df.columns:
            df['reviews_text'] = df['reviews'].apply(lambda x: ' '.join(x) if isinstance(x, list) else '')
            text_features.append('reviews_text')
        
        # Combine text features
        df['combined_text'] = ''
        for col in text_features:
            df['combined_text'] += ' ' + df[col]
        
        # Vectorize text
        self.text_vectorizer = TfidfVectorizer(
            max_features=1000,
            min_df=2,
            ngram_range=(1, 2),
            stop_words='english'
        )
        
        X_text = self.text_vectorizer.fit_transform(df['combined_text']).toarray()
        
        # Process numeric features
        numeric_features = []
        
        # Extract ratings if available
        if 'rating' in df.columns:
            df['rating_numeric'] = df['rating'].apply(self._extract_numeric_rating)
            numeric_features.append('rating_numeric')
        
        # Extract price indicators
        if 'price' in df.columns:
            df['has_price'] = df['price'].notna() & (df['price'] != '')
            numeric_features.append('has_price')
        
        # Extract category indicators
        if 'category' in df.columns:
            df['hotel_in_category'] = df['category'].fillna('').str.lower().apply(
                lambda x: 1 if re.search(r'hotel|accommodation|lodging|guest.house|motel|inn|rental|furnished', x) else 0
            )
            numeric_features.append('hotel_in_category')
        
        # Extract website indicators
        if 'website' in df.columns:
            df['has_website'] = df['website'].notna() & (df['website'] != '')
            numeric_features.append('has_website')
        
        # Process name length as feature
        if 'name' in df.columns:
            df['name_length'] = df['name'].str.len()
            numeric_features.append('name_length')
        
        # Create numeric feature array
        X_numeric = df[numeric_features].fillna(0).values if numeric_features else np.zeros((len(df), 0))
        
        # Combine features
        X = np.hstack([X_text, X_numeric])
        
        # Store feature names for later interpretation
        text_feature_names = self.text_vectorizer.get_feature_names_out()
        self.feature_names = list(text_feature_names) + numeric_features
        
        return X, y
    
    def _extract_numeric_rating(self, rating):
        """Extract numeric rating from various formats"""
        if pd.isna(rating):
            return 0
        
        if isinstance(rating, (int, float)):
            return float(rating)
        
        # If it's a string, try to extract numeric part
        if isinstance(rating, str):
            # Try to extract first number from string
            numbers = re.findall(r'\d+\.?\d*', rating)
            if numbers:
                return float(numbers[0])
        
        return 0
    
    def classify_properties(self, properties):
        """
        Classify properties as hotels or non-hotels
        
        Args:
            properties: List of property dictionaries
            
        Returns:
            List of properties with classification scores
        """
        self.logger.info(f"Classifying {len(properties)} properties")
        
        if not self.model_trained or self.model is None:
            self.logger.warning("Model not trained, attempting to load pre-trained model")
            self._load_model()
            
            if not self.model_trained:
                self.logger.error("No trained model available")
                # Return original properties with default classification
                for prop in properties:
                    prop["hotel_confidence"] = 0.5
                    prop["is_hotel_prediction"] = True  # Assume all are hotels for safety
                return properties
        
        try:
            # Prepare data for classification
            X = self._prepare_properties_for_classification(properties)
            
            # Get probability predictions
            probabilities = self.model.predict_proba(X)
            
            # Add predictions to properties
            for i, prop in enumerate(properties):
                # Get probability of being a hotel (class 1)
                hotel_probability = probabilities[i][1]
                prop["hotel_confidence"] = float(hotel_probability)
                prop["is_hotel_prediction"] = hotel_probability >= 0.5
                
                # Add feature contributions for interpretability
                if hasattr(self.model, 'feature_importances_'):
                    top_features = self._get_top_feature_contributions(X[i], self.model.feature_importances_)
                    prop["top_features"] = top_features
            
            self.logger.info(f"Classified {len(properties)} properties")
            
            return properties
            
        except Exception as e:
            self.logger.error(f"Error classifying properties: {str(e)}")
            # Return original properties with default classification
            for prop in properties:
                prop["hotel_confidence"] = 0.5
                prop["is_hotel_prediction"] = True  # Assume all are hotels for safety
            return properties
    
    def _prepare_properties_for_classification(self, properties):
        """
        Prepare properties for classification
        
        Args:
            properties: List of property dictionaries
            
        Returns:
            Feature array for classification
        """
        # Convert to DataFrame for easier processing
        df = pd.DataFrame(properties)
        
        # Process text features (name, description, etc.)
        text_features = []
        
        # Process name
        if 'name' in df.columns:
            df['name'] = df['name'].fillna('').astype(str)
            text_features.append('name')
        
        # Process description if available
        if 'description' in df.columns:
            df['description'] = df['description'].fillna('').astype(str)
            text_features.append('description')
        
        # Process address if available
        if 'address' in df.columns:
            df['address'] = df['address'].fillna('').astype(str)
            text_features.append('address')
        
        # Process reviews if available
        if 'reviews' in df.columns:
            df['reviews_text'] = df['reviews'].apply(lambda x: ' '.join(x) if isinstance(x, list) else '')
            text_features.append('reviews_text')
        
        # Combine text features
        df['combined_text'] = ''
        for col in text_features:
            df['combined_text'] += ' ' + df[col]
        
        # Vectorize text using the trained vectorizer
        if self.text_vectorizer:
            X_text = self.text_vectorizer.transform(df['combined_text']).toarray()
        else:
            self.logger.warning("No text vectorizer available, using empty text features")
            X_text = np.zeros((len(df), 0))
        
        # Process numeric features
        numeric_features = []
        
        # Extract ratings if available
        if 'rating' in df.columns:
            df['rating_numeric'] = df['rating'].apply(self._extract_numeric_rating)
            numeric_features.append('rating_numeric')
        
        # Extract price indicators
        if 'price' in df.columns:
            df['has_price'] = df['price'].notna() & (df['price'] != '')
            numeric_features.append('has_price')
        
        # Extract category indicators
        if 'category' in df.columns:
            df['hotel_in_category'] = df['category'].fillna('').str.lower().apply(
                lambda x: 1 if re.search(r'hotel|accommodation|lodging|guest.house|motel|inn|rental|furnished', x) else 0
            )
            numeric_features.append('hotel_in_category')
        
        # Extract website indicators
        if 'website' in df.columns:
            df['has_website'] = df['website'].notna() & (df['website'] != '')
            numeric_features.append('has_website')
        
        # Process name length as feature
        if 'name' in df.columns:
            df['name_length'] = df['name'].str.len()
            numeric_features.append('name_length')
        
        # Create numeric feature array
        X_numeric = df[numeric_features].fillna(0).values if numeric_features else np.zeros((len(df), 0))
        
        # Ensure we have the right number of numeric features
        if self.feature_names and numeric_features:
            expected_num_numeric = len(self.feature_names) - X_text.shape[1]
            if X_numeric.shape[1] != expected_num_numeric:
                # Pad with zeros if we don't have all expected features
                X_numeric_padded = np.zeros((len(df), expected_num_numeric))
                X_numeric_padded[:, :X_numeric.shape[1]] = X_numeric
                X_numeric = X_numeric_padded
        
        # Combine features
        X = np.hstack([X_text, X_numeric])
        
        return X
    
    def _get_top_feature_contributions(self, feature_vector, feature_importances, top_n=5):
        """
        Get top feature contributions for a single prediction
        
        Args:
            feature_vector: Feature vector for a single instance
            feature_importances: Feature importance values from the model
            top_n: Number of top features to return
            
        Returns:
            List of (feature_name, contribution) tuples
        """
        # Calculate contribution for each feature
        contributions = feature_vector * feature_importances
        
        # Get indices of top contributions
        top_indices = np.argsort(contributions)[-top_n:][::-1]
        
        # Create list of (feature_name, contribution) tuples
        top_features = []
        for idx in top_indices:
            if idx < len(self.feature_names):
                feature_name = self.feature_names[idx]
                contribution = float(contributions[idx])
                top_features.append((feature_name, contribution))
        
        return top_features
    
    def save_model(self, model_path=None):
        """
        Save the trained model to disk
        
        Args:
            model_path: Path to save the model, if None uses default path
            
        Returns:
            Dict with save results
        """
        if not self.model_trained or self.model is None:
            self.logger.error("No trained model to save")
            return {"error": "No trained model to save"}
        
        try:
            # Use default path if not provided
            if model_path is None:
                model_dir = os.path.join(os.getcwd(), "models")
                os.makedirs(model_dir, exist_ok=True)
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                model_path = os.path.join(model_dir, f"hotel_classifier_{timestamp}.pkl")
            
            # Create model data dictionary
            model_data = {
                "model": self.model,
                "text_vectorizer": self.text_vectorizer,
                "feature_names": self.feature_names,
                "timestamp": datetime.now().isoformat()
            }
            
            # Save model to disk
            with open(model_path, 'wb') as f:
                pickle.dump(model_data, f)
            
            self.logger.info(f"Model saved to {model_path}")
            
            return {
                "status": "success",
                "model_path": model_path,
                "timestamp": datetime.now().isoformat()
            }
            
        except Exception as e:
            self.logger.error(f"Error saving model: {str(e)}")
            return {"error": f"Error saving model: {str(e)}"}
    
    def _load_model(self, model_path=None):
        """
        Load a pre-trained model from disk
        
        Args:
            model_path: Path to the model file, if None tries to find the most recent model
            
        Returns:
            Boolean indicating if model was loaded successfully
        """
        try:
            # If no path provided, try to find the most recent model
            if model_path is None:
                model_dir = os.path.join(os.getcwd(), "models")
                
                if not os.path.exists(model_dir):
                    self.logger.error(f"Model directory {model_dir} does not exist")
                    return False
                
                # Find all model files
                model_files = [f for f in os.listdir(model_dir) if f.startswith("hotel_classifier_") and f.endswith(".pkl")]
                
                if not model_files:
                    self.logger.error("No model files found")
                    return False
                
                # Sort by timestamp (newest first)
                model_files.sort(reverse=True)
                
                # Use the most recent model
                model_path = os.path.join(model_dir, model_files[0])
            
            # Load model from disk
            with open(model_path, 'rb') as f:
                model_data = pickle.load(f)
            
            # Extract model components
            self.model = model_data["model"]
            self.text_vectorizer = model_data["text_vectorizer"]
            self.feature_names = model_data["feature_names"]
            
            self.model_trained = True
            self.logger.info(f"Model loaded from {model_path}")
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error loading model: {str(e)}")
            return False