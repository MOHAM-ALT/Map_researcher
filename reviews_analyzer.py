# reviews_analysis/reviews_analyzer.py
import os
import json
import re
import logging
import numpy as np
import pandas as pd
from datetime import datetime
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.cluster import KMeans
from collections import Counter
import nltk
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords

class ReviewsAnalyzer:
    """Class for analyzing reviews to identify hotel operations"""
    
    def __init__(self, db=None, config=None):
        """Initialize the ReviewsAnalyzer class"""
        self.db = db
        self.config = config
        self.logger = logging.getLogger("reviews_analyzer")
        
        # Initialize NLTK resources
        try:
            nltk.download('punkt', quiet=True)
            nltk.download('stopwords', quiet=True)
            self.arabic_stopwords = set(stopwords.words('arabic'))
            self.english_stopwords = set(stopwords.words('english'))
        except:
            self.logger.warning("Failed to download NLTK resources, using limited stopwords")
            self.arabic_stopwords = set()
            self.english_stopwords = {'a', 'an', 'the', 'and', 'or', 'but', 'if', 'then', 'else', 'when', 'at', 'from', 'by', 'for', 'with', 'about', 'in', 'on', 'to'}
        
        # Initialize hotel-related keywords
        self.hotel_keywords = {
            'arabic': {
                'accommodations': [
                    'فندق', 'شقة', 'غرفة', 'جناح', 'سرير', 'نزل', 'إقامة', 'مبيت', 'أجنحة', 'مفروشة', 'سكن'
                ],
                'services': [
                    'استقبال', 'حجز', 'مفتاح', 'غرفة', 'خدمة', 'طلب', 'استعلامات', 'موظف', 'نزيل', 'ضيف',
                    'تنظيف', 'منظف', 'تسجيل الوصول', 'تسجيل المغادرة'
                ],
                'amenities': [
                    'شرشف', 'منشفة', 'مناشف', 'وسادة', 'وسائد', 'سرير', 'أسرة', 'حمام', 'دش', 'مكيف', 'تكييف',
                    'ثلاجة', 'تلفزيون', 'تلفاز', 'واي فاي', 'انترنت', 'مسبح', 'موقف', 'مواقف'
                ],
                'activities': [
                    'إقامة', 'ليلة', 'ليال', 'يوم', 'أيام', 'سهرة', 'إجازة', 'عطلة', 'أسبوع', 'نهاية الأسبوع'
                ]
            },
            'english': {
                'accommodations': [
                    'hotel', 'apartment', 'room', 'suite', 'bed', 'inn', 'lodge', 'stay', 'accommodation',
                    'furnished', 'hostel', 'motel', 'resort'
                ],
                'services': [
                    'reception', 'booking', 'key', 'room', 'service', 'request', 'concierge', 'staff', 'guest',
                    'visitor', 'cleaning', 'cleaner', 'check-in', 'check-out', 'checkout', 'reservation'
                ],
                'amenities': [
                    'sheet', 'towel', 'pillow', 'bed', 'beds', 'bathroom', 'shower', 'ac', 'air conditioning',
                    'fridge', 'tv', 'television', 'wifi', 'internet', 'pool', 'parking'
                ],
                'activities': [
                    'stay', 'night', 'nights', 'day', 'days', 'evening', 'vacation', 'holiday', 'week', 'weekend'
                ]
            }
        }
    
    def analyze_place_reviews(self, place_data):
        """
        Analyze a place's reviews to determine if it's likely a hotel
        
        Args:
            place_data: Place dictionary with reviews
            
        Returns:
            Dict with analysis results
        """
        self.logger.info(f"Analyzing reviews for place: {place_data.get('name', 'Unknown')}")
        
        try:
            # Extract reviews
            reviews = place_data.get('reviews', [])
            
            if not reviews:
                self.logger.warning("No reviews found")
                return {
                    "is_likely_hotel": False,
                    "confidence": 0.0,
                    "hotel_indicators": [],
                    "review_count": 0
                }
            
            # Combine reviews into a single text
            combined_reviews = " ".join(reviews)
            
            # Detect language
            lang = self._detect_language(combined_reviews)
            
            # Extract hotel indicators
            hotel_indicators = self._extract_hotel_indicators(reviews, lang)
            
            # Calculate confidence score
            indicator_count = sum(indicator['count'] for indicator in hotel_indicators)
            total_words = len(combined_reviews.split())
            
            # Normalize score based on review length
            confidence = min(1.0, indicator_count / max(10, total_words/5))
            
            is_likely_hotel = confidence >= 0.4  # Threshold for hotel determination
            
            return {
                "is_likely_hotel": is_likely_hotel,
                "confidence": confidence,
                "hotel_indicators": hotel_indicators,
                "review_count": len(reviews),
                "language": lang,
                "word_count": total_words
            }
            
        except Exception as e:
            self.logger.error(f"Error analyzing reviews: {str(e)}")
            return {
                "error": f"Error analyzing reviews: {str(e)}",
                "is_likely_hotel": False,
                "confidence": 0.0
            }
    
    def _detect_language(self, text):
        """
        Detect if text is primarily Arabic or English
        
        Args:
            text: Text to analyze
            
        Returns:
            String language code ('ar' or 'en')
        """
        # Count Arabic characters
        arabic_pattern = re.compile(r'[\u0600-\u06FF\u0750-\u077F\u08A0-\u08FF\uFB50-\uFDFF\uFE70-\uFEFF]')
        arabic_count = len(arabic_pattern.findall(text))
        
        # Count English characters
        english_pattern = re.compile(r'[a-zA-Z]')
        english_count = len(english_pattern.findall(text))
        
        return 'ar' if arabic_count > english_count else 'en'
    
    def _extract_hotel_indicators(self, reviews, lang):
        """
        Extract hotel indicators from reviews
        
        Args:
            reviews: List of review texts
            lang: Language code ('ar' or 'en')
            
        Returns:
            List of hotel indicators with counts
        """
        # Choose keyword lists based on language
        if lang == 'ar':
            keyword_dict = self.hotel_keywords['arabic']
            stopwords_set = self.arabic_stopwords
        else:
            keyword_dict = self.hotel_keywords['english']
            stopwords_set = self.english_stopwords
        
        # Flatten keyword lists
        all_keywords = []
        for category, keywords in keyword_dict.items():
            all_keywords.extend(keywords)
        
        # Count keyword occurrences
        keyword_counts = Counter()
        
        for review in reviews:
            # Tokenize words
            words = word_tokenize(review.lower()) if lang == 'en' else re.findall(r'\b\w+\b', review)
            
            # Remove stopwords
            words = [word for word in words if word not in stopwords_set]
            
            # Count keywords
            for word in words:
                if word in all_keywords:
                    keyword_counts[word] += 1
        
        # Group by category
        category_indicators = []
        
        for category, keywords in keyword_dict.items():
            category_counts = [(keyword, keyword_counts[keyword]) for keyword in keywords if keyword_counts[keyword] > 0]
            
            if category_counts:
                # Sort by count (descending)
                category_counts.sort(key=lambda x: x[1], reverse=True)
                
                category_indicators.append({
                    "category": category,
                    "keywords": [{"word": word, "count": count} for word, count in category_counts],
                    "count": sum(count for _, count in category_counts)
                })
        
        # Sort categories by total count
        category_indicators.sort(key=lambda x: x["count"], reverse=True)
        
        return category_indicators
    
    def batch_analyze_places(self, places, min_reviews=3):
        """
        Analyze reviews for multiple places to identify hotels
        
        Args:
            places: List of place dictionaries
            min_reviews: Minimum number of reviews required for analysis
            
        Returns:
            Dict with analysis results
        """
        self.logger.info(f"Batch analyzing {len(places)} places")
        
        try:
            # Filter places with sufficient reviews
            places_with_reviews = [p for p in places if p.get('reviews') and len(p['reviews']) >= min_reviews]
            
            self.logger.info(f"Found {len(places_with_reviews)} places with {min_reviews}+ reviews")
            
            if not places_with_reviews:
                return {
                    "status": "warning",
                    "message": f"No places found with {min_reviews}+ reviews",
                    "likely_hotels": [],
                    "total_analyzed": 0
                }
            
            # Analyze each place
            analysis_results = []
            
            for place in places_with_reviews:
                result = self.analyze_place_reviews(place)
                
                # Add place information to result
                result["name"] = place.get("name", "Unknown")
                result["place_id"] = place.get("place_id")
                result["category"] = place.get("category")
                result["address"] = place.get("address")
                result["latitude"] = place.get("latitude")
                result["longitude"] = place.get("longitude")
                
                analysis_results.append(result)
            
            # Sort by confidence (descending)
            analysis_results.sort(key=lambda x: x.get("confidence", 0), reverse=True)
            
            # Filter likely hotels
            likely_hotels = [r for r in analysis_results if r.get("is_likely_hotel")]
            
            self.logger.info(f"Found {len(likely_hotels)} likely hotels out of {len(analysis_results)} analyzed places")
            
            return {
                "status": "success",
                "total_analyzed": len(analysis_results),
                "likely_hotels": likely_hotels,
                "likely_hotels_count": len(likely_hotels)
            }
            
        except Exception as e:
            self.logger.error(f"Error in batch analysis: {str(e)}")
            return {"error": f"Error in batch analysis: {str(e)}"}
    
    def identify_hidden_hotels(self, places):
        """
        Identify places that don't identify as hotels but are likely to be hotels
        
        Args:
            places: List of place dictionaries
            
        Returns:
            List of hidden hotel places
        """
        self.logger.info(f"Identifying hidden hotels among {len(places)} places")
        
        try:
            # Filter out places already categorized as hotels
            non_hotel_places = []
            
            for place in places:
                category = place.get("category", "").lower()
                name = place.get("name", "").lower()
                
                # Check if already identified as hotel in category or name
                hotel_keywords = {'hotel', 'hostel', 'motel', 'inn', 'lodge', 'resort', 'accommodation',
                                 'فندق', 'نزل', 'فنادق', 'أجنحة فندقية', 'شقق فندقية'}
                
                is_categorized_as_hotel = any(keyword in category for keyword in hotel_keywords)
                has_hotel_in_name = any(keyword in name for keyword in hotel_keywords)
                
                if not is_categorized_as_hotel and not has_hotel_in_name:
                    non_hotel_places.append(place)
            
            self.logger.info(f"Found {len(non_hotel_places)} places not explicitly categorized as hotels")
            
            # Analyze reviews for non-hotel places
            hidden_hotels = []
            
            for place in non_hotel_places:
                # Check if place has reviews
                if place.get("reviews") and len(place["reviews"]) >= 3:
                    # Analyze reviews
                    analysis = self.analyze_place_reviews(place)
                    
                    if analysis.get("is_likely_hotel"):
                        # This place is likely a hidden hotel
                        hidden_hotel = place.copy()
                        hidden_hotel["review_analysis"] = analysis
                        hidden_hotels.append(hidden_hotel)
            
            self.logger.info(f"Found {len(hidden_hotels)} hidden hotels")
            
            # Sort by confidence (descending)
            hidden_hotels.sort(key=lambda x: x["review_analysis"].get("confidence", 0), reverse=True)
            
            return hidden_hotels
            
        except Exception as e:
            self.logger.error(f"Error identifying hidden hotels: {str(e)}")
            return []
    
    def analyze_review_clusters(self, places, n_clusters=5):
        """
        Cluster reviews to identify common themes
        
        Args:
            places: List of place dictionaries with reviews
            n_clusters: Number of clusters to identify
            
        Returns:
            Dict with clustering results
        """
        self.logger.info(f"Clustering reviews from {len(places)} places")
        
        try:
            # Extract all reviews
            all_reviews = []
            
            for place in places:
                reviews = place.get('reviews', [])
                
                for review in reviews:
                    all_reviews.append({
                        "text": review,
                        "place_id": place.get("place_id"),
                        "place_name": place.get("name")
                    })
            
            if len(all_reviews) < n_clusters:
                self.logger.warning(f"Not enough reviews for clustering (found {len(all_reviews)})")
                return {"error": "Not enough reviews for clustering"}
            
            # Vectorize reviews
            vectorizer = TfidfVectorizer(max_features=1000, stop_words='english')
            X = vectorizer.fit_transform([r["text"] for r in all_reviews])
            
            # Apply KMeans clustering
            kmeans = KMeans(n_clusters=n_clusters, random_state=42)
            kmeans.fit(X)
            
            # Get labels for each review
            labels = kmeans.labels_
            
            # Get feature names
            feature_names = vectorizer.get_feature_names_out()
            
            # Get top terms for each cluster
            order_centroids = kmeans.cluster_centers_.argsort()[:, ::-1]
            
            # Prepare clusters
            clusters = []
            
            for i in range(n_clusters):
                # Get reviews in this cluster
                cluster_reviews = [all_reviews[j] for j in range(len(all_reviews)) if labels[j] == i]
                
                # Get top terms
                top_terms = [feature_names[ind] for ind in order_centroids[i, :10]]
                
                # Check if this cluster has hotel-related terms
                hotel_terms = {'hotel', 'room', 'bed', 'stay', 'reception', 'check', 'breakfast',
                              'towel', 'shower', 'service', 'clean', 'accommodation', 'night'}
                
                has_hotel_terms = any(term in hotel_terms for term in top_terms)
                
                clusters.append({
                    "cluster_id": i,
                    "review_count": len(cluster_reviews),
                    "top_terms": top_terms,
                    "has_hotel_terms": has_hotel_terms,
                    "sample_reviews": [r["text"] for r in cluster_reviews[:5]],
                    "places": list(set(r["place_name"] for r in cluster_reviews))
                })
            
            # Sort clusters by size (largest first)
            clusters.sort(key=lambda x: x["review_count"], reverse=True)
            
            self.logger.info(f"Created {len(clusters)} review clusters")
            
            return {
                "status": "success",
                "total_reviews": len(all_reviews),
                "clusters": clusters
            }
            
        except Exception as e:
            self.logger.error(f"Error clustering reviews: {str(e)}")
            return {"error": f"Error clustering reviews: {str(e)}"}