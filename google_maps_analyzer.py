# web_discovery/google_maps_analyzer.py
import json
import time
import random
import logging
import re
from datetime import datetime, timedelta
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.cluster import DBSCAN

class GoogleMapsAnalyzer:
    """Advanced Google Maps data extraction and analysis for hotel discovery"""
    
    def __init__(self, db=None, config=None):
        """Initialize the Google Maps analyzer"""
        self.db = db
        self.config = config
        self.logger = logging.getLogger("google_maps_analyzer")
        self.driver = None
        self.hotel_indicators = self._initialize_hotel_indicators()
        self.non_hotel_false_positives = self._initialize_false_positives()
        
    def _initialize_hotel_indicators(self):
        """Initialize phrases and patterns that indicate hotel operations"""
        return {
            'review_phrases': [
                'الغرفة', 'الغرف', 'النزيل', 'النزلاء', 'الاستقبال', 'المفتاح', 
                'السرير', 'الأسرة', 'الفندق', 'الإقامة', 'الحجز', 'شقة', 'مفروشة',
                'يومي', 'الليلة', 'ليالي', 'استأجرت', 'نزلت', 'الوسائد', 'المناشف', 
                'الشراشف', 'الحمام', 'جناح', 'غرفة نوم', 'الإفطار', 'الخدمة', 'الاستضافة',
                'check in', 'reception', 'room', 'rooms', 'bed', 'beds', 'towels',
                'breakfast', 'stay', 'suite', 'hotel', 'apartment', 'night', 'booking'
            ],
            'name_patterns': [
                r'فندق', r'شقق', r'أجنحة', r'نزل', r'غرف', r'للإيجار اليومي', r'مفروشة',
                r'hotel', r'apartments', r'suites', r'rooms', r'inn', r'furnished', r'daily'
            ],
            'amenities': [
                'مواقف سيارات', 'واي فاي', 'تكييف', 'خدمة الغرف', 'استقبال 24 ساعة',
                'parking', 'wifi', 'air conditioning', '24-hour', 'room service'
            ],
            'place_categories': [
                'فندق', 'شقق مفروشة', 'سكن', 'إقامة', 'شقق فندقية',
                'hotel', 'lodging', 'accommodation', 'apartment'
            ]
        }
        
    def _initialize_false_positives(self):
        """Initialize phrases and patterns that might falsely indicate hotels"""
        return {
            'categories': [
                'مطعم', 'مقهى', 'متجر', 'مستشفى', 'عيادة', 'مكتب', 'مدرسة',
                'restaurant', 'cafe', 'store', 'hospital', 'clinic', 'office', 'school'
            ],
            'review_phrases': [
                'طلبت', 'الطعام', 'الوجبة', 'المنيو', 'الأكل', 'تسوق', 'اشتريت',
                'ordered', 'food', 'meal', 'menu', 'purchased', 'bought', 'shopping'
            ]
        }
    
    def initialize_webdriver(self):
        """Initialize the Selenium WebDriver with proper configuration"""
        options = webdriver.ChromeOptions()
        options.add_argument("--headless")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-gpu")
        options.add_argument("--window-size=1920,1080")
        options.add_argument("--lang=ar")
        
        # Add user agent to avoid detection
        user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36"
        options.add_argument(f'user-agent={user_agent}')
        
        self.driver = webdriver.Chrome(options=options)
        self.driver.implicitly_wait(10)
        self.logger.info("WebDriver initialized successfully")
        
    def close_webdriver(self):
        """Close the WebDriver"""
        if self.driver:
            self.driver.quit()
            self.driver = None
            self.logger.info("WebDriver closed")
            
    def discover_hotels_in_area(self, location, radius=2000, max_results=100):
        """
        Discover potential hotels in the specified area using Google Maps
        
        Args:
            location: Location name or coordinates
            radius: Search radius in meters
            max_results: Maximum number of results to return
            
        Returns:
            List of discovered potential hotel properties
        """
        self.logger.info(f"Starting hotel discovery in {location} with radius {radius}m")
        
        try:
            if not self.driver:
                self.initialize_webdriver()
                
            # First search for known hotels to establish baseline
            official_hotels = self._search_gmaps(location, "فندق", radius)
            official_hotels += self._search_gmaps(location, "hotel", radius)
            
            self.logger.info(f"Found {len(official_hotels)} official hotels")
            
            # Now search for other types of accommodations
            accommodations = []
            search_terms = [
                "شقق مفروشة", "أجنحة فندقية", "شقق للإيجار", "غرف للإيجار",
                "نزل", "شقق مخدومة", "إيجار يومي", "furnished apartments",
                "suites", "rooms for rent", "daily rental", "serviced apartments"
            ]
            
            for term in search_terms:
                results = self._search_gmaps(location, term, radius)
                accommodations.extend(results)
                # Add random delay to avoid being blocked
                time.sleep(random.uniform(2, 5))
                
            self.logger.info(f"Found {len(accommodations)} additional accommodations")
            
            # Now search for buildings and other places that might be hotels
            buildings = self._search_gmaps(location, "مبنى", radius)
            buildings += self._search_gmaps(location, "building", radius)
            
            # Identify potential hotels among buildings
            potential_hotel_buildings = []
            for building in buildings:
                if self._check_if_potential_hotel(building):
                    potential_hotel_buildings.append(building)
            
            self.logger.info(f"Found {len(potential_hotel_buildings)} potential hotel buildings")
            
            # Combine all results and remove duplicates
            all_results = official_hotels + accommodations + potential_hotel_buildings
            unique_results = self._remove_duplicates(all_results)
            
            # Enrich data with more details
            enriched_results = []
            for place in unique_results[:max_results]:
                place_details = self._get_place_details(place['place_id'])
                if place_details:
                    # Combine the original data with the detailed information
                    combined_data = {**place, **place_details}
                    enriched_results.append(combined_data)
                    
            self.logger.info(f"Returning {len(enriched_results)} enriched results")
            return enriched_results
            
        except Exception as e:
            self.logger.error(f"Error in discover_hotels_in_area: {str(e)}")
            return []
        finally:
            # Don't close the driver here, as we might reuse it
            pass
    
    def _search_gmaps(self, location, search_term, radius):
        """
        Search Google Maps for the specified term
        
        Args:
            location: Location name or coordinates
            search_term: Term to search for
            radius: Search radius in meters
            
        Returns:
            List of places matching the search criteria
        """
        try:
            # Format the Google Maps search URL
            if isinstance(location, tuple) and len(location) == 2:
                # If location is provided as coordinates
                lat, lng = location
                search_url = f"https://www.google.com/maps/search/{search_term}/@{lat},{lng},{14}z"
            else:
                # If location is provided as a name
                search_url = f"https://www.google.com/maps/search/{search_term}+{location}"
                
            self.driver.get(search_url)
            
            # Wait for results to load
            WebDriverWait(self.driver, 15).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "div[role='feed']"))
            )
            
            # Scroll to load more results
            feed_element = self.driver.find_element(By.CSS_SELECTOR, "div[role='feed']")
            last_height = self.driver.execute_script("return arguments[0].scrollHeight", feed_element)
            
            results_count = 0
            max_scrolls = 10
            scroll_count = 0
            
            while scroll_count < max_scrolls:
                # Scroll down
                self.driver.execute_script("arguments[0].scrollTo(0, arguments[0].scrollHeight)", feed_element)
                time.sleep(2)  # Wait for new results to load
                
                # Calculate new scroll height and compare with last scroll height
                new_height = self.driver.execute_script("return arguments[0].scrollHeight", feed_element)
                current_results = len(self.driver.find_elements(By.CSS_SELECTOR, "div[role='feed'] > div > div > a"))
                
                if new_height == last_height or current_results == results_count:
                    # If heights are the same or no new results, we've reached the end
                    break
                
                last_height = new_height
                results_count = current_results
                scroll_count += 1
                
            # Extract place data
            place_elements = self.driver.find_elements(By.CSS_SELECTOR, "div[role='feed'] > div > div > a")
            places = []
            
            for element in place_elements:
                try:
                    name = element.find_element(By.CSS_SELECTOR, "div[role='heading']").text
                    
                    # Get place details and ratings if available
                    rating_element = element.find_elements(By.CSS_SELECTOR, "span[role='img']")
                    rating = rating_element[0].get_attribute("aria-label").split()[0] if rating_element else None
                    
                    # Get number of reviews
                    reviews_element = element.find_elements(By.CSS_SELECTOR, "span:nth-child(3)")
                    reviews = reviews_element[0].text.strip("()") if reviews_element else None
                    
                    # Get category
                    category_element = element.find_elements(By.CSS_SELECTOR, "div:nth-child(2) > div:nth-child(2)")
                    category = category_element[0].text if category_element else None
                    
                    # Get place ID from the URL
                    url = element.get_attribute("href")
                    place_id = None
                    if "placeid" in url:
                        place_id = url.split("placeid=")[1].split("&")[0]
                    
                    # Get coordinates from the URL if available
                    lat, lng = None, None
                    if "@" in url:
                        coords_part = url.split("@")[1].split(",")
                        if len(coords_part) >= 2:
                            try:
                                lat = float(coords_part[0])
                                lng = float(coords_part[1])
                            except:
                                pass
                    
                    place_data = {
                        "name": name,
                        "rating": rating,
                        "reviews_count": reviews,
                        "category": category,
                        "url": url,
                        "place_id": place_id,
                        "latitude": lat,
                        "longitude": lng,
                        "source": "google_maps",
                        "search_term": search_term
                    }
                    
                    places.append(place_data)
                except Exception as e:
                    self.logger.warning(f"Error extracting place data: {str(e)}")
                    continue
            
            self.logger.info(f"Found {len(places)} places for search term '{search_term}'")
            return places
            
        except Exception as e:
            self.logger.error(f"Error in _search_gmaps: {str(e)}")
            return []
    
    def _get_place_details(self, place_id):
        """
        Get detailed information about a place using its place_id
        
        Args:
            place_id: Google Maps place ID
            
        Returns:
            Dict with detailed place information
        """
        try:
            # Navigate to the place page
            self.driver.get(f"https://www.google.com/maps/place/?q=place_id:{place_id}")
            
            # Wait for the page to load
            WebDriverWait(self.driver, 15).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "h1.fontHeadlineLarge"))
            )
            
            # Extract detailed information
            details = {}
            
            # Get name
            name_element = self.driver.find_elements(By.CSS_SELECTOR, "h1.fontHeadlineLarge")
            if name_element:
                details["name"] = name_element[0].text
            
            # Get category
            category_element = self.driver.find_elements(By.CSS_SELECTOR, "button[jsaction='pane.rating.category']")
            if category_element:
                details["detailed_category"] = category_element[0].text
            
            # Get address
            address_element = self.driver.find_elements(By.CSS_SELECTOR, "button[data-item-id='address']")
            if address_element:
                details["address"] = address_element[0].text
            
            # Get phone number
            phone_element = self.driver.find_elements(By.CSS_SELECTOR, "button[data-item-id='phone:tel']")
            if phone_element:
                details["phone"] = phone_element[0].text
            
            # Get website
            website_element = self.driver.find_elements(By.CSS_SELECTOR, "a[data-item-id='authority']")
            if website_element:
                details["website"] = website_element[0].get_attribute("href")
            
            # Get opening hours
            hours_element = self.driver.find_elements(By.CSS_SELECTOR, "div[aria-label^='Hours']")
            if hours_element:
                hours_text = hours_element[0].text
                if "24" in hours_text:
                    details["hours_24"] = True
                details["hours"] = hours_text
            
            # Get reviews if available
            try:
                # Click on reviews tab
                reviews_tab = self.driver.find_element(By.CSS_SELECTOR, "button[jsaction='pane.rating.moreReviews']")
                reviews_tab.click()
                
                # Wait for reviews to load
                WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "div[jsaction='pane.review.read']"))
                )
                
                # Scroll to load more reviews
                for _ in range(3):  # Scroll a few times to load more reviews
                    self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                    time.sleep(1.5)
                
                # Extract reviews
                review_elements = self.driver.find_elements(By.CSS_SELECTOR, "div[jsaction='pane.review.read']")
                reviews = []
                
                for review in review_elements[:10]:  # Limit to first 10 reviews
                    review_text = review.find_element(By.CSS_SELECTOR, "span[jsaction='pane.review.expandReview']").text
                    reviews.append(review_text)
                
                details["reviews"] = reviews
                
                # Calculate hotel indicator score based on reviews
                hotel_score = self._calculate_hotel_indicator_score(reviews)
                details["hotel_indicator_score"] = hotel_score
                
            except Exception as e:
                self.logger.info(f"Could not extract reviews: {str(e)}")
            
            # Get photos count
            photos_element = self.driver.find_elements(By.CSS_SELECTOR, "button[jsaction='pane.heroHeader.photos']")
            if photos_element:
                photos_text = photos_element[0].text
                try:
                    details["photos_count"] = int(re.search(r'\d+', photos_text).group())
                except:
                    pass
            
            # Get other amenities/services
            amenities_elements = self.driver.find_elements(By.CSS_SELECTOR, "div[jsaction='pane.attributes.expand']")
            if amenities_elements:
                amenities = amenities_elements[0].text.split('\n')
                details["amenities"] = amenities
            
            # Check for hotel-specific elements in the page content
            page_text = self.driver.page_source
            
            # Look for indicators of hotel operations in the page content
            hotel_phrases = ['reception', 'check-in', 'check-out', 'room service', 
                            'الاستقبال', 'تسجيل الوصول', 'تسجيل المغادرة', 'خدمة الغرف']
            
            for phrase in hotel_phrases:
                if phrase.lower() in page_text.lower():
                    if "hotel_indicators" not in details:
                        details["hotel_indicators"] = []
                    details["hotel_indicators"].append(phrase)
            
            return details
            
        except Exception as e:
            self.logger.error(f"Error in _get_place_details: {str(e)}")
            return {}
    
    def _calculate_hotel_indicator_score(self, reviews):
        """
        Calculate a score indicating likelihood that the place is a hotel based on review text
        
        Args:
            reviews: List of review texts
            
        Returns:
            Float score between 0-1 indicating hotel likelihood
        """
        if not reviews:
            return 0.0
        
        # Combine all reviews into one text
        combined_text = " ".join(reviews).lower()
        
        # Count occurrences of hotel indicator phrases
        indicator_count = 0
        for phrase in self.hotel_indicators['review_phrases']:
            if phrase.lower() in combined_text:
                indicator_count += combined_text.count(phrase.lower())
        
        # Count occurrences of false positive phrases
        false_positive_count = 0
        for phrase in self.non_hotel_false_positives['review_phrases']:
            if phrase.lower() in combined_text:
                false_positive_count += combined_text.count(phrase.lower())
        
        # Calculate score based on indicators and text length
        text_length = len(combined_text.split())
        indicator_ratio = indicator_count / max(1, text_length) * 100
        false_positive_ratio = false_positive_count / max(1, text_length) * 100
        
        # Adjust score: increase for indicators, decrease for false positives
        score = min(1.0, max(0.0, (indicator_ratio - false_positive_ratio) / 10))
        
        return score
    
    def _check_if_potential_hotel(self, place_data):
        """
        Check if a place might be an unlicensed hotel based on available data
        
        Args:
            place_data: Place data dictionary
            
        Returns:
            Boolean indicating if the place is potentially a hotel
        """
        # If already categorized as a hotel or accommodation, return True
        if place_data.get("category"):
            category = place_data["category"].lower()
            for hotel_category in self.hotel_indicators['place_categories']:
                if hotel_category.lower() in category:
                    return True
        
        # Check name for hotel-related terms
        if place_data.get("name"):
            name = place_data["name"].lower()
            for pattern in self.hotel_indicators['name_patterns']:
                if re.search(pattern.lower(), name):
                    return True
        
        # For buildings and other uncategorized places, we need more investigation
        return False
    
    def _remove_duplicates(self, places):
        """
        Remove duplicate places from the results
        
        Args:
            places: List of place dictionaries
            
        Returns:
            Deduplicated list of places
        """
        unique_places = []
        seen_ids = set()
        seen_coords = set()
        
        for place in places:
            # If we have a place_id, use that for deduplication
            if place.get("place_id") and place["place_id"] not in seen_ids:
                seen_ids.add(place["place_id"])
                unique_places.append(place)
                continue
            
            # If we have coordinates, check proximity
            if place.get("latitude") and place.get("longitude"):
                is_duplicate = False
                coords = (place["latitude"], place["longitude"])
                
                for seen_coord in seen_coords:
                    # If coordinates are very close (within 50 meters), consider it a duplicate
                    distance = self._haversine_distance(coords[0], coords[1], seen_coord[0], seen_coord[1])
                    if distance < 0.05:  # 50 meters in km
                        is_duplicate = True
                        break
                
                if not is_duplicate:
                    seen_coords.add(coords)
                    unique_places.append(place)
                    continue
            
            # If we don't have place_id or coordinates, use name for deduplication
            if place.get("name"):
                name_exists = False
                for existing_place in unique_places:
                    if existing_place.get("name") and self._calculate_string_similarity(place["name"], existing_place["name"]) > 0.8:
                        name_exists = True
                        break
                
                if not name_exists:
                    unique_places.append(place)
        
        return unique_places
    
    def _haversine_distance(self, lat1, lon1, lat2, lon2):
        """Calculate the great circle distance between two points in kilometers"""
        # Convert decimal degrees to radians
        lat1, lon1, lat2, lon2 = map(np.radians, [lat1, lon1, lat2, lon2])
        
        # Haversine formula
        dlon = lon2 - lon1
        dlat = lat2 - lat1
        a = np.sin(dlat/2)**2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon/2)**2
        c = 2 * np.arcsin(np.sqrt(a))
        r = 6371  # Radius of Earth in kilometers
        
        return c * r
    
    def _calculate_string_similarity(self, str1, str2):
        """Calculate similarity between two strings using Levenshtein distance"""
        if not str1 or not str2:
            return 0
            
        # Convert to lowercase
        s1 = str1.lower()
        s2 = str2.lower()
        
        # Simple Levenshtein implementation
        rows = len(s1) + 1
        cols = len(s2) + 1
        dist = [[0 for x in range(cols)] for x in range(rows)]
        
        for i in range(rows):
            dist[i][0] = i
            
        for i in range(cols):
            dist[0][i] = i
            
        for i in range(1, rows):
            for j in range(1, cols):
                cost = 0 if s1[i-1] == s2[j-1] else 1
                dist[i][j] = min(
                    dist[i-1][j] + 1,      # deletion
                    dist[i][j-1] + 1,      # insertion
                    dist[i-1][j-1] + cost  # substitution
                )
                
        max_len = max(len(s1), len(s2))
        similarity = 1 - (dist[rows-1][cols-1] / max_len) if max_len > 0 else 0
        return similarity
        
    def analyze_reviews_for_hotel_indicators(self, reviews):
        """
        Analyze reviews to identify phrases indicating hotel operations
        
        Args:
            reviews: List of review texts
            
        Returns:
            Dictionary with analysis results
        """
        if not reviews:
            return {"score": 0, "indicators": []}
        
        combined_text = " ".join(reviews).lower()
        found_indicators = []
        
        for phrase in self.hotel_indicators['review_phrases']:
            if phrase.lower() in combined_text:
                occurrences = combined_text.count(phrase.lower())
                found_indicators.append({"phrase": phrase, "occurrences": occurrences})
        
        # Sort indicators by occurrence count
        found_indicators.sort(key=lambda x: x["occurrences"], reverse=True)
        
        # Calculate overall score
        total_indicators = sum(indicator["occurrences"] for indicator in found_indicators)
        text_length = len(combined_text.split())
        score = min(1.0, total_indicators / max(1, text_length/10))
        
        return {
            "score": score,
            "indicators": found_indicators[:10]  # Return top 10 indicators
        }
    
    def cluster_locations_by_density(self, locations, eps=0.1, min_samples=3):
        """
        Cluster locations to identify areas with high density of potential hotels
        
        Args:
            locations: List of (latitude, longitude) tuples
            eps: Maximum distance between two samples for them to be in the same cluster
            min_samples: Minimum number of samples in a cluster
            
        Returns:
            Dictionary with cluster information
        """
        if len(locations) < min_samples:
            return {"clusters": [], "noise": locations}
        
        # Convert locations to numpy array
        X = np.array(locations)
        
        # Apply DBSCAN clustering
        db = DBSCAN(eps=eps, min_samples=min_samples, metric=self._haversine_distance_for_clustering)
        db.fit(X)
        
        labels = db.labels_
        n_clusters = len(set(labels)) - (1 if -1 in labels else 0)
        
        # Prepare results
        clusters = []
        noise_points = []
        
        for i in range(n_clusters):
            cluster_points = X[labels == i]
            
            # Calculate cluster center
            center_lat = np.mean(cluster_points[:, 0])
            center_lng = np.mean(cluster_points[:, 1])
            
            clusters.append({
                "center": (center_lat, center_lng),
                "points": cluster_points.tolist(),
                "count": len(cluster_points)
            })
        
        # Collect noise points
        noise_points = X[labels == -1].tolist()
        
        return {
            "clusters": clusters,
            "noise": noise_points,
            "total_clusters": n_clusters
        }
    
    def _haversine_distance_for_clustering(self, p1, p2):
        """Haversine distance for DBSCAN clustering"""
        return self._haversine_distance(p1[0], p1[1], p2[0], p2[1])
    
    # Advanced methods for analyzing Google Maps data
    def discover_hotels_from_search_history(self, user_location):
        """
        Discover hotels by analyzing search patterns in Google Maps based on user location
        """
        pass
        
    def detect_hotel_from_street_view(self, location):
        """
        Analyze Street View images to detect visual indicators of hotels
        """
        pass
        
    def find_missing_category_hotels(self, region):
        """
        Find places without proper categorization that might be hotels
        """
        pass