# discovery_integration.py

class DiscoveryIntegration:
    """Integrated discovery system that combines multiple discovery methods"""
    
    def __init__(self, db=None, config=None):
        self.db = db
        self.config = config
        self.logger = logging.getLogger("discovery_integration")
        
        # Initialize components as needed
        self.components_initialized = False
    
    def initialize_components(self):
        """Initialize all discovery components"""
        if self.components_initialized:
            return
            
        try:
            # Import and initialize components
            from web_discovery.google_maps_analyzer import GoogleMapsAnalyzer
            from web_discovery.booking_platforms_discovery import BookingPlatformsDiscovery
            from location_analysis.smart_location_analyzer import SmartLocationAnalyzer
            from machine_learning.hotel_classifier import HotelClassifier
            from analysis.pattern_discovery import PatternDiscovery
            from reviews_analysis.reviews_analyzer import ReviewsAnalyzer
            
            self.google_maps_analyzer = GoogleMapsAnalyzer(self.db, self.config)
            self.booking_platforms_discovery = BookingPlatformsDiscovery(self.db, self.config)
            self.smart_location_analyzer = SmartLocationAnalyzer(self.db, self.config)
            self.hotel_classifier = HotelClassifier(self.db, self.config)
            self.pattern_discovery = PatternDiscovery(self.db, self.config)
            self.reviews_analyzer = ReviewsAnalyzer(self.db, self.config)
            
            self.components_initialized = True
            self.logger.info("All discovery components initialized successfully")
            
        except Exception as e:
            self.logger.error(f"Error initializing discovery components: {str(e)}")
            raise
    
    def comprehensive_search(self, location, radius=5000, options=None):
        """
        Perform a comprehensive search using all available methods
        
        Args:
            location: Location name or coordinates
            radius: Search radius in meters
            options: Dictionary of search options
            
        Returns:
            Dict with comprehensive search results
        """
        # Initialize components
        self.initialize_components()
        
        # Set default options
        if options is None:
            options = {
                "use_google_maps": True,
                "use_booking_platforms": True,
                "use_location_analysis": True,
                "use_ml_classification": True,
                "analyze_reviews": True,
                "find_patterns": True
            }
        
        self.logger.info(f"Starting comprehensive search for location: {location}")
        
        all_results = []
        
        # Step 1: Google Maps discovery
        if options.get("use_google_maps", True):
            try:
                google_results = self.google_maps_analyzer.discover_hotels_in_area(location, radius)
                all_results.extend(google_results)
                self.logger.info(f"Found {len(google_results)} results from Google Maps")
            except Exception as e:
                self.logger.error(f"Error in Google Maps discovery: {str(e)}")
        
        # Step 2: Booking platforms discovery
        if options.get("use_booking_platforms", True):
            try:
                booking_results = self.booking_platforms_discovery.discover_hotels_in_area(location)
                all_results.extend(booking_results)
                self.logger.info(f"Found {len(booking_results)} results from booking platforms")
            except Exception as e:
                self.logger.error(f"Error in booking platforms discovery: {str(e)}")
        
        # Step 3: Smart location analysis
        if options.get("use_location_analysis", True):
            try:
                # Commercial areas analysis
                commercial_results = self.smart_location_analyzer.analyze_commercial_areas(location, radius)
                all_results.extend(commercial_results)
                
                # Tourist areas analysis
                tourist_results = self.smart_location_analyzer.analyze_tourist_areas(location, radius)
                all_results.extend(tourist_results)
                
                self.logger.info(f"Found {len(commercial_results) + len(tourist_results)} results from location analysis")
            except Exception as e:
                self.logger.error(f"Error in location analysis: {str(e)}")
        
        # Remove duplicates
        unique_results = self._remove_duplicates(all_results)
        self.logger.info(f"Found {len(unique_results)} unique results after deduplication")
        
        # Step 4: ML classification
        if options.get("use_ml_classification", True) and unique_results:
            try:
                classified_results = self.hotel_classifier.classify_properties(unique_results)
                self.logger.info("Properties classified using ML model")
            except Exception as e:
                self.logger.error(f"Error in ML classification: {str(e)}")
                classified_results = unique_results
        else:
            classified_results = unique_results
        
        # Step 5: Reviews analysis
        if options.get("analyze_reviews", True) and classified_results:
            try:
                # Identify hidden hotels through reviews
                hidden_hotels = self.reviews_analyzer.identify_hidden_hotels(classified_results)
                self.logger.info(f"Found {len(hidden_hotels)} hidden hotels through review analysis")
                
                # Update classification for hidden hotels
                for hotel in classified_results:
                    for hidden in hidden_hotels:
                        if self._is_same_place(hotel, hidden):
                            hotel["is_hotel_prediction"] = True
                            hotel["hotel_confidence"] = hidden["review_analysis"].get("confidence", 0.5)
                            hotel["review_indicators"] = hidden["review_analysis"].get("hotel_indicators", [])
            except Exception as e:
                self.logger.error(f"Error in reviews analysis: {str(e)}")
        
        # Step 6: Pattern discovery
        pattern_results = None
        if options.get("find_patterns", True) and len(classified_results) >= 2:
            try:
                pattern_results = self.pattern_discovery.find_property_groups(classified_results)
                self.logger.info(f"Found {pattern_results.get('group_count', 0)} property groups")
            except Exception as e:
                self.logger.error(f"Error in pattern discovery: {str(e)}")
        
        return {
            "status": "success",
            "total_results": len(classified_results),
            "hotels": classified_results,
            "pattern_analysis": pattern_results,
            "search_params": {
                "location": location,
                "radius": radius,
                "options": options
            }
        }
    
    def _remove_duplicates(self, results):
        """Remove duplicate properties from results"""
        unique_results = []
        seen_ids = set()
        seen_coords = {}
        
        for result in results:
            # Check if we have a unique ID
            unique_id = result.get("place_id") or result.get("id") or result.get("property_id")
            
            if unique_id and unique_id in seen_ids:
                continue
            
            if unique_id:
                seen_ids.add(unique_id)
            
            # Check coordinates
            if result.get("latitude") and result.get("longitude"):
                # Round coordinates to 5 decimal places (approx. 1 meter precision)
                lat = round(float(result["latitude"]), 5)
                lng = round(float(result["longitude"]), 5)
                coord_key = f"{lat},{lng}"
                
                if coord_key in seen_coords:
                    # Combine with existing entry
                    existing = seen_coords[coord_key]
                    
                    # Merge source information
                    if "source" in result and "source" in existing:
                        if isinstance(existing["source"], list):
                            if result["source"] not in existing["source"]:
                                existing["source"].append(result["source"])
                        else:
                            existing["source"] = [existing["source"], result["source"]]
                    
                    continue
                
                seen_coords[coord_key] = result
            
            # Check name similarity
            is_duplicate = False
            for existing in unique_results:
                if self._is_same_place(result, existing):
                    is_duplicate = True
                    break
            
            if not is_duplicate:
                unique_results.append(result)
        
        return unique_results
    
    def _is_same_place(self, place1, place2):
        """Check if two places are the same"""
        # Check ID
        if place1.get("place_id") and place1.get("place_id") == place2.get("place_id"):
            return True
        
        # Check coordinates (if available)
        if (place1.get("latitude") and place1.get("longitude") and 
            place2.get("latitude") and place2.get("longitude")):
            try:
                lat1, lng1 = float(place1["latitude"]), float(place1["longitude"])
                lat2, lng2 = float(place2["latitude"]), float(place2["longitude"])
                
                from math import radians, sin, cos, sqrt, atan2
                
                # Haversine formula
                R = 6371  # Earth radius in km
                dLat = radians(lat2 - lat1)
                dLon = radians(lng2 - lng1)
                a = sin(dLat/2)**2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dLon/2)**2
                c = 2 * atan2(sqrt(a), sqrt(1-a))
                distance = R * c * 1000  # Distance in meters
                
                # If distance is less than 50 meters, consider the same place
                if distance < 50:
                    return True
            except:
                pass
        
        # Check name similarity
        if place1.get("name") and place2.get("name"):
            name1 = place1["name"].lower()
            name2 = place2["name"].lower()
            
            # Exact match
            if name1 == name2:
                return True
            
            # Check for high similarity
            if len(name1) > 3 and len(name2) > 3:
                # Simple similarity check - can be enhanced
                if name1 in name2 or name2 in name1:
                    return True
        
        return False