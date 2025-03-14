#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Data Discovery Module for Map_researcher 0.4
This module provides functions for discovering and collecting hotel data.
"""

import os
import sys
import time
import json
import logging
import concurrent.futures
from typing import Dict, List, Any, Tuple, Optional
from datetime import datetime, timedelta
import requests
import math

# Initialize logger
logger = logging.getLogger("data_discovery")

class DataDiscovery:
    """Class for hotel discovery and data collection operations"""
    
    def __init__(self, db=None, scraper=None):
        """
        Initialize the data discovery module
        
        Args:
            db: Database instance
            scraper: HotelScraper instance
        """
        self.db = db
        self.scraper = scraper
    
    def discover_by_location(self, location: str, radius: int = 5000, language: str = "en", 
                             data_sources: List[str] = None) -> List[Dict]:
        """
        Comprehensive discovery by location
        
        Args:
            location (str): Location name or coordinates
            radius (int): Search radius in meters
            language (str): Search language (en, ar)
            data_sources (List[str]): List of data sources to use
            
        Returns:
            List[Dict]: List of discovered hotels
        """
        logger.info(f"Starting location discovery for: {location}, radius: {radius}m")
        
        # Set default data sources if not provided
        if data_sources is None:
            data_sources = ["google_places", "openstreetmap", "booking", "tripadvisor", "expedia"]
        
        results = []
        
        # Google Places API (if available and selected)
        if "google_places" in data_sources and self.scraper and hasattr(self.scraper, '_search_google_places'):
            try:
                logger.info("Searching Google Places API")
                google_results = self.scraper._search_google_places(location, radius, language)
                results.extend(google_results)
                logger.info(f"Found {len(google_results)} hotels from Google Places")
            except Exception as e:
                logger.error(f"Error searching Google Places: {e}")
        
        # OpenStreetMap (if selected)
        if "openstreetmap" in data_sources and self.scraper and hasattr(self.scraper, '_search_openstreetmap'):
            try:
                logger.info("Searching OpenStreetMap")
                osm_results = self.scraper._search_openstreetmap(location, radius, language)
                results.extend(osm_results)
                logger.info(f"Found {len(osm_results)} hotels from OpenStreetMap")
            except Exception as e:
                logger.error(f"Error searching OpenStreetMap: {e}")
        
        # Booking.com (if selected)
        if "booking" in data_sources and self.scraper and hasattr(self.scraper, '_search_booking_com'):
            try:
                logger.info("Searching Booking.com")
                booking_results = self.scraper._search_booking_com(location, radius, language)
                results.extend(booking_results)
                logger.info(f"Found {len(booking_results)} hotels from Booking.com")
            except Exception as e:
                logger.error(f"Error searching Booking.com: {e}")
                # If the method doesn't exist yet, log a message
                if not hasattr(self.scraper, '_search_booking_com'):
                    logger.info("Booking.com search not implemented yet")
        
        # TripAdvisor (if selected)
        if "tripadvisor" in data_sources and self.scraper and hasattr(self.scraper, '_search_tripadvisor'):
            try:
                logger.info("Searching TripAdvisor")
                tripadvisor_results = self.scraper._search_tripadvisor(location, radius, language)
                results.extend(tripadvisor_results)
                logger.info(f"Found {len(tripadvisor_results)} hotels from TripAdvisor")
            except Exception as e:
                logger.error(f"Error searching TripAdvisor: {e}")
                # If the method doesn't exist yet, log a message
                if not hasattr(self.scraper, '_search_tripadvisor'):
                    logger.info("TripAdvisor search not implemented yet")
        
        # Expedia (if selected)
        if "expedia" in data_sources and self.scraper and hasattr(self.scraper, '_search_expedia'):
            try:
                logger.info("Searching Expedia")
                expedia_results = self.scraper._search_expedia(location, radius, language)
                results.extend(expedia_results)
                logger.info(f"Found {len(expedia_results)} hotels from Expedia")
            except Exception as e:
                logger.error(f"Error searching Expedia: {e}")
                # If the method doesn't exist yet, log a message
                if not hasattr(self.scraper, '_search_expedia'):
                    logger.info("Expedia search not implemented yet")
        
        # Apply matching algorithm if scraper has it implemented
        if self.scraper and hasattr(self.scraper, 'enhanced_matching_algorithm'):
            try:
                logger.info("Applying enhanced matching algorithm")
                results = self.scraper.enhanced_matching_algorithm(results)
                logger.info(f"After matching: {len(results)} unique hotels")
            except Exception as e:
                logger.error(f"Error applying matching algorithm: {e}")
                # If the method doesn't exist yet, use basic comparison
                logger.info("Using basic comparison to remove duplicates")
                results = self._remove_duplicates(results)
        else:
            # Use basic duplicate removal
            results = self._remove_duplicates(results)
        
        logger.info(f"Discovery complete. Found {len(results)} unique hotels")
        return results
    
    def _remove_duplicates(self, hotels: List[Dict]) -> List[Dict]:
        """
        Simple method to remove duplicate hotels based on name and coordinates
        
        Args:
            hotels (List[Dict]): List of hotels
            
        Returns:
            List[Dict]: List of unique hotels
        """
        unique_hotels = []
        seen_names = set()
        seen_coords = set()
        
        for hotel in hotels:
            name = hotel.get('name', '').lower()
            lat = hotel.get('latitude')
            lng = hotel.get('longitude')
            
            # Skip hotels without name or coordinates
            if not name or not lat or not lng:
                unique_hotels.append(hotel)
                continue
            
            # Check if we have seen this name before
            if name in seen_names:
                continue
            
            # Check if we have seen similar coordinates
            coords_seen = False
            for seen_lat, seen_lng in seen_coords:
                # Calculate distance (approximate)
                dist = self._haversine_distance(lat, lng, seen_lat, seen_lng)
                if dist < 0.05:  # Within 50 meters
                    coords_seen = True
                    break
            
            if not coords_seen:
                seen_names.add(name)
                seen_coords.add((lat, lng))
                unique_hotels.append(hotel)
        
        return unique_hotels
    
    def _haversine_distance(self, lat1: float, lon1: float, lat2: float, lon2: float) -> float:
        """
        Calculate distance between two geographical points using Haversine formula
        
        Args:
            lat1 (float): Latitude of first point
            lon1 (float): Longitude of first point
            lat2 (float): Latitude of second point
            lon2 (float): Longitude of second point
            
        Returns:
            float: Distance in kilometers
        """
        # Convert decimal degrees to radians
        lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])
        
        # Haversine formula
        dlon = lon2 - lon1
        dlat = lat2 - lat1
        a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
        c = 2 * math.asin(math.sqrt(a))
        r = 6371  # Radius of Earth in kilometers
        
        return c * r
    
    def discover_by_name(self, name: str, location: str = None, radius: int = 5000, 
                         language: str = "en", data_sources: List[str] = None) -> List[Dict]:
        """
        Advanced discovery by name and optional location
        
        Args:
            name (str): Hotel name or keywords
            location (str, optional): Location name or coordinates
            radius (int): Search radius in meters (used if location provided)
            language (str): Search language (en, ar)
            data_sources (List[str]): List of data sources to use
            
        Returns:
            List[Dict]: List of discovered hotels
        """
        logger.info(f"Starting name-based discovery for: {name}, location: {location}")
        
        # Set default data sources if not provided
        if data_sources is None:
            data_sources = ["google_places", "openstreetmap", "booking", "tripadvisor", "expedia"]
        
        results = []
        
        # If location is provided, narrow search to that location
        if location:
            # First search by location, then filter by name
            all_hotels = self.discover_by_location(location, radius, language, data_sources)
            
            # Filter results by name
            for hotel in all_hotels:
                hotel_name = hotel.get('name', '').lower()
                if name.lower() in hotel_name:
                    results.append(hotel)
            
            logger.info(f"Found {len(results)} hotels matching name '{name}' in location '{location}'")
        
        else:
            # Direct name search without location constraint
            
            # Google Places API (if available and selected)
            if "google_places" in data_sources and self.scraper and hasattr(self.scraper, '_search_google_places_by_name'):
                try:
                    logger.info("Searching Google Places API by name")
                    google_results = self.scraper._search_google_places_by_name(name, language)
                    results.extend(google_results)
                    logger.info(f"Found {len(google_results)} hotels from Google Places")
                except Exception as e:
                    logger.error(f"Error searching Google Places by name: {e}")
                    # If the method doesn't exist yet, log a message
                    if not hasattr(self.scraper, '_search_google_places_by_name'):
                        logger.info("Google Places search by name not implemented yet")
            
            # OpenStreetMap (if selected)
            if "openstreetmap" in data_sources and self.scraper and hasattr(self.scraper, '_search_openstreetmap_by_name'):
                try:
                    logger.info("Searching OpenStreetMap by name")
                    osm_results = self.scraper._search_openstreetmap_by_name(name, language)
                    results.extend(osm_results)
                    logger.info(f"Found {len(osm_results)} hotels from OpenStreetMap")
                except Exception as e:
                    logger.error(f"Error searching OpenStreetMap by name: {e}")
                    # If the method doesn't exist yet, log a message
                    if not hasattr(self.scraper, '_search_openstreetmap_by_name'):
                        logger.info("OpenStreetMap search by name not implemented yet")
            
            # Add other data sources (Booking.com, TripAdvisor, etc.)
            # Same pattern as above
        
        # Apply matching algorithm if available
        if self.scraper and hasattr(self.scraper, 'enhanced_matching_algorithm'):
            try:
                logger.info("Applying enhanced matching algorithm")
                results = self.scraper.enhanced_matching_algorithm(results)
                logger.info(f"After matching: {len(results)} unique hotels")
            except Exception as e:
                logger.error(f"Error applying matching algorithm: {e}")
                # If the method doesn't exist yet, use basic comparison
                logger.info("Using basic comparison to remove duplicates")
                results = self._remove_duplicates(results)
        else:
            # Use basic duplicate removal
            results = self._remove_duplicates(results)
        
        logger.info(f"Name-based discovery complete. Found {len(results)} unique hotels")
        return results
    
    def smart_discovery(self, start_location: str, max_radius: int = 10000, 
                        max_points: int = 20, stop_criteria: Dict = None) -> List[Dict]:
        """
        Smart discovery with gradual expansion from a point
        
        Args:
            start_location (str): Starting location (name or coordinates)
            max_radius (int): Maximum search radius in kilometers
            max_points (int): Maximum number of search points
            stop_criteria (Dict): Criteria to stop the search
            
        Returns:
            List[Dict]: List of discovered hotels
        """
        logger.info(f"Starting smart discovery from {start_location}, max radius: {max_radius}km")
        
        # Initialize result list and tracking sets
        all_hotels = []
        processed_locations = set()
        search_points = [(start_location, 1000)]  # Start with 1km radius
        
        # Set default stop criteria if not provided
        if stop_criteria is None:
            stop_criteria = {
                "max_hotels": 1000,  # Stop after finding 1000 hotels
                "max_time": 3600,    # Stop after 1 hour
                "coverage_percent": 90  # Stop after covering 90% of the area
            }
        
        # Start tracking time
        start_time = time.time()
        
        # Process each point until we run out or meet stop criteria
        while search_points and len(processed_locations) < max_points:
            # Check if we should stop based on time
            if "max_time" in stop_criteria and (time.time() - start_time) > stop_criteria["max_time"]:
                logger.info(f"Smart discovery stopped: reached maximum time limit")
                break
            
            # Check if we should stop based on number of hotels
            if "max_hotels" in stop_criteria and len(all_hotels) >= stop_criteria["max_hotels"]:
                logger.info(f"Smart discovery stopped: found {len(all_hotels)} hotels")
                break
            
            # Get next search point
            current_location, radius = search_points.pop(0)
            
            # Skip if already processed
            if current_location in processed_locations:
                continue
            
            logger.info(f"Searching at {current_location} with radius {radius}m")
            
            # Mark as processed
            processed_locations.add(current_location)
            
            # Search hotels at this location
            try:
                hotels = self.discover_by_location(current_location, radius)
                
                # Add new hotels to results (avoid duplicates)
                new_hotels = [h for h in hotels if not self._is_duplicate_hotel(h, all_hotels)]
                all_hotels.extend(new_hotels)
                
                logger.info(f"Found {len(new_hotels)} new hotels at {current_location}")
                
                # If we found hotels, add new search points based on their locations
                if new_hotels and radius <= max_radius:
                    new_points = self._extract_search_points(new_hotels, processed_locations)
                    
                    # Add new points with increased radius
                    for point in new_points[:min(5, len(new_points))]:  # Limit to 5 new points
                        search_points.append((point, min(radius * 1.5, max_radius)))
            
            except Exception as e:
                logger.error(f"Error during smart discovery at {current_location}: {e}")
        
        logger.info(f"Smart discovery complete. Found {len(all_hotels)} unique hotels from {len(processed_locations)} locations")
        return all_hotels
    
    def _extract_search_points(self, hotels: List[Dict], existing_points: set) -> List[str]:
        """
        Extract new search points from discovered hotels
        
        Args:
            hotels (List[Dict]): List of hotels
            existing_points (set): Set of existing search points
            
        Returns:
            List[str]: List of new search points as coordinate strings
        """
        new_points = []
        locations = set()
        
        for hotel in hotels:
            lat = hotel.get('latitude')
            lng = hotel.get('longitude')
            
            if lat and lng:
                # Create coordinate string
                coord_str = f"{lat},{lng}"
                
                # Check if this is a new location and not too close to existing points
                if coord_str not in existing_points and coord_str not in locations:
                    # Check distance from existing points
                    too_close = False
                    for point in existing_points:
                        try:
                            if ',' in point:
                                p_lat, p_lng = map(float, point.split(','))
                                dist = self._haversine_distance(lat, lng, p_lat, p_lng)
                                if dist < 1.0:  # Within 1 km
                                    too_close = True
                                    break
                        except:
                            pass
                    
                    if not too_close:
                        locations.add(coord_str)
                        new_points.append(coord_str)
        
        return new_points
    
    def _is_duplicate_hotel(self, hotel: Dict, existing_hotels: List[Dict]) -> bool:
        """
        Check if a hotel is a duplicate of any in the existing list
        
        Args:
            hotel (Dict): Hotel data
            existing_hotels (List[Dict]): List of existing hotels
            
        Returns:
            bool: True if duplicate, False otherwise
        """
        # Check by name and approximate location
        hotel_name = hotel.get('name', '').lower()
        lat = hotel.get('latitude')
        lng = hotel.get('longitude')
        
        if not hotel_name or not lat or not lng:
            return False
        
        for existing in existing_hotels:
            # Check name similarity
            existing_name = existing.get('name', '').lower()
            if hotel_name == existing_name:
                return True
            
            # Check location proximity
            existing_lat = existing.get('latitude')
            existing_lng = existing.get('longitude')
            
            if existing_lat and existing_lng:
                try:
                    dist = self._haversine_distance(lat, lng, existing_lat, existing_lng)
                    if dist < 0.05:  # Within 50 meters
                        return True
                except:
                    pass
        
        return False
    
    def collect_detailed_info(self, hotel_id: str = None, hotel_data: Dict = None) -> Dict:
        """
        Collect detailed information for a specific hotel
        
        Args:
            hotel_id (str, optional): Hotel ID in the database
            hotel_data (Dict, optional): Hotel data if not in database
            
        Returns:
            Dict: Detailed hotel information
        """
        logger.info(f"Collecting detailed information for hotel ID: {hotel_id}")
        
        # Get hotel data if ID is provided
        if hotel_id and not hotel_data and self.db:
            try:
                hotel_data = self.db.get_hotel_by_id(hotel_id)
            except Exception as e:
                logger.error(f"Error retrieving hotel data from database: {e}")
                return {"error": "Failed to retrieve hotel data"}
        
        if not hotel_data:
            return {"error": "No hotel data provided"}
        
        # Initialize result with base hotel data
        result = hotel_data.copy()
        
        # Collect detailed information from all possible sources
        sources_checked = []
        
        # 1. Get detailed info from Google Places if available
        if self.scraper and hasattr(self.scraper, '_get_google_place_details'):
            try:
                place_id = None
                
                # Try to get place_id from additional_info
                if hotel_data.get('additional_info'):
                    try:
                        additional_info = json.loads(hotel_data.get('additional_info', '{}'))
                        place_id = additional_info.get('place_id')
                    except:
                        pass
                
                # If place_id found, get details
                if place_id:
                    logger.info(f"Getting Google Places details for place_id: {place_id}")
                    google_details = self.scraper._get_google_place_details(place_id)
                    
                    if google_details:
                        # Merge details into result
                        for key, value in google_details.items():
                            if key not in result or not result[key]:
                                result[key] = value
                        
                        sources_checked.append("Google Places")
            except Exception as e:
                logger.error(f"Error getting Google Places details: {e}")
        
        # 2. Get detailed info from OpenStreetMap if available
        if self.scraper and hasattr(self.scraper, '_get_osm_details'):
            try:
                osm_id = None
                
                # Try to get osm_id from additional_info
                if hotel_data.get('additional_info'):
                    try:
                        additional_info = json.loads(hotel_data.get('additional_info', '{}'))
                        osm_id = additional_info.get('osm_id')
                        osm_type = additional_info.get('osm_type')
                        
                        if osm_id and osm_type:
                            osm_id = f"{osm_type}:{osm_id}"
                    except:
                        pass
                
                # If osm_id found, get details
                if osm_id:
                    logger.info(f"Getting OpenStreetMap details for osm_id: {osm_id}")
                    osm_details = self.scraper._get_osm_details(osm_id)
                    
                    if osm_details:
                        # Merge details into result
                        for key, value in osm_details.items():
                            if key not in result or not result[key]:
                                result[key] = value
                        
                        sources_checked.append("OpenStreetMap")
            except Exception as e:
                logger.error(f"Error getting OpenStreetMap details: {e}")
        
        # 3. Extract information from hotel website if available
        if hotel_data.get('website') and self.scraper and hasattr(self.scraper, 'extract_hotel_website_info'):
            try:
                logger.info(f"Extracting information from website: {hotel_data['website']}")
                website_info = self.scraper.extract_hotel_website_info(hotel_data['website'])
                
                if website_info:
                    # Merge website information
                    for key, value in website_info.items():
                        if key not in result:
                            result[key] = value
                        elif key == 'contact' and isinstance(value, dict):
                            # Merge contact information
                            for contact_key, contact_value in value.items():
                                if contact_key not in result or not result[contact_key]:
                                    result[contact_key] = contact_value
                    
                    sources_checked.append("Hotel Website")
            except Exception as e:
                logger.error(f"Error extracting website information: {e}")
        
        # 4. Check for hotel history and add it to the result
        if hotel_id and self.db and hasattr(self.db, 'get_hotel_history'):
            try:
                logger.info(f"Getting hotel history for hotel ID: {hotel_id}")
                history = self.db.get_hotel_history(hotel_id)
                
                if history:
                    result['history'] = history
                    sources_checked.append("Historical Records")
            except Exception as e:
                logger.error(f"Error getting hotel history: {e}")
        
        # 5. Check for presence on booking platforms
        if self.scraper and hasattr(self.scraper, 'check_booking_platforms'):
            try:
                logger.info(f"Checking booking platforms for hotel: {hotel_data.get('name')}")
                platforms_data = self.scraper.check_booking_platforms(hotel_data)
                
                if platforms_data:
                    result['booking_platforms'] = platforms_data
                    sources_checked.append("Booking Platforms")
            except Exception as e:
                logger.error(f"Error checking booking platforms: {e}")
                # If the method doesn't exist yet, log a message
                if not hasattr(self.scraper, 'check_booking_platforms'):
                    logger.info("Booking platforms check not implemented yet")
        
        # Add metadata about the collection process
        result['detail_collection'] = {
            'timestamp': datetime.now().isoformat(),
            'sources_checked': sources_checked,
            'success': len(sources_checked) > 0
        }
        
        logger.info(f"Detailed information collection complete. Sources checked: {', '.join(sources_checked)}")
        return result
    
    def import_from_file(self, file_path: str, file_type: str = None) -> Dict:
        """
        Import hotel data from an external file
        
        Args:
            file_path (str): Path to the file
            file_type (str, optional): File type (csv, excel, json)
            
        Returns:
            Dict: Import results
        """
        logger.info(f"Importing data from file: {file_path}")
        
        # Determine file type if not provided
        if not file_type:
            ext = os.path.splitext(file_path)[1].lower()
            if ext == '.csv':
                file_type = 'csv'
            elif ext in ['.xls', '.xlsx']:
                file_type = 'excel'
            elif ext == '.json':
                file_type = 'json'
            else:
                logger.error(f"Unknown file type: {ext}")
                return {"error": f"Unknown file type: {ext}"}
        
        # Check if file exists
        if not os.path.exists(file_path):
            logger.error(f"File not found: {file_path}")
            return {"error": "File not found"}
        
        # Import based on file type
        try:
            imported_hotels = []
            
            if file_type == 'csv':
                # Import from CSV
                import pandas as pd
                df = pd.read_csv(file_path)
                records = df.to_dict('records')
                
                # Process and save each record
                for record in records:
                    # Convert to hotel data format
                    hotel_data = self._convert_to_hotel_data(record)
                    
                    # Save to database if available
                    if self.db and hasattr(self.db, 'save_hotel'):
                        hotel_id = self.db.save_hotel(hotel_data)
                        hotel_data['id'] = hotel_id
                    
                    imported_hotels.append(hotel_data)
            
            elif file_type == 'excel':
                # Import from Excel
                import pandas as pd
                df = pd.read_excel(file_path)
                records = df.to_dict('records')
                
                # Process and save each record
                for record in records:
                    # Convert to hotel data format
                    hotel_data = self._convert_to_hotel_data(record)
                    
                    # Save to database if available
                    if self.db and hasattr(self.db, 'save_hotel'):
                        hotel_id = self.db.save_hotel(hotel_data)
                        hotel_data['id'] = hotel_id
                    
                    imported_hotels.append(hotel_data)
            
            elif file_type == 'json':
                # Import from JSON
                with open(file_path, 'r', encoding='utf-8') as f:
                    json_data = json.load(f)
                
                # Handle both array and object formats
                if isinstance(json_data, list):
                    records = json_data
                else:
                    records = [json_data]
                
                # Process and save each record
                for record in records:
                    # Convert to hotel data format
                    hotel_data = self._convert_to_hotel_data(record)
                    
                    # Save to database if available
                    if self.db and hasattr(self.db, 'save_hotel'):
                        hotel_id = self.db.save_hotel(hotel_data)
                        hotel_data['id'] = hotel_id
                    
                    imported_hotels.append(hotel_data)
            
            logger.info(f"Import complete. Imported {len(imported_hotels)} hotels")
            
            return {
                "status": "success",
                "imported_count": len(imported_hotels),
                "first_10_hotels": imported_hotels[:10] if len(imported_hotels) > 10 else imported_hotels
            }
            
        except Exception as e:
            logger.error(f"Error importing data: {e}")
            import traceback
            traceback.print_exc()
            return {"error": f"Import failed: {str(e)}"}
    
    def _convert_to_hotel_data(self, record: Dict) -> Dict:
        """
        Convert a record from imported file to hotel data format
        
        Args:
            record (Dict): Record from imported file
            
        Returns:
            Dict: Formatted hotel data
        """
        # Map common field names to our format
        field_mapping = {
            # Common CSV headers to our internal format
            'name': ['name', 'hotel_name', 'property_name', 'title'],
            'address': ['address', 'hotel_address', 'property_address', 'street_address'],
            'city': ['city', 'town', 'municipality'],
            'country': ['country', 'country_name'],
            'latitude': ['latitude', 'lat', 'y', 'latitude_degrees'],
            'longitude': ['longitude', 'lng', 'lon', 'x', 'longitude_degrees'],
            'phone': ['phone', 'telephone', 'contact_phone', 'phone_number'],
            'email': ['email', 'contact_email', 'email_address'],
            'website': ['website', 'url', 'web_address', 'hotel_website'],
            'stars': ['stars', 'rating', 'hotel_stars', 'star_rating'],
            'price_range': ['price_range', 'price', 'price_category'],
            'facilities': ['facilities', 'amenities', 'services'],
            'legal_status': ['legal_status', 'status', 'hotel_status'],
            'data_source': ['data_source', 'source']
        }
        
        # Create new hotel data
        hotel_data = {}
        
        # Map fields using the mapping
        for our_field, possible_names in field_mapping.items():
            for name in possible_names:
                if name in record:
                    hotel_data[our_field] = record[name]
                    break
        
        # Add any remaining fields that might be useful
        for key, value in record.items():
            if key not in [item for sublist in field_mapping.values() for item in sublist]:
                # Store in additional_info if not already mapped
                if 'additional_info' not in hotel_data:
                    hotel_data['additional_info'] = {}
                
                hotel_data['additional_info'][key] = value
        
        # If additional_info exists, convert to JSON string
        if 'additional_info' in hotel_data and isinstance(hotel_data['additional_info'], dict):
            hotel_data['additional_info'] = json.dumps(hotel_data['additional_info'])
        
        # Add metadata
        hotel_data['last_updated'] = datetime.now().isoformat()
        if 'data_source' not in hotel_data:
            hotel_data['data_source'] = 'Imported Data'
        
        return hotel_data
    
    def city_scan(self, city: str, country: str = None, radius: int = 10000, 
                 subdivide: bool = True, max_subdivisions: int = 16) -> Dict:
        """
        Perform a comprehensive scan of an entire city
        
        Args:
            city (str): City name
            country (str, optional): Country name to improve geolocation accuracy
            radius (int): Initial search radius in meters
            subdivide (bool): Whether to subdivide the city into smaller search areas
            max_subdivisions (int): Maximum number of subdivisions
            
        Returns:
            Dict: Scan results
        """
        logger.info(f"Starting city scan for {city}, {country if country else ''}")
        
        # Format location string
        location = city
        if country:
            location = f"{city}, {country}"
        
        # Get city coordinates first
        coordinates = self._get_city_coordinates(location)
        
        if not coordinates:
            logger.error(f"Could not get coordinates for {location}")
            return {"error": f"Could not get coordinates for {location}"}
        
        city_lat, city_lng = coordinates
        logger.info(f"City coordinates: {city_lat}, {city_lng}")
        
        all_hotels = []
        processed_areas = 0
        
        # Simple scan if no subdivision requested
        if not subdivide:
            logger.info(f"Performing simple scan with radius {radius}m")
            hotels = self.discover_by_location(f"{city_lat},{city_lng}", radius)
            all_hotels.extend(hotels)
            processed_areas = 1
        else:
            # Perform subdivision scan
            logger.info(f"Performing subdivision scan with max {max_subdivisions} subdivisions")
            
            # Calculate initial subdivision
            # We'll create a grid around the city center
            # Start with a 2x2 grid and increase if needed
            
            grid_size = 2
            while grid_size * grid_size < max_subdivisions:
                lat_step = (radius * 2) / 111000 / grid_size  # Convert meters to approximate degrees
                lng_step = lat_step / math.cos(math.radians(city_lat))  # Adjust for longitude compression
                
                # Calculate grid bounds
                min_lat = city_lat - (lat_step * grid_size / 2)
                min_lng = city_lng - (lng_step * grid_size / 2)
                
                # Create search points
                search_points = []
                for i in range(grid_size):
                    for j in range(grid_size):
                        point_lat = min_lat + (i * lat_step) + (lat_step / 2)
                        point_lng = min_lng + (j * lng_step) + (lng_step / 2)
                        search_points.append((point_lat, point_lng))
                
                # If we have enough points, break
                if len(search_points) >= max_subdivisions:
                    break
                
                # Increase grid size
                grid_size += 1
            
            # Perform search at each point
            sub_radius = radius / (grid_size * 0.75)  # Reduce radius based on grid size with some overlap
            
            for i, (lat, lng) in enumerate(search_points):
                if i >= max_subdivisions:
                    break
                
                logger.info(f"Scanning subdivision {i+1}/{len(search_points)}: {lat}, {lng}")
                hotels = self.discover_by_location(f"{lat},{lng}", int(sub_radius))
                
                # Add new hotels (avoid duplicates)
                new_hotels = [h for h in hotels if not self._is_duplicate_hotel(h, all_hotels)]
                all_hotels.extend(new_hotels)
                
                logger.info(f"Found {len(new_hotels)} new hotels in subdivision {i+1}")
                processed_areas += 1
        
        # Save to database if available
        saved_count = 0
        if self.db and all_hotels:
            logger.info(f"Saving {len(all_hotels)} hotels to database")
            for hotel in all_hotels:
                try:
                    hotel_id = self.db.save_hotel(hotel)
                    saved_count += 1
                except Exception as e:
                    logger.error(f"Error saving hotel to database: {e}")
        
        logger.info(f"City scan complete. Found {len(all_hotels)} hotels in {processed_areas} areas. Saved {saved_count} to database.")
        
        return {
            "status": "success",
            "city": city,
            "coordinates": {"latitude": city_lat, "longitude": city_lng},
            "total_hotels_found": len(all_hotels),
            "areas_processed": processed_areas,
            "hotels_saved": saved_count,
            "sample_hotels": all_hotels[:10] if len(all_hotels) > 10 else all_hotels
        }
    
    def _get_city_coordinates(self, location: str) -> Optional[Tuple[float, float]]:
        """
        Get coordinates for a city using geocoding
        
        Args:
            location (str): City name, optionally with country
            
        Returns:
            Optional[Tuple[float, float]]: Coordinates (latitude, longitude) or None if not found
        """
        try:
            # Try to use scraper's method if available
            if self.scraper and hasattr(self.scraper, '_get_coordinates'):
                return self.scraper._get_coordinates(location)
            
            # Fallback to direct Nominatim API call
            nominatim_url = "https://nominatim.openstreetmap.org/search"
            params = {
                "q": location,
                "format": "json",
                "limit": 1
            }
            
            # Add contact information (required by Nominatim)
            headers = {
                "User-Agent": "Map_researcher0.4"
            }
            
            response = requests.get(nominatim_url, params=params, headers=headers)
            data = response.json()
            
            if data and len(data) > 0:
                return float(data[0]['lat']), float(data[0]['lon'])
            
            return None
        
        except Exception as e:
            logger.error(f"Error getting coordinates for {location}: {e}")
            return None
    
    def update_hotel_data(self, hotel_id: str = None, hotel_data: Dict = None) -> Dict:
        """
        Update data for an existing hotel
        
        Args:
            hotel_id (str, optional): Hotel ID in the database
            hotel_data (Dict, optional): Updated hotel data
            
        Returns:
            Dict: Update results
        """
        logger.info(f"Updating data for hotel ID: {hotel_id}")
        
        # Check if hotel exists in database
        if hotel_id and self.db and hasattr(self.db, 'get_hotel_by_id'):
            try:
                existing_hotel = self.db.get_hotel_by_id(hotel_id)
                if not existing_hotel:
                    logger.error(f"Hotel not found in database: {hotel_id}")
                    return {"error": "Hotel not found in database"}
            except Exception as e:
                logger.error(f"Error checking hotel in database: {e}")
                return {"error": f"Database error: {str(e)}"}
        else:
            return {"error": "Missing hotel ID or database connection"}
        
        # If hotel_data not provided, collect fresh data
        if not hotel_data:
            hotel_data = self.collect_detailed_info(hotel_id, existing_hotel)
        
        # Update in database
        if self.db and hasattr(self.db, 'update_hotel'):
            try:
                # Add update metadata
                hotel_data['last_updated'] = datetime.now().isoformat()
                
                # Track changes for history
                changes = {}
                for key, value in hotel_data.items():
                    if key in existing_hotel and existing_hotel[key] != value:
                        changes[key] = {
                            'old': existing_hotel[key],
                            'new': value
                        }
                
                # Update in database
                success = self.db.update_hotel(hotel_id, hotel_data)
                
                # Record history if supported
                if changes and hasattr(self.db, 'record_hotel_history'):
                    for key, change in changes.items():
                        self.db.record_hotel_history(
                            hotel_id=hotel_id,
                            event_type=f"{key}_change",
                            old_value=change['old'],
                            new_value=change['new'],
                            source="auto_update"
                        )
                
                if success:
                    logger.info(f"Hotel data updated successfully. Changed fields: {', '.join(changes.keys())}")
                    return {
                        "status": "success",
                        "message": "Hotel data updated successfully",
                        "changed_fields": list(changes.keys()),
                        "hotel_id": hotel_id
                    }
                else:
                    logger.error("Failed to update hotel data")
                    return {"error": "Failed to update hotel data"}
            
            except Exception as e:
                logger.error(f"Error updating hotel data: {e}")
                return {"error": f"Update failed: {str(e)}"}
        
        return {"error": "Database update not available"}
    
    def update_temporal_data(self, hotel_id: str = None, time_range: Tuple[datetime, datetime] = None) -> Dict:
        """
        Update historical and temporal data for a hotel
        
        Args:
            hotel_id (str, optional): Hotel ID in the database
            time_range (Tuple[datetime, datetime], optional): Time range to analyze
            
        Returns:
            Dict: Update results
        """
        logger.info(f"Updating temporal data for hotel ID: {hotel_id}")
        
        # Check if hotel exists in database
        if hotel_id and self.db and hasattr(self.db, 'get_hotel_by_id'):
            try:
                existing_hotel = self.db.get_hotel_by_id(hotel_id)
                if not existing_hotel:
                    logger.error(f"Hotel not found in database: {hotel_id}")
                    return {"error": "Hotel not found in database"}
            except Exception as e:
                logger.error(f"Error checking hotel in database: {e}")
                return {"error": f"Database error: {str(e)}"}
        else:
            return {"error": "Missing hotel ID or database connection"}
        
        # Set default time range if not provided (last 3 years)
        if not time_range:
            end_date = datetime.now()
            start_date = end_date - timedelta(days=3*365)
            time_range = (start_date, end_date)
        
        temporal_updates = {}
        
        # 1. Check Internet Archive for website changes if available
        if existing_hotel.get('website') and hasattr(self, 'wayback_machine_search'):
            try:
                logger.info(f"Searching Wayback Machine for website: {existing_hotel['website']}")
                wayback_data = self.wayback_machine_search(existing_hotel['website'], time_range)
                
                if wayback_data.get('status') == 'found':
                    temporal_updates['website_archives'] = wayback_data
                    
                    # Extract data from archives if possible
                    if hasattr(self, '_extract_data_from_wayback') and wayback_data.get('archive_url'):
                        archive_data = self._extract_data_from_wayback(wayback_data['archive_url'])
                        if archive_data:
                            temporal_updates['website_historical_data'] = archive_data
            except Exception as e:
                logger.error(f"Error searching Wayback Machine: {e}")
        
        # 2. Check for platform listing changes
        if hasattr(self, 'track_platform_transitions') and (existing_hotel.get('name') or existing_hotel.get('latitude')):
            try:
                logger.info(f"Tracking platform transitions for hotel: {existing_hotel.get('name')}")
                platform_data = self.track_platform_transitions(
                    hotel_name=existing_hotel.get('name'),
                    location=(existing_hotel.get('latitude'), existing_hotel.get('longitude')),
                    time_range=time_range
                )
                
                if platform_data:
                    temporal_updates['platform_transitions'] = platform_data
            except Exception as e:
                logger.error(f"Error tracking platform transitions: {e}")
        
        # 3. Check for ownership changes in business registries
        if hasattr(self, '_search_business_records') and existing_hotel.get('name'):
            try:
                logger.info(f"Searching business records for hotel: {existing_hotel.get('name')}")
                business_data = self._search_business_records(
                    hotel_name=existing_hotel.get('name'),
                    address=existing_hotel.get('address'),
                    time_range=time_range
                )
                
                if business_data:
                    temporal_updates['business_records'] = business_data
                    
                    # Record ownership changes if found
                    if hasattr(self.db, 'record_hotel_history'):
                        for record in business_data:
                            if record.get('event_type') == 'ownership_change':
                                self.db.record_hotel_history(
                                    hotel_id=hotel_id,
                                    event_type='ownership_change',
                                    old_value=record.get('old_owner'),
                                    new_value=record.get('new_owner'),
                                    source='business_registry',
                                    event_date=record.get('timestamp')
                                )
            except Exception as e:
                logger.error(f"Error searching business records: {e}")
        
        # 4. Update hotel data with temporal findings
        if temporal_updates and self.db and hasattr(self.db, 'update_hotel'):
            try:
                # Prepare update data
                update_data = {
                    'last_updated': datetime.now().isoformat(),
                    'temporal_data': json.dumps(temporal_updates)
                }
                
                # Update in database
                success = self.db.update_hotel(hotel_id, update_data)
                
                if success:
                    logger.info(f"Temporal data updated successfully with {len(temporal_updates)} updates")
                    return {
                        "status": "success",
                        "message": "Temporal data updated successfully",
                        "updates": list(temporal_updates.keys()),
                        "hotel_id": hotel_id
                    }
                else:
                    logger.error("Failed to update temporal data")
                    return {"error": "Failed to update temporal data"}
            
            except Exception as e:
                logger.error(f"Error updating temporal data: {e}")
                return {"error": f"Update failed: {str(e)}"}
        
        # If no updates found
        if not temporal_updates:
            logger.info(f"No temporal updates found for hotel ID: {hotel_id}")
            return {
                "status": "success",
                "message": "No temporal updates found",
                "hotel_id": hotel_id
            }
        
        return {"error": "Database update not available"}
    
    def search_rental_platforms(self, hotel_data: Dict = None, radius: int = 100) -> List[Dict]:
        """
        Search for hotel on non-traditional rental platforms
        
        Args:
            hotel_data (Dict): Hotel data
            radius (int): Search radius in meters around the hotel coordinates
            
        Returns:
            List[Dict]: List of rental listings
        """
        if not hotel_data:
            return []
        
        logger.info(f"Searching rental platforms for hotel: {hotel_data.get('name')}")
        
        platforms = [
            {'name': 'airbnb', 'search_func': self._search_airbnb if hasattr(self, '_search_airbnb') else None},
            {'name': 'vrbo', 'search_func': self._search_vrbo if hasattr(self, '_search_vrbo') else None},
            {'name': 'booking_apartments', 'search_func': self._search_booking_apartments if hasattr(self, '_search_booking_apartments') else None},
            {'name': 'local_rental', 'search_func': self._search_local_rental if hasattr(self, '_search_local_rental') else None}
        ]
        
        results = []
        
        # Get hotel coordinates
        lat = hotel_data.get('latitude')
        lng = hotel_data.get('longitude')
        
        if not lat or not lng:
            logger.error("Hotel coordinates not available")
            return []
        
        # Search each platform
        for platform in platforms:
            if platform['search_func']:
                try:
                    logger.info(f"Searching {platform['name']} for rentals near coordinates: {lat}, {lng}")
                    platform_results = platform['search_func'](
                        location=(lat, lng),
                        radius=radius,
                        hotel_name=hotel_data.get('name')
                    )
                    
                    if platform_results:
                        for result in platform_results:
                            result['platform'] = platform['name']
                        
                        results.extend(platform_results)
                        logger.info(f"Found {len(platform_results)} listings on {platform['name']}")
                
                except Exception as e:
                    logger.error(f"Error searching {platform['name']}: {e}")
        
        # Calculate similarity and distance for each result
        for result in results:
            try:
                # Calculate name similarity
                if hotel_data.get('name') and result.get('name'):
                    result['name_similarity'] = self._calculate_name_similarity(
                        hotel_data['name'], result['name']
                    )
                
                # Calculate distance
                if result.get('latitude') and result.get('longitude'):
                    result['distance'] = self._haversine_distance(
                        lat, lng, result['latitude'], result['longitude']
                    ) * 1000  # Convert to meters
            except Exception as e:
                logger.error(f"Error calculating similarity/distance: {e}")
        
        # Sort by distance
        results.sort(key=lambda x: x.get('distance', float('inf')))
        
        logger.info(f"Found {len(results)} total rental listings across all platforms")
        return results
    
    def _calculate_name_similarity(self, name1: str, name2: str) -> float:
        """
        Calculate similarity between two names using Levenshtein distance
        
        Args:
            name1 (str): First name
            name2 (str): Second name
            
        Returns:
            float: Similarity score (0-1)
        """
        # Simple implementation of Levenshtein distance
        # For production, consider using a dedicated library
        
        # Convert to lowercase
        s1 = name1.lower()
        s2 = name2.lower()
        
        # Create matrix
        rows = len(s1) + 1
        cols = len(s2) + 1
        distance = [[0 for _ in range(cols)] for _ in range(rows)]
        
        # Initialize first row and column
        for i in range(rows):
            distance[i][0] = i
        for j in range(cols):
            distance[0][j] = j
        
        # Calculate distance
        for i in range(1, rows):
            for j in range(1, cols):
                cost = 0 if s1[i-1] == s2[j-1] else 1
                distance[i][j] = min(
                    distance[i-1][j] + 1,      # deletion
                    distance[i][j-1] + 1,      # insertion
                    distance[i-1][j-1] + cost  # substitution
                )
        
        # Calculate similarity score
        max_len = max(len(s1), len(s2))
        if max_len == 0:
            return 1.0
        
        return 1.0 - (distance[rows-1][cols-1] / max_len)

# If run directly, display module info
if __name__ == "__main__":
    print("Data Discovery Module for Map_researcher 0.4")
    print("This module provides functions for discovering and collecting hotel data.")