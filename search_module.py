#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Search Module for Map_researcher 0.4
This module provides advanced search functionality for hotel data.
"""

import os
import sys
import time
import json
import logging
import math
from typing import Dict, List, Any, Tuple, Optional, Union
from datetime import datetime, timedelta

# Initialize logger
logger = logging.getLogger("search_module")

class SearchModule:
    """Class for advanced hotel search operations"""
    
    def __init__(self, db=None, scraper=None):
        """
        Initialize the search module
        
        Args:
            db: Database instance
            scraper: HotelScraper instance
        """
        self.db = db
        self.scraper = scraper
        self.last_search_results = []
        self.search_history = []
    
    def advanced_search(self, criteria: Dict[str, Any]) -> List[Dict]:
        """
        Perform advanced multi-criteria search
        
        Args:
            criteria: Dictionary of search criteria
            
        Returns:
            List[Dict]: List of matching hotels
        """
        logger.info(f"Starting advanced search with criteria: {criteria}")
        
        # Build database query
        query = {}
        
        # Process name criteria
        if criteria.get('name'):
            query['name'] = f"%{criteria['name']}%"
        
        # Process location criteria
        if criteria.get('city'):
            query['city'] = criteria['city']
        
        if criteria.get('country'):
            query['country'] = criteria['country']
        
        # Process status criteria
        if criteria.get('status') and criteria['status'] != 'all':
            query['operation_status'] = criteria['status']
        
        # Process risk level criteria
        if criteria.get('risk_level') and criteria['risk_level'] != 'all':
            query['risk_level'] = criteria['risk_level']
        
        # Process data source criteria
        if criteria.get('data_source'):
            query['data_source'] = criteria['data_source']
        
        # Process date range criteria
        date_conditions = []
        if criteria.get('date_from'):
            try:
                date_from = datetime.strptime(criteria['date_from'], "%Y-%m-%d")
                date_conditions.append(f"last_updated >= '{date_from.isoformat()}'")
            except:
                logger.warning(f"Invalid date format for date_from: {criteria['date_from']}")
        
        if criteria.get('date_to'):
            try:
                date_to = datetime.strptime(criteria['date_to'], "%Y-%m-%d")
                date_conditions.append(f"last_updated <= '{date_to.isoformat()}'")
            except:
                logger.warning(f"Invalid date format for date_to: {criteria['date_to']}")
        
        # Process rating criteria
        if criteria.get('min_stars') and criteria['min_stars'].replace('.', '', 1).isdigit():
            min_stars = float(criteria['min_stars'])
            query['stars'] = f">= {min_stars}"
        
        # Process special criteria (has website, contact info, etc.)
        special_conditions = []
        
        if criteria.get('has_website') == 'y':
            special_conditions.append("website IS NOT NULL AND website != ''")
        elif criteria.get('has_website') == 'n':
            special_conditions.append("(website IS NULL OR website = '')")
        
        if criteria.get('has_contact') == 'y':
            special_conditions.append("(phone IS NOT NULL AND phone != '') OR (email IS NOT NULL AND email != '')")
        elif criteria.get('has_contact') == 'n':
            special_conditions.append("(phone IS NULL OR phone = '') AND (email IS NULL OR email = '')")
        
        if criteria.get('official_platform') == 'y':
            special_conditions.append("EXISTS (SELECT 1 FROM hotel_listings WHERE hotel_listings.hotel_id = hotels.id AND platform_id IN (SELECT id FROM platforms WHERE is_official = 1))")
        elif criteria.get('official_platform') == 'n':
            special_conditions.append("NOT EXISTS (SELECT 1 FROM hotel_listings WHERE hotel_listings.hotel_id = hotels.id AND platform_id IN (SELECT id FROM platforms WHERE is_official = 1))")
        
        if criteria.get('private_platform') == 'y':
            special_conditions.append("EXISTS (SELECT 1 FROM hotel_listings WHERE hotel_listings.hotel_id = hotels.id AND platform_id IN (SELECT id FROM platforms WHERE is_official = 0))")
        elif criteria.get('private_platform') == 'n':
            special_conditions.append("NOT EXISTS (SELECT 1 FROM hotel_listings WHERE hotel_listings.hotel_id = hotels.id AND platform_id IN (SELECT id FROM platforms WHERE is_official = 0))")
        
        # Execute search
        results = []
        
        if self.db and hasattr(self.db, 'search_hotels'):
            try:
                # Add any special conditions to the query
                if date_conditions or special_conditions:
                    query['_conditions'] = date_conditions + special_conditions
                
                logger.info(f"Executing database search with query: {query}")
                results = self.db.search_hotels(query)
                logger.info(f"Found {len(results)} matching hotels")
            except Exception as e:
                logger.error(f"Error searching database: {e}")
        else:
            logger.error("Database not available for search")
        
        # Store search results for later use
        self.last_search_results = results
        
        # Add to search history
        self.search_history.append({
            'timestamp': datetime.now().isoformat(),
            'criteria': criteria,
            'result_count': len(results)
        })
        
        return results
    
    def search_by_name(self, name: str, exact_match: bool = False) -> List[Dict]:
        """
        Search for hotels by name
        
        Args:
            name: Hotel name to search for
            exact_match: Whether to perform exact match
            
        Returns:
            List[Dict]: List of matching hotels
        """
        logger.info(f"Searching for hotels by name: {name}, exact_match: {exact_match}")
        
        if not name:
            logger.warning("Empty name provided for search")
            return []
        
        results = []
        
        if self.db and hasattr(self.db, 'search_hotels'):
            try:
                query = {}
                
                if exact_match:
                    query['name'] = name
                else:
                    query['name'] = f"%{name}%"
                
                logger.info(f"Executing database search with query: {query}")
                results = self.db.search_hotels(query)
                logger.info(f"Found {len(results)} matching hotels")
            except Exception as e:
                logger.error(f"Error searching database: {e}")
        else:
            logger.error("Database not available for search")
        
        # Store search results for later use
        self.last_search_results = results
        
        # Add to search history
        self.search_history.append({
            'timestamp': datetime.now().isoformat(),
            'criteria': {'name': name, 'exact_match': exact_match},
            'result_count': len(results)
        })
        
        return results
    
    def search_sparse_info(self, min_data_threshold: float = 0.3, missing_fields: List[str] = None) -> List[Dict]:
        """
        Search for hotels with sparse information
        
        Args:
            min_data_threshold: Minimum data completeness threshold (0.1-0.9)
            missing_fields: List of fields that should be missing
            
        Returns:
            List[Dict]: List of hotels with sparse information
        """
        logger.info(f"Searching for hotels with sparse information: threshold {min_data_threshold}")
        
        # Set default missing fields if not provided
        if missing_fields is None:
            missing_fields = ['name', 'address', 'phone', 'email', 'website', 'stars']
        
        results = []
        
        if self.db and hasattr(self.db, 'get_all_hotels'):
            try:
                # Get all hotels (inefficient for large databases, but allows complex filtering)
                all_hotels = self.db.get_all_hotels()
                logger.info(f"Retrieved {len(all_hotels)} hotels for sparse info analysis")
                
                # Filter hotels based on data completeness
                for hotel in all_hotels:
                    # Calculate data completeness
                    completed_fields = 0
                    total_fields = len(missing_fields)
                    
                    for field in missing_fields:
                        if hotel.get(field):
                            completed_fields += 1
                    
                    data_percentage = completed_fields / total_fields if total_fields > 0 else 1.0
                    
                    # If below threshold, add to results
                    if data_percentage < min_data_threshold:
                        # Add data percentage to hotel data
                        hotel['data_percentage'] = data_percentage
                        hotel['missing_fields'] = [field for field in missing_fields if not hotel.get(field)]
                        results.append(hotel)
                
                logger.info(f"Found {len(results)} hotels with sparse information")
                
                # Sort by data completeness (ascending)
                results.sort(key=lambda x: x.get('data_percentage', 1.0))
            except Exception as e:
                logger.error(f"Error searching for hotels with sparse information: {e}")
        else:
            logger.error("Database not available for search")
        
        # Store search results for later use
        self.last_search_results = results
        
        # Add to search history
        self.search_history.append({
            'timestamp': datetime.now().isoformat(),
            'criteria': {'min_data_threshold': min_data_threshold, 'missing_fields': missing_fields},
            'result_count': len(results)
        })
        
        return results
    
    def search_same_owner(self, owner_name: str = None, hotel_id: str = None) -> List[Dict]:
        """
        Search for hotels with the same owner/operator
        
        Args:
            owner_name: Owner/operator name
            hotel_id: Hotel ID to find other hotels by the same owner
            
        Returns:
            List[Dict]: List of hotels with the same owner
        """
        logger.info(f"Searching for hotels with same owner: {owner_name or 'from hotel ID ' + str(hotel_id)}")
        
        if not owner_name and not hotel_id:
            logger.warning("No owner name or hotel ID provided")
            return []
        
        # If hotel ID provided but no owner name, get owner name from hotel
        if hotel_id and not owner_name and self.db and hasattr(self.db, 'get_hotel_by_id'):
            try:
                hotel = self.db.get_hotel_by_id(hotel_id)
                if hotel:
                    # Try to get owner information
                    if hotel.get('ownership_info'):
                        try:
                            owner_info = json.loads(hotel.get('ownership_info', '{}'))
                            owner_name = owner_info.get('owner_name')
                        except:
                            pass
                    
                    # If still no owner name, try additional_info
                    if not owner_name and hotel.get('additional_info'):
                        try:
                            additional_info = json.loads(hotel.get('additional_info', '{}'))
                            owner_name = (additional_info.get('owner_name') or 
                                         additional_info.get('owner') or 
                                         additional_info.get('operator'))
                        except:
                            pass
            except Exception as e:
                logger.error(f"Error getting hotel information: {e}")
        
        if not owner_name:
            logger.warning("Could not determine owner name for search")
            return []
        
        results = []
        
        if self.db and hasattr(self.db, 'search_hotels'):
            try:
                # Try several search approaches
                
                # 1. Direct search in ownership_info field
                query1 = {"ownership_info": f"%\"{owner_name}\"%"}
                results1 = self.db.search_hotels(query1)
                
                # 2. Search in additional_info field
                query2 = {"additional_info": f"%\"{owner_name}\"%"}
                results2 = self.db.search_hotels(query2)
                
                # Combine results (avoid duplicates)
                all_ids = set()
                for hotel in results1 + results2:
                    if hotel.get('id') and hotel.get('id') not in all_ids:
                        all_ids.add(hotel.get('id'))
                        results.append(hotel)
                
                logger.info(f"Found {len(results)} hotels with owner matching '{owner_name}'")
            except Exception as e:
                logger.error(f"Error searching for hotels with same owner: {e}")
        else:
            logger.error("Database not available for search")
        
        # Store search results for later use
        self.last_search_results = results
        
        # Add to search history
        self.search_history.append({
            'timestamp': datetime.now().isoformat(),
            'criteria': {'owner_name': owner_name, 'hotel_id': hotel_id},
            'result_count': len(results)
        })
        
        return results
    
    def search_nearby(self, location: Union[str, Tuple[float, float]], radius: int = 500) -> List[Dict]:
        """
        Search for hotels near a specific location
        
        Args:
            location: Location name or coordinates (latitude, longitude)
            radius: Search radius in meters
            
        Returns:
            List[Dict]: List of nearby hotels
        """
        logger.info(f"Searching for hotels near location: {location}, radius: {radius}m")
        
        coordinates = None
        
        # Convert location string to coordinates if needed
        if isinstance(location, str):
            if ',' in location and all(part.replace('.', '', 1).replace('-', '', 1).isdigit() 
                                     for part in location.split(',')):
                # Location is already in coordinate format
                try:
                    lat, lng = map(float, location.split(','))
                    coordinates = (lat, lng)
                except:
                    pass
            else:
                # Try to geocode location
                if self.scraper and hasattr(self.scraper, '_get_coordinates'):
                    try:
                        coordinates = self.scraper._get_coordinates(location)
                    except Exception as e:
                        logger.error(f"Error geocoding location: {e}")
        elif isinstance(location, tuple) and len(location) == 2:
            # Location is already coordinates
            coordinates = location
        
        if not coordinates:
            logger.error(f"Could not determine coordinates for location: {location}")
            return []
        
        lat, lng = coordinates
        logger.info(f"Using coordinates: {lat}, {lng}")
        
        results = []
        
        if self.db and hasattr(self.db, 'get_all_hotels'):
            try:
                # Get all hotels (inefficient for large databases, could be optimized)
                all_hotels = self.db.get_all_hotels()
                logger.info(f"Retrieved {len(all_hotels)} hotels for proximity search")
                
                # Filter hotels based on proximity
                for hotel in all_hotels:
                    hotel_lat = hotel.get('latitude')
                    hotel_lng = hotel.get('longitude')
                    
                    if hotel_lat and hotel_lng:
                        # Calculate distance
                        distance = self._haversine_distance(lat, lng, hotel_lat, hotel_lng) * 1000  # in meters
                        
                        # If within radius, add to results
                        if distance <= radius:
                            hotel['distance'] = round(distance, 1)
                            results.append(hotel)
                
                logger.info(f"Found {len(results)} hotels within {radius}m")
                
                # Sort by distance
                results.sort(key=lambda x: x.get('distance', float('inf')))
            except Exception as e:
                logger.error(f"Error searching for nearby hotels: {e}")
        else:
            logger.error("Database not available for search")
        
        # Store search results for later use
        self.last_search_results = results
        
        # Add to search history
        self.search_history.append({
            'timestamp': datetime.now().isoformat(),
            'criteria': {'location': location, 'radius': radius},
            'result_count': len(results)
        })
        
        return results
    
    def _haversine_distance(self, lat1: float, lon1: float, lat2: float, lon2: float) -> float:
        """
        Calculate distance between two geographical points using Haversine formula
        
        Args:
            lat1: Latitude of first point
            lon1: Longitude of first point
            lat2: Latitude of second point
            lon2: Longitude of second point
            
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
    
    def search_similar(self, hotel_id: str = None, hotel_data: Dict = None, 
                       similarity_threshold: float = 0.7) -> List[Dict]:
        """
        Search for hotels similar to a reference hotel
        
        Args:
            hotel_id: Reference hotel ID
            hotel_data: Reference hotel data (if ID not provided)
            similarity_threshold: Minimum similarity score (0-1)
            
        Returns:
            List[Dict]: List of similar hotels
        """
        logger.info(f"Searching for hotels similar to hotel ID: {hotel_id}")
        
        # Get reference hotel data if ID provided but no data
        reference_hotel = hotel_data
        if hotel_id and not reference_hotel and self.db and hasattr(self.db, 'get_hotel_by_id'):
            try:
                reference_hotel = self.db.get_hotel_by_id(hotel_id)
            except Exception as e:
                logger.error(f"Error retrieving hotel data: {e}")
        
        if not reference_hotel:
            logger.error("No reference hotel provided for similarity search")
            return []
        
        results = []
        
        if self.db and hasattr(self.db, 'get_all_hotels'):
            try:
                # Get all hotels
                all_hotels = self.db.get_all_hotels()
                logger.info(f"Retrieved {len(all_hotels)} hotels for similarity analysis")
                
                # Calculate similarity for each hotel
                for hotel in all_hotels:
                    # Skip if it's the same hotel
                    if hotel.get('id') == hotel_id:
                        continue
                    
                    # Calculate similarity score
                    similarity = self._calculate_similarity(reference_hotel, hotel)
                    
                    # If above threshold, add to results
                    if similarity >= similarity_threshold:
                        hotel['similarity_score'] = similarity
                        results.append(hotel)
                
                logger.info(f"Found {len(results)} similar hotels")
                
                # Sort by similarity score (descending)
                results.sort(key=lambda x: x.get('similarity_score', 0), reverse=True)
            except Exception as e:
                logger.error(f"Error searching for similar hotels: {e}")
        else:
            logger.error("Database not available for search")
        
        # Store search results for later use
        self.last_search_results = results
        
        # Add to search history
        self.search_history.append({
            'timestamp': datetime.now().isoformat(),
            'criteria': {'hotel_id': hotel_id, 'similarity_threshold': similarity_threshold},
            'result_count': len(results)
        })
        
        return results
    
    def _calculate_similarity(self, hotel1: Dict, hotel2: Dict) -> float:
        """
        Calculate similarity between two hotels
        
        Args:
            hotel1: First hotel data
            hotel2: Second hotel data
            
        Returns:
            float: Similarity score (0-1)
        """
        # Calculate similarity based on various factors
        scores = []
        
        # Name similarity (highest weight)
        if hotel1.get('name') and hotel2.get('name'):
            name_similarity = self._calculate_string_similarity(hotel1['name'], hotel2['name'])
            scores.append(name_similarity * 3)  # Triple weight
        
        # Address similarity
        if hotel1.get('address') and hotel2.get('address'):
            address_similarity = self._calculate_string_similarity(hotel1['address'], hotel2['address'])
            scores.append(address_similarity * 2)  # Double weight
        
        # Location proximity
        if (hotel1.get('latitude') and hotel1.get('longitude') and 
            hotel2.get('latitude') and hotel2.get('longitude')):
            
            distance = self._haversine_distance(
                hotel1['latitude'], hotel1['longitude'],
                hotel2['latitude'], hotel2['longitude']
            )
            
            # Convert distance to similarity score
            # 0km = 1.0, 1km = 0.5, 2km+ = 0.0
            location_similarity = max(0, 1 - (distance / 2))
            scores.append(location_similarity * 2)  # Double weight
        
        # Stars similarity
        if hotel1.get('stars') and hotel2.get('stars'):
            # Calculate difference in stars (normalized to 0-1)
            stars_diff = abs(float(hotel1['stars']) - float(hotel2['stars']))
            stars_similarity = max(0, 1 - (stars_diff / 5))  # 5-star scale
            scores.append(stars_similarity)
        
        # Price range similarity
        if hotel1.get('price_range') and hotel2.get('price_range'):
            if hotel1['price_range'] == hotel2['price_range']:
                scores.append(1.0)
            else:
                scores.append(0.5)  # Partial match
        
        # Facilities similarity
        if hotel1.get('facilities') and hotel2.get('facilities'):
            facilities_similarity = self._calculate_string_similarity(hotel1['facilities'], hotel2['facilities'])
            scores.append(facilities_similarity)
        
        # Calculate final similarity score
        if scores:
            return sum(scores) / len(scores)
        else:
            return 0.0
    
    def _calculate_string_similarity(self, str1: str, str2: str) -> float:
        """
        Calculate similarity between two strings
        
        Args:
            str1: First string
            str2: Second string
            
        Returns:
            float: Similarity score (0-1)
        """
        # Simple Levenshtein-based implementation
        # For production, consider using a dedicated library
        
        # Convert to lowercase
        s1 = str1.lower()
        s2 = str2.lower()
        
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
    
    def search_unverified(self, days_threshold: int = 30) -> List[Dict]:
        """
        Search for hotels that haven't been verified recently
        
        Args:
            days_threshold: Threshold in days for unverified hotels
            
        Returns:
            List[Dict]: List of unverified hotels
        """
        logger.info(f"Searching for unverified hotels (threshold: {days_threshold} days)")
        
        results = []
        
        if self.db and hasattr(self.db, 'get_all_hotels'):
            try:
                # Get all hotels
                all_hotels = self.db.get_all_hotels()
                logger.info(f"Retrieved {len(all_hotels)} hotels for verification check")
                
                # Calculate cutoff date
                cutoff_date = datetime.now() - timedelta(days=days_threshold)
                
                # Filter hotels
                for hotel in all_hotels:
                    # Check verification status
                    if hotel.get('verification_status') == 'unverified':
                        results.append(hotel)
                        continue
                    
                    # Check last verification date
                    if hotel.get('last_verification_date'):
                        try:
                            last_verified = datetime.fromisoformat(hotel['last_verification_date'])
                            if last_verified < cutoff_date:
                                results.append(hotel)
                        except:
                            # If date format is invalid, consider unverified
                            results.append(hotel)
                    else:
                        # No verification date, consider unverified
                        results.append(hotel)
                
                logger.info(f"Found {len(results)} unverified hotels")
                
                # Sort by last verification date (oldest first)
                results.sort(key=lambda x: x.get('last_verification_date', '0000-00-00'))
            except Exception as e:
                logger.error(f"Error searching for unverified hotels: {e}")
        else:
            logger.error("Database not available for search")
        
        # Store search results for later use
        self.last_search_results = results
        
        # Add to search history
        self.search_history.append({
            'timestamp': datetime.now().isoformat(),
            'criteria': {'days_threshold': days_threshold},
            'result_count': len(results)
        })
        
        return results
    
    def search_recent_changes(self, days_threshold: int = 7) -> List[Dict]:
        """
        Search for hotels with recent changes
        
        Args:
            days_threshold: Threshold in days for recent changes
            
        Returns:
            List[Dict]: List of hotels with recent changes
        """
        logger.info(f"Searching for hotels with recent changes (threshold: {days_threshold} days)")
        
        results = []
        
        if self.db and hasattr(self.db, 'get_hotel_history'):
            try:
                # Calculate cutoff date
                cutoff_date = datetime.now() - timedelta(days=days_threshold)
                cutoff_str = cutoff_date.isoformat()
                
                # Query for recent history entries
                history_query = f"SELECT DISTINCT hotel_id FROM hotel_history WHERE event_date >= '{cutoff_str}'"
                history_hotels = self.db.execute_query(history_query)
                
                # Get full hotel data for each ID
                for history_entry in history_hotels:
                    hotel_id = history_entry[0]
                    hotel = self.db.get_hotel_by_id(hotel_id)
                    
                    if hotel:
                        # Get recent changes for this hotel
                        changes = self.db.get_hotel_history(hotel_id, limit=5, after_date=cutoff_str)
                        
                        if changes:
                            hotel['recent_changes'] = changes
                            results.append(hotel)
                
                logger.info(f"Found {len(results)} hotels with recent changes")
                
                # Sort by most recent change
                results.sort(key=lambda x: max(c.get('event_date', '0000-00-00') for c in x.get('recent_changes', [])), 
                           reverse=True)
            except Exception as e:
                logger.error(f"Error searching for hotels with recent changes: {e}")
                
                # Fallback to last_updated field
                try:
                    all_hotels = self.db.get_all_hotels()
                    logger.info(f"Retrieved {len(all_hotels)} hotels for recent changes check (fallback)")
                    
                    for hotel in all_hotels:
                        if hotel.get('last_updated'):
                            try:
                                last_updated = datetime.fromisoformat(hotel['last_updated'])
                                if last_updated >= cutoff_date:
                                    results.append(hotel)
                            except:
                                pass
                    
                    logger.info(f"Found {len(results)} hotels with recent updates (fallback)")
                    
                    # Sort by last updated (most recent first)
                    results.sort(key=lambda x: x.get('last_updated', '0000-00-00'), reverse=True)
                except Exception as e2:
                    logger.error(f"Error in fallback search for recent changes: {e2}")
        else:
            logger.error("Database or history access not available")
        
        # Store search results for later use
        self.last_search_results = results
        
        # Add to search history
        self.search_history.append({
            'timestamp': datetime.now().isoformat(),
            'criteria': {'days_threshold': days_threshold},
            'result_count': len(results)
        })
        
        return results
    
    def view_hotel_details(self, hotel_id: str) -> Dict:
        """
        Get detailed information for a specific hotel
        
        Args:
            hotel_id: Hotel ID
            
        Returns:
            Dict: Detailed hotel information
        """
        logger.info(f"Retrieving detailed information for hotel ID: {hotel_id}")
        
        if not hotel_id:
            logger.error("No hotel ID provided")
            return {"error": "No hotel ID provided"}
        
        result = {}
        
        if self.db and hasattr(self.db, 'get_hotel_by_id'):
            try:
                # Get basic hotel data
                hotel = self.db.get_hotel_by_id(hotel_id)
                
                if not hotel:
                    logger.error(f"Hotel not found: {hotel_id}")
                    return {"error": "Hotel not found"}
                
                # Start with base hotel data
                result = hotel.copy()
                
                # Add history if available
                if hasattr(self.db, 'get_hotel_history'):
                    history = self.db.get_hotel_history(hotel_id)
                    if history:
                        result['history'] = history
                
                # Add platform listings if available
                if hasattr(self.db, 'get_hotel_listings'):
                    listings = self.db.get_hotel_listings(hotel_id)
                    if listings:
                        result['listings'] = listings
                
                # Add related hotels if available
                if hasattr(self.db, 'get_hotel_relationships'):
                    relationships = self.db.get_hotel_relationships(hotel_id)
                    if relationships:
                        # Get full data for related hotels
                        related_hotels = []
                        for rel in relationships:
                            other_id = rel['hotel_id_a'] if rel['hotel_id_a'] != hotel_id else rel['hotel_id_b']
                            other_hotel = self.db.get_hotel_by_id(other_id)
                            if other_hotel:
                                related_hotels.append({
                                    'hotel': other_hotel,
                                    'relationship': rel
                                })
                        
                        result['related_hotels'] = related_hotels
                
                # Add risk analysis if available
                if hasattr(self.db, 'get_hotel_risk_analysis'):
                    risk_analysis = self.db.get_hotel_risk_analysis(hotel_id)
                    if risk_analysis:
                        result['risk_analysis'] = risk_analysis
                
                logger.info(f"Retrieved detailed information for hotel: {hotel.get('name')}")
            except Exception as e:
                logger.error(f"Error retrieving hotel details: {e}")
                return {"error": f"Failed to retrieve hotel details: {str(e)}"}
        else:
            logger.error("Database not available for hotel details retrieval")
            return {"error": "Database not available"}
        
        return result
    
    def format_search_results(self, results: List[Dict], format_type: str = 'summary') -> Dict:
        """
        Format search results for display
        
        Args:
            results: List of hotel results
            format_type: Format type (summary, detailed, coordinates, contacts)
            
        Returns:
            Dict: Formatted results
        """
        logger.info(f"Formatting {len(results)} search results as {format_type}")
        
        if format_type == 'summary':
            # Basic summary format
            formatted_results = []
            for hotel in results:
                formatted_results.append({
                    'id': hotel.get('id'),
                    'name': hotel.get('name', 'Unknown'),
                    'address': hotel.get('address', 'N/A'),
                    'city': hotel.get('city', 'N/A'),
                    'country': hotel.get('country', 'N/A'),
                    'stars': hotel.get('stars', 'N/A'),
                    'data_source': hotel.get('data_source', 'N/A')
                })
            
            return {
                'count': len(results),
                'format': 'summary',
                'results': formatted_results
            }
        
        elif format_type == 'detailed':
            # Detailed format with more fields
            formatted_results = []
            for hotel in results:
                formatted_results.append({
                    'id': hotel.get('id'),
                    'name': hotel.get('name', 'Unknown'),
                    'address': hotel.get('address', 'N/A'),
                    'city': hotel.get('city', 'N/A'),
                    'country': hotel.get('country', 'N/A'),
                    'stars': hotel.get('stars', 'N/A'),
                    'price_range': hotel.get('price_range', 'N/A'),
                    'latitude': hotel.get('latitude', 'N/A'),
                    'longitude': hotel.get('longitude', 'N/A'),
                    'phone': hotel.get('phone', 'N/A'),
                    'email': hotel.get('email', 'N/A'),
                    'website': hotel.get('website', 'N/A'),
                    'facilities': hotel.get('facilities', 'N/A'),
                    'legal_status': hotel.get('legal_status', 'N/A'),
                    'data_source': hotel.get('data_source', 'N/A'),
                    'last_updated': hotel.get('last_updated', 'N/A')
                })
            
            return {
                'count': len(results),
                'format': 'detailed',
                'results': formatted_results
            }
        
        elif format_type == 'coordinates':
            # Format focused on geographic coordinates
            formatted_results = []
            for hotel in results:
                if hotel.get('latitude') and hotel.get('longitude'):
                    formatted_results.append({
                        'id': hotel.get('id'),
                        'name': hotel.get('name', 'Unknown'),
                        'latitude': hotel.get('latitude'),
                        'longitude': hotel.get('longitude'),
                        'address': hotel.get('address', 'N/A')
                    })
            
            return {
                'count': len(formatted_results),
                'format': 'coordinates',
                'results': formatted_results
            }
        
        elif format_type == 'contacts':
            # Format focused on contact information
            formatted_results = []
            for hotel in results:
                formatted_results.append({
                    'id': hotel.get('id'),
                    'name': hotel.get('name', 'Unknown'),
                    'phone': hotel.get('phone', 'N/A'),
                    'email': hotel.get('email', 'N/A'),
                    'website': hotel.get('website', 'N/A'),
                    'address': hotel.get('address', 'N/A')
                })
            
            return {
                'count': len(results),
                'format': 'contacts',
                'results': formatted_results
            }
        
        else:
            # Invalid format type, return basic summary
            logger.warning(f"Invalid format type: {format_type}, using summary instead")
            return self.format_search_results(results, 'summary')
    
    def get_search_history(self, limit: int = 10) -> List[Dict]:
        """
        Get search history
        
        Args:
            limit: Maximum number of history entries to return
            
        Returns:
            List[Dict]: Search history entries
        """
        # Return most recent entries first
        return sorted(self.search_history, key=lambda x: x.get('timestamp', ''), reverse=True)[:limit]

# If run directly, display module info
if __name__ == "__main__":
    print("Search Module for Map_researcher 0.4")
    print("This module provides advanced search functionality for hotel data.")