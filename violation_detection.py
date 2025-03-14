#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Violation Detection Module for Map_researcher 0.4
This module provides functionality for detecting potential hotel violations.
"""

import os
import sys
import time
import json
import logging
import math
import re
from typing import Dict, List, Any, Tuple, Optional, Union
from datetime import datetime, timedelta
import numpy as np

try:
    from sklearn.cluster import DBSCAN
    SKLEARN_AVAILABLE = True
except ImportError:
    SKLEARN_AVAILABLE = False

# Initialize logger
logger = logging.getLogger("violation_detection")

class ViolationDetection:
    """Class for detecting potential hotel violations"""
    
    def __init__(self, db=None, scraper=None):
        """
        Initialize the violation detection module
        
        Args:
            db: Database instance
            scraper: HotelScraper instance
        """
        self.db = db
        self.scraper = scraper
    
    def analyze_potential_violations(self, criteria: Dict = None) -> List[Dict]:
        """
        Perform comprehensive violations analysis
        
        Args:
            criteria: Analysis criteria
            
        Returns:
            List[Dict]: List of hotels with potential violations
        """
        logger.info(f"Analyzing potential violations with criteria: {criteria}")
        
        # Set default criteria if not provided
        if not criteria:
            criteria = {
                'min_risk_score': 3,
                'include_unverified': True,
                'max_results': 100
            }
        
        # Get all hotels
        all_hotels = []
        if self.db and hasattr(self.db, 'get_all_hotels'):
            try:
                all_hotels = self.db.get_all_hotels()
                logger.info(f"Retrieved {len(all_hotels)} hotels for violation analysis")
            except Exception as e:
                logger.error(f"Error retrieving hotels: {e}")
                return []
        else:
            logger.error("Database not available for retrieval")
            return []
        
        # Analyze each hotel for violations
        results = []
        
        for hotel in all_hotels:
            try:
                # Analyze risk factors
                risk_analysis = self._analyze_hotel_risk_factors(hotel)
                
                # If risk score meets threshold, add to results
                if risk_analysis['risk_score'] >= criteria.get('min_risk_score', 3):
                    hotel_with_risk = hotel.copy()
                    hotel_with_risk['risk_analysis'] = risk_analysis
                    results.append(hotel_with_risk)
            except Exception as e:
                logger.error(f"Error analyzing hotel {hotel.get('id')}: {e}")
        
        # Sort by risk score (highest first)
        results.sort(key=lambda x: x.get('risk_analysis', {}).get('risk_score', 0), reverse=True)
        
        # Limit results if requested
        max_results = criteria.get('max_results')
        if max_results and len(results) > max_results:
            results = results[:max_results]
        
        logger.info(f"Found {len(results)} hotels with potential violations")
        return results
    
    def _analyze_hotel_risk_factors(self, hotel: Dict) -> Dict:
        """
        Analyze risk factors for a hotel
        
        Args:
            hotel: Hotel data
            
        Returns:
            Dict: Risk analysis results
        """
        # Initialize risk analysis
        risk_analysis = {
            'risk_factors': [],
            'risk_score': 0,
            'risk_level': 'low'
        }
        
        # 1. Check for sparse data
        missing_fields = []
        essential_fields = ['name', 'address', 'phone', 'email', 'website', 'stars']
        
        for field in essential_fields:
            if not hotel.get(field):
                missing_fields.append(field)
        
        if missing_fields:
            severity = 'high' if len(missing_fields) >= 4 else 'medium' if len(missing_fields) >= 2 else 'low'
            risk_analysis['risk_factors'].append({
                'type': 'sparse_data',
                'severity': severity,
                'details': f"Missing {len(missing_fields)} essential fields: {', '.join(missing_fields)}"
            })
            
            # Add to risk score
            if severity == 'high':
                risk_analysis['risk_score'] += 3
            elif severity == 'medium':
                risk_analysis['risk_score'] += 2
            else:
                risk_analysis['risk_score'] += 1
        
        # 2. Check for recent changes (if history is available)
        if hotel.get('id') and self.db and hasattr(self.db, 'get_hotel_history'):
            try:
                # Get recent history (last 90 days)
                cutoff_date = datetime.now() - timedelta(days=90)
                cutoff_str = cutoff_date.isoformat()
                
                history = self.db.get_hotel_history(hotel.get('id'), after_date=cutoff_str)
                
                if history:
                    # Check for significant changes
                    significant_changes = []
                    
                    for record in history:
                        event_type = record.get('event_type', '').lower()
                        
                        # Significant change types
                        if ('name' in event_type or 
                            'owner' in event_type or 
                            'status' in event_type or 
                            'classification' in event_type):
                            significant_changes.append(record)
                    
                    if significant_changes:
                        severity = 'high' if len(significant_changes) >= 3 else 'medium' if len(significant_changes) >= 2 else 'low'
                        risk_analysis['risk_factors'].append({
                            'type': 'recent_significant_changes',
                            'severity': severity,
                            'details': f"{len(significant_changes)} significant changes in last 90 days"
                        })
                        
                        # Add to risk score
                        if severity == 'high':
                            risk_analysis['risk_score'] += 3
                        elif severity == 'medium':
                            risk_analysis['risk_score'] += 2
                        else:
                            risk_analysis['risk_score'] += 1
            except Exception as e:
                logger.error(f"Error checking recent changes: {e}")
        
        # 3. Check for unofficial platforms (if available)
        if hotel.get('id') and self.db and hasattr(self.db, 'get_hotel_listings'):
            try:
                # Get hotel listings
                listings = self.db.get_hotel_listings(hotel.get('id'))
                
                if listings:
                    # Count official and unofficial platforms
                    official_count = 0
                    unofficial_count = 0
                    
                    for listing in listings:
                        is_official = listing.get('is_official')
                        if is_official:
                            official_count += 1
                        else:
                            unofficial_count += 1
                    
                    # Check for unusual patterns
                    if unofficial_count > 0 and official_count == 0:
                        # Only on unofficial platforms
                        risk_analysis['risk_factors'].append({
                            'type': 'unofficial_platforms_only',
                            'severity': 'high',
                            'details': f"Listed on {unofficial_count} unofficial platforms, no official platforms"
                        })
                        risk_analysis['risk_score'] += 3
                    elif unofficial_count > official_count:
                        # More unofficial than official
                        risk_analysis['risk_factors'].append({
                            'type': 'more_unofficial_platforms',
                            'severity': 'medium',
                            'details': f"Listed on more unofficial ({unofficial_count}) than official ({official_count}) platforms"
                        })
                        risk_analysis['risk_score'] += 2
                    elif unofficial_count > 0:
                        # Some unofficial platforms
                        risk_analysis['risk_factors'].append({
                            'type': 'some_unofficial_platforms',
                            'severity': 'low',
                            'details': f"Listed on {unofficial_count} unofficial platforms"
                        })
                        risk_analysis['risk_score'] += 1
            except Exception as e:
                logger.error(f"Error checking platforms: {e}")
        
        # 4. Check classification vs. actual facilities
        if hotel.get('stars') and hotel.get('facilities'):
            try:
                stars = float(hotel.get('stars'))
                facilities = hotel.get('facilities', '').lower()
                
                # Define expected facilities by star rating
                expected_facilities = {
                    5: ['restaurant', 'pool', 'spa', 'gym', 'room service', 'concierge'],
                    4: ['restaurant', 'pool', 'gym', 'room service'],
                    3: ['restaurant', 'room service'],
                    2: [],
                    1: []
                }
                
                # Check for star rating vs facilities mismatch
                if stars >= 4:
                    expected = expected_facilities[5 if stars >= 5 else 4]
                    missing = [f for f in expected if f not in facilities]
                    
                    if len(missing) >= len(expected) // 2:
                        risk_analysis['risk_factors'].append({
                            'type': 'facilities_rating_mismatch',
                            'severity': 'medium',
                            'details': f"{stars}-star rating but missing key facilities: {', '.join(missing)}"
                        })
                        risk_analysis['risk_score'] += 2
            except Exception as e:
                logger.error(f"Error checking facilities vs rating: {e}")
        
        # 5. Check for residential area location
        if hotel.get('latitude') and hotel.get('longitude') and self.scraper and hasattr(self.scraper, 'check_location_type'):
            try:
                location_type = self.scraper.check_location_type(
                    hotel.get('latitude'),
                    hotel.get('longitude')
                )
                
                if location_type == 'residential':
                    risk_analysis['risk_factors'].append({
                        'type': 'residential_location',
                        'severity': 'medium',
                        'details': "Hotel located in residential area"
                    })
                    risk_analysis['risk_score'] += 2
            except Exception as e:
                logger.error(f"Error checking location type: {e}")
        
        # 6. Check existing risk_score/verification_status if available
        if hotel.get('risk_score'):
            try:
                existing_score = float(hotel.get('risk_score'))
                if existing_score >= 7:
                    risk_analysis['risk_factors'].append({
                        'type': 'existing_high_risk',
                        'severity': 'high',
                        'details': f"Existing high risk score: {existing_score}"
                    })
                    risk_analysis['risk_score'] += 3
                elif existing_score >= 4:
                    risk_analysis['risk_factors'].append({
                        'type': 'existing_medium_risk',
                        'severity': 'medium',
                        'details': f"Existing medium risk score: {existing_score}"
                    })
                    risk_analysis['risk_score'] += 2
            except Exception as e:
                logger.error(f"Error checking existing risk score: {e}")
        
        if hotel.get('verification_status') == 'unverified':
            risk_analysis['risk_factors'].append({
                'type': 'unverified',
                'severity': 'medium',
                'details': "Hotel not verified"
            })
            risk_analysis['risk_score'] += 2
        
        # Set overall risk level based on score
        if risk_analysis['risk_score'] >= 7:
            risk_analysis['risk_level'] = 'high'
        elif risk_analysis['risk_score'] >= 3:
            risk_analysis['risk_level'] = 'medium'
        else:
            risk_analysis['risk_level'] = 'low'
        
        return risk_analysis
    
    def check_data_inconsistencies(self, hotel_id: str = None, hotel_data: Dict = None) -> Dict:
        """
        Check for data inconsistencies between sources
        
        Args:
            hotel_id: Hotel ID
            hotel_data: Hotel data (if ID not provided)
            
        Returns:
            Dict: Inconsistency analysis
        """
        logger.info(f"Checking data inconsistencies for hotel ID: {hotel_id}")
        
        # Get hotel data if ID provided but no data
        if hotel_id and not hotel_data and self.db and hasattr(self.db, 'get_hotel_by_id'):
            try:
                hotel_data = self.db.get_hotel_by_id(hotel_id)
            except Exception as e:
                logger.error(f"Error retrieving hotel data: {e}")
                return {"error": f"Database error: {str(e)}"}
        
        if not hotel_data:
            logger.error("No hotel data provided")
            return {"error": "No hotel data provided"}
        
        # Initialize result
        result = {
            'hotel_id': hotel_id,
            'hotel_name': hotel_data.get('name', 'Unknown'),
            'inconsistencies': [],
            'data_sources': [],
            'risk_score': 0
        }
        
        # Get data from all available sources
        source_data = {}
        
        # 1. Main database record
        source_data['database'] = hotel_data
        result['data_sources'].append('database')
        
        # 2. Platform listings data
        if hotel_id and self.db and hasattr(self.db, 'get_hotel_listings'):
            try:
                listings = self.db.get_hotel_listings(hotel_id)
                
                for listing in listings:
                    platform = listing.get('platform_name', listing.get('platform_id', 'unknown'))
                    source_data[f"platform_{platform}"] = listing
                    result['data_sources'].append(f"platform_{platform}")
            except Exception as e:
                logger.error(f"Error retrieving hotel listings: {e}")
        
        # 3. Check latest from APIs if available
        if self.scraper and hotel_data.get('place_id') and hasattr(self.scraper, '_get_google_place_details'):
            try:
                google_details = self.scraper._get_google_place_details(hotel_data.get('place_id'))
                if google_details:
                    source_data['google_places'] = google_details
                    result['data_sources'].append('google_places')
            except Exception as e:
                logger.error(f"Error retrieving Google Places details: {e}")
        
        if self.scraper and hotel_data.get('osm_id') and hasattr(self.scraper, '_get_osm_details'):
            try:
                osm_id = hotel_data.get('osm_id')
                osm_type = hotel_data.get('osm_type', '')
                osm_id_str = f"{osm_type}:{osm_id}" if osm_type else osm_id
                
                osm_details = self.scraper._get_osm_details(osm_id_str)
                if osm_details:
                    source_data['openstreetmap'] = osm_details
                    result['data_sources'].append('openstreetmap')
            except Exception as e:
                logger.error(f"Error retrieving OpenStreetMap details: {e}")
        
        # If website is available, try to extract data
        if self.scraper and hotel_data.get('website') and hasattr(self.scraper, 'extract_hotel_website_info'):
            try:
                website_info = self.scraper.extract_hotel_website_info(hotel_data.get('website'))
                if website_info:
                    source_data['website'] = website_info
                    result['data_sources'].append('website')
            except Exception as e:
                logger.error(f"Error extracting website information: {e}")
        
        # Check for inconsistencies across sources
        if len(source_data) >= 2:
            # 1. Name inconsistencies
            name_values = {}
            for source, data in source_data.items():
                if isinstance(data, dict) and data.get('name'):
                    name = data.get('name').lower()
                    if name not in name_values:
                        name_values[name] = []
                    name_values[name].append(source)
            
            if len(name_values) >= 2:
                # Multiple different names
                result['inconsistencies'].append({
                    'field': 'name',
                    'severity': 'high' if len(name_values) >= 3 else 'medium',
                    'values': {name: sources for name, sources in name_values.items()}
                })
                
                # Add to risk score
                result['risk_score'] += 3 if len(name_values) >= 3 else 2
        # 2. Address inconsistencies
            address_values = {}
            for source, data in source_data.items():
                if isinstance(data, dict) and data.get('address'):
                    address = data.get('address').lower()
                    if address not in address_values:
                        address_values[address] = []
                    address_values[address].append(source)
            
            if len(address_values) >= 2:
                # Multiple different addresses
                result['inconsistencies'].append({
                    'field': 'address',
                    'severity': 'high',
                    'values': {addr: sources for addr, sources in address_values.items()}
                })
                
                # Add to risk score
                result['risk_score'] += 3
            
            # 3. Contact information inconsistencies
            for field in ['phone', 'email', 'website']:
                field_values = {}
                for source, data in source_data.items():
                    if isinstance(data, dict) and data.get(field):
                        value = data.get(field).lower()
                        if value not in field_values:
                            field_values[value] = []
                        field_values[value].append(source)
                
                if len(field_values) >= 2:
                    # Multiple different values
                    result['inconsistencies'].append({
                        'field': field,
                        'severity': 'medium',
                        'values': {val: sources for val, sources in field_values.items()}
                    })
                    
                    # Add to risk score
                    result['risk_score'] += 2
            
            # 4. Star rating inconsistencies
            stars_values = {}
            for source, data in source_data.items():
                if isinstance(data, dict) and data.get('stars'):
                    try:
                        stars = float(data.get('stars'))
                        if stars not in stars_values:
                            stars_values[stars] = []
                        stars_values[stars].append(source)
                    except:
                        pass
            
            if len(stars_values) >= 2:
                # Check for significant difference (more than 1 star)
                max_diff = max(stars_values.keys()) - min(stars_values.keys())
                if max_diff > 1:
                    result['inconsistencies'].append({
                        'field': 'stars',
                        'severity': 'high' if max_diff >= 2 else 'medium',
                        'values': {f"{stars}★": sources for stars, sources in stars_values.items()}
                    })
                    
                    # Add to risk score
                    result['risk_score'] += 3 if max_diff >= 2 else 2
        
        # Set overall risk level based on score
        if result['risk_score'] >= 6:
            result['risk_level'] = 'high'
        elif result['risk_score'] >= 3:
            result['risk_level'] = 'medium'
        else:
            result['risk_level'] = 'low'
        
        logger.info(f"Found {len(result['inconsistencies'])} inconsistencies for hotel {hotel_data.get('name')}")
        return result
    
    def detect_dual_activity(self, hotel_id: str = None, hotel_data: Dict = None) -> Dict:
        """
        Detect hotels with dual activity (e.g., hotel and residential)
        
        Args:
            hotel_id: Hotel ID
            hotel_data: Hotel data (if ID not provided)
            
        Returns:
            Dict: Dual activity analysis
        """
        logger.info(f"Detecting dual activity for hotel ID: {hotel_id}")
        
        # Get hotel data if ID provided but no data
        if hotel_id and not hotel_data and self.db and hasattr(self.db, 'get_hotel_by_id'):
            try:
                hotel_data = self.db.get_hotel_by_id(hotel_id)
            except Exception as e:
                logger.error(f"Error retrieving hotel data: {e}")
                return {"error": f"Database error: {str(e)}"}
        
        if not hotel_data:
            logger.error("No hotel data provided")
            return {"error": "No hotel data provided"}
        
        # Initialize result
        result = {
            'hotel_id': hotel_id,
            'hotel_name': hotel_data.get('name', 'Unknown'),
            'dual_activity_indicators': [],
            'risk_score': 0
        }
        
        # 1. Check for listings on both hotel and rental platforms
        if hotel_id and self.db and hasattr(self.db, 'get_hotel_listings'):
            try:
                listings = self.db.get_hotel_listings(hotel_id)
                
                hotel_platforms = []
                rental_platforms = []
                
                for listing in listings:
                    platform_type = listing.get('platform_type', '').lower()
                    platform_name = listing.get('platform_name', listing.get('platform_id', 'unknown'))
                    
                    if 'hotel' in platform_type or platform_name.lower() in ['booking.com', 'expedia', 'hotels.com', 'tripadvisor']:
                        hotel_platforms.append(platform_name)
                    
                    if 'rental' in platform_type or 'apartment' in platform_type or platform_name.lower() in ['airbnb', 'vrbo', 'booking_apartments']:
                        rental_platforms.append(platform_name)
                
                if hotel_platforms and rental_platforms:
                    result['dual_activity_indicators'].append({
                        'type': 'dual_platform_listings',
                        'severity': 'high',
                        'details': f"Listed on both hotel platforms ({', '.join(hotel_platforms)}) and rental platforms ({', '.join(rental_platforms)})"
                    })
                    
                    # Add to risk score
                    result['risk_score'] += 3
            except Exception as e:
                logger.error(f"Error checking platform listings: {e}")
        
        # 2. Check for mixed classification/terms in name and description
        if hotel_data.get('name') or hotel_data.get('additional_info'):
            try:
                # Combine name and description for analysis
                text_to_analyze = hotel_data.get('name', '').lower()
                
                if hotel_data.get('additional_info'):
                    try:
                        additional_info = json.loads(hotel_data.get('additional_info', '{}'))
                        if additional_info.get('description'):
                            text_to_analyze += ' ' + additional_info.get('description').lower()
                    except:
                        pass
                
                # Look for mixed terminology
                hotel_terms = ['hotel', 'resort', 'inn', 'motel', 'lodge']
                residential_terms = ['apartment', 'residence', 'flat', 'condo', 'studio', 'home', 'house']
                
                found_hotel_terms = [term for term in hotel_terms if term in text_to_analyze]
                found_residential_terms = [term for term in residential_terms if term in text_to_analyze]
                
                if found_hotel_terms and found_residential_terms:
                    result['dual_activity_indicators'].append({
                        'type': 'mixed_terminology',
                        'severity': 'medium',
                        'details': f"Uses both hotel terms ({', '.join(found_hotel_terms)}) and residential terms ({', '.join(found_residential_terms)})"
                    })
                    
                    # Add to risk score
                    result['risk_score'] += 2
            except Exception as e:
                logger.error(f"Error analyzing name and description: {e}")
        
        # 3. Check for location type mismatch
        if hotel_data.get('latitude') and hotel_data.get('longitude') and self.scraper and hasattr(self.scraper, 'check_location_type'):
            try:
                location_type = self.scraper.check_location_type(
                    hotel_data.get('latitude'),
                    hotel_data.get('longitude')
                )
                
                if location_type == 'residential' and hotel_data.get('stars', 0) >= 3:
                    result['dual_activity_indicators'].append({
                        'type': 'high_end_hotel_residential_area',
                        'severity': 'high',
                        'details': f"{hotel_data.get('stars')}★ hotel in residential area"
                    })
                    
                    # Add to risk score
                    result['risk_score'] += 3
            except Exception as e:
                logger.error(f"Error checking location type: {e}")
        
        # 4. Check for historical classification changes
        if hotel_id and self.db and hasattr(self.db, 'get_hotel_history'):
            try:
                history = self.db.get_hotel_history(hotel_id)
                
                # Look for classification or status changes
                classification_changes = [h for h in history if 'classification' in h.get('event_type', '').lower() or 'status' in h.get('event_type', '').lower()]
                
                for change in classification_changes:
                    old_value = change.get('old_value', '').lower()
                    new_value = change.get('new_value', '').lower()
                    
                    # Look for switches between hotel and residential
                    hotel_to_residential = any(term in old_value for term in hotel_terms) and any(term in new_value for term in residential_terms)
                    residential_to_hotel = any(term in old_value for term in residential_terms) and any(term in new_value for term in hotel_terms)
                    
                    if hotel_to_residential or residential_to_hotel:
                        result['dual_activity_indicators'].append({
                            'type': 'classification_switch',
                            'severity': 'high',
                            'details': f"Changed classification from '{old_value}' to '{new_value}' on {change.get('event_date', '')}"
                        })
                        
                        # Add to risk score
                        result['risk_score'] += 3
                        break  # One such change is enough
            except Exception as e:
                logger.error(f"Error checking classification history: {e}")
        
        # Set overall risk level based on score
        if result['risk_score'] >= 5:
            result['risk_level'] = 'high'
        elif result['risk_score'] >= 2:
            result['risk_level'] = 'medium'
        else:
            result['risk_level'] = 'low'
        
        logger.info(f"Found {len(result['dual_activity_indicators'])} dual activity indicators for hotel {hotel_data.get('name')}")
        return result
    
    def analyze_unofficial_hotels(self, criteria: Dict = None) -> List[Dict]:
        """
        Analyze hotels not found in official records
        
        Args:
            criteria: Analysis criteria
            
        Returns:
            List[Dict]: List of unofficial hotels
        """
        logger.info(f"Analyzing unofficial hotels with criteria: {criteria}")
        
        # Set default criteria if not provided
        if not criteria:
            criteria = {
                'min_listing_age_days': 30,
                'include_inactive': False,
                'max_results': 100
            }
        
        # Initialize results
        results = []
        
        # 1. Check if we have access to official databases
        has_official_db = False
        if hasattr(self, 'has_official_records_access'):
            has_official_db = self.has_official_records_access()
        
        if has_official_db:
            # Direct comparison with official database
            if self.db and hasattr(self.db, 'get_all_hotels'):
                try:
                    all_hotels = self.db.get_all_hotels()
                    logger.info(f"Retrieved {len(all_hotels)} hotels for official records check")
                    
                    for hotel in all_hotels:
                        # Check each hotel against official records
                        if self.check_hotel_in_official_records(hotel) is False:
                            # Hotel not in official records
                            hotel_copy = hotel.copy()
                            hotel_copy['unofficial_status'] = {
                                'in_official_records': False,
                                'verification_method': 'direct_database_check',
                                'risk_level': 'high'
                            }
                            results.append(hotel_copy)
                except Exception as e:
                    logger.error(f"Error checking against official records: {e}")
        else:
            # No direct access to official database, use heuristics
            if self.db and hasattr(self.db, 'get_all_hotels') and hasattr(self.db, 'get_hotel_listings'):
                try:
                    all_hotels = self.db.get_all_hotels()
                    logger.info(f"Retrieved {len(all_hotels)} hotels for unofficial status analysis")
                    
                    for hotel in all_hotels:
                        hotel_id = hotel.get('id')
                        if not hotel_id:
                            continue
                        
                        # Get hotel listings
                        try:
                            listings = self.db.get_hotel_listings(hotel_id)
                            
                            # Check if hotel is only on unofficial platforms
                            if listings:
                                official_listings = [l for l in listings if l.get('is_official')]
                                unofficial_listings = [l for l in listings if not l.get('is_official')]
                                
                                if unofficial_listings and not official_listings:
                                    # Only listed on unofficial platforms
                                    hotel_copy = hotel.copy()
                                    hotel_copy['unofficial_status'] = {
                                        'in_official_records': False,
                                        'verification_method': 'platform_listings_check',
                                        'risk_level': 'high',
                                        'unofficial_platforms': [l.get('platform_name', l.get('platform_id')) for l in unofficial_listings]
                                    }
                                    results.append(hotel_copy)
                        except Exception as e:
                            logger.error(f"Error checking hotel listings: {e}")
                except Exception as e:
                    logger.error(f"Error retrieving hotels: {e}")
        
        # Apply additional criteria
        filtered_results = []
        for hotel in results:
            # Check listing age if criteria specified
            if criteria.get('min_listing_age_days'):
                # Get the oldest listing
                listings = self.db.get_hotel_listings(hotel.get('id')) if self.db and hasattr(self.db, 'get_hotel_listings') else []
                oldest_listing_date = None
                
                for listing in listings:
                    first_seen = listing.get('first_seen_date')
                    if first_seen:
                        try:
                            listing_date = datetime.fromisoformat(first_seen.split('T')[0])
                            if oldest_listing_date is None or listing_date < oldest_listing_date:
                                oldest_listing_date = listing_date
                        except:
                            pass
                
                # Skip if listing is too new
                if oldest_listing_date:
                    days_old = (datetime.now() - oldest_listing_date).days
                    if days_old < criteria.get('min_listing_age_days'):
                        continue
            
            # Check active status if criteria specified
            if not criteria.get('include_inactive') and hotel.get('operation_status') == 'inactive':
                continue
            
            filtered_results.append(hotel)
        
        # Sort by risk level
        filtered_results.sort(key=lambda x: 0 if x.get('unofficial_status', {}).get('risk_level') == 'high' else 
                                         (1 if x.get('unofficial_status', {}).get('risk_level') == 'medium' else 2))
        
        # Limit results if requested
        max_results = criteria.get('max_results')
        if max_results and len(filtered_results) > max_results:
            filtered_results = filtered_results[:max_results]
        
        logger.info(f"Found {len(filtered_results)} unofficial hotels")
        return filtered_results
    
    def check_hotel_in_official_records(self, hotel: Dict) -> Optional[bool]:
        """
        Check if a hotel exists in official records
        
        Args:
            hotel: Hotel data
            
        Returns:
            Optional[bool]: True if found, False if not found, None if check failed
        """
        # This is a placeholder - actual implementation would depend on
        # available official data sources and APIs
        logger.warning("check_hotel_in_official_records is not fully implemented")
        return None
    
    def detect_unusual_clusters(self, criteria: Dict = None) -> List[Dict]:
        """
        Detect unusual hotel clusters
        
        Args:
            criteria: Detection criteria
            
        Returns:
            List[Dict]: List of unusual clusters
        """
        logger.info(f"Detecting unusual hotel clusters with criteria: {criteria}")
        
        # Set default criteria if not provided
        if not criteria:
            criteria = {
                'min_cluster_size': 3,
                'max_distance_meters': 100,
                'min_risk_score': 3
            }
        
        # Initialize results
        results = []
        
        # Get all hotels with coordinates
        hotels_with_coords = []
        if self.db and hasattr(self.db, 'get_all_hotels'):
            try:
                all_hotels = self.db.get_all_hotels()
                hotels_with_coords = [h for h in all_hotels if h.get('latitude') and h.get('longitude')]
                logger.info(f"Retrieved {len(hotels_with_coords)} hotels with coordinates for cluster analysis")
            except Exception as e:
                logger.error(f"Error retrieving hotels: {e}")
                return []
        else:
            logger.error("Database not available for retrieval")
            return []
        
        # Use DBSCAN for clustering if available, otherwise use simple distance-based clustering
        if SKLEARN_AVAILABLE and len(hotels_with_coords) >= criteria.get('min_cluster_size', 3):
            try:
                # Extract coordinates
                coords = np.array([[h.get('latitude'), h.get('longitude')] for h in hotels_with_coords])
                
                # Convert distance threshold from meters to degrees (approximate)
                # 1 degree ≈ 111,000 meters at equator
                eps = criteria.get('max_distance_meters', 100) / 111000
                
                # Run DBSCAN
                clustering = DBSCAN(eps=eps, min_samples=criteria.get('min_cluster_size', 3)).fit(coords)
                
                # Process clusters
                clusters = {}
                for i, label in enumerate(clustering.labels_):
                    if label >= 0:  # Skip noise points (-1)
                        if label not in clusters:
                            clusters[label] = []
                        clusters[label].append(hotels_with_coords[i])
                
                # Analyze each cluster
                for cluster_id, cluster_hotels in clusters.items():
                    cluster_analysis = self._analyze_hotel_cluster(cluster_hotels, criteria)
                    if cluster_analysis:
                        results.append(cluster_analysis)
            except Exception as e:
                logger.error(f"Error performing DBSCAN clustering: {e}")
                # Fall back to simple clustering
                results = self._simple_cluster_analysis(hotels_with_coords, criteria)
        else:
            # Use simple distance-based clustering
            results = self._simple_cluster_analysis(hotels_with_coords, criteria)
        
        logger.info(f"Found {len(results)} unusual hotel clusters")
        return results
    
    def _simple_cluster_analysis(self, hotels: List[Dict], criteria: Dict) -> List[Dict]:
        """
        Perform simple distance-based cluster analysis
        
        Args:
            hotels: List of hotels with coordinates
            criteria: Detection criteria
            
        Returns:
            List[Dict]: List of unusual clusters
        """
        results = []
        processed = set()
        
        for i, hotel in enumerate(hotels):
            if i in processed:
                continue
            
            lat1 = hotel.get('latitude')
            lng1 = hotel.get('longitude')
            
            cluster = [hotel]
            processed.add(i)
            
            # Find nearby hotels
            for j, other_hotel in enumerate(hotels):
                if j in processed or i == j:
                    continue
                
                lat2 = other_hotel.get('latitude')
                lng2 = other_hotel.get('longitude')
                
                # Calculate distance
                distance_meters = self._haversine_distance(lat1, lng1, lat2, lng2) * 1000
                
                if distance_meters <= criteria.get('max_distance_meters', 100):
                    cluster.append(other_hotel)
                    processed.add(j)
            
            # Check if cluster meets size criteria
            if len(cluster) >= criteria.get('min_cluster_size', 3):
                cluster_analysis = self._analyze_hotel_cluster(cluster, criteria)
                if cluster_analysis:
                    results.append(cluster_analysis)
        
        return results
    
    def _analyze_hotel_cluster(self, cluster_hotels: List[Dict], criteria: Dict) -> Optional[Dict]:
        """
        Analyze a hotel cluster for unusual patterns
        
        Args:
            cluster_hotels: List of hotels in the cluster
            criteria: Detection criteria
            
        Returns:
            Optional[Dict]: Cluster analysis or None if not unusual
        """
        # Skip small clusters
        if len(cluster_hotels) < criteria.get('min_cluster_size', 3):
            return None
        
        # Calculate center point
        lat_sum = sum(float(h.get('latitude', 0)) for h in cluster_hotels)
        lng_sum = sum(float(h.get('longitude', 0)) for h in cluster_hotels)
        center_lat = lat_sum / len(cluster_hotels)
        center_lng = lng_sum / len(cluster_hotels)
        
        # Initialize cluster data
        cluster_data = {
            'center': {
                'latitude': center_lat,
                'longitude': center_lng
            },
            'radius_meters': criteria.get('max_distance_meters', 100),
            'hotels': cluster_hotels,
            'hotel_count': len(cluster_hotels),
            'patterns': [],
            'risk_score': 0
        }
        
        # Check for unusual patterns
        
        # 1. Different owners in close proximity
        owners = set()
        for hotel in cluster_hotels:
            owner = None
            
            # Try to extract owner information
            if hotel.get('ownership_info'):
                try:
                    owner_info = json.loads(hotel.get('ownership_info', '{}'))
                    owner = owner_info.get('owner_name')
                except:
                    pass
            
            if not owner and hotel.get('additional_info'):
                try:
                    additional_info = json.loads(hotel.get('additional_info', '{}'))
                    owner = (additional_info.get('owner_name') or 
                            additional_info.get('owner') or 
                            additional_info.get('operator'))
                except:
                    pass
            
            if owner:
                owners.add(owner)
        
        # Different owners in close proximity
        if len(owners) > 1:
            cluster_data['patterns'].append({
                'type': 'multiple_owners',
                'severity': 'medium',
                'details': f"{len(owners)} different owners in close proximity"
            })
            cluster_data['risk_score'] += 2
        
        # 2. Mixture of official and unofficial hotels
        official_count = 0
        unofficial_count = 0
        
        for hotel in cluster_hotels:
            # Check verification status
            if hotel.get('verification_status') == 'verified':
                official_count += 1
            else:
                unofficial_count += 1
        
        if official_count > 0 and unofficial_count > 0:
            cluster_data['patterns'].append({
                'type': 'official_unofficial_mix',
                'severity': 'high',
                'details': f"Mix of official ({official_count}) and unofficial ({unofficial_count}) hotels in close proximity"
            })
            cluster_data['risk_score'] += 3
        
        # 3. Unusual density
        # Average hotel density varies by location, but high density in small area is suspicious
        if len(cluster_hotels) >= 5 and criteria.get('max_distance_meters', 100) <= 100:
            cluster_data['patterns'].append({
                'type': 'high_density',
                'severity': 'medium',
                'details': f"High density of {len(cluster_hotels)} hotels within {criteria.get('max_distance_meters', 100)}m radius"
            })
            cluster_data['risk_score'] += 2
        
        # 4. Similar names or patterns
        names = [h.get('name', '').lower() for h in cluster_hotels if h.get('name')]
        if names:
            # Check for similar prefixes/suffixes
            name_parts = []
            for name in names:
                parts = name.split()
                if parts:
                    name_parts.extend(parts)
            
            # Count frequency of each word
            word_counts = {}
            for part in name_parts:
                if len(part) >= 4:  # Skip short words
                    word_counts[part] = word_counts.get(part, 0) + 1
            
            # Find common words
            common_words = [word for word, count in word_counts.items() if count >= len(cluster_hotels) / 2]
            
            if common_words:
                cluster_data['patterns'].append({
                    'type': 'similar_naming',
                    'severity': 'medium',
                    'details': f"Similar naming patterns using words: {', '.join(common_words)}"
                })
                cluster_data['risk_score'] += 2
        
        # Skip if no patterns or low risk score
        if not cluster_data['patterns'] or cluster_data['risk_score'] < criteria.get('min_risk_score', 3):
            return None
        
        # Set overall risk level
        if cluster_data['risk_score'] >= 6:
            cluster_data['risk_level'] = 'high'
        elif cluster_data['risk_score'] >= 3:
            cluster_data['risk_level'] = 'medium'
        else:
            cluster_data['risk_level'] = 'low'
        
        return cluster_data
    
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
    
    def analyze_evasion_patterns(self, criteria: Dict = None) -> List[Dict]:
        """
        Analyze known evasion patterns
        
        Args:
            criteria: Analysis criteria
            
        Returns:
            List[Dict]: List of hotels with evasion patterns
        """
        logger.info(f"Analyzing evasion patterns with criteria: {criteria}")
        
        # Set default criteria if not provided
        if not criteria:
            criteria = {
                'min_confidence': 0.7,
                'max_results': 100,
                'include_inactive': False
            }
        
        # Define known evasion patterns to check
        evasion_patterns = [
            {
                'name': 'name_hotel_without_license',
                'description': 'Property uses "hotel" in name but operates without hotel license',
                'check_function': self._check_hotel_in_name_without_license,
                'severity': 'high'
            },
            {
                'name': 'residential_with_high_turnover',
                'description': 'Residential property with hotel-like turnover rate',
                'check_function': self._check_residential_high_turnover,
                'severity': 'high'
            },
            {
                'name': 'unregistered_chain_operation',
                'description': 'Multiple properties operated by same owner without proper registration',
                'check_function': self._check_unregistered_chain,
                'severity': 'high'
            },
            {
                'name': 'split_property_listings',
                'description': 'Single property listed as multiple separate units to avoid regulations',
                'check_function': self._check_split_property,
                'severity': 'medium'
            },
            {
                'name': 'platform_switching',
                'description': 'Property switches between different booking platforms to avoid detection',
                'check_function': self._check_platform_switching,
                'severity': 'medium'
            }
        ]
        
        # Get all hotels
        all_hotels = []
        if self.db and hasattr(self.db, 'get_all_hotels'):
            try:
                all_hotels = self.db.get_all_hotels()
                
                # Filter inactive if requested
                if not criteria.get('include_inactive'):
                    all_hotels = [h for h in all_hotels if h.get('operation_status') != 'inactive']
                
                logger.info(f"Retrieved {len(all_hotels)} hotels for evasion pattern analysis")
            except Exception as e:
                logger.error(f"Error retrieving hotels: {e}")
                return []
        else:
            logger.error("Database not available for retrieval")
            return []
        
        # Check each hotel against each pattern
        results = []
        
        for hotel in all_hotels:
            hotel_id = hotel.get('id')
            if not hotel_id:
                continue
            
            # Check each pattern
            detected_patterns = []
            
            for pattern in evasion_patterns:
                try:
                    detection_result = pattern['check_function'](hotel)
                    
                    if detection_result and detection_result.get('detected'):
                        confidence = detection_result.get('confidence', 0)
                        
                        if confidence >= criteria.get('min_confidence', 0.7):
                            detected_patterns.append({
                                'pattern': pattern['name'],
                                'description': pattern['description'],
                                'severity': pattern['severity'],
                                'confidence': confidence,
                                'details': detection_result.get('details', '')
                            })
                except Exception as e:
                    logger.error(f"Error checking pattern {pattern['name']} for hotel {hotel_id}: {e}")
            
            if detected_patterns:
                # Calculate overall risk score
                risk_score = sum(3 if p['severity'] == 'high' else 2 if p['severity'] == 'medium' else 1 
                                for p in detected_patterns)
                
                # Add to results
                hotel_with_patterns = hotel.copy()
                hotel_with_patterns['evasion_patterns'] = detected_patterns
                hotel_with_patterns['risk_score'] = risk_score
                hotel_with_patterns['risk_level'] = 'high' if risk_score >= 5 else 'medium' if risk_score >= 3 else 'low'
                
                results.append(hotel_with_patterns)
        
        # Sort by risk score (highest first)
        results.sort(key=lambda x: x.get('risk_score', 0), reverse=True)
        
        # Limit results if requested
        max_results = criteria.get('max_results')
        if max_results and len(results) > max_results:
            results = results[:max_results]
        
        logger.info(f"Found {len(results)} hotels with evasion patterns")
        return results
    
    # Pattern check functions
    
    def _check_hotel_in_name_without_license(self, hotel: Dict) -> Dict:
        """
        Check if property uses "hotel" in name but operates without hotel license
        
        Args:
            hotel: Hotel data
            
        Returns:
            Dict: Detection result
        """
        result = {
            'detected': False,
            'confidence': 0,
            'details': ''
        }
        
        # Check if "hotel" is in the name
        if hotel.get('name') and 'hotel' in hotel.get('name', '').lower():
            # Check if it has proper license
            has_license = False
            
            # Check legal_status field
            if hotel.get('legal_status'):
                legal_status = hotel.get('legal_status', '').lower()
                if 'hotel' in legal_status and 'license' in legal_status:
                    has_license = True
            
            # Check for license info in additional_info
            if not has_license and hotel.get('additional_info'):
                try:
                    additional_info = json.loads(hotel.get('additional_info', '{}'))
                    
                    # Look for license information
                    for key, value in additional_info.items():
                        if ('license' in key.lower() or 'permit' in key.lower()) and value:
                            has_license = True
                            break
                except:
                    pass
            
            # If no license found, mark as detected
            if not has_license:
                result['detected'] = True
                result['confidence'] = 0.8
                result['details'] = f"Property uses 'hotel' in name ({hotel.get('name')}) but no license information found"
        
        return result
    
    def _check_residential_high_turnover(self, hotel: Dict) -> Dict:
        """
        Check if residential property has hotel-like turnover rate
        
        Args:
            hotel: Hotel data
            
        Returns:
            Dict: Detection result
        """
        result = {
            'detected': False,
            'confidence': 0,
            'details': ''
        }
        
        # Check if property is classified as residential
        is_residential = False
        
        if hotel.get('legal_status'):
            legal_status = hotel.get('legal_status', '').lower()
            if 'residential' in legal_status or 'apartment' in legal_status:
                is_residential = True
        
        if not is_residential and hotel.get('name'):
            # Check name for residential indicators
            name = hotel.get('name', '').lower()
            if any(term in name for term in ['apartment', 'residence', 'flat', 'condo', 'studio']):
                is_residential = True
        
        if not is_residential:
            return result  # Not a residential property
        
        # Check for high turnover indicators
        high_turnover = False
        turnover_indicators = []
        
        # Check listings on hotel booking platforms
        if hotel.get('id') and self.db and hasattr(self.db, 'get_hotel_listings'):
            try:
                listings = self.db.get_hotel_listings(hotel.get('id'))
                
                # Count listings on hotel booking platforms
                hotel_platform_count = 0
                
                for listing in listings:
                    platform_type = listing.get('platform_type', '').lower()
                    platform_name = listing.get('platform_name', listing.get('platform_id', '')).lower()
                    
                    if ('hotel' in platform_type or 
                        platform_name in ['booking.com', 'expedia', 'hotels.com', 'tripadvisor']):
                        hotel_platform_count += 1
                
                if hotel_platform_count > 0:
                    high_turnover = True
                    turnover_indicators.append(f"Listed on {hotel_platform_count} hotel booking platforms")
            except Exception as e:
                logger.error(f"Error checking listings: {e}")
        
        # Check for short-term rental indicators in additional info
        if hotel.get('additional_info'):
            try:
                additional_info = json.loads(hotel.get('additional_info', '{}'))
                
                # Look for minimum stay information
                for key, value in additional_info.items():
                    if 'minimum stay' in key.lower() or 'min stay' in key.lower():
                        try:
                            # Parse minimum stay value
                            if isinstance(value, (int, float)):
                                min_stay = float(value)
                            elif isinstance(value, str):
                                # Extract number from string (e.g. "2 nights")
                                match = re.search(r'(\d+)', value)
                                if match:
                                    min_stay = float(match.group(1))
                                else:
                                    continue
                            else:
                                continue
                            
                            # If minimum stay is 7 days or less, consider high turnover
                            if min_stay <= 7:
                                high_turnover = True
                                turnover_indicators.append(f"Short minimum stay: {value}")
                        except:
                            pass
            except:
                pass
        
        # If high turnover indicators found, mark as detected
        if high_turnover:
            result['detected'] = True
            result['confidence'] = 0.7
            result['details'] = f"Residential property with hotel-like turnover indicators: {', '.join(turnover_indicators)}"
        
        return result
    
    def _check_unregistered_chain(self, hotel: Dict) -> Dict:
        """
        Check if hotel is part of an unregistered chain operation
        
        Args:
            hotel: Hotel data
            
        Returns:
            Dict: Detection result
        """
        result = {
            'detected': False,
            'confidence': 0,
            'details': ''
        }
        
        # This check requires analysis across multiple hotels
        # First, try to extract owner information
        owner = None
        
        if hotel.get('ownership_info'):
            try:
                owner_info = json.loads(hotel.get('ownership_info', '{}'))
                owner = owner_info.get('owner_name')
            except:
                pass
        
        if not owner and hotel.get('additional_info'):
            try:
                additional_info = json.loads(hotel.get('additional_info', '{}'))
                owner = (additional_info.get('owner_name') or 
                        additional_info.get('owner') or 
                        additional_info.get('operator'))
            except:
                pass
        
        if not owner:
            return result  # Cannot determine owner
        
        # Look for other hotels with same owner
        same_owner_hotels = []
        
        if self.db and hasattr(self.db, 'get_all_hotels'):
            try:
                all_hotels = self.db.get_all_hotels()
                
                for other_hotel in all_hotels:
                    if other_hotel.get('id') == hotel.get('id'):
                        continue  # Skip current hotel
                    
                    other_owner = None
                    
                    # Extract owner info using same logic as above
                    if other_hotel.get('ownership_info'):
                        try:
                            owner_info = json.loads(other_hotel.get('ownership_info', '{}'))
                            other_owner = owner_info.get('owner_name')
                        except:
                            pass
                    
                    if not other_owner and other_hotel.get('additional_info'):
                        try:
                            additional_info = json.loads(other_hotel.get('additional_info', '{}'))
                            other_owner = (additional_info.get('owner_name') or 
                                          additional_info.get('owner') or 
                                          additional_info.get('operator'))
                        except:
                            pass
                    
                    # Check if same owner
                    if other_owner and self._calculate_string_similarity(owner, other_owner) > 0.8:
                        same_owner_hotels.append(other_hotel)
            except Exception as e:
                logger.error(f"Error checking for same owner hotels: {e}")
        
        # If multiple properties found with same owner, check if they're properly registered
        if len(same_owner_hotels) >= 2:
            # Check if any are missing proper registration
            unregistered_count = 0
            
            # Check current hotel
            if not self._has_proper_registration(hotel):
                unregistered_count += 1
            
            # Check other hotels with same owner
            for other_hotel in same_owner_hotels:
                if not self._has_proper_registration(other_hotel):
                    unregistered_count += 1
            
            # If more than one is unregistered, mark as detected
            if unregistered_count >= 2:
                result['detected'] = True
                result['confidence'] = 0.7 + (min(unregistered_count, 5) - 1) * 0.05  # Increase confidence with more properties
                result['details'] = f"Found {unregistered_count} properties operated by '{owner}' without proper registration"
        
        return result
    
    def _has_proper_registration(self, hotel: Dict) -> bool:
        """
        Check if hotel has proper registration
        
        Args:
            hotel: Hotel data
            
        Returns:
            bool: True if properly registered, False otherwise
        """
        # Check legal_status field
        if hotel.get('legal_status'):
            legal_status = hotel.get('legal_status', '').lower()
            if 'registered' in legal_status or 'licensed' in legal_status:
                return True
        
        # Check for license info in additional_info
        if hotel.get('additional_info'):
            try:
                additional_info = json.loads(hotel.get('additional_info', '{}'))
                
                # Look for license or registration information
                for key, value in additional_info.items():
                    if any(term in key.lower() for term in ['license', 'permit', 'registration', 'certificate']) and value:
                        return True
            except:
                pass
        
        # Check verification_status
        if hotel.get('verification_status') == 'verified':
            return True
        
        # No registration info found
        return False
    
    def _check_split_property(self, hotel: Dict) -> Dict:
        """
        Check if property is split into multiple listings to avoid regulations
        
        Args:
            hotel: Hotel data
            
        Returns:
            Dict: Detection result
        """
        result = {
            'detected': False,
            'confidence': 0,
            'details': ''
        }
        
        # This pattern is hard to detect without detailed booking data
        # We'll look for indicators in the name and proximity to similar properties
        
        # Check for numbering in name (e.g., "Apartment 1", "Villa 2A")
        name = hotel.get('name', '').lower()
        has_numbering = bool(re.search(r'(apt|apartment|room|suite|unit|villa)\s+(\d+[a-z]?)', name))
        
        if not has_numbering:
            # Also check for common split property indicators
            split_indicators = ['studio', 'room in', 'part of', 'section', 'floor']
            has_split_indicator = any(indicator in name for indicator in split_indicators)
            
            if not has_split_indicator:
                return result  # No indicators found
        
        # Look for nearby properties with similar names
        similar_nearby = []
        
        if hotel.get('latitude') and hotel.get('longitude') and self.db and hasattr(self.db, 'get_all_hotels'):
            try:
                all_hotels = self.db.get_all_hotels()
                
                lat1 = hotel.get('latitude')
                lng1 = hotel.get('longitude')
                
                for other_hotel in all_hotels:
                    if other_hotel.get('id') == hotel.get('id'):
                        continue  # Skip current hotel
                    
                    lat2 = other_hotel.get('latitude')
                    lng2 = other_hotel.get('longitude')
                    
                    if not lat2 or not lng2:
                        continue
                    
                    # Check if nearby (within 50 meters)
                    distance = self._haversine_distance(lat1, lng1, lat2, lng2) * 1000
                    if distance <= 50:
                        # Check for name similarity
                        other_name = other_hotel.get('name', '').lower()
                        
                        # Look for common base name
                        base_name = re.sub(r'(apt|apartment|room|suite|unit|villa)\s+(\d+[a-z]?)', '', name).strip()
                        other_base_name = re.sub(r'(apt|apartment|room|suite|unit|villa)\s+(\d+[a-z]?)', '', other_name).strip()
                        
                        if base_name and other_base_name and self._calculate_string_similarity(base_name, other_base_name) > 0.7:
                            similar_nearby.append(other_hotel)
            except Exception as e:
                logger.error(f"Error checking for nearby similar properties: {e}")
        
        # If similar nearby properties found, mark as detected
        if similar_nearby:
            result['detected'] = True
            result['confidence'] = 0.6 + min(len(similar_nearby), 5) * 0.05  # Increase confidence with more properties
            result['details'] = f"Property appears to be part of split listing with {len(similar_nearby)} similar nearby properties"
        
        return result
    
    def _check_platform_switching(self, hotel: Dict) -> Dict:
        """
        Check if property switches between different booking platforms to avoid detection
        
        Args:
            hotel: Hotel data
            
        Returns:
            Dict: Detection result
        """
        result = {
            'detected': False,
            'confidence': 0,
            'details': ''
        }
        
        # Check platform listing history
        if hotel.get('id') and self.db and hasattr(self.db, 'get_hotel_listings') and hasattr(self.db, 'get_platform_history'):
            try:
                # Get current listings
                current_listings = self.db.get_hotel_listings(hotel.get('id'))
                
                # Get platform history
                platform_history = self.db.get_platform_history(hotel.get('id'))
                
                if platform_history:
                    # Look for pattern of adding and removing platforms
                    platforms_added = set()
                    platforms_removed = set()
                    
                    for event in platform_history:
                        event_type = event.get('event_type', '')
                        platform = event.get('platform_name', event.get('platform_id', ''))
                        
                        if event_type == 'platform_added':
                            platforms_added.add(platform)
                        elif event_type == 'platform_removed':
                            platforms_removed.add(platform)
                    
                    # Check for platforms that were added and later removed
                    switching_platforms = platforms_added.intersection(platforms_removed)
                    
                    # Also check for frequent changes within a short period
                    recent_changes = 0
                    cutoff_date = datetime.now() - timedelta(days=180)  # Last 6 months
                    
                    for event in platform_history:
                        try:
                            event_date = datetime.fromisoformat(event.get('event_date', '').split('T')[0])
                            if event_date >= cutoff_date:
                                recent_changes += 1
                        except:
                            pass
                    
                    # Detect switching if platforms were added and removed or if frequent recent changes
                    if switching_platforms or recent_changes >= 3:
                        result['detected'] = True
                        result['confidence'] = 0.6 + min(len(switching_platforms), 3) * 0.1
                        
                        details = []
                        if switching_platforms:
                            details.append(f"Switched on/off {len(switching_platforms)} platforms: {', '.join(switching_platforms)}")
                        if recent_changes >= 3:
                            details.append(f"{recent_changes} platform changes in last 6 months")
                            
                        result['details'] = "; ".join(details)
            except Exception as e:
                logger.error(f"Error checking platform switching: {e}")
        
        return result
    
    def check_residential_hotels(self, criteria: Dict = None) -> List[Dict]:
        """
        Check hotels located in residential areas
        
        Args:
            criteria: Check criteria
            
        Returns:
            List[Dict]: List of hotels in residential areas
        """
        logger.info(f"Checking hotels in residential areas with criteria: {criteria}")
        
        # Set default criteria if not provided
        if not criteria:
            criteria = {
                'min_stars': 3,
                'max_results': 100
            }
        
        # Get all hotels with coordinates
        hotels_with_coords = []
        if self.db and hasattr(self.db, 'get_all_hotels'):
            try:
                all_hotels = self.db.get_all_hotels()
                hotels_with_coords = [h for h in all_hotels if h.get('latitude') and h.get('longitude')]
                logger.info(f"Retrieved {len(hotels_with_coords)} hotels with coordinates for location check")
            except Exception as e:
                logger.error(f"Error retrieving hotels: {e}")
                return []
        else:
            logger.error("Database not available for retrieval")
            return []
        
        # Check each hotel's location
        results = []
        
        for hotel in hotels_with_coords:
            # Skip if stars below threshold
            stars = hotel.get('stars')
            if stars and criteria.get('min_stars') and float(stars) < criteria.get('min_stars'):
                continue
            
            try:
                # Check location type
                location_type = None
                
                # Use scraper if available
                if self.scraper and hasattr(self.scraper, 'check_location_type'):
                    location_type = self.scraper.check_location_type(
                        hotel.get('latitude'),
                        hotel.get('longitude')
                    )
                else:
                    # Try to use location information in hotel data
                    if hotel.get('area_type'):
                        location_type = hotel.get('area_type')
                    elif hotel.get('additional_info'):
                        try:
                            additional_info = json.loads(hotel.get('additional_info', '{}'))
                            location_type = (additional_info.get('area_type') or 
                                           additional_info.get('location_type') or 
                                           additional_info.get('zone_type'))
                        except:
                            pass
                
                # If location is residential, add to results
                if location_type and 'residential' in location_type.lower():
                    hotel_copy = hotel.copy()
                    hotel_copy['location_analysis'] = {
                        'location_type': location_type,
                        'verification_method': 'check_location_type' if hasattr(self.scraper, 'check_location_type') else 'metadata'
                    }
                    
                    # Calculate risk level based on stars
                    stars = hotel.get('stars')
                    if stars:
                        try:
                            stars_value = float(stars)
                            if stars_value >= 4:
                                hotel_copy['location_analysis']['risk_level'] = 'high'
                                hotel_copy['location_analysis']['details'] = f"{stars}★ hotel in residential area"
                            elif stars_value >= 3:
                                hotel_copy['location_analysis']['risk_level'] = 'medium'
                                hotel_copy['location_analysis']['details'] = f"{stars}★ hotel in residential area"
                            else:
                                hotel_copy['location_analysis']['risk_level'] = 'low'
                                hotel_copy['location_analysis']['details'] = f"Small hotel in residential area"
                        except:
                            hotel_copy['location_analysis']['risk_level'] = 'medium'
                            hotel_copy['location_analysis']['details'] = "Hotel in residential area"
                    else:
                        hotel_copy['location_analysis']['risk_level'] = 'medium'
                        hotel_copy['location_analysis']['details'] = "Hotel in residential area"
                    
                    results.append(hotel_copy)
            except Exception as e:
                logger.error(f"Error checking location for hotel {hotel.get('id')}: {e}")
        
        # Sort by stars (highest first)
        results.sort(key=lambda x: float(x.get('stars', 0)) if x.get('stars') else 0, reverse=True)
        
        # Limit results if requested
        max_results = criteria.get('max_results')
        if max_results and len(results) > max_results:
            results = results[:max_results]
        
        logger.info(f"Found {len(results)} hotels in residential areas")
        return results
    
    def list_high_risk_hotels(self, criteria: Dict = None) -> List[Dict]:
        """
        List hotels with high risk of violations
        
        Args:
            criteria: Listing criteria
            
        Returns:
            List[Dict]: List of high-risk hotels
        """
        logger.info(f"Listing high-risk hotels with criteria: {criteria}")
        
        # Set default criteria if not provided
        if not criteria:
            criteria = {
                'min_risk_score': 5,
                'max_results': 100,
                'sort_by': 'risk_score'  # 'risk_score', 'stars', 'last_updated'
            }
        
        # Get all hotels
        all_hotels = []
        if self.db and hasattr(self.db, 'get_all_hotels'):
            try:
                all_hotels = self.db.get_all_hotels()
                logger.info(f"Retrieved {len(all_hotels)} hotels for risk assessment")
            except Exception as e:
                logger.error(f"Error retrieving hotels: {e}")
                return []
        else:
            logger.error("Database not available for retrieval")
            return []
        
        # Analyze each hotel's risk
        high_risk_hotels = []
        
        for hotel in all_hotels:
            # Use existing risk score if available
            risk_score = None
            
            if hotel.get('risk_score'):
                try:
                    risk_score = float(hotel.get('risk_score'))
                except:
                    pass
            
            # If no risk score, calculate it
            if risk_score is None:
                risk_analysis = self._analyze_hotel_risk_factors(hotel)
                risk_score = risk_analysis.get('risk_score', 0)
                
                # Add risk analysis to hotel data
                hotel_with_risk = hotel.copy()
                hotel_with_risk['risk_analysis'] = risk_analysis
                hotel = hotel_with_risk
            
            # Check if meets threshold
            if risk_score >= criteria.get('min_risk_score', 5):
                high_risk_hotels.append(hotel)
        
        # Sort by specified criteria
        sort_by = criteria.get('sort_by', 'risk_score')
        
        if sort_by == 'risk_score':
            high_risk_hotels.sort(key=lambda x: float(x.get('risk_score', 0)) 
                                 if x.get('risk_score') 
                                 else x.get('risk_analysis', {}).get('risk_score', 0), 
                                 reverse=True)
        elif sort_by == 'stars':
            high_risk_hotels.sort(key=lambda x: float(x.get('stars', 0)) if x.get('stars') else 0, reverse=True)
        elif sort_by == 'last_updated':
            high_risk_hotels.sort(key=lambda x: x.get('last_updated', '0000-00-00'), reverse=True)
        
        # Limit results if requested
        max_results = criteria.get('max_results')
        if max_results and len(high_risk_hotels) > max_results:
            high_risk_hotels = high_risk_hotels[:max_results]
        
        logger.info(f"Found {len(high_risk_hotels)} high-risk hotels")
        return high_risk_hotels
    
    def create_violations_report(self, criteria: Dict = None) -> Dict:
        """
        Create detailed violations report
        
        Args:
            criteria: Report criteria
            
        Returns:
            Dict: Violations report
        """
        logger.info(f"Creating violations report with criteria: {criteria}")
        
        # Set default criteria if not provided
        if not criteria:
            criteria = {
                'min_risk_score': 3,
                'include_recent_changes': True,
                'include_location_analysis': True,
                'include_data_inconsistencies': True
            }
        
        # Initialize report
        report = {
            'generated_at': datetime.now().isoformat(),
            'criteria': criteria,
            'summary': {},
            'high_risk_hotels': [],
            'medium_risk_hotels': [],
            'risk_factors': {},
            'recommendations': []
        }
        
        # Get all hotels
        all_hotels = []
        if self.db and hasattr(self.db, 'get_all_hotels'):
            try:
                all_hotels = self.db.get_all_hotels()
                logger.info(f"Retrieved {len(all_hotels)} hotels for violations report")
            except Exception as e:
                logger.error(f"Error retrieving hotels: {e}")
                return {
                    'error': f"Database error: {str(e)}",
                    'generated_at': datetime.now().isoformat()
                }
        else:
            logger.error("Database not available for retrieval")
            return {
                'error': "Database not available",
                'generated_at': datetime.now().isoformat()
            }
        
        # Analyze each hotel
        hotels_by_risk = {
            'high': [],
            'medium': [],
            'low': []
        }
        
        risk_factors_count = {}
        
        for hotel in all_hotels:
            # Analyze risk factors
            risk_analysis = self._analyze_hotel_risk_factors(hotel)
            
            # Skip if below threshold
            if risk_analysis['risk_score'] < criteria.get('min_risk_score', 3):
                continue
            
            # Add risk analysis to hotel data
            hotel_with_risk = hotel.copy()
            hotel_with_risk['risk_analysis'] = risk_analysis
            
            # Add to appropriate risk level list
            risk_level = risk_analysis.get('risk_level', 'low')
            hotels_by_risk[risk_level].append(hotel_with_risk)
            
            # Count risk factors
            for factor in risk_analysis.get('risk_factors', []):
                factor_type = factor.get('type', 'unknown')
                risk_factors_count[factor_type] = risk_factors_count.get(factor_type, 0) + 1
            
            # Add additional analysis if requested
            if criteria.get('include_recent_changes') and hotel.get('id'):
                try:
                    # Get recent changes (last 90 days)
                    if self.db and hasattr(self.db, 'get_hotel_history'):
                        cutoff_date = datetime.now() - timedelta(days=90)
                        cutoff_str = cutoff_date.isoformat()
                        
                        history = self.db.get_hotel_history(hotel.get('id'), after_date=cutoff_str)
                        
                        if history:
                            hotel_with_risk['recent_changes'] = history
                except Exception as e:
                    logger.error(f"Error getting recent changes: {e}")
            
            if criteria.get('include_location_analysis') and hotel.get('latitude') and hotel.get('longitude'):
                try:
                    # Check location type
                    if self.scraper and hasattr(self.scraper, 'check_location_type'):
                        location_type = self.scraper.check_location_type(
                            hotel.get('latitude'),
                            hotel.get('longitude')
                        )
                        
                        hotel_with_risk['location_analysis'] = {
                            'location_type': location_type
                        }
                except Exception as e:
                    logger.error(f"Error analyzing location: {e}")
            
            if criteria.get('include_data_inconsistencies') and hotel.get('id'):
                try:
                    # Check for data inconsistencies
                    inconsistencies = self.check_data_inconsistencies(hotel_id=hotel.get('id'))
                    
                    if inconsistencies and inconsistencies.get('inconsistencies'):
                        hotel_with_risk['data_inconsistencies'] = inconsistencies
                except Exception as e:
                    logger.error(f"Error checking data inconsistencies: {e}")
        
        # Update report data
        report['high_risk_hotels'] = hotels_by_risk['high']
        report['medium_risk_hotels'] = hotels_by_risk['medium']
        
        report['summary'] = {
            'total_hotels': len(all_hotels),
            'high_risk_count': len(hotels_by_risk['high']),
            'medium_risk_count': len(hotels_by_risk['medium']),
            'low_risk_count': len(hotels_by_risk['low']),
            'total_risk_hotels': sum(len(hotels_by_risk[level]) for level in ['high', 'medium', 'low'])
        }
        
        # Sort risk factors by frequency
        sorted_factors = sorted(risk_factors_count.items(), key=lambda x: x[1], reverse=True)
        report['risk_factors'] = {factor: count for factor, count in sorted_factors}
        
        # Generate recommendations
        recommendations = []
        
        if hotels_by_risk['high']:
            recommendations.append({
                'priority': 'high',
                'action': 'immediate_verification',
                'description': f"Conduct immediate verification of {len(hotels_by_risk['high'])} high-risk hotels"
            })
        
        if hotels_by_risk['medium']:
            recommendations.append({
                'priority': 'medium',
                'action': 'scheduled_verification',
                'description': f"Schedule verification of {len(hotels_by_risk['medium'])} medium-risk hotels"
            })
        
        # Add specific recommendations based on risk factors
        for factor, count in sorted_factors[:3]:  # Top 3 risk factors
            if factor == 'sparse_data':
                recommendations.append({
                    'priority': 'medium',
                    'action': 'data_collection',
                    'description': f"Improve data collection for {count} hotels with incomplete information"
                })
            elif factor == 'unofficial_platforms_only':
                recommendations.append({
                    'priority': 'high',
                    'action': 'platform_verification',
                    'description': f"Verify {count} hotels listed only on unofficial platforms"
                })
            elif factor == 'residential_location':
                recommendations.append({
                    'priority': 'medium',
                    'action': 'zoning_check',
                    'description': f"Check zoning compliance for {count} hotels in residential areas"
                })
        
        report['recommendations'] = recommendations
        
        logger.info(f"Created violations report: {len(hotels_by_risk['high'])} high risk, {len(hotels_by_risk['medium'])} medium risk hotels")
        return report
    
    def _calculate_string_similarity(self, str1: str, str2: str) -> float:
        """
        Calculate similarity between two strings
        
        Args:
            str1: First string
            str2: Second string
            
        Returns:
            float: Similarity score (0-1)
        """
        if not str1 or not str2:
            return 0.0
        
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

# If run directly, display module info
if __name__ == "__main__":
    print("Violation Detection Module for Map_researcher 0.4")
    print("This module provides functionality for detecting potential hotel violations.")