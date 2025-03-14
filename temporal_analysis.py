#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Temporal Analysis Module for Map_researcher 0.4
This module provides functionality for analyzing hotel history and temporal changes.
"""

import os
import sys
import time
import json
import logging
import re
import requests
from typing import Dict, List, Any, Tuple, Optional, Union
from datetime import datetime, timedelta

# Initialize logger
logger = logging.getLogger("temporal_analysis")

class TemporalAnalysis:
    """Class for hotel history and temporal analysis operations"""
    
    def __init__(self, db=None, scraper=None):
        """
        Initialize the temporal analysis module
        
        Args:
            db: Database instance
            scraper: HotelScraper instance
        """
        self.db = db
        self.scraper = scraper
    
    def analyze_hotel_history(self, hotel_id: str) -> Dict:
        """
        Analyze history of a specific hotel
        
        Args:
            hotel_id: Hotel ID
            
        Returns:
            Dict: Hotel history analysis
        """
        logger.info(f"Analyzing history for hotel ID: {hotel_id}")
        
        if not hotel_id:
            logger.error("No hotel ID provided")
            return {"error": "No hotel ID provided"}
        
        # Get hotel data
        hotel = None
        if self.db and hasattr(self.db, 'get_hotel_by_id'):
            try:
                hotel = self.db.get_hotel_by_id(hotel_id)
                if not hotel:
                    logger.error(f"Hotel not found: {hotel_id}")
                    return {"error": "Hotel not found"}
            except Exception as e:
                logger.error(f"Error retrieving hotel data: {e}")
                return {"error": f"Database error: {str(e)}"}
        else:
            logger.error("Database not available for retrieval")
            return {"error": "Database not available"}
        
        # Initialize result
        result = {
            'hotel_id': hotel_id,
            'hotel_name': hotel.get('name', 'Unknown'),
            'first_seen': hotel.get('first_seen_date', 'Unknown'),
            'last_updated': hotel.get('last_updated', 'Unknown'),
            'name_changes': [],
            'ownership_changes': [],
            'status_changes': [],
            'listing_changes': [],
            'location_changes': [],
            'suspicious_patterns': []
        }
        
        # Get hotel history
        history = []
        if self.db and hasattr(self.db, 'get_hotel_history'):
            try:
                history = self.db.get_hotel_history(hotel_id)
                logger.info(f"Retrieved {len(history)} history records for hotel")
            except Exception as e:
                logger.error(f"Error retrieving hotel history: {e}")
        
        # Process history records
        if history:
            # Sort by date
            history.sort(key=lambda x: x.get('event_date', '0000-00-00'))
            
            # Process each record by type
            for record in history:
                event_type = record.get('event_type', '')
                
                # Name changes
                if event_type == 'name_change' or 'name' in event_type:
                    try:
                        old_name = record.get('old_value', '')
                        new_name = record.get('new_value', '')
                        result['name_changes'].append({
                            'date': record.get('event_date', ''),
                            'old_name': old_name,
                            'new_name': new_name,
                            'source': record.get('source', 'Unknown')
                        })
                    except:
                        pass
                
                # Ownership changes
                elif event_type == 'ownership_change' or 'owner' in event_type:
                    try:
                        old_owner = record.get('old_value', '')
                        new_owner = record.get('new_value', '')
                        result['ownership_changes'].append({
                            'date': record.get('event_date', ''),
                            'old_owner': old_owner,
                            'new_owner': new_owner,
                            'source': record.get('source', 'Unknown')
                        })
                    except:
                        pass
                
                # Status changes
                elif event_type == 'legal_status_change' or 'status' in event_type:
                    try:
                        old_status = record.get('old_value', '')
                        new_status = record.get('new_value', '')
                        result['status_changes'].append({
                            'date': record.get('event_date', ''),
                            'old_status': old_status,
                            'new_status': new_status,
                            'source': record.get('source', 'Unknown')
                        })
                    except:
                        pass
                
                # Listing changes
                elif event_type == 'listing_change' or 'platform' in event_type:
                    try:
                        old_value = record.get('old_value', '')
                        new_value = record.get('new_value', '')
                        result['listing_changes'].append({
                            'date': record.get('event_date', ''),
                            'old_value': old_value,
                            'new_value': new_value,
                            'source': record.get('source', 'Unknown')
                        })
                    except:
                        pass
                
                # Location changes
                elif event_type == 'location_change' or 'address' in event_type:
                    try:
                        old_value = record.get('old_value', '')
                        new_value = record.get('new_value', '')
                        result['location_changes'].append({
                            'date': record.get('event_date', ''),
                            'old_value': old_value,
                            'new_value': new_value,
                            'source': record.get('source', 'Unknown')
                        })
                    except:
                        pass
        
        # Analyze patterns
        suspicious_patterns = self._analyze_temporal_patterns(hotel, history)
        if suspicious_patterns:
            result['suspicious_patterns'] = suspicious_patterns
        
        # Calculate overall risk score
        risk_score = 0
        for pattern in suspicious_patterns:
            if pattern.get('severity') == 'high':
                risk_score += 3
            elif pattern.get('severity') == 'medium':
                risk_score += 2
            else:
                risk_score += 1
        
        result['risk_score'] = risk_score
        result['risk_level'] = 'high' if risk_score >= 5 else 'medium' if risk_score >= 2 else 'low'
        
        logger.info(f"Completed history analysis for hotel {hotel.get('name')}")
        return result
    
    def _analyze_temporal_patterns(self, hotel: Dict, history: List[Dict]) -> List[Dict]:
        """
        Analyze hotel history for suspicious patterns
        
        Args:
            hotel: Hotel data
            history: Hotel history records
            
        Returns:
            List[Dict]: List of suspicious patterns
        """
        patterns = []
        
        # 1. Check for frequent name changes
        name_changes = [h for h in history if h.get('event_type') == 'name_change' or 'name' in h.get('event_type', '')]
        if len(name_changes) >= 2:
            # Check timeframe
            if len(name_changes) >= 2:
                try:
                    first_date = datetime.fromisoformat(name_changes[0].get('event_date', '').split('T')[0])
                    last_date = datetime.fromisoformat(name_changes[-1].get('event_date', '').split('T')[0])
                    days_diff = (last_date - first_date).days
                    
                    if days_diff < 365 and len(name_changes) >= 2:
                        patterns.append({
                            'type': 'frequent_name_changes',
                            'severity': 'high',
                            'details': f"{len(name_changes)} name changes in {days_diff} days"
                        })
                    elif days_diff < 730 and len(name_changes) >= 2:
                        patterns.append({
                            'type': 'frequent_name_changes',
                            'severity': 'medium',
                            'details': f"{len(name_changes)} name changes in {days_diff} days"
                        })
                except Exception as e:
                    logger.error(f"Error analyzing name changes: {e}")
        
        # 2. Check for quick ownership changes
        ownership_changes = [h for h in history if h.get('event_type') == 'ownership_change' or 'owner' in h.get('event_type', '')]
        if len(ownership_changes) >= 2:
            # Check timeframe
            try:
                first_date = datetime.fromisoformat(ownership_changes[0].get('event_date', '').split('T')[0])
                last_date = datetime.fromisoformat(ownership_changes[-1].get('event_date', '').split('T')[0])
                days_diff = (last_date - first_date).days
                
                if days_diff < 365 and len(ownership_changes) >= 2:
                    patterns.append({
                        'type': 'frequent_ownership_changes',
                        'severity': 'high',
                        'details': f"{len(ownership_changes)} ownership changes in {days_diff} days"
                    })
                elif days_diff < 730 and len(ownership_changes) >= 2:
                    patterns.append({
                        'type': 'frequent_ownership_changes',
                        'severity': 'medium',
                        'details': f"{len(ownership_changes)} ownership changes in {days_diff} days"
                    })
            except Exception as e:
                logger.error(f"Error analyzing ownership changes: {e}")
        
        # 3. Check for status/classification changes
        status_changes = [h for h in history if h.get('event_type') == 'legal_status_change' or 'status' in h.get('event_type', '')]
        if status_changes:
            for change in status_changes:
                try:
                    old_status = change.get('old_value', '')
                    new_status = change.get('new_value', '')
                    
                    # Check for downgrades
                    if ('hotel' in old_status.lower() and 'private' in new_status.lower()) or \
                       ('hotel' in old_status.lower() and 'residential' in new_status.lower()) or \
                       ('commercial' in old_status.lower() and 'residential' in new_status.lower()):
                        patterns.append({
                            'type': 'downgrade_to_residential',
                            'severity': 'high',
                            'details': f"Changed from '{old_status}' to '{new_status}' on {change.get('event_date', '')}"
                        })
                except Exception as e:
                    logger.error(f"Error analyzing status changes: {e}")
        
        # 4. Check for platform transitions
        listing_changes = [h for h in history if h.get('event_type') == 'listing_change' or 'platform' in h.get('event_type', '')]
        if listing_changes:
            # Look for transitions from official to private platforms
            for i in range(len(listing_changes) - 1):
                try:
                    old_record = listing_changes[i]
                    new_record = listing_changes[i+1]
                    
                    old_value = old_record.get('new_value', '')  # Use new_value from older record
                    new_value = new_record.get('new_value', '')
                    
                    # Check for transition patterns
                    if ('booking.com' in old_value.lower() or 'expedia' in old_value.lower() or 'tripadvisor' in old_value.lower()) and \
                       ('airbnb' in new_value.lower() or 'private' in new_value.lower() or 'rental' in new_value.lower()):
                        patterns.append({
                            'type': 'official_to_private_transition',
                            'severity': 'high',
                            'details': f"Transitioned from official platform to private rental platform"
                        })
                except Exception as e:
                    logger.error(f"Error analyzing listing changes: {e}")
        
        # 5. Check for location changes with same owner
        location_changes = [h for h in history if h.get('event_type') == 'location_change' or 'address' in h.get('event_type', '')]
        if location_changes and ownership_changes:
            # Check if location changed but ownership remained same
            try:
                # Get latest owner
                latest_owner = ownership_changes[-1].get('new_value', '')
                
                # Check if location changed after ownership was established
                for change in location_changes:
                    change_date = datetime.fromisoformat(change.get('event_date', '').split('T')[0])
                    ownership_date = datetime.fromisoformat(ownership_changes[-1].get('event_date', '').split('T')[0])
                    
                    if change_date > ownership_date:
                        patterns.append({
                            'type': 'location_change_same_owner',
                            'severity': 'medium',
                            'details': f"Location changed on {change.get('event_date', '')} while under same ownership"
                        })
                        break
            except Exception as e:
                logger.error(f"Error analyzing location changes: {e}")
        
        # 6. Check for combination patterns
        if name_changes and status_changes:
            # Check if name changed shortly after status change or vice versa
            try:
                for name_change in name_changes:
                    name_date = datetime.fromisoformat(name_change.get('event_date', '').split('T')[0])
                    
                    for status_change in status_changes:
                        status_date = datetime.fromisoformat(status_change.get('event_date', '').split('T')[0])
                        days_diff = abs((name_date - status_date).days)
                        
                        if days_diff < 30:
                            patterns.append({
                                'type': 'name_status_change_correlation',
                                'severity': 'high',
                                'details': f"Name and status changed within {days_diff} days of each other"
                            })
                            break
            except Exception as e:
                logger.error(f"Error analyzing combination patterns: {e}")
        
        return patterns
    
    def search_historical_changes(self, criteria: Dict) -> List[Dict]:
        """
        Search for hotels with specific historical changes
        
        Args:
            criteria: Search criteria
            
        Returns:
            List[Dict]: Hotels matching criteria
        """
        logger.info(f"Searching for hotels with historical changes: {criteria}")
        
        results = []
        
        if not self.db or not hasattr(self.db, 'get_all_hotels') or not hasattr(self.db, 'get_hotel_history'):
            logger.error("Database or history functionality not available")
            return []
        
        try:
            # Get all hotels
            all_hotels = self.db.get_all_hotels()
            logger.info(f"Retrieved {len(all_hotels)} hotels for historical analysis")
            
            # Process each hotel
            for hotel in all_hotels:
                hotel_id = hotel.get('id')
                if not hotel_id:
                    continue
                
                # Get hotel history
                history = self.db.get_hotel_history(hotel_id)
                if not history:
                    continue
                
                # Apply criteria
                match = False
                
                # Filter by change type
                if criteria.get('change_type'):
                    change_type = criteria['change_type']
                    matching_changes = [h for h in history if change_type in h.get('event_type', '')]
                    if matching_changes:
                        match = True
                    else:
                        continue  # No matching changes, skip this hotel
                
                # Filter by date range
                if criteria.get('date_from') or criteria.get('date_to'):
                    date_match = False
                    
                    try:
                        # Parse date criteria
                        date_from = datetime.fromisoformat(criteria['date_from']) if criteria.get('date_from') else None
                        date_to = datetime.fromisoformat(criteria['date_to']) if criteria.get('date_to') else None
                        
                        # Check each history record
                        for record in history:
                            try:
                                event_date = datetime.fromisoformat(record.get('event_date', '').split('T')[0])
                                
                                if date_from and date_to:
                                    if date_from <= event_date <= date_to:
                                        date_match = True
                                        break
                                elif date_from:
                                    if date_from <= event_date:
                                        date_match = True
                                        break
                                elif date_to:
                                    if event_date <= date_to:
                                        date_match = True
                                        break
                            except:
                                continue
                    except Exception as e:
                        logger.error(f"Error processing date criteria: {e}")
                    
                    if not date_match:
                        continue  # No matching dates, skip this hotel
                
                # Filter by source
                if criteria.get('source'):
                    source_match = False
                    source = criteria['source'].lower()
                    
                    for record in history:
                        record_source = record.get('source', '').lower()
                        if source in record_source:
                            source_match = True
                            break
                    
                    if not source_match:
                        continue  # No matching source, skip this hotel
                
                # If we get here, all criteria matched
                match = True
                
                if match:
                    # Add matching history to hotel data
                    hotel_with_history = hotel.copy()
                    hotel_with_history['history'] = history
                    results.append(hotel_with_history)
            
            logger.info(f"Found {len(results)} hotels matching historical criteria")
        except Exception as e:
            logger.error(f"Error searching for historical changes: {e}")
        
        return results
    
    def detect_owner_changes(self, time_period: int = 365) -> List[Dict]:
        """
        Detect hotels with ownership changes in the specified time period
        
        Args:
            time_period: Time period in days to look back
            
        Returns:
            List[Dict]: Hotels with ownership changes
        """
        logger.info(f"Detecting hotels with ownership changes in the last {time_period} days")
        
        # Calculate cutoff date
        cutoff_date = datetime.now() - timedelta(days=time_period)
        cutoff_str = cutoff_date.isoformat()
        
        # Use search_historical_changes with ownership criteria
        criteria = {
            'change_type': 'ownership',
            'date_from': cutoff_str
        }
        
        return self.search_historical_changes(criteria)
    
    def track_platform_transitions(self, hotel_id: str = None, hotel_name: str = None, 
                                  location: Tuple[float, float] = None,
                                  time_range: Tuple[datetime, datetime] = None) -> List[Dict]:
        """
        Track hotel transitions between platforms
        
        Args:
            hotel_id: Hotel ID (optional)
            hotel_name: Hotel name (optional, used if ID not provided)
            location: Hotel coordinates (optional, used if ID not provided)
            time_range: Time range to analyze
            
        Returns:
            List[Dict]: Platform transition records
        """
        logger.info(f"Tracking platform transitions for hotel: {hotel_id or hotel_name}")
        
        # Get hotel data if ID is provided
        hotel_data = None
        if hotel_id and self.db and hasattr(self.db, 'get_hotel_by_id'):
            try:
                hotel_data = self.db.get_hotel_by_id(hotel_id)
                if hotel_data:
                    hotel_name = hotel_data.get('name')
                    location = (hotel_data.get('latitude'), hotel_data.get('longitude'))
            except Exception as e:
                logger.error(f"Error retrieving hotel data: {e}")
        
        if not hotel_name and not location:
            logger.error("No hotel identification provided")
            return []
        
        # Set default time range if not provided (last 3 years)
        if not time_range:
            end_date = datetime.now()
            start_date = end_date - timedelta(days=3*365)
            time_range = (start_date, end_date)
        
        transitions = []
        
        # 1. Check hotel history if available
        if hotel_id and self.db and hasattr(self.db, 'get_hotel_history'):
            try:
                # Get listing-related history
                history = self.db.get_hotel_history(hotel_id)
                listing_changes = [h for h in history if 'listing' in h.get('event_type', '') or 'platform' in h.get('event_type', '')]
                
                for change in listing_changes:
                    try:
                        event_date = datetime.fromisoformat(change.get('event_date', '').split('T')[0])
                        
                        # Check if in time range
                        if time_range[0] <= event_date <= time_range[1]:
                            transitions.append({
                                'date': change.get('event_date', ''),
                                'platform_change': {
                                    'old': change.get('old_value', ''),
                                    'new': change.get('new_value', '')
                                },
                                'source': change.get('source', 'Database History')
                            })
                    except Exception as e:
                        logger.error(f"Error processing history record: {e}")
            except Exception as e:
                logger.error(f"Error retrieving hotel history: {e}")
        
        # 2. Check hotel listings if available
        if hotel_id and self.db and hasattr(self.db, 'get_hotel_listings'):
            try:
                listings = self.db.get_hotel_listings(hotel_id)
                
                # Sort by first seen date
                listings.sort(key=lambda x: x.get('first_seen_date', '0000-00-00'))
                
                # Look for transitions
                for i in range(len(listings) - 1):
                    try:
                        listing1 = listings[i]
                        listing2 = listings[i+1]
                        
                        # Check if in time range
                        date1 = datetime.fromisoformat(listing1.get('first_seen_date', '').split('T')[0])
                        date2 = datetime.fromisoformat(listing2.get('first_seen_date', '').split('T')[0])
                        
                        if (time_range[0] <= date1 <= time_range[1] or 
                            time_range[0] <= date2 <= time_range[1]):
                            
                            # Get platform names
                            platform1 = listing1.get('platform_name', listing1.get('platform_id', 'Unknown'))
                            platform2 = listing2.get('platform_name', listing2.get('platform_id', 'Unknown'))
                            
                            transitions.append({
                                'date': listing2.get('first_seen_date', ''),
                                'platform_change': {
                                    'old': platform1,
                                    'new': platform2
                                },
                                'source': 'Listing Timeline'
                            })
                    except Exception as e:
                        logger.error(f"Error processing listing transition: {e}")
            except Exception as e:
                logger.error(f"Error retrieving hotel listings: {e}")
        
        # 3. Search in archive data if name is available
        if hotel_name and self.scraper and hasattr(self.scraper, 'search_historical_platforms'):
            try:
                archive_results = self.scraper.search_historical_platforms(
                    hotel_name=hotel_name,
                    location=location,
                    time_range=time_range
                )
                
                if archive_results:
                    for result in archive_results:
                        transitions.append({
                            'date': result.get('date', ''),
                            'platform_change': {
                                'old': result.get('old_platform', ''),
                                'new': result.get('new_platform', '')
                            },
                            'source': 'Archive Search'
                        })
            except Exception as e:
                logger.error(f"Error searching historical platforms: {e}")
        
        # Sort transitions by date
        transitions.sort(key=lambda x: x.get('date', '0000-00-00'))
        
        logger.info(f"Found {len(transitions)} platform transitions")
        return transitions
    
    def compare_hotels_history(self, hotel_id1: str, hotel_id2: str) -> Dict:
        """
        Compare historical data between two hotels
        
        Args:
            hotel_id1: First hotel ID
            hotel_id2: Second hotel ID
            
        Returns:
            Dict: Comparison results
        """
        logger.info(f"Comparing history between hotels: {hotel_id1} and {hotel_id2}")
        
        # Get hotel data
        hotel1 = None
        hotel2 = None
        
        if self.db and hasattr(self.db, 'get_hotel_by_id'):
            try:
                hotel1 = self.db.get_hotel_by_id(hotel_id1)
                hotel2 = self.db.get_hotel_by_id(hotel_id2)
                
                if not hotel1 or not hotel2:
                    missing = []
                    if not hotel1:
                        missing.append(hotel_id1)
                    if not hotel2:
                        missing.append(hotel_id2)
                    
                    logger.error(f"Hotels not found: {', '.join(missing)}")
                    return {"error": f"Hotels not found: {', '.join(missing)}"}
            except Exception as e:
                logger.error(f"Error retrieving hotel data: {e}")
                return {"error": f"Database error: {str(e)}"}
        else:
            logger.error("Database not available for retrieval")
            return {"error": "Database not available"}
        
        # Get hotel history
        history1 = []
        history2 = []
        
        if self.db and hasattr(self.db, 'get_hotel_history'):
            try:
                history1 = self.db.get_hotel_history(hotel_id1)
                history2 = self.db.get_hotel_history(hotel_id2)
                
                logger.info(f"Retrieved {len(history1)} records for hotel 1 and {len(history2)} records for hotel 2")
            except Exception as e:
                logger.error(f"Error retrieving hotel history: {e}")
        
        # Prepare result
        result = {
            'hotel1': {
                'id': hotel_id1,
                'name': hotel1.get('name', 'Unknown'),
                'address': hotel1.get('address', 'N/A'),
                'history_count': len(history1)
            },
            'hotel2': {
                'id': hotel_id2,
                'name': hotel2.get('name', 'Unknown'),
                'address': hotel2.get('address', 'N/A'),
                'history_count': len(history2)
            },
            'similarities': [],
            'correlations': [],
            'timeline': []
        }
        
        # Basic similarity check
        if hotel1.get('address') and hotel2.get('address'):
            address_similarity = self._calculate_string_similarity(hotel1['address'], hotel2['address'])
            if address_similarity > 0.7:
                result['similarities'].append({
                    'type': 'address',
                    'score': address_similarity,
                    'value1': hotel1['address'],
                    'value2': hotel2['address']
                })
        
        if hotel1.get('phone') and hotel2.get('phone'):
            if self._normalize_phone(hotel1['phone']) == self._normalize_phone(hotel2['phone']):
                result['similarities'].append({
                    'type': 'phone',
                    'score': 1.0,
                    'value1': hotel1['phone'],
                    'value2': hotel2['phone']
                })
        
        if hotel1.get('email') and hotel2.get('email'):
            if hotel1['email'].lower() == hotel2['email'].lower():
                result['similarities'].append({
                    'type': 'email',
                    'score': 1.0,
                    'value1': hotel1['email'],
                    'value2': hotel2['email']
                })
        
        # Check for ownership similarity
        owner1 = self._extract_owner(hotel1)
        owner2 = self._extract_owner(hotel2)
        
        if owner1 and owner2:
            owner_similarity = self._calculate_string_similarity(owner1, owner2)
            if owner_similarity > 0.7:
                result['similarities'].append({
                    'type': 'owner',
                    'score': owner_similarity,
                    'value1': owner1,
                    'value2': owner2
                })
        
        # Compare history timelines
        combined_timeline = []
        
        # Process history1
        for record in history1:
            combined_timeline.append({
                'date': record.get('event_date', ''),
                'hotel_id': hotel_id1,
                'hotel_name': hotel1.get('name', 'Unknown'),
                'event_type': record.get('event_type', ''),
                'old_value': record.get('old_value', ''),
                'new_value': record.get('new_value', ''),
                'source': record.get('source', '')
            })
        
        # Process history2
        for record in history2:
            combined_timeline.append({
                'date': record.get('event_date', ''),
                'hotel_id': hotel_id2,
                'hotel_name': hotel2.get('name', 'Unknown'),
                'event_type': record.get('event_type', ''),
                'old_value': record.get('old_value', ''),
                'new_value': record.get('new_value', ''),
                'source': record.get('source', '')
            })
        
        # Sort timeline by date
        combined_timeline.sort(key=lambda x: x.get('date', '0000-00-00'))
        
        # Add to result
        result['timeline'] = combined_timeline
        
        # Look for temporal correlations
        correlations = self._find_temporal_correlations(combined_timeline)
        if correlations:
            result['correlations'] = correlations
        
        logger.info(f"Completed history comparison between hotels {hotel1.get('name')} and {hotel2.get('name')}")
        return result
    
    def _find_temporal_correlations(self, timeline: List[Dict]) -> List[Dict]:
        """
        Find temporal correlations in a combined timeline
        
        Args:
            timeline: Combined timeline of events
            
        Returns:
            List[Dict]: List of correlations
        """
        correlations = []
        
        # Group events by date
        date_groups = {}
        for event in timeline:
            date = event.get('date', '').split('T')[0]  # Get just the date part
            if date not in date_groups:
                date_groups[date] = []
            date_groups[date].append(event)
        
        # Look for same-day events across different hotels
        for date, events in date_groups.items():
            if len(events) >= 2:
                # Check if events are from different hotels
                hotel_ids = set(e.get('hotel_id') for e in events)
                if len(hotel_ids) >= 2:
                    # Same day events from different hotels
                    correlations.append({
                        'type': 'same_day_events',
                        'date': date,
                        'events': events
                    })
        
        # Look for similar events in close time proximity
        for i, event1 in enumerate(timeline):
            hotel1_id = event1.get('hotel_id')
            event1_type = event1.get('event_type', '')
            event1_date = event1.get('date', '')
            
            # Skip if missing critical info
            if not hotel1_id or not event1_type or not event1_date:
                continue
            
            try:
                event1_datetime = datetime.fromisoformat(event1_date.split('T')[0])
                
                # Look for similar events in other hotels within 30 days
                for j, event2 in enumerate(timeline):
                    # Skip if same event or same hotel
                    if i == j or event2.get('hotel_id') == hotel1_id:
                        continue
                    
                    event2_type = event2.get('event_type', '')
                    event2_date = event2.get('date', '')
                    
                    # Skip if missing critical info or different event type
                    if not event2_type or not event2_date or event2_type != event1_type:
                        continue
                    
                    try:
                        event2_datetime = datetime.fromisoformat(event2_date.split('T')[0])
                        days_diff = abs((event2_datetime - event1_datetime).days)
                        
                        if days_diff <= 30:
                            correlations.append({
                                'type': 'similar_events_close_timeframe',
                                'event_type': event1_type,
                                'days_between': days_diff,
                                'event1': event1,
                                'event2': event2
                            })
                    except:
                        continue
            except:
                continue
        
        return correlations
    
    def _normalize_phone(self, phone: str) -> str:
        """
        Normalize phone number for comparison
        
        Args:
            phone: Phone number
            
        Returns:
            str: Normalized phone number
        """
        if not phone:
            return ""
        
        # Remove non-digit characters
        return re.sub(r'[^0-9]', '', phone)
    
    def _extract_owner(self, hotel: Dict) -> str:
        """
        Extract owner information from hotel data
        
        Args:
            hotel: Hotel data
            
        Returns:
            str: Owner name or empty string if not found
        """
        if not hotel:
            return ""
        
        # Try ownership_info field first
        if hotel.get('ownership_info'):
            try:
                owner_info = json.loads(hotel.get('ownership_info', '{}'))
                owner_name = owner_info.get('owner_name')
                if owner_name:
                    return owner_name
            except:
                pass
        
        # Try additional_info
        if hotel.get('additional_info'):
            try:
                additional_info = json.loads(hotel.get('additional_info', '{}'))
                owner_name = (additional_info.get('owner_name') or 
                             additional_info.get('owner') or 
                             additional_info.get('operator'))
                if owner_name:
                    return owner_name
            except:
                pass
        
        return ""
    
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
    
    def analyze_location_changes(self, location: Union[str, Tuple[float, float]], 
                                radius: int = 500, time_range: Tuple[datetime, datetime] = None) -> List[Dict]:
        """
        Analyze changes at the same geographic location over time
        
        Args:
            location: Location name or coordinates
            radius: Search radius in meters
            time_range: Time range to analyze
            
        Returns:
            List[Dict]: Analysis results
        """
        logger.info(f"Analyzing changes at location: {location}, radius: {radius}m")
        
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
        
        # Set default time range if not provided (last 5 years)
        if not time_range:
            end_date = datetime.now()
            start_date = end_date - timedelta(days=5*365)
            time_range = (start_date, end_date)
        
        # Find hotels in the specified location
        hotels_in_location = []
        
        if self.db and hasattr(self.db, 'get_all_hotels'):
            try:
                # Get all hotels
                all_hotels = self.db.get_all_hotels()
                logger.info(f"Retrieved {len(all_hotels)} hotels for location analysis")
                
                # Filter hotels based on proximity
                for hotel in all_hotels:
                    hotel_lat = hotel.get('latitude')
                    hotel_lng = hotel.get('longitude')
                    
                    if hotel_lat and hotel_lng:
                        # Calculate distance
                        try:
                            distance = self._haversine_distance(lat, lng, hotel_lat, hotel_lng) * 1000  # in meters
                            
                            # If within radius, add to results
                            if distance <= radius:
                                hotel['distance'] = round(distance, 1)
                                hotels_in_location.append(hotel)
                        except:
                            continue
                
                logger.info(f"Found {len(hotels_in_location)} hotels within {radius}m")
            except Exception as e:
                logger.error(f"Error finding hotels in location: {e}")
        
        # Get history for each hotel
        hotels_with_history = []
        
        if self.db and hasattr(self.db, 'get_hotel_history'):
            for hotel in hotels_in_location:
                try:
                    history = self.db.get_hotel_history(hotel.get('id'))
                    
                    # Filter history by time range
                    filtered_history = []
                    for record in history:
                        try:
                            event_date = datetime.fromisoformat(record.get('event_date', '').split('T')[0])
                            if time_range[0] <= event_date <= time_range[1]:
                                filtered_history.append(record)
                        except:
                            continue
                    
                    if filtered_history:
                        hotel_with_history = hotel.copy()
                        hotel_with_history['history'] = filtered_history
                        hotels_with_history.append(hotel_with_history)
                except Exception as e:
                    logger.error(f"Error retrieving history for hotel {hotel.get('id')}: {e}")
        
        # Analyze changes
        result = {
            'location': {
                'latitude': lat,
                'longitude': lng,
                'radius': radius
            },
            'time_range': {
                'start': time_range[0].isoformat(),
                'end': time_range[1].isoformat()
            },
            'total_hotels': len(hotels_in_location),
            'hotels_with_history': len(hotels_with_history),
            'timeline': [],
            'patterns': []
        }
        
        # Build unified timeline
        unified_timeline = []
        
        for hotel in hotels_with_history:
            for record in hotel.get('history', []):
                unified_timeline.append({
                    'date': record.get('event_date', ''),
                    'hotel_id': hotel.get('id'),
                    'hotel_name': hotel.get('name', 'Unknown'),
                    'event_type': record.get('event_type', ''),
                    'old_value': record.get('old_value', ''),
                    'new_value': record.get('new_value', ''),
                    'source': record.get('source', '')
                })
        
        # Sort timeline by date
        unified_timeline.sort(key=lambda x: x.get('date', '0000-00-00'))
        result['timeline'] = unified_timeline
        
        # Look for patterns
        if unified_timeline:
            # 1. Check for multiple changes in the same location
            hotel_ids = set(event.get('hotel_id') for event in unified_timeline)
            if len(hotel_ids) > 1:
                result['patterns'].append({
                    'type': 'multiple_hotels_same_location',
                    'count': len(hotel_ids),
                    'hotel_ids': list(hotel_ids)
                })
            
            # 2. Check for sequential closures and openings
            closure_events = [e for e in unified_timeline if 'closure' in e.get('event_type', '').lower() or 
                             ('status' in e.get('event_type', '').lower() and 'closed' in e.get('new_value', '').lower())]
            
            opening_events = [e for e in unified_timeline if 'opening' in e.get('event_type', '').lower() or 
                             ('status' in e.get('event_type', '').lower() and 'open' in e.get('new_value', '').lower())]
            
            if closure_events and opening_events:
                for closure in closure_events:
                    try:
                        closure_date = datetime.fromisoformat(closure.get('date', '').split('T')[0])
                        
                        # Find openings that happened after this closure
                        for opening in opening_events:
                            try:
                                opening_date = datetime.fromisoformat(opening.get('date', '').split('T')[0])
                                
                                # Check if opening happened after closure and for different hotel
                                if (opening_date > closure_date and 
                                    opening.get('hotel_id') != closure.get('hotel_id') and
                                    (opening_date - closure_date).days <= 180):  # Within 6 months
                                    
                                    result['patterns'].append({
                                        'type': 'sequential_closure_opening',
                                        'days_between': (opening_date - closure_date).days,
                                        'closure': closure,
                                        'opening': opening
                                    })
                            except:
                                continue
                    except:
                        continue
        
        logger.info(f"Completed location change analysis: found {len(result['patterns'])} patterns")
        return result
    
    def extract_from_archive(self, url: str, time_range: Tuple[datetime, datetime] = None) -> Dict:
        """
        Extract information from Internet Archive
        
        Args:
            url: Website URL to analyze
            time_range: Time range to analyze
            
        Returns:
            Dict: Extracted information
        """
        logger.info(f"Extracting archived information for URL: {url}")
        
        if not url:
            logger.error("No URL provided")
            return {"error": "No URL provided"}
        
        # Set default time range if not provided (last 5 years)
        if not time_range:
            end_date = datetime.now()
            start_date = end_date - timedelta(days=5*365)
            time_range = (start_date, end_date)
        
        result = {
            'url': url,
            'time_range': {
                'start': time_range[0].isoformat(),
                'end': time_range[1].isoformat()
            },
            'snapshots': [],
            'changes': [],
            'extracted_data': []
        }
        
        # Search Wayback Machine API
        try:
            # Format timestamps for Wayback Machine
            start_timestamp = time_range[0].strftime("%Y%m%d")
            end_timestamp = time_range[1].strftime("%Y%m%d")
            
            # Build API URL
            cdx_api_url = f"https://web.archive.org/cdx/search/cdx"
            
            params = {
                "url": url,
                "from": start_timestamp,
                "to": end_timestamp,
                "output": "json",
                "fl": "timestamp,original,statuscode,digest",
                "collapse": "digest"  # Collapse duplicate captures
            }
            
            # Send request
            response = requests.get(cdx_api_url, params=params)
            data = response.json()
            
            # Process response
            if data and len(data) > 1:  # First row is header
                # Get header row
                header = data[0]
                
                # Process snapshots
                snapshots = []
                
                for row in data[1:]:
                    # Create dict from header and row
                    snapshot = dict(zip(header, row))
                    
                    # Add snapshot URL
                    timestamp = snapshot.get('timestamp', '')
                    snapshot['archive_url'] = f"https://web.archive.org/web/{timestamp}/{url}"
                    
                    # Format date
                    try:
                        date_str = timestamp[:4] + '-' + timestamp[4:6] + '-' + timestamp[6:8]
                        snapshot['date'] = date_str
                    except:
                        snapshot['date'] = ''
                    
                    snapshots.append(snapshot)
                
                result['snapshots'] = snapshots
                logger.info(f"Found {len(snapshots)} snapshots for URL: {url}")
                
                # Select key snapshots for content extraction (first, last, and some in between)
                extraction_candidates = []
                
                if len(snapshots) == 1:
                    extraction_candidates = snapshots
                elif len(snapshots) == 2:
                    extraction_candidates = snapshots
                elif len(snapshots) <= 5:
                    extraction_candidates = [snapshots[0], snapshots[-1]]  # First and last
                    # Add middle snapshot
                    middle_idx = len(snapshots) // 2
                    extraction_candidates.append(snapshots[middle_idx])
                else:
                    extraction_candidates = [snapshots[0], snapshots[-1]]  # First and last
                    
                    # Add some evenly spaced snapshots in between
                    step = len(snapshots) // 4
                    for i in range(step, len(snapshots) - 1, step):
                        extraction_candidates.append(snapshots[i])
                
                # Extract content from selected snapshots
                for candidate in extraction_candidates:
                    archive_url = candidate.get('archive_url')
                    
                    if archive_url:
                        extracted = self._extract_data_from_wayback(archive_url)
                        
                        if extracted:
                            result['extracted_data'].append({
                                'date': candidate.get('date', ''),
                                'archive_url': archive_url,
                                'data': extracted
                            })
                
                # Detect changes between snapshots
                if len(result['extracted_data']) >= 2:
                    for i in range(len(result['extracted_data']) - 1):
                        snapshot1 = result['extracted_data'][i]
                        snapshot2 = result['extracted_data'][i+1]
                        
                        changes = self._detect_changes_between_snapshots(snapshot1, snapshot2)
                        
                        if changes:
                            result['changes'].append({
                                'from_date': snapshot1.get('date', ''),
                                'to_date': snapshot2.get('date', ''),
                                'changes': changes
                            })
            else:
                logger.warning(f"No archived snapshots found for URL: {url}")
        except Exception as e:
            logger.error(f"Error searching Wayback Machine: {e}")
            return {"error": f"Error searching Wayback Machine: {str(e)}"}
        
        return result
    
    def _extract_data_from_wayback(self, archive_url: str) -> Dict:
        """
        Extract data from a Wayback Machine snapshot
        
        Args:
            archive_url: Wayback Machine snapshot URL
            
        Returns:
            Dict: Extracted data
        """
        try:
            # Send request to get the archived page
            headers = {
                "User-Agent": "Map_researcher0.4 (Academic Research Tool)"
            }
            
            response = requests.get(archive_url, headers=headers)
            
            if response.status_code != 200:
                logger.error(f"Failed to retrieve archived page: {response.status_code}")
                return {}
            
            # Parse HTML content
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Extract key information
            result = {
                'title': soup.title.text.strip() if soup.title else '',
                'contact_info': {},
                'hotel_info': {}
            }
            
            # Look for contact information
            # Phone
            phone_patterns = [
                r'(\+\d{1,3}[- .]?)?\(?\d{3}\)?[- .]?\d{3}[- .]?\d{4}',  # General format
                r'(\+\d{1,3}[- .]?)?\d{5,}',  # Simple digits
                r'Tel[:.]\s*([^<>\n]+)',  # Following "Tel:"
                r'Phone[:.]\s*([^<>\n]+)'  # Following "Phone:"
            ]
            
            for pattern in phone_patterns:
                phone_matches = re.findall(pattern, response.text)
                if phone_matches:
                    result['contact_info']['phone'] = phone_matches[0]
                    break
            
            # Email
            email_pattern = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
            email_matches = re.findall(email_pattern, response.text)
            if email_matches:
                result['contact_info']['email'] = email_matches[0]
            
            # Address
            address_patterns = [
                r'Address[:.]\s*([^<>\n]+)',
                r'Location[:.]\s*([^<>\n]+)'
            ]
            
            for pattern in address_patterns:
                address_matches = re.findall(pattern, response.text)
                if address_matches:
                    result['contact_info']['address'] = address_matches[0].strip()
                    break
            
            # Hotel information
            # Stars/rating
            rating_patterns = [
                r'(\d[-.]\d|\d) stars?',
                r'(\d[-.]\d|\d)-stars?',
                r'(\d[-.]\d|\d) ?★'
            ]
            
            for pattern in rating_patterns:
                rating_matches = re.findall(pattern, response.text.lower())
                if rating_matches:
                    result['hotel_info']['stars'] = rating_matches[0]
                    break
            
            # Facilities
            facility_patterns = [
                r'Facilities[:.]\s*([^<>\n]+)',
                r'Amenities[:.]\s*([^<>\n]+)'
            ]
            
            for pattern in facility_patterns:
                facility_matches = re.findall(pattern, response.text)
                if facility_matches:
                    result['hotel_info']['facilities'] = facility_matches[0].strip()
                    break
            
            # Owner/operator information
            owner_patterns = [
                r'(managed by|operated by|owned by)[:.]\s*([^<>\n]+)',
                r'Owner[:.]\s*([^<>\n]+)',
                r'Management[:.]\s*([^<>\n]+)'
            ]
            
            for pattern in owner_patterns:
                owner_matches = re.findall(pattern, response.text.lower())
                if owner_matches:
                    if isinstance(owner_matches[0], tuple):
                        result['hotel_info']['owner'] = owner_matches[0][1].strip()
                    else:
                        result['hotel_info']['owner'] = owner_matches[0].strip()
                    break
            
            return result
            
        except Exception as e:
            logger.error(f"Error extracting data from Wayback Machine: {e}")
            return {}
    
    def _detect_changes_between_snapshots(self, snapshot1: Dict, snapshot2: Dict) -> List[Dict]:
        """
        Detect changes between two archived snapshots
        
        Args:
            snapshot1: First snapshot data
            snapshot2: Second snapshot data
            
        Returns:
            List[Dict]: List of detected changes
        """
        changes = []
        
        # Compare title
        if snapshot1.get('data', {}).get('title') != snapshot2.get('data', {}).get('title'):
            changes.append({
                'type': 'title_change',
                'old': snapshot1.get('data', {}).get('title', ''),
                'new': snapshot2.get('data', {}).get('title', '')
            })
        
        # Compare contact information
        contact1 = snapshot1.get('data', {}).get('contact_info', {})
        contact2 = snapshot2.get('data', {}).get('contact_info', {})
        
        for field in ['phone', 'email', 'address']:
            if contact1.get(field) != contact2.get(field):
                changes.append({
                    'type': f'contact_{field}_change',
                    'old': contact1.get(field, ''),
                    'new': contact2.get(field, '')
                })
        
        # Compare hotel information
        hotel1 = snapshot1.get('data', {}).get('hotel_info', {})
        hotel2 = snapshot2.get('data', {}).get('hotel_info', {})
        
        for field in ['stars', 'facilities', 'owner']:
            if hotel1.get(field) != hotel2.get(field):
                changes.append({
                    'type': f'hotel_{field}_change',
                    'old': hotel1.get(field, ''),
                    'new': hotel2.get(field, '')
                })
        
        return changes
    
    def analyze_permits_history(self, hotel_id: str = None, hotel_data: Dict = None) -> Dict:
        """
        Analyze historical record of permits and licenses
        
        Args:
            hotel_id: Hotel ID
            hotel_data: Hotel data (if ID not provided)
            
        Returns:
            Dict: Permit history analysis
        """
        logger.info(f"Analyzing permits history for hotel ID: {hotel_id}")
        
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
            'permits': [],
            'license_changes': [],
            'classification_changes': [],
            'timeline': [],
            'suspicious_patterns': []
        }
        
        # Search for permit records in hotel history
        if hotel_id and self.db and hasattr(self.db, 'get_hotel_history'):
            try:
                # Get all history records
                history = self.db.get_hotel_history(hotel_id)
                
                # Filter permit/license related records
                permit_keywords = ['permit', 'license', 'classification', 'approval', 'certificate']
                permit_records = []
                
                for record in history:
                    event_type = record.get('event_type', '').lower()
                    if any(keyword in event_type for keyword in permit_keywords):
                        permit_records.append(record)
                        continue
                    
                    # Also check notes field
                    notes = record.get('notes', '').lower()
                    if any(keyword in notes for keyword in permit_keywords):
                        permit_records.append(record)
                
                # Process permit records
                for record in permit_records:
                    event_type = record.get('event_type', '').lower()
                    
                    if 'permit' in event_type or 'license' in event_type:
                        # Add to permits list
                        result['permits'].append({
                            'date': record.get('event_date', ''),
                            'type': record.get('event_type', ''),
                            'old_value': record.get('old_value', ''),
                            'new_value': record.get('new_value', ''),
                            'source': record.get('source', ''),
                            'notes': record.get('notes', '')
                        })
                    
                    if 'license' in event_type:
                        # Add to license changes
                        result['license_changes'].append({
                            'date': record.get('event_date', ''),
                            'old_license': record.get('old_value', ''),
                            'new_license': record.get('new_value', ''),
                            'source': record.get('source', '')
                        })
                    
                    if 'classification' in event_type:
                        # Add to classification changes
                        result['classification_changes'].append({
                            'date': record.get('event_date', ''),
                            'old_classification': record.get('old_value', ''),
                            'new_classification': record.get('new_value', ''),
                            'source': record.get('source', '')
                        })
                    
                    # Add to timeline
                    result['timeline'].append({
                        'date': record.get('event_date', ''),
                        'event_type': record.get('event_type', ''),
                        'change': f"From '{record.get('old_value', '')}' to '{record.get('new_value', '')}'",
                        'source': record.get('source', '')
                    })
                
                # Sort timeline by date
                result['timeline'].sort(key=lambda x: x.get('date', '0000-00-00'))
                
                logger.info(f"Found {len(permit_records)} permit/license related records")
            except Exception as e:
                logger.error(f"Error searching permit history: {e}")
        
        # Check for permit information in additional_info field
        if hotel_data.get('additional_info'):
            try:
                additional_info = json.loads(hotel_data.get('additional_info', '{}'))
                
                # Look for permit/license information
                for key, value in additional_info.items():
                    if any(keyword in key.lower() for keyword in ['permit', 'license', 'certification']):
                        result['permits'].append({
                            'type': key,
                            'value': value,
                            'source': 'Hotel Data'
                        })
            except Exception as e:
                logger.error(f"Error processing additional info: {e}")
        
        # Analyze patterns
        if result['timeline']:
            # 1. Check for frequent classification changes
            if len(result['classification_changes']) >= 2:
                try:
                    first_change = result['classification_changes'][0]
                    last_change = result['classification_changes'][-1]
                    
                    first_date = datetime.fromisoformat(first_change.get('date', '').split('T')[0])
                    last_date = datetime.fromisoformat(last_change.get('date', '').split('T')[0])
                    
                    days_diff = (last_date - first_date).days
                    
                    if days_diff <= 365 and len(result['classification_changes']) >= 2:
                        result['suspicious_patterns'].append({
                            'type': 'frequent_classification_changes',
                            'severity': 'high',
                            'details': f"{len(result['classification_changes'])} classification changes in {days_diff} days"
                        })
                except Exception as e:
                    logger.error(f"Error analyzing classification changes: {e}")
            
            # 2. Check for downgrade patterns
            for change in result['classification_changes']:
                try:
                    old_class = change.get('old_classification', '').lower()
                    new_class = change.get('new_classification', '').lower()
                    
                    # Check for star rating downgrades
                    if ('star' in old_class and 'star' in new_class):
                        old_stars = re.search(r'(\d+)', old_class)
                        new_stars = re.search(r'(\d+)', new_class)
                        
                        if old_stars and new_stars and int(old_stars.group(1)) > int(new_stars.group(1)):
                            result['suspicious_patterns'].append({
                                'type': 'star_rating_downgrade',
                                'severity': 'medium',
                                'details': f"Downgraded from {old_stars.group(1)} stars to {new_stars.group(1)} stars"
                            })
                    
                    # Check for type downgrades
                    downgrades = [
                        ('hotel', 'motel'),
                        ('hotel', 'guesthouse'),
                        ('hotel', 'residence'),
                        ('hotel', 'apartment'),
                        ('commercial', 'residential')
                    ]
                    
                    for higher, lower in downgrades:
                        if higher in old_class and lower in new_class:
                            result['suspicious_patterns'].append({
                                'type': 'type_downgrade',
                                'severity': 'high',
                                'details': f"Downgraded from {higher} to {lower} classification"
                            })
                except Exception as e:
                    logger.error(f"Error analyzing downgrade patterns: {e}")
        
        # Calculate overall risk score
        risk_score = 0
        for pattern in result['suspicious_patterns']:
            if pattern.get('severity') == 'high':
                risk_score += 3
            elif pattern.get('severity') == 'medium':
                risk_score += 2
            else:
                risk_score += 1
        
        result['risk_score'] = risk_score
        result['risk_level'] = 'high' if risk_score >= 3 else 'medium' if risk_score >= 1 else 'low'
        
        logger.info(f"Completed permits history analysis for hotel {hotel_data.get('name')}")
        return result
    
    def historical_changes_report(self, criteria: Dict = None) -> Dict:
        """
        Generate a comprehensive report on historical changes
        
        Args:
            criteria: Report criteria
            
        Returns:
            Dict: Comprehensive report
        """
        logger.info(f"Generating historical changes report with criteria: {criteria}")
        
        # Set default criteria if not provided
        if not criteria:
            criteria = {
                'time_period': 365,  # Last year
                'min_changes': 1,    # At least one change
                'include_types': ['name', 'owner', 'status', 'license', 'platform']
            }
        
        # Initialize report
        report = {
            'generated_at': datetime.now().isoformat(),
            'criteria': criteria,
            'summary': {},
            'hotels_with_changes': [],
            'change_frequency': {},
            'detailed_changes': {}
        }
        
        # Calculate time period
        time_period = criteria.get('time_period', 365)
        cutoff_date = datetime.now() - timedelta(days=time_period)
        cutoff_str = cutoff_date.isoformat()
        
        # Get all hotels with changes in the specified period
        hotels_with_changes = []
        
        if self.db and hasattr(self.db, 'get_all_hotels') and hasattr(self.db, 'get_hotel_history'):
            try:
                # Get all hotels
                all_hotels = self.db.get_all_hotels()
                logger.info(f"Analyzing {len(all_hotels)} hotels for historical report")
                
                # Track various statistics
                total_changes = 0
                change_types = {}
                hotels_by_change_count = {}
                
                # Process each hotel
                for hotel in all_hotels:
                    hotel_id = hotel.get('id')
                    if not hotel_id:
                        continue
                    
                    # Get hotel history
                    history = self.db.get_hotel_history(hotel_id)
                    
                    # Filter by date and change types
                    relevant_changes = []
                    for record in history:
                        try:
                            # Check date
                            event_date = datetime.fromisoformat(record.get('event_date', '').split('T')[0])
                            if event_date < cutoff_date:
                                continue
                            
                            # Check change type
                            event_type = record.get('event_type', '').lower()
                            include_types = [t.lower() for t in criteria.get('include_types', [])]
                            
                            if include_types and not any(t in event_type for t in include_types):
                                continue
                            
                            # Add to relevant changes
                            relevant_changes.append(record)
                            
                            # Update statistics
                            change_type = next((t for t in include_types if t in event_type), 'other')
                            change_types[change_type] = change_types.get(change_type, 0) + 1
                            total_changes += 1
                        except Exception as e:
                            logger.error(f"Error processing history record: {e}")
                    
                    # Check if hotel meets criteria
                    if len(relevant_changes) >= criteria.get('min_changes', 1):
                        hotel_with_changes = hotel.copy()
                        hotel_with_changes['changes'] = relevant_changes
                        hotel_with_changes['change_count'] = len(relevant_changes)
                        hotels_with_changes.append(hotel_with_changes)
                        
                        # Update hotels by change count
                        count_group = min(10, len(relevant_changes))  # Group by 1, 2, ..., 10+
                        if count_group >= 10:
                            count_group = "10+"
                        hotels_by_change_count[count_group] = hotels_by_change_count.get(count_group, 0) + 1
                
                # Complete report data
                report['hotels_with_changes'] = hotels_with_changes
                report['summary'] = {
                    'total_hotels': len(all_hotels),
                    'hotels_with_changes': len(hotels_with_changes),
                    'total_changes': total_changes,
                    'time_period_days': time_period,
                    'average_changes_per_hotel': round(total_changes / len(hotels_with_changes), 2) if hotels_with_changes else 0
                }
                report['change_frequency'] = {
                    'by_type': change_types,
                    'by_hotel_count': hotels_by_change_count
                }
                
                # Process detailed changes
                for change_type in change_types.keys():
                    hotels_with_this_change = []
                    for hotel in hotels_with_changes:
                        matching_changes = [c for c in hotel.get('changes', []) if change_type in c.get('event_type', '').lower()]
                        if matching_changes:
                            hotels_with_this_change.append({
                                'id': hotel.get('id'),
                                'name': hotel.get('name'),
                                'changes': matching_changes
                            })
                    
                    report['detailed_changes'][change_type] = {
                        'count': change_types[change_type],
                        'affected_hotels': len(hotels_with_this_change),
                        'examples': hotels_with_this_change[:5]  # First 5 examples
                    }
                
                # Sort hotels by change count (descending)
                report['hotels_with_changes'].sort(key=lambda x: x.get('change_count', 0), reverse=True)
                
                logger.info(f"Completed report: {len(hotels_with_changes)} hotels with changes")
            except Exception as e:
                logger.error(f"Error generating historical report: {e}")
                return {"error": f"Failed to generate report: {str(e)}"}
        else:
            logger.error("Database or history functionality not available")
            return {"error": "Database or history functionality not available"}
        
        return report

# If run directly, display module info
if __name__ == "__main__":
    print("Temporal Analysis Module for Map_researcher 0.4")
    print("This module provides functionality for analyzing hotel history and temporal changes.")