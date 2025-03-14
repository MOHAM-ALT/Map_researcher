# analysis/pattern_discovery.py
import os
import json
import logging
import numpy as np
import pandas as pd
from datetime import datetime
from sklearn.cluster import DBSCAN
import networkx as nx
import re
from collections import Counter

class PatternDiscovery:
    """Class for discovering patterns and relationships among hotel properties"""
    
    def __init__(self, db=None, config=None):
        """Initialize the PatternDiscovery class"""
        self.db = db
        self.config = config
        self.logger = logging.getLogger("pattern_discovery")
        
    def find_property_groups(self, properties, similarity_threshold=0.7):
        """
        Find groups of related properties based on various criteria
        
        Args:
            properties: List of property dictionaries
            similarity_threshold: Threshold for considering properties as related
            
        Returns:
            Dict with property groups
        """
        self.logger.info(f"Finding property groups among {len(properties)} properties")
        
        try:
            # Create a graph to represent property relationships
            G = nx.Graph()
            
            # Add nodes for each property
            for i, prop in enumerate(properties):
                G.add_node(i, **prop)
            
            # Add edges between similar properties
            for i in range(len(properties)):
                for j in range(i + 1, len(properties)):
                    similarity = self._calculate_property_similarity(properties[i], properties[j])
                    
                    if similarity >= similarity_threshold:
                        G.add_edge(i, j, weight=similarity)
            
            # Find connected components (property groups)
            connected_components = list(nx.connected_components(G))
            
            # Create property groups
            property_groups = []
            
            for i, component in enumerate(connected_components):
                # Get properties in this group
                group_properties = [properties[idx] for idx in component]
                
                # Only consider groups with at least 2 properties
                if len(group_properties) >= 2:
                    # Calculate group characteristics
                    group_characteristics = self._calculate_group_characteristics(group_properties)
                    
                    group = {
                        "group_id": i,
                        "properties": group_properties,
                        "property_count": len(group_properties),
                        **group_characteristics
                    }
                    
                    property_groups.append(group)
            
            # Sort groups by size (largest first)
            property_groups.sort(key=lambda x: x["property_count"], reverse=True)
            
            self.logger.info(f"Found {len(property_groups)} property groups")
            
            return {
                "status": "success",
                "group_count": len(property_groups),
                "groups": property_groups,
                "ungrouped_count": len(properties) - sum(len(g["properties"]) for g in property_groups),
                "total_properties": len(properties)
            }
            
        except Exception as e:
            self.logger.error(f"Error finding property groups: {str(e)}")
            return {"error": f"Error finding property groups: {str(e)}"}
    
    def _calculate_property_similarity(self, prop1, prop2):
        """
        Calculate similarity between two properties
        
        Args:
            prop1: First property dictionary
            prop2: Second property dictionary
            
        Returns:
            Float similarity score (0-1)
        """
        # Initialize similarity score components
        similarity_scores = []
        
        # Check name similarity
        if prop1.get("name") and prop2.get("name"):
            name_similarity = self._calculate_string_similarity(prop1["name"], prop2["name"])
            similarity_scores.append((name_similarity, 0.4))  # Weight: 0.4
        
        # Check address similarity
        if prop1.get("address") and prop2.get("address"):
            address_similarity = self._calculate_string_similarity(prop1["address"], prop2["address"])
            similarity_scores.append((address_similarity, 0.3))  # Weight: 0.3
        
        # Check geographic proximity
        if (prop1.get("latitude") and prop1.get("longitude") and 
            prop2.get("latitude") and prop2.get("longitude")):
            
            dist = self._calculate_geographic_distance(
                prop1["latitude"], prop1["longitude"],
                prop2["latitude"], prop2["longitude"]
            )
            
            # Convert distance to similarity score (0-1)
            # 0 km -> 1.0, 1 km -> 0.5, 2+ km -> 0.0
            geo_similarity = max(0, 1 - dist/2)
            similarity_scores.append((geo_similarity, 0.3))  # Weight: 0.3
        
        # Check phone number similarity
        if prop1.get("phone") and prop2.get("phone"):
            phone_similarity = self._calculate_phone_similarity(prop1["phone"], prop2["phone"])
            if phone_similarity > 0:
                similarity_scores.append((phone_similarity, 0.4))  # Weight: 0.4
        
        # Check owner/contact similarity
        owner1 = prop1.get("owner") or prop1.get("contact_name")
        owner2 = prop2.get("owner") or prop2.get("contact_name")
        
        if owner1 and owner2:
            owner_similarity = self._calculate_string_similarity(owner1, owner2)
            similarity_scores.append((owner_similarity, 0.4))  # Weight: 0.4
        
        # Calculate weighted average
        if similarity_scores:
            weighted_sum = sum(score * weight for score, weight in similarity_scores)
            total_weight = sum(weight for _, weight in similarity_scores)
            return weighted_sum / total_weight
        else:
            return 0.0
    
    def _calculate_string_similarity(self, str1, str2):
        """Calculate similarity between two strings using Levenshtein distance"""
        if not str1 or not str2:
            return 0
        
        # Convert to lowercase and strip spaces
        s1 = str1.lower().strip()
        s2 = str2.lower().strip()
        
        # Quick check for exact match
        if s1 == s2:
            return 1.0
        
        # Check for substring
        if s1 in s2 or s2 in s1:
            shorter = s1 if len(s1) < len(s2) else s2
            longer = s2 if len(s1) < len(s2) else s1
            return len(shorter) / len(longer)
        
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
        similarity = 1 - (dist[rows-1][cols-1] / max_len)
        return similarity
    
    def _calculate_geographic_distance(self, lat1, lon1, lat2, lon2):
        """Calculate the great circle distance between two points in kilometers"""
        # Convert decimal degrees to radians
        lat1, lon1, lat2, lon2 = map(np.radians, [float(lat1), float(lon1), float(lat2), float(lon2)])
        
        # Haversine formula
        dlon = lon2 - lon1
        dlat = lat2 - lat1
        a = np.sin(dlat/2)**2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon/2)**2
        c = 2 * np.arcsin(np.sqrt(a))
        r = 6371  # Radius of Earth in kilometers
        
        return c * r
    
    def _calculate_phone_similarity(self, phone1, phone2):
        """Calculate similarity between two phone numbers"""
        # Normalize phone numbers (remove non-digits)
        p1 = re.sub(r'\D', '', str(phone1))
        p2 = re.sub(r'\D', '', str(phone2))
        
        # Check if either is empty
        if not p1 or not p2:
            return 0
        
        # If digits are the same, return 1.0
        if p1 == p2:
            return 1.0
        
        # Check for partial matches (e.g., different area codes but same number)
        # Focus on last digits (usually more significant)
        min_len = min(len(p1), len(p2))
        
        if min_len >= 6:
            # Check last 6 digits
            if p1[-6:] == p2[-6:]:
                return 0.9
            
            # Check last 4 digits
            if p1[-4:] == p2[-4:]:
                return 0.7
        
        return 0.0
    
    def _calculate_group_characteristics(self, properties):
        """
        Calculate characteristics of a property group
        
        Args:
            properties: List of property dictionaries in the group
            
        Returns:
            Dict with group characteristics
        """
        # Calculate geographic centroid
        latitudes = [float(p["latitude"]) for p in properties if p.get("latitude")]
        longitudes = [float(p["longitude"]) for p in properties if p.get("longitude")]
        
        if latitudes and longitudes:
            centroid_lat = np.mean(latitudes)
            centroid_lng = np.mean(longitudes)
            
            # Calculate geographic spread (max distance from centroid)
            distances = [self._calculate_geographic_distance(
                centroid_lat, centroid_lng, lat, lng
            ) for lat, lng in zip(latitudes, longitudes)]
            
            geographic_spread = max(distances) if distances else 0
        else:
            centroid_lat = None
            centroid_lng = None
            geographic_spread = None
        
        # Analyze names for patterns
        names = [p.get("name", "") for p in properties if p.get("name")]
        name_words = []
        
        for name in names:
            words = re.findall(r'\b\w+\b', name.lower())
            name_words.extend(words)
        
        # Find common words in names
        word_counter = Counter(name_words)
        common_words = [word for word, count in word_counter.most_common(5) if count > 1]
        
        # Find common owners/contacts
        owners = [p.get("owner") or p.get("contact_name") for p in properties 
                 if p.get("owner") or p.get("contact_name")]
        
        owner_counter = Counter(owners)
        common_owners = [owner for owner, count in owner_counter.most_common(3) if count > 1]
        
        # Check group characteristics
        sources = [p.get("source") for p in properties if p.get("source")]
        source_counter = Counter(sources)
        primary_source = source_counter.most_common(1)[0][0] if source_counter else None
        
        # Check if properties are on multiple platforms
        platforms = [p.get("platform") for p in properties if p.get("platform")]
        platform_counter = Counter(platforms)
        multi_platform = len(platform_counter) > 1
        
        return {
            "centroid": {"latitude": centroid_lat, "longitude": centroid_lng} if centroid_lat and centroid_lng else None,
            "geographic_spread_km": geographic_spread,
            "common_name_words": common_words,
            "common_owners": common_owners,
            "primary_source": primary_source,
            "source_distribution": dict(source_counter),
            "multi_platform": multi_platform,
            "platform_distribution": dict(platform_counter) if platforms else None
        }
    
    def detect_ownership_patterns(self, properties, min_group_size=2):
        """
        Detect patterns of ownership across multiple properties
        
        Args:
            properties: List of property dictionaries
            min_group_size: Minimum number of properties to consider as a group
            
        Returns:
            Dict with ownership patterns
        """
        self.logger.info(f"Detecting ownership patterns among {len(properties)} properties")
        
        try:
            # Group properties by owner/contact
            owner_groups = {}
            
            for prop in properties:
                # Get owner information
                owner = prop.get("owner") or prop.get("contact_name")
                
                if owner:
                    # Normalize owner name
                    owner_key = owner.lower().strip()
                    
                    if owner_key not in owner_groups:
                        owner_groups[owner_key] = []
                    
                    owner_groups[owner_key].append(prop)
            
            # Filter groups by size
            large_owner_groups = {owner: group for owner, group in owner_groups.items() 
                                 if len(group) >= min_group_size}
            
            # Calculate statistics for each group
            ownership_patterns = []
            
            for owner, group in large_owner_groups.items():
                # Calculate group characteristics
                group_characteristics = self._calculate_group_characteristics(group)
                
                pattern = {
                    "owner": owner,
                    "property_count": len(group),
                    "properties": group,
                    **group_characteristics
                }
                
                ownership_patterns.append(pattern)
            
            # Sort patterns by property count (largest first)
            ownership_patterns.sort(key=lambda x: x["property_count"], reverse=True)
            
            self.logger.info(f"Found {len(ownership_patterns)} ownership patterns")
            
            return {
                "status": "success",
                "pattern_count": len(ownership_patterns),
                "patterns": ownership_patterns,
                "total_properties_in_patterns": sum(len(p["properties"]) for p in ownership_patterns)
            }
            
        except Exception as e:
            self.logger.error(f"Error detecting ownership patterns: {str(e)}")
            return {"error": f"Error detecting ownership patterns: {str(e)}"}
    
    def detect_geographic_clusters(self, properties, max_distance=0.2, min_cluster_size=3):
        """
        Detect geographic clusters of properties
        
        Args:
            properties: List of property dictionaries
            max_distance: Maximum distance in kilometers for clustering
            min_cluster_size: Minimum number of properties to consider as a cluster
            
        Returns:
            Dict with geographic clusters
        """
        self.logger.info(f"Detecting geographic clusters among {len(properties)} properties")
        
        try:
            # Extract properties with valid coordinates
            valid_properties = [p for p in properties if p.get("latitude") and p.get("longitude")]
            
            if len(valid_properties) < min_cluster_size:
                self.logger.warning(f"Not enough properties with valid coordinates (found {len(valid_properties)})")
                return {"error": "Not enough properties with valid coordinates"}
            
            # Extract coordinates
            coordinates = np.array([[float(p["latitude"]), float(p["longitude"])] for p in valid_properties])
            
            # Apply DBSCAN clustering
            db = DBSCAN(eps=max_distance/111, min_samples=min_cluster_size, metric='haversine')
            db.fit(np.radians(coordinates))
            
            # Get cluster labels
            labels = db.labels_
            
            # Count clusters (excluding noise with label -1)
            n_clusters = len(set(labels)) - (1 if -1 in labels else 0)
            
            self.logger.info(f"Found {n_clusters} geographic clusters")
            
            # Group properties by cluster
            clusters = []
            
            for cluster_id in range(n_clusters):
                # Get indices of properties in this cluster
                cluster_indices = np.where(labels == cluster_id)[0]
                
                # Get properties in this cluster
                cluster_properties = [valid_properties[i] for i in cluster_indices]
                
                # Calculate cluster characteristics
                cluster_characteristics = self._calculate_group_characteristics(cluster_properties)
                
                cluster = {
                    "cluster_id": cluster_id,
                    "properties": cluster_properties,
                    "property_count": len(cluster_properties),
                    **cluster_characteristics
                }
                
                clusters.append(cluster)
            
            # Sort clusters by size (largest first)
            clusters.sort(key=lambda x: x["property_count"], reverse=True)
            
            # Get properties not in any cluster (noise)
            noise_indices = np.where(labels == -1)[0]
            noise_properties = [valid_properties[i] for i in noise_indices]
            
            return {
                "status": "success",
                "cluster_count": n_clusters,
                "clusters": clusters,
                "noise_count": len(noise_properties),
                "total_properties": len(valid_properties)
            }
            
        except Exception as e:
            self.logger.error(f"Error detecting geographic clusters: {str(e)}")
            return {"error": f"Error detecting geographic clusters: {str(e)}"}
    
    def analyze_naming_patterns(self, properties):
        """
        Analyze patterns in property names
        
        Args:
            properties: List of property dictionaries
            
        Returns:
            Dict with naming patterns
        """
        self.logger.info(f"Analyzing naming patterns among {len(properties)} properties")
        
        try:
            # Extract names
            names = [p.get("name", "") for p in properties if p.get("name")]
            
            if not names:
                self.logger.warning("No property names found")
                return {"error": "No property names found"}
            
            # Extract words from names
            word_pattern = re.compile(r'\b\w+\b')
            all_words = []
            
            for name in names:
                words = word_pattern.findall(name.lower())
                all_words.extend(words)
            
            # Count word frequencies
            word_counter = Counter(all_words)
            
            # Find common words (excluding very common words)
            common_words = []
            stop_words = {'the', 'and', 'in', 'of', 'for', 'a', 'an', 'on', 'at', 'to', 'by'}
            
            for word, count in word_counter.most_common(50):
                if word not in stop_words and len(word) > 2:
                    common_words.append({"word": word, "count": count})
            
            # Look for specific hotel-related terms
            hotel_terms = {'hotel', 'inn', 'suites', 'lodge', 'resort', 'apartments',
                           'hostel', 'motel', 'residence', 'فندق', 'نزل', 'شقق', 'أجنحة'}
            
            hotel_term_counts = {term: word_counter.get(term, 0) for term in hotel_terms}
            
            # Filter non-zero counts
            hotel_term_counts = {term: count for term, count in hotel_term_counts.items() if count > 0}
            
            # Look for common naming patterns
            patterns = []
            
            # Pattern: <Name> Hotel/Inn/etc.
            suffix_pattern = re.compile(r'(.+)\s+(hotel|inn|suites|lodge|resort|apartments|hostel|motel|residence|فندق|نزل|شقق|أجنحة)', re.IGNORECASE)
            suffix_count = sum(1 for name in names if suffix_pattern.search(name.lower()))
            
            if suffix_count > 0:
                patterns.append({
                    "pattern": "<Name> Hotel/Inn/etc.",
                    "count": suffix_count,
                    "percentage": suffix_count / len(names) * 100
                })
            
            # Pattern: Hotel/Inn/etc. <Name>
            prefix_pattern = re.compile(r'(hotel|inn|suites|lodge|resort|apartments|hostel|motel|residence|فندق|نزل|شقق|أجنحة)\s+(.+)', re.IGNORECASE)
            prefix_count = sum(1 for name in names if prefix_pattern.search(name.lower()))
            
            if prefix_count > 0:
                patterns.append({
                    "pattern": "Hotel/Inn/etc. <Name>",
                    "count": prefix_count,
                    "percentage": prefix_count / len(names) * 100
                })
            
            # Pattern: Al/The <Name>
            al_pattern = re.compile(r'(the|al)\s+(.+)', re.IGNORECASE)
            al_count = sum(1 for name in names if al_pattern.search(name.lower()))
            
            if al_count > 0:
                patterns.append({
                    "pattern": "Al/The <Name>",
                    "count": al_count,
                    "percentage": al_count / len(names) * 100
                })
            
            self.logger.info(f"Analyzed {len(names)} property names")
            
            return {
                "status": "success",
                "total_names": len(names),
                "common_words": common_words[:20],  # Top 20 common words
                "hotel_term_counts": hotel_term_counts,
                "naming_patterns": patterns
            }
            
        except Exception as e:
            self.logger.error(f"Error analyzing naming patterns: {str(e)}")
            return {"error": f"Error analyzing naming patterns: {str(e)}"}