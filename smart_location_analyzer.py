# location_analysis/smart_location_analyzer.py
import os
import json
import logging
import numpy as np
import math
from datetime import datetime
from sklearn.cluster import DBSCAN
import requests
from urllib.parse import quote

class SmartLocationAnalyzer:
    """Advanced location analysis for identifying potential hotel locations"""
    
    def __init__(self, db=None, config=None):
        """Initialize the SmartLocationAnalyzer class"""
        self.db = db
        self.config = config
        self.logger = logging.getLogger("smart_location_analyzer")
        self.api_keys = self._load_api_keys()
        
    def _load_api_keys(self):
        """Load API keys from configuration"""
        api_keys = {}
        
        if self.config and 'api_keys' in self.config:
            api_keys = self.config['api_keys']
        
        return api_keys
    
    def analyze_commercial_areas(self, city, radius=5000):
        """
        Analyze commercial areas in a city to identify potential hotel locations
        
        Args:
            city: City name
            radius: Search radius in meters
            
        Returns:
            List of potential hotel locations
        """
        self.logger.info(f"Analyzing commercial areas in {city}")
        
        try:
            # Get city coordinates first
            city_coords = self._get_city_coordinates(city)
            
            if not city_coords:
                self.logger.error(f"Could not get coordinates for {city}")
                return []
            
            # Find commercial zones
            commercial_zones = self._find_commercial_zones(city_coords, radius)
            self.logger.info(f"Found {len(commercial_zones)} commercial zones in {city}")
            
            # For each commercial zone, look for buildings that might be hotels
            potential_hotels = []
            
            for zone in commercial_zones:
                zone_hotels = self._analyze_zone_for_hotels(zone["location"], zone["radius"])
                potential_hotels.extend(zone_hotels)
            
            self.logger.info(f"Found {len(potential_hotels)} potential hotels in commercial areas")
            
            return potential_hotels
            
        except Exception as e:
            self.logger.error(f"Error analyzing commercial areas: {str(e)}")
            return []
    
    def _get_city_coordinates(self, city):
        """
        Get the coordinates for a city using Nominatim API
        
        Args:
            city: City name
            
        Returns:
            Tuple (latitude, longitude) or None if not found
        """
        try:
            # Use Nominatim API to get coordinates
            encoded_city = quote(city)
            url = f"https://nominatim.openstreetmap.org/search?q={encoded_city}&format=json&limit=1"
            
            headers = {
                "User-Agent": "Map_researcher 0.5"
            }
            
            response = requests.get(url, headers=headers)
            
            if response.status_code == 200:
                data = response.json()
                
                if data and len(data) > 0:
                    latitude = float(data[0]["lat"])
                    longitude = float(data[0]["lon"])
                    return (latitude, longitude)
            
            return None
            
        except Exception as e:
            self.logger.error(f"Error getting city coordinates: {str(e)}")
            return None
    
    def _find_commercial_zones(self, city_coords, radius):
        """
        Find commercial zones in a city
        
        Args:
            city_coords: City coordinates (latitude, longitude)
            radius: Search radius in meters
            
        Returns:
            List of commercial zone dictionaries
        """
        try:
            # Use Overpass API to find commercial zones
            lat, lon = city_coords
            
            # Adjust radius to degree approximation
            radius_deg = radius / 111000  # Rough conversion from meters to degrees
            
            # Query Overpass API for commercial areas
            overpass_url = "https://overpass-api.de/api/interpreter"
            
            overpass_query = f"""
            [out:json];
            (
              node["landuse"="commercial"]({lat-radius_deg},{lon-radius_deg},{lat+radius_deg},{lon+radius_deg});
              way["landuse"="commercial"]({lat-radius_deg},{lon-radius_deg},{lat+radius_deg},{lon+radius_deg});
              relation["landuse"="commercial"]({lat-radius_deg},{lon-radius_deg},{lat+radius_deg},{lon+radius_deg});
              
              node["shop"]({lat-radius_deg},{lon-radius_deg},{lat+radius_deg},{lon+radius_deg});
              way["shop"]({lat-radius_deg},{lon-radius_deg},{lat+radius_deg},{lon+radius_deg});
              
              node["amenity"="marketplace"]({lat-radius_deg},{lon-radius_deg},{lat+radius_deg},{lon+radius_deg});
              way["amenity"="marketplace"]({lat-radius_deg},{lon-radius_deg},{lat+radius_deg},{lon+radius_deg});
            );
            out center;
            """
            
            response = requests.post(overpass_url, data={"data": overpass_query})
            
            if response.status_code == 200:
                data = response.json()
                
                # Extract commercial zones
                commercial_zones = []
                
                for element in data.get("elements", []):
                    if "center" in element:
                        # For ways and relations, use the center point
                        center_lat = element["center"]["lat"]
                        center_lon = element["center"]["lon"]
                    elif "lat" in element and "lon" in element:
                        # For nodes, use the node coordinates
                        center_lat = element["lat"]
                        center_lon = element["lon"]
                    else:
                        continue
                    
                    # Determine zone type and radius
                    zone_type = "unknown"
                    zone_radius = 100  # Default radius in meters
                    
                    if "tags" in element:
                        tags = element["tags"]
                        
                        if "landuse" in tags and tags["landuse"] == "commercial":
                            zone_type = "commercial_area"
                            zone_radius = 300  # Larger radius for commercial areas
                        elif "shop" in tags:
                            zone_type = "shopping_area"
                            shop_type = tags["shop"]
                            
                            # Adjust radius based on shop type
                            if shop_type in ["mall", "supermarket", "department_store"]:
                                zone_radius = 200
                            else:
                                zone_radius = 100
                        elif "amenity" in tags and tags["amenity"] == "marketplace":
                            zone_type = "marketplace"
                            zone_radius = 200
                    
                    zone_data = {
                        "location": (center_lat, center_lon),
                        "type": zone_type,
                        "radius": zone_radius,
                        "osm_id": element.get("id"),
                        "osm_type": element.get("type")
                    }
                    
                    commercial_zones.append(zone_data)
                
                # Remove duplicates by clustering nearby zones
                if commercial_zones:
                    clustered_zones = self._cluster_nearby_zones(commercial_zones)
                    return clustered_zones
                else:
                    return []
            
            return []
            
        except Exception as e:
            self.logger.error(f"Error finding commercial zones: {str(e)}")
            return []
    
    def _cluster_nearby_zones(self, zones, max_distance=200):
        """
        Cluster nearby zones to remove duplicates
        
        Args:
            zones: List of zone dictionaries
            max_distance: Maximum distance in meters to consider zones as the same cluster
            
        Returns:
            List of clustered zone dictionaries
        """
        if len(zones) <= 1:
            return zones
        
        # Extract coordinates
        coordinates = np.array([[zone["location"][0], zone["location"][1]] for zone in zones])
        
        # Convert max_distance from meters to degrees
        max_distance_deg = max_distance / 111000
        
        # Apply DBSCAN clustering
        clustering = DBSCAN(eps=max_distance_deg, min_samples=1, metric="haversine").fit(coordinates)
        
        # Group zones by cluster
        clusters = {}
        for i, cluster_id in enumerate(clustering.labels_):
            if cluster_id not in clusters:
                clusters[cluster_id] = []
            clusters[cluster_id].append(zones[i])
        
        # Merge zones in each cluster
        clustered_zones = []
        
        for cluster_id, cluster_zones in clusters.items():
            if len(cluster_zones) == 1:
                # Single zone in cluster, just add it
                clustered_zones.append(cluster_zones[0])
            else:
                # Multiple zones, merge them
                # Calculate average coordinates
                avg_lat = np.mean([zone["location"][0] for zone in cluster_zones])
                avg_lon = np.mean([zone["location"][1] for zone in cluster_zones])
                
                # Use maximum radius
                max_radius = max([zone["radius"] for zone in cluster_zones])
                
                # Determine most specific type
                zone_types = [zone["type"] for zone in cluster_zones]
                if "commercial_area" in zone_types:
                    merged_type = "commercial_area"
                elif "shopping_area" in zone_types:
                    merged_type = "shopping_area"
                else:
                    merged_type = zone_types[0]
                
                merged_zone = {
                    "location": (avg_lat, avg_lon),
                    "type": merged_type,
                    "radius": max_radius,
                    "merged_count": len(cluster_zones)
                }
                
                clustered_zones.append(merged_zone)
        
        return clustered_zones
    
    def _analyze_zone_for_hotels(self, zone_location, radius):
        """
        Analyze a zone for potential hotels
        
        Args:
            zone_location: Zone coordinates (latitude, longitude)
            radius: Search radius in meters
            
        Returns:
            List of potential hotel properties
        """
        try:
            lat, lon = zone_location
            
            # Adjust radius to degree approximation
            radius_deg = radius / 111000  # Rough conversion from meters to degrees
            
            # Query Overpass API for buildings and amenities that might be hotels
            overpass_url = "https://overpass-api.de/api/interpreter"
            
            overpass_query = f"""
            [out:json];
            (
              node["tourism"="hotel"]({lat-radius_deg},{lon-radius_deg},{lat+radius_deg},{lon+radius_deg});
              way["tourism"="hotel"]({lat-radius_deg},{lon-radius_deg},{lat+radius_deg},{lon+radius_deg});
              relation["tourism"="hotel"]({lat-radius_deg},{lon-radius_deg},{lat+radius_deg},{lon+radius_deg});
              
              node["tourism"="apartment"]({lat-radius_deg},{lon-radius_deg},{lat+radius_deg},{lon+radius_deg});
              way["tourism"="apartment"]({lat-radius_deg},{lon-radius_deg},{lat+radius_deg},{lon+radius_deg});
              
              node["tourism"="guest_house"]({lat-radius_deg},{lon-radius_deg},{lat+radius_deg},{lon+radius_deg});
              way["tourism"="guest_house"]({lat-radius_deg},{lon-radius_deg},{lat+radius_deg},{lon+radius_deg});
              
              node["building"="hotel"]({lat-radius_deg},{lon-radius_deg},{lat+radius_deg},{lon+radius_deg});
              way["building"="hotel"]({lat-radius_deg},{lon-radius_deg},{lat+radius_deg},{lon+radius_deg});
              
              node["building"="apartments"]({lat-radius_deg},{lon-radius_deg},{lat+radius_deg},{lon+radius_deg});
              way["building"="apartments"]({lat-radius_deg},{lon-radius_deg},{lat+radius_deg},{lon+radius_deg});
              
              node["amenity"="hotel"]({lat-radius_deg},{lon-radius_deg},{lat+radius_deg},{lon+radius_deg});
              way["amenity"="hotel"]({lat-radius_deg},{lon-radius_deg},{lat+radius_deg},{lon+radius_deg});
            );
            out center;
            """
            
            response = requests.post(overpass_url, data={"data": overpass_query})
            
            if response.status_code == 200:
                data = response.json()
                
                # Extract potential hotels
                potential_hotels = []
                
                for element in data.get("elements", []):
                    if "center" in element:
                        # For ways and relations, use the center point
                        building_lat = element["center"]["lat"]
                        building_lon = element["center"]["lon"]
                    elif "lat" in element and "lon" in element:
                        # For nodes, use the node coordinates
                        building_lat = element["lat"]
                        building_lon = element["lon"]
                    else:
                        continue
                    
                    # Extract tags
                    tags = element.get("tags", {})
                    
                    # Determine building type
                    building_type = "unknown"
                    confidence = 0.5  # Default confidence
                    
                    if "tourism" in tags:
                        tourism_value = tags["tourism"]
                        if tourism_value == "hotel":
                            building_type = "hotel"
                            confidence = 0.9
                        elif tourism_value == "apartment":
                            building_type = "apartment"
                            confidence = 0.8
                        elif tourism_value == "guest_house":
                            building_type = "guest_house"
                            confidence = 0.7
                    elif "building" in tags:
                        building_value = tags["building"]
                        if building_value == "hotel":
                            building_type = "hotel"
                            confidence = 0.9
                        elif building_value == "apartments":
                            building_type = "apartment_building"
                            confidence = 0.6
                    elif "amenity" in tags and tags["amenity"] == "hotel":
                        building_type = "hotel"
                        confidence = 0.9
                    
                    # Get name if available
                    name = tags.get("name", "Unnamed " + building_type.capitalize())
                    
                    # Check if it has address information
                    has_address = "addr:street" in tags or "addr:housenumber" in tags
                    
                    # Format address if available
                    address = ""
                    if has_address:
                        address_parts = []
                        
                        if "addr:housenumber" in tags:
                            address_parts.append(tags["addr:housenumber"])
                        
                        if "addr:street" in tags:
                            address_parts.append(tags["addr:street"])
                        
                        if "addr:city" in tags:
                            address_parts.append(tags["addr:city"])
                        
                        if "addr:postcode" in tags:
                            address_parts.append(tags["addr:postcode"])
                        
                        address = ", ".join(address_parts)
                    
                    hotel_data = {
                        "name": name,
                        "type": building_type,
                        "latitude": building_lat,
                        "longitude": building_lon,
                        "address": address,
                        "source": "osm",
                        "osm_id": element.get("id"),
                        "osm_type": element.get("type"),
                        "confidence": confidence,
                        "tags": tags,
                        "discovery_method": "commercial_zone_analysis",
                        "data_timestamp": datetime.now().isoformat()
                    }
                    
                    potential_hotels.append(hotel_data)
                
                return potential_hotels
            
            return []
            
        except Exception as e:
            self.logger.error(f"Error analyzing zone for hotels: {str(e)}")
            return []
    
    def analyze_tourist_areas(self, city, radius=5000):
        """
        Analyze tourist areas in a city to identify potential hotels
        
        Args:
            city: City name
            radius: Search radius in meters
            
        Returns:
            List of potential hotel properties
        """
        self.logger.info(f"Analyzing tourist areas in {city}")
        
        try:
            # Get city coordinates first
            city_coords = self._get_city_coordinates(city)
            
            if not city_coords:
                self.logger.error(f"Could not get coordinates for {city}")
                return []
            
            # Find tourist attractions
            tourist_attractions = self._find_tourist_attractions(city_coords, radius)
            self.logger.info(f"Found {len(tourist_attractions)} tourist attractions in {city}")
            
            # For each attraction, look for nearby accommodations
            potential_hotels = []
            
            for attraction in tourist_attractions:
                nearby_hotels = self._find_accommodations_near_poi(attraction["location"], 500)  # 500m radius around attraction
                potential_hotels.extend(nearby_hotels)
            
            # Remove duplicates
            unique_hotels = self._remove_duplicate_properties(potential_hotels)
            
            self.logger.info(f"Found {len(unique_hotels)} potential hotels near tourist attractions")
            
            return unique_hotels
            
        except Exception as e:
            self.logger.error(f"Error analyzing tourist areas: {str(e)}")
            return []
    
    def _find_tourist_attractions(self, city_coords, radius):
        """
        Find tourist attractions in a city
        
        Args:
            city_coords: City coordinates (latitude, longitude)
            radius: Search radius in meters
            
        Returns:
            List of tourist attraction dictionaries
        """
        try:
            lat, lon = city_coords
            
            # Adjust radius to degree approximation
            radius_deg = radius / 111000  # Rough conversion from meters to degrees
            
            # Query Overpass API for tourist attractions
            overpass_url = "https://overpass-api.de/api/interpreter"
            
            overpass_query = f"""
            [out:json];
            (
              node["tourism"="attraction"]({lat-radius_deg},{lon-radius_deg},{lat+radius_deg},{lon+radius_deg});
              way["tourism"="attraction"]({lat-radius_deg},{lon-radius_deg},{lat+radius_deg},{lon+radius_deg});
              relation["tourism"="attraction"]({lat-radius_deg},{lon-radius_deg},{lat+radius_deg},{lon+radius_deg});
              
              node["historic"]({lat-radius_deg},{lon-radius_deg},{lat+radius_deg},{lon+radius_deg});
              way["historic"]({lat-radius_deg},{lon-radius_deg},{lat+radius_deg},{lon+radius_deg});
              
              node["leisure"="park"]({lat-radius_deg},{lon-radius_deg},{lat+radius_deg},{lon+radius_deg});
              way["leisure"="park"]({lat-radius_deg},{lon-radius_deg},{lat+radius_deg},{lon+radius_deg});
              
              node["amenity"="theatre"]({lat-radius_deg},{lon-radius_deg},{lat+radius_deg},{lon+radius_deg});
              way["amenity"="theatre"]({lat-radius_deg},{lon-radius_deg},{lat+radius_deg},{lon+radius_deg});
              
              node["amenity"="marketplace"]({lat-radius_deg},{lon-radius_deg},{lat+radius_deg},{lon+radius_deg});
              way["amenity"="marketplace"]({lat-radius_deg},{lon-radius_deg},{lat+radius_deg},{lon+radius_deg});
            );
            out center;
            """
            
            response = requests.post(overpass_url, data={"data": overpass_query})
            
            if response.status_code == 200:
                data = response.json()
                
                # Extract tourist attractions
                attractions = []
                
                for element in data.get("elements", []):
                    if "center" in element:
                        # For ways and relations, use the center point
                        attraction_lat = element["center"]["lat"]
                        attraction_lon = element["center"]["lon"]
                    elif "lat" in element and "lon" in element:
                        # For nodes, use the node coordinates
                        attraction_lat = element["lat"]
                        attraction_lon = element["lon"]
                    else:
                        continue
                    
                    # Extract tags
                    tags = element.get("tags", {})
                    
                    # Determine attraction type
                    attraction_type = "unknown"
                    
                    if "tourism" in tags and tags["tourism"] == "attraction":
                        attraction_type = "tourist_attraction"
                    elif "historic" in tags:
                        attraction_type = "historic_site"
                    elif "leisure" in tags and tags["leisure"] == "park":
                        attraction_type = "park"
                    elif "amenity" in tags:
                        attraction_type = tags["amenity"]
                    
                    # Get name if available
                    name = tags.get("name", "Unnamed " + attraction_type.replace("_", " ").capitalize())
                    
                    attraction_data = {
                        "name": name,
                        "type": attraction_type,
                        "location": (attraction_lat, attraction_lon),
                        "osm_id": element.get("id"),
                        "osm_type": element.get("type"),
                        "importance": self._estimate_attraction_importance(tags)
                    }
                    
                    attractions.append(attraction_data)
                
                return attractions
            
            return []
            
        except Exception as e:
            self.logger.error(f"Error finding tourist attractions: {str(e)}")
            return []
    
    def _estimate_attraction_importance(self, tags):
        """
        Estimate the importance of a tourist attraction based on its tags
        
        Args:
            tags: OSM tags dictionary
            
        Returns:
            Float importance score (0-1)
        """
        importance = 0.5  # Default importance
        
        # Check if it has a Wikipedia tag (indicates significance)
        if "wikipedia" in tags or "wikidata" in tags:
            importance += 0.3
        
        # Check for description
        if "description" in tags:
            importance += 0.1
        
        # Check for website (indicates commercial importance)
        if "website" in tags:
            importance += 0.1
        
        # Limit importance to 0-1 range
        return min(1.0, importance)
    
    def _find_accommodations_near_poi(self, poi_location, radius):
        """
        Find accommodations near a point of interest
        
        Args:
            poi_location: POI coordinates (latitude, longitude)
            radius: Search radius in meters
            
        Returns:
            List of accommodation properties
        """
        # Similar to _analyze_zone_for_hotels, but with parameter adjustments
        # for tourist areas vs. commercial areas
        try:
            lat, lon = poi_location
            
            # Adjust radius to degree approximation
            radius_deg = radius / 111000  # Rough conversion from meters to degrees
            
            # Query Overpass API for accommodations
            overpass_url = "https://overpass-api.de/api/interpreter"
            
            overpass_query = f"""
            [out:json];
            (
              node["tourism"="hotel"]({lat-radius_deg},{lon-radius_deg},{lat+radius_deg},{lon+radius_deg});
              way["tourism"="hotel"]({lat-radius_deg},{lon-radius_deg},{lat+radius_deg},{lon+radius_deg});
              
              node["tourism"="apartment"]({lat-radius_deg},{lon-radius_deg},{lat+radius_deg},{lon+radius_deg});
              way["tourism"="apartment"]({lat-radius_deg},{lon-radius_deg},{lat+radius_deg},{lon+radius_deg});
              
              node["tourism"="guest_house"]({lat-radius_deg},{lon-radius_deg},{lat+radius_deg},{lon+radius_deg});
              way["tourism"="guest_house"]({lat-radius_deg},{lon-radius_deg},{lat+radius_deg},{lon+radius_deg});
              
              node["tourism"="hostel"]({lat-radius_deg},{lon-radius_deg},{lat+radius_deg},{lon+radius_deg});
              way["tourism"="hostel"]({lat-radius_deg},{lon-radius_deg},{lat+radius_deg},{lon+radius_deg});
              
              node["building"="hotel"]({lat-radius_deg},{lon-radius_deg},{lat+radius_deg},{lon+radius_deg});
              way["building"="hotel"]({lat-radius_deg},{lon-radius_deg},{lat+radius_deg},{lon+radius_deg});
            );
            out center;
            """
            
            response = requests.post(overpass_url, data={"data": overpass_query})
            
            if response.status_code == 200:
                data = response.json()
                
                # Extract potential accommodations
                accommodations = []
                
                for element in data.get("elements", []):
                    # Process similar to _analyze_zone_for_hotels method
                    if "center" in element:
                        # For ways and relations, use the center point
                        building_lat = element["center"]["lat"]
                        building_lon = element["center"]["lon"]
                    elif "lat" in element and "lon" in element:
                        # For nodes, use the node coordinates
                        building_lat = element["lat"]
                        building_lon = element["lon"]
                    else:
                        continue
                    
                    # Extract tags
                    tags = element.get("tags", {})
                    
                    # Determine building type
                    building_type = "unknown"
                    confidence = 0.5
                    
                    if "tourism" in tags:
                        tourism_value = tags["tourism"]
                        if tourism_value == "hotel":
                            building_type = "hotel"
                            confidence = 0.9
                        elif tourism_value == "apartment":
                            building_type = "apartment"
                            confidence = 0.8
                        elif tourism_value == "guest_house":
                            building_type = "guest_house"
                            confidence = 0.7
                        elif tourism_value == "hostel":
                            building_type = "hostel"
                            confidence = 0.7
                    elif "building" in tags and tags["building"] == "hotel":
                        building_type = "hotel"
                        confidence = 0.9
                    
                    # Get name if available
                    name = tags.get("name", "Unnamed " + building_type.capitalize())
                    
                    # Format address if available
                    address = ""
                    address_parts = []
                    
                    if "addr:housenumber" in tags:
                        address_parts.append(tags["addr:housenumber"])
                    
                    if "addr:street" in tags:
                        address_parts.append(tags["addr:street"])
                    
                    if "addr:city" in tags:
                        address_parts.append(tags["addr:city"])
                    
                    address = ", ".join(address_parts)
                    
                    # Calculate distance from POI
                    distance = self._haversine_distance(lat, lon, building_lat, building_lon) * 1000  # in meters
                    
                    accommodation_data = {
                        "name": name,
                        "type": building_type,
                        "latitude": building_lat,
                        "longitude": building_lon,
                        "address": address,
                        "source": "osm",
                        "osm_id": element.get("id"),
                        "osm_type": element.get("type"),
                        "confidence": confidence,
                        "distance_to_poi": distance,
                        "tags": tags,
                        "discovery_method": "tourist_area_analysis",
                        "data_timestamp": datetime.now().isoformat()
                    }
                    
                    accommodations.append(accommodation_data)
                
                return accommodations
            
            return []
            
        except Exception as e:
            self.logger.error(f"Error finding accommodations near POI: {str(e)}")
            return []
    
    def _remove_duplicate_properties(self, properties):
        """
        Remove duplicate properties from the results
        
        Args:
            properties: List of property dictionaries
            
        Returns:
            Deduplicated list of properties
        """
        unique_properties = []
        seen_ids = set()
        seen_coords = set()
        
        for prop in properties:
            # If we have an OSM ID, use that for deduplication
            if prop.get("osm_id") and prop["osm_id"] not in seen_ids:
                seen_ids.add(prop["osm_id"])
                unique_properties.append(prop)
                continue
            
            # If we have coordinates, check proximity
            if prop.get("latitude") and prop.get("longitude"):
                is_duplicate = False
                coord_key = f"{prop['latitude']:.6f},{prop['longitude']:.6f}"
                
                if coord_key in seen_coords:
                    is_duplicate = True
                
                if not is_duplicate:
                    seen_coords.add(coord_key)
                    unique_properties.append(prop)
                    continue
            
            # If we don't have OSM ID or coordinates, just add it
            unique_properties.append(prop)
        
        return unique_properties
    
    def _haversine_distance(self, lat1, lon1, lat2, lon2):
        """Calculate the great circle distance between two points in kilometers"""
        # Convert decimal degrees to radians
        lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])
        
        # Haversine formula
        dlon = lon2 - lon1
        dlat = lat2 - lat1
        a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
        c = 2 * math.asin(math.sqrt(a))
        r = 6371  # Radius of Earth in kilometers
        
        return c * r
    
    def analyze_major_roads(self, city, radius=5000):
        """
        Analyze major roads in a city to identify potential hotels
        
        Args:
            city: City name
            radius: Search radius in meters
            
        Returns:
            List of potential hotel properties
        """
        self.logger.info(f"Analyzing major roads in {city}")
        
        # Similar implementation to analyze_commercial_areas but focused on major roads
        # Hotels are often located along major roads for visibility and accessibility
        pass
    
    def analyze_residential_commercial_boundaries(self, city, radius=5000):
        """
        Analyze boundaries between residential and commercial areas
        
        Args:
            city: City name
            radius: Search radius in meters
            
        Returns:
            List of potential hotel properties
        """
        self.logger.info(f"Analyzing residential-commercial boundaries in {city}")
        
        # Implement analysis of the boundaries between residential and commercial areas
        # These transition zones often contain small hotels and furnished apartments
        pass