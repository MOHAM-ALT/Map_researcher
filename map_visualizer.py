# visualization/map_visualizer.py

import folium
from folium.plugins import MarkerCluster
import os
import logging
from datetime import datetime

class MapVisualizer:
    """Class for visualizing hotels on interactive maps"""
    
    def __init__(self, db=None, config=None):
        self.db = db
        self.config = config
        self.logger = logging.getLogger("map_visualizer")
    
    def create_hotel_map(self, hotels, output_path=None, title=None):
        """
        Create an interactive map with hotel markers
        
        Args:
            hotels: List of hotel dictionaries
            output_path: Output file path (optional)
            title: Map title (optional)
            
        Returns:
            Dict with map creation results
        """
        self.logger.info(f"Creating map for {len(hotels)} hotels")
        
        try:
            # Filter hotels with valid coordinates
            valid_hotels = [h for h in hotels if h.get('latitude') and h.get('longitude')]
            
            if not valid_hotels:
                self.logger.warning("No hotels with valid coordinates")
                return {"error": "No hotels with valid coordinates"}
            
            # Determine map center
            center_lat = sum(float(h['latitude']) for h in valid_hotels) / len(valid_hotels)
            center_lng = sum(float(h['longitude']) for h in valid_hotels) / len(valid_hotels)
            
            # Create map
            m = folium.Map(location=[center_lat, center_lng], zoom_start=13)
            
            # Add title
            if title:
                title_html = f"""
                    <h3 align="center" style="font-size:16px">{title}</h3>
                    <p align="center">Created on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
                """
                m.get_root().html.add_child(folium.Element(title_html))
            
            # Add cluster markers
            marker_cluster = MarkerCluster().add_to(m)
            
            # Add hotel markers
            for hotel in valid_hotels:
                # Determine marker color based on classification/confidence
                color = 'blue'  # Default color
                
                if 'is_hotel_prediction' in hotel:
                    color = 'green' if hotel['is_hotel_prediction'] else 'gray'
                
                if 'hotel_confidence' in hotel:
                    confidence = float(hotel['hotel_confidence'])
                    if confidence >= 0.8:
                        color = 'green'
                    elif confidence >= 0.5:
                        color = 'orange'
                    else:
                        color = 'red'
                
                # Create popup content
                popup_html = f"""
                <div style="min-width:200px">
                    <h4>{hotel.get('name', 'Unknown')}</h4>
                """
                
                if hotel.get('address'):
                    popup_html += f"<p><b>Address:</b> {hotel['address']}</p>"
                
                if hotel.get('category'):
                    popup_html += f"<p><b>Category:</b> {hotel['category']}</p>"
                
                if hotel.get('rating'):
                    popup_html += f"<p><b>Rating:</b> {hotel['rating']}</p>"
                
                if hotel.get('hotel_confidence'):
                    popup_html += f"<p><b>Hotel Confidence:</b> {hotel['hotel_confidence']:.2f}</p>"
                
                if hotel.get('source'):
                    popup_html += f"<p><b>Source:</b> {hotel['source']}</p>"
                
                popup_html += "</div>"
                
                # Create marker
                folium.Marker(
                    location=[float(hotel['latitude']), float(hotel['longitude'])],
                    popup=folium.Popup(popup_html, max_width=300),
                    tooltip=hotel.get('name', 'Unknown'),
                    icon=folium.Icon(color=color, icon='home')
                ).add_to(marker_cluster)
            
            # Save map if output path provided
            if output_path:
                # Create directory if it doesn't exist
                os.makedirs(os.path.dirname(output_path), exist_ok=True)
                m.save(output_path)
                self.logger.info(f"Map saved to {output_path}")
            
            return {
                "status": "success",
                "map_object": m,
                "hotel_count": len(valid_hotels),
                "file_path": output_path
            }
            
        except Exception as e:
            self.logger.error(f"Error creating map: {str(e)}")
            return {"error": f"Error creating map: {str(e)}"}