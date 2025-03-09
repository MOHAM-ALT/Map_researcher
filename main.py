#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Map_researcher 0.3V - Hotel data collection and analysis application
"""

# ================================================
# 1. BASIC IMPORTS AND BOOTSTRAP
# ================================================
import os
import sys
import time
import logging
import argparse
import math
import re
from datetime import datetime, timedelta

try:
    # Set excepthook to prevent crashes
    import sys
    def custom_excepthook(exc_type, exc_value, exc_traceback):
        print("*** ERROR: برنامج توقف بسبب خطأ ***")
        print(f"نوع الخطأ: {exc_type.__name__}")
        print(f"رسالة الخطأ: {exc_value}")
        import traceback
        traceback.print_tb(exc_traceback)
        print("\nاضغط Enter للخروج...")
        input()
    sys.excepthook = custom_excepthook
except:
    pass

# Try to load bootstrap first
try:
    import bootstrap
    bootstrap.run_bootstrap()
except ImportError:
    print("Warning: bootstrap module not found, some features may be limited")

# ================================================
# 2. LIBRARY HANDLING WITH FALLBACKS
# ================================================
# Try to import colorama with error handling if not available
try:
    from colorama import Fore, Back, Style, init
    # Initialize color system
    init(autoreset=True)
    COLORAMA_AVAILABLE = True
except ImportError:
    print("Warning: colorama library not found. Installing required packages...")
    # Try to install the library
    try:
        import subprocess
        subprocess.check_call([sys.executable, "-m", "pip", "install", "colorama"])
        from colorama import Fore, Back, Style, init
        init(autoreset=True)
        COLORAMA_AVAILABLE = True
        print("colorama installed successfully.")
    except:
        print("Failed to install colorama. Will continue without colored output.")
        # Define alternative color variables if library is not available
        class DummyColor:
            def __getattr__(self, name):
                return ""
        Fore = Back = Style = DummyColor()
        COLORAMA_AVAILABLE = False

# Try to import tabulate with error handling
try:
    from tabulate import tabulate
except ImportError:
    print("Warning: tabulate library not found. Installing required packages...")
    try:
        import subprocess
        subprocess.check_call([sys.executable, "-m", "pip", "install", "tabulate"])
        from tabulate import tabulate
        print("tabulate installed successfully.")
    except:
        print("Failed to install tabulate. Some display features may not work properly.")
        
        # Define a simple tabulate function as fallback
        def tabulate(data, headers=None, tablefmt="simple"):
            if not data:
                return ""
            
            result = ""
            if headers:
                result += " | ".join(str(h) for h in headers) + "\n"
                result += "-" * (len(result)) + "\n"
            
            for row in data:
                result += " | ".join(str(cell) for cell in row) + "\n"
                
            return result

# Try to import rich with error handling
try:
    from rich.console import Console
    from rich.table import Table
    from rich.progress import track
except ImportError:
    print("Warning: rich library not found. Installing required packages...")
    try:
        import subprocess
        subprocess.check_call([sys.executable, "-m", "pip", "install", "rich"])
        from rich.console import Console
        from rich.table import Table
        from rich.progress import track
        print("rich installed successfully.")
    except:
        print("Failed to install rich. Will use alternative display methods.")
        
        # Define simple alternatives
        class DummyTable:
            def __init__(self, *args, **kwargs):
                self.rows = []
                self.headers = []
                
            def add_column(self, header, *args, **kwargs):
                self.headers.append(header)
                
            def add_row(self, *cells):
                self.rows.append(cells)
                
            def __str__(self):
                return tabulate(self.rows, self.headers)
            
        class DummyConsole:
            def print(self, *args, **kwargs):
                if isinstance(args[0], DummyTable):
                    print(str(args[0]))
                else:
                    print(*args)
                    
        def track(iterable, description=None, total=None):
            if description:
                print(description)
            return iterable
        
        Console = DummyConsole
        Table = DummyTable

# Try to import folium with error handling
try:
    import folium
except ImportError:
    print("Warning: folium library not found. Installing required packages...")
    try:
        import subprocess
        subprocess.check_call([sys.executable, "-m", "pip", "install", "folium"])
        import folium
        print("folium installed successfully.")
    except:
        print("Failed to install folium. Map visualization will not be available.")
        
        # Create a minimal folium fallback
        class Map:
            def __init__(self, *args, **kwargs):
                print("Warning: folium not available. Map features limited.")
                self._children = {}
            
            def add_to(self, obj):
                return self
            
            def add_child(self, child, name=None):
                return self
            
            def save(self, path):
                with open(path, "w") as f:
                    f.write("<html><body><h1>Map Not Available</h1><p>Folium library not installed</p></body></html>")
                print(f"Created placeholder map at {path}")
                return path
        
        class Marker:
            def __init__(self, *args, **kwargs):
                pass
            
            def add_to(self, obj):
                return self
        
        class Icon:
            def __init__(self, *args, **kwargs):
                pass
        
        # Create the folium module
        folium = type('folium', (), {
            'Map': Map,
            'Marker': Marker,
            'Icon': Icon
        })

# Try to import web browser module
try:
    import webbrowser
except ImportError:
    print("Warning: webbrowser module not available. Opening maps in browser will not work.")

# Import data processing libraries
try:
    import pandas as pd
except ImportError:
    print("Warning: pandas library not found. Installing required packages...")
    try:
        import subprocess
        subprocess.check_call([sys.executable, "-m", "pip", "install", "pandas"])
        import pandas as pd
        print("pandas installed successfully.")
    except:
        print("Failed to install pandas. Data processing capabilities will be limited.")

# Import database and request libraries
import sqlite3
import csv
import json
import requests
from typing import Dict, List, Tuple, Optional, Any, Union

# ================================================
# 3. LOGGING SETUP
# ================================================
# Setup logging system
try:
    log_dir = os.path.dirname("logs/hotel_researcher.log")
    os.makedirs(log_dir, exist_ok=True)
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler("logs/hotel_researcher.log", encoding='utf-8'),
            logging.StreamHandler()
        ]
    )
except Exception as e:
    print(f"Error setting up logging: {e}. Defaulting to console logging only.")
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

logger = logging.getLogger("Map_researcher")

# ================================================
# 4. DATABASE CLASS
# ================================================
class Database:
    """Class for handling different database types"""
    
    def __init__(self, db_type: str, db_path: str):
        """
        Initialize database connection
        
        Args:
            db_type (str): Database type ('sqlite', 'csv', 'excel', 'postgresql')
            db_path (str): Database path or filename
        """
        # Store database type and path
        self.db_type = db_type.lower()
        self.db_path = db_path
        self.conn = None
        self.cursor = None
        
        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        
        # Initialize database connection based on type
        if self.db_type == 'sqlite':
            self.conn = sqlite3.connect(db_path)
            self.cursor = self.conn.cursor()
            self._setup_sqlite_schema()
        elif self.db_type == 'postgresql':
            # Temporarily disable PostgreSQL and use SQLite instead
            logger.warning("PostgreSQL support temporarily disabled. Using SQLite instead.")
            print(f"{Fore.YELLOW}PostgreSQL support temporarily disabled. Using SQLite instead.{Style.RESET_ALL}")
            # Use SQLite as an alternative
            self.db_type = 'sqlite'
            default_path = "data/hotels.db"
            self.db_path = default_path
            self.conn = sqlite3.connect(default_path)
            self.cursor = self.conn.cursor()
            self._setup_sqlite_schema()
        elif self.db_type not in ['csv', 'excel']:
            logger.error(f"Unsupported database type: {db_type}")
            print(f"{Fore.RED}Unsupported database type: {db_type}. Using SQLite instead.{Style.RESET_ALL}")
            # Use SQLite as an alternative
            self.db_type = 'sqlite'
            default_path = "data/hotels.db"
            self.db_path = default_path
            self.conn = sqlite3.connect(default_path)
            self.cursor = self.conn.cursor()
            self._setup_sqlite_schema()
    
    # Create SQLite database structure
    def _setup_sqlite_schema(self):
        """Create SQLite database tables"""
        # Hotels table
        self.cursor.execute('''
        CREATE TABLE IF NOT EXISTS hotels (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            address TEXT,
            city TEXT,
            country TEXT,
            latitude REAL,
            longitude REAL,
            phone TEXT,
            email TEXT,
            website TEXT,
            stars INTEGER,
            price_range TEXT,
            facilities TEXT,
            legal_status TEXT,
            data_source TEXT,
            last_updated TIMESTAMP,
            google_maps_match TEXT,
            coordinate_distance REAL,
            additional_info TEXT
        )
        ''')
        
        # Searches table
        self.cursor.execute('''
        CREATE TABLE IF NOT EXISTS searches (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            query TEXT NOT NULL,
            location TEXT,
            timestamp TIMESTAMP,
            results_count INTEGER,
            search_params TEXT
        )
        ''')
        
        # Save changes
        self.conn.commit()
    
    # Create PostgreSQL database structure
    def _setup_postgresql_schema(self):
        """Create PostgreSQL database tables"""
        # Hotels table
        self.cursor.execute('''
        CREATE TABLE IF NOT EXISTS hotels (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL,
            address TEXT,
            city TEXT,
            country TEXT,
            latitude REAL,
            longitude REAL,
            phone TEXT,
            email TEXT,
            website TEXT,
            stars INTEGER,
            price_range TEXT,
            facilities TEXT,
            legal_status TEXT,
            data_source TEXT,
            last_updated TIMESTAMP,
            google_maps_match TEXT,
            coordinate_distance REAL,
            additional_info TEXT
        )
        ''')
        
        # Searches table
        self.cursor.execute('''
        CREATE TABLE IF NOT EXISTS searches (
            id SERIAL PRIMARY KEY,
            query TEXT NOT NULL,
            location TEXT,
            timestamp TIMESTAMP,
            results_count INTEGER,
            search_params TEXT
        )
        ''')
        
        # Save changes
        self.conn.commit()
    
    # Save hotel information to the database
    def save_hotel(self, hotel_data: Dict[str, Any]) -> int:
        """
        Save hotel information to the database
        
        Args:
            hotel_data (Dict): Hotel data
            
        Returns:
            int: Hotel ID
        """
        # Save to SQL database
        if self.db_type == 'sqlite' or self.db_type == 'postgresql':
            # Add update date
            hotel_data['last_updated'] = datetime.now()
            
            # Prepare input values
            columns = ', '.join(hotel_data.keys())
            placeholders = ', '.join(['?' for _ in hotel_data])
            
            # Modify tags for PostgreSQL
            if self.db_type == 'postgresql':
                placeholders = ', '.join(['%s' for _ in hotel_data])
            
            # Create insert query
            query = f"INSERT INTO hotels ({columns}) VALUES ({placeholders})"
            
            # Execute query
            self.cursor.execute(query, list(hotel_data.values()))
            self.conn.commit()
            
            # Return added hotel ID
            if self.db_type == 'sqlite':
                return self.cursor.lastrowid
            else:
                self.cursor.execute("SELECT lastval()")
                return self.cursor.fetchone()[0]
        
        # Save to CSV file
        elif self.db_type == 'csv':
            # Add update date
            hotel_data['last_updated'] = datetime.now().isoformat()
            
            # Check if file exists
            file_exists = os.path.isfile(self.db_path)
            
            # Write data to CSV file
            with open(self.db_path, mode='a', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=hotel_data.keys())
                
                # Write headers if file is new
                if not file_exists:
                    writer.writeheader()
                
                writer.writerow(hotel_data)
                
            # Read file to get row count (to return approximate ID)
            with open(self.db_path, mode='r', encoding='utf-8') as f:
                return sum(1 for _ in f) - 1  # Subtract 1 to account for column header
        
        # Save to Excel file
        elif self.db_type == 'excel':
            # Add update date
            hotel_data['last_updated'] = datetime.now()
            
            # Check if file exists
            if os.path.isfile(self.db_path):
                # Read existing file
                df = pd.read_excel(self.db_path)
                
                # Add new data
                new_row = pd.DataFrame([hotel_data])
                df = pd.concat([df, new_row], ignore_index=True)
            else:
                # Create new DataFrame
                df = pd.DataFrame([hotel_data])
            
            # Save to Excel file
            df.to_excel(self.db_path, index=False)
            
            # Return row count as ID
            return len(df)
    
    # Save search query to the database
    def save_search(self, search_data: Dict[str, Any]) -> int:
        """
        Save search query to the database
        
        Args:
            search_data (Dict): Search data
            
        Returns:
            int: Search ID
        """
        # Save to SQL database
        if self.db_type == 'sqlite' or self.db_type == 'postgresql':
            # Add search time
            search_data['timestamp'] = datetime.now()
            
            # Convert search parameters to JSON if present
            if 'search_params' in search_data and isinstance(search_data['search_params'], dict):
                search_data['search_params'] = json.dumps(search_data['search_params'])
            
            # Prepare input values
            columns = ', '.join(search_data.keys())
            placeholders = ', '.join(['?' for _ in search_data])
            
            # Modify tags for PostgreSQL
            if self.db_type == 'postgresql':
                placeholders = ', '.join(['%s' for _ in search_data])
            
            # Create insert query
            query = f"INSERT INTO searches ({columns}) VALUES ({placeholders})"
            
            # Execute query
            self.cursor.execute(query, list(search_data.values()))
            self.conn.commit()
            
            # Return added search ID
            if self.db_type == 'sqlite':
                return self.cursor.lastrowid
            else:
                self.cursor.execute("SELECT lastval()")
                return self.cursor.fetchone()[0]
        
        # We won't store queries in CSV or Excel in this example
        # Can be added in the future if needed
        
        return -1
    
    # Search for hotels using different criteria
    def search_hotels(self, query: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Search for hotels using different criteria
        
        Args:
            query (Dict): Search criteria
            
        Returns:
            List[Dict]: List of hotels matching criteria
        """
        # Search in SQL database
        if self.db_type == 'sqlite' or self.db_type == 'postgresql':
            # Build search query
            conditions = []
            params = []
            
            for key, value in query.items():
                if value:
                    if isinstance(value, str) and '%' in value:
                        # Search using LIKE
                        conditions.append(f"{key} LIKE ?")
                        params.append(value)
                    else:
                        # Search with exact match
                        conditions.append(f"{key} = ?")
                        params.append(value)
            
            # Modify tags for PostgreSQL
            if self.db_type == 'postgresql':
                conditions = [c.replace('?', '%s') for c in conditions]
            
            # Create final query
            sql_query = "SELECT * FROM hotels"
            if conditions:
                sql_query += " WHERE " + " AND ".join(conditions)
            
            # Execute query
            self.cursor.execute(sql_query, params)
            
            # Retrieve results
            columns = [desc[0] for desc in self.cursor.description]
            results = []
            
            for row in self.cursor.fetchall():
                results.append(dict(zip(columns, row)))
            
            return results
        
        # Search in CSV file
        elif self.db_type == 'csv':
            # Read CSV file
            if not os.path.isfile(self.db_path):
                return []
            
            df = pd.read_csv(self.db_path, encoding='utf-8')
            
            # Apply search criteria
            for key, value in query.items():
                if value and key in df.columns:
                    if isinstance(value, str) and '%' in value:
                        # Search using contains
                        value = value.replace('%', '')
                        df = df[df[key].str.contains(value, na=False)]
                    else:
                        # Search with exact match
                        df = df[df[key] == value]
            
            # Convert results to list of dictionaries
            return df.to_dict('records')
        
        # Search in Excel file
        elif self.db_type == 'excel':
            # Read Excel file
            if not os.path.isfile(self.db_path):
                return []
            
            df = pd.read_excel(self.db_path)
            
            # Apply search criteria
            for key, value in query.items():
                if value and key in df.columns:
                    if isinstance(value, str) and '%' in value:
                        # Search using contains
                        value = value.replace('%', '')
                        df = df[df[key].str.contains(value, na=False)]
                    else:
                        # Search with exact match
                        df = df[df[key] == value]
            
            # Convert results to list of dictionaries
            return df.to_dict('records')
    
    # Get all hotels from database
    def get_all_hotels(self) -> List[Dict[str, Any]]:
        """
        Get all hotels from the database
        
        Returns:
            List[Dict]: List of all hotels
        """
        return self.search_hotels({})
    
    # Get hotel by ID
    def get_hotel_by_id(self, hotel_id: int) -> Optional[Dict[str, Any]]:
        """
        Get hotel by ID
        
        Args:
            hotel_id (int): Hotel ID
            
        Returns:
            Dict: Hotel data or None if not found
        """
        results = self.search_hotels({"id": hotel_id})
        return results[0] if results else None
    
    # Update hotel information
    def update_hotel(self, hotel_id: int, update_data: Dict[str, Any]) -> bool:
        """
        Update hotel information
        
        Args:
            hotel_id (int): Hotel ID
            update_data (Dict): Updated hotel data
            
        Returns:
            bool: Success status
        """
        # Update in SQL database
        if self.db_type == 'sqlite' or self.db_type == 'postgresql':
            # Add update date
            update_data['last_updated'] = datetime.now()
            
            # Build update query
            updates = []
            params = []
            
            for key, value in update_data.items():
                updates.append(f"{key} = ?")
                params.append(value)
            
            # Modify tags for PostgreSQL
            if self.db_type == 'postgresql':
                updates = [u.replace('?', '%s') for u in updates]
            
            # Add hotel ID
            params.append(hotel_id)
            
            # Create final query
            query = f"UPDATE hotels SET {', '.join(updates)} WHERE id = ?"
            if self.db_type == 'postgresql':
                query = query.replace('?', '%s')
            
            # Execute query
            self.cursor.execute(query, params)
            self.conn.commit()
            
            return self.cursor.rowcount > 0
        
        # Update in CSV file
        elif self.db_type == 'csv':
            # Add update date
            update_data['last_updated'] = datetime.now().isoformat()
            
            # Check if file exists
            if not os.path.isfile(self.db_path):
                return False
            
            # Read CSV file
            df = pd.read_csv(self.db_path, encoding='utf-8')
            
            # Find hotel by ID
            if 'id' not in df.columns or hotel_id not in df['id'].values:
                return False
            
            # Update data
            for key, value in update_data.items():
                if key in df.columns:
                    df.loc[df['id'] == hotel_id, key] = value
            
            # Save to CSV file
            df.to_csv(self.db_path, index=False, encoding='utf-8')
            
            return True
        
        # Update in Excel file
        elif self.db_type == 'excel':
            # Add update date
            update_data['last_updated'] = datetime.now()
            
            # Check if file exists
            if not os.path.isfile(self.db_path):
                return False
            
            # Read Excel file
            df = pd.read_excel(self.db_path)
            
            # Find hotel by ID
            if 'id' not in df.columns or hotel_id not in df['id'].values:
                return False
            
            # Update data
            for key, value in update_data.items():
                if key in df.columns:
                    df.loc[df['id'] == hotel_id, key] = value
            
            # Save to Excel file
            df.to_excel(self.db_path, index=False)
            
            return True
    
    # Delete hotel from database
    def delete_hotel(self, hotel_id: int) -> bool:
        """
        Delete hotel from database
        
        Args:
            hotel_id (int): Hotel ID
            
        Returns:
            bool: Success status
        """
        # Delete from SQL database
        if self.db_type == 'sqlite' or self.db_type == 'postgresql':
            # Create delete query
            query = "DELETE FROM hotels WHERE id = ?"
            if self.db_type == 'postgresql':
                query = query.replace('?', '%s')
            
            # Execute query
            self.cursor.execute(query, (hotel_id,))
            self.conn.commit()
            
            return self.cursor.rowcount > 0
        
        # Delete from CSV file
        elif self.db_type == 'csv':
            # Check if file exists
            if not os.path.isfile(self.db_path):
                return False
            
            # Read CSV file
            df = pd.read_csv(self.db_path, encoding='utf-8')
            
            # Find hotel by ID
            if 'id' not in df.columns or hotel_id not in df['id'].values:
                return False
            
            # Delete row
            df = df[df['id'] != hotel_id]
            
            # Save to CSV file
            df.to_csv(self.db_path, index=False, encoding='utf-8')
            
            return True
        
        # Delete from Excel file
        elif self.db_type == 'excel':
            # Check if file exists
            if not os.path.isfile(self.db_path):
                return False
            
            # Read Excel file
            df = pd.read_excel(self.db_path)
            
            # Find hotel by ID
            if 'id' not in df.columns or hotel_id not in df['id'].values:
                return False
            
            # Delete row
            df = df[df['id'] != hotel_id]
            
            # Save to Excel file
            df.to_excel(self.db_path, index=False)
            
            return True
    
    # Export data to different formats
    def export_data(self, output_format: str, output_path: str, query: Dict[str, Any] = None) -> bool:
        """
        Export data to different formats
        
        Args:
            output_format (str): Output format (csv, excel, json)
            output_path (str): Output file path
            query (Dict): Search criteria (None for all data)
            
        Returns:
            bool: Success status
        """
        # Get data
        data = self.search_hotels(query or {})
        
        if not data:
            logger.warning("No data to export")
            return False
        
        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        # Export based on format
        output_format = output_format.lower()
        
        try:
            if output_format == 'csv':
                # Convert to DataFrame and export to CSV
                df = pd.DataFrame(data)
                df.to_csv(output_path, index=False, encoding='utf-8')
                return True
            
            elif output_format == 'excel':
                # Convert to DataFrame and export to Excel
                df = pd.DataFrame(data)
                df.to_excel(output_path, index=False)
                return True
            
            elif output_format == 'json':
                # Export to JSON
                with open(output_path, 'w', encoding='utf-8') as f:
                    json.dump(data, f, ensure_ascii=False, indent=4)
                return True
            
            else:
                logger.error(f"Unsupported export format: {output_format}")
                return False
                
        except Exception as e:
            logger.error(f"Error exporting data: {e}")
            return False
    
    # Generate statistics and reports
    def generate_statistics(self) -> Dict[str, Any]:
        """
        Generate statistics and reports from the database
        
        Returns:
            Dict: Statistics and report data
        """
        hotels = self.get_all_hotels()
        
        if not hotels:
            return {"error": "No data available"}
        
        # Basic statistics
        stats = {
            "total_hotels": len(hotels),
            "data_sources": {},
            "countries": {},
            "cities": {},
            "average_stars": 0,
            "hotels_with_coordinates": 0,
            "hotels_with_website": 0,
            "hotels_with_phone": 0,
            "hotels_with_email": 0
        }
        
        # Process each hotel
        total_stars = 0
        stars_count = 0
        
        for hotel in hotels:
            # Count data sources
            source = hotel.get("data_source", "Unknown")
            stats["data_sources"][source] = stats["data_sources"].get(source, 0) + 1
            
            # Count countries
            country = hotel.get("country", "Unknown")
            stats["countries"][country] = stats["countries"].get(country, 0) + 1
            
            # Count cities
            city = hotel.get("city", "Unknown")
            stats["cities"][city] = stats["cities"].get(city, 0) + 1
            
            # Count stars
            if hotel.get("stars"):
                try:
                    stars = float(hotel.get("stars"))
                    total_stars += stars
                    stars_count += 1
                except (ValueError, TypeError):
                    pass
            
            # Count hotels with coordinates
            if hotel.get("latitude") and hotel.get("longitude"):
                stats["hotels_with_coordinates"] += 1
            
            # Count hotels with website
            if hotel.get("website"):
                stats["hotels_with_website"] += 1
            
            # Count hotels with phone
            if hotel.get("phone"):
                stats["hotels_with_phone"] += 1
            
            # Count hotels with email
            if hotel.get("email"):
                stats["hotels_with_email"] += 1
        
        # Calculate average stars
        stats["average_stars"] = round(total_stars / stars_count, 2) if stars_count > 0 else 0
        
        # Calculate percentage of hotels with coordinates
        stats["percentage_with_coordinates"] = round(stats["hotels_with_coordinates"] / stats["total_hotels"] * 100, 2) if stats["total_hotels"] > 0 else 0
        
        # Calculate percentage of hotels with website
        stats["percentage_with_website"] = round(stats["hotels_with_website"] / stats["total_hotels"] * 100, 2) if stats["total_hotels"] > 0 else 0
        
        # Calculate percentage of hotels with phone
        stats["percentage_with_phone"] = round(stats["hotels_with_phone"] / stats["total_hotels"] * 100, 2) if stats["total_hotels"] > 0 else 0
        
        # Calculate percentage of hotels with email
        stats["percentage_with_email"] = round(stats["hotels_with_email"] / stats["total_hotels"] * 100, 2) if stats["total_hotels"] > 0 else 0
        
        return stats
    
    # Close database connection
    def close(self):
        """Close database connection"""
        if self.conn:
            self.conn.close()

# ================================================
# 5. HOTEL SCRAPER CLASS
# ================================================
class HotelScraper:
    """Class for extracting hotel information from different sources"""
    
    def __init__(self, api_keys: Dict[str, str] = None):
        """
        Initialize hotel information extractor
        
        Args:
            api_keys (Dict): API keys for different services
        """
        self.api_keys = api_keys or {}
    
    # Search for hotels in a specific area
    def search_hotels_by_location(self, location: str, radius: int = 5000, language: str = "en") -> List[Dict[str, Any]]:
        """
        Search for hotels in a specific area
        
        Args:
            location (str): Location name or coordinates
            radius (int): Search radius in meters
            language (str): Search language (en, ar)
            
        Returns:
            List[Dict]: List of hotels in the specified area
        """
        results = []
        
        # Use Google Places API if key is available
        if 'google_places' in self.api_keys:
            google_results = self._search_google_places(location, radius, language)
            results.extend(google_results)
        
        # Use OpenStreetMap/Nominatim (doesn't need API key)
        osm_results = self._search_openstreetmap(location, radius, language)
        results.extend(osm_results)
        
        # Compare and enrich results
        self._compare_and_enrich_results(results)
        
        # Add more data sources here in the future
        
        return results
    
    # Search for hotels using Google Places API
    def _search_google_places(self, location: str, radius: int, language: str = "en") -> List[Dict[str, Any]]:
        """
        Search for hotels using Google Places API
        
        Args:
            location (str): Location name or coordinates
            radius (int): Search radius in meters
            language (str): Search language (en, ar)
            
        Returns:
            List[Dict]: List of hotels from Google Places
        """
        logger.info(f"Searching Google Places for: {location}")
        
        try:
            # Convert location to coordinates if not already
            coords = self._get_coordinates(location)
            if not coords:
                logger.error(f"Could not convert location to coordinates: {location}")
                return []
            
            lat, lng = coords
            
            # Create Google Places API request
            base_url = "https://maps.googleapis.com/maps/api/place/nearbysearch/json"
            
            # Search using several timeframes to find all accommodations
            all_results = []
            
            # Create list of future dates to search (today, next week, next month, 3 months from now)
            search_dates = [
                datetime.now(),
                datetime.now() + timedelta(days=7),
                datetime.now() + timedelta(days=30),
                datetime.now() + timedelta(days=90)
            ]
            
            # Set language for search
            lang_param = language if language in ['en', 'ar'] else 'en'
            
            # Keywords to use in different languages
            keywords = []
            if language == 'ar' or language == 'both':
                keywords.extend(['فندق', 'نزل', 'شقق فندقية', 'منتجع'])
            if language == 'en' or language == 'both':
                keywords.extend(['hotel', 'resort', 'lodging', 'accommodation', 'motel', 'hostel', 'inn'])
            
            # If no keywords are specified, use default English ones
            if not keywords:
                keywords = ['hotel', 'resort', 'lodging', 'accommodation']
            
            # Search with each keyword
            for keyword in keywords:
                for search_date in search_dates:
                    # Format date for API
                    date_str = search_date.strftime("%Y-%m-%d")
                    
                    params = {
                        "location": f"{lat},{lng}",
                        "radius": radius,
                        "type": "lodging",  # Place type (hotel)
                        "keyword": keyword,  # Add keyword search
                        "key": self.api_keys['google_places'],
                        "language": lang_param,
                        # Add date parameters to find more results
                        "openNow": "true"  # Adding this parameter sometimes helps find more active places
                    }
                    
                    # Send request
                    response = requests.get(base_url, params=params)
                    data = response.json()
                    
                    # Process results
                    if data.get('status') == 'OK':
                        for place in data.get('results', []):
                            # Check if we've already found this place
                            place_id = place.get('place_id')
                            if not any(r.get('place_id') == place_id for r in all_results):
                                all_results.append(place)
            
            # Convert all results to standard format
            results = []
            for place in all_results:
                hotel = {
                    'name': place.get('name', ''),
                    'address': place.get('vicinity', ''),
                    'latitude': place.get('geometry', {}).get('location', {}).get('lat'),
                    'longitude': place.get('geometry', {}).get('location', {}).get('lng'),
                    'data_source': 'Google Places',
                    'stars': place.get('rating'),
                    'place_id': place.get('place_id'),
                    # Store place details as additional info
                    'additional_info': json.dumps({"place_id": place.get('place_id')})
                }
                results.append(hotel)
            
            return results
            
        except Exception as e:
            logger.error(f"Error searching Google Places: {e}")
            return []
    
    # Search for hotels using OpenStreetMap/Nominatim
    def _search_openstreetmap(self, location: str, radius: int, language: str = "en") -> List[Dict[str, Any]]:
        """
        Search for hotels using OpenStreetMap/Nominatim
        
        Args:
            location (str): Location name or coordinates
            radius (int): Search radius in meters
            language (str): Search language (en, ar)
            
        Returns:
            List[Dict]: List of hotels from OpenStreetMap
        """
        logger.info(f"Searching OpenStreetMap for: {location}")
        
        try:
            # Convert location to coordinates if not already
            coords = self._get_coordinates(location)
            if not coords:
                logger.error(f"Could not convert location to coordinates: {location}")
                return []
            
            lat, lng = coords
            
            # Set language tag based on input
            name_tags = ['name']
            if language == 'ar':
                name_tags.extend(['name:ar'])
            elif language == 'en':
                name_tags.extend(['name:en'])
            else:  # both or any other value
                name_tags.extend(['name:en', 'name:ar'])
            
            # Create Overpass query
            overpass_url = "https://overpass-api.de/api/interpreter"
            
            # Convert radius from meters to degrees (approximately)
            # 1 degree ≈ 111000 meters (varies by latitude)
            radius_deg = radius / 111000
            
            # Create Overpass query
            overpass_query = f"""
            [out:json];
            (
              node["tourism"="hotel"](around:{radius},{lat},{lng});
              way["tourism"="hotel"](around:{radius},{lat},{lng});
              relation["tourism"="hotel"](around:{radius},{lat},{lng});
              node["tourism"="motel"](around:{radius},{lat},{lng});
              way["tourism"="motel"](around:{radius},{lat},{lng});
              relation["tourism"="motel"](around:{radius},{lat},{lng});
              node["tourism"="hostel"](around:{radius},{lat},{lng});
              way["tourism"="hostel"](around:{radius},{lat},{lng});
              relation["tourism"="hostel"](around:{radius},{lat},{lng});
              node["tourism"="guest_house"](around:{radius},{lat},{lng});
              way["tourism"="guest_house"](around:{radius},{lat},{lng});
              relation["tourism"="guest_house"](around:{radius},{lat},{lng});
              node["building"="hotel"](around:{radius},{lat},{lng});
              way["building"="hotel"](around:{radius},{lat},{lng});
              relation["building"="hotel"](around:{radius},{lat},{lng});
              node["amenity"="hotel"](around:{radius},{lat},{lng});
              way["amenity"="hotel"](around:{radius},{lat},{lng});
              relation["amenity"="hotel"](around:{radius},{lat},{lng});
              node["tourism"="apartment"](around:{radius},{lat},{lng});
              way["tourism"="apartment"](around:{radius},{lat},{lng});
              relation["tourism"="apartment"](around:{radius},{lat},{lng});
              node["tourism"="resort"](around:{radius},{lat},{lng});
              way["tourism"="resort"](around:{radius},{lat},{lng});
              relation["tourism"="resort"](around:{radius},{lat},{lng});
            );
            out body;
            >;
            out skel qt;
            """
            
            # Send request
            response = requests.post(overpass_url, data={"data": overpass_query})
            
            # Check for successful request
            if response.status_code != 200:
                logger.error(f"Overpass API request failed with code: {response.status_code}")
                return []
            
            # Process results
            data = response.json()
            results = []
            
            for element in data.get('elements', []):
                # Extract information from the elements
                tags = element.get('tags', {})
                
                if element['type'] == 'node':
                    lat_val = element.get('lat')
                    lon_val = element.get('lon')
                else:
                    # For ways and relations, we need to calculate the center (this is approximate)
                    coords_list = []
                    if 'center' in element:
                        lat_val = element['center']['lat']
                        lon_val = element['center']['lon']
                    else:
                        # This is not accurate but will be used as a fallback
                        lat_val = lat
                        lon_val = lng
                
                # Get hotel name based on language preference
                hotel_name = ''
                if language == 'ar' and tags.get('name:ar'):
                    hotel_name = tags.get('name:ar')
                elif language == 'en' and tags.get('name:en'):
                    hotel_name = tags.get('name:en')
                else:
                    # Default to any available name
                    hotel_name = tags.get('name', tags.get('name:en', tags.get('name:ar', '')))
                
                # Create hotel dictionary
                hotel = {
                    'name': hotel_name,
                    'address': tags.get('addr:full', f"{tags.get('addr:street', '')} {tags.get('addr:housenumber', '')}").strip(),
                    'city': tags.get('addr:city', ''),
                    'country': tags.get('addr:country', ''),
                    'latitude': lat_val,
                    'longitude': lon_val,
                    'phone': tags.get('phone', tags.get('contact:phone', '')),
                    'email': tags.get('email', tags.get('contact:email', '')),
                    'website': tags.get('website', tags.get('contact:website', '')),
                    'stars': tags.get('stars', None),
                    'price_range': tags.get('price_range', ''),
                    'facilities': tags.get('facilities', tags.get('amenity', '')),
                    'data_source': 'OpenStreetMap',
                    'osm_id': element.get('id'),
                    'osm_type': element.get('type'),
                    'additional_info': json.dumps({
                        "osm_id": element.get('id'),
                        "osm_type": element.get('type'),
                        "amenities": tags.get('amenity', ''),
                        "rooms": tags.get('rooms', ''),
                        "accommodation": tags.get('accommodation', ''),
                        "tourism": tags.get('tourism', ''),
                        "internet_access": tags.get('internet_access', ''),
                        "swimming_pool": tags.get('swimming_pool', ''),
                        "wheelchair": tags.get('wheelchair', '')
                    })
                }
                
                # Add hotel to results if it has a name
                if hotel['name']:
                    results.append(hotel)
            
            return results
            
        except Exception as e:
            logger.error(f"Error searching OpenStreetMap: {e}")
            return []
    
    # Convert location name to coordinates
    def _get_coordinates(self, location: str) -> Optional[Tuple[float, float]]:
        """
        Convert location name to coordinates
        
        Args:
            location (str): Location name or coordinates
            
        Returns:
            Tuple[float, float]: Location coordinates (latitude, longitude)
        """
        # Check if location is already coordinates
        if ',' in location:
            try:
                lat, lng = map(float, location.split(','))
                return lat, lng
            except ValueError:
                pass
        
        # Use Nominatim API to convert name to coordinates
        try:
            nominatim_url = "https://nominatim.openstreetmap.org/search"
            params = {
                "q": location,
                "format": "json",
                "limit": 1
            }
            
            # Add contact information (required by Nominatim)
            headers = {
                "User-Agent": "Map_researcher0.3V"
            }
            
            response = requests.get(nominatim_url, params=params, headers=headers)
            data = response.json()
            
            if data and len(data) > 0:
                return float(data[0]['lat']), float(data[0]['lon'])
            
            return None
        
        except Exception as e:
            logger.error(f"Error converting location to coordinates: {e}")
            return None
    
    # Calculate distance between two geographical points
    def _calculate_distance(self, lat1: float, lon1: float, lat2: float, lon2: float) -> float:
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
    
    # Compare and enrich results from different sources
    def _compare_and_enrich_results(self, results: List[Dict[str, Any]]) -> None:
        """
        Compare and enrich results from different sources
        
        Args:
            results (List[Dict]): List of hotel results to compare
        """
        # Group results by proximity (hotels close to each other)
        groups = {}
        processed = set()
        
        for i, hotel_a in enumerate(results):
            if i in processed:
                continue
                
            lat_a = hotel_a.get('latitude')
            lng_a = hotel_a.get('longitude')
            
            if not lat_a or not lng_a:
                continue
                
            group = [hotel_a]
            processed.add(i)
            
            for j, hotel_b in enumerate(results):
                if j in processed:
                    continue
                    
                lat_b = hotel_b.get('latitude')
                lng_b = hotel_b.get('longitude')
                
                if not lat_b or not lng_b:
                    continue
                
                # Calculate distance between points
                distance = self._calculate_distance(lat_a, lng_a, lat_b, lng_b)
                
                # If within 50 meters, consider it may be the same hotel
                if distance < 0.05:  # 50 meters in km
                    group.append(hotel_b)
                    processed.add(j)
                    
                    # Store matching information
                    hotel_b['google_maps_match'] = hotel_a.get('name', '')
                    hotel_b['coordinate_distance'] = distance * 1000  # Convert to meters
            
            # Store group
            group_id = f"group_{len(groups)}"
            groups[group_id] = group
            
        # Enrich results with matching information
        for group_id, hotels in groups.items():
            if len(hotels) > 1:
                # Look for Google Places hotel among matches if any
                google_hotel = next((h for h in hotels if h.get('data_source') == 'Google Places'), None)
                
                if google_hotel:
                    for hotel in hotels:
                        if hotel.get('data_source') != 'Google Places':
                            hotel['google_maps_match'] = google_hotel.get('name', '')
                            
                            # Calculate distance
                            lat_a = hotel.get('latitude')
                            lng_a = hotel.get('longitude')
                            lat_b = google_hotel.get('latitude')
                            lng_b = google_hotel.get('longitude')
                            
                            if lat_a and lng_a and lat_b and lng_b:
                                distance = self._calculate_distance(lat_a, lng_a, lat_b, lng_b)
                                hotel['coordinate_distance'] = distance * 1000  # Convert to meters
    
    # Fetch detailed information about a hotel from its ID
    def get_hotel_details(self, hotel_id: str, source: str) -> Dict[str, Any]:
        """
        Fetch detailed information about a hotel from its ID
        
        Args:
            hotel_id (str): Hotel ID (Google Place ID or OSM ID)
            source (str): Source of the hotel information ('google' or 'osm')
            
        Returns:
            Dict: Detailed hotel information
        """
        if source.lower() == 'google' and 'google_places' in self.api_keys:
            return self._get_google_place_details(hotel_id)
        elif source.lower() == 'osm':
            return self._get_osm_details(hotel_id)
        else:
            logger.error(f"Unsupported source {source} or missing API key")
            return {}
    
    # Get Google Place details
    def _get_google_place_details(self, place_id: str) -> Dict[str, Any]:
        """
        Get Google Place details using place ID
        
        Args:
            place_id (str): Google Place ID
            
        Returns:
            Dict: Detailed place information
        """
        try:
            # Create Google Places API request
            base_url = "https://maps.googleapis.com/maps/api/place/details/json"
            
            params = {
                "place_id": place_id,
                "fields": "name,formatted_address,formatted_phone_number,international_phone_number,website,rating,price_level,review,opening_hours,photo,geometry",
                "key": self.api_keys['google_places']
            }
            
            # Send request
            response = requests.get(base_url, params=params)
            data = response.json()
            
            # Process result
            if data.get('status') == 'OK':
                result = data.get('result', {})
                
                # Format details
                details = {
                    'name': result.get('name', ''),
                    'address': result.get('formatted_address', ''),
                    'phone': result.get('formatted_phone_number', result.get('international_phone_number', '')),
                    'website': result.get('website', ''),
                    'stars': result.get('rating'),
                    'price_range': result.get('price_level'),
                    'latitude': result.get('geometry', {}).get('location', {}).get('lat'),
                    'longitude': result.get('geometry', {}).get('location', {}).get('lng'),
                    'opening_hours': result.get('opening_hours', {}).get('weekday_text', []),
                    'reviews': result.get('reviews', []),
                    'photos': result.get('photos', []),
                    'data_source': 'Google Places (detailed)',
                    'last_updated': datetime.now().isoformat()
                }
                
                return details
            
            return {}
            
        except Exception as e:
            logger.error(f"Error fetching Google Place details: {e}")
            return {}
    
    # Get OpenStreetMap details
    def _get_osm_details(self, osm_id: str) -> Dict[str, Any]:
        """
        Get OpenStreetMap details using OSM ID
        
        Args:
            osm_id (str): OpenStreetMap ID
            
        Returns:
            Dict: Detailed OSM information
        """
        try:
            # Parse OSM ID and type
            if ':' in osm_id:
                osm_type, osm_id_num = osm_id.split(':')
            else:
                # Default to node if not specified
                osm_type = 'node'
                osm_id_num = osm_id
            
            # Create Overpass API request
            overpass_url = "https://overpass-api.de/api/interpreter"
            
            # Create Overpass query
            overpass_query = f"""
            [out:json];
            {osm_type}({osm_id_num});
            out body;
            """
            
            # Send request
            response = requests.post(overpass_url, data={"data": overpass_query})
            
            # Check if request was successful
            if response.status_code != 200:
                logger.error(f"Overpass API request failed with code: {response.status_code}")
                return {}
            
            # Process result
            data = response.json()
            
            if data.get('elements') and len(data['elements']) > 0:
                element = data['elements'][0]
                tags = element.get('tags', {})
                
                # Format details
                details = {
                    'name': tags.get('name', ''),
                    'address': tags.get('addr:full', f"{tags.get('addr:street', '')} {tags.get('addr:housenumber', '')}").strip(),
                    'city': tags.get('addr:city', ''),
                    'country': tags.get('addr:country', ''),
                    'phone': tags.get('phone', tags.get('contact:phone', '')),
                    'email': tags.get('email', tags.get('contact:email', '')),
                    'website': tags.get('website', tags.get('contact:website', '')),
                    'stars': tags.get('stars'),
                    'facilities': ', '.join([k for k, v in tags.items() if k.startswith('amenity') or k.startswith('facility')]),
                    'wheelchair': tags.get('wheelchair'),
                    'internet_access': tags.get('internet_access'),
                    'rooms': tags.get('rooms'),
                    'beds': tags.get('beds'),
                    'operator': tags.get('operator'),
                    'brand': tags.get('brand'),
                    'cuisine': tags.get('cuisine'),
                    'outdoor_seating': tags.get('outdoor_seating'),
                    'swimming_pool': tags.get('swimming_pool'),
                    'data_source': 'OpenStreetMap (detailed)',
                    'last_updated': datetime.now().isoformat()
                }
                
                return details
            
            return {}
            
        except Exception as e:
            logger.error(f"Error fetching OSM details: {e}")
            return {}
    
    # Search for hotels by name or keywords
    def search_hotels_by_name(self, name: str, location: str = None, radius: int = 5000, language: str = "en") -> List[Dict[str, Any]]:
        """
        Search for hotels by name or keywords
        
        Args:
            name (str): Hotel name or keywords
            location (str): Location name or coordinates (optional)
            radius (int): Search radius in meters (used if location is provided)
            language (str): Search language (en, ar)
            
        Returns:
            List[Dict]: List of hotels matching search criteria
        """
        results = []
        
        # Check if location is provided
        if location:
            # First search by location, then filter by name
            location_results = self.search_hotels_by_location(location, radius, language)
            
            # Filter results by name
            for hotel in location_results:
                hotel_name = hotel.get('name', '').lower()
                if name.lower() in hotel_name:
                    results.append(hotel)
        else:
            # Search directly by name using Google Places API if available
            if 'google_places' in self.api_keys:
                google_results = self._search_google_places_by_name(name, language)
                results.extend(google_results)
            
            # Search by name using OpenStreetMap Nominatim
            osm_results = self._search_openstreetmap_by_name(name, language)
            results.extend(osm_results)
        
        return results
    
    # Search for hotels by name using Google Places API
    def _search_google_places_by_name(self, name: str, language: str = "en") -> List[Dict[str, Any]]:
        """
        Search for hotels by name using Google Places API
        
        Args:
            name (str): Hotel name or keywords
            language (str): Search language (en, ar)
            
        Returns:
            List[Dict]: List of hotels matching name
        """
        logger.info(f"Searching Google Places for hotel name: {name}")
        
        try:
            # Create Google Places API request
            base_url = "https://maps.googleapis.com/maps/api/place/textsearch/json"
            
            # Set language parameter
            lang_param = language if language in ['en', 'ar'] else 'en'
            
            # Create request parameters
            params = {
                "query": f"{name} hotel",
                "type": "lodging",
                "key": self.api_keys['google_places'],
                "language": lang_param
            }
            
            # Send request
            response = requests.get(base_url, params=params)
            data = response.json()
            
            # Process results
            results = []
            if data.get('status') == 'OK':
                for place in data.get('results', []):
                    hotel = {
                        'name': place.get('name', ''),
                        'address': place.get('formatted_address', ''),
                        'latitude': place.get('geometry', {}).get('location', {}).get('lat'),
                        'longitude': place.get('geometry', {}).get('location', {}).get('lng'),
                        'data_source': 'Google Places',
                        'stars': place.get('rating'),
                        'place_id': place.get('place_id'),
                        'additional_info': json.dumps({"place_id": place.get('place_id')})
                    }
                    results.append(hotel)
            
            return results
            
        except Exception as e:
            logger.error(f"Error searching Google Places by name: {e}")
            return []
    
    # Search for hotels by name using OpenStreetMap Nominatim
    def _search_openstreetmap_by_name(self, name: str, language: str = "en") -> List[Dict[str, Any]]:
        """
        Search for hotels by name using OpenStreetMap Nominatim
        
        Args:
            name (str): Hotel name or keywords
            language (str): Search language (en, ar)
            
        Returns:
            List[Dict]: List of hotels matching name
        """
        logger.info(f"Searching OpenStreetMap for hotel name: {name}")
        
        try:
            # Create Nominatim API request
            nominatim_url = "https://nominatim.openstreetmap.org/search"
            
            # Set language parameter
            lang_param = language if language in ['en', 'ar'] else 'en'
            
            # Create request parameters
            params = {
                "q": f"{name} hotel",
                "format": "json",
                "addressdetails": 1,
                "extratags": 1,
                "namedetails": 1,
                "limit": 20,
                "accept-language": lang_param
            }
            
            # Add user agent information (required by Nominatim)
            headers = {
                "User-Agent": "Map_researcher0.3V"
            }
            
            # Send request
            response = requests.get(nominatim_url, params=params, headers=headers)
            data = response.json()
            
            # Process results
            results = []
            for place in data:
                # Filter only hotels, accommodations, etc.
                if (place.get('type') in ['hotel', 'hostel', 'motel', 'guest_house'] or 
                    place.get('class') == 'tourism' or 
                    'extratags' in place and place['extratags'].get('tourism') in ['hotel', 'hostel', 'motel', 'guest_house']):
                    
                    address = place.get('address', {})
                    
                    # Create hotel dictionary
                    hotel = {
                        'name': place.get('namedetails', {}).get(f'name:{lang_param}', place.get('display_name')),
                        'address': f"{address.get('road', '')} {address.get('house_number', '')}",
                        'city': address.get('city', address.get('town', address.get('village', ''))),
                        'country': address.get('country', ''),
                        'latitude': float(place.get('lat')),
                        'longitude': float(place.get('lon')),
                        'data_source': 'OpenStreetMap',
                        'osm_id': place.get('osm_id'),
                        'osm_type': place.get('osm_type'),
                        'additional_info': json.dumps({
                            "osm_id": place.get('osm_id'),
                            "osm_type": place.get('osm_type')
                        })
                    }
                    results.append(hotel)
            
            return results
            
        except Exception as e:
            logger.error(f"Error searching OpenStreetMap by name: {e}")
            return []
    
    # Generate visualization map of hotels
    def generate_map(self, hotels: List[Dict[str, Any]], output_file: str = "hotel_map.html") -> str:
        """
        Generate visualization map of hotels
        
        Args:
            hotels (List[Dict]): List of hotels to visualize
            output_file (str): Output HTML file path
            
        Returns:
            str: Path to generated map file
        """
        try:
            # Check if folium is available
            if not isinstance(folium, type) and not hasattr(folium, 'Map'):
                logger.error("Folium module not available. Map cannot be generated.")
                return ""
            
            # Filter hotels with coordinates
            hotels_with_coords = [h for h in hotels if h.get('latitude') and h.get('longitude')]
            
            if not hotels_with_coords:
                logger.warning("No hotels with coordinates to display on map")
                return ""
            
            # Calculate map center (average of coordinates)
            lat_sum = sum(float(h.get('latitude', 0)) for h in hotels_with_coords)
            lng_sum = sum(float(h.get('longitude', 0)) for h in hotels_with_coords)
            center_lat = lat_sum / len(hotels_with_coords)
            center_lng = lng_sum / len(hotels_with_coords)
            
            # Create map
            hotel_map = folium.Map(location=[center_lat, center_lng], zoom_start=13)
            
            # Add markers for each hotel
            for hotel in hotels_with_coords:
                # Prepare tooltip content
                tooltip = f"{hotel.get('name', 'Unknown Hotel')}<br>{hotel.get('address', '')}"
                if hotel.get('stars'):
                    tooltip += f"<br>Rating: {hotel.get('stars')} ★"
                
                # Set icon color based on data source
                icon_color = 'blue'
                if hotel.get('data_source') == 'Google Places':
                    icon_color = 'red'
                elif hotel.get('data_source') == 'OpenStreetMap':
                    icon_color = 'green'
                
                # Create marker
                folium.Marker(
                    location=[float(hotel.get('latitude')), float(hotel.get('longitude'))],
                    popup=tooltip,
                    tooltip=hotel.get('name', 'Unknown Hotel'),
                    icon=folium.Icon(color=icon_color, icon='hotel', prefix='fa')
                ).add_to(hotel_map)
            
            # Save map to file
            hotel_map.save(output_file)
            
            logger.info(f"Map generated and saved to {output_file}")
            return output_file
            
        except Exception as e:
            logger.error(f"Error generating map: {e}")
            return ""

# ================================================
# 6. COMMAND LINE INTERFACE
# ================================================
# أضف هذا قبل دالة main مباشرة
class CommandLineInterface:
    """Class for managing the command-line interface"""
    
    def __init__(self, db, scraper):
        self.db = db
        self.scraper = scraper
        self.console = Console()
        
    def main_menu(self):
        self.console.print("\n[bold blue]MAP RESEARCHER 0.3V - HOTEL DATA COLLECTION TOOL[/bold blue]")
        self.console.print("=" * 60)
        self.console.print("[yellow]Menu functionality not implemented yet.[/yellow]")
        input("Press Enter to exit...")
        
    # Main menu for interactive mode
    def main_menu(self):
        """Display main menu and handle user commands"""
        menu_options = {
            '1': 'Search hotels by location',
            '2': 'Search hotels by name',
            '3': 'View all hotels in database',
            '4': 'Export hotel data',
            '5': 'View statistics',
            '6': 'Generate map visualization',
            '7': 'Advanced search options',
            '8': 'Update/delete hotel records',
            '9': 'Get detailed info for a hotel',
            '0': 'Exit'
        }
        
        while True:
            self.console.print("\n[bold blue]MAP RESEARCHER 0.3V - HOTEL DATA COLLECTION TOOL[/bold blue]")
            self.console.print("=" * 60)
            
            for key, value in menu_options.items():
                self.console.print(f"[green]{key}[/green]: {value}")
            
            choice = input("\nEnter option number: ")
            
            if choice == '0':
                self.console.print("[yellow]Exiting program. Goodbye![/yellow]")
                break
                
            elif choice == '1':
                self.search_by_location_menu()
                
            elif choice == '2':
                self.search_by_name_menu()
                
            elif choice == '3':
                self.view_all_hotels()
                
            elif choice == '4':
                self.export_data_menu()
                
            elif choice == '5':
                self.view_statistics()
                
            elif choice == '6':
                self.generate_map_menu()
                
            elif choice == '7':
                self.advanced_search_menu()
                
            elif choice == '8':
                self.update_delete_menu()
                
            elif choice == '9':
                self.detailed_info_menu()
                
            else:
                self.console.print("[red]Invalid option. Please try again.[/red]")
    
    # Menu for searching hotels by location
    def search_by_location_menu(self):
        """Search hotels by location menu"""
        self.console.print("\n[bold blue]SEARCH HOTELS BY LOCATION[/bold blue]")
        self.console.print("=" * 60)
        
        location = input("Enter location (city, address, or coordinates): ")
        if not location:
            self.console.print("[red]Location cannot be empty.[/red]")
            return
            
        radius = input("Enter search radius in meters (default 5000): ")
        radius = int(radius) if radius.isdigit() else 5000
        
        language = input("Enter search language (en, ar, both - default: en): ").lower()
        if language not in ['en', 'ar', 'both']:
            language = 'en'
        
        self.console.print(f"\nSearching for hotels in [green]{location}[/green] with radius [green]{radius}m[/green]...")
        
        # Search for hotels
        hotels = self.scraper.search_hotels_by_location(location, radius, language)
        
        if hotels:
            # Save hotels to database
            saved_count = 0
            for hotel in track(hotels, description="Saving hotels to database..."):
                if hotel.get('name'):  # Only save hotels with a name
                    # Remove non-database fields
                    hotel_data = hotel.copy()
                    for field in ['place_id', 'osm_id', 'osm_type']:
                        if field in hotel_data:
                            del hotel_data[field]
                    
                    # Save to database
                    self.db.save_hotel(hotel_data)
                    saved_count += 1
            
            # Display results
            self.console.print(f"\n[green]Found {len(hotels)} hotels, saved {saved_count} to database.[/green]")
            self.display_hotels(hotels[:20])  # Display first 20 results
            
            # Save search query
            search_data = {
                'query': f"Location: {location}",
                'location': location,
                'results_count': len(hotels),
                'search_params': json.dumps({
                    'radius': radius,
                    'language': language
                })
            }
            self.db.save_search(search_data)
            
            # Ask to generate map
            if input("\nGenerate map visualization? (y/n): ").lower() == 'y':
                self.generate_map(hotels)
        else:
            self.console.print("[red]No hotels found for this location.[/red]")
    
    # Menu for searching hotels by name
    def search_by_name_menu(self):
        """Search hotels by name menu"""
        self.console.print("\n[bold blue]SEARCH HOTELS BY NAME[/bold blue]")
        self.console.print("=" * 60)
        
        name = input("Enter hotel name or keywords: ")
        if not name:
            self.console.print("[red]Name cannot be empty.[/red]")
            return
            
        location = input("Enter location (optional, leave empty to search globally): ")
        
        radius = input("Enter search radius in meters (default 5000, used only if location is provided): ")
        radius = int(radius) if radius.isdigit() else 5000
        
        language = input("Enter search language (en, ar, both - default: en): ").lower()
        if language not in ['en', 'ar', 'both']:
            language = 'en'
        
        self.console.print(f"\nSearching for hotels with name [green]{name}[/green]...")
        
        # Search for hotels
        hotels = self.scraper.search_hotels_by_name(name, location, radius, language)
        
        if hotels:
            # Save hotels to database
            saved_count = 0
            for hotel in track(hotels, description="Saving hotels to database..."):
                if hotel.get('name'):  # Only save hotels with a name
                    # Remove non-database fields
                    hotel_data = hotel.copy()
                    for field in ['place_id', 'osm_id', 'osm_type']:
                        if field in hotel_data:
                            del hotel_data[field]
                    
                    # Save to database
                    self.db.save_hotel(hotel_data)
                    saved_count += 1
            
            # Display results
            self.console.print(f"\n[green]Found {len(hotels)} hotels, saved {saved_count} to database.[/green]")
            self.display_hotels(hotels[:20])  # Display first 20 results
            
            # Save search query
            search_data = {
                'query': f"Name: {name}",
                'location': location if location else "Global search",
                'results_count': len(hotels),
                'search_params': json.dumps({
                    'name': name,
                    'radius': radius if location else None,
                    'language': language
                })
            }
            self.db.save_search(search_data)
            
            # Ask to generate map
            if input("\nGenerate map visualization? (y/n): ").lower() == 'y':
                self.generate_map(hotels)
        else:
            self.console.print("[red]No hotels found with this name.[/red]")
    
    # View all hotels in database
    def view_all_hotels(self):
        """Display all hotels in database"""
        self.console.print("\n[bold blue]ALL HOTELS IN DATABASE[/bold blue]")
        self.console.print("=" * 60)
        
        hotels = self.db.get_all_hotels()
        
        if hotels:
            self.console.print(f"\n[green]Found {len(hotels)} hotels in database.[/green]")
            
            # Ask for pagination
            page_size = 20
            current_page = 0
            total_pages = (len(hotels) - 1) // page_size + 1
            
            while True:
                start_idx = current_page * page_size
                end_idx = min(start_idx + page_size, len(hotels))
                
                self.display_hotels(hotels[start_idx:end_idx])
                
                self.console.print(f"\nPage {current_page + 1} of {total_pages}")
                
                nav = input("Navigation (n: next, p: prev, j: jump to page, q: quit): ").lower()
                
                if nav == 'q':
                    break
                elif nav == 'n' and current_page < total_pages - 1:
                    current_page += 1
                elif nav == 'p' and current_page > 0:
                    current_page -= 1
                elif nav == 'j':
                    page = input(f"Enter page number (1-{total_pages}): ")
                    try:
                        page = int(page)
                        if 1 <= page <= total_pages:
                            current_page = page - 1
                        else:
                            self.console.print("[red]Invalid page number.[/red]")
                    except ValueError:
                        self.console.print("[red]Invalid input.[/red]")
        else:
            self.console.print("[red]No hotels found in database.[/red]")
    
    # Menu for exporting data
    def export_data_menu(self):
        """Export data menu"""
        self.console.print("\n[bold blue]EXPORT HOTEL DATA[/bold blue]")
        self.console.print("=" * 60)
        
        formats = {
            '1': 'CSV',
            '2': 'Excel',
            '3': 'JSON'
        }
        
        for key, value in formats.items():
            self.console.print(f"[green]{key}[/green]: {value}")
        
        format_choice = input("\nSelect export format: ")
        
        if format_choice not in formats:
            self.console.print("[red]Invalid format choice.[/red]")
            return
        
        # Convert choice to actual format string
        output_format = formats[format_choice].lower()
        
        output_path = input(f"Enter output file path (default: exports/hotels.{output_format}): ")
        if not output_path:
            os.makedirs("exports", exist_ok=True)
            output_path = f"exports/hotels.{output_format}"
        
        # Ask for filter criteria
        use_filter = input("Do you want to filter the data before export? (y/n): ").lower() == 'y'
        query = {}
        
        if use_filter:
            self.console.print("\n[yellow]Enter filter criteria (leave empty to skip):[/yellow]")
            name = input("Hotel name contains: ")
            if name:
                query['name'] = f"%{name}%"
            
            city = input("City: ")
            if city:
                query['city'] = city
            
            country = input("Country: ")
            if country:
                query['country'] = country
            
            source = input("Data source: ")
            if source:
                query['data_source'] = source
        
        # Export data
        success = self.db.export_data(output_format, output_path, query)
        
        if success:
            self.console.print(f"\n[green]Data exported successfully to {output_path}[/green]")
        else:
            self.console.print("[red]Error exporting data.[/red]")
    
    # View statistics
    def view_statistics(self):
        """Display statistics and reports"""
        self.console.print("\n[bold blue]DATABASE STATISTICS[/bold blue]")
        self.console.print("=" * 60)
        
        stats = self.db.generate_statistics()
        
        if 'error' in stats:
            self.console.print(f"[red]{stats['error']}[/red]")
            return
        
        # Create statistics table
        table = Table(title="Hotel Database Statistics")
        table.add_column("Metric", style="cyan")
        table.add_column("Value", style="green")
        
        table.add_row("Total Hotels", str(stats['total_hotels']))
        table.add_row("Average Rating", f"{stats['average_stars']} ★")
        table.add_row("Hotels with Coordinates", f"{stats['hotels_with_coordinates']} ({stats['percentage_with_coordinates']}%)")
        table.add_row("Hotels with Website", f"{stats['hotels_with_website']} ({stats['percentage_with_website']}%)")
        table.add_row("Hotels with Phone", f"{stats['hotels_with_phone']} ({stats['percentage_with_phone']}%)")
        table.add_row("Hotels with Email", f"{stats['hotels_with_email']} ({stats['percentage_with_email']}%)")
        
        self.console.print(table)
        
        # Display data sources breakdown
        if stats['data_sources']:
            self.console.print("\n[bold]Data Sources Breakdown:[/bold]")
            sources_table = Table()
            sources_table.add_column("Source", style="cyan")
            sources_table.add_column("Count", style="green")
            sources_table.add_column("Percentage", style="yellow")
            
            for source, count in stats['data_sources'].items():
                percentage = round(count / stats['total_hotels'] * 100, 2)
                sources_table.add_row(source, str(count), f"{percentage}%")
            
            self.console.print(sources_table)
        
        # Display countries breakdown
        if stats['countries']:
            self.console.print("\n[bold]Countries Breakdown:[/bold]")
            countries_table = Table()
            countries_table.add_column("Country", style="cyan")
            countries_table.add_column("Count", style="green")
            countries_table.add_column("Percentage", style="yellow")
            
            # Sort countries by count (descending)
            sorted_countries = sorted(stats['countries'].items(), key=lambda x: x[1], reverse=True)
            
            for country, count in sorted_countries[:10]:  # Show top 10 countries
                percentage = round(count / stats['total_hotels'] * 100, 2)
                countries_table.add_row(country, str(count), f"{percentage}%")
            
            self.console.print(countries_table)
        
        # Display cities breakdown
        if stats['cities']:
            self.console.print("\n[bold]Top Cities:[/bold]")
            cities_table = Table()
            cities_table.add_column("City", style="cyan")
            cities_table.add_column("Count", style="green")
            cities_table.add_column("Percentage", style="yellow")
            
            # Sort cities by count (descending)
            sorted_cities = sorted(stats['cities'].items(), key=lambda x: x[1], reverse=True)
            
            for city, count in sorted_cities[:10]:  # Show top 10 cities
                percentage = round(count / stats['total_hotels'] * 100, 2)
                cities_table.add_row(city, str(count), f"{percentage}%")
            
            self.console.print(cities_table)
    
    # Menu for generating map visualization
    def generate_map_menu(self):
        """Generate map visualization menu"""
        self.console.print("\n[bold blue]GENERATE MAP VISUALIZATION[/bold blue]")
        self.console.print("=" * 60)
        
        # Check if folium is available
        if not isinstance(folium, type) and not hasattr(folium, 'Map'):
            self.console.print("[red]Folium module not available. Map cannot be generated.[/red]")
            self.console.print("[yellow]Please install folium using: pip install folium[/yellow]")
            return
        
        # Ask for filter criteria
        self.console.print("\n[yellow]Enter filter criteria to select hotels for map (leave empty to include all):[/yellow]")
        
        query = {}
        name = input("Hotel name contains: ")
        if name:
            query['name'] = f"%{name}%"
        
        city = input("City: ")
        if city:
            query['city'] = city
        
        country = input("Country: ")
        if country:
            query['country'] = country
        
        source = input("Data source: ")
        if source:
            query['data_source'] = source
        
        # Get hotels
        hotels = self.db.search_hotels(query)
        
        if not hotels:
            self.console.print("[red]No hotels found matching criteria.[/red]")
            return
        
        self.console.print(f"\n[green]Found {len(hotels)} hotels matching criteria.[/green]")
        
        # Generate map
        self.generate_map(hotels)
    
    # Generate map visualization
    def generate_map(self, hotels):
        """Generate map visualization from hotel list"""
        output_file = input("Enter output map file path (default: maps/hotel_map.html): ")
        if not output_file:
            os.makedirs("maps", exist_ok=True)
            output_file = "maps/hotel_map.html"
        
        self.console.print("\nGenerating map...")
        
        # Generate map
        map_file = self.scraper.generate_map(hotels, output_file)
        
        if map_file:
            self.console.print(f"[green]Map generated successfully: {map_file}[/green]")
            
            # Ask to open map in browser
            if input("Open map in browser? (y/n): ").lower() == 'y':
                try:
                    if webbrowser:
                        webbrowser.open(f"file://{os.path.abspath(map_file)}")
                    else:
                        self.console.print("[yellow]Webbrowser module not available. Cannot open map automatically.[/yellow]")
                        self.console.print(f"[yellow]You can manually open the file: {os.path.abspath(map_file)}[/yellow]")
                except Exception as e:
                    self.console.print(f"[red]Error opening map in browser: {e}[/red]")
                    self.console.print(f"[yellow]You can manually open the file: {os.path.abspath(map_file)}[/yellow]")
        else:
            self.console.print("[red]Error generating map.[/red]")
    
    # Menu for advanced search options
    def advanced_search_menu(self):
        """Advanced search menu"""
        self.console.print("\n[bold blue]ADVANCED SEARCH OPTIONS[/bold blue]")
        self.console.print("=" * 60)
        
        self.console.print("[yellow]Enter search criteria (leave empty to skip):[/yellow]")
        
        # Build query
        query = {}
        
        name = input("Hotel name contains: ")
        if name:
            query['name'] = f"%{name}%"
        
        city = input("City: ")
        if city:
            query['city'] = city
        
        country = input("Country: ")
        if country:
            query['country'] = country
        
        min_stars = input("Minimum stars rating: ")
        if min_stars and min_stars.replace('.', '', 1).isdigit():
            query['stars'] = float(min_stars)
        
        data_source = input("Data source: ")
        if data_source:
            query['data_source'] = data_source
        
        # Show additional criteria options
        self.console.print("\n[yellow]Additional criteria:[/yellow]")
        
        has_website = input("Has website (y/n/skip): ").lower()
        if has_website == 'y':
            query['website'] = '%'  # Any non-empty value
        elif has_website == 'n':
            query['website'] = ''  # Empty value
        
        has_phone = input("Has phone (y/n/skip): ").lower()
        if has_phone == 'y':
            query['phone'] = '%'  # Any non-empty value
        elif has_phone == 'n':
            query['phone'] = ''  # Empty value
        
        # Execute search
        if not query:
            self.console.print("[red]No search criteria specified.[/red]")
            return
        
        self.console.print("\nSearching hotels...")
        hotels = self.db.search_hotels(query)
        
        if hotels:
            self.console.print(f"\n[green]Found {len(hotels)} hotels matching criteria.[/green]")
            self.display_hotels(hotels[:20])  # Display first 20 results
            
            # Ask for pagination or export
            action = input("\nWhat would you like to do? (v: view all, e: export results, m: generate map, q: quit): ").lower()
            
            if action == 'v':
                # Pagination for viewing all results
                page_size = 20
                current_page = 0
                total_pages = (len(hotels) - 1) // page_size + 1
                
                while True:
                    start_idx = current_page * page_size
                    end_idx = min(start_idx + page_size, len(hotels))
                    
                    self.display_hotels(hotels[start_idx:end_idx])
                    
                    self.console.print(f"\nPage {current_page + 1} of {total_pages}")
                    
                    nav = input("Navigation (n: next, p: prev, j: jump to page, q: quit): ").lower()
                    
                    if nav == 'q':
                        break
                    elif nav == 'n' and current_page < total_pages - 1:
                        current_page += 1
                    elif nav == 'p' and current_page > 0:
                        current_page -= 1
                    elif nav == 'j':
                        page = input(f"Enter page number (1-{total_pages}): ")
                        try:
                            page = int(page)
                            if 1 <= page <= total_pages:
                                current_page = page - 1
                            else:
                                self.console.print("[red]Invalid page number.[/red]")
                        except ValueError:
                            self.console.print("[red]Invalid input.[/red]")
            
            elif action == 'e':
                # Export results
                formats = {
                    '1': 'CSV',
                    '2': 'Excel',
                    '3': 'JSON'
                }
                
                for key, value in formats.items():
                    self.console.print(f"[green]{key}[/green]: {value}")
                
                format_choice = input("\nSelect export format: ")
                
                if format_choice in formats:
                    # Convert choice to actual format string
                    output_format = formats[format_choice].lower()
                    
                    output_path = input(f"Enter output file path (default: exports/search_results.{output_format}): ")
                    if not output_path:
                        os.makedirs("exports", exist_ok=True)
                        output_path = f"exports/search_results.{output_format}"
                    
                    # Convert hotels to a format suitable for exporting
                    if self.db.export_data(output_format, output_path, query):
                        self.console.print(f"\n[green]Search results exported successfully to {output_path}[/green]")
                    else:
                        self.console.print("[red]Error exporting search results.[/red]")
            
            elif action == 'm':
                # Generate map
                self.generate_map(hotels)
        else:
            self.console.print("[red]No hotels found matching criteria.[/red]")
    
    # Menu for updating or deleting hotel records
    def update_delete_menu(self):
        """Update or delete hotel records menu"""
        self.console.print("\n[bold blue]UPDATE/DELETE HOTEL RECORDS[/bold blue]")
        self.console.print("=" * 60)
        
        # Ask for hotel ID
        hotel_id_input = input("Enter hotel ID: ")
        if not hotel_id_input.isdigit():
            self.console.print("[red]Invalid hotel ID.[/red]")
            return
        
        hotel_id = int(hotel_id_input)
        
        # Get hotel data
        hotel = self.db.get_hotel_by_id(hotel_id)
        
        if not hotel:
            self.console.print(f"[red]No hotel found with ID {hotel_id}.[/red]")
            return
        
        # Display hotel information
        self.console.print("\n[bold]Current Hotel Information:[/bold]")
        for key, value in hotel.items():
            self.console.print(f"[cyan]{key}[/cyan]: {value}")
        
        # Ask for action
        action = input("\nWhat would you like to do? (u: update, d: delete, c: cancel): ").lower()
        
        if action == 'u':
            # Update hotel information
            self.console.print("\n[yellow]Enter new values (leave empty to keep current value):[/yellow]")
            
            update_data = {}
            
            # Basic fields
            name = input(f"Name [{hotel.get('name', '')}]: ")
            if name:
                update_data['name'] = name
            
            address = input(f"Address [{hotel.get('address', '')}]: ")
            if address:
                update_data['address'] = address
            
            city = input(f"City [{hotel.get('city', '')}]: ")
            if city:
                update_data['city'] = city
            
            country = input(f"Country [{hotel.get('country', '')}]: ")
            if country:
                update_data['country'] = country
            
            phone = input(f"Phone [{hotel.get('phone', '')}]: ")
            if phone:
                update_data['phone'] = phone
            
            email = input(f"Email [{hotel.get('email', '')}]: ")
            if email:
                update_data['email'] = email
            
            website = input(f"Website [{hotel.get('website', '')}]: ")
            if website:
                update_data['website'] = website
            
            stars = input(f"Stars [{hotel.get('stars', '')}]: ")
            if stars and stars.replace('.', '', 1).isdigit():
                update_data['stars'] = float(stars)
            
            # Execute update
            if update_data:
                if self.db.update_hotel(hotel_id, update_data):
                    self.console.print(f"\n[green]Hotel record updated successfully.[/green]")
                    
                    # Display updated information
                    updated_hotel = self.db.get_hotel_by_id(hotel_id)
                    self.console.print("\n[bold]Updated Hotel Information:[/bold]")
                    for key, value in updated_hotel.items():
                        if key in update_data:
                            self.console.print(f"[cyan]{key}[/cyan]: [yellow]{value}[/yellow]")
                        else:
                            self.console.print(f"[cyan]{key}[/cyan]: {value}")
                else:
                    self.console.print("[red]Error updating hotel record.[/red]")
            else:
                self.console.print("[yellow]No changes made.[/yellow]")
        
        elif action == 'd':
            # Delete hotel record
            confirm = input(f"Are you sure you want to delete hotel '{hotel.get('name', '')}' (ID: {hotel_id})? (y/n): ").lower()
            
            if confirm == 'y':
                if self.db.delete_hotel(hotel_id):
                    self.console.print(f"\n[green]Hotel record deleted successfully.[/green]")
                else:
                    self.console.print("[red]Error deleting hotel record.[/red]")
            else:
                self.console.print("[yellow]Deletion cancelled.[/yellow]")
        
        elif action == 'c':
            self.console.print("[yellow]Operation cancelled.[/yellow]")
        
        else:
            self.console.print("[red]Invalid option.[/red]")
    
    # Menu for getting detailed information about a hotel
    def detailed_info_menu(self):
        """Get detailed information about a hotel menu"""
        self.console.print("\n[bold blue]GET DETAILED HOTEL INFORMATION[/bold blue]")
        self.console.print("=" * 60)
        
        # Ask for hotel ID
        hotel_id_input = input("Enter hotel ID: ")
        if not hotel_id_input.isdigit():
            self.console.print("[red]Invalid hotel ID.[/red]")
            return
        
        hotel_id = int(hotel_id_input)
        
        # Get hotel data
        hotel = self.db.get_hotel_by_id(hotel_id)
        
        if not hotel:
            self.console.print(f"[red]No hotel found with ID {hotel_id}.[/red]")
            return
        
        # Get source and ID for detailed info
        source = hotel.get('data_source', '').lower()
        
        if 'google places' in source:
            # Try to get place_id from additional_info
            try:
                additional_info = json.loads(hotel.get('additional_info', '{}'))
                place_id = additional_info.get('place_id')
                
                if place_id:
                    self.console.print(f"\n[yellow]Fetching detailed information for hotel from Google Places...[/yellow]")
                    detailed_info = self.scraper.get_hotel_details(place_id, 'google')
                    
                    if detailed_info:
                        self.console.print("\n[bold]Detailed Hotel Information:[/bold]")
                        # Display basic information
                        self.console.print(f"[bold cyan]Name:[/bold cyan] {detailed_info.get('name', '')}")
                        self.console.print(f"[bold cyan]Address:[/bold cyan] {detailed_info.get('address', '')}")
                        self.console.print(f"[bold cyan]Phone:[/bold cyan] {detailed_info.get('phone', '')}")
                        self.console.print(f"[bold cyan]Website:[/bold cyan] {detailed_info.get('website', '')}")
                        self.console.print(f"[bold cyan]Rating:[/bold cyan] {detailed_info.get('stars', '')} ★")
                        
                        # Display opening hours if available
                        opening_hours = detailed_info.get('opening_hours', [])
                        if opening_hours:
                            self.console.print("\n[bold cyan]Opening Hours:[/bold cyan]")
                            for hours in opening_hours:
                                self.console.print(f"  {hours}")
                        
                        # Display reviews if available
                        reviews = detailed_info.get('reviews', [])
                        if reviews:
                            self.console.print("\n[bold cyan]Recent Reviews:[/bold cyan]")
                            for i, review in enumerate(reviews[:3]):  # Show first 3 reviews
                                self.console.print(f"  [bold]Review {i+1}:[/bold] {review.get('text', '')} - Rating: {review.get('rating', '')} ★")
                    else:
                        self.console.print("[red]Could not fetch detailed information.[/red]")
                else:
                    self.console.print("[red]No Google Places ID found for this hotel.[/red]")
            except Exception as e:
                self.console.print(f"[red]Error fetching detailed information: {e}[/red]")
        
        elif 'openstreetmap' in source:
            # Try to get osm_id and osm_type from additional_info
            try:
                additional_info = json.loads(hotel.get('additional_info', '{}'))
                osm_id = additional_info.get('osm_id')
                osm_type = additional_info.get('osm_type')
                
                if osm_id:
                    osm_id_str = f"{osm_type}:{osm_id}" if osm_type else osm_id
                    self.console.print(f"\n[yellow]Fetching detailed information for hotel from OpenStreetMap...[/yellow]")
                    detailed_info = self.scraper.get_hotel_details(osm_id_str, 'osm')
                    
                    if detailed_info:
                        self.console.print("\n[bold]Detailed Hotel Information:[/bold]")
                        # Display basic information
                        self.console.print(f"[bold cyan]Name:[/bold cyan] {detailed_info.get('name', '')}")
                        self.console.print(f"[bold cyan]Address:[/bold cyan] {detailed_info.get('address', '')}")
                        self.console.print(f"[bold cyan]City:[/bold cyan] {detailed_info.get('city', '')}")
                        self.console.print(f"[bold cyan]Country:[/bold cyan] {detailed_info.get('country', '')}")
                        self.console.print(f"[bold cyan]Phone:[/bold cyan] {detailed_info.get('phone', '')}")
                        self.console.print(f"[bold cyan]Email:[/bold cyan] {detailed_info.get('email', '')}")
                        self.console.print(f"[bold cyan]Website:[/bold cyan] {detailed_info.get('website', '')}")
                        self.console.print(f"[bold cyan]Stars:[/bold cyan] {detailed_info.get('stars', '')}")
                        
                        # Display facilities if available
                        if detailed_info.get('facilities'):
                            self.console.print(f"[bold cyan]Facilities:[/bold cyan] {detailed_info.get('facilities', '')}")
                        
                        # Display accessibility information
                        if detailed_info.get('wheelchair'):
                            self.console.print(f"[bold cyan]Wheelchair Access:[/bold cyan] {detailed_info.get('wheelchair', '')}")
                        
                        # Display internet information
                        if detailed_info.get('internet_access'):
                            self.console.print(f"[bold cyan]Internet Access:[/bold cyan] {detailed_info.get('internet_access', '')}")
                        
                        # Display rooms information
                        if detailed_info.get('rooms'):
                            self.console.print(f"[bold cyan]Number of Rooms:[/bold cyan] {detailed_info.get('rooms', '')}")
                        
                        # Display swimming pool information
                        if detailed_info.get('swimming_pool'):
                            self.console.print(f"[bold cyan]Swimming Pool:[/bold cyan] {detailed_info.get('swimming_pool', '')}")
                    else:
                        self.console.print("[red]Could not fetch detailed information.[/red]")
                else:
                    self.console.print("[red]No OpenStreetMap ID found for this hotel.[/red]")
            except Exception as e:
                self.console.print(f"[red]Error fetching detailed information: {e}[/red]")
        
        else:
            self.console.print("[yellow]Detailed information is only available for hotels from Google Places or OpenStreetMap.[/yellow]")
        
        # Ask if user wants to update hotel information
        if input("\nDo you want to update this hotel's information? (y/n): ").lower() == 'y':
            self.update_delete_menu()
    
    # Helper method to display hotels
    def display_hotels(self, hotels):
        """
        Display hotels in a table format
        
        Args:
            hotels (List[Dict]): List of hotels to display
        """
        if not hotels:
            self.console.print("[yellow]No hotels to display.[/yellow]")
            return
        
        # Create table
        table = Table(title=f"Hotels ({len(hotels)} results)")
        
        # Add columns
        table.add_column("ID", style="cyan", no_wrap=True)
        table.add_column("Name", style="green")
        table.add_column("Address", style="blue")
        table.add_column("City", style="blue")
        table.add_column("Country", style="blue")
        table.add_column("Rating", style="yellow")
        table.add_column("Source", style="magenta")
        
        # Add rows
        for hotel in hotels:
            table.add_row(
                str(hotel.get('id', 'N/A')),
                hotel.get('name', 'N/A'),
                hotel.get('address', 'N/A'),
                hotel.get('city', 'N/A'),
                hotel.get('country', 'N/A'),
                f"{hotel.get('stars', 'N/A')} ★" if hotel.get('stars') else 'N/A',
                hotel.get('data_source', 'N/A')
            )
        
        # Print table
        self.console.print(table)

# ================================================
# 7. MAIN FUNCTION
# ================================================
def main():
    # Create command-line interface
    console = Console()
    
    console.print("[bold blue]MAP RESEARCHER 0.3V - HOTEL DATA COLLECTION TOOL[/bold blue]")
    console.print("=" * 60)
    console.print("Welcome to the Map Researcher tool!")
    
    # Create instances
    try:
        import json
        config = {}
        if os.path.exists('config/config.json'):
            with open('config/config.json', 'r') as f:
                config = json.load(f)
        
        # Get API keys from config
        api_keys = config.get('api_keys', {})
        
        # Get database settings
        db_settings = config.get('database', {'type': 'sqlite', 'path': 'data/hotels.db'})
        
        # Initialize database
        db = Database(db_settings.get('type', 'sqlite'), db_settings.get('path', 'data/hotels.db'))
        
        # Initialize scraper
        scraper = HotelScraper(api_keys)
        
        # Initialize CLI
        cli = CommandLineInterface(db, scraper)
        
        # Run main menu
        cli.main_menu()
        
    except Exception as e:
        console.print(f"[red]Error initializing application: {e}[/red]")
        import traceback
        traceback.print_exc()
        input("Press Enter to exit...")
    finally:
        # Ensure database connection is closed
        if 'db' in locals() and hasattr(db, 'close'):
            db.close()

# Run the main function if executed directly
if __name__ == "__main__":
    main()