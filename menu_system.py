#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Menu System for Map_researcher 0.5
This module provides the main menu interface and navigation for the application.
"""

import os
import sys
import time
import json
import logging
import traceback
from datetime import datetime
from typing import Dict, List, Any, Callable, Optional

class MenuSystem:
    """Main class for handling the menu system of the application"""
    
    def __init__(self, db=None, config=None):
        """
        Initialize the menu system
        
        Args:
            db: Database instance
            scraper: HotelScraper instance
        """
        self.db = db
        self.config = config
        self.current_menu = "main"
        self.previous_menus = []
        self.search_results = []
        self.exit_requested = False
        self.logger = logging.getLogger("menu_system")
        
        # Define color codes for terminal
        self.colors = {
            "reset": "\033[0m",
            "bold": "\033[1m",
            "blue": "\033[94m",
            "green": "\033[92m",
            "yellow": "\033[93m",
            "red": "\033[91m",
            "cyan": "\033[96m",
            "magenta": "\033[95m",
            "white": "\033[97m",
            "black": "\033[30m",
            "bg_blue": "\033[44m",
            "bg_green": "\033[42m",
            "bg_yellow": "\033[43m"
        }
        
        # Load configuration
        self._load_config()
        
        # Initialize menu system
        self._init_menus()
        
        # Initialize components
        self._init_components()
    
    def _load_config(self):
        """Load configuration from file"""
        try:
            if self.config is None:
                config_path = os.path.join("config", "config.json")
                if os.path.exists(config_path):
                    with open(config_path, 'r', encoding='utf-8') as f:
                        self.config = json.load(f)
                    self.logger.info("Configuration loaded from file")
                else:
                    self.logger.warning(f"Config file {config_path} not found, using defaults")
                    self.config = {}
        except Exception as e:
            self.logger.error(f"Error loading configuration: {str(e)}")
            self.config = {}
    
    def _init_components(self):
        """Initialize system components"""
        self.discovery_integration = None
        self.map_visualizer = None
        
        self.initialized_components = {
            "google_maps_analyzer": False,
            "booking_platforms_discovery": False,
            "smart_location_analyzer": False,
            "hotel_classifier": False,
            "pattern_discovery": False,
            "reviews_analyzer": False,
            "discovery_integration": False,
            "map_visualizer": False
        }
    
    def _init_component(self, component_name):
        """Initialize a specific component"""
        if self.initialized_components.get(component_name, False):
            return True
            
        try:
            if component_name == "discovery_integration":
                from discovery_integration import DiscoveryIntegration
                self.discovery_integration = DiscoveryIntegration(self.db, self.config)
                self.initialized_components["discovery_integration"] = True
                return True
                
            elif component_name == "map_visualizer":
                from visualization.map_visualizer import MapVisualizer
                self.map_visualizer = MapVisualizer(self.db, self.config)
                self.initialized_components["map_visualizer"] = True
                return True
                
            elif component_name == "google_maps_analyzer":
                from web_discovery.google_maps_analyzer import GoogleMapsAnalyzer
                self.google_maps_analyzer = GoogleMapsAnalyzer(self.db, self.config)
                self.initialized_components["google_maps_analyzer"] = True
                return True
                
            elif component_name == "booking_platforms_discovery":
                from web_discovery.booking_platforms_discovery import BookingPlatformsDiscovery
                self.booking_platforms_discovery = BookingPlatformsDiscovery(self.db, self.config)
                self.initialized_components["booking_platforms_discovery"] = True
                return True
                
            elif component_name == "smart_location_analyzer":
                from location_analysis.smart_location_analyzer import SmartLocationAnalyzer
                self.smart_location_analyzer = SmartLocationAnalyzer(self.db, self.config)
                self.initialized_components["smart_location_analyzer"] = True
                return True
                
            elif component_name == "hotel_classifier":
                from machine_learning.hotel_classifier import HotelClassifier
                self.hotel_classifier = HotelClassifier(self.db, self.config)
                self.initialized_components["hotel_classifier"] = True
                return True
                
            elif component_name == "pattern_discovery":
                from analysis.pattern_discovery import PatternDiscovery
                self.pattern_discovery = PatternDiscovery(self.db, self.config)
                self.initialized_components["pattern_discovery"] = True
                return True
                
            elif component_name == "reviews_analyzer":
                from reviews_analysis.reviews_analyzer import ReviewsAnalyzer
                self.reviews_analyzer = ReviewsAnalyzer(self.db, self.config)
                self.initialized_components["reviews_analyzer"] = True
                return True
                
            else:
                self.logger.warning(f"Unknown component: {component_name}")
                return False
                
        except Exception as e:
            self.logger.error(f"Error initializing component {component_name}: {str(e)}")
            traceback.print_exc()
            return False
    
    def _init_menus(self):
        """Initialize all menu definitions"""
        # Main menu
        self.menus = {
            "main": {
                "title": "MAP RESEARCHER 0.5 - HOTEL DATA EXPLORATION SYSTEM",
                "options": [
                    {"key": "1", "label": "Discovery and Collection Operations", "action": self.show_discovery_menu},
                    {"key": "2", "label": "Search and Query Operations", "action": self.show_search_menu},
                    {"key": "3", "label": "Temporal and Historical Analysis", "action": self.show_temporal_menu},
                    {"key": "4", "label": "Potential Violations Analysis", "action": self.show_violations_menu},
                    {"key": "5", "label": "Data and Report Export", "action": self.show_export_menu},
                    {"key": "6", "label": "Database Management", "action": self.show_database_menu},
                    {"key": "7", "label": "Statistics and Reports", "action": self.show_statistics_menu},
                    {"key": "8", "label": "Advanced Settings", "action": self.show_settings_menu},
                    {"key": "0", "label": "Exit", "action": self.exit_application}
                ]
            },
            
            # Discovery and Collection Menu
            "discovery": {
                "title": "DISCOVERY AND COLLECTION OPERATIONS",
                "options": [
                    {"key": "1", "label": "Comprehensive Location-based Discovery", "action": self.location_discovery},
                    {"key": "2", "label": "Advanced Name and Location Discovery", "action": self.name_location_discovery},
                    {"key": "3", "label": "Smart Discovery (Gradual Expansion)", "action": self.smart_discovery},
                    {"key": "4", "label": "Detailed Information Collection for Specific Hotel", "action": self.detailed_info_collection},
                    {"key": "5", "label": "Import Data from External File", "action": self.import_external_data},
                    {"key": "6", "label": "City-wide Comprehensive Scan", "action": self.city_scan},
                    {"key": "7", "label": "Update Existing Hotel Data", "action": self.update_hotel_data},
                    {"key": "8", "label": "Update History and Temporal Changes", "action": self.update_temporal_data},
                    {"key": "9", "label": "Search in Non-Traditional Rental Platforms", "action": self.search_rental_platforms},
                    {"key": "a", "label": "Google Maps Advanced Analysis", "action": self.google_maps_advanced_analysis},
                    {"key": "b", "label": "Discover Hotels from Reviews", "action": self.discover_from_reviews},
                    {"key": "c", "label": "Analyze Commercial Areas", "action": self.analyze_commercial_areas},
                    {"key": "d", "label": "Integrated Multi-Source Discovery", "action": self.integrated_discovery},
                    {"key": "0", "label": "Return to Main Menu", "action": self.back_to_main_menu}
                ]
            },
            
            # Search Menu
            "search": {
                "title": "SEARCH AND QUERY OPERATIONS",
                "options": [
                    {"key": "1", "label": "Advanced Multi-criteria Search", "action": self.advanced_search},
                    {"key": "2", "label": "Search for Specific Hotel by Name", "action": self.search_by_name},
                    {"key": "3", "label": "Search for Hotels with Sparse Information", "action": self.search_sparse_info},
                    {"key": "4", "label": "Search for Hotels with Same Owner/Operator", "action": self.search_same_owner},
                    {"key": "5", "label": "Search for Geographically Close Hotels", "action": self.search_nearby},
                    {"key": "6", "label": "Search for Similar Hotels", "action": self.search_similar},
                    {"key": "7", "label": "Search for Unverified Hotels", "action": self.search_unverified},
                    {"key": "8", "label": "Search for Hotels with Recent Changes", "action": self.search_recent_changes},
                    {"key": "9", "label": "View Detailed Information for a Specific Hotel", "action": self.view_hotel_details},
                    {"key": "a", "label": "Discover Hotel Patterns and Groups", "action": self.discover_hotel_patterns},
                    {"key": "b", "label": "Find Hotels by Review Keywords", "action": self.find_by_review_keywords},
                    {"key": "0", "label": "Return to Main Menu", "action": self.back_to_main_menu}
                ]
            },
            
            # Temporal Analysis Menu
            "temporal": {
                "title": "TEMPORAL AND HISTORICAL ANALYSIS",
                "options": [
                    {"key": "1", "label": "Analyze History of a Specific Hotel", "action": self.analyze_hotel_history},
                    {"key": "2", "label": "Search for Historical Changes of Hotels", "action": self.search_historical_changes},
                    {"key": "3", "label": "Detect Changes in Owners/Operators", "action": self.detect_owner_changes},
                    {"key": "4", "label": "Track Hotel Transitions Between Platforms", "action": self.track_platform_transitions},
                    {"key": "5", "label": "Historical Comparison Between Two Hotels", "action": self.compare_hotels_history},
                    {"key": "6", "label": "Analyze Changes at the Same Geographic Location", "action": self.analyze_location_changes},
                    {"key": "7", "label": "Extract Information from Internet Archive", "action": self.extract_from_archive},
                    {"key": "8", "label": "Analyze Historical Record of Permits and Licenses", "action": self.analyze_permits_history},
                    {"key": "9", "label": "Comprehensive Report on Historical Changes", "action": self.historical_changes_report},
                    {"key": "0", "label": "Return to Main Menu", "action": self.back_to_main_menu}
                ]
            },
            
            # Violations Analysis Menu
            "violations": {
                "title": "POTENTIAL VIOLATIONS ANALYSIS",
                "options": [
                    {"key": "1", "label": "Comprehensive Violations Analysis", "action": self.analyze_violations},
                    {"key": "2", "label": "Check for Data Inconsistencies Between Sources", "action": self.check_inconsistencies},
                    {"key": "3", "label": "Detect Hotels with Dual Activity", "action": self.detect_dual_activity},
                    {"key": "4", "label": "Analyze Hotels Not in Official Records", "action": self.analyze_unofficial},
                    {"key": "5", "label": "Detect Unusual Hotel Clusters", "action": self.detect_unusual_clusters},
                    {"key": "6", "label": "Analyze Known Evasion Patterns", "action": self.analyze_evasion_patterns},
                    {"key": "7", "label": "Check Hotels Located in Residential Areas", "action": self.check_residential_hotels},
                    {"key": "8", "label": "List of High-Risk Hotels", "action": self.list_high_risk},
                    {"key": "9", "label": "Create Detailed Violations Report", "action": self.create_violations_report},
                    {"key": "0", "label": "Return to Main Menu", "action": self.back_to_main_menu}
                ]
            },
            
            # Export Menu
            "export": {
                "title": "DATA AND REPORT EXPORT",
                "options": [
                    {"key": "1", "label": "Export Results in Excel Format", "action": self.export_excel},
                    {"key": "2", "label": "Export Results in CSV Format", "action": self.export_csv},
                    {"key": "3", "label": "Export Results in JSON Format", "action": self.export_json},
                    {"key": "4", "label": "Export Potential Violations Report", "action": self.export_violations_report},
                    {"key": "5", "label": "Export Timeline Chart", "action": self.export_timeline},
                    {"key": "6", "label": "Export Historical Analysis Data", "action": self.export_historical_analysis},
                    {"key": "7", "label": "Export Hotel Data on Map (HTML)", "action": self.export_map},
                    {"key": "8", "label": "Export Statistical Summary", "action": self.export_statistics},
                    {"key": "9", "label": "Create Custom Report", "action": self.create_custom_report},
                    {"key": "0", "label": "Return to Main Menu", "action": self.back_to_main_menu}
                ]
            },
            
            # Database Management Menu
            "database": {
                "title": "DATABASE MANAGEMENT",
                "options": [
                    {"key": "1", "label": "View Database Information", "action": self.view_database_info},
                    {"key": "2", "label": "Add Hotel Manually", "action": self.add_hotel_manually},
                    {"key": "3", "label": "Update a Specific Hotel", "action": self.update_specific_hotel},
                    {"key": "4", "label": "Delete Hotel from Database", "action": self.delete_hotel},
                    {"key": "5", "label": "Merge Duplicate Records", "action": self.merge_duplicates},
                    {"key": "6", "label": "Clean Invalid Data", "action": self.clean_invalid_data},
                    {"key": "7", "label": "Rebuild Database Indexes", "action": self.rebuild_indexes},
                    {"key": "8", "label": "Backup Database", "action": self.backup_database},
                    {"key": "9", "label": "Restore Database from Backup", "action": self.restore_database},
                    {"key": "0", "label": "Return to Main Menu", "action": self.back_to_main_menu}
                ]
            },
            
            # Statistics Menu
            "statistics": {
                "title": "STATISTICS AND REPORTS",
                "options": [
                    {"key": "1", "label": "Comprehensive Statistical Report", "action": self.comprehensive_statistics},
                    {"key": "2", "label": "Hotel Distribution by Geographic Areas", "action": self.geographic_distribution},
                    {"key": "3", "label": "Source Statistics and Coverage Rates", "action": self.source_statistics},
                    {"key": "4", "label": "Potential Violations Trend Analysis", "action": self.violations_trends},
                    {"key": "5", "label": "Data Comparison with Previous Periods", "action": self.period_comparison},
                    {"key": "6", "label": "Hotel Statistics by Classification", "action": self.classification_statistics},
                    {"key": "7", "label": "Activity and Changes Report", "action": self.activity_report},
                    {"key": "8", "label": "Booking and Rental Platform Report", "action": self.platform_report},
                    {"key": "9", "label": "Custom Report", "action": self.custom_statistics_report},
                    {"key": "0", "label": "Return to Main Menu", "action": self.back_to_main_menu}
                ]
            },
            
            # Settings Menu
            "settings": {
                "title": "ADVANCED SETTINGS",
                "options": [
                    {"key": "1", "label": "Data Sources and API Settings", "action": self.data_source_settings},
                    {"key": "2", "label": "Matching and Comparison Settings", "action": self.matching_settings},
                    {"key": "3", "label": "Historical Analysis Settings", "action": self.historical_settings},
                    {"key": "4", "label": "Risk Assessment Settings", "action": self.risk_settings},
                    {"key": "5", "label": "Suspicious Hotel Detection Criteria", "action": self.detection_settings},
                    {"key": "6", "label": "Violations Detector Settings", "action": self.violations_settings},
                    {"key": "7", "label": "Database Settings", "action": self.database_settings},
                    {"key": "8", "label": "Data Export Settings", "action": self.export_settings},
                    {"key": "9", "label": "Restore Default Settings", "action": self.restore_default_settings},
                    {"key": "0", "label": "Return to Main Menu", "action": self.back_to_main_menu}
                ]
            },
        }
    
    def run(self):
        """Run the menu system main loop"""
        # Clear screen and show welcome message
        self.clear_screen()
        print(f"{self.colors['bold']}{self.colors['blue']}Welcome to Map_researcher 0.5{self.colors['reset']}")
        print("Hotel data collection and analysis application")
        print(f"{self.colors['yellow']}Created for hotel regulation and compliance monitoring{self.colors['reset']}")
        print("\nPress Enter to continue...")
        input()
        
        # Main menu loop
        while not self.exit_requested:
            self.show_menu(self.current_menu)
            choice = input("\nEnter option number or letter: ")
            self.process_choice(self.current_menu, choice)
    
    def show_menu(self, menu_name):
        """
        Display the specified menu
        
        Args:
            menu_name (str): The name of the menu to display
        """
        self.clear_screen()
        
        if menu_name in self.menus:
            menu = self.menus[menu_name]
            
            # Display header
            print("=" * 70)
            print(f"{self.colors['bold']}{self.colors['bg_blue']}{self.colors['white']} {menu['title']} {self.colors['reset']}")
            print("=" * 70)
            print()
            
            # Display options
            for option in menu["options"]:
                if option["key"] == "0":
                    print()  # Add extra space before the return/exit option
                color = self.colors["green"] if option["key"] != "0" else self.colors["yellow"]
                print(f"{color}[{option['key']}]{self.colors['reset']} {option['label']}")
    
    def process_choice(self, menu_name, choice):
        """
        Process user choice from a menu
        
        Args:
            menu_name (str): The name of the current menu
            choice (str): The user's choice
        """
        if menu_name in self.menus:
            menu = self.menus[menu_name]
            
            # Find matching option
            for option in menu["options"]:
                if option["key"] == choice:
                    # Call the action function
                    option["action"]()
                    return
            
            # If we get here, no valid option was chosen
            print(f"{self.colors['red']}Invalid option. Please try again.{self.colors['reset']}")
            time.sleep(1.5)
    
    def clear_screen(self):
        """Clear the terminal screen"""
        os.system('cls' if os.name == 'nt' else 'clear')
    
    def back_to_main_menu(self):
        """Return to main menu"""
        self.current_menu = "main"
    
    def exit_application(self):
        """Exit the application"""
        self.clear_screen()
        print(f"{self.colors['yellow']}Thank you for using Map_researcher 0.5{self.colors['reset']}")
        print("Exiting application...")
        self.exit_requested = True
    
    def show_discovery_menu(self):
        """Show discovery and collection menu"""
        self.current_menu = "discovery"
    
    def show_search_menu(self):
        """Show search and query menu"""
        self.current_menu = "search"
    
    def show_temporal_menu(self):
        """Show temporal analysis menu"""
        self.current_menu = "temporal"
    
    def show_violations_menu(self):
        """Show violations analysis menu"""
        self.current_menu = "violations"
    
    def show_export_menu(self):
        """Show export menu"""
        self.current_menu = "export"
    
    def show_database_menu(self):
        """Show database management menu"""
        self.current_menu = "database"
    
    def show_statistics_menu(self):
        """Show statistics menu"""
        self.current_menu = "statistics"
    
    def show_settings_menu(self):
        """Show settings menu"""
        self.current_menu = "settings"
    
    # Discovery Menu Functions
    def location_discovery(self):
        """Comprehensive location-based discovery"""
        self.clear_screen()
        print(f"{self.colors['green']}=== Comprehensive Location-based Discovery ==={self.colors['reset']}")
        print("\nEnter location details for search:")
        
        location = input("Location (city, area, or coordinates): ")
        if not location:
            print(f"{self.colors['red']}No location entered!{self.colors['reset']}")
            input("\nPress Enter to return...")
            return
        
        radius = input("Search radius in meters (default: 5000): ")
        radius = int(radius) if radius.isdigit() else 5000
        
        print("\nSearching...")
        
        # Initialize GoogleMapsAnalyzer if needed
        if not self._init_component("google_maps_analyzer"):
            print(f"\n{self.colors['red']}Error initializing Google Maps Analyzer.{self.colors['reset']}")
            input("\nPress Enter to return...")
            return
        
        # Perform discovery
        try:
            results = self.google_maps_analyzer.discover_hotels_in_area(location, radius=radius)
            
            # Process and display results
            if results:
                print(f"\n{self.colors['green']}Found {len(results)} potential establishments!{self.colors['reset']}")
                
                # Display summary of results
                self._display_discovery_results(results)
                
                self.search_results = results
                
                # Save to database if available
                if self.db and hasattr(self.db, 'save_hotels'):
                    save = input("\nDo you want to save results to database? (y/n): ")
                    if save.lower() in ['y', 'yes']:
                        saved = self.db.save_hotels(results)
                        print(f"\n{self.colors['green']}Saved {saved} establishments to database{self.colors['reset']}")
                
                # Offer to export data
                export = input("\nDo you want to export results? (y/n): ")
                if export.lower() in ['y', 'yes']:
                    self._export_results_menu(results)
            else:
                print(f"\n{self.colors['yellow']}No results found!{self.colors['reset']}")
        
        except Exception as e:
            print(f"\n{self.colors['red']}Error during search: {str(e)}{self.colors['reset']}")
            traceback.print_exc()
        
        input("\nPress Enter to return...")
    
    def _display_discovery_results(self, results, limit=10):
        """Display discovery results summary"""
        print(f"\n{self.colors['bold']}Top {min(limit, len(results))} results:{self.colors['reset']}")
        
        for i, hotel in enumerate(results[:limit]):
            name = hotel.get('name', 'Unnamed')
            address = hotel.get('address', 'No address')
            category = hotel.get('category', 'Uncategorized')
            source = hotel.get('source', 'Unknown source')
            
            print(f"{i+1}. {self.colors['bold']}{name}{self.colors['reset']}")
            print(f"   Address: {address}")
            print(f"   Category: {category}")
            print(f"   Source: {source}")
            print()
        
        if len(results) > limit:
            print(f"...and {len(results) - limit} more.")
    
    def _export_results_menu(self, results):
        """Display menu for exporting results"""
        self.clear_screen()
        print(f"{self.colors['green']}=== Export Options ==={self.colors['reset']}")
        print("\nChoose format to export data:")
        print("1. Excel (.xlsx)")
        print("2. CSV (.csv)")
        print("3. JSON (.json)")
        print("4. Interactive Map (.html)")
        print("0. Cancel")
        
        choice = input("\nEnter option: ")
        
        if choice == "1":
            self._export_to_excel(results)
        elif choice == "2":
            self._export_to_csv(results)
        elif choice == "3":
            self._export_to_json(results)
        elif choice == "4":
            self._export_to_map(results)
    
    def _export_to_excel(self, results):
        """Export results to Excel"""
        try:
            from export_module import ExportModule
            exporter = ExportModule(self.db)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_path = f"exports/hotels_export_{timestamp}.xlsx"
            
            export_result = exporter.export_excel(results, output_path)
            
            if export_result.get("status") == "success":
                print(f"\n{self.colors['green']}Data exported successfully to {output_path}{self.colors['reset']}")
            else:
                print(f"\n{self.colors['red']}Export failed: {export_result.get('error', 'Unknown error')}{self.colors['reset']}")
        except Exception as e:
            print(f"\n{self.colors['red']}Error exporting to Excel: {str(e)}{self.colors['reset']}")
    
    def _export_to_csv(self, results):
        """Export results to CSV"""
        try:
            from export_module import ExportModule
            exporter = ExportModule(self.db)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_path = f"exports/hotels_export_{timestamp}.csv"
            
            export_result = exporter.export_csv(results, output_path)
            
            if export_result.get("status") == "success":
                print(f"\n{self.colors['green']}Data exported successfully to {output_path}{self.colors['reset']}")
            else:
                print(f"\n{self.colors['red']}Export failed: {export_result.get('error', 'Unknown error')}{self.colors['reset']}")
        except Exception as e:
            print(f"\n{self.colors['red']}Error exporting to CSV: {str(e)}{self.colors['reset']}")
    
    def _export_to_json(self, results):
        """Export results to JSON"""
        try:
            from export_module import ExportModule
            exporter = ExportModule(self.db)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_path = f"exports/hotels_export_{timestamp}.json"
            
            export_result = exporter.export_json(results, output_path)
            
            if export_result.get("status") == "success":
                print(f"\n{self.colors['green']}Data exported successfully to {output_path}{self.colors['reset']}")
            else:
                print(f"\n{self.colors['red']}Export failed: {export_result.get('error', 'Unknown error')}{self.colors['reset']}")
        except Exception as e:
            print(f"\n{self.colors['red']}Error exporting to JSON: {str(e)}{self.colors['reset']}")
    
    def _export_to_map(self, results):
        """Export results to interactive map"""
        if not self._init_component("map_visualizer"):
            print(f"\n{self.colors['red']}Error initializing map visualizer.{self.colors['reset']}")
            return
            
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_path = f"exports/hotels_map_{timestamp}.html"
            
            result = self.map_visualizer.create_hotel_map(
                results, 
                output_path=output_path,
                title=f"Hotel Map - {len(results)} establishments"
            )
            
            if result.get("error"):
                print(f"\n{self.colors['red']}Map creation failed: {result['error']}{self.colors['reset']}")
            else:
                print(f"\n{self.colors['green']}Map created successfully: {output_path}{self.colors['reset']}")
                print(f"Included {result.get('hotel_count', 0)} establishments with valid coordinates")
            
        except Exception as e:
            print(f"\n{self.colors['red']}Error creating map: {str(e)}{self.colors['reset']}")
    
    def name_location_discovery(self):
        """Advanced name and location discovery"""
        self.clear_screen()
        print(f"{self.colors['green']}=== Advanced Name and Location Discovery ==={self.colors['reset']}")
        print("\nThis function searches for hotels by name and optional location")
        
        name = input("Hotel name or keywords: ")
        if not name:
            print(f"{self.colors['red']}No name entered!{self.colors['reset']}")
            input("\nPress Enter to return...")
            return
        
        location = input("Location (optional, can be city or coordinates): ")
        
        radius = input("Search radius in meters (default: 5000): ")
        radius = int(radius) if radius.isdigit() else 5000
        
        print("\nSearching...")
        
        try:
            if not hasattr(self, 'data_discovery'):
                from data_discovery import DataDiscovery
                self.data_discovery = DataDiscovery(self.db)
            
            if location:
                results = self.data_discovery.discover_by_name(name, location, radius)
            else:
                results = self.data_discovery.discover_by_name(name)
            
            if results:
                print(f"\n{self.colors['green']}Found {len(results)} establishments matching '{name}'{self.colors['reset']}")
                self._display_discovery_results(results)
                self.search_results = results
                
                # Save to database if available
                if self.db and hasattr(self.db, 'save_hotels'):
                    save = input("\nDo you want to save results to database? (y/n): ")
                    if save.lower() in ['y', 'yes']:
                        saved = self.db.save_hotels(results)
                        print(f"\n{self.colors['green']}Saved {saved} establishments to database{self.colors['reset']}")
                
                # Offer to export data
                export = input("\nDo you want to export results? (y/n): ")
                if export.lower() in ['y', 'yes']:
                    self._export_results_menu(results)
            else:
                print(f"\n{self.colors['yellow']}No results found!{self.colors['reset']}")
            
        except Exception as e:
            print(f"\n{self.colors['red']}Error during search: {str(e)}{self.colors['reset']}")
        
        input("\nPress Enter to return...")
    
    def smart_discovery(self):
        """Smart discovery with gradual expansion"""
        self.show_not_implemented_message("Smart Discovery")
    
    def detailed_info_collection(self):
        """Detailed information collection for specific hotel"""
        self.show_not_implemented_message("Detailed Information Collection")
    
    def import_external_data(self):
        """Import data from external file"""
        self.show_not_implemented_message("Import External Data")
    
    def city_scan(self):
        """City-wide comprehensive scan"""
        self.show_not_implemented_message("City-wide Comprehensive Scan")
    
    def update_hotel_data(self):
        """Update existing hotel data"""
        self.show_not_implemented_message("Update Hotel Data")
    
    def update_temporal_data(self):
        """Update history and temporal changes"""
        self.show_not_implemented_message("Update Temporal Data")
    
    def search_rental_platforms(self):
        """Search in non-traditional rental platforms"""
        self.clear_screen()
        print(f"{self.colors['green']}=== Search in Non-Traditional Rental Platforms ==={self.colors['reset']}")
        
        location = input("Location (city, area, or coordinates): ")
        if not location:
            print(f"{self.colors['red']}No location entered!{self.colors['reset']}")
            input("\nPress Enter to return...")
            return
        
        radius = input("Search radius in meters (default: 2000): ")
        radius = int(radius) if radius.isdigit() else 2000
        
        print("\nSearching rental platforms...")
        
        # Initialize component if needed
        if not self._init_component("booking_platforms_discovery"):
            print(f"\n{self.colors['red']}Error initializing booking platforms discovery.{self.colors['reset']}")
            input("\nPress Enter to return...")
            return
        
        try:
            # Get tomorrow's date for check-in
            checkin_date = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")
            checkout_date = (datetime.now() + timedelta(days=2)).strftime("%Y-%m-%d")
            
            platforms = ['airbnb', 'booking', 'expedia', 'local_platforms', 'haraj']
            
            results = self.booking_platforms_discovery.discover_hotels_in_area(
                location, 
                checkin_date=checkin_date,
                checkout_date=checkout_date,
                platforms=platforms
            )
            
            if results:
                print(f"\n{self.colors['green']}Found {len(results)} rental properties{self.colors['reset']}")
                
                # Group by platform
                by_platform = {}
                for result in results:
                    platform = result.get('platform', 'unknown')
                    if platform not in by_platform:
                        by_platform[platform] = []
                    by_platform[platform].append(result)
                
                # Display summary by platform
                for platform, props in by_platform.items():
                    print(f"\n{self.colors['cyan']}Platform: {platform.upper()} - {len(props)} properties{self.colors['reset']}")
                    for i, prop in enumerate(props[:3]):  # Show top 3 from each platform
                        name = prop.get('name', 'Unnamed')
                        price = prop.get('price', 'Unknown price')
                        print(f"{i+1}. {name} - {price}")
                    if len(props) > 3:
                        print(f"   ...and {len(props)-3} more.")
                
                self.search_results = results
                
                # Save to database if available
                if self.db and hasattr(self.db, 'save_hotels'):
                    save = input("\nDo you want to save results to database? (y/n): ")
                    if save.lower() in ['y', 'yes']:
                        saved = self.db.save_hotels(results)
                        print(f"\n{self.colors['green']}Saved {saved} properties to database{self.colors['reset']}")
                
                # Offer to export data
                export = input("\nDo you want to export results? (y/n): ")
                if export.lower() in ['y', 'yes']:
                    self._export_results_menu(results)
            else:
                print(f"\n{self.colors['yellow']}No rental properties found!{self.colors['reset']}")
            
        except Exception as e:
            print(f"\n{self.colors['red']}Error searching rental platforms: {str(e)}{self.colors['reset']}")
            traceback.print_exc()
        
        input("\nPress Enter to return...")
    
    def google_maps_advanced_analysis(self):
        """Google Maps advanced analysis"""
        self.clear_screen()
        print(f"{self.colors['green']}=== Google Maps Advanced Analysis ==={self.colors['reset']}")
        print("This function performs deep analysis of Google Maps data to find hidden hotels.")
        
        location = input("Location (city, area, or coordinates): ")
        if not location:
            print(f"{self.colors['red']}No location entered!{self.colors['reset']}")
            input("\nPress Enter to return...")
            return
        
        radius = input("Search radius in meters (default: 5000): ")
        radius = int(radius) if radius.isdigit() else 5000
        
        max_results = input("Maximum results to analyze (default: 100): ")
        max_results = int(max_results) if max_results.isdigit() else 100
        
        print("\nPerforming advanced analysis (this may take some time)...")
        
        # Initialize GoogleMapsAnalyzer if needed
        if not self._init_component("google_maps_analyzer"):
            print(f"\n{self.colors['red']}Error initializing Google Maps Analyzer.{self.colors['reset']}")
            input("\nPress Enter to return...")
            return
        
        try:
            # Discovery phase
            results = self.google_maps_analyzer.discover_hotels_in_area(location, radius, max_results)
            
            if not results:
                print(f"\n{self.colors['yellow']}No results found!{self.colors['reset']}")
                input("\nPress Enter to return...")
                return
            
            print(f"\n{self.colors['green']}Found {len(results)} potential establishments{self.colors['reset']}")
            
            # Now analyze reviews to detect hidden hotels
            if not self._init_component("reviews_analyzer"):
                print(f"\n{self.colors['yellow']}Reviews analyzer not available. Skipping review analysis.{self.colors['reset']}")
            else:
                print("\nAnalyzing reviews for hotel indicators...")
                hidden_hotels = self.reviews_analyzer.identify_hidden_hotels(results)
                
                if hidden_hotels:
                    print(f"\n{self.colors['green']}Found {len(hidden_hotels)} hidden hotels based on review analysis!{self.colors['reset']}")
                    
                    print(f"\n{self.colors['bold']}Top hidden hotels:{self.colors['reset']}")
                    for i, hotel in enumerate(hidden_hotels[:5]):
                        name = hotel.get('name', 'Unnamed')
                        confidence = hotel.get('review_analysis', {}).get('confidence', 0)
                        
                        print(f"{i+1}. {self.colors['bold']}{name}{self.colors['reset']} - {confidence*100:.1f}% confidence")
                        print(f"   Category: {hotel.get('category', 'Uncategorized')}")
                        print(f"   Address: {hotel.get('address', 'No address')}")
                        
                        # Show top hotel indicators
                        indicators = hotel.get('review_analysis', {}).get('hotel_indicators', [])
                        if indicators:
                            print(f"   Top indicator: {indicators[0]['category']} ({indicators[0]['count']} mentions)")
                        print()
                    
                    # Add hidden hotels to results with special tag
                    for hotel in hidden_hotels:
                        hotel['is_hidden_hotel'] = True
                        
                    # Filter original results to remove duplicates with hidden hotels
                    seen_ids = {hotel.get('place_id') for hotel in hidden_hotels if hotel.get('place_id')}
                    filtered_results = [hotel for hotel in results if hotel.get('place_id') not in seen_ids]
                    
                    # Combine hidden hotels with other results
                    combined_results = hidden_hotels + filtered_results
                    self.search_results = combined_results
                    
                    # Save to database if available
                    if self.db and hasattr(self.db, 'save_hotels'):
                        save = input("\nDo you want to save results to database? (y/n): ")
                        if save.lower() in ['y', 'yes']:
                            saved = self.db.save_hotels(combined_results)
                            print(f"\n{self.colors['green']}Saved {saved} establishments to database{self.colors['reset']}")
                    
                    # Offer to export data
                    export = input("\nDo you want to export results? (y/n): ")
                    if export.lower() in ['y', 'yes']:
                        self._export_results_menu(combined_results)
                else:
                    print(f"\n{self.colors['yellow']}No hidden hotels detected through review analysis.{self.colors['reset']}")
                    self.search_results = results
            
        except Exception as e:
            print(f"\n{self.colors['red']}Error during advanced analysis: {str(e)}{self.colors['reset']}")
            traceback.print_exc()
        
        input("\nPress Enter to return...")
    
    def discover_from_reviews(self):
        """Discover hotels from reviews"""
        self.show_not_implemented_message("Discover Hotels from Reviews")
    
    def analyze_commercial_areas(self):
        """Analyze commercial areas for hotels"""
        self.clear_screen()
        print(f"{self.colors['green']}=== Analyze Commercial Areas for Hotels ==={self.colors['reset']}")
        
        city = input("City name: ")
        if not city:
            print(f"{self.colors['red']}No city entered!{self.colors['reset']}")
            input("\nPress Enter to return...")
            return
        
        radius = input("Search radius in meters (default: 5000): ")
        radius = int(radius) if radius.isdigit() else 5000
        
        print("\nAnalyzing commercial areas (this may take some time)...")
        
        # Initialize SmartLocationAnalyzer if needed
        if not self._init_component("smart_location_analyzer"):
            print(f"\n{self.colors['red']}Error initializing Smart Location Analyzer.{self.colors['reset']}")
            input("\nPress Enter to return...")
            return
        
        try:
            results = self.smart_location_analyzer.analyze_commercial_areas(city, radius)
            
            if results:
                print(f"\n{self.colors['green']}Found {len(results)} potential hotels in commercial areas{self.colors['reset']}")
                self._display_discovery_results(results)
                self.search_results = results
                
                # Save to database if available
                if self.db and hasattr(self.db, 'save_hotels'):
                    save = input("\nDo you want to save results to database? (y/n): ")
                    if save.lower() in ['y', 'yes']:
                        saved = self.db.save_hotels(results)
                        print(f"\n{self.colors['green']}Saved {saved} establishments to database{self.colors['reset']}")
                
                # Offer to export data
                export = input("\nDo you want to export results? (y/n): ")
                if export.lower() in ['y', 'yes']:
                    self._export_results_menu(results)
            else:
                print(f"\n{self.colors['yellow']}No hotels found in commercial areas!{self.colors['reset']}")
            
        except Exception as e:
            print(f"\n{self.colors['red']}Error analyzing commercial areas: {str(e)}{self.colors['reset']}")
            traceback.print_exc()
        
        input("\nPress Enter to return...")
    
    def integrated_discovery(self):
        """Integrated multi-source discovery"""
        self.clear_screen()
        print(f"{self.colors['green']}=== Integrated Multi-Source Discovery ==={self.colors['reset']}")
        print("This function searches for hotels using multiple discovery methods in one operation.")
        
        location = input("Location (city, area, or coordinates): ")
        if not location:
            print(f"{self.colors['red']}No location entered!{self.colors['reset']}")
            input("\nPress Enter to return...")
            return
        
        radius = input("Search radius in meters (default: 5000): ")
        radius = int(radius) if radius.isdigit() else 5000
        
        print("\nSelect discovery methods to use:")
        print("1. Google Maps Analysis [Y/n]")
        print("2. Booking Platforms Search [Y/n]")
        print("3. Smart Location Analysis [Y/n]")
        print("4. Review Analysis [Y/n]")
        print("5. Pattern Discovery [Y/n]")
        
        # Get options
        use_google = input("Use Google Maps? [Y/n]: ").lower() != 'n'
        use_booking = input("Use Booking Platforms? [Y/n]: ").lower() != 'n'
        use_location = input("Use Smart Location Analysis? [Y/n]: ").lower() != 'n'
        use_reviews = input("Use Review Analysis? [Y/n]: ").lower() != 'n'
        use_patterns = input("Use Pattern Discovery? [Y/n]: ").lower() != 'n'
        
        options = {
            "use_google_maps": use_google,
            "use_booking_platforms": use_booking,
            "use_location_analysis": use_location,
            "analyze_reviews": use_reviews,
            "find_patterns": use_patterns
        }
        
        print("\nStarting integrated discovery (this may take some time)...")
        
        # Initialize integration component
        if not self._init_component("discovery_integration"):
            print(f"\n{self.colors['red']}Error initializing Discovery Integration.{self.colors['reset']}")
            input("\nPress Enter to return...")
            return
        
        try:
            result = self.discovery_integration.comprehensive_search(location, radius, options)
            
            if result.get("status") == "success":
                hotels = result.get("hotels", [])
                print(f"\n{self.colors['green']}Found {len(hotels)} establishments{self.colors['reset']}")
                
                # Display results
                self._display_discovery_results(hotels)
                self.search_results = hotels
                
                # If pattern analysis was performed, show summary
                if use_patterns and result.get("pattern_analysis"):
                    patterns = result["pattern_analysis"]
                    if patterns.get("groups"):
                        print(f"\n{self.colors['cyan']}Found {len(patterns['groups'])} property groups{self.colors['reset']}")
                        for i, group in enumerate(patterns["groups"][:3]):
                            print(f"Group {i+1}: {group['property_count']} properties")
                
                # Save to database if available
                if self.db and hasattr(self.db, 'save_hotels'):
                    save = input("\nDo you want to save results to database? (y/n): ")
                    if save.lower() in ['y', 'yes']:
                        saved = self.db.save_hotels(hotels)
                        print(f"\n{self.colors['green']}Saved {saved} establishments to database{self.colors['reset']}")
                
                # Offer to export data
                export = input("\nDo you want to export results? (y/n): ")
                if export.lower() in ['y', 'yes']:
                    self._export_results_menu(hotels)
            else:
                print(f"\n{self.colors['red']}Discovery failed: {result.get('error', 'Unknown error')}{self.colors['reset']}")
            
        except Exception as e:
            print(f"\n{self.colors['red']}Error during integrated discovery: {str(e)}{self.colors['reset']}")
            traceback.print_exc()
        
        input("\nPress Enter to return...")
    
    # Search Menu Functions
    def advanced_search(self):
        """Advanced multi-criteria search"""
        self.show_not_implemented_message("Advanced Multi-criteria Search")
    
    def search_by_name(self):
        """Search for specific hotel by name"""
        self.show_not_implemented_message("Search by Name")
    
    def search_sparse_info(self):
        """Search for hotels with sparse information"""
        self.show_not_implemented_message("Search for Hotels with Sparse Information")
    
    def search_same_owner(self):
        """Search for hotels with same owner/operator"""
        self.show_not_implemented_message("Search for Hotels with Same Owner")
    
    def search_nearby(self):
        """Search for geographically close hotels"""
        self.show_not_implemented_message("Search for Nearby Hotels")
    
    def search_similar(self):
        """Search for similar hotels"""
        self.show_not_implemented_message("Search for Similar Hotels")
    
    def search_unverified(self):
        """Search for unverified hotels"""
        self.show_not_implemented_message("Search for Unverified Hotels")
    
    def search_recent_changes(self):
        """Search for hotels with recent changes"""
        self.show_not_implemented_message("Search for Hotels with Recent Changes")
    
    def view_hotel_details(self):
        """View detailed information for a specific hotel"""
        self.show_not_implemented_message("View Hotel Details")
    
    def discover_hotel_patterns(self):
        """Discover hotel patterns and groups"""
        self.clear_screen()
        print(f"{self.colors['green']}=== Discover Hotel Patterns and Groups ==={self.colors['reset']}")
        
        if not self.search_results:
            print(f"{self.colors['yellow']}No search results available. Please perform a search first.{self.colors['reset']}")
            input("\nPress Enter to return...")
            return
        
        print(f"Analyzing patterns among {len(self.search_results)} establishments...")
        
        # Initialize PatternDiscovery if needed
        if not self._init_component("pattern_discovery"):
            print(f"\n{self.colors['red']}Error initializing Pattern Discovery.{self.colors['reset']}")
            input("\nPress Enter to return...")
            return
        
        try:
            similarity = input("Similarity threshold (0.1-0.9, default: 0.7): ")
            try:
                similarity = float(similarity)
                if similarity < 0.1 or similarity > 0.9:
                    similarity = 0.7
            except:
                similarity = 0.7
            
            # Find property groups
            result = self.pattern_discovery.find_property_groups(self.search_results, similarity_threshold=similarity)
            
            if result.get("status") == "success":
                groups = result.get("groups", [])
                print(f"\n{self.colors['green']}Found {len(groups)} property groups{self.colors['reset']}")
                
                # Display groups
                for i, group in enumerate(groups[:5]):  # Show top 5 groups
                    properties = group.get("properties", [])
                    
                    print(f"\n{self.colors['cyan']}=== Group {i+1} - {len(properties)} properties ==={self.colors['reset']}")
                    
                    # Show common characteristics
                    if group.get("common_name_words"):
                        print(f"Common words in names: {', '.join(group['common_name_words'])}")
                    
                    if group.get("common_owners"):
                        print(f"Common owners: {', '.join(group['common_owners'])}")
                    
                    if group.get("geographic_spread_km"):
                        print(f"Geographic spread: {group['geographic_spread_km']:.2f} km")
                    
                    # List properties in group
                    print("\nProperties in this group:")
                    for j, prop in enumerate(properties[:3]):
                        name = prop.get('name', 'Unnamed')
                        address = prop.get('address', 'No address')
                        print(f"{j+1}. {name} - {address}")
                    
                    if len(properties) > 3:
                        print(f"   ...and {len(properties)-3} more.")
                
                if len(groups) > 5:
                    print(f"\n...and {len(groups)-5} more groups.")
                
                # Offer to export findings
                export = input("\nDo you want to export pattern analysis? (y/n): ")
                if export.lower() in ['y', 'yes']:
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    output_path = f"exports/pattern_analysis_{timestamp}.json"
                    
                    os.makedirs("exports", exist_ok=True)
                    with open(output_path, 'w', encoding='utf-8') as f:
                        json.dump(result, f, indent=2, default=str)
                    
                    print(f"\n{self.colors['green']}Pattern analysis exported to {output_path}{self.colors['reset']}")
            else:
                print(f"\n{self.colors['red']}Pattern analysis failed: {result.get('error', 'Unknown error')}{self.colors['reset']}")
            
        except Exception as e:
            print(f"\n{self.colors['red']}Error during pattern analysis: {str(e)}{self.colors['reset']}")
            traceback.print_exc()
        
        input("\nPress Enter to return...")
    
    def find_by_review_keywords(self):
        """Find hotels by review keywords"""
        self.show_not_implemented_message("Find Hotels by Review Keywords")
    
    # Temporal Analysis Menu Functions
    def analyze_hotel_history(self):
        """Analyze history of a specific hotel"""
        self.show_not_implemented_message("Analyze Hotel History")
    
    def search_historical_changes(self):
        """Search for historical changes of hotels"""
        self.show_not_implemented_message("Search for Historical Changes")
    
    def detect_owner_changes(self):
        """Detect changes in owners/operators"""
        self.show_not_implemented_message("Detect Owner Changes")
    
    def track_platform_transitions(self):
        """Track hotel transitions between platforms"""
        self.show_not_implemented_message("Track Platform Transitions")
    
    def compare_hotels_history(self):
        """Historical comparison between two hotels"""
        self.show_not_implemented_message("Compare Hotels History")
    
    def analyze_location_changes(self):
        """Analyze changes at the same geographic location"""
        self.show_not_implemented_message("Analyze Location Changes")
    
    def extract_from_archive(self):
        """Extract information from internet archive"""
        self.show_not_implemented_message("Extract from Internet Archive")
    
    def analyze_permits_history(self):
        """Analyze historical record of permits and licenses"""
        self.show_not_implemented_message("Analyze Permits History")
    
    def historical_changes_report(self):
        """Comprehensive report on historical changes"""
        self.show_not_implemented_message("Historical Changes Report")
    
    # Violations Analysis Menu Functions
    def analyze_violations(self):
        """Comprehensive violations analysis"""
        self.show_not_implemented_message("Analyze Violations")
    
    def check_inconsistencies(self):
        """Check for data inconsistencies between sources"""
        self.show_not_implemented_message("Check Inconsistencies")
    
    def detect_dual_activity(self):
        """Detect hotels with dual activity"""
        self.show_not_implemented_message("Detect Dual Activity")
    
    def analyze_unofficial(self):
        """Analyze hotels not in official records"""
        self.show_not_implemented_message("Analyze Unofficial Hotels")
    
    def detect_unusual_clusters(self):
        """Detect unusual hotel clusters"""
        self.show_not_implemented_message("Detect Unusual Clusters")
    
    def analyze_evasion_patterns(self):
        """Analyze known evasion patterns"""
        self.show_not_implemented_message("Analyze Evasion Patterns")
    
    def check_residential_hotels(self):
        """Check hotels located in residential areas"""
        self.show_not_implemented_message("Check Residential Hotels")
    
    def list_high_risk(self):
        """List of high-risk hotels"""
        self.show_not_implemented_message("List High-Risk Hotels")
    
    def create_violations_report(self):
        """Create detailed violations report"""
        self.show_not_implemented_message("Create Violations Report")
    
    # Export Menu Functions
    def export_excel(self):
        """Export results in Excel format"""
        self.show_not_implemented_message("Export to Excel")
    
    def export_csv(self):
        """Export results in CSV format"""
        self.show_not_implemented_message("Export to CSV")
    
    def export_json(self):
        """Export results in JSON format"""
        self.show_not_implemented_message("Export to JSON")
    
    def export_violations_report(self):
        """Export potential violations report"""
        self.show_not_implemented_message("Export Violations Report")
    
    def export_timeline(self):
        """Export timeline chart"""
        self.show_not_implemented_message("Export Timeline")
    
    def export_historical_analysis(self):
        """Export historical analysis data"""
        self.show_not_implemented_message("Export Historical Analysis")
    
    def export_map(self):
        """Export hotel data on map (HTML)"""
        self.show_not_implemented_message("Export Map")
    
    def export_statistics(self):
        """Export statistical summary"""
        self.show_not_implemented_message("Export Statistics")
    
    def create_custom_report(self):
        """Create custom report"""
        self.show_not_implemented_message("Create Custom Report")
    
    # Database Management Menu Functions
    def view_database_info(self):
        """View database information"""
        self.show_not_implemented_message("View Database Info")
    
    def add_hotel_manually(self):
        """Add hotel manually"""
        self.show_not_implemented_message("Add Hotel Manually")
    
    def update_specific_hotel(self):
        """Update a specific hotel"""
        self.show_not_implemented_message("Update Specific Hotel")
    
    def delete_hotel(self):
        """Delete hotel from database"""
        self.show_not_implemented_message("Delete Hotel")
    
    def merge_duplicates(self):
        """Merge duplicate records"""
        self.show_not_implemented_message("Merge Duplicates")
    
    def clean_invalid_data(self):
        """Clean invalid data"""
        self.show_not_implemented_message("Clean Invalid Data")
    
    def rebuild_indexes(self):
        """Rebuild database indexes"""
        self.show_not_implemented_message("Rebuild Indexes")
    
    def backup_database(self):
        """Backup database"""
        self.show_not_implemented_message("Backup Database")
    
    def restore_database(self):
        """Restore database from backup"""
        self.show_not_implemented_message("Restore Database")
    
    # Statistics Menu Functions
    def comprehensive_statistics(self):
        """Comprehensive statistical report"""
        self.show_not_implemented_message("Comprehensive Statistics")
    
    def geographic_distribution(self):
        """Hotel distribution by geographic areas"""
        self.show_not_implemented_message("Geographic Distribution")
    
    def source_statistics(self):
        """Source statistics and coverage rates"""
        self.show_not_implemented_message("Source Statistics")
    
    def violations_trends(self):
        """Potential violations trend analysis"""
        self.show_not_implemented_message("Violations Trends")
    
    def period_comparison(self):
        """Data comparison with previous periods"""
        self.show_not_implemented_message("Period Comparison")
    
    def classification_statistics(self):
        """Hotel statistics by classification"""
        self.show_not_implemented_message("Classification Statistics")
    
    def activity_report(self):
        """Activity and changes report"""
        self.show_not_implemented_message("Activity Report")
    
    def platform_report(self):
        """Booking and rental platform report"""
        self.show_not_implemented_message("Platform Report")
    
    def custom_statistics_report(self):
        """Custom report"""
        self.show_not_implemented_message("Custom Statistics Report")
    
    # Settings Menu Functions
    def data_source_settings(self):
        """Data sources and API settings"""
        self.show_not_implemented_message("Data Source Settings")
    
    def matching_settings(self):
        """Matching and comparison settings"""
        self.show_not_implemented_message("Matching Settings")
    
    def historical_settings(self):
        """Historical analysis settings"""
        self.show_not_implemented_message("Historical Settings")
    
    def risk_settings(self):
        """Risk assessment settings"""
        self.show_not_implemented_message("Risk Settings")
    
    def detection_settings(self):
        """Suspicious hotel detection criteria"""
        self.show_not_implemented_message("Detection Settings")
    
    def violations_settings(self):
        """Violations detector settings"""
        self.show_not_implemented_message("Violations Settings")
    
    def database_settings(self):
        """Database settings"""
        self.show_not_implemented_message("Database Settings")
    
    def export_settings(self):
        """Data export settings"""
        self.show_not_implemented_message("Export Settings")
    
    def restore_default_settings(self):
        """Restore default settings"""
        self.show_not_implemented_message("Restore Default Settings")
    
    def show_not_implemented_message(self, feature_name):
        """
        Show a message for features not yet implemented
        
        Args:
            feature_name (str): Name of the not implemented feature
        """
        self.clear_screen()
        print(f"{self.colors['yellow']}Feature '{feature_name}' is not implemented yet.{self.colors['reset']}")
        print("\nThis functionality will be available in future versions of Map_researcher.")
        print("\nPress Enter to return...")
        input()

# If run directly, create and run the menu system
if __name__ == "__main__":
    menu = MenuSystem()
    menu.run()