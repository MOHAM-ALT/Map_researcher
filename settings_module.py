#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Settings Module for Map_researcher 0.4
This module provides functionality for managing application settings.
"""

import os
import sys
import json
import logging
from typing import Dict, Any, Optional

# Initialize logger
logger = logging.getLogger("settings_module")

class SettingsModule:
    """Class for managing application settings"""
    
    def __init__(self, config_path: str = "config/config.json"):
        """
        Initialize the settings module
        
        Args:
            config_path: Path to the configuration file
        """
        self.config_path = config_path
        self.settings = {}
        self.default_settings = self._get_default_settings()
        
        # Load settings
        self.load_settings()
    
    def load_settings(self) -> Dict:
        """
        Load settings from configuration file
        
        Returns:
            Dict: Loaded settings
        """
        try:
            # Create config directory if it doesn't exist
            os.makedirs(os.path.dirname(self.config_path), exist_ok=True)
            
            # Load existing settings if file exists
            if os.path.exists(self.config_path):
                with open(self.config_path, 'r', encoding='utf-8') as f:
                    self.settings = json.load(f)
                logger.info(f"Settings loaded from {self.config_path}")
            else:
                # Create default settings file
                self.settings = self.default_settings
                self.save_settings()
                logger.info(f"Default settings created at {self.config_path}")
            
            return self.settings
        
        except Exception as e:
            logger.error(f"Error loading settings: {e}")
            # Use default settings if loading fails
            self.settings = self.default_settings
            return self.settings
    
    def save_settings(self) -> bool:
        """
        Save settings to configuration file
        
        Returns:
            bool: Success status
        """
        try:
            # Create config directory if it doesn't exist
            os.makedirs(os.path.dirname(self.config_path), exist_ok=True)
            
            # Save settings
            with open(self.config_path, 'w', encoding='utf-8') as f:
                json.dump(self.settings, f, ensure_ascii=False, indent=2)
            
            logger.info(f"Settings saved to {self.config_path}")
            return True
        
        except Exception as e:
            logger.error(f"Error saving settings: {e}")
            return False
    
    def get_setting(self, section: str, key: str, default: Any = None) -> Any:
        """
        Get a specific setting value
        
        Args:
            section: Settings section
            key: Setting key
            default: Default value if setting not found
            
        Returns:
            Any: Setting value
        """
        try:
            return self.settings.get(section, {}).get(key, default)
        except Exception as e:
            logger.error(f"Error getting setting {section}.{key}: {e}")
            return default
    
    def set_setting(self, section: str, key: str, value: Any) -> bool:
        """
        Set a specific setting value
        
        Args:
            section: Settings section
            key: Setting key
            value: Setting value
            
        Returns:
            bool: Success status
        """
        try:
            # Create section if it doesn't exist
            if section not in self.settings:
                self.settings[section] = {}
            
            # Set value
            self.settings[section][key] = value
            
            # Save settings
            return self.save_settings()
        
        except Exception as e:
            logger.error(f"Error setting {section}.{key}: {e}")
            return False
    
    def get_section(self, section: str) -> Dict:
        """
        Get all settings in a section
        
        Args:
            section: Settings section
            
        Returns:
            Dict: Section settings
        """
        return self.settings.get(section, {})
    
    def update_section(self, section: str, values: Dict) -> bool:
        """
        Update multiple settings in a section
        
        Args:
            section: Settings section
            values: Dictionary of setting values
            
        Returns:
            bool: Success status
        """
        try:
            # Create section if it doesn't exist
            if section not in self.settings:
                self.settings[section] = {}
            
            # Update values
            self.settings[section].update(values)
            
            # Save settings
            return self.save_settings()
        
        except Exception as e:
            logger.error(f"Error updating section {section}: {e}")
            return False
    
    def reset_section(self, section: str) -> bool:
        """
        Reset a section to default settings
        
        Args:
            section: Settings section
            
        Returns:
            bool: Success status
        """
        try:
            # Get default section settings
            default_section = self.default_settings.get(section, {})
            
            # Reset section
            self.settings[section] = default_section
            
            # Save settings
            return self.save_settings()
        
        except Exception as e:
            logger.error(f"Error resetting section {section}: {e}")
            return False
    
    def reset_all_settings(self) -> bool:
        """
        Reset all settings to defaults
        
        Returns:
            bool: Success status
        """
        try:
            # Reset all settings
            self.settings = self.default_settings.copy()
            
            # Save settings
            return self.save_settings()
        
        except Exception as e:
            logger.error(f"Error resetting all settings: {e}")
            return False
    
    def _get_default_settings(self) -> Dict:
        """
        Get default application settings
        
        Returns:
            Dict: Default settings
        """
        return {
            "api_keys": {
                "google_places": "",
                "openstreetmap": "",
                "booking_com": "",
                "tripadvisor": ""
            },
            "database": {
                "type": "sqlite",
                "path": "data/hotels.db",
                "backup_enabled": True,
                "backup_interval_days": 7
            },
            "discovery": {
                "default_radius": 5000,
                "default_language": "en",
                "max_results": 500,
                "save_automatically": True,
                "default_sources": ["google_places", "openstreetmap", "booking"]
            },
            "search": {
                "default_similarity_threshold": 0.7,
                "max_results": 100,
                "sort_by": "relevance"
            },
            "temporal": {
                "min_history_days": 365,
                "verification_interval_days": 30,
                "enable_wayback_search": True
            },
            "violations": {
                "min_risk_score": 3,
                "high_risk_threshold": 7,
                "medium_risk_threshold": 3,
                "enable_auto_detection": True
            },
            "export": {
                "default_format": "excel",
                "default_export_path": "exports",
                "include_risk_analysis": True,
                "include_history": True
            },
            "interface": {
                "language": "en",
                "color_scheme": "default",
                "enable_animations": True
            },
            "logging": {
                "level": "INFO",
                "max_log_size_mb": 10,
                "max_log_files": 5
            }
        }
    
    def get_api_keys(self) -> Dict:
        """
        Get all API keys
        
        Returns:
            Dict: API keys
        """
        return self.get_section("api_keys")
    
    def set_api_key(self, provider: str, key: str) -> bool:
        """
        Set an API key
        
        Args:
            provider: API provider
            key: API key
            
        Returns:
            bool: Success status
        """
        return self.set_setting("api_keys", provider, key)
    
    def get_database_settings(self) -> Dict:
        """
        Get database settings
        
        Returns:
            Dict: Database settings
        """
        return self.get_section("database")
    
    def update_database_settings(self, settings: Dict) -> bool:
        """
        Update database settings
        
        Args:
            settings: New database settings
            
        Returns:
            bool: Success status
        """
        return self.update_section("database", settings)
    
    def get_discovery_settings(self) -> Dict:
        """
        Get discovery settings
        
        Returns:
            Dict: Discovery settings
        """
        return self.get_section("discovery")
    
    def update_discovery_settings(self, settings: Dict) -> bool:
        """
        Update discovery settings
        
        Args:
            settings: New discovery settings
            
        Returns:
            bool: Success status
        """
        return self.update_section("discovery", settings)
    
    def get_search_settings(self) -> Dict:
        """
        Get search settings
        
        Returns:
            Dict: Search settings
        """
        return self.get_section("search")
    
    def update_search_settings(self, settings: Dict) -> bool:
        """
        Update search settings
        
        Args:
            settings: New search settings
            
        Returns:
            bool: Success status
        """
        return self.update_section("search", settings)
    
    def get_temporal_settings(self) -> Dict:
        """
        Get temporal analysis settings
        
        Returns:
            Dict: Temporal analysis settings
        """
        return self.get_section("temporal")
    
    def update_temporal_settings(self, settings: Dict) -> bool:
        """
        Update temporal analysis settings
        
        Args:
            settings: New temporal analysis settings
            
        Returns:
            bool: Success status
        """
        return self.update_section("temporal", settings)
    
    def get_violations_settings(self) -> Dict:
        """
        Get violations detection settings
        
        Returns:
            Dict: Violations detection settings
        """
        return self.get_section("violations")
    
    def update_violations_settings(self, settings: Dict) -> bool:
        """
        Update violations detection settings
        
        Args:
            settings: New violations detection settings
            
        Returns:
            bool: Success status
        """
        return self.update_section("violations", settings)
    
    def get_export_settings(self) -> Dict:
        """
        Get export settings
        
        Returns:
            Dict: Export settings
        """
        return self.get_section("export")
    
    def update_export_settings(self, settings: Dict) -> bool:
        """
        Update export settings
        
        Args:
            settings: New export settings
            
        Returns:
            bool: Success status
        """
        return self.update_section("export", settings)
    
    def get_interface_settings(self) -> Dict:
        """
        Get interface settings
        
        Returns:
            Dict: Interface settings
        """
        return self.get_section("interface")
    
    def update_interface_settings(self, settings: Dict) -> bool:
        """
        Update interface settings
        
        Args:
            settings: New interface settings
            
        Returns:
            bool: Success status
        """
        return self.update_section("interface", settings)
    
    def get_logging_settings(self) -> Dict:
        """
        Get logging settings
        
        Returns:
            Dict: Logging settings
        """
        return self.get_section("logging")
    
    def update_logging_settings(self, settings: Dict) -> bool:
        """
        Update logging settings
        
        Args:
            settings: New logging settings
            
        Returns:
            bool: Success status
        """
        return self.update_section("logging", settings)

# If run directly, display module info
if __name__ == "__main__":
    print("Settings Module for Map_researcher 0.4")
    print("This module provides functionality for managing application settings.")