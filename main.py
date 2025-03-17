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
import json
from datetime import datetime, timedelta

try:
    # Set excepthook to prevent crashes
    import sys
    def custom_excepthook(exc_type, exc_value, exc_traceback):
        print("*** ERROR: Program stopped due to an error ***")
        print(f"Error type: {exc_type.__name__}")
        print(f"Error message: {exc_value}")
        import traceback
        traceback.print_tb(exc_traceback)
        print("\nPress Enter to exit...")
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

# ================================================
# 3. CONFIG LOADING
# ================================================
def load_config():
    """Load configuration from file"""
    config = {}
    config_path = os.path.join("config", "config.json")
    
    try:
        if os.path.exists(config_path):
            with open(config_path, 'r', encoding='utf-8') as f:
                config = json.load(f)
            print(f"{Fore.GREEN}Configuration loaded from {config_path}{Style.RESET_ALL}")
        else:
            print(f"{Fore.YELLOW}Configuration file {config_path} not found. Using defaults.{Style.RESET_ALL}")
            # Create default config
            config = {
                "api_keys": {
                    "google_maps": "",
                    "openstreetmap": ""
                },
                "search_settings": {
                    "default_radius": 5000,
                    "max_results": 100
                },
                "database": {
                    "type": "sqlite",
                    "path": "data/hotels.db"
                }
            }
            
            # Try to create config directory and save default config
            try:
                os.makedirs("config", exist_ok=True)
                with open(config_path, 'w', encoding='utf-8') as f:
                    json.dump(config, f, indent=2)
                print(f"{Fore.GREEN}Created default configuration file at {config_path}{Style.RESET_ALL}")
            except:
                print(f"{Fore.RED}Could not create default configuration file{Style.RESET_ALL}")
    except Exception as e:
        print(f"{Fore.RED}Error loading configuration: {str(e)}{Style.RESET_ALL}")
        config = {}
    
    return config

# ================================================
# 4. DIRECTORY STRUCTURE VERIFICATION
# ================================================
def verify_directory_structure():
    """Verify and create necessary directories"""
    directories = [
        "data",
        "exports",
        "logs",
        "maps",
        "models",
        "config"
    ]
    
    for directory in directories:
        os.makedirs(directory, exist_ok=True)
    
    # Also verify module directories
    module_dirs = [
        "web_discovery",
        "location_analysis",
        "machine_learning",
        "analysis",
        "reviews_analysis",
        "visualization"
    ]
    
    for directory in module_dirs:
        if not os.path.exists(directory):
            print(f"{Fore.YELLOW}Warning: Module directory '{directory}' not found.{Style.RESET_ALL}")
            print(f"This may indicate missing components. Some features might not work.")

# ================================================
# 5. MAIN APPLICATION CLASS
# ================================================
class MapResearcher:
    """Main application class for Map_researcher"""
    
    def __init__(self, args=None):
        """
        Initialize the application
        
        Args:
            args: Command-line arguments
        """
        self.args = args or {}
        self.db = None
        self.config = load_config()
        self.menu = None
        
        # Initialize database
        self._init_database()
    
    def _init_database(self):
        """Initialize the database connection"""
        try:
            from Database import Database
            
            # Get database settings from config or arguments
            db_type = self.args.get('db_type') or self.config.get('database', {}).get('type', 'sqlite')
            db_path = self.args.get('db_path') or self.config.get('database', {}).get('path', 'data/hotels.db')
            
            self.db = Database(db_type, db_path)
            print(f"{Fore.GREEN}Database initialized: {db_type} at {db_path}{Style.RESET_ALL}")
        except Exception as e:
            print(f"{Fore.RED}Error initializing database: {e}{Style.RESET_ALL}")
            import traceback
            traceback.print_exc()
    
    def run(self):
        """Run the application"""
        # Display welcome message
        self._display_welcome()
        
        # Run menu system
        try:
            from menu_system import MenuSystem
            self.menu = MenuSystem(self.db, self.config)
            self.menu.run()
        except Exception as e:
            print(f"{Fore.RED}Error in menu system: {e}{Style.RESET_ALL}")
            import traceback
            traceback.print_exc()
            print("\nApplication could not start properly. Press Enter to exit...")
            input()
    
    def _display_welcome(self):
        """Display welcome message"""
        print("\n" + "="*60)
        print(f"{Fore.BLUE}{Style.BRIGHT}  Map_researcher 0.5 - Hotel Data Collection Tool  {Style.RESET_ALL}")
        print("="*60)
        print("Initializing application components...")
        
        # Check for API keys
        if not self.config.get('api_keys', {}).get('google_maps'):
            print(f"{Fore.YELLOW}Warning: Google Maps API key not configured.{Style.RESET_ALL}")
            print("Some search features may not work properly.")
            print(f"You can set this in {Fore.CYAN}config/config.json{Style.RESET_ALL}\n")

# ================================================
# 6. COMMAND-LINE ARGUMENT HANDLING
# ================================================
def parse_arguments():
    """Parse command-line arguments"""
    parser = argparse.ArgumentParser(description='Map_researcher 0.5 - Hotel data collection and analysis application')
    
    # Add arguments
    parser.add_argument('--db-type', dest='db_type', default=None,
                      help='Database type (sqlite, postgresql, csv, excel)')
    parser.add_argument('--db-path', dest='db_path', default=None,
                      help='Database path or connection string')
    parser.add_argument('--config', dest='config_path', default='config/config.json',
                      help='Path to configuration file')
    parser.add_argument('--debug', dest='debug_mode', action='store_true',
                      help='Enable debug mode')
    
    return parser.parse_args()

# ================================================
# 7. MAIN FUNCTION
# ================================================
def main():
    """Main application entry point"""
    # Verify directory structure
    verify_directory_structure()
    
    # Parse command-line arguments
    args = parse_arguments()
    
    # Set up logging level based on debug flag
    if args.debug_mode:
        logging.getLogger().setLevel(logging.DEBUG)
        print(f"{Fore.YELLOW}Debug mode enabled{Style.RESET_ALL}")
    else:
        logging.getLogger().setLevel(logging.INFO)
    
    # Create and run application
    try:
        app = MapResearcher(vars(args))
        app.run()
    except Exception as e:
        print(f"{Fore.RED}Error initializing application: {e}{Style.RESET_ALL}")
        import traceback
        traceback.print_exc()
        input("Press Enter to exit...")
    finally:
        # Ensure database connection is closed
        if 'app' in locals() and hasattr(app, 'db') and app.db and hasattr(app.db, 'close'):
            app.db.close()

# Run the main function if executed directly
if __name__ == "__main__":
    main()