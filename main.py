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

# Import all existing libraries with fallbacks
# (existing code for importing tabulate, rich, folium, etc.)

# ================================================
# 3. IMPORT APPLICATION MODULES
# ================================================
# Import the menu system
try:
    from menu_system import MenuSystem
    MENU_SYSTEM_AVAILABLE = True
except ImportError:
    print(f"{Fore.YELLOW}Error: menu_system module not found. Menu interface will not be available.{Style.RESET_ALL}")
    print("Please ensure the menu_system.py file is in the same directory.")
    MENU_SYSTEM_AVAILABLE = False

# Import the data discovery module
try:
    from data_discovery import DataDiscovery
    DATA_DISCOVERY_AVAILABLE = True
except ImportError:
    print(f"{Fore.YELLOW}Warning: data_discovery module not found. Discovery features will be limited.{Style.RESET_ALL}")
    DATA_DISCOVERY_AVAILABLE = False

# Import the search module
try:
    from search_module import SearchModule
    SEARCH_MODULE_AVAILABLE = True
except ImportError:
    print(f"{Fore.YELLOW}Warning: search_module module not found. Search features will be limited.{Style.RESET_ALL}")
    SEARCH_MODULE_AVAILABLE = False

# Import the temporal analysis module
try:
    from temporal_analysis import TemporalAnalysis
    TEMPORAL_ANALYSIS_AVAILABLE = True
except ImportError:
    print(f"{Fore.YELLOW}Warning: temporal_analysis module not found. Temporal analysis features will be limited.{Style.RESET_ALL}")
    TEMPORAL_ANALYSIS_AVAILABLE = False

# Import the violation detection module
try:
    from violation_detection import ViolationDetection
    VIOLATION_DETECTION_AVAILABLE = True
except ImportError:
    print(f"{Fore.YELLOW}Warning: violation_detection module not found. Violation detection features will be limited.{Style.RESET_ALL}")
    VIOLATION_DETECTION_AVAILABLE = False

# ================================================
# 4. MAIN APPLICATION CLASS
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
        self.scraper = None
        self.menu = None
        self.data_discovery = None
        self.search_module = None
        self.temporal_analysis = None
        self.violation_detection = None
        
        # Initialize modules
        self._init_modules()
    
    def _init_modules(self):
        """Initialize application modules"""
        # Initialize database
        try:
            from Database import Database
            db_type = self.args.get('db_type', 'sqlite')
            db_path = self.args.get('db_path', 'data/hotels.db')
            self.db = Database(db_type, db_path)
            print(f"{Fore.GREEN}Database initialized: {db_type} at {db_path}{Style.RESET_ALL}")
        except Exception as e:
            print(f"{Fore.RED}Error initializing database: {e}{Style.RESET_ALL}")
            import traceback
            traceback.print_exc()
        
        # Initialize scraper
        try:
            from HotelScraper import HotelScraper
            api_keys = {}
            if os.path.exists('config/config.json'):
                try:
                    import json
                    with open('config/config.json', 'r') as f:
                        config = json.load(f)
                        api_keys = config.get('api_keys', {})
                except Exception as e:
                    print(f"{Fore.YELLOW}Error loading API keys: {e}{Style.RESET_ALL}")
            
            self.scraper = HotelScraper(api_keys)
            print(f"{Fore.GREEN}Hotel scraper initialized{Style.RESET_ALL}")
        except Exception as e:
            print(f"{Fore.RED}Error initializing hotel scraper: {e}{Style.RESET_ALL}")
            import traceback
            traceback.print_exc()
        
        # Initialize data discovery
        if DATA_DISCOVERY_AVAILABLE:
            try:
                self.data_discovery = DataDiscovery(self.db, self.scraper)
                print(f"{Fore.GREEN}Data discovery module initialized{Style.RESET_ALL}")
            except Exception as e:
                print(f"{Fore.RED}Error initializing data discovery module: {e}{Style.RESET_ALL}")
                import traceback
                traceback.print_exc()
        
        # Initialize search module
        if SEARCH_MODULE_AVAILABLE:
            try:
                self.search_module = SearchModule(self.db, self.scraper)
                print(f"{Fore.GREEN}Search module initialized{Style.RESET_ALL}")
            except Exception as e:
                print(f"{Fore.RED}Error initializing search module: {e}{Style.RESET_ALL}")
                import traceback
                traceback.print_exc()
        
        # Initialize temporal analysis
        if TEMPORAL_ANALYSIS_AVAILABLE:
            try:
                self.temporal_analysis = TemporalAnalysis(self.db, self.scraper)
                print(f"{Fore.GREEN}Temporal analysis module initialized{Style.RESET_ALL}")
            except Exception as e:
                print(f"{Fore.RED}Error initializing temporal analysis module: {e}{Style.RESET_ALL}")
                import traceback
                traceback.print_exc()
        
        # Initialize violation detection
        if VIOLATION_DETECTION_AVAILABLE:
            try:
                self.violation_detection = ViolationDetection(self.db, self.scraper)
                print(f"{Fore.GREEN}Violation detection module initialized{Style.RESET_ALL}")
            except Exception as e:
                print(f"{Fore.RED}Error initializing violation detection module: {e}{Style.RESET_ALL}")
                import traceback
                traceback.print_exc()
    
    def run(self):
        """Run the application"""
        # Display welcome message
        self._display_welcome()
        
        # Run menu system if available
        if MENU_SYSTEM_AVAILABLE:
            try:
                self.menu = MenuSystem(self.db, self.scraper)
                
                # Connect modules to menu system if available
                if self.data_discovery:
                    setattr(self.menu, 'data_discovery', self.data_discovery)
                
                if self.search_module:
                    setattr(self.menu, 'search_module', self.search_module)
                
                if self.temporal_analysis:
                    setattr(self.menu, 'temporal_analysis', self.temporal_analysis)
                
                if self.violation_detection:
                    setattr(self.menu, 'violation_detection', self.violation_detection)
                
                self.menu.run()
            except Exception as e:
                print(f"{Fore.RED}Error in menu system: {e}{Style.RESET_ALL}")
                import traceback
                traceback.print_exc()
        else:
            print(f"{Fore.YELLOW}Menu system not available. Using legacy interface.{Style.RESET_ALL}")
            self._run_legacy_interface()
    
    def _display_welcome(self):
        """Display welcome message"""
        print("\n" + "="*60)
        print(f"{Fore.BLUE}{Style.BRIGHT}  Map_researcher 0.4 - Hotel Data Collection Tool  {Style.RESET_ALL}")
        print("="*60)
        print("Initializing application components...")
    
    def _run_legacy_interface(self):
        """Run legacy command-line interface"""
        # This will be used if the menu system is not available
        # Implement a simplified version of the existing interface
        print(f"{Fore.YELLOW}Using legacy interface. New features may not be available.{Style.RESET_ALL}")
        
        # Create CLI instance from the old code
        try:
            from CommandLineInterface import CommandLineInterface
            cli = CommandLineInterface(self.db, self.scraper)
            cli.main_menu()
        except Exception as e:
            print(f"{Fore.RED}Error in legacy interface: {e}{Style.RESET_ALL}")
            import traceback
            traceback.print_exc()
            print("\nApplication could not start. Press Enter to exit...")
            input()
# ================================================
# 5. COMMAND-LINE ARGUMENT HANDLING
# ================================================
def parse_arguments():
    """Parse command-line arguments"""
    parser = argparse.ArgumentParser(description='Map_researcher 0.4 - Hotel data collection and analysis application')
    
    # Add arguments
    parser.add_argument('--db-type', dest='db_type', default='sqlite',
                      help='Database type (sqlite, postgresql, csv, excel)')
    parser.add_argument('--db-path', dest='db_path', default='data/hotels.db',
                      help='Database path or connection string')
    parser.add_argument('--config', dest='config_path', default='config/config.json',
                      help='Path to configuration file')
    parser.add_argument('--legacy', dest='use_legacy', action='store_true',
                      help='Use legacy interface instead of menu system')
    parser.add_argument('--debug', dest='debug_mode', action='store_true',
                      help='Enable debug mode')
    
    return parser.parse_args()

# ================================================
# 6. MAIN FUNCTION
# ================================================
def main():
    """Main application entry point"""
    # Parse command-line arguments
    args = parse_arguments()
    
    # Set up logging level based on debug flag
    if args.debug_mode:
        logging.getLogger().setLevel(logging.DEBUG)
        print(f"{Fore.YELLOW}Debug mode enabled{Style.RESET_ALL}")
    
    # Create and run application
    try:
        app = MapResearcher(vars(args))
        
        # Force legacy interface if requested
        if args.use_legacy:
            global MENU_SYSTEM_AVAILABLE
            MENU_SYSTEM_AVAILABLE = False
        
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