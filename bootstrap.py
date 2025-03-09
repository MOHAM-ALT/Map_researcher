# -*- coding: utf-8 -*-
"""
Map_researcher0.3V Bootstrap Module

This module initializes the application, checks for required dependencies,
and provides fallbacks where needed.
"""

import os
import sys
import logging
import importlib
from datetime import datetime

# Create necessary directories
os.makedirs('logs', exist_ok=True)
os.makedirs('data', exist_ok=True)
os.makedirs('exports', exist_ok=True)
os.makedirs('config', exist_ok=True)
os.makedirs('maps', exist_ok=True)

# Setup logging
try:
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler("logs/hotel_researcher.log", encoding='utf-8'),
            logging.StreamHandler()
        ]
    )
except Exception as e:
    print(f"Error setting up logging: {e}")
    logging.basicConfig(level=logging.INFO)

logger = logging.getLogger("bootstrap")
logger.info("Starting Map_researcher0.3V")

# Try to import fallbacks module
try:
    import fallbacks
    logger.info("Fallbacks module loaded successfully")
    has_fallbacks = True
except ImportError:
    logger.warning("Fallbacks module not found, will try direct imports")
    has_fallbacks = False

# Check for required libraries and use fallbacks if needed
def safe_import(module_name, fallback_message=None):
    """Try to import a module safely, with fallback handling"""
    try:
        return importlib.import_module(module_name)
    except ImportError:
        if fallback_message:
            logger.warning(f"Failed to import {module_name}: {fallback_message}")
        else:
            logger.warning(f"Failed to import {module_name}")
        return None

# Check and patch libraries
libraries_status = {}

# Check for tabulate
tabulate_module = safe_import('tabulate', "Table formatting will be basic")
libraries_status['tabulate'] = tabulate_module is not None

# Check for rich components
rich_module = safe_import('rich', "Rich UI features will be limited")
libraries_status['rich'] = rich_module is not None

if libraries_status['rich']:
    try:
        from rich.console import Console
        from rich.table import Table
        from rich.progress import track
        libraries_status['rich_components'] = True
    except ImportError:
        libraries_status['rich_components'] = False
        logger.warning("Rich components not fully available")
else:
    libraries_status['rich_components'] = False

# Check for folium
folium_module = safe_import('folium', "Map visualization will not be available")
libraries_status['folium'] = folium_module is not None

# Check for pandas
pandas_module = safe_import('pandas', "Data handling will be limited")
libraries_status['pandas'] = pandas_module is not None

# Check for database support
psycopg2_module = safe_import('psycopg2', "PostgreSQL support not available")
libraries_status['postgresql'] = psycopg2_module is not None

# Print bootstrap summary
def print_status_table():
    """Print a summary table of library status"""
    try:
        from rich.console import Console
        from rich.table import Table
        
        console = Console()
        table = Table(title="Map_researcher0.3V System Status")
        
        table.add_column("Component", style="cyan")
        table.add_column("Status", style="green")
        
        for lib, status in libraries_status.items():
            status_text = "[green]Available[/green]" if status else "[yellow]Limited[/yellow]"
            table.add_row(lib, status_text)
        
        console.print(table)
    except:
        # Fallback to simple printing
        print("\nSystem Status:")
        print("-------------")
        for lib, status in libraries_status.items():
            status_text = "Available" if status else "Limited"
            print(f"- {lib}: {status_text}")

# Apply patches if needed
def apply_patches():
    """Apply any necessary patches to make the application work with limited libraries"""
    if not has_fallbacks:
        logger.info("No fallbacks module, checking for individual fallbacks")
        
        # If rich is not available, create basic Console and Table classes
        if not libraries_status['rich']:
            logger.info("Creating rich fallbacks")
            
            class Console:
                def print(self, *args, **kwargs):
                    print(*args)
            
            class Table:
                def __init__(self, title=None):
                    self.title = title
                    self.rows = []
                    self.headers = []
                
                def add_column(self, header, *args, **kwargs):
                    self.headers.append(header)
                
                def add_row(self, *cells):
                    self.rows.append(cells)
            
            def track(iterable, description=None, total=None):
                if description:
                    print(description)
                return iterable
            
            # Add to modules
            sys.modules['rich.console'] = type('console', (), {'Console': Console})
            sys.modules['rich.table'] = type('table', (), {'Table': Table})
            sys.modules['rich.progress'] = type('progress', (), {'track': track})
    
    logger.info("Patches applied successfully")

# Run the bootstrap process
def run_bootstrap():
    """Run the main bootstrap process"""
    print("\n" + "="*60)
    print("  Map_researcher0.3V - System Bootstrap  ")
    print("="*60)
    
    # Show status
    print_status_table()
    
    # Apply patches
    apply_patches()
    
    print("\nBootstrap complete. System is ready.")
    print("="*60 + "\n")
    
    return libraries_status

# Run bootstrap if this module is executed directly
if __name__ == "__main__":
    run_bootstrap()