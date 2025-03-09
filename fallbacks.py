# -*- coding: utf-8 -*-
"""
Fallbacks for Map_researcher0.3V

This module provides fallback implementations for libraries that might not be installed,
allowing the application to run even with partial installations.
"""

import sys
import os
import logging
from datetime import datetime

# Setup basic logging
try:
    if not os.path.exists('logs'):
        os.makedirs('logs')
    
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

logger = logging.getLogger("fallbacks")

# Dictionary to track which fallbacks are being used
FALLBACKS_USED = {}

def init_fallbacks():
    """Initialize and install necessary fallbacks"""
    logger.info("Initializing fallbacks module")
    
    # Try to import each module, use fallback if not available
    init_tabulate_fallback()
    init_rich_fallback()
    init_folium_fallback()
    init_psycopg2_fallback()
    
    # Log what fallbacks are being used
    for module, is_used in FALLBACKS_USED.items():
        if is_used:
            logger.warning(f"Using fallback for {module}")
        else:
            logger.info(f"Using actual {module} library")
    
    return FALLBACKS_USED

def init_tabulate_fallback():
    """Initialize tabulate fallback if needed"""
    try:
        import tabulate
        FALLBACKS_USED['tabulate'] = False
        return
    except ImportError:
        logger.warning("tabulate not found, using fallback")
        FALLBACKS_USED['tabulate'] = True
        
    # Create tabulate fallback
    class TabulateFallback:
        def __call__(self, data, headers=None, tablefmt="simple"):
            if not data:
                return ""
            
            result = ""
            if headers:
                result += " | ".join([str(h) for h in headers]) + "\n"
                result += "-" * len(result) + "\n"
            
            for row in data:
                result += " | ".join([str(cell) for cell in row]) + "\n"
            
            return result
    
    # Install the fallback
    sys.modules['tabulate'] = type('tabulate', (), {
        '__doc__': 'Fallback tabulate module',
        'tabulate': TabulateFallback()
    })

def init_rich_fallback():
    """Initialize rich fallback if needed"""
    try:
        import rich
        FALLBACKS_USED['rich'] = False
        return
    except ImportError:
        logger.warning("rich not found, using fallback")
        FALLBACKS_USED['rich'] = True
    
    # Create rich module and console
    class Console:
        def __init__(self, *args, **kwargs):
            pass
        
        def print(self, *args, **kwargs):
            if args and hasattr(args[0], '__str__'):
                print(str(args[0]))
            else:
                print(*args)
    
    class Table:
        def __init__(self, *args, **kwargs):
            self.title = kwargs.get('title', '')
            self.rows = []
            self.headers = []
        
        def add_column(self, header, *args, **kwargs):
            self.headers.append(header)
        
        def add_row(self, *cells):
            self.rows.append(cells)
        
        def __str__(self):
            if self.title:
                result = self.title + "\n" + "=" * len(self.title) + "\n\n"
            else:
                result = ""
            
            if self.headers:
                result += " | ".join(self.headers) + "\n"
                result += "-" * len(result) + "\n"
            
            for row in self.rows:
                result += " | ".join([str(cell) for cell in row]) + "\n"
            
            return result
    
    def track(iterable, description=None, total=None):
        if description:
            print(description)
        return iterable
    
    # Create fake rich module
    rich_module = type('rich', (), {
        '__doc__': 'Fallback rich module',
        'Console': Console,
        'Table': Table,
        'track': track
    })
    
    # Create fake rich.console module
    console_module = type('console', (), {
        '__doc__': 'Fallback rich.console module',
        'Console': Console
    })
    
    # Create fake rich.table module
    table_module = type('table', (), {
        '__doc__': 'Fallback rich.table module',
        'Table': Table
    })
    
    # Create fake rich.progress module
    progress_module = type('progress', (), {
        '__doc__': 'Fallback rich.progress module',
        'track': track
    })
    
    # Install all the fallbacks
    sys.modules['rich'] = rich_module
    sys.modules['rich.console'] = console_module
    sys.modules['rich.table'] = table_module
    sys.modules['rich.progress'] = progress_module

def init_folium_fallback():
    """Initialize folium fallback if needed"""
    try:
        import folium
        FALLBACKS_USED['folium'] = False
        return
    except ImportError:
        logger.warning("folium not found, using fallback")
        FALLBACKS_USED['folium'] = True
    
    # Create folium fallbacks
    class Map:
        def __init__(self, location=None, zoom_start=None, **kwargs):
            self.location = location
            self.zoom_start = zoom_start
            self._children = {}
        
        def add_to(self, obj):
            return self
        
        def add_child(self, child, name=None, **kwargs):
            if name is None:
                name = f"child_{len(self._children)}"
            self._children[name] = child
            return self
        
        def save(self, path):
            with open(path, "w") as f:
                f.write("<html><body><h1>Map Unavailable</h1>")
                f.write("<p>Folium library not installed properly.</p>")
                f.write("<p>Please install folium to use map features.</p>")
                f.write("</body></html>")
            print(f"Created placeholder map at {path}")
            return path
    
    class Marker:
        def __init__(self, location=None, popup=None, tooltip=None, icon=None, **kwargs):
            self.location = location
            self.popup = popup
            self.tooltip = tooltip
            self.icon = icon
        
        def add_to(self, obj):
            if hasattr(obj, 'add_child'):
                obj.add_child(self)
            return self
    
    class Icon:
        def __init__(self, color=None, icon=None, prefix=None, **kwargs):
            self.color = color
            self.icon = icon
            self.prefix = prefix
    
    # Create fake folium module
    folium_module = type('folium', (), {
        '__doc__': 'Fallback folium module',
        'Map': Map,
        'Marker': Marker,
        'Icon': Icon
    })
    
    # Install the fallback
    sys.modules['folium'] = folium_module

def init_psycopg2_fallback():
    """Initialize psycopg2 fallback if needed"""
    try:
        import psycopg2
        FALLBACKS_USED['psycopg2'] = False
        return
    except ImportError:
        logger.warning("psycopg2 not found, using fallback")
        FALLBACKS_USED['psycopg2'] = True
    
    # Create exceptions
    class DatabaseError(Exception):
        pass
    
    class OperationalError(DatabaseError):
        pass
    
    class InterfaceError(DatabaseError):
        pass
    
    # Create connection function
    def connect(*args, **kwargs):
        raise DatabaseError("PostgreSQL not available. Please use SQLite instead.")
    
    # Create fake psycopg2 module
    psycopg2_module = type('psycopg2', (), {
        '__doc__': 'Fallback psycopg2 module',
        'connect': connect,
        'DatabaseError': DatabaseError,
        'OperationalError': OperationalError,
        'InterfaceError': InterfaceError
    })
    
    # Install the fallback
    sys.modules['psycopg2'] = psycopg2_module

# Initialize fallbacks when module is imported
init_fallbacks()