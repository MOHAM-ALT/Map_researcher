# -*- coding: utf-8 -*-
"""
Map_researcher0.3V Package Initialization
"""

# Ensure fallbacks are initialized first
try:
    import fallbacks
except ImportError:
    pass

# Try to run bootstrap
try:
    import bootstrap
    bootstrap.run_bootstrap()
except ImportError:
    pass