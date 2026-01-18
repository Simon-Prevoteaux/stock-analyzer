"""
Flask extensions and shared instances

This module holds the shared database and fetcher instances
that are used across all routes.
"""

import os
import sys

# Add parent directory to path to import libs
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from libs.stock_fetcher import StockFetcher
from libs.database import StockDatabase

# Get absolute path to data directory (project root / data / stocks.db)
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
db_path = os.path.join(project_root, 'data', 'stocks.db')

# Shared instances - initialized once
fetcher = StockFetcher()
db = StockDatabase(db_path)

# FRED API Key getter
_fred_api_key = None


def init_fred_api_key():
    """Initialize FRED API key from environment"""
    global _fred_api_key
    _fred_api_key = os.getenv('FRED_API_KEY')


def get_fred_api_key():
    """Get the FRED API key"""
    return _fred_api_key
