"""
Database package for stock analyzer

This package provides a modular database layer with specialized repositories
for different data domains.

Main entry point: StockDatabase (backwards compatible)
"""

from libs.db.connection import DatabaseConnection
from libs.db.stock_repository import StockRepository
from libs.db.watchlist_repository import WatchlistRepository
from libs.db.portfolio_repository import PortfolioRepository
from libs.db.screening import StockScreener
from libs.db.financial_repository import FinancialRepository
from libs.db.technical_repository import TechnicalRepository
from libs.db.macro_repository import MacroRepository

__all__ = [
    'DatabaseConnection',
    'StockRepository',
    'WatchlistRepository',
    'PortfolioRepository',
    'StockScreener',
    'FinancialRepository',
    'TechnicalRepository',
    'MacroRepository',
]
