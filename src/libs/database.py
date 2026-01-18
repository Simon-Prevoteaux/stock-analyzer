"""
Database module for storing and retrieving stock data
Uses SQLite for local file-based storage

This module provides a unified StockDatabase class that acts as a facade
over the modular repository classes in libs/db/.

For new code, consider using the specialized repositories directly:
- StockRepository: Stock CRUD operations
- WatchlistRepository: Watchlist management
- PortfolioRepository: Portfolio management
- StockScreener: Stock filtering and screening
- FinancialRepository: Financial history and growth metrics
- TechnicalRepository: Price history and technical indicators
- MacroRepository: Macroeconomic data
"""

import pandas as pd
from typing import List, Dict, Optional

from libs.db.connection import DatabaseConnection
from libs.db.stock_repository import StockRepository
from libs.db.watchlist_repository import WatchlistRepository
from libs.db.portfolio_repository import PortfolioRepository
from libs.db.screening import StockScreener
from libs.db.financial_repository import FinancialRepository
from libs.db.technical_repository import TechnicalRepository
from libs.db.macro_repository import MacroRepository


class StockDatabase:
    """
    Unified database interface for stock data management.

    This class provides a backwards-compatible facade over the modular
    repository classes. All existing code using StockDatabase will continue
    to work without modification.

    For new code, you can access the underlying repositories directly via:
    - db.stocks: StockRepository
    - db.watchlist_repo: WatchlistRepository
    - db.portfolio_repo: PortfolioRepository
    - db.screener: StockScreener
    - db.financials: FinancialRepository
    - db.technical: TechnicalRepository
    - db.macro: MacroRepository
    """

    def __init__(self, db_path: str = 'data/stocks.db'):
        """
        Initialize database connection and repositories

        Args:
            db_path: Path to SQLite database file
        """
        # Initialize connection (handles schema creation)
        self._connection = DatabaseConnection(db_path)
        self.conn = self._connection.conn
        self.db_path = db_path

        # Initialize repositories
        self.stocks = StockRepository(self.conn)
        self.watchlist_repo = WatchlistRepository(self.conn)
        self.portfolio_repo = PortfolioRepository(self.conn)
        self.screener = StockScreener(self.conn)
        self.financials = FinancialRepository(self.conn)
        self.technical = TechnicalRepository(self.conn)
        self.macro = MacroRepository(self.conn)

    # ==========================================================================
    # Stock CRUD operations (delegated to StockRepository)
    # ==========================================================================

    def save_stock(self, stock_data: Dict) -> bool:
        """Save or update stock data"""
        return self.stocks.save_stock(stock_data)

    def save_multiple_stocks(self, stocks_df: pd.DataFrame) -> int:
        """Save multiple stocks from a DataFrame"""
        return self.stocks.save_multiple_stocks(stocks_df)

    def get_stock(self, ticker: str) -> Optional[Dict]:
        """Retrieve stock data by ticker"""
        return self.stocks.get_stock(ticker)

    def get_all_stocks(self) -> pd.DataFrame:
        """Retrieve all stocks from database with growth metrics"""
        return self.stocks.get_all_stocks()

    def get_stocks_by_sector(self, sector: str) -> pd.DataFrame:
        """Retrieve stocks by sector"""
        return self.stocks.get_stocks_by_sector(sector)

    def get_sectors(self) -> List[str]:
        """Get list of all sectors in database"""
        return self.stocks.get_sectors()

    def search_stocks(self, keyword: str) -> pd.DataFrame:
        """Search stocks by ticker or company name"""
        return self.stocks.search_stocks(keyword)

    def delete_stock(self, ticker: str) -> bool:
        """Delete a stock from the database"""
        return self.stocks.delete_stock(ticker)

    def save_snapshot(self, ticker: str, data: Dict):
        """Save historical snapshot of stock data"""
        return self.stocks.save_snapshot(ticker, data)

    # ==========================================================================
    # Watchlist operations (delegated to WatchlistRepository)
    # ==========================================================================

    def add_to_watchlist(self, ticker: str, notes: str = '') -> bool:
        """Add stock to watchlist"""
        return self.watchlist_repo.add_to_watchlist(ticker, notes)

    def get_watchlist(self) -> pd.DataFrame:
        """Get all stocks in watchlist with growth metrics"""
        return self.watchlist_repo.get_watchlist()

    def remove_from_watchlist(self, ticker: str) -> bool:
        """Remove stock from watchlist"""
        return self.watchlist_repo.remove_from_watchlist(ticker)

    def update_watchlist_notes(self, ticker: str, notes: str) -> bool:
        """Update notes for a stock in watchlist"""
        return self.watchlist_repo.update_watchlist_notes(ticker, notes)

    def update_watchlist_ranking(self, ticker: str, ranking: int) -> bool:
        """Update ranking for a stock in watchlist"""
        return self.watchlist_repo.update_watchlist_ranking(ticker, ranking)

    # ==========================================================================
    # Portfolio operations (delegated to PortfolioRepository)
    # ==========================================================================

    def add_to_portfolio(self, ticker: str, notes: str = '') -> bool:
        """Add stock to portfolio"""
        return self.portfolio_repo.add_to_portfolio(ticker, notes)

    def get_portfolio(self) -> pd.DataFrame:
        """Get all stocks in portfolio with growth metrics"""
        return self.portfolio_repo.get_portfolio()

    def remove_from_portfolio(self, ticker: str) -> bool:
        """Remove stock from portfolio"""
        return self.portfolio_repo.remove_from_portfolio(ticker)

    # ==========================================================================
    # Stock screening operations (delegated to StockScreener)
    # ==========================================================================

    def get_high_risk_stocks(self, min_bubble_score: int = 6) -> pd.DataFrame:
        """Get stocks with high bubble scores"""
        return self.screener.get_high_risk_stocks(min_bubble_score)

    def get_value_stocks(self, max_pe: float = 20, max_ps: float = 3) -> pd.DataFrame:
        """Get potential value stocks"""
        return self.screener.get_value_stocks(max_pe, max_ps)

    def get_near_value_stocks(self) -> pd.DataFrame:
        """Get stocks that are close to being value plays"""
        return self.screener.get_near_value_stocks()

    def get_enhanced_value_stocks(self,
                                   max_pe: float = 20,
                                   max_ps: float = 3,
                                   min_consistency: float = 60,
                                   min_growth: float = 0.05) -> pd.DataFrame:
        """Get value stocks with growth quality filters"""
        return self.screener.get_enhanced_value_stocks(max_pe, max_ps, min_consistency, min_growth)

    def get_quality_growth_stocks(self, min_cagr: float = 20, min_consistency: int = 70, max_peg: float = 2.5) -> pd.DataFrame:
        """Find high-growth stocks with consistent, sustainable patterns"""
        return self.screener.get_quality_growth_stocks(min_cagr, min_consistency, max_peg)

    def get_growth_inflection_stocks(self, min_consistency: int = 60, max_pe: float = 40) -> pd.DataFrame:
        """Find stocks with accelerating growth (inflection points)"""
        return self.screener.get_growth_inflection_stocks(min_consistency, max_pe)

    def get_rule_of_40_stocks(self, min_rule_of_40: float = 40, sector_filter: str = None) -> pd.DataFrame:
        """Find stocks with efficient growth (Rule of 40)"""
        return self.screener.get_rule_of_40_stocks(min_rule_of_40, sector_filter)

    def get_margin_expansion_stocks(self, min_revenue_growth: float = 15, min_operating_leverage: float = 1.0) -> pd.DataFrame:
        """Find stocks with improving profitability alongside growth"""
        return self.screener.get_margin_expansion_stocks(min_revenue_growth, min_operating_leverage)

    def get_cash_generative_growth_stocks(self, min_revenue_growth: float = 20, min_fcf_margin: float = 10) -> pd.DataFrame:
        """Find high-growth stocks that also generate positive free cash flow"""
        return self.screener.get_cash_generative_growth_stocks(min_revenue_growth, min_fcf_margin)

    def add_sector_rankings(self, stocks_df: pd.DataFrame) -> pd.DataFrame:
        """Add sector-relative growth rankings to stock DataFrame"""
        return self.screener.add_sector_rankings(stocks_df)

    # ==========================================================================
    # Financial history operations (delegated to FinancialRepository)
    # ==========================================================================

    def save_financial_history(self, ticker: str, financial_data: List[Dict]) -> int:
        """Save historical financial data"""
        return self.financials.save_financial_history(ticker, financial_data)

    def get_financial_history(self, ticker: str, period_type: str = None) -> List[Dict]:
        """Retrieve financial history for a ticker"""
        return self.financials.get_financial_history(ticker, period_type)

    def save_growth_metrics(self, ticker: str, metrics: Dict) -> bool:
        """Save calculated growth metrics"""
        return self.financials.save_growth_metrics(ticker, metrics)

    def get_growth_metrics(self, ticker: str) -> Optional[Dict]:
        """Retrieve calculated growth metrics for a ticker"""
        return self.financials.get_growth_metrics(ticker)

    # ==========================================================================
    # Technical analysis operations (delegated to TechnicalRepository)
    # ==========================================================================

    def save_price_history(self, ticker: str, price_data: List[Dict]) -> bool:
        """Save historical price data to database"""
        return self.technical.save_price_history(ticker, price_data)

    def get_price_history(self, ticker: str, days: int = None) -> pd.DataFrame:
        """Get historical price data for a stock"""
        return self.technical.get_price_history(ticker, days)

    def save_technical_indicators(self, ticker: str, indicators: Dict) -> bool:
        """Save calculated technical indicators"""
        return self.technical.save_technical_indicators(ticker, indicators)

    def get_technical_indicators(self, ticker: str) -> Optional[Dict]:
        """Get cached technical indicators for a stock"""
        return self.technical.get_technical_indicators(ticker)

    # ==========================================================================
    # Macro data operations (delegated to MacroRepository)
    # ==========================================================================

    def save_macro_data(self, data_type: str, series_id: str, observations: List[Dict]) -> int:
        """Save macro data observations to database"""
        return self.macro.save_macro_data(data_type, series_id, observations)

    def get_macro_data(self, data_type: str, series_id: str,
                       start_date: Optional[str] = None,
                       end_date: Optional[str] = None) -> pd.DataFrame:
        """Get macro data from database"""
        return self.macro.get_macro_data(data_type, series_id, start_date, end_date)

    def get_latest_macro_snapshot(self) -> Dict:
        """Get latest macro data snapshot for quick dashboard loading"""
        return self.macro.get_latest_macro_snapshot()

    # ==========================================================================
    # Connection management
    # ==========================================================================

    def close(self):
        """Close database connection"""
        self._connection.close()
