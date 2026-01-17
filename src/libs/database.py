"""
Database module for storing and retrieving stock data
Uses SQLite for local file-based storage
"""

import sqlite3
import pandas as pd
from typing import List, Dict, Optional
from datetime import datetime, timedelta
import json


class StockDatabase:
    """Manages stock data persistence using SQLite"""

    def __init__(self, db_path: str = 'data/stocks.db'):
        """
        Initialize database connection

        Args:
            db_path: Path to SQLite database file
        """
        self.db_path = db_path
        self.conn = None
        self._connect()
        self._create_tables()

    def _connect(self):
        """Establish database connection"""
        self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row

    def _create_tables(self):
        """Create necessary database tables"""
        cursor = self.conn.cursor()

        # Stock data table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS stocks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ticker TEXT NOT NULL,
                company_name TEXT,
                market_cap REAL,
                revenue REAL,
                earnings REAL,
                pe_ratio REAL,
                forward_pe REAL,
                ps_ratio REAL,
                eps REAL,
                revenue_growth REAL,
                earnings_growth REAL,
                profit_margin REAL,
                operating_margin REAL,
                current_price REAL,
                sector TEXT,
                industry TEXT,
                is_profitable BOOLEAN,
                bubble_score INTEGER,
                risk_level TEXT,
                last_updated TIMESTAMP,
                price_to_book REAL,
                current_ratio REAL,
                free_cash_flow REAL,
                enterprise_value REAL,
                target_price REAL,
                UNIQUE(ticker)
            )
        ''')

        # Add new columns if they don't exist (for existing databases)
        try:
            cursor.execute('ALTER TABLE stocks ADD COLUMN price_to_book REAL')
        except sqlite3.OperationalError:
            pass  # Column already exists

        try:
            cursor.execute('ALTER TABLE stocks ADD COLUMN current_ratio REAL')
        except sqlite3.OperationalError:
            pass

        try:
            cursor.execute('ALTER TABLE stocks ADD COLUMN free_cash_flow REAL')
        except sqlite3.OperationalError:
            pass

        try:
            cursor.execute('ALTER TABLE stocks ADD COLUMN enterprise_value REAL')
        except sqlite3.OperationalError:
            pass

        try:
            cursor.execute('ALTER TABLE stocks ADD COLUMN target_price REAL')
        except sqlite3.OperationalError:
            pass

        # Add columns for new forecasting models
        try:
            cursor.execute('ALTER TABLE stocks ADD COLUMN book_value REAL')
        except sqlite3.OperationalError:
            pass

        try:
            cursor.execute('ALTER TABLE stocks ADD COLUMN dividend_rate REAL')
        except sqlite3.OperationalError:
            pass

        try:
            cursor.execute('ALTER TABLE stocks ADD COLUMN dividend_yield REAL')
        except sqlite3.OperationalError:
            pass

        # Watchlist table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS watchlist (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ticker TEXT NOT NULL,
                added_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                notes TEXT,
                ranking INTEGER DEFAULT 0,
                UNIQUE(ticker)
            )
        ''')

        # Add ranking column if it doesn't exist (for existing databases)
        try:
            cursor.execute('ALTER TABLE watchlist ADD COLUMN ranking INTEGER DEFAULT 0')
        except sqlite3.OperationalError:
            pass  # Column already exists

        # Portfolio table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS portfolio (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ticker TEXT NOT NULL,
                added_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                notes TEXT,
                UNIQUE(ticker)
            )
        ''')

        # Historical snapshots table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS historical_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ticker TEXT NOT NULL,
                snapshot_date TIMESTAMP,
                data TEXT,
                FOREIGN KEY (ticker) REFERENCES stocks(ticker)
            )
        ''')

        # Create new tables for historical financial data
        self._migrate_schema()

        self.conn.commit()

    def _migrate_schema(self):
        """Create new tables for historical tracking"""
        cursor = self.conn.cursor()

        # Financial history table - stores time-series financial data
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS financial_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ticker TEXT NOT NULL,
                period_end_date TEXT NOT NULL,
                period_type TEXT NOT NULL,
                revenue REAL,
                earnings REAL,
                gross_profit REAL,
                operating_income REAL,
                ebitda REAL,
                net_income REAL,
                eps REAL,
                shares_outstanding REAL,
                fetched_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(ticker, period_end_date, period_type),
                FOREIGN KEY (ticker) REFERENCES stocks(ticker)
            )
        ''')

        # Create index for fast queries
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_financial_ticker_date
            ON financial_history(ticker, period_end_date DESC)
        ''')

        # Growth metrics table - stores calculated growth analytics
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS growth_metrics (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ticker TEXT NOT NULL UNIQUE,
                revenue_cagr_3y REAL,
                revenue_cagr_5y REAL,
                earnings_cagr_3y REAL,
                earnings_cagr_5y REAL,
                avg_quarterly_revenue_growth REAL,
                avg_quarterly_earnings_growth REAL,
                revenue_consistency_score REAL,
                earnings_consistency_score REAL,
                revenue_growth_accelerating BOOLEAN,
                earnings_growth_accelerating BOOLEAN,
                consecutive_profitable_quarters INTEGER,
                data_points_count INTEGER,
                oldest_data_date TEXT,
                newest_data_date TEXT,
                last_calculated TIMESTAMP,
                peg_3y_cagr REAL,
                peg_quarterly REAL,
                peg_yfinance REAL,
                peg_average REAL,
                FOREIGN KEY (ticker) REFERENCES stocks(ticker)
            )
        ''')

        # Add PEG columns if they don't exist (for existing databases)
        for col in ['peg_3y_cagr', 'peg_quarterly', 'peg_yfinance', 'peg_average']:
            try:
                cursor.execute(f'ALTER TABLE growth_metrics ADD COLUMN {col} REAL')
            except sqlite3.OperationalError:
                pass  # Column already exists

        # Add new financial_history columns for Phase 1 enhancements
        for col in ['operating_cash_flow', 'capital_expenditures', 'free_cash_flow_calculated',
                    'gross_margin', 'operating_margin', 'profit_margin_quarterly']:
            try:
                cursor.execute(f'ALTER TABLE financial_history ADD COLUMN {col} REAL')
            except sqlite3.OperationalError:
                pass  # Column already exists

        # Add new growth_metrics columns for Phase 1 enhancements
        for col in ['fcf_cagr_3y', 'fcf_margin', 'rule_of_40', 'operating_leverage', 'cash_conversion_ratio']:
            try:
                cursor.execute(f'ALTER TABLE growth_metrics ADD COLUMN {col} REAL')
            except sqlite3.OperationalError:
                pass  # Column already exists

        # Add text columns for margin_trend and growth_stage
        for col in ['margin_trend', 'growth_stage']:
            try:
                cursor.execute(f'ALTER TABLE growth_metrics ADD COLUMN {col} TEXT')
            except sqlite3.OperationalError:
                pass  # Column already exists

        # Price history table - stores historical OHLC data for technical analysis
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS price_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ticker TEXT NOT NULL,
                date TEXT NOT NULL,
                open REAL,
                high REAL,
                low REAL,
                close REAL,
                volume INTEGER,
                fetched_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(ticker, date),
                FOREIGN KEY (ticker) REFERENCES stocks(ticker)
            )
        ''')

        # Create index for fast queries on price history
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_price_history_ticker_date
            ON price_history(ticker, date DESC)
        ''')

        # Technical indicators cache table - stores calculated technical analysis
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS technical_indicators (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ticker TEXT NOT NULL UNIQUE,
                support_levels TEXT,
                resistance_levels TEXT,
                trend_slope REAL,
                trend_r_squared REAL,
                trend_target_30d REAL,
                trend_target_90d REAL,
                last_calculated TIMESTAMP,
                FOREIGN KEY (ticker) REFERENCES stocks(ticker)
            )
        ''')

        # Macro data table - stores macroeconomic data from FRED
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS macro_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                data_type TEXT NOT NULL,
                series_id TEXT NOT NULL,
                date TEXT NOT NULL,
                value REAL,
                metadata TEXT,
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(data_type, series_id, date)
            )
        ''')

        # Create index for fast queries on macro data
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_macro_data_series_date
            ON macro_data(series_id, date DESC)
        ''')

        self.conn.commit()

    def save_stock(self, stock_data: Dict) -> bool:
        """
        Save or update stock data

        Args:
            stock_data: Dictionary containing stock metrics

        Returns:
            True if successful, False otherwise
        """
        try:
            cursor = self.conn.cursor()

            cursor.execute('''
                INSERT OR REPLACE INTO stocks (
                    ticker, company_name, market_cap, revenue, earnings,
                    pe_ratio, forward_pe, ps_ratio, eps, revenue_growth,
                    earnings_growth, profit_margin, operating_margin,
                    current_price, sector, industry, is_profitable,
                    bubble_score, risk_level, last_updated,
                    price_to_book, current_ratio, free_cash_flow,
                    enterprise_value, target_price, book_value,
                    dividend_rate, dividend_yield
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                stock_data.get('ticker'),
                stock_data.get('company_name'),
                stock_data.get('market_cap'),
                stock_data.get('revenue'),
                stock_data.get('earnings'),
                stock_data.get('pe_ratio'),
                stock_data.get('forward_pe'),
                stock_data.get('ps_ratio'),
                stock_data.get('eps'),
                stock_data.get('revenue_growth'),
                stock_data.get('earnings_growth'),
                stock_data.get('profit_margin'),
                stock_data.get('operating_margin'),
                stock_data.get('current_price'),
                stock_data.get('sector'),
                stock_data.get('industry'),
                stock_data.get('is_profitable'),
                stock_data.get('bubble_score'),
                stock_data.get('risk_level'),
                stock_data.get('last_updated'),
                stock_data.get('price_to_book'),
                stock_data.get('current_ratio'),
                stock_data.get('free_cash_flow'),
                stock_data.get('enterprise_value'),
                stock_data.get('target_price'),
                stock_data.get('book_value'),
                stock_data.get('dividend_rate'),
                stock_data.get('dividend_yield')
            ))

            self.conn.commit()
            return True

        except Exception as e:
            print(f"Error saving stock data: {str(e)}")
            return False

    def save_multiple_stocks(self, stocks_df: pd.DataFrame) -> int:
        """
        Save multiple stocks from a DataFrame

        Args:
            stocks_df: DataFrame containing stock data

        Returns:
            Number of stocks saved successfully
        """
        count = 0
        for _, row in stocks_df.iterrows():
            if self.save_stock(row.to_dict()):
                count += 1
        return count

    def get_stock(self, ticker: str) -> Optional[Dict]:
        """
        Retrieve stock data by ticker

        Args:
            ticker: Stock ticker symbol

        Returns:
            Dictionary containing stock data or None
        """
        cursor = self.conn.cursor()
        cursor.execute('SELECT * FROM stocks WHERE ticker = ?', (ticker.upper(),))
        row = cursor.fetchone()

        if row:
            return dict(row)
        return None

    def get_all_stocks(self) -> pd.DataFrame:
        """
        Retrieve all stocks from database with growth metrics

        Returns:
            DataFrame containing all stock data with growth metrics
        """
        query = '''
            SELECT s.*,
                   g.peg_average, g.peg_3y_cagr, g.peg_quarterly, g.peg_yfinance,
                   g.revenue_cagr_3y, g.earnings_cagr_3y,
                   g.revenue_consistency_score, g.earnings_consistency_score,
                   g.growth_stage
            FROM stocks s
            LEFT JOIN growth_metrics g ON s.ticker = g.ticker
            ORDER BY s.market_cap DESC
        '''
        return pd.read_sql_query(query, self.conn)

    def get_stocks_by_sector(self, sector: str) -> pd.DataFrame:
        """
        Retrieve stocks by sector

        Args:
            sector: Sector name

        Returns:
            DataFrame containing filtered stocks
        """
        query = 'SELECT * FROM stocks WHERE sector = ? ORDER BY market_cap DESC'
        return pd.read_sql_query(query, self.conn, params=(sector,))

    def get_high_risk_stocks(self, min_bubble_score: int = 6) -> pd.DataFrame:
        """
        Get stocks with high bubble scores

        Args:
            min_bubble_score: Minimum bubble score threshold

        Returns:
            DataFrame containing high-risk stocks with growth metrics
        """
        query = '''
            SELECT s.*, g.peg_average, g.peg_3y_cagr, g.peg_quarterly, g.peg_yfinance
            FROM stocks s
            LEFT JOIN growth_metrics g ON s.ticker = g.ticker
            WHERE s.bubble_score >= ?
            ORDER BY s.bubble_score DESC
        '''
        return pd.read_sql_query(query, self.conn, params=(min_bubble_score,))

    def get_value_stocks(self, max_pe: float = 20, max_ps: float = 3) -> pd.DataFrame:
        """
        Get potential value stocks

        Args:
            max_pe: Maximum P/E ratio
            max_ps: Maximum P/S ratio

        Returns:
            DataFrame containing value stocks with growth metrics
        """
        query = '''
            SELECT s.*, g.peg_average, g.peg_3y_cagr, g.peg_quarterly, g.peg_yfinance
            FROM stocks s
            LEFT JOIN growth_metrics g ON s.ticker = g.ticker
            WHERE s.pe_ratio <= ? AND s.ps_ratio <= ? AND s.is_profitable = 1
            ORDER BY s.ps_ratio ASC
        '''
        return pd.read_sql_query(query, self.conn, params=(max_pe, max_ps))

    def get_near_value_stocks(self) -> pd.DataFrame:
        """
        Get stocks that are close to being value plays - meeting some but not all criteria,
        or just slightly outside the thresholds.

        Criteria (profitable stocks that match ONE of):
        - P/E between 20-30 AND P/S <= 5 (close on P/E)
        - P/E <= 25 AND P/S between 3-5 (close on P/S)

        Excludes stocks that already qualify as value plays (P/E <= 20 AND P/S <= 3)

        Returns:
            DataFrame containing near-value stocks
        """
        query = '''
            SELECT s.*, g.peg_average, g.peg_3y_cagr, g.peg_quarterly, g.peg_yfinance
            FROM stocks s
            LEFT JOIN growth_metrics g ON s.ticker = g.ticker
            WHERE s.is_profitable = 1
            AND NOT (s.pe_ratio <= 20 AND s.ps_ratio <= 3)
            AND (
                (s.pe_ratio > 20 AND s.pe_ratio <= 30 AND s.ps_ratio <= 5)
                OR (s.pe_ratio <= 25 AND s.ps_ratio > 3 AND s.ps_ratio <= 5)
            )
            ORDER BY
                CASE
                    WHEN s.pe_ratio <= 20 THEN s.ps_ratio - 3
                    WHEN s.ps_ratio <= 3 THEN s.pe_ratio - 20
                    ELSE (s.pe_ratio - 20) / 10.0 + (s.ps_ratio - 3) / 2.0
                END ASC
        '''
        return pd.read_sql_query(query, self.conn)

    def add_to_watchlist(self, ticker: str, notes: str = '') -> bool:
        """
        Add stock to watchlist

        Args:
            ticker: Stock ticker symbol
            notes: Optional notes

        Returns:
            True if successful
        """
        try:
            cursor = self.conn.cursor()
            cursor.execute('''
                INSERT OR IGNORE INTO watchlist (ticker, notes)
                VALUES (?, ?)
            ''', (ticker.upper(), notes))
            self.conn.commit()
            return True
        except Exception as e:
            print(f"Error adding to watchlist: {str(e)}")
            return False

    def get_watchlist(self) -> pd.DataFrame:
        """Get all stocks in watchlist with growth metrics"""
        query = '''
            SELECT w.ticker, w.added_date, w.notes, w.ranking, s.*, g.peg_average, g.peg_3y_cagr, g.peg_quarterly, g.peg_yfinance
            FROM watchlist w
            LEFT JOIN stocks s ON w.ticker = s.ticker
            LEFT JOIN growth_metrics g ON w.ticker = g.ticker
            ORDER BY w.ranking DESC, w.added_date DESC
        '''
        return pd.read_sql_query(query, self.conn)

    def remove_from_watchlist(self, ticker: str) -> bool:
        """Remove stock from watchlist"""
        try:
            cursor = self.conn.cursor()
            cursor.execute('DELETE FROM watchlist WHERE ticker = ?', (ticker.upper(),))
            self.conn.commit()
            return True
        except Exception as e:
            print(f"Error removing from watchlist: {str(e)}")
            return False

    def update_watchlist_notes(self, ticker: str, notes: str) -> bool:
        """
        Update notes for a stock in watchlist

        Args:
            ticker: Stock ticker symbol
            notes: New notes text

        Returns:
            True if successful
        """
        try:
            cursor = self.conn.cursor()
            cursor.execute('''
                UPDATE watchlist
                SET notes = ?
                WHERE ticker = ?
            ''', (notes, ticker.upper()))
            self.conn.commit()
            return True
        except Exception as e:
            print(f"Error updating watchlist notes: {str(e)}")
            return False

    def update_watchlist_ranking(self, ticker: str, ranking: int) -> bool:
        """
        Update ranking for a stock in watchlist

        Args:
            ticker: Stock ticker symbol
            ranking: New ranking (0-5 scale)

        Returns:
            True if successful
        """
        try:
            cursor = self.conn.cursor()
            cursor.execute('''
                UPDATE watchlist
                SET ranking = ?
                WHERE ticker = ?
            ''', (ranking, ticker.upper()))
            self.conn.commit()
            return True
        except Exception as e:
            print(f"Error updating watchlist ranking: {str(e)}")
            return False

    def add_to_portfolio(self, ticker: str, notes: str = '') -> bool:
        """
        Add stock to portfolio

        Args:
            ticker: Stock ticker symbol
            notes: Optional notes

        Returns:
            True if successful
        """
        try:
            cursor = self.conn.cursor()
            cursor.execute('''
                INSERT OR IGNORE INTO portfolio (ticker, notes)
                VALUES (?, ?)
            ''', (ticker.upper(), notes))
            self.conn.commit()
            return True
        except Exception as e:
            print(f"Error adding to portfolio: {str(e)}")
            return False

    def get_portfolio(self) -> pd.DataFrame:
        """Get all stocks in portfolio with growth metrics"""
        query = '''
            SELECT p.ticker, p.added_date, p.notes, s.*, g.peg_average, g.peg_3y_cagr, g.peg_quarterly, g.peg_yfinance
            FROM portfolio p
            LEFT JOIN stocks s ON p.ticker = s.ticker
            LEFT JOIN growth_metrics g ON p.ticker = g.ticker
            ORDER BY p.added_date DESC
        '''
        return pd.read_sql_query(query, self.conn)

    def remove_from_portfolio(self, ticker: str) -> bool:
        """Remove stock from portfolio"""
        try:
            cursor = self.conn.cursor()
            cursor.execute('DELETE FROM portfolio WHERE ticker = ?', (ticker.upper(),))
            self.conn.commit()
            return True
        except Exception as e:
            print(f"Error removing from portfolio: {str(e)}")
            return False

    def save_snapshot(self, ticker: str, data: Dict):
        """Save historical snapshot of stock data"""
        try:
            cursor = self.conn.cursor()
            cursor.execute('''
                INSERT INTO historical_snapshots (ticker, snapshot_date, data)
                VALUES (?, ?, ?)
            ''', (ticker.upper(), datetime.now(), json.dumps(data)))
            self.conn.commit()
        except Exception as e:
            print(f"Error saving snapshot: {str(e)}")

    def get_sectors(self) -> List[str]:
        """Get list of all sectors in database"""
        cursor = self.conn.cursor()
        cursor.execute('SELECT DISTINCT sector FROM stocks WHERE sector IS NOT NULL ORDER BY sector')
        return [row[0] for row in cursor.fetchall()]

    def search_stocks(self, keyword: str) -> pd.DataFrame:
        """
        Search stocks by ticker or company name

        Args:
            keyword: Search term

        Returns:
            DataFrame containing matching stocks
        """
        query = '''
            SELECT * FROM stocks
            WHERE ticker LIKE ? OR company_name LIKE ?
            ORDER BY market_cap DESC
        '''
        search_term = f'%{keyword}%'
        return pd.read_sql_query(query, self.conn, params=(search_term, search_term))

    def delete_stock(self, ticker: str) -> bool:
        """
        Delete a stock from the database

        Args:
            ticker: Stock ticker symbol

        Returns:
            True if successful, False otherwise
        """
        try:
            cursor = self.conn.cursor()
            cursor.execute('DELETE FROM stocks WHERE ticker = ?', (ticker.upper(),))
            self.conn.commit()
            return cursor.rowcount > 0
        except Exception as e:
            print(f"Error deleting stock: {str(e)}")
            return False

    def save_financial_history(self, ticker: str, financial_data: List[Dict]) -> int:
        """
        Save historical financial data

        Uses INSERT OR REPLACE to update existing records with new fields.
        This ensures that when we add new columns (like cash flow data),
        existing records get updated with the new data on refresh.

        Args:
            ticker: Stock ticker
            financial_data: List of financial data points

        Returns:
            Number of records saved/updated
        """
        if not financial_data:
            return 0

        cursor = self.conn.cursor()
        saved_count = 0

        for record in financial_data:
            try:
                cursor.execute('''
                    INSERT OR REPLACE INTO financial_history
                    (ticker, period_end_date, period_type, revenue, earnings,
                     gross_profit, operating_income, ebitda, net_income, eps, shares_outstanding,
                     operating_cash_flow, capital_expenditures, free_cash_flow_calculated,
                     gross_margin, operating_margin, profit_margin_quarterly)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    ticker.upper(),
                    record.get('period_end_date'),
                    record.get('period_type'),
                    record.get('revenue'),
                    record.get('earnings'),
                    record.get('gross_profit'),
                    record.get('operating_income'),
                    record.get('ebitda'),
                    record.get('net_income'),
                    record.get('eps'),
                    record.get('shares_outstanding'),
                    record.get('operating_cash_flow'),
                    record.get('capital_expenditures'),
                    record.get('free_cash_flow_calculated'),
                    record.get('gross_margin'),
                    record.get('operating_margin'),
                    record.get('profit_margin_quarterly')
                ))
                # Count all successful operations (inserts or updates)
                saved_count += 1
            except Exception as e:
                print(f"Error saving financial record: {str(e)}")

        self.conn.commit()
        return saved_count

    def get_financial_history(self, ticker: str, period_type: str = None) -> List[Dict]:
        """
        Retrieve financial history for a ticker

        Args:
            ticker: Stock ticker
            period_type: Optional filter for 'quarterly' or 'annual'

        Returns:
            List of financial records
        """
        query = 'SELECT * FROM financial_history WHERE ticker = ?'
        params = [ticker.upper()]

        if period_type:
            query += ' AND period_type = ?'
            params.append(period_type)

        query += ' ORDER BY period_end_date DESC'

        cursor = self.conn.cursor()
        cursor.execute(query, params)

        return [dict(row) for row in cursor.fetchall()]

    def save_growth_metrics(self, ticker: str, metrics: Dict) -> bool:
        """Save calculated growth metrics"""
        try:
            cursor = self.conn.cursor()
            cursor.execute('''
                INSERT OR REPLACE INTO growth_metrics (
                    ticker, revenue_cagr_3y, revenue_cagr_5y, earnings_cagr_3y, earnings_cagr_5y,
                    avg_quarterly_revenue_growth, avg_quarterly_earnings_growth,
                    revenue_consistency_score, earnings_consistency_score,
                    revenue_growth_accelerating, earnings_growth_accelerating,
                    consecutive_profitable_quarters, data_points_count,
                    oldest_data_date, newest_data_date, last_calculated,
                    peg_3y_cagr, peg_quarterly, peg_yfinance, peg_average,
                    fcf_cagr_3y, fcf_margin, rule_of_40, operating_leverage,
                    cash_conversion_ratio, margin_trend, growth_stage
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                ticker.upper(),
                metrics.get('revenue_cagr_3y'),
                metrics.get('revenue_cagr_5y'),
                metrics.get('earnings_cagr_3y'),
                metrics.get('earnings_cagr_5y'),
                metrics.get('avg_quarterly_revenue_growth'),
                metrics.get('avg_quarterly_earnings_growth'),
                metrics.get('revenue_consistency_score'),
                metrics.get('earnings_consistency_score'),
                metrics.get('revenue_growth_accelerating'),
                metrics.get('earnings_growth_accelerating'),
                metrics.get('consecutive_profitable_quarters'),
                metrics.get('data_points_count'),
                metrics.get('oldest_data_date'),
                metrics.get('newest_data_date'),
                metrics.get('peg_3y_cagr'),
                metrics.get('peg_quarterly'),
                metrics.get('peg_yfinance'),
                metrics.get('peg_average'),
                metrics.get('fcf_cagr_3y'),
                metrics.get('fcf_margin'),
                metrics.get('rule_of_40'),
                metrics.get('operating_leverage'),
                metrics.get('cash_conversion_ratio'),
                metrics.get('margin_trend'),
                metrics.get('growth_stage')
            ))
            self.conn.commit()
            return True
        except Exception as e:
            print(f"Error saving growth metrics: {str(e)}")
            return False

    def get_growth_metrics(self, ticker: str) -> Optional[Dict]:
        """Retrieve calculated growth metrics for a ticker"""
        cursor = self.conn.cursor()
        cursor.execute('SELECT * FROM growth_metrics WHERE ticker = ?', (ticker.upper(),))
        row = cursor.fetchone()
        return dict(row) if row else None

    def get_enhanced_value_stocks(self,
                                   max_pe: float = 20,
                                   max_ps: float = 3,
                                   min_consistency: float = 60,
                                   min_growth: float = 0.05) -> pd.DataFrame:
        """
        Get value stocks with growth quality filters

        Args:
            max_pe: Maximum P/E ratio
            max_ps: Maximum P/S ratio
            min_consistency: Minimum consistency score (0-100)
            min_growth: Minimum average quarterly growth rate

        Returns:
            DataFrame of enhanced value stocks
        """
        query = '''
            SELECT s.*, g.*
            FROM stocks s
            LEFT JOIN growth_metrics g ON s.ticker = g.ticker
            WHERE s.pe_ratio <= ?
            AND s.ps_ratio <= ?
            AND s.is_profitable = 1
            AND (g.revenue_consistency_score >= ? OR g.earnings_consistency_score >= ?)
            AND (g.avg_quarterly_revenue_growth >= ? OR g.avg_quarterly_earnings_growth >= ?)
            ORDER BY g.revenue_consistency_score DESC, s.ps_ratio ASC
        '''
        return pd.read_sql_query(query, self.conn,
                                params=(max_pe, max_ps, min_consistency, min_consistency, min_growth, min_growth))

    def get_quality_growth_stocks(self, min_cagr: float = 20, min_consistency: int = 70, max_peg: float = 2.5) -> pd.DataFrame:
        """
        Find high-growth stocks with consistent, sustainable patterns

        Quality Growth = High CAGR + High Consistency + Reasonable Valuation
        (Accelerating growth is a bonus but not required)

        Args:
            min_cagr: Minimum earnings/revenue CAGR (as percentage, e.g., 20 for 20%)
            min_consistency: Minimum consistency score (0-100)
            max_peg: Maximum PEG ratio (growth-adjusted valuation)

        Returns:
            DataFrame of quality growth stocks sorted by consistency and valuation
        """
        query = '''
            SELECT s.*,
                   g.earnings_cagr_3y, g.revenue_cagr_3y,
                   g.earnings_consistency_score, g.revenue_consistency_score,
                   g.peg_average, g.revenue_growth_accelerating,
                   g.earnings_growth_accelerating,
                   g.consecutive_profitable_quarters,
                   g.growth_stage
            FROM stocks s
            LEFT JOIN growth_metrics g ON s.ticker = g.ticker
            WHERE (g.earnings_cagr_3y >= ? OR g.revenue_cagr_3y >= ?)
              AND g.earnings_consistency_score >= ?
              AND g.peg_average IS NOT NULL
              AND g.peg_average <= ?
            ORDER BY
              CASE WHEN g.revenue_growth_accelerating = 1 OR g.earnings_growth_accelerating = 1 THEN 0 ELSE 1 END,
              g.earnings_consistency_score DESC,
              g.peg_average ASC
        '''
        return pd.read_sql_query(query, self.conn, params=(min_cagr/100, min_cagr/100, min_consistency, max_peg))

    def get_growth_inflection_stocks(self, min_consistency: int = 60, max_pe: float = 40) -> pd.DataFrame:
        """
        Find stocks with accelerating growth (inflection points)

        Growth Inflection = Recent growth > Historical + Reasonable valuation

        Args:
            min_consistency: Minimum consistency score (0-100) to filter noise
            max_pe: Maximum P/E ratio to avoid overvalued stocks

        Returns:
            DataFrame of stocks showing growth acceleration
        """
        query = '''
            SELECT s.*,
                   g.revenue_growth_accelerating, g.earnings_growth_accelerating,
                   g.earnings_cagr_3y, g.revenue_cagr_3y,
                   g.earnings_consistency_score, g.revenue_consistency_score,
                   g.avg_quarterly_revenue_growth, g.avg_quarterly_earnings_growth,
                   g.consecutive_profitable_quarters,
                   g.peg_average,
                   g.growth_stage
            FROM stocks s
            LEFT JOIN growth_metrics g ON s.ticker = g.ticker
            WHERE (g.revenue_growth_accelerating = 1 OR g.earnings_growth_accelerating = 1)
              AND (g.earnings_consistency_score >= ? OR g.revenue_consistency_score >= ?)
              AND (s.pe_ratio IS NULL OR s.pe_ratio <= ?)
            ORDER BY
              CASE WHEN g.revenue_growth_accelerating = 1 AND g.earnings_growth_accelerating = 1 THEN 0 ELSE 1 END,
              g.revenue_cagr_3y DESC,
              g.earnings_cagr_3y DESC
        '''
        return pd.read_sql_query(query, self.conn, params=(min_consistency, min_consistency, max_pe))

    def add_sector_rankings(self, stocks_df: pd.DataFrame) -> pd.DataFrame:
        """
        Add sector-relative growth rankings to stock DataFrame

        Calculates:
        - Sector median CAGR for revenue and earnings
        - Growth vs sector median (ratio)
        - Percentile rank within sector (0-100)

        Args:
            stocks_df: DataFrame with stock data including sector and growth metrics

        Returns:
            Enhanced DataFrame with sector comparison columns
        """
        if stocks_df.empty or 'sector' not in stocks_df.columns:
            return stocks_df

        # Calculate sector medians for revenue and earnings CAGR
        sector_medians = stocks_df.groupby('sector').agg({
            'revenue_cagr_3y': 'median',
            'earnings_cagr_3y': 'median'
        }).reset_index()

        # Rename columns to avoid conflicts
        sector_medians.columns = ['sector', 'revenue_cagr_3y_sector_median', 'earnings_cagr_3y_sector_median']

        # Merge sector medians back to original DataFrame
        stocks_df = stocks_df.merge(sector_medians, on='sector', how='left')

        # Calculate relative growth vs sector median
        stocks_df['revenue_vs_sector'] = None
        stocks_df['earnings_vs_sector'] = None

        # Revenue vs sector
        mask = (stocks_df['revenue_cagr_3y'].notna()) & (stocks_df['revenue_cagr_3y_sector_median'].notna()) & (stocks_df['revenue_cagr_3y_sector_median'] != 0)
        stocks_df.loc[mask, 'revenue_vs_sector'] = stocks_df.loc[mask, 'revenue_cagr_3y'] / stocks_df.loc[mask, 'revenue_cagr_3y_sector_median']

        # Earnings vs sector
        mask = (stocks_df['earnings_cagr_3y'].notna()) & (stocks_df['earnings_cagr_3y_sector_median'].notna()) & (stocks_df['earnings_cagr_3y_sector_median'] != 0)
        stocks_df.loc[mask, 'earnings_vs_sector'] = stocks_df.loc[mask, 'earnings_cagr_3y'] / stocks_df.loc[mask, 'earnings_cagr_3y_sector_median']

        # Calculate percentile rank within sector (0-1 scale, then convert to 0-100)
        stocks_df['sector_revenue_rank_pct'] = stocks_df.groupby('sector')['revenue_cagr_3y'].rank(pct=True) * 100
        stocks_df['sector_earnings_rank_pct'] = stocks_df.groupby('sector')['earnings_cagr_3y'].rank(pct=True) * 100

        return stocks_df

    def get_rule_of_40_stocks(self, min_rule_of_40: float = 40, sector_filter: str = None) -> pd.DataFrame:
        """
        Find stocks with efficient growth (Revenue Growth % + FCF Margin % >= threshold)

        Rule of 40 = Revenue Growth Rate (%) + FCF Margin (%)
        Widely used for SaaS/cloud companies to balance growth with profitability

        Args:
            min_rule_of_40: Minimum Rule of 40 score (default: 40)
            sector_filter: Optional sector to filter (e.g., 'Technology', 'Communication Services')

        Returns:
            DataFrame of stocks meeting Rule of 40 threshold, sorted by score
        """
        if sector_filter:
            query = '''
                SELECT s.*,
                       g.revenue_cagr_3y, g.fcf_margin, g.rule_of_40,
                       g.growth_stage, g.fcf_cagr_3y, g.cash_conversion_ratio,
                       g.operating_leverage, g.margin_trend
                FROM stocks s
                LEFT JOIN growth_metrics g ON s.ticker = g.ticker
                WHERE g.rule_of_40 IS NOT NULL
                  AND g.rule_of_40 >= ?
                  AND s.sector = ?
                ORDER BY g.rule_of_40 DESC
            '''
            return pd.read_sql_query(query, self.conn, params=(min_rule_of_40, sector_filter))
        else:
            query = '''
                SELECT s.*,
                       g.revenue_cagr_3y, g.fcf_margin, g.rule_of_40,
                       g.growth_stage, g.fcf_cagr_3y, g.cash_conversion_ratio,
                       g.operating_leverage, g.margin_trend
                FROM stocks s
                LEFT JOIN growth_metrics g ON s.ticker = g.ticker
                WHERE g.rule_of_40 IS NOT NULL
                  AND g.rule_of_40 >= ?
                ORDER BY g.rule_of_40 DESC
            '''
            return pd.read_sql_query(query, self.conn, params=(min_rule_of_40,))

    def get_margin_expansion_stocks(self, min_revenue_growth: float = 15, min_operating_leverage: float = 1.0) -> pd.DataFrame:
        """
        Find stocks with improving profitability alongside growth

        Identifies companies where margins are expanding (recent 4Q margins > previous 4Q)
        AND earnings are growing faster than revenue (operating leverage > 1.0)

        Args:
            min_revenue_growth: Minimum revenue CAGR percentage (default: 15%)
            min_operating_leverage: Minimum operating leverage ratio (default: 1.0)

        Returns:
            DataFrame of stocks with margin expansion, sorted by operating leverage
        """
        query = '''
            SELECT s.*,
                   g.revenue_cagr_3y, g.earnings_cagr_3y,
                   g.margin_trend, g.operating_leverage,
                   g.growth_stage, g.earnings_consistency_score,
                   s.profit_margin, s.operating_margin
            FROM stocks s
            LEFT JOIN growth_metrics g ON s.ticker = g.ticker
            WHERE g.revenue_cagr_3y IS NOT NULL
              AND g.revenue_cagr_3y >= ?
              AND g.margin_trend = 'expanding'
              AND g.operating_leverage IS NOT NULL
              AND g.operating_leverage >= ?
            ORDER BY g.operating_leverage DESC
        '''
        return pd.read_sql_query(query, self.conn, params=(min_revenue_growth/100, min_operating_leverage))

    def get_cash_generative_growth_stocks(self, min_revenue_growth: float = 20, min_fcf_margin: float = 10) -> pd.DataFrame:
        """
        Find high-growth stocks that also generate positive free cash flow

        This is a rare and valuable combination - most high-growth companies burn cash.
        FCF-positive growth indicates sustainable expansion with lower financial risk.

        Args:
            min_revenue_growth: Minimum revenue CAGR percentage (default: 20%)
            min_fcf_margin: Minimum FCF margin percentage (default: 10%)

        Returns:
            DataFrame of cash-generative growth stocks, sorted by revenue growth
        """
        query = '''
            SELECT s.*,
                   g.revenue_cagr_3y, g.earnings_cagr_3y,
                   g.fcf_margin, g.fcf_cagr_3y,
                   g.cash_conversion_ratio, g.growth_stage,
                   g.earnings_consistency_score,
                   s.free_cash_flow, s.profit_margin
            FROM stocks s
            LEFT JOIN growth_metrics g ON s.ticker = g.ticker
            WHERE g.revenue_cagr_3y IS NOT NULL
              AND g.revenue_cagr_3y >= ?
              AND s.free_cash_flow IS NOT NULL
              AND s.free_cash_flow > 0
              AND g.fcf_margin IS NOT NULL
              AND g.fcf_margin >= ?
              AND g.cash_conversion_ratio IS NOT NULL
              AND g.cash_conversion_ratio > 0.8
            ORDER BY g.revenue_cagr_3y DESC
        '''
        return pd.read_sql_query(query, self.conn, params=(min_revenue_growth/100, min_fcf_margin/100))

    def save_price_history(self, ticker: str, price_data: List[Dict]) -> bool:
        """
        Save historical price data to database

        Args:
            ticker: Stock ticker symbol
            price_data: List of dictionaries with OHLC data

        Returns:
            True if successful
        """
        try:
            cursor = self.conn.cursor()
            for data_point in price_data:
                cursor.execute('''
                    INSERT OR REPLACE INTO price_history
                    (ticker, date, open, high, low, close, volume)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                ''', (
                    ticker.upper(),
                    data_point['date'],
                    data_point['open'],
                    data_point['high'],
                    data_point['low'],
                    data_point['close'],
                    data_point['volume']
                ))
            self.conn.commit()
            return True
        except Exception as e:
            print(f"Error saving price history: {str(e)}")
            return False

    def get_price_history(self, ticker: str, days: int = None) -> pd.DataFrame:
        """
        Get historical price data for a stock

        Args:
            ticker: Stock ticker symbol
            days: Optional number of recent days to fetch

        Returns:
            DataFrame with price history
        """
        if days:
            query = '''
                SELECT * FROM price_history
                WHERE ticker = ?
                ORDER BY date DESC
                LIMIT ?
            '''
            df = pd.read_sql_query(query, self.conn, params=(ticker.upper(), days))
        else:
            query = '''
                SELECT * FROM price_history
                WHERE ticker = ?
                ORDER BY date DESC
            '''
            df = pd.read_sql_query(query, self.conn, params=(ticker.upper(),))

        # Sort by date ascending for analysis
        if not df.empty:
            df['date'] = pd.to_datetime(df['date'])
            df = df.sort_values('date', ascending=True)

        return df

    def save_technical_indicators(self, ticker: str, indicators: Dict) -> bool:
        """
        Save calculated technical indicators

        Args:
            ticker: Stock ticker symbol
            indicators: Dictionary with calculated indicators

        Returns:
            True if successful
        """
        try:
            cursor = self.conn.cursor()
            cursor.execute('''
                INSERT OR REPLACE INTO technical_indicators
                (ticker, support_levels, resistance_levels, trend_slope,
                 trend_r_squared, trend_target_30d, trend_target_90d, last_calculated)
                VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ''', (
                ticker.upper(),
                json.dumps(indicators.get('support_levels', [])),
                json.dumps(indicators.get('resistance_levels', [])),
                indicators.get('trend_slope'),
                indicators.get('trend_r_squared'),
                indicators.get('trend_target_30d'),
                indicators.get('trend_target_90d')
            ))
            self.conn.commit()
            return True
        except Exception as e:
            print(f"Error saving technical indicators: {str(e)}")
            return False

    def get_technical_indicators(self, ticker: str) -> Optional[Dict]:
        """Get cached technical indicators for a stock"""
        cursor = self.conn.cursor()
        cursor.execute('''
            SELECT * FROM technical_indicators
            WHERE ticker = ?
        ''', (ticker.upper(),))
        row = cursor.fetchone()

        if row:
            return {
                'ticker': row['ticker'],
                'support_levels': json.loads(row['support_levels']) if row['support_levels'] else [],
                'resistance_levels': json.loads(row['resistance_levels']) if row['resistance_levels'] else [],
                'trend_slope': row['trend_slope'],
                'trend_r_squared': row['trend_r_squared'],
                'trend_target_30d': row['trend_target_30d'],
                'trend_target_90d': row['trend_target_90d'],
                'last_calculated': row['last_calculated']
            }
        return None

    def save_macro_data(self, data_type: str, series_id: str, observations: List[Dict]) -> int:
        """
        Save macro data observations to database

        Args:
            data_type: Type of data (fx_rate, gold, yield, credit_spread)
            series_id: FRED series ID
            observations: List of dicts with date and value

        Returns:
            Number of records saved
        """
        try:
            cursor = self.conn.cursor()
            saved_count = 0

            for obs in observations:
                cursor.execute('''
                    INSERT OR REPLACE INTO macro_data (
                        data_type, series_id, date, value, last_updated
                    ) VALUES (?, ?, ?, ?, ?)
                ''', (
                    data_type,
                    series_id,
                    obs.get('date'),
                    obs.get('value'),
                    datetime.now()
                ))
                saved_count += 1

            self.conn.commit()
            return saved_count

        except Exception as e:
            print(f"Error saving macro data: {str(e)}")
            return 0

    def get_macro_data(self, data_type: str, series_id: str,
                       start_date: Optional[str] = None,
                       end_date: Optional[str] = None) -> pd.DataFrame:
        """
        Get macro data from database

        Args:
            data_type: Type of data
            series_id: FRED series ID
            start_date: Optional start date (YYYY-MM-DD)
            end_date: Optional end date (YYYY-MM-DD)

        Returns:
            DataFrame with date and value columns
        """
        query = '''
            SELECT date, value FROM macro_data
            WHERE data_type = ? AND series_id = ?
        '''
        params = [data_type, series_id]

        if start_date:
            query += ' AND date >= ?'
            params.append(start_date)

        if end_date:
            query += ' AND date <= ?'
            params.append(end_date)

        query += ' ORDER BY date'

        df = pd.read_sql_query(query, self.conn, params=params)
        if not df.empty:
            df['date'] = pd.to_datetime(df['date'])
        return df

    def get_latest_macro_snapshot(self) -> Dict:
        """
        Get latest macro data snapshot for quick dashboard loading

        Returns:
            Dict with latest values for key series
        """
        cursor = self.conn.cursor()

        snapshot = {}

        # Get latest currency rates
        for currency in ['EUR', 'JPY', 'CNY', 'CHF']:
            cursor.execute('''
                SELECT value, date FROM macro_data
                WHERE data_type = 'fx_rate' AND series_id LIKE ?
                ORDER BY date DESC LIMIT 1
            ''', (f'%{currency}%',))
            row = cursor.fetchone()
            if row:
                snapshot[f'{currency}_rate'] = {
                    'value': row['value'],
                    'date': row['date']
                }

        # Get latest gold price
        cursor.execute('''
            SELECT value, date FROM macro_data
            WHERE data_type = 'gold'
            ORDER BY date DESC LIMIT 1
        ''')
        row = cursor.fetchone()
        if row:
            snapshot['gold'] = {
                'value': row['value'],
                'date': row['date']
            }

        # Get latest Treasury yields
        for maturity in ['1M', '3M', '6M', '1Y', '2Y', '3Y', '5Y', '7Y', '10Y', '20Y', '30Y']:
            cursor.execute('''
                SELECT value, date FROM macro_data
                WHERE data_type = 'yield' AND series_id LIKE ?
                ORDER BY date DESC LIMIT 1
            ''', (f'%{maturity}%',))
            row = cursor.fetchone()
            if row:
                snapshot[f'yield_{maturity}'] = {
                    'value': row['value'],
                    'date': row['date']
                }

        return snapshot

    def close(self):
        """Close database connection"""
        if self.conn:
            self.conn.close()
