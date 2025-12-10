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

        # Watchlist table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS watchlist (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ticker TEXT NOT NULL,
                added_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                notes TEXT,
                UNIQUE(ticker)
            )
        ''')

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
                FOREIGN KEY (ticker) REFERENCES stocks(ticker)
            )
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
                    enterprise_value, target_price
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                stock_data.get('target_price')
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
        Retrieve all stocks from database

        Returns:
            DataFrame containing all stock data
        """
        query = 'SELECT * FROM stocks ORDER BY market_cap DESC'
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
            DataFrame containing high-risk stocks
        """
        query = 'SELECT * FROM stocks WHERE bubble_score >= ? ORDER BY bubble_score DESC'
        return pd.read_sql_query(query, self.conn, params=(min_bubble_score,))

    def get_value_stocks(self, max_pe: float = 20, max_ps: float = 3) -> pd.DataFrame:
        """
        Get potential value stocks

        Args:
            max_pe: Maximum P/E ratio
            max_ps: Maximum P/S ratio

        Returns:
            DataFrame containing value stocks
        """
        query = '''
            SELECT * FROM stocks
            WHERE pe_ratio <= ? AND ps_ratio <= ? AND is_profitable = 1
            ORDER BY ps_ratio ASC
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
            SELECT * FROM stocks
            WHERE is_profitable = 1
            AND NOT (pe_ratio <= 20 AND ps_ratio <= 3)
            AND (
                (pe_ratio > 20 AND pe_ratio <= 30 AND ps_ratio <= 5)
                OR (pe_ratio <= 25 AND ps_ratio > 3 AND ps_ratio <= 5)
            )
            ORDER BY
                CASE
                    WHEN pe_ratio <= 20 THEN ps_ratio - 3
                    WHEN ps_ratio <= 3 THEN pe_ratio - 20
                    ELSE (pe_ratio - 20) / 10.0 + (ps_ratio - 3) / 2.0
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
        """Get all stocks in watchlist"""
        query = '''
            SELECT w.ticker, w.added_date, w.notes, s.*
            FROM watchlist w
            LEFT JOIN stocks s ON w.ticker = s.ticker
            ORDER BY w.added_date DESC
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
        """Get all stocks in portfolio"""
        query = '''
            SELECT p.ticker, p.added_date, p.notes, s.*
            FROM portfolio p
            LEFT JOIN stocks s ON p.ticker = s.ticker
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

        Uses INSERT OR IGNORE to preserve existing historical data.
        This ensures that old data (no longer available from Yahoo Finance)
        is never lost when refreshing stock data.

        Args:
            ticker: Stock ticker
            financial_data: List of financial data points

        Returns:
            Number of NEW records saved (existing records are skipped)
        """
        if not financial_data:
            return 0

        cursor = self.conn.cursor()
        saved_count = 0

        for record in financial_data:
            try:
                cursor.execute('''
                    INSERT OR IGNORE INTO financial_history
                    (ticker, period_end_date, period_type, revenue, earnings,
                     gross_profit, operating_income, ebitda, net_income, eps, shares_outstanding)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                    record.get('shares_outstanding')
                ))
                # Check if row was actually inserted (rowcount > 0 means new record)
                if cursor.rowcount > 0:
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
                    oldest_data_date, newest_data_date, last_calculated
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
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
                metrics.get('newest_data_date')
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

    def close(self):
        """Close database connection"""
        if self.conn:
            self.conn.close()
