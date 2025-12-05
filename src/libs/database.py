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
                UNIQUE(ticker)
            )
        ''')

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
                    bubble_score, risk_level, last_updated
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                stock_data.get('last_updated')
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

    def close(self):
        """Close database connection"""
        if self.conn:
            self.conn.close()
