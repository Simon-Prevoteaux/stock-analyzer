"""
Database connection and schema management
"""

import sqlite3
from typing import Optional


class DatabaseConnection:
    """Manages SQLite database connection and schema"""

    def __init__(self, db_path: str = 'data/stocks.db'):
        """
        Initialize database connection

        Args:
            db_path: Path to SQLite database file
        """
        self.db_path = db_path
        self.conn: Optional[sqlite3.Connection] = None
        self._connect()
        self._create_tables()

    def _connect(self):
        """Establish database connection"""
        self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row

    def _create_tables(self):
        """Create all necessary database tables"""
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

        # Add columns for schema migrations
        self._add_column_if_not_exists(cursor, 'stocks', 'price_to_book', 'REAL')
        self._add_column_if_not_exists(cursor, 'stocks', 'current_ratio', 'REAL')
        self._add_column_if_not_exists(cursor, 'stocks', 'free_cash_flow', 'REAL')
        self._add_column_if_not_exists(cursor, 'stocks', 'enterprise_value', 'REAL')
        self._add_column_if_not_exists(cursor, 'stocks', 'target_price', 'REAL')
        self._add_column_if_not_exists(cursor, 'stocks', 'book_value', 'REAL')
        self._add_column_if_not_exists(cursor, 'stocks', 'dividend_rate', 'REAL')
        self._add_column_if_not_exists(cursor, 'stocks', 'dividend_yield', 'REAL')

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
        self._add_column_if_not_exists(cursor, 'watchlist', 'ranking', 'INTEGER DEFAULT 0')

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

        # Create additional tables
        self._create_financial_tables(cursor)
        self._create_technical_tables(cursor)
        self._create_macro_tables(cursor)

        self.conn.commit()

    def _add_column_if_not_exists(self, cursor, table: str, column: str, col_type: str):
        """Safely add a column to a table if it doesn't exist"""
        try:
            cursor.execute(f'ALTER TABLE {table} ADD COLUMN {column} {col_type}')
        except sqlite3.OperationalError:
            pass  # Column already exists

    def _create_financial_tables(self, cursor):
        """Create tables for financial history and growth metrics"""
        # Financial history table
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

        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_financial_ticker_date
            ON financial_history(ticker, period_end_date DESC)
        ''')

        # Add financial history columns
        for col in ['operating_cash_flow', 'capital_expenditures', 'free_cash_flow_calculated',
                    'gross_margin', 'operating_margin', 'profit_margin_quarterly']:
            self._add_column_if_not_exists(cursor, 'financial_history', col, 'REAL')

        # Growth metrics table
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

        # Add growth metrics columns
        for col in ['peg_3y_cagr', 'peg_quarterly', 'peg_yfinance', 'peg_average']:
            self._add_column_if_not_exists(cursor, 'growth_metrics', col, 'REAL')

        for col in ['fcf_cagr_3y', 'fcf_margin', 'rule_of_40', 'operating_leverage', 'cash_conversion_ratio']:
            self._add_column_if_not_exists(cursor, 'growth_metrics', col, 'REAL')

        for col in ['margin_trend', 'growth_stage']:
            self._add_column_if_not_exists(cursor, 'growth_metrics', col, 'TEXT')

    def _create_technical_tables(self, cursor):
        """Create tables for price history and technical indicators"""
        # Price history table
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

        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_price_history_ticker_date
            ON price_history(ticker, date DESC)
        ''')

        # Technical indicators cache table
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

    def _create_macro_tables(self, cursor):
        """Create tables for macroeconomic data"""
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

        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_macro_data_series_date
            ON macro_data(series_id, date DESC)
        ''')

    def close(self):
        """Close database connection"""
        if self.conn:
            self.conn.close()
