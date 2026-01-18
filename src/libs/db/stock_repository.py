"""
Stock data repository - CRUD operations for stock data
"""

import pandas as pd
from typing import Dict, List, Optional
from datetime import datetime
import json


class StockRepository:
    """Handles stock data persistence operations"""

    def __init__(self, conn):
        """
        Initialize with database connection

        Args:
            conn: SQLite connection object
        """
        self.conn = conn

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
