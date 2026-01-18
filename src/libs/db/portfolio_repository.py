"""
Portfolio repository - Manage user's portfolio
"""

import pandas as pd


class PortfolioRepository:
    """Handles portfolio persistence operations"""

    def __init__(self, conn):
        """
        Initialize with database connection

        Args:
            conn: SQLite connection object
        """
        self.conn = conn

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
            SELECT p.ticker, p.added_date, p.notes, s.*,
                   g.peg_average, g.peg_3y_cagr, g.peg_quarterly, g.peg_yfinance
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
