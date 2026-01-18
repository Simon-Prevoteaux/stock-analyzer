"""
Watchlist repository - Manage user's watchlist
"""

import pandas as pd


class WatchlistRepository:
    """Handles watchlist persistence operations"""

    def __init__(self, conn):
        """
        Initialize with database connection

        Args:
            conn: SQLite connection object
        """
        self.conn = conn

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
            SELECT w.ticker, w.added_date, w.notes, w.ranking, s.*,
                   g.peg_average, g.peg_3y_cagr, g.peg_quarterly, g.peg_yfinance
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
