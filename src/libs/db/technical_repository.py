"""
Technical repository - Price history and technical indicators
"""

import pandas as pd
from typing import Dict, List, Optional
import json


class TechnicalRepository:
    """Handles price history and technical indicators persistence"""

    def __init__(self, conn):
        """
        Initialize with database connection

        Args:
            conn: SQLite connection object
        """
        self.conn = conn

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
