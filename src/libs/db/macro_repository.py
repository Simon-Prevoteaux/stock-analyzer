"""
Macro repository - Macroeconomic data persistence
"""

import pandas as pd
from typing import Dict, List, Optional
from datetime import datetime


class MacroRepository:
    """Handles macroeconomic data persistence"""

    def __init__(self, conn):
        """
        Initialize with database connection

        Args:
            conn: SQLite connection object
        """
        self.conn = conn

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
