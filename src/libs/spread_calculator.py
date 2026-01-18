"""
Spread Calculator - Yield spread and credit spread calculations

Extracted from MacroDataFetcher for better modularity.
Handles calculation of yield curve spreads and credit spreads
with historical context and trend analysis.
"""

import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, Optional


class SpreadCalculator:
    """
    Calculates and analyzes yield spreads and credit spreads.

    This class handles the calculation logic for yield curve spreads (10Y-2Y, 10Y-3M, 30Y-5Y)
    and provides historical context and trend analysis.
    """

    def __init__(self, data_fetcher):
        """
        Initialize with a data fetcher that provides yield data.

        Args:
            data_fetcher: MacroDataFetcher instance for fetching FRED data
        """
        self.fetcher = data_fetcher

    def _get_historical_spread(self, df1: pd.DataFrame, df2: pd.DataFrame, days_back: int) -> Optional[float]:
        """
        Get spread value from days_back ago.

        Args:
            df1: DataFrame with date and value for first yield
            df2: DataFrame with date and value for second yield
            days_back: Number of days to look back

        Returns:
            Spread value or None if data not available
        """
        if df1.empty or df2.empty:
            return None

        target_date = df1.iloc[-1]['date'] - timedelta(days=days_back)
        hist_df1 = df1[df1['date'] <= target_date]
        hist_df2 = df2[df2['date'] <= target_date]

        if not hist_df1.empty and not hist_df2.empty:
            return hist_df1.iloc[-1]['value'] - hist_df2.iloc[-1]['value']
        return None

    def _calculate_single_spread(self, df_long: pd.DataFrame, df_short: pd.DataFrame,
                                  trend_threshold: float = 0.1) -> Optional[Dict]:
        """
        Calculate a single spread with historical context and trend.

        Args:
            df_long: DataFrame for longer maturity yield
            df_short: DataFrame for shorter maturity yield
            trend_threshold: Threshold for determining trend direction

        Returns:
            Dict with spread data or None if insufficient data
        """
        if df_long.empty or df_short.empty:
            return None

        current_long = df_long.iloc[-1]['value']
        current_short = df_short.iloc[-1]['value']
        spread_current = current_long - current_short

        # Historical spreads at different lookbacks
        spread_1m = self._get_historical_spread(df_long, df_short, 30)
        spread_3m = self._get_historical_spread(df_long, df_short, 90)
        spread_6m = self._get_historical_spread(df_long, df_short, 180)
        spread_1y = self._get_historical_spread(df_long, df_short, 365)

        # Calculate changes
        change_1m = (spread_current - spread_1m) if spread_1m is not None else None
        change_3m = (spread_current - spread_3m) if spread_3m is not None else None
        change_6m = (spread_current - spread_6m) if spread_6m is not None else None
        change_1y = (spread_current - spread_1y) if spread_1y is not None else None

        # Determine trend
        trend = None
        if change_3m is not None:
            if change_3m > trend_threshold:
                trend = 'EXPANDING'
            elif change_3m < -trend_threshold:
                trend = 'CONTRACTING'
            else:
                trend = 'STABLE'

        return {
            'current': round(spread_current, 2),
            '1m_ago': round(spread_1m, 2) if spread_1m is not None else None,
            '3m_ago': round(spread_3m, 2) if spread_3m is not None else None,
            '6m_ago': round(spread_6m, 2) if spread_6m is not None else None,
            '1y_ago': round(spread_1y, 2) if spread_1y is not None else None,
            'change_1m': round(change_1m, 2) if change_1m is not None else None,
            'change_3m': round(change_3m, 2) if change_3m is not None else None,
            'change_6m': round(change_6m, 2) if change_6m is not None else None,
            'change_1y': round(change_1y, 2) if change_1y is not None else None,
            'trend': trend,
        }

    def calculate_yield_spreads(self) -> Dict:
        """
        Calculate key yield spreads with historical context and trend analysis.

        Calculates:
        - 10Y-2Y spread (classic recession indicator)
        - 10Y-3M spread (Fed's preferred measure)
        - 30Y-5Y spread (long-term curve shape)

        Returns:
            Dict with spread calculations, historical values, and trend indicators
        """
        lookback_days = 1095  # 3 years for context
        start_date = (datetime.now() - timedelta(days=lookback_days)).strftime('%Y-%m-%d')

        # Fetch required yield data
        yields_10y = self.fetcher._fetch_series(
            self.fetcher.TREASURY_SERIES['10Y'], start_date=start_date)
        yields_2y = self.fetcher._fetch_series(
            self.fetcher.TREASURY_SERIES['2Y'], start_date=start_date)
        yields_3m = self.fetcher._fetch_series(
            self.fetcher.TREASURY_SERIES['3M'], start_date=start_date)
        yields_30y = self.fetcher._fetch_series(
            self.fetcher.TREASURY_SERIES['30Y'], start_date=start_date)
        yields_5y = self.fetcher._fetch_series(
            self.fetcher.TREASURY_SERIES['5Y'], start_date=start_date)

        spreads = {}

        # 10Y-2Y spread
        spread_10y2y = self._calculate_single_spread(yields_10y, yields_2y, trend_threshold=0.1)
        if spread_10y2y:
            spreads['10y2y'] = spread_10y2y

        # 10Y-3M spread
        spread_10y3m = self._calculate_single_spread(yields_10y, yields_3m, trend_threshold=0.1)
        if spread_10y3m:
            spreads['10y3m'] = spread_10y3m

        # 30Y-5Y spread (lower threshold due to smaller typical movements)
        spread_30y5y = self._calculate_single_spread(yields_30y, yields_5y, trend_threshold=0.05)
        if spread_30y5y:
            spreads['30y5y'] = spread_30y5y

        return spreads

    def get_spread_history(self, lookback_days: int = 365) -> Dict:
        """
        Get historical spread data for charting.

        Args:
            lookback_days: Days of history to fetch (default 1 year)

        Returns:
            Dict with dates and spread values for each spread type
        """
        start_date = (datetime.now() - timedelta(days=lookback_days)).strftime('%Y-%m-%d')

        yields_10y = self.fetcher._fetch_series(
            self.fetcher.TREASURY_SERIES['10Y'], start_date=start_date)
        yields_2y = self.fetcher._fetch_series(
            self.fetcher.TREASURY_SERIES['2Y'], start_date=start_date)
        yields_3m = self.fetcher._fetch_series(
            self.fetcher.TREASURY_SERIES['3M'], start_date=start_date)
        yields_30y = self.fetcher._fetch_series(
            self.fetcher.TREASURY_SERIES['30Y'], start_date=start_date)
        yields_5y = self.fetcher._fetch_series(
            self.fetcher.TREASURY_SERIES['5Y'], start_date=start_date)

        history = {
            'dates': [],
            '10y2y': [],
            '10y3m': [],
            '30y5y': []
        }

        # 10Y-2Y spread history
        if not yields_10y.empty and not yields_2y.empty:
            merged = pd.merge(yields_10y, yields_2y, on='date', suffixes=('_10y', '_2y'))

            for _, row in merged.iterrows():
                date_str = row['date'].strftime('%Y-%m-%d')
                if date_str not in history['dates']:
                    history['dates'].append(date_str)

                spread = row['value_10y'] - row['value_2y']
                history['10y2y'].append(round(spread, 2))

        # 10Y-3M spread history
        if not yields_10y.empty and not yields_3m.empty:
            merged = pd.merge(yields_10y, yields_3m, on='date', suffixes=('_10y', '_3m'))

            if not history['dates']:
                for _, row in merged.iterrows():
                    history['dates'].append(row['date'].strftime('%Y-%m-%d'))

            for _, row in merged.iterrows():
                spread = row['value_10y'] - row['value_3m']
                history['10y3m'].append(round(spread, 2))

        # 30Y-5Y spread history
        if not yields_30y.empty and not yields_5y.empty:
            merged = pd.merge(yields_30y, yields_5y, on='date', suffixes=('_30y', '_5y'))

            if not history['dates']:
                for _, row in merged.iterrows():
                    history['dates'].append(row['date'].strftime('%Y-%m-%d'))

            for _, row in merged.iterrows():
                spread = row['value_30y'] - row['value_5y']
                history['30y5y'].append(round(spread, 2))

        return history

    def calculate_credit_spreads(self) -> Dict:
        """
        Calculate corporate credit spreads with historical percentile rankings.

        Returns:
            Dict with current spreads and percentile rankings for:
            - Investment Grade (corporate_master)
            - High Yield
            - BBB
        """
        lookback_days = 3650  # 10 years for percentile calc

        spreads = {}

        for spread_type, series_id in self.fetcher.CREDIT_SPREAD_SERIES.items():
            df = self.fetcher._fetch_series(
                series_id,
                start_date=(datetime.now() - timedelta(days=lookback_days)).strftime('%Y-%m-%d')
            )

            if not df.empty:
                current_value = df.iloc[-1]['value']

                # Calculate percentile (what percentage of historical values are below current)
                percentile = (df['value'] <= current_value).sum() / len(df) * 100

                spreads[spread_type] = {
                    'current': round(current_value, 0),
                    'percentile': round(percentile, 1),
                }
            else:
                spreads[spread_type] = {
                    'current': None,
                    'percentile': None,
                }

        return spreads
