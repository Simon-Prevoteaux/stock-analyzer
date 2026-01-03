"""
Technical Analysis Module
Calculates support/resistance levels, trends, and price targets
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timedelta
from scipy import stats


class TechnicalAnalyzer:
    """Analyzes price history to compute technical indicators"""

    def __init__(self, price_history_df: pd.DataFrame):
        """
        Initialize with historical price data

        Args:
            price_history_df: DataFrame with columns: date, open, high, low, close, volume
        """
        self.df = price_history_df.copy()
        if not self.df.empty:
            self.df['date'] = pd.to_datetime(self.df['date'])
            self.df = self.df.sort_values('date')
            self.df.reset_index(drop=True, inplace=True)

    def calculate_support_resistance(self,
                                     window: int = 20,
                                     num_levels: int = 3) -> Dict[str, List[float]]:
        """
        Calculate support and resistance levels using local maxima/minima

        Args:
            window: Rolling window size for peak detection
            num_levels: Number of support/resistance levels to identify

        Returns:
            Dictionary with 'support_levels' and 'resistance_levels' lists
        """
        if self.df.empty or len(self.df) < window * 2:
            return {'support_levels': [], 'resistance_levels': []}

        highs = self.df['high'].values
        lows = self.df['low'].values

        # Find local maxima (resistance)
        resistance_points = []
        for i in range(window, len(highs) - window):
            if highs[i] == max(highs[i-window:i+window+1]):
                resistance_points.append(highs[i])

        # Find local minima (support)
        support_points = []
        for i in range(window, len(lows) - window):
            if lows[i] == min(lows[i-window:i+window+1]):
                support_points.append(lows[i])

        # Cluster nearby levels (within 2%)
        def cluster_levels(levels, tolerance=0.02):
            if not levels:
                return []

            levels_sorted = sorted(levels)
            clusters = []
            current_cluster = [levels_sorted[0]]

            for level in levels_sorted[1:]:
                if (level - current_cluster[-1]) / current_cluster[-1] <= tolerance:
                    current_cluster.append(level)
                else:
                    clusters.append(np.mean(current_cluster))
                    current_cluster = [level]

            clusters.append(np.mean(current_cluster))
            return clusters

        support_clustered = cluster_levels(support_points)
        resistance_clustered = cluster_levels(resistance_points)

        # Get top N strongest levels (those tested multiple times)
        support_levels = sorted(support_clustered)[-num_levels:] if support_clustered else []
        resistance_levels = sorted(resistance_clustered)[:num_levels] if resistance_clustered else []

        return {
            'support_levels': [round(s, 2) for s in support_levels],
            'resistance_levels': [round(r, 2) for r in resistance_levels]
        }

    def calculate_pivot_points(self) -> Dict[str, float]:
        """
        Calculate classical pivot points from most recent period

        Returns:
            Dictionary with pivot, support, and resistance levels
        """
        if self.df.empty:
            return {}

        # Use last available period
        last_row = self.df.iloc[-1]
        high = last_row['high']
        low = last_row['low']
        close = last_row['close']

        pivot = (high + low + close) / 3

        resistance_1 = (2 * pivot) - low
        support_1 = (2 * pivot) - high

        resistance_2 = pivot + (high - low)
        support_2 = pivot - (high - low)

        resistance_3 = high + 2 * (pivot - low)
        support_3 = low - 2 * (high - pivot)

        return {
            'pivot': round(pivot, 2),
            'r1': round(resistance_1, 2),
            'r2': round(resistance_2, 2),
            'r3': round(resistance_3, 2),
            's1': round(support_1, 2),
            's2': round(support_2, 2),
            's3': round(support_3, 2)
        }

    def calculate_trend(self, period_days: int = 90) -> Dict:
        """
        Calculate trend using linear regression on recent data

        Args:
            period_days: Number of recent days to analyze

        Returns:
            Dictionary with trend metrics and projections
        """
        if self.df.empty:
            return {}

        # Use most recent period_days
        recent_df = self.df.tail(period_days).copy()

        if len(recent_df) < 10:  # Need minimum data points
            return {}

        # Prepare data for regression
        recent_df['days_index'] = range(len(recent_df))
        X = recent_df['days_index'].values
        y = recent_df['close'].values

        # Linear regression
        slope, intercept, r_value, p_value, std_err = stats.linregress(X, y)

        # Calculate projections
        last_index = len(recent_df) - 1
        current_price = y[-1]

        # Project 30 days and 90 days forward
        target_30d = intercept + slope * (last_index + 30)
        target_90d = intercept + slope * (last_index + 90)

        # Determine trend direction
        trend_direction = 'Bullish' if slope > 0 else 'Bearish'

        # Calculate moving averages for confirmation
        ma_20 = recent_df['close'].tail(20).mean() if len(recent_df) >= 20 else None
        ma_50 = recent_df['close'].tail(50).mean() if len(recent_df) >= 50 else None

        return {
            'slope': round(slope, 4),
            'r_squared': round(r_value ** 2, 4),
            'trend_direction': trend_direction,
            'current_price': round(current_price, 2),
            'target_30d': round(target_30d, 2),
            'target_90d': round(target_90d, 2),
            'upside_30d_percent': round(((target_30d - current_price) / current_price) * 100, 2),
            'upside_90d_percent': round(((target_90d - current_price) / current_price) * 100, 2),
            'ma_20': round(ma_20, 2) if ma_20 else None,
            'ma_50': round(ma_50, 2) if ma_50 else None,
            'p_value': round(p_value, 4)
        }

    def calculate_support_resistance_targets(self, current_price: float) -> Dict:
        """
        Calculate price targets based on support/resistance levels

        Args:
            current_price: Current stock price

        Returns:
            Dictionary with target prices and potential moves
        """
        sr_levels = self.calculate_support_resistance()
        support_levels = sr_levels['support_levels']
        resistance_levels = sr_levels['resistance_levels']

        # Find next resistance above current price
        next_resistance = None
        for r in sorted(resistance_levels):
            if r > current_price:
                next_resistance = r
                break

        # Find next support below current price
        next_support = None
        for s in sorted(support_levels, reverse=True):
            if s < current_price:
                next_support = s
                break

        result = {
            'current_price': round(current_price, 2),
            'next_resistance': next_resistance,
            'next_support': next_support
        }

        if next_resistance:
            result['upside_to_resistance'] = round(
                ((next_resistance - current_price) / current_price) * 100, 2
            )

        if next_support:
            result['downside_to_support'] = round(
                ((current_price - next_support) / current_price) * 100, 2
            )

        return result

    def calculate_all_indicators(self, current_price: float) -> Dict:
        """
        Calculate comprehensive technical analysis

        Args:
            current_price: Current stock price

        Returns:
            Dictionary with all technical indicators
        """
        sr_levels = self.calculate_support_resistance()
        pivot_points = self.calculate_pivot_points()
        trend = self.calculate_trend()
        sr_targets = self.calculate_support_resistance_targets(current_price)

        return {
            'support_levels': sr_levels['support_levels'],
            'resistance_levels': sr_levels['resistance_levels'],
            'pivot_points': pivot_points,
            'trend': trend,
            'price_targets': sr_targets,
            'data_points': len(self.df),
            'oldest_date': self.df['date'].min().strftime('%Y-%m-%d') if not self.df.empty else None,
            'newest_date': self.df['date'].max().strftime('%Y-%m-%d') if not self.df.empty else None
        }

    def get_chart_data(self, include_indicators: bool = True) -> Dict:
        """
        Prepare data for Chart.js visualization

        Args:
            include_indicators: Whether to include MA and Bollinger Bands

        Returns:
            Dictionary formatted for Chart.js
        """
        if self.df.empty:
            return {'labels': [], 'datasets': []}

        # Prepare price data
        labels = self.df['date'].dt.strftime('%Y-%m-%d').tolist()
        close_prices = self.df['close'].tolist()

        datasets = [
            {
                'label': 'Close Price',
                'data': close_prices,
                'borderColor': '#00ff88',
                'backgroundColor': 'rgba(0, 255, 136, 0.1)',
                'fill': False,
                'borderWidth': 2,
                'tension': 0.1
            }
        ]

        if include_indicators and len(self.df) >= 20:
            # Add 20-day moving average
            ma_20 = self.df['close'].rolling(window=20).mean().tolist()
            datasets.append({
                'label': '20-Day MA',
                'data': ma_20,
                'borderColor': '#ffd700',
                'backgroundColor': 'rgba(255, 215, 0, 0.1)',
                'fill': False,
                'borderWidth': 1,
                'borderDash': [5, 5]
            })

        if include_indicators and len(self.df) >= 50:
            # Add 50-day moving average
            ma_50 = self.df['close'].rolling(window=50).mean().tolist()
            datasets.append({
                'label': '50-Day MA',
                'data': ma_50,
                'borderColor': '#ff3366',
                'backgroundColor': 'rgba(255, 51, 102, 0.1)',
                'fill': False,
                'borderWidth': 1,
                'borderDash': [10, 5]
            })

        return {
            'labels': labels,
            'datasets': datasets
        }
