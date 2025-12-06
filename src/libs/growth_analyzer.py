"""
Growth Analysis Module
Calculates growth rates, trends, and consistency metrics from historical data
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional
from datetime import datetime, timedelta


class GrowthAnalyzer:
    """Analyzes historical financial data to compute growth metrics"""

    def __init__(self, financial_history: List[Dict]):
        """
        Initialize with historical financial data

        Args:
            financial_history: List of financial data points from database
        """
        self.df = pd.DataFrame(financial_history)
        if not self.df.empty:
            self.df['period_end_date'] = pd.to_datetime(self.df['period_end_date'])
            self.df = self.df.sort_values('period_end_date')

    def calculate_cagr(self, metric: str, years: int) -> Optional[float]:
        """
        Calculate Compound Annual Growth Rate

        Args:
            metric: Column name (e.g., 'revenue', 'earnings')
            years: Number of years to look back

        Returns:
            CAGR as decimal (e.g., 0.15 for 15%)
        """
        if self.df.empty or metric not in self.df.columns:
            return None

        # Filter to annual data for CAGR
        annual_data = self.df[self.df['period_type'] == 'annual'].copy()
        if len(annual_data) < 2:
            return None

        # Get data points separated by desired years
        cutoff_date = annual_data['period_end_date'].max() - timedelta(days=years*365)
        relevant_data = annual_data[annual_data['period_end_date'] >= cutoff_date]

        if len(relevant_data) < 2:
            return None

        start_value = relevant_data[metric].iloc[0]
        end_value = relevant_data[metric].iloc[-1]

        if not start_value or start_value <= 0 or not end_value:
            return None

        actual_years = (relevant_data['period_end_date'].iloc[-1] -
                       relevant_data['period_end_date'].iloc[0]).days / 365.25

        if actual_years < 0.5:
            return None

        cagr = (end_value / start_value) ** (1 / actual_years) - 1
        return cagr

    def calculate_average_quarterly_growth(self, metric: str, periods: int = 8) -> Optional[float]:
        """
        Calculate average quarter-over-quarter growth rate

        Args:
            metric: Column name
            periods: Number of recent quarters to analyze

        Returns:
            Average QoQ growth rate
        """
        if self.df.empty or metric not in self.df.columns:
            return None

        quarterly_data = self.df[self.df['period_type'] == 'quarterly'].copy()
        if len(quarterly_data) < 2:
            return None

        # Take most recent N quarters
        recent_data = quarterly_data.tail(periods + 1)

        # Calculate growth rates
        growth_rates = []
        for i in range(1, len(recent_data)):
            prev_val = recent_data[metric].iloc[i-1]
            curr_val = recent_data[metric].iloc[i]

            if prev_val and prev_val > 0 and curr_val:
                growth = (curr_val - prev_val) / prev_val
                growth_rates.append(growth)

        if not growth_rates:
            return None

        return np.mean(growth_rates)

    def calculate_consistency_score(self, metric: str, periods: int = 12) -> float:
        """
        Calculate consistency score (0-100) based on growth volatility

        Higher score = more consistent growth

        Args:
            metric: Column name
            periods: Number of quarters to analyze

        Returns:
            Score from 0-100
        """
        if self.df.empty or metric not in self.df.columns:
            return 0.0

        quarterly_data = self.df[self.df['period_type'] == 'quarterly'].copy()
        if len(quarterly_data) < 3:
            return 0.0

        recent_data = quarterly_data.tail(periods + 1)

        # Calculate quarter-over-quarter growth rates
        growth_rates = []
        positive_count = 0

        for i in range(1, len(recent_data)):
            prev_val = recent_data[metric].iloc[i-1]
            curr_val = recent_data[metric].iloc[i]

            if prev_val and prev_val != 0 and curr_val:
                growth = (curr_val - prev_val) / prev_val
                growth_rates.append(growth)
                if growth > 0:
                    positive_count += 1

        if not growth_rates:
            return 0.0

        # Components of consistency score:
        # 1. Percentage of positive growth periods (40 points)
        positive_score = (positive_count / len(growth_rates)) * 40

        # 2. Low volatility (30 points) - inverse of coefficient of variation
        std_dev = np.std(growth_rates)
        mean_growth = np.mean(growth_rates)
        if abs(mean_growth) > 0.01:
            cv = std_dev / abs(mean_growth)
            volatility_score = max(0, 30 - (cv * 10))  # Lower CV = higher score
        else:
            volatility_score = 0

        # 3. Positive average growth (30 points)
        avg_growth_score = 30 if mean_growth > 0 else 0

        total_score = positive_score + volatility_score + avg_growth_score
        return min(100, max(0, total_score))

    def detect_growth_acceleration(self, metric: str) -> bool:
        """
        Detect if growth is accelerating (recent growth > historical average)

        Args:
            metric: Column name

        Returns:
            True if growth is accelerating
        """
        if self.df.empty or metric not in self.df.columns:
            return False

        quarterly_data = self.df[self.df['period_type'] == 'quarterly'].copy()
        if len(quarterly_data) < 6:
            return False

        # Recent growth (last 4 quarters)
        recent_growth = self.calculate_average_quarterly_growth(metric, periods=4)

        # Historical growth (all available data)
        historical_growth = self.calculate_average_quarterly_growth(
            metric,
            periods=len(quarterly_data) - 1
        )

        if recent_growth is None or historical_growth is None:
            return False

        # Accelerating if recent > historical by significant margin
        return recent_growth > (historical_growth * 1.1)

    def count_consecutive_profitable_quarters(self) -> int:
        """Count consecutive quarters with positive earnings"""
        if self.df.empty or 'net_income' not in self.df.columns:
            return 0

        quarterly_data = self.df[self.df['period_type'] == 'quarterly'].copy()
        if quarterly_data.empty:
            return 0

        # Start from most recent and count backwards
        consecutive = 0
        for _, row in quarterly_data.iloc[::-1].iterrows():
            if row['net_income'] and row['net_income'] > 0:
                consecutive += 1
            else:
                break

        return consecutive

    def calculate_all_metrics(self) -> Dict:
        """
        Calculate all growth metrics

        Returns:
            Dictionary with all calculated metrics
        """
        if self.df.empty:
            return {}

        metrics = {
            # CAGR metrics
            'revenue_cagr_3y': self.calculate_cagr('revenue', 3),
            'revenue_cagr_5y': self.calculate_cagr('revenue', 5),
            'earnings_cagr_3y': self.calculate_cagr('earnings', 3),
            'earnings_cagr_5y': self.calculate_cagr('earnings', 5),

            # Quarterly averages
            'avg_quarterly_revenue_growth': self.calculate_average_quarterly_growth('revenue'),
            'avg_quarterly_earnings_growth': self.calculate_average_quarterly_growth('earnings'),

            # Consistency scores
            'revenue_consistency_score': self.calculate_consistency_score('revenue'),
            'earnings_consistency_score': self.calculate_consistency_score('earnings'),

            # Trends
            'revenue_growth_accelerating': self.detect_growth_acceleration('revenue'),
            'earnings_growth_accelerating': self.detect_growth_acceleration('earnings'),
            'consecutive_profitable_quarters': self.count_consecutive_profitable_quarters(),

            # Metadata
            'data_points_count': len(self.df),
            'oldest_data_date': self.df['period_end_date'].min().strftime('%Y-%m-%d'),
            'newest_data_date': self.df['period_end_date'].max().strftime('%Y-%m-%d'),
        }

        return metrics
