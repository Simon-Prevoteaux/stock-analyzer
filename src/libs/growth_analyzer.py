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

        # Filter to annual data for CAGR and exclude zero values
        annual_data = self.df[self.df['period_type'] == 'annual'].copy()
        annual_data = annual_data[annual_data[metric].notna() & (annual_data[metric] != 0)]

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

        # Filter quarterly data and exclude zero values
        quarterly_data = self.df[self.df['period_type'] == 'quarterly'].copy()
        quarterly_data = quarterly_data[quarterly_data[metric].notna() & (quarterly_data[metric] != 0)]

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

        # Filter quarterly data and exclude zero values
        quarterly_data = self.df[self.df['period_type'] == 'quarterly'].copy()
        quarterly_data = quarterly_data[quarterly_data[metric].notna() & (quarterly_data[metric] != 0)]

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

        # Filter quarterly data and exclude zero values
        quarterly_data = self.df[self.df['period_type'] == 'quarterly'].copy()
        quarterly_data = quarterly_data[quarterly_data[metric].notna() & (quarterly_data[metric] != 0)]

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

        # More permissive: Accelerating if recent growth is positive and >= historical
        # OR if recent growth is at least 5% higher than historical
        # This catches both consistent growers and true accelerators
        if historical_growth <= 0:
            # If historical was flat/negative, any positive recent growth counts
            return recent_growth > 0
        else:
            # If both positive, recent just needs to match or exceed historical by 5%+
            return recent_growth >= (historical_growth * 1.05)

    def count_consecutive_profitable_quarters(self) -> int:
        """Count consecutive quarters with positive earnings"""
        if self.df.empty or 'net_income' not in self.df.columns:
            return 0

        # Filter quarterly data and exclude zero values
        quarterly_data = self.df[self.df['period_type'] == 'quarterly'].copy()
        quarterly_data = quarterly_data[quarterly_data['net_income'].notna() & (quarterly_data['net_income'] != 0)]

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

    def calculate_peg_ratio(self, pe_ratio: Optional[float]) -> Dict[str, Optional[float]]:
        """
        Calculate PEG ratio (Price/Earnings to Growth) using Peter Lynch's methodology

        PEG = P/E Ratio / Earnings Growth Rate (in %)

        We calculate 3 PEG ratios using different growth measures:
        1. peg_3y_cagr: Using 3-year CAGR
        2. peg_quarterly: Using annualized quarterly average growth
        3. peg_yfinance: Using yfinance earnings growth (fallback)

        Interpretation (Peter Lynch):
        - PEG < 1.0: Undervalued (good)
        - PEG 1.0-2.0: Fairly valued (OK)
        - PEG > 2.0: Overvalued (high)

        Args:
            pe_ratio: Current P/E ratio from stock data

        Returns:
            Dictionary with peg_3y_cagr, peg_quarterly, peg_yfinance, peg_average
        """
        result = {
            'peg_3y_cagr': None,
            'peg_quarterly': None,
            'peg_yfinance': None,
            'peg_average': None
        }

        # Can't calculate PEG without P/E ratio
        if not pe_ratio or pe_ratio <= 0:
            return result

        # Get earnings growth rates
        earnings_cagr_3y = self.calculate_cagr('earnings', 3)
        avg_quarterly_earnings = self.calculate_average_quarterly_growth('earnings')

        peg_values = []

        # PEG using 3-year CAGR
        if earnings_cagr_3y and earnings_cagr_3y > 0:
            growth_percent = earnings_cagr_3y * 100  # Convert to percentage
            result['peg_3y_cagr'] = pe_ratio / growth_percent
            peg_values.append(result['peg_3y_cagr'])

        # PEG using annualized quarterly average
        if avg_quarterly_earnings and avg_quarterly_earnings > 0:
            # Annualize quarterly growth: (1 + quarterly_rate)^4 - 1
            annualized_growth = ((1 + avg_quarterly_earnings) ** 4 - 1) * 100
            result['peg_quarterly'] = pe_ratio / annualized_growth
            peg_values.append(result['peg_quarterly'])

        # Calculate average of available PEG ratios
        if peg_values:
            result['peg_average'] = sum(peg_values) / len(peg_values)

        return result

    def calculate_fcf_metrics(self, current_fcf: Optional[float] = None, current_revenue: Optional[float] = None) -> Dict:
        """
        Calculate Free Cash Flow metrics

        Args:
            current_fcf: Current free cash flow (optional)
            current_revenue: Current revenue (optional)

        Returns:
            Dictionary with fcf_cagr_3y, fcf_margin, cash_conversion_ratio
        """
        result = {
            'fcf_cagr_3y': None,
            'fcf_margin': None,
            'cash_conversion_ratio': None
        }

        # Calculate FCF CAGR if we have historical FCF data
        if 'free_cash_flow_calculated' in self.df.columns:
            result['fcf_cagr_3y'] = self.calculate_cagr('free_cash_flow_calculated', 3)

        # Calculate FCF Margin = FCF / Revenue
        if current_fcf is not None and current_revenue is not None and current_revenue > 0:
            result['fcf_margin'] = current_fcf / current_revenue

        # Calculate Cash Conversion Ratio = FCF / Net Income
        if current_fcf is not None and 'net_income' in self.df.columns:
            quarterly_data = self.df[self.df['period_type'] == 'quarterly'].copy()
            if not quarterly_data.empty:
                latest_net_income = quarterly_data['net_income'].iloc[-1]
                if latest_net_income and latest_net_income > 0:
                    result['cash_conversion_ratio'] = current_fcf / latest_net_income

        return result

    def calculate_rule_of_40(self, revenue_growth: Optional[float], fcf_margin: Optional[float]) -> Optional[float]:
        """
        Calculate Rule of 40 (Growth + Profitability metric for SaaS)

        Rule of 40 = Revenue Growth % + FCF Margin %

        Args:
            revenue_growth: Revenue growth rate (as decimal, e.g., 0.25 for 25%)
            fcf_margin: FCF margin (as decimal, e.g., 0.15 for 15%)

        Returns:
            Rule of 40 score (as number, e.g., 40 for 40%)
        """
        if revenue_growth is None or fcf_margin is None:
            return None

        return (revenue_growth * 100) + (fcf_margin * 100)

    def calculate_operating_leverage(self) -> Optional[float]:
        """
        Calculate Operating Leverage = Earnings Growth / Revenue Growth

        Operating leverage > 1.0 indicates improving profitability
        (earnings growing faster than revenue)

        Returns:
            Operating leverage ratio
        """
        earnings_growth = self.calculate_average_quarterly_growth('earnings')
        revenue_growth = self.calculate_average_quarterly_growth('revenue')

        if earnings_growth is None or revenue_growth is None or revenue_growth <= 0:
            return None

        return earnings_growth / revenue_growth

    def calculate_margin_trend(self) -> Optional[str]:
        """
        Analyze if profit margins are expanding, contracting, or stable

        Compares recent quarters vs earlier quarters (adaptive based on data availability)
        - If 8+ quarters: compare recent 4Q vs previous 4Q
        - If 6-7 quarters: compare recent 3Q vs previous 3Q
        - If 4-5 quarters: compare recent 2Q vs previous 2Q
        - If <4 quarters: insufficient data

        Returns:
            'expanding', 'contracting', or 'stable'
        """
        if self.df.empty or 'profit_margin_quarterly' not in self.df.columns:
            return None

        # Filter quarterly data with valid margins
        quarterly_data = self.df[self.df['period_type'] == 'quarterly'].copy()
        quarterly_data = quarterly_data[quarterly_data['profit_margin_quarterly'].notna()]

        total_quarters = len(quarterly_data)

        if total_quarters < 4:
            return None

        # Adaptive comparison based on available data
        if total_quarters >= 8:
            # Compare recent 4Q vs previous 4Q
            recent_data = quarterly_data.tail(8)
            previous_avg = recent_data.head(4)['profit_margin_quarterly'].mean()
            recent_avg = recent_data.tail(4)['profit_margin_quarterly'].mean()
        elif total_quarters >= 6:
            # Compare recent 3Q vs previous 3Q
            recent_data = quarterly_data.tail(6)
            previous_avg = recent_data.head(3)['profit_margin_quarterly'].mean()
            recent_avg = recent_data.tail(3)['profit_margin_quarterly'].mean()
        else:
            # Compare recent 2Q vs previous 2Q (for 4-5 quarters)
            recent_data = quarterly_data.tail(4)
            previous_avg = recent_data.head(2)['profit_margin_quarterly'].mean()
            recent_avg = recent_data.tail(2)['profit_margin_quarterly'].mean()

        if pd.isna(previous_avg) or pd.isna(recent_avg):
            return None

        # Determine trend (use 10% threshold for significant change)
        change_pct = (recent_avg - previous_avg) / abs(previous_avg) if previous_avg != 0 else 0

        if change_pct > 0.10:
            return 'expanding'
        elif change_pct < -0.10:
            return 'contracting'
        else:
            return 'stable'

    def classify_growth_stage(self, metrics: Dict) -> Optional[str]:
        """
        Classify stock into growth lifecycle stage

        Stages:
        - early_growth: Very high growth (>50%), low margins, high volatility
        - rapid_growth: High growth (20-50%), accelerating
        - mature_growth: Moderate growth (5-20%), consistent, profitable
        - inflection: Accelerating from lower base (<20% historical)
        - declining: Negative or very low growth

        Args:
            metrics: Dictionary containing calculated metrics (CAGR, margins, etc.)

        Returns:
            Growth stage classification string
        """
        cagr = metrics.get('earnings_cagr_3y', 0) or 0
        revenue_cagr = metrics.get('revenue_cagr_3y', 0) or 0

        # Use whichever is available (prefer earnings CAGR)
        primary_cagr = cagr if cagr != 0 else revenue_cagr

        # Get additional context
        accelerating = metrics.get('revenue_growth_accelerating', False) or metrics.get('earnings_growth_accelerating', False)

        # Get profit margin from current data if available
        profit_margin = 0
        if 'profit_margin_quarterly' in self.df.columns:
            quarterly_data = self.df[self.df['period_type'] == 'quarterly'].copy()
            if not quarterly_data.empty and 'profit_margin_quarterly' in quarterly_data.columns:
                latest_margin = quarterly_data['profit_margin_quarterly'].iloc[-1]
                if pd.notna(latest_margin):
                    profit_margin = latest_margin

        # Classification logic
        if primary_cagr > 0.50 and profit_margin < 0.10:
            return 'early_growth'
        elif primary_cagr >= 0.20 and primary_cagr <= 0.50 and accelerating:
            return 'rapid_growth'
        elif primary_cagr >= 0.05 and primary_cagr < 0.20 and profit_margin > 0.10:
            return 'mature_growth'
        elif accelerating and primary_cagr < 0.20:
            return 'inflection'
        elif primary_cagr < 0:
            return 'declining'
        else:
            return 'stable'

    def calculate_all_metrics(self, current_fcf: Optional[float] = None,
                             current_revenue: Optional[float] = None) -> Dict:
        """
        Calculate all growth metrics

        Args:
            current_fcf: Current free cash flow (optional, for FCF metrics)
            current_revenue: Current revenue (optional, for FCF margin and Rule of 40)

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

        # Add new FCF metrics
        fcf_metrics = self.calculate_fcf_metrics(current_fcf, current_revenue)
        metrics.update(fcf_metrics)

        # Add operating leverage
        metrics['operating_leverage'] = self.calculate_operating_leverage()

        # Add margin trend
        metrics['margin_trend'] = self.calculate_margin_trend()

        # Calculate Rule of 40 if we have FCF margin and revenue growth
        if fcf_metrics['fcf_margin'] is not None and metrics.get('revenue_cagr_3y') is not None:
            metrics['rule_of_40'] = self.calculate_rule_of_40(
                metrics['revenue_cagr_3y'],
                fcf_metrics['fcf_margin']
            )
        else:
            metrics['rule_of_40'] = None

        # Classify growth stage (needs metrics to be calculated first)
        metrics['growth_stage'] = self.classify_growth_stage(metrics)

        return metrics
