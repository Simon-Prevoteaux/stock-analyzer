"""
Stock Data Fetcher
Fetches stock metrics from Yahoo Finance API using yfinance
"""

import yfinance as yf
import pandas as pd
from typing import Dict, List, Optional
from datetime import datetime


class StockFetcher:
    """Fetches stock data and calculates key metrics"""

    def __init__(self):
        self.cache = {}

    def fetch_stock_data(self, ticker: str) -> Optional[Dict]:
        """
        Fetch comprehensive stock data for a single ticker

        Args:
            ticker: Stock ticker symbol (e.g., 'AAPL', 'MSFT')

        Returns:
            Dictionary containing stock metrics or None if fetch fails
        """
        try:
            stock = yf.Ticker(ticker)
            info = stock.info

            # Extract key metrics
            data = {
                'ticker': ticker.upper(),
                'company_name': info.get('longName', ticker),
                'market_cap': info.get('marketCap', 0),
                'revenue': info.get('totalRevenue', 0),
                'earnings': info.get('netIncomeToCommon', 0),
                'pe_ratio': info.get('trailingPE', 0),
                'forward_pe': info.get('forwardPE', 0),
                'ps_ratio': self._calculate_ps_ratio(info.get('marketCap'), info.get('totalRevenue')),
                'eps': info.get('trailingEps', 0),
                'revenue_growth': info.get('revenueGrowth', 0),
                'earnings_growth': info.get('earningsGrowth', 0),
                'profit_margin': info.get('profitMargins', 0),
                'operating_margin': info.get('operatingMargins', 0),
                'current_price': info.get('currentPrice', 0),
                'sector': info.get('sector', 'Unknown'),
                'industry': info.get('industry', 'Unknown'),
                'is_profitable': info.get('netIncomeToCommon', 0) > 0,
                'last_updated': datetime.now().isoformat(),
                # New metrics
                'price_to_book': info.get('priceToBook', 0),
                'current_ratio': info.get('currentRatio', 0),
                'free_cash_flow': info.get('freeCashflow', 0),
                'enterprise_value': info.get('enterpriseValue', 0),
                'target_price': info.get('targetMeanPrice', 0)
            }

            # Calculate bubble score (simple heuristic)
            data['bubble_score'] = self._calculate_bubble_score(data)
            data['risk_level'] = self._determine_risk_level(data['bubble_score'])

            return data

        except Exception as e:
            print(f"Error fetching data for {ticker}: {str(e)}")
            return None

    def fetch_multiple_stocks(self, tickers: List[str]) -> pd.DataFrame:
        """
        Fetch data for multiple stocks

        Args:
            tickers: List of ticker symbols

        Returns:
            DataFrame containing all stock metrics
        """
        stocks_data = []

        for ticker in tickers:
            print(f"Fetching data for {ticker}...")
            data = self.fetch_stock_data(ticker)
            if data:
                stocks_data.append(data)

        if stocks_data:
            df = pd.DataFrame(stocks_data)
            return df
        else:
            return pd.DataFrame()

    def _calculate_ps_ratio(self, market_cap: Optional[float], revenue: Optional[float]) -> float:
        """Calculate Price-to-Sales ratio"""
        if market_cap and revenue and revenue > 0:
            return market_cap / revenue
        return 0

    def _calculate_bubble_score(self, data: Dict) -> int:
        """
        Calculate a bubble score (0-10) based on valuation metrics
        Higher score = more overvalued
        """
        score = 0

        # P/E ratio scoring
        pe = data.get('pe_ratio', 0)
        if pe > 200:
            score += 3
        elif pe > 100:
            score += 2
        elif pe > 50:
            score += 1

        # P/S ratio scoring
        ps = data.get('ps_ratio', 0)
        if ps > 50:
            score += 3
        elif ps > 20:
            score += 2
        elif ps > 10:
            score += 1

        # Profitability check
        if not data.get('is_profitable', False):
            score += 2

        # Growth vs valuation
        revenue_growth = data.get('revenue_growth', 0) or 0
        if ps > 15 and revenue_growth < 0.2:  # High P/S with low growth
            score += 2

        return min(score, 10)

    def _determine_risk_level(self, bubble_score: int) -> str:
        """Determine risk level based on bubble score"""
        if bubble_score >= 8:
            return 'EXTREME'
        elif bubble_score >= 6:
            return 'VERY HIGH'
        elif bubble_score >= 4:
            return 'HIGH'
        elif bubble_score >= 2:
            return 'MEDIUM'
        else:
            return 'LOW'

    def compare_stocks(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Add comparison metrics to stocks DataFrame

        Args:
            df: DataFrame with stock data

        Returns:
            DataFrame with additional comparison columns
        """
        if df.empty:
            return df

        # Add percentile rankings
        df['pe_percentile'] = df['pe_ratio'].rank(pct=True) * 100
        df['ps_percentile'] = df['ps_ratio'].rank(pct=True) * 100
        df['growth_percentile'] = df['revenue_growth'].rank(pct=True) * 100

        # Value vs Growth classification
        df['classification'] = df.apply(self._classify_stock, axis=1)

        return df

    def _classify_stock(self, row) -> str:
        """Classify stock as Value, Growth, or Overvalued"""
        pe = row.get('pe_ratio', 0)
        ps = row.get('ps_ratio', 0)
        growth = row.get('revenue_growth', 0) or 0

        if pe < 20 and ps < 3:
            return 'Value Play'
        elif growth > 0.3 and (pe < growth * 100 or ps < 10):
            return 'Growth'
        elif pe > 100 or ps > 30:
            return 'Overvalued'
        else:
            return 'Fairly Valued'

    def fetch_historical_financials(self, ticker: str, period_type: str = 'quarterly') -> List[Dict]:
        """
        Fetch historical financial statements from yfinance

        Args:
            ticker: Stock ticker symbol
            period_type: 'quarterly' or 'annual'

        Returns:
            List of dictionaries containing financial data points
        """
        try:
            stock = yf.Ticker(ticker)

            # Get income statement based on period type
            if period_type == 'quarterly':
                income_stmt = stock.quarterly_income_stmt
                cash_flow = stock.quarterly_cashflow
            else:
                income_stmt = stock.income_stmt
                cash_flow = stock.cashflow

            if income_stmt is None or income_stmt.empty:
                return []

            # Log how many periods we're getting
            print(f"  Found {len(income_stmt.columns)} {period_type} periods for {ticker}")

            # Transform from DataFrame to list of dicts
            financial_data = []
            for date_col in income_stmt.columns:
                # Get revenue for margin calculations
                revenue = self._safe_get(income_stmt, 'Total Revenue', date_col)
                gross_profit = self._safe_get(income_stmt, 'Gross Profit', date_col)
                operating_income = self._safe_get(income_stmt, 'Operating Income', date_col)
                net_income = self._safe_get(income_stmt, 'Net Income', date_col)

                # Get cash flow data
                operating_cash_flow = None
                capital_expenditures = None
                free_cash_flow_calculated = None

                if cash_flow is not None and not cash_flow.empty and date_col in cash_flow.columns:
                    # Try different field name variations
                    operating_cash_flow = (self._safe_get(cash_flow, 'Operating Cash Flow', date_col) or
                                          self._safe_get(cash_flow, 'Cash Flow From Continuing Operating Activities', date_col))

                    # Capital Expenditure might be negative, so we check for it
                    capital_expenditures = (self._safe_get(cash_flow, 'Capital Expenditure', date_col) or
                                           self._safe_get(cash_flow, 'Purchase Of PPE', date_col))

                    # Calculate FCF if we have both components
                    if operating_cash_flow is not None and capital_expenditures is not None:
                        # CapEx is typically negative in yfinance, so we add it
                        free_cash_flow_calculated = operating_cash_flow + capital_expenditures

                # Calculate margins
                gross_margin = None
                operating_margin = None
                profit_margin_quarterly = None

                if revenue and revenue > 0:
                    if gross_profit is not None:
                        gross_margin = gross_profit / revenue
                    if operating_income is not None:
                        operating_margin = operating_income / revenue
                    if net_income is not None:
                        profit_margin_quarterly = net_income / revenue

                period_data = {
                    'ticker': ticker.upper(),
                    'period_end_date': date_col.strftime('%Y-%m-%d'),
                    'period_type': period_type,
                    'revenue': revenue,
                    'gross_profit': gross_profit,
                    'operating_income': operating_income,
                    'ebitda': self._safe_get(income_stmt, 'EBITDA', date_col),
                    'net_income': net_income,
                    'earnings': net_income,
                    # EPS will be calculated from Net Income / Shares Outstanding if needed
                    'eps': None,  # Can be calculated later if we have shares outstanding data
                    # New cash flow fields
                    'operating_cash_flow': operating_cash_flow,
                    'capital_expenditures': capital_expenditures,
                    'free_cash_flow_calculated': free_cash_flow_calculated,
                    # New margin fields
                    'gross_margin': gross_margin,
                    'operating_margin': operating_margin,
                    'profit_margin_quarterly': profit_margin_quarterly
                }

                financial_data.append(period_data)

            return financial_data

        except Exception as e:
            print(f"Error fetching historical financials for {ticker}: {str(e)}")
            return []

    def _safe_get(self, df: pd.DataFrame, row_name: str, col_name) -> Optional[float]:
        """Safely extract value from DataFrame"""
        try:
            if row_name in df.index:
                value = df.loc[row_name, col_name]
                return float(value) if pd.notna(value) else None
            return None
        except:
            return None

    def fetch_stock_with_history(self, ticker: str) -> Dict:
        """
        Fetch both current data and historical financials

        Returns:
            Dict with 'current', 'quarterly_history', and 'annual_history' keys
        """
        current_data = self.fetch_stock_data(ticker)

        quarterly_history = self.fetch_historical_financials(ticker, 'quarterly')
        annual_history = self.fetch_historical_financials(ticker, 'annual')

        return {
            'current': current_data,
            'quarterly_history': quarterly_history,
            'annual_history': annual_history
        }
