"""
Financial repository - Historical financial data and growth metrics
"""

import pandas as pd
from typing import Dict, List, Optional


class FinancialRepository:
    """Handles financial history and growth metrics persistence"""

    def __init__(self, conn):
        """
        Initialize with database connection

        Args:
            conn: SQLite connection object
        """
        self.conn = conn

    def save_financial_history(self, ticker: str, financial_data: List[Dict]) -> int:
        """
        Save historical financial data

        Uses INSERT OR REPLACE to update existing records with new fields.
        This ensures that when we add new columns (like cash flow data),
        existing records get updated with the new data on refresh.

        Args:
            ticker: Stock ticker
            financial_data: List of financial data points

        Returns:
            Number of records saved/updated
        """
        if not financial_data:
            return 0

        cursor = self.conn.cursor()
        saved_count = 0

        for record in financial_data:
            try:
                cursor.execute('''
                    INSERT OR REPLACE INTO financial_history
                    (ticker, period_end_date, period_type, revenue, earnings,
                     gross_profit, operating_income, ebitda, net_income, eps, shares_outstanding,
                     operating_cash_flow, capital_expenditures, free_cash_flow_calculated,
                     gross_margin, operating_margin, profit_margin_quarterly)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    ticker.upper(),
                    record.get('period_end_date'),
                    record.get('period_type'),
                    record.get('revenue'),
                    record.get('earnings'),
                    record.get('gross_profit'),
                    record.get('operating_income'),
                    record.get('ebitda'),
                    record.get('net_income'),
                    record.get('eps'),
                    record.get('shares_outstanding'),
                    record.get('operating_cash_flow'),
                    record.get('capital_expenditures'),
                    record.get('free_cash_flow_calculated'),
                    record.get('gross_margin'),
                    record.get('operating_margin'),
                    record.get('profit_margin_quarterly')
                ))
                saved_count += 1
            except Exception as e:
                print(f"Error saving financial record: {str(e)}")

        self.conn.commit()
        return saved_count

    def get_financial_history(self, ticker: str, period_type: str = None) -> List[Dict]:
        """
        Retrieve financial history for a ticker

        Args:
            ticker: Stock ticker
            period_type: Optional filter for 'quarterly' or 'annual'

        Returns:
            List of financial records
        """
        query = 'SELECT * FROM financial_history WHERE ticker = ?'
        params = [ticker.upper()]

        if period_type:
            query += ' AND period_type = ?'
            params.append(period_type)

        query += ' ORDER BY period_end_date DESC'

        cursor = self.conn.cursor()
        cursor.execute(query, params)

        return [dict(row) for row in cursor.fetchall()]

    def save_growth_metrics(self, ticker: str, metrics: Dict) -> bool:
        """Save calculated growth metrics"""
        try:
            cursor = self.conn.cursor()
            cursor.execute('''
                INSERT OR REPLACE INTO growth_metrics (
                    ticker, revenue_cagr_3y, revenue_cagr_5y, earnings_cagr_3y, earnings_cagr_5y,
                    avg_quarterly_revenue_growth, avg_quarterly_earnings_growth,
                    revenue_consistency_score, earnings_consistency_score,
                    revenue_growth_accelerating, earnings_growth_accelerating,
                    consecutive_profitable_quarters, data_points_count,
                    oldest_data_date, newest_data_date, last_calculated,
                    peg_3y_cagr, peg_quarterly, peg_yfinance, peg_average,
                    fcf_cagr_3y, fcf_margin, rule_of_40, operating_leverage,
                    cash_conversion_ratio, margin_trend, growth_stage
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                ticker.upper(),
                metrics.get('revenue_cagr_3y'),
                metrics.get('revenue_cagr_5y'),
                metrics.get('earnings_cagr_3y'),
                metrics.get('earnings_cagr_5y'),
                metrics.get('avg_quarterly_revenue_growth'),
                metrics.get('avg_quarterly_earnings_growth'),
                metrics.get('revenue_consistency_score'),
                metrics.get('earnings_consistency_score'),
                metrics.get('revenue_growth_accelerating'),
                metrics.get('earnings_growth_accelerating'),
                metrics.get('consecutive_profitable_quarters'),
                metrics.get('data_points_count'),
                metrics.get('oldest_data_date'),
                metrics.get('newest_data_date'),
                metrics.get('peg_3y_cagr'),
                metrics.get('peg_quarterly'),
                metrics.get('peg_yfinance'),
                metrics.get('peg_average'),
                metrics.get('fcf_cagr_3y'),
                metrics.get('fcf_margin'),
                metrics.get('rule_of_40'),
                metrics.get('operating_leverage'),
                metrics.get('cash_conversion_ratio'),
                metrics.get('margin_trend'),
                metrics.get('growth_stage')
            ))
            self.conn.commit()
            return True
        except Exception as e:
            print(f"Error saving growth metrics: {str(e)}")
            return False

    def get_growth_metrics(self, ticker: str) -> Optional[Dict]:
        """Retrieve calculated growth metrics for a ticker"""
        cursor = self.conn.cursor()
        cursor.execute('SELECT * FROM growth_metrics WHERE ticker = ?', (ticker.upper(),))
        row = cursor.fetchone()
        return dict(row) if row else None
