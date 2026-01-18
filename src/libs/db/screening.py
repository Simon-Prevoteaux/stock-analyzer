"""
Stock screening - Filter and find stocks by various criteria
"""

import pandas as pd


class StockScreener:
    """Handles stock screening and filtering operations"""

    def __init__(self, conn):
        """
        Initialize with database connection

        Args:
            conn: SQLite connection object
        """
        self.conn = conn

    def get_high_risk_stocks(self, min_bubble_score: int = 6) -> pd.DataFrame:
        """
        Get stocks with high bubble scores

        Args:
            min_bubble_score: Minimum bubble score threshold

        Returns:
            DataFrame containing high-risk stocks with growth metrics
        """
        query = '''
            SELECT s.*, g.peg_average, g.peg_3y_cagr, g.peg_quarterly, g.peg_yfinance
            FROM stocks s
            LEFT JOIN growth_metrics g ON s.ticker = g.ticker
            WHERE s.bubble_score >= ?
            ORDER BY s.bubble_score DESC
        '''
        return pd.read_sql_query(query, self.conn, params=(min_bubble_score,))

    def get_value_stocks(self, max_pe: float = 20, max_ps: float = 3) -> pd.DataFrame:
        """
        Get potential value stocks

        Args:
            max_pe: Maximum P/E ratio
            max_ps: Maximum P/S ratio

        Returns:
            DataFrame containing value stocks with growth metrics
        """
        query = '''
            SELECT s.*, g.peg_average, g.peg_3y_cagr, g.peg_quarterly, g.peg_yfinance
            FROM stocks s
            LEFT JOIN growth_metrics g ON s.ticker = g.ticker
            WHERE s.pe_ratio <= ? AND s.ps_ratio <= ? AND s.is_profitable = 1
            ORDER BY s.ps_ratio ASC
        '''
        return pd.read_sql_query(query, self.conn, params=(max_pe, max_ps))

    def get_near_value_stocks(self) -> pd.DataFrame:
        """
        Get stocks that are close to being value plays - meeting some but not all criteria,
        or just slightly outside the thresholds.

        Criteria (profitable stocks that match ONE of):
        - P/E between 20-30 AND P/S <= 5 (close on P/E)
        - P/E <= 25 AND P/S between 3-5 (close on P/S)

        Excludes stocks that already qualify as value plays (P/E <= 20 AND P/S <= 3)

        Returns:
            DataFrame containing near-value stocks
        """
        query = '''
            SELECT s.*, g.peg_average, g.peg_3y_cagr, g.peg_quarterly, g.peg_yfinance
            FROM stocks s
            LEFT JOIN growth_metrics g ON s.ticker = g.ticker
            WHERE s.is_profitable = 1
            AND NOT (s.pe_ratio <= 20 AND s.ps_ratio <= 3)
            AND (
                (s.pe_ratio > 20 AND s.pe_ratio <= 30 AND s.ps_ratio <= 5)
                OR (s.pe_ratio <= 25 AND s.ps_ratio > 3 AND s.ps_ratio <= 5)
            )
            ORDER BY
                CASE
                    WHEN s.pe_ratio <= 20 THEN s.ps_ratio - 3
                    WHEN s.ps_ratio <= 3 THEN s.pe_ratio - 20
                    ELSE (s.pe_ratio - 20) / 10.0 + (s.ps_ratio - 3) / 2.0
                END ASC
        '''
        return pd.read_sql_query(query, self.conn)

    def get_enhanced_value_stocks(self,
                                   max_pe: float = 20,
                                   max_ps: float = 3,
                                   min_consistency: float = 60,
                                   min_growth: float = 0.05) -> pd.DataFrame:
        """
        Get value stocks with growth quality filters

        Args:
            max_pe: Maximum P/E ratio
            max_ps: Maximum P/S ratio
            min_consistency: Minimum consistency score (0-100)
            min_growth: Minimum average quarterly growth rate

        Returns:
            DataFrame of enhanced value stocks
        """
        query = '''
            SELECT s.*, g.*
            FROM stocks s
            LEFT JOIN growth_metrics g ON s.ticker = g.ticker
            WHERE s.pe_ratio <= ?
            AND s.ps_ratio <= ?
            AND s.is_profitable = 1
            AND (g.revenue_consistency_score >= ? OR g.earnings_consistency_score >= ?)
            AND (g.avg_quarterly_revenue_growth >= ? OR g.avg_quarterly_earnings_growth >= ?)
            ORDER BY g.revenue_consistency_score DESC, s.ps_ratio ASC
        '''
        return pd.read_sql_query(query, self.conn,
                                params=(max_pe, max_ps, min_consistency, min_consistency, min_growth, min_growth))

    def get_quality_growth_stocks(self, min_cagr: float = 20, min_consistency: int = 70, max_peg: float = 2.5) -> pd.DataFrame:
        """
        Find high-growth stocks with consistent, sustainable patterns

        Quality Growth = High CAGR + High Consistency + Reasonable Valuation
        (Accelerating growth is a bonus but not required)

        Args:
            min_cagr: Minimum earnings/revenue CAGR (as percentage, e.g., 20 for 20%)
            min_consistency: Minimum consistency score (0-100)
            max_peg: Maximum PEG ratio (growth-adjusted valuation)

        Returns:
            DataFrame of quality growth stocks sorted by consistency and valuation
        """
        query = '''
            SELECT s.*,
                   g.earnings_cagr_3y, g.revenue_cagr_3y,
                   g.earnings_consistency_score, g.revenue_consistency_score,
                   g.peg_average, g.revenue_growth_accelerating,
                   g.earnings_growth_accelerating,
                   g.consecutive_profitable_quarters,
                   g.growth_stage
            FROM stocks s
            LEFT JOIN growth_metrics g ON s.ticker = g.ticker
            WHERE (g.earnings_cagr_3y >= ? OR g.revenue_cagr_3y >= ?)
              AND g.earnings_consistency_score >= ?
              AND g.peg_average IS NOT NULL
              AND g.peg_average <= ?
            ORDER BY
              CASE WHEN g.revenue_growth_accelerating = 1 OR g.earnings_growth_accelerating = 1 THEN 0 ELSE 1 END,
              g.earnings_consistency_score DESC,
              g.peg_average ASC
        '''
        return pd.read_sql_query(query, self.conn, params=(min_cagr/100, min_cagr/100, min_consistency, max_peg))

    def get_growth_inflection_stocks(self, min_consistency: int = 60, max_pe: float = 40) -> pd.DataFrame:
        """
        Find stocks with accelerating growth (inflection points)

        Growth Inflection = Recent growth > Historical + Reasonable valuation

        Args:
            min_consistency: Minimum consistency score (0-100) to filter noise
            max_pe: Maximum P/E ratio to avoid overvalued stocks

        Returns:
            DataFrame of stocks showing growth acceleration
        """
        query = '''
            SELECT s.*,
                   g.revenue_growth_accelerating, g.earnings_growth_accelerating,
                   g.earnings_cagr_3y, g.revenue_cagr_3y,
                   g.earnings_consistency_score, g.revenue_consistency_score,
                   g.avg_quarterly_revenue_growth, g.avg_quarterly_earnings_growth,
                   g.consecutive_profitable_quarters,
                   g.peg_average,
                   g.growth_stage
            FROM stocks s
            LEFT JOIN growth_metrics g ON s.ticker = g.ticker
            WHERE (g.revenue_growth_accelerating = 1 OR g.earnings_growth_accelerating = 1)
              AND (g.earnings_consistency_score >= ? OR g.revenue_consistency_score >= ?)
              AND (s.pe_ratio IS NULL OR s.pe_ratio <= ?)
            ORDER BY
              CASE WHEN g.revenue_growth_accelerating = 1 AND g.earnings_growth_accelerating = 1 THEN 0 ELSE 1 END,
              g.revenue_cagr_3y DESC,
              g.earnings_cagr_3y DESC
        '''
        return pd.read_sql_query(query, self.conn, params=(min_consistency, min_consistency, max_pe))

    def get_rule_of_40_stocks(self, min_rule_of_40: float = 40, sector_filter: str = None) -> pd.DataFrame:
        """
        Find stocks with efficient growth (Revenue Growth % + FCF Margin % >= threshold)

        Rule of 40 = Revenue Growth Rate (%) + FCF Margin (%)
        Widely used for SaaS/cloud companies to balance growth with profitability

        Args:
            min_rule_of_40: Minimum Rule of 40 score (default: 40)
            sector_filter: Optional sector to filter (e.g., 'Technology', 'Communication Services')

        Returns:
            DataFrame of stocks meeting Rule of 40 threshold, sorted by score
        """
        if sector_filter:
            query = '''
                SELECT s.*,
                       g.revenue_cagr_3y, g.fcf_margin, g.rule_of_40,
                       g.growth_stage, g.fcf_cagr_3y, g.cash_conversion_ratio,
                       g.operating_leverage, g.margin_trend
                FROM stocks s
                LEFT JOIN growth_metrics g ON s.ticker = g.ticker
                WHERE g.rule_of_40 IS NOT NULL
                  AND g.rule_of_40 >= ?
                  AND s.sector = ?
                ORDER BY g.rule_of_40 DESC
            '''
            return pd.read_sql_query(query, self.conn, params=(min_rule_of_40, sector_filter))
        else:
            query = '''
                SELECT s.*,
                       g.revenue_cagr_3y, g.fcf_margin, g.rule_of_40,
                       g.growth_stage, g.fcf_cagr_3y, g.cash_conversion_ratio,
                       g.operating_leverage, g.margin_trend
                FROM stocks s
                LEFT JOIN growth_metrics g ON s.ticker = g.ticker
                WHERE g.rule_of_40 IS NOT NULL
                  AND g.rule_of_40 >= ?
                ORDER BY g.rule_of_40 DESC
            '''
            return pd.read_sql_query(query, self.conn, params=(min_rule_of_40,))

    def get_margin_expansion_stocks(self, min_revenue_growth: float = 15, min_operating_leverage: float = 1.0) -> pd.DataFrame:
        """
        Find stocks with improving profitability alongside growth

        Identifies companies where margins are expanding (recent 4Q margins > previous 4Q)
        AND earnings are growing faster than revenue (operating leverage > 1.0)

        Args:
            min_revenue_growth: Minimum revenue CAGR percentage (default: 15%)
            min_operating_leverage: Minimum operating leverage ratio (default: 1.0)

        Returns:
            DataFrame of stocks with margin expansion, sorted by operating leverage
        """
        query = '''
            SELECT s.*,
                   g.revenue_cagr_3y, g.earnings_cagr_3y,
                   g.margin_trend, g.operating_leverage,
                   g.growth_stage, g.earnings_consistency_score,
                   s.profit_margin, s.operating_margin
            FROM stocks s
            LEFT JOIN growth_metrics g ON s.ticker = g.ticker
            WHERE g.revenue_cagr_3y IS NOT NULL
              AND g.revenue_cagr_3y >= ?
              AND g.margin_trend = 'expanding'
              AND g.operating_leverage IS NOT NULL
              AND g.operating_leverage >= ?
            ORDER BY g.operating_leverage DESC
        '''
        return pd.read_sql_query(query, self.conn, params=(min_revenue_growth/100, min_operating_leverage))

    def get_cash_generative_growth_stocks(self, min_revenue_growth: float = 20, min_fcf_margin: float = 10) -> pd.DataFrame:
        """
        Find high-growth stocks that also generate positive free cash flow

        This is a rare and valuable combination - most high-growth companies burn cash.
        FCF-positive growth indicates sustainable expansion with lower financial risk.

        Args:
            min_revenue_growth: Minimum revenue CAGR percentage (default: 20%)
            min_fcf_margin: Minimum FCF margin percentage (default: 10%)

        Returns:
            DataFrame of cash-generative growth stocks, sorted by revenue growth
        """
        query = '''
            SELECT s.*,
                   g.revenue_cagr_3y, g.earnings_cagr_3y,
                   g.fcf_margin, g.fcf_cagr_3y,
                   g.cash_conversion_ratio, g.growth_stage,
                   g.earnings_consistency_score,
                   s.free_cash_flow, s.profit_margin
            FROM stocks s
            LEFT JOIN growth_metrics g ON s.ticker = g.ticker
            WHERE g.revenue_cagr_3y IS NOT NULL
              AND g.revenue_cagr_3y >= ?
              AND s.free_cash_flow IS NOT NULL
              AND s.free_cash_flow > 0
              AND g.fcf_margin IS NOT NULL
              AND g.fcf_margin >= ?
              AND g.cash_conversion_ratio IS NOT NULL
              AND g.cash_conversion_ratio > 0.8
            ORDER BY g.revenue_cagr_3y DESC
        '''
        return pd.read_sql_query(query, self.conn, params=(min_revenue_growth/100, min_fcf_margin/100))

    def add_sector_rankings(self, stocks_df: pd.DataFrame) -> pd.DataFrame:
        """
        Add sector-relative growth rankings to stock DataFrame

        Calculates:
        - Sector median CAGR for revenue and earnings
        - Growth vs sector median (ratio)
        - Percentile rank within sector (0-100)

        Args:
            stocks_df: DataFrame with stock data including sector and growth metrics

        Returns:
            Enhanced DataFrame with sector comparison columns
        """
        if stocks_df.empty or 'sector' not in stocks_df.columns:
            return stocks_df

        # Calculate sector medians for revenue and earnings CAGR
        sector_medians = stocks_df.groupby('sector').agg({
            'revenue_cagr_3y': 'median',
            'earnings_cagr_3y': 'median'
        }).reset_index()

        # Rename columns to avoid conflicts
        sector_medians.columns = ['sector', 'revenue_cagr_3y_sector_median', 'earnings_cagr_3y_sector_median']

        # Merge sector medians back to original DataFrame
        stocks_df = stocks_df.merge(sector_medians, on='sector', how='left')

        # Calculate relative growth vs sector median
        stocks_df['revenue_vs_sector'] = None
        stocks_df['earnings_vs_sector'] = None

        # Revenue vs sector
        mask = (stocks_df['revenue_cagr_3y'].notna()) & (stocks_df['revenue_cagr_3y_sector_median'].notna()) & (stocks_df['revenue_cagr_3y_sector_median'] != 0)
        stocks_df.loc[mask, 'revenue_vs_sector'] = stocks_df.loc[mask, 'revenue_cagr_3y'] / stocks_df.loc[mask, 'revenue_cagr_3y_sector_median']

        # Earnings vs sector
        mask = (stocks_df['earnings_cagr_3y'].notna()) & (stocks_df['earnings_cagr_3y_sector_median'].notna()) & (stocks_df['earnings_cagr_3y_sector_median'] != 0)
        stocks_df.loc[mask, 'earnings_vs_sector'] = stocks_df.loc[mask, 'earnings_cagr_3y'] / stocks_df.loc[mask, 'earnings_cagr_3y_sector_median']

        # Calculate percentile rank within sector (0-1 scale, then convert to 0-100)
        stocks_df['sector_revenue_rank_pct'] = stocks_df.groupby('sector')['revenue_cagr_3y'].rank(pct=True) * 100
        stocks_df['sector_earnings_rank_pct'] = stocks_df.groupby('sector')['earnings_cagr_3y'].rank(pct=True) * 100

        return stocks_df
