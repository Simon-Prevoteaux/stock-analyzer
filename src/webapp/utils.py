"""
Utility functions shared across routes

This module contains helper functions that are used in multiple route handlers.
"""

from libs.growth_analyzer import GrowthAnalyzer


def process_stock_fetch(ticker, fetcher, db):
    """
    Fetch and save stock data including historical data and growth metrics.

    Args:
        ticker: Stock ticker symbol
        fetcher: StockFetcher instance
        db: StockDatabase instance

    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Fetch all data including historical
        full_data = fetcher.fetch_stock_with_history(ticker)

        if not full_data.get('current'):
            return False

        # Save current data
        db.save_stock(full_data['current'])

        # Save historical data
        if full_data.get('quarterly_history'):
            db.save_financial_history(ticker, full_data['quarterly_history'])
        if full_data.get('annual_history'):
            db.save_financial_history(ticker, full_data['annual_history'])

        # Calculate and save growth metrics
        all_history = db.get_financial_history(ticker)
        if all_history:
            analyzer = GrowthAnalyzer(all_history)
            current_fcf = full_data['current'].get('free_cash_flow')
            current_revenue = full_data['current'].get('revenue')
            metrics = analyzer.calculate_all_metrics(current_fcf, current_revenue)

            # Calculate PEG ratios
            pe_ratio = full_data['current'].get('pe_ratio')
            peg_ratios = analyzer.calculate_peg_ratio(pe_ratio)
            metrics.update(peg_ratios)

            # Also calculate PEG using yfinance earnings growth as fallback
            earnings_growth_yf = full_data['current'].get('earnings_growth')
            if pe_ratio and pe_ratio > 0 and earnings_growth_yf and earnings_growth_yf > 0:
                metrics['peg_yfinance'] = pe_ratio / (earnings_growth_yf * 100)
                # Recalculate average including yfinance PEG
                peg_values = [v for v in [metrics.get('peg_3y_cagr'),
                                          metrics.get('peg_quarterly'),
                                          metrics.get('peg_yfinance')] if v is not None]
                if peg_values:
                    metrics['peg_average'] = sum(peg_values) / len(peg_values)

            db.save_growth_metrics(ticker, metrics)

        return True

    except Exception as e:
        print(f"Error fetching {ticker}: {str(e)}")
        return False
