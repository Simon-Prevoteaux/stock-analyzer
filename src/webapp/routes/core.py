"""
Core routes - Home, Fetch, Screener, Stock Detail, Comparison
"""

from flask import render_template, request, redirect, url_for
from webapp.routes import core_bp
from webapp.extensions import db, fetcher
from webapp.utils import process_stock_fetch
from libs.stock_lists import get_all_lists
from libs.growth_analyzer import GrowthAnalyzer
import pandas as pd


@core_bp.route('/')
def index():
    """Home page with overview"""
    stats = {
        'total_stocks': len(db.get_all_stocks()),
        'sectors': len(db.get_sectors()),
        'high_risk': len(db.get_high_risk_stocks()),
        'watchlist_count': len(db.get_watchlist()),
        'portfolio_count': len(db.get_portfolio())
    }
    return render_template('index.html', stats=stats)


@core_bp.route('/fetch', methods=['GET', 'POST'])
def fetch_stocks():
    """Fetch new stock data"""
    fetched_count = 25
    not_fetched_count = 28

    if request.method == 'POST':
        tickers_input = request.form.get('tickers', '')
        tickers = [t.strip().upper() for t in tickers_input.split(',') if t.strip()]

        if tickers:
            saved_count = 0
            failed_tickers = []

            for ticker in tickers:
                success = process_stock_fetch(ticker, fetcher, db)
                if success:
                    saved_count += 1
                else:
                    failed_tickers.append(ticker)

            if saved_count > 0:
                message = f'Successfully fetched and saved {saved_count} stocks with historical data'
                if failed_tickers:
                    message += f' (Failed: {", ".join(failed_tickers)})'
                return render_template('fetch.html',
                                       stock_lists=get_all_lists(),
                                       fetched_count=fetched_count,
                                       not_fetched_count=not_fetched_count,
                                       success=True,
                                       message=message)
            else:
                return render_template('fetch.html',
                                       stock_lists=get_all_lists(),
                                       fetched_count=fetched_count,
                                       not_fetched_count=not_fetched_count,
                                       error=True,
                                       message='Failed to fetch stock data')

    return render_template('fetch.html',
                           stock_lists=get_all_lists(),
                           fetched_count=fetched_count,
                           not_fetched_count=not_fetched_count)


@core_bp.route('/screener')
def screener():
    """Stock screener page"""
    stocks_df = db.get_all_stocks()
    available_stocks = stocks_df['ticker'].tolist() if not stocks_df.empty else []

    # Apply filters
    min_market_cap = request.args.get('min_market_cap', type=float)
    max_pe = request.args.get('max_pe', type=float)
    max_ps = request.args.get('max_ps', type=float)
    sector = request.args.get('sector')
    min_growth = request.args.get('min_growth', type=float)
    risk_level = request.args.get('risk_level')
    max_peg = request.args.get('max_peg', type=float)
    top_sector_performers = request.args.get('top_sector_performers', type=str)

    if not stocks_df.empty:
        stocks_df = db.add_sector_rankings(stocks_df)

        if min_market_cap:
            stocks_df = stocks_df[stocks_df['market_cap'] >= min_market_cap]
        if max_pe:
            stocks_df = stocks_df[stocks_df['pe_ratio'] <= max_pe]
        if max_ps:
            stocks_df = stocks_df[stocks_df['ps_ratio'] <= max_ps]
        if sector:
            stocks_df = stocks_df[stocks_df['sector'] == sector]
        if min_growth:
            stocks_df = stocks_df[stocks_df['revenue_growth'] >= min_growth]
        if risk_level:
            stocks_df = stocks_df[stocks_df['risk_level'] == risk_level]
        if max_peg:
            stocks_df = stocks_df[stocks_df['peg_average'].notna() & (stocks_df['peg_average'] <= max_peg)]

        if top_sector_performers == 'true':
            stocks_df = stocks_df[
                ((stocks_df['sector_revenue_rank_pct'].notna()) & (stocks_df['sector_revenue_rank_pct'] >= 75)) &
                ((stocks_df['sector_earnings_rank_pct'].notna()) & (stocks_df['sector_earnings_rank_pct'] >= 75))
            ]

        stocks_df = fetcher.compare_stocks(stocks_df)

    sectors = db.get_sectors()
    risk_levels = ['LOW', 'MEDIUM', 'HIGH', 'VERY HIGH', 'EXTREME']

    return render_template('screener.html',
                           stocks=stocks_df.to_dict('records') if not stocks_df.empty else [],
                           sectors=sectors,
                           risk_levels=risk_levels,
                           available_stocks=available_stocks,
                           top_sector_performers=top_sector_performers)


@core_bp.route('/stock/<ticker>')
def stock_detail(ticker):
    """Stock detail page with historical data"""
    stock = db.get_stock(ticker.upper())

    if not stock:
        return render_template('error.html', message=f'Stock {ticker} not found'), 404

    growth_metrics = db.get_growth_metrics(ticker)
    quarterly_history = db.get_financial_history(ticker, 'quarterly')
    annual_history = db.get_financial_history(ticker, 'annual')

    # Prepare chart data
    revenue_chart_data_quarterly = _prepare_chart_data(quarterly_history, 'revenue', 'Quarterly Revenue', quarterly=True)
    earnings_chart_data_quarterly = _prepare_chart_data(quarterly_history, 'earnings', 'Quarterly Earnings', quarterly=True)
    revenue_chart_data_annual = _prepare_chart_data(annual_history, 'revenue', 'Annual Revenue', quarterly=False)
    earnings_chart_data_annual = _prepare_chart_data(annual_history, 'earnings', 'Annual Earnings', quarterly=False)

    technical_data = db.get_technical_indicators(ticker.upper())
    strategy_matches = _check_strategy_matches(stock, growth_metrics)

    return render_template('stock_detail.html',
                         stock=stock,
                         growth_metrics=growth_metrics,
                         strategy_matches=strategy_matches,
                         technical_data=technical_data,
                         has_historical_data=bool(quarterly_history or annual_history),
                         revenue_chart_data_quarterly=revenue_chart_data_quarterly,
                         earnings_chart_data_quarterly=earnings_chart_data_quarterly,
                         revenue_chart_data_annual=revenue_chart_data_annual,
                         earnings_chart_data_annual=earnings_chart_data_annual,
                         quarterly_periods=len(quarterly_history) if quarterly_history else 0,
                         annual_periods=len(annual_history) if annual_history else 0)


@core_bp.route('/comparison')
def comparison():
    """Compare multiple stocks"""
    tickers_param = request.args.get('tickers', '')
    tickers = [t.strip().upper() for t in tickers_param.split(',') if t.strip()]

    stocks_df = db.get_all_stocks()
    available_stocks = stocks_df['ticker'].tolist() if not stocks_df.empty else []

    stocks = []
    growth_metrics_list = []
    if tickers:
        for ticker in tickers:
            stock = db.get_stock(ticker)
            if stock:
                stocks.append(stock)
                growth_metrics = db.get_growth_metrics(ticker)
                growth_metrics_list.append(growth_metrics if growth_metrics else {})

    # Get suggestions
    watchlist_df = db.get_watchlist()
    watchlist_tickers = _get_tickers_from_df(watchlist_df)

    portfolio_df = db.get_portfolio()
    portfolio_tickers = _get_tickers_from_df(portfolio_df)

    stock_lists = get_all_lists()
    suggestions = {
        'watchlist': {
            'name': 'My Watchlist',
            'tickers': watchlist_tickers[:10],
            'description': 'Compare stocks from your watchlist'
        },
        'portfolio': {
            'name': 'My Portfolio',
            'tickers': portfolio_tickers[:10],
            'description': 'Compare stocks from your portfolio'
        },
        'mag_7': stock_lists.get('mag_7', {}),
        'software_cloud': {
            'name': 'Top Cloud Software',
            'tickers': ['MSFT', 'GOOGL', 'ORCL', 'CRM', 'SNOW', 'PLTR'],
            'description': 'Compare leading cloud and software companies'
        },
        'chip_leaders': {
            'name': 'Chip Leaders',
            'tickers': ['NVDA', 'AMD', 'INTC', 'TSM', 'AVGO'],
            'description': 'Compare top semiconductor companies'
        },
        'finance_giants': {
            'name': 'Finance Giants',
            'tickers': ['JPM', 'BAC', 'GS', 'MS', 'WFC'],
            'description': 'Compare major banks and financial institutions'
        }
    }

    return render_template('comparison.html', stocks=stocks, growth_metrics_list=growth_metrics_list,
                          suggestions=suggestions, available_stocks=available_stocks)


@core_bp.route('/delete/<ticker>', methods=['POST'])
def delete_stock(ticker):
    """Delete a stock from the database"""
    if db.delete_stock(ticker):
        return redirect(url_for('core.screener'))
    else:
        return render_template('error.html', message=f'Failed to delete stock {ticker}'), 400


@core_bp.route('/refresh-all', methods=['POST'])
def refresh_all_stocks():
    """Refresh all stocks in the database with historical data"""
    stocks_df = db.get_all_stocks()
    if stocks_df.empty:
        return render_template('fetch.html',
                               stock_lists=get_all_lists(),
                               error=True,
                               message='No stocks to refresh')

    tickers = stocks_df['ticker'].tolist()
    saved_count = 0
    failed_tickers = []

    for ticker in tickers:
        print(f"Refreshing {ticker} with historical data...")
        success = process_stock_fetch(ticker, fetcher, db)
        if success:
            saved_count += 1
        else:
            failed_tickers.append(ticker)

    if saved_count > 0:
        message = f'Successfully refreshed {saved_count} stocks with historical data'
        if failed_tickers:
            message += f' (Failed: {", ".join(failed_tickers)})'
        return render_template('fetch.html',
                               stock_lists=get_all_lists(),
                               fetched_count=25,
                               not_fetched_count=28,
                               success=True,
                               message=message)
    else:
        return render_template('fetch.html',
                               stock_lists=get_all_lists(),
                               fetched_count=25,
                               not_fetched_count=28,
                               error=True,
                               message='Failed to refresh stock data')


# Helper functions

def _prepare_chart_data(history, field, label, quarterly=True):
    """Prepare chart data from historical records"""
    if not history:
        return None

    sorted_history = sorted(history, key=lambda x: x['period_end_date'])

    if field == 'earnings':
        filtered = [(h['period_end_date'][:7 if quarterly else 4], h[field])
                    for h in sorted_history
                    if h[field] and h[field] != 0]
    else:
        filtered = [(h['period_end_date'][:7 if quarterly else 4], h[field])
                    for h in sorted_history
                    if h[field] and h[field] > 0]

    if not filtered:
        return None

    color = '#00ff88' if field == 'revenue' else '#ffd700'

    return {
        'labels': [item[0] for item in filtered],
        'datasets': [{
            'label': label,
            'data': [item[1] for item in filtered],
            'borderColor': color,
            'fill': True
        }]
    }


def _check_strategy_matches(stock, growth_metrics):
    """Check which strategies this stock qualifies for"""
    matches = {
        'value_play': False,
        'quality_growth': False,
        'growth_inflection': False,
        'rule_of_40': False,
        'margin_expansion': False,
        'cash_generative_growth': False
    }

    if not stock or not growth_metrics:
        return matches

    # Value Play
    if (stock.get('pe_ratio') and stock['pe_ratio'] <= 20 and
        stock.get('ps_ratio') and stock['ps_ratio'] <= 3 and
        stock.get('is_profitable') and
        (growth_metrics.get('revenue_consistency_score', 0) >= 60 or
         growth_metrics.get('earnings_consistency_score', 0) >= 60) and
        (growth_metrics.get('avg_quarterly_revenue_growth', 0) >= 0.05 or
         growth_metrics.get('avg_quarterly_earnings_growth', 0) >= 0.05)):
        matches['value_play'] = True

    # Quality Growth
    if (((growth_metrics.get('earnings_cagr_3y') or 0) >= 0.20 or
         (growth_metrics.get('revenue_cagr_3y') or 0) >= 0.20) and
        (growth_metrics.get('earnings_consistency_score') or 0) >= 70 and
        growth_metrics.get('peg_average') and growth_metrics['peg_average'] <= 2.5):
        matches['quality_growth'] = True

    # Growth Inflection
    if ((growth_metrics.get('revenue_growth_accelerating') or
         growth_metrics.get('earnings_growth_accelerating')) and
        (growth_metrics.get('earnings_consistency_score', 0) >= 60 or
         growth_metrics.get('revenue_consistency_score', 0) >= 60) and
        (not stock.get('pe_ratio') or stock['pe_ratio'] <= 40)):
        matches['growth_inflection'] = True

    # Rule of 40
    if growth_metrics.get('rule_of_40') and growth_metrics['rule_of_40'] >= 40:
        matches['rule_of_40'] = True

    # Margin Expansion
    if ((growth_metrics.get('revenue_cagr_3y') or 0) >= 0.15 and
        growth_metrics.get('margin_trend') == 'expanding' and
        growth_metrics.get('operating_leverage') and growth_metrics['operating_leverage'] >= 1.0):
        matches['margin_expansion'] = True

    # Cash-Generative Growth
    if ((growth_metrics.get('revenue_cagr_3y') or 0) >= 0.20 and
        stock.get('free_cash_flow') and stock['free_cash_flow'] > 0 and
        growth_metrics.get('fcf_margin') and growth_metrics['fcf_margin'] >= 0.10 and
        growth_metrics.get('cash_conversion_ratio') and growth_metrics['cash_conversion_ratio'] > 0.8):
        matches['cash_generative_growth'] = True

    return matches


def _get_tickers_from_df(df):
    """Extract tickers from a DataFrame, handling potential duplicate columns"""
    if df.empty:
        return []

    if isinstance(df['ticker'], pd.DataFrame):
        return df['ticker'].iloc[:, 0].tolist()
    return df['ticker'].tolist()
