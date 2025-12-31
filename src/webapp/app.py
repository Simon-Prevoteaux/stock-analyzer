"""
Flask Web Application for Stock Analysis
"""

from flask import Flask, render_template, request, jsonify, redirect, url_for
import sys
import os
import pandas as pd

# Add parent directory to path to import libs
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from libs.stock_fetcher import StockFetcher
from libs.database import StockDatabase
from libs.stock_lists import get_all_lists
from libs.forecaster import StockForecaster
from libs.growth_analyzer import GrowthAnalyzer

app = Flask(__name__)
app.config['SECRET_KEY'] = 'stock-analyzer-secret-key'

# Initialize components
fetcher = StockFetcher()

# Get absolute path to data directory (project root / data / stocks.db)
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
db_path = os.path.join(project_root, 'data', 'stocks.db')
db = StockDatabase(db_path)


# Make available stocks list available to all templates
@app.context_processor
def inject_available_stocks():
    """Inject available stocks list into all templates for global search"""
    stocks_df = db.get_all_stocks()
    available_stocks = stocks_df['ticker'].tolist() if not stocks_df.empty else []
    return dict(available_stocks=available_stocks)


@app.route('/')
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


@app.route('/fetch', methods=['GET', 'POST'])
def fetch_stocks():
    """Fetch new stock data"""
    # Indicator counts for the info section
    fetched_count = 25  # Number of metrics currently fetched
    not_fetched_count = 28  # Number of available but not fetched metrics

    if request.method == 'POST':
        tickers_input = request.form.get('tickers', '')
        tickers = [t.strip().upper() for t in tickers_input.split(',') if t.strip()]

        if tickers:
            saved_count = 0
            failed_tickers = []

            for ticker in tickers:
                try:
                    # Fetch all data including historical
                    full_data = fetcher.fetch_stock_with_history(ticker)

                    if full_data.get('current'):
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
                            # Pass current FCF and revenue for new metrics
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

                        saved_count += 1
                    else:
                        failed_tickers.append(ticker)
                except Exception as e:
                    print(f"Error fetching {ticker}: {str(e)}")
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


@app.route('/fetch-with-history', methods=['POST'])
def fetch_with_history():
    """Fetch stock data including historical financials"""
    tickers_input = request.form.get('tickers', '')
    tickers = [t.strip().upper() for t in tickers_input.split(',') if t.strip()]

    if not tickers:
        return jsonify({'error': 'No tickers provided'}), 400

    results = []
    for ticker in tickers:
        try:
            # Fetch all data
            full_data = fetcher.fetch_stock_with_history(ticker)

            if full_data.get('current'):
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
                    # Pass current FCF and revenue for new metrics
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

                results.append({'ticker': ticker, 'success': True})
            else:
                results.append({'ticker': ticker, 'success': False, 'error': 'Failed to fetch data'})
        except Exception as e:
            results.append({'ticker': ticker, 'success': False, 'error': str(e)})

    return jsonify({'results': results})


@app.route('/screener')
def screener():
    """Stock screener page"""
    stocks_df = db.get_all_stocks()

    # Get all available stocks for search
    available_stocks = stocks_df['ticker'].tolist() if not stocks_df.empty else []

    # Apply filters
    min_market_cap = request.args.get('min_market_cap', type=float)
    max_pe = request.args.get('max_pe', type=float)
    max_ps = request.args.get('max_ps', type=float)
    sector = request.args.get('sector')
    min_growth = request.args.get('min_growth', type=float)
    risk_level = request.args.get('risk_level')
    max_peg = request.args.get('max_peg', type=float)
    top_sector_performers = request.args.get('top_sector_performers', type=str)  # 'true' or None

    if not stocks_df.empty:
        # Add sector rankings BEFORE filtering
        # This ensures sector medians are calculated from full dataset
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

        # Filter for top 25% sector performers if requested (requires BOTH revenue AND earnings in top 25%)
        if top_sector_performers == 'true':
            stocks_df = stocks_df[
                ((stocks_df['sector_revenue_rank_pct'].notna()) & (stocks_df['sector_revenue_rank_pct'] >= 75)) &
                ((stocks_df['sector_earnings_rank_pct'].notna()) & (stocks_df['sector_earnings_rank_pct'] >= 75))
            ]

        # Add comparison metrics
        stocks_df = fetcher.compare_stocks(stocks_df)

    sectors = db.get_sectors()
    risk_levels = ['LOW', 'MEDIUM', 'HIGH', 'VERY HIGH', 'EXTREME']

    return render_template('screener.html',
                           stocks=stocks_df.to_dict('records') if not stocks_df.empty else [],
                           sectors=sectors,
                           risk_levels=risk_levels,
                           available_stocks=available_stocks,
                           top_sector_performers=top_sector_performers)


@app.route('/stock/<ticker>')
def stock_detail(ticker):
    """Stock detail page with historical data"""
    stock = db.get_stock(ticker.upper())

    if not stock:
        return render_template('error.html', message=f'Stock {ticker} not found'), 404

    # Get growth metrics
    growth_metrics = db.get_growth_metrics(ticker)

    # Get historical data for charts
    quarterly_history = db.get_financial_history(ticker, 'quarterly')
    annual_history = db.get_financial_history(ticker, 'annual')

    # Prepare quarterly chart data
    revenue_chart_data_quarterly = None
    earnings_chart_data_quarterly = None

    if quarterly_history:
        # Sort by date
        sorted_quarterly = sorted(quarterly_history, key=lambda x: x['period_end_date'])

        # Filter out zero values for revenue chart
        filtered_revenue = [(h['period_end_date'][:7], h['revenue'])
                            for h in sorted_quarterly
                            if h['revenue'] and h['revenue'] > 0]

        revenue_chart_data_quarterly = {
            'labels': [item[0] for item in filtered_revenue],  # YYYY-MM format
            'datasets': [{
                'label': 'Quarterly Revenue',
                'data': [item[1] for item in filtered_revenue],
                'borderColor': '#00ff88',
                'fill': True
            }]
        } if filtered_revenue else None

        # Filter out zero values for earnings chart
        filtered_earnings = [(h['period_end_date'][:7], h['earnings'])
                             for h in sorted_quarterly
                             if h['earnings'] and h['earnings'] != 0]  # Allow negative earnings

        earnings_chart_data_quarterly = {
            'labels': [item[0] for item in filtered_earnings],
            'datasets': [{
                'label': 'Quarterly Earnings',
                'data': [item[1] for item in filtered_earnings],
                'borderColor': '#ffd700',
                'fill': True
            }]
        } if filtered_earnings else None

    # Prepare annual chart data (goes back further - usually 4-5 years)
    revenue_chart_data_annual = None
    earnings_chart_data_annual = None

    if annual_history:
        # Sort by date
        sorted_annual = sorted(annual_history, key=lambda x: x['period_end_date'])

        # Filter out zero values for revenue chart
        filtered_revenue_annual = [(h['period_end_date'][:4], h['revenue'])
                                   for h in sorted_annual
                                   if h['revenue'] and h['revenue'] > 0]

        revenue_chart_data_annual = {
            'labels': [item[0] for item in filtered_revenue_annual],  # YYYY format
            'datasets': [{
                'label': 'Annual Revenue',
                'data': [item[1] for item in filtered_revenue_annual],
                'borderColor': '#00ff88',
                'fill': True
            }]
        } if filtered_revenue_annual else None

        # Filter out zero values for earnings chart
        filtered_earnings_annual = [(h['period_end_date'][:4], h['earnings'])
                                    for h in sorted_annual
                                    if h['earnings'] and h['earnings'] != 0]  # Allow negative earnings

        earnings_chart_data_annual = {
            'labels': [item[0] for item in filtered_earnings_annual],
            'datasets': [{
                'label': 'Annual Earnings',
                'data': [item[1] for item in filtered_earnings_annual],
                'borderColor': '#ffd700',
                'fill': True
            }]
        } if filtered_earnings_annual else None

    # Check which strategies this stock qualifies for
    strategy_matches = {
        'value_play': False,
        'quality_growth': False,
        'growth_inflection': False,
        'rule_of_40': False,
        'margin_expansion': False,
        'cash_generative_growth': False
    }

    if stock and growth_metrics:
        # Value Play: P/E < 20, P/S < 3, Profitable, Consistency > 60, Growth > 5%
        if (stock.get('pe_ratio') and stock['pe_ratio'] <= 20 and
            stock.get('ps_ratio') and stock['ps_ratio'] <= 3 and
            stock.get('is_profitable') and
            (growth_metrics.get('revenue_consistency_score', 0) >= 60 or
             growth_metrics.get('earnings_consistency_score', 0) >= 60) and
            (growth_metrics.get('avg_quarterly_revenue_growth', 0) >= 0.05 or
             growth_metrics.get('avg_quarterly_earnings_growth', 0) >= 0.05)):
            strategy_matches['value_play'] = True

        # Quality Growth: CAGR >= 20%, Consistency >= 70, PEG <= 2.5
        if (((growth_metrics.get('earnings_cagr_3y') or 0) >= 0.20 or
             (growth_metrics.get('revenue_cagr_3y') or 0) >= 0.20) and
            (growth_metrics.get('earnings_consistency_score') or 0) >= 70 and
            growth_metrics.get('peg_average') and growth_metrics['peg_average'] <= 2.5):
            strategy_matches['quality_growth'] = True

        # Growth Inflection: Accelerating growth + Consistency >= 60 + P/E <= 40
        if ((growth_metrics.get('revenue_growth_accelerating') or
             growth_metrics.get('earnings_growth_accelerating')) and
            (growth_metrics.get('earnings_consistency_score', 0) >= 60 or
             growth_metrics.get('revenue_consistency_score', 0) >= 60) and
            (not stock.get('pe_ratio') or stock['pe_ratio'] <= 40)):
            strategy_matches['growth_inflection'] = True

        # Rule of 40: Rule of 40 >= 40
        if growth_metrics.get('rule_of_40') and growth_metrics['rule_of_40'] >= 40:
            strategy_matches['rule_of_40'] = True

        # Margin Expansion: Revenue CAGR >= 15%, Margin Trend = expanding, Operating Leverage >= 1.0
        if ((growth_metrics.get('revenue_cagr_3y') or 0) >= 0.15 and
            growth_metrics.get('margin_trend') == 'expanding' and
            growth_metrics.get('operating_leverage') and growth_metrics['operating_leverage'] >= 1.0):
            strategy_matches['margin_expansion'] = True

        # Cash-Generative Growth: Revenue CAGR >= 20%, FCF > 0, FCF Margin >= 10%, Cash Conversion > 0.8
        if ((growth_metrics.get('revenue_cagr_3y') or 0) >= 0.20 and
            stock.get('free_cash_flow') and stock['free_cash_flow'] > 0 and
            growth_metrics.get('fcf_margin') and growth_metrics['fcf_margin'] >= 0.10 and
            growth_metrics.get('cash_conversion_ratio') and growth_metrics['cash_conversion_ratio'] > 0.8):
            strategy_matches['cash_generative_growth'] = True

    return render_template('stock_detail.html',
                         stock=stock,
                         growth_metrics=growth_metrics,
                         strategy_matches=strategy_matches,
                         has_historical_data=bool(quarterly_history or annual_history),
                         revenue_chart_data_quarterly=revenue_chart_data_quarterly,
                         earnings_chart_data_quarterly=earnings_chart_data_quarterly,
                         revenue_chart_data_annual=revenue_chart_data_annual,
                         earnings_chart_data_annual=earnings_chart_data_annual,
                         quarterly_periods=len(quarterly_history) if quarterly_history else 0,
                         annual_periods=len(annual_history) if annual_history else 0)


@app.route('/comparison')
def comparison():
    """Compare multiple stocks"""
    tickers_param = request.args.get('tickers', '')
    tickers = [t.strip().upper() for t in tickers_param.split(',') if t.strip()]

    # Get all available stocks for search
    stocks_df = db.get_all_stocks()
    available_stocks = stocks_df['ticker'].tolist() if not stocks_df.empty else []

    stocks = []
    growth_metrics_list = []
    if tickers:
        for ticker in tickers:
            stock = db.get_stock(ticker)
            if stock:
                stocks.append(stock)
                # Fetch growth metrics for each stock
                growth_metrics = db.get_growth_metrics(ticker)
                growth_metrics_list.append(growth_metrics if growth_metrics else {})

    # Get watchlist for suggestions
    watchlist_df = db.get_watchlist()
    # Handle potential duplicate 'ticker' columns from JOIN
    if not watchlist_df.empty:
        if isinstance(watchlist_df['ticker'], pd.DataFrame):
            # If 'ticker' returns a DataFrame (duplicate columns), get the first one
            watchlist_tickers = watchlist_df['ticker'].iloc[:, 0].tolist()
        else:
            watchlist_tickers = watchlist_df['ticker'].tolist()
    else:
        watchlist_tickers = []

    # Get portfolio for suggestions
    portfolio_df = db.get_portfolio()
    if not portfolio_df.empty:
        if isinstance(portfolio_df['ticker'], pd.DataFrame):
            portfolio_tickers = portfolio_df['ticker'].iloc[:, 0].tolist()
        else:
            portfolio_tickers = portfolio_df['ticker'].tolist()
    else:
        portfolio_tickers = []

    # Get some curated comparison suggestions
    stock_lists = get_all_lists()
    suggestions = {
        'watchlist': {
            'name': 'My Watchlist',
            'tickers': watchlist_tickers[:10],  # Limit to 10 for comparison
            'description': 'Compare stocks from your watchlist'
        },
        'portfolio': {
            'name': 'My Portfolio',
            'tickers': portfolio_tickers[:10],  # Limit to 10 for comparison
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

    return render_template('comparison.html', stocks=stocks, growth_metrics_list=growth_metrics_list, suggestions=suggestions, available_stocks=available_stocks)


@app.route('/watchlist')
def watchlist():
    """User's watchlist"""
    watchlist_df = db.get_watchlist()
    return render_template('watchlist.html',
                           stocks=watchlist_df.to_dict('records') if not watchlist_df.empty else [])


@app.route('/watchlist/add/<ticker>', methods=['POST'])
def add_to_watchlist(ticker):
    """Add stock to watchlist"""
    notes = request.form.get('notes', '')
    db.add_to_watchlist(ticker, notes)
    return redirect(url_for('watchlist'))


@app.route('/watchlist/remove/<ticker>', methods=['POST'])
def remove_from_watchlist(ticker):
    """Remove stock from watchlist"""
    db.remove_from_watchlist(ticker)
    return redirect(url_for('watchlist'))


@app.route('/portfolio')
def portfolio():
    """User's portfolio"""
    portfolio_df = db.get_portfolio()
    return render_template('portfolio.html',
                           stocks=portfolio_df.to_dict('records') if not portfolio_df.empty else [])


@app.route('/portfolio/add/<ticker>', methods=['POST'])
def add_to_portfolio(ticker):
    """Add stock to portfolio"""
    notes = request.form.get('notes', '')
    db.add_to_portfolio(ticker, notes)
    return redirect(url_for('portfolio'))


@app.route('/portfolio/remove/<ticker>', methods=['POST'])
def remove_from_portfolio(ticker):
    """Remove stock from portfolio"""
    db.remove_from_portfolio(ticker)
    return redirect(url_for('portfolio'))


@app.route('/api/search')
def api_search():
    """API endpoint for stock search"""
    keyword = request.args.get('q', '')
    if keyword:
        results_df = db.search_stocks(keyword)
        return jsonify(results_df.to_dict('records'))
    return jsonify([])


@app.route('/api/all-tickers')
def api_all_tickers():
    """API endpoint to get all tickers as comma-separated string"""
    stocks_df = db.get_all_stocks()
    if stocks_df.empty:
        return jsonify({'tickers': '', 'count': 0})

    tickers = sorted(stocks_df['ticker'].tolist())
    ticker_string = ', '.join(tickers)

    return jsonify({
        'tickers': ticker_string,
        'count': len(tickers)
    })


@app.route('/api/refresh/<ticker>', methods=['POST'])
def api_refresh_stock(ticker):
    """API endpoint to refresh stock data with historical financials"""
    try:
        # Fetch all data including historical
        full_data = fetcher.fetch_stock_with_history(ticker)

        if full_data.get('current'):
            # Save snapshot before updating
            old_data = db.get_stock(ticker)
            if old_data:
                db.save_snapshot(ticker, old_data)

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
                # Pass current FCF and revenue for new metrics
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

            return jsonify({'success': True, 'data': full_data['current']})
        return jsonify({'success': False, 'error': 'Failed to fetch data'}), 400
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 400


@app.route('/refresh-all', methods=['POST'])
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
        try:
            print(f"Refreshing {ticker} with historical data...")

            # Fetch all data including historical
            full_data = fetcher.fetch_stock_with_history(ticker)

            if full_data.get('current'):
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
                    # Pass current FCF and revenue for new metrics
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

                saved_count += 1
            else:
                failed_tickers.append(ticker)
        except Exception as e:
            print(f"Error refreshing {ticker}: {str(e)}")
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


@app.route('/delete/<ticker>', methods=['POST'])
def delete_stock(ticker):
    """Delete a stock from the database"""
    if db.delete_stock(ticker):
        return redirect(url_for('screener'))
    else:
        return render_template('error.html', message=f'Failed to delete stock {ticker}'), 400


@app.route('/value-plays')
def value_plays():
    """Show potential value stocks with enhanced filtering"""
    # Get filter parameters
    min_consistency = request.args.get('min_consistency', 60, type=float)
    require_growth = request.args.get('require_growth', 'false').lower() == 'true'
    use_enhanced = request.args.get('use_enhanced', 'false').lower() == 'true'

    # Get enhanced value stocks if filters are active
    if use_enhanced:
        enhanced_stocks_df = db.get_enhanced_value_stocks(
            min_consistency=min_consistency,
            min_growth=0.05 if require_growth else 0
        )
    else:
        enhanced_stocks_df = pd.DataFrame()

    # Still get traditional value stocks for comparison
    value_stocks_df = db.get_value_stocks()
    near_value_stocks_df = db.get_near_value_stocks()

    return render_template('value_plays.html',
                         stocks=value_stocks_df.to_dict('records') if not value_stocks_df.empty else [],
                         near_value_stocks=near_value_stocks_df.to_dict('records') if not near_value_stocks_df.empty else [],
                         enhanced_stocks=enhanced_stocks_df.to_dict('records') if not enhanced_stocks_df.empty else [],
                         min_consistency=min_consistency,
                         require_growth=require_growth,
                         use_enhanced=use_enhanced)


@app.route('/bubble-territory')
def bubble_territory():
    """Show stocks in bubble territory"""
    bubble_stocks_df = db.get_high_risk_stocks(min_bubble_score=6)
    return render_template('bubble_territory.html',
                           stocks=bubble_stocks_df.to_dict('records') if not bubble_stocks_df.empty else [])


@app.route('/quality-growth')
def quality_growth():
    """Quality growth stocks with sustainable, consistent patterns"""
    # Get filter parameters
    min_cagr = request.args.get('min_cagr', 20, type=float)
    min_consistency = request.args.get('min_consistency', 70, type=int)
    max_peg = request.args.get('max_peg', 2.5, type=float)

    # Get quality growth stocks
    stocks_df = db.get_quality_growth_stocks(min_cagr, min_consistency, max_peg)

    # Get all stocks for search
    all_stocks_df = db.get_all_stocks()
    available_stocks = all_stocks_df['ticker'].tolist() if not all_stocks_df.empty else []

    return render_template('quality_growth.html',
                         stocks=stocks_df.to_dict('records') if not stocks_df.empty else [],
                         available_stocks=available_stocks,
                         min_cagr=min_cagr,
                         min_consistency=min_consistency,
                         max_peg=max_peg)


@app.route('/growth-inflection')
def growth_inflection():
    """Stocks showing growth acceleration (inflection points)"""
    # Get filter parameters
    min_consistency = request.args.get('min_consistency', 60, type=int)
    max_pe = request.args.get('max_pe', 40, type=float)

    # Get growth inflection stocks
    stocks_df = db.get_growth_inflection_stocks(min_consistency, max_pe)

    # Get all stocks for search
    all_stocks_df = db.get_all_stocks()
    available_stocks = all_stocks_df['ticker'].tolist() if not all_stocks_df.empty else []

    return render_template('growth_inflection.html',
                         stocks=stocks_df.to_dict('records') if not stocks_df.empty else [],
                         available_stocks=available_stocks,
                         min_consistency=min_consistency,
                         max_pe=max_pe)


@app.route('/rule-of-40')
def rule_of_40():
    """Stocks with efficient growth (Rule of 40 metric for SaaS/cloud companies)"""
    # Get filter parameters
    min_rule_of_40 = request.args.get('min_rule_of_40', 40, type=float)
    sector_filter = request.args.get('sector', type=str)

    # Get Rule of 40 stocks
    stocks_df = db.get_rule_of_40_stocks(min_rule_of_40, sector_filter)

    # Get sectors for filter dropdown
    sectors = db.get_sectors()

    # Get all stocks for search
    all_stocks_df = db.get_all_stocks()
    available_stocks = all_stocks_df['ticker'].tolist() if not all_stocks_df.empty else []

    return render_template('rule_of_40.html',
                         stocks=stocks_df.to_dict('records') if not stocks_df.empty else [],
                         available_stocks=available_stocks,
                         min_rule_of_40=min_rule_of_40,
                         sector_filter=sector_filter,
                         sectors=sectors)


@app.route('/margin-expansion')
def margin_expansion():
    """Stocks with expanding margins and improving profitability"""
    # Get filter parameters
    min_revenue_growth = request.args.get('min_revenue_growth', 15, type=float)
    min_operating_leverage = request.args.get('min_operating_leverage', 1.0, type=float)

    # Get margin expansion stocks
    stocks_df = db.get_margin_expansion_stocks(min_revenue_growth, min_operating_leverage)

    # Get all stocks for search
    all_stocks_df = db.get_all_stocks()
    available_stocks = all_stocks_df['ticker'].tolist() if not all_stocks_df.empty else []

    return render_template('margin_expansion.html',
                         stocks=stocks_df.to_dict('records') if not stocks_df.empty else [],
                         available_stocks=available_stocks,
                         min_revenue_growth=min_revenue_growth,
                         min_operating_leverage=min_operating_leverage)


@app.route('/cash-generative-growth')
def cash_generative_growth():
    """High-growth stocks that also generate positive free cash flow"""
    # Get filter parameters
    min_revenue_growth = request.args.get('min_revenue_growth', 20, type=float)
    min_fcf_margin = request.args.get('min_fcf_margin', 10, type=float)

    # Get cash-generative growth stocks
    stocks_df = db.get_cash_generative_growth_stocks(min_revenue_growth, min_fcf_margin)

    # Get all stocks for search
    all_stocks_df = db.get_all_stocks()
    available_stocks = all_stocks_df['ticker'].tolist() if not all_stocks_df.empty else []

    return render_template('cash_generative_growth.html',
                         stocks=stocks_df.to_dict('records') if not stocks_df.empty else [],
                         available_stocks=available_stocks,
                         min_revenue_growth=min_revenue_growth,
                         min_fcf_margin=min_fcf_margin)


@app.route('/forecast')
def forecast():
    """Stock price forecasting page"""
    ticker = request.args.get('ticker', '').upper()
    stocks_df = db.get_all_stocks()
    available_stocks = stocks_df['ticker'].tolist() if not stocks_df.empty else []

    forecast_results = None
    stock_data = None
    error = None

    if ticker:
        stock_data = db.get_stock(ticker)
        if stock_data:
            # Get historical growth metrics for smarter forecasting
            growth_metrics = db.get_growth_metrics(ticker)
            forecaster = StockForecaster(stock_data, growth_metrics)

            # Get parameters from request or use defaults
            years = request.args.get('years', 5, type=int)

            # Earnings model parameters
            earnings_growth = request.args.get('earnings_growth', type=float)
            growth_decay = request.args.get('growth_decay', 0.10, type=float)
            terminal_pe = request.args.get('terminal_pe', type=float)

            # Revenue model parameters
            revenue_growth = request.args.get('revenue_growth', type=float)
            rev_growth_decay = request.args.get('rev_growth_decay', 0.15, type=float)
            terminal_ps = request.args.get('terminal_ps', type=float)

            # DCF parameters
            fcf_growth = request.args.get('fcf_growth', 0.10, type=float)
            discount_rate = request.args.get('discount_rate', 0.10, type=float)
            terminal_growth = request.args.get('terminal_growth', 0.03, type=float)

            # Monte Carlo parameters
            volatility = request.args.get('volatility', 0.30, type=float)
            simulations = request.args.get('simulations', 1000, type=int)

            # PEG valuation parameters
            peg_growth_rate = request.args.get('peg_growth_rate', type=float)
            fair_peg = request.args.get('fair_peg', type=float)

            # Use run_all_models which includes new classical models and consensus
            forecast_results = forecaster.run_all_models(years=years)

            # Override with custom parameters if provided
            if earnings_growth or terminal_pe:
                forecast_results["earnings_model"] = forecaster.earnings_growth_model(
                    growth_rate=earnings_growth,
                    growth_decay=growth_decay,
                    terminal_pe=terminal_pe,
                    years=years
                )
            if revenue_growth or terminal_ps:
                forecast_results["revenue_model"] = forecaster.revenue_growth_model(
                    growth_rate=revenue_growth,
                    growth_decay=rev_growth_decay,
                    terminal_ps=terminal_ps,
                    years=years
                )
            if fcf_growth != 0.10 or discount_rate != 0.10:
                forecast_results["dcf_model"] = forecaster.dcf_model(
                    fcf_growth=fcf_growth,
                    discount_rate=discount_rate,
                    terminal_growth=terminal_growth,
                    years=years
                )
            if volatility != 0.30 or simulations != 1000:
                forecast_results["monte_carlo"] = forecaster.monte_carlo_simulation(
                    volatility=volatility,
                    years=years,
                    simulations=simulations
                )
            if peg_growth_rate or fair_peg:
                forecast_results["peg_valuation"] = forecaster.peg_based_valuation(
                    growth_rate=peg_growth_rate,
                    fair_peg=fair_peg
                )

            # Recalculate consensus if any forecast parameters were provided
            # Check if user has modified any parameters (any request args means parameters were changed)
            forecast_params = ['earnings_growth', 'growth_decay', 'terminal_pe', 'revenue_growth',
                             'rev_growth_decay', 'terminal_ps', 'fcf_growth', 'discount_rate',
                             'terminal_growth', 'volatility', 'simulations', 'peg_growth_rate', 'fair_peg']
            has_custom_params = any(param in request.args for param in forecast_params)

            if has_custom_params:
                all_models = [
                    forecast_results["earnings_model"],
                    forecast_results["revenue_model"],
                    forecast_results["dcf_model"],
                    forecast_results["monte_carlo"],
                    forecast_results.get("graham_number"),
                    forecast_results.get("gordon_growth"),
                    forecast_results.get("peg_valuation"),
                    forecast_results.get("ps_sector")
                ]
                forecast_results["consensus"] = forecaster.calculate_consensus([m for m in all_models if m])
        else:
            error = f"Stock {ticker} not found in database. Please fetch it first."

    # Prepare growth rate source information for UI
    growth_rate_info = None
    if forecast_results:
        # Collect all available sources for user selection
        revenue_sources = []
        earnings_sources = []

        # Add historical 3Y CAGR if available
        if growth_metrics and growth_metrics.get('revenue_cagr_3y'):
            revenue_sources.append({
                'key': 'cagr_3y',
                'label': '3-Year CAGR (Historical)',
                'value': growth_metrics.get('revenue_cagr_3y')
            })
        if growth_metrics and growth_metrics.get('earnings_cagr_3y'):
            earnings_sources.append({
                'key': 'cagr_3y',
                'label': '3-Year CAGR (Historical)',
                'value': growth_metrics.get('earnings_cagr_3y')
            })

        # Add quarterly average if available (annualized)
        if growth_metrics and growth_metrics.get('avg_quarterly_revenue_growth'):
            quarterly_rev = growth_metrics.get('avg_quarterly_revenue_growth')
            annualized_rev = ((1 + quarterly_rev) ** 4 - 1) if quarterly_rev else 0
            revenue_sources.append({
                'key': 'quarterly_avg',
                'label': 'Quarterly Average - Annualized (Recent Trend)',
                'value': annualized_rev
            })
        if growth_metrics and growth_metrics.get('avg_quarterly_earnings_growth'):
            quarterly_earn = growth_metrics.get('avg_quarterly_earnings_growth')
            annualized_earn = ((1 + quarterly_earn) ** 4 - 1) if quarterly_earn else 0
            earnings_sources.append({
                'key': 'quarterly_avg',
                'label': 'Quarterly Average - Annualized (Recent Trend)',
                'value': annualized_earn
            })

        # Always add yfinance single-point as fallback
        if stock_data.get('revenue_growth'):
            revenue_sources.append({
                'key': 'yfinance',
                'label': 'yfinance (Single-Point)',
                'value': stock_data.get('revenue_growth', 0)
            })
        if stock_data.get('earnings_growth'):
            earnings_sources.append({
                'key': 'yfinance',
                'label': 'yfinance (Single-Point)',
                'value': stock_data.get('earnings_growth', 0)
            })

        growth_rate_info = {
            'has_historical_data': bool(growth_metrics),
            'revenue_growth_used': forecaster.revenue_growth,
            'earnings_growth_used': forecaster.earnings_growth,
            'revenue_source': 'Historical 3Y CAGR' if growth_metrics and growth_metrics.get('revenue_cagr_3y') else
                            ('Historical Quarterly Avg' if growth_metrics and growth_metrics.get('avg_quarterly_revenue_growth') else 'yfinance'),
            'earnings_source': 'Historical 3Y CAGR' if growth_metrics and growth_metrics.get('earnings_cagr_3y') else
                             ('Historical Quarterly Avg' if growth_metrics and growth_metrics.get('avg_quarterly_earnings_growth') else 'yfinance'),
            'revenue_cagr_3y': growth_metrics.get('revenue_cagr_3y') if growth_metrics else None,
            'earnings_cagr_3y': growth_metrics.get('earnings_cagr_3y') if growth_metrics else None,
            'consistency_score': growth_metrics.get('revenue_consistency_score') if growth_metrics else None,
            'revenue_sources': revenue_sources,
            'earnings_sources': earnings_sources
        }

    return render_template('forecast.html',
                           available_stocks=available_stocks,
                           selected_ticker=ticker,
                           stock=stock_data,
                           results=forecast_results,
                           growth_rate_info=growth_rate_info,
                           error=error)


@app.route('/api/forecast', methods=['GET'])
def api_forecast():
    """API endpoint for AJAX forecast updates"""
    ticker = request.args.get('ticker', '').upper()

    if not ticker:
        return jsonify({'error': 'Ticker required'}), 400

    stock_data = db.get_stock(ticker)
    if not stock_data:
        return jsonify({'error': f'Stock {ticker} not found'}), 404

    # Get parameters from request
    years = int(request.args.get('years', 5))
    earnings_growth = float(request.args.get('earnings_growth', 0.15))
    terminal_pe = float(request.args.get('terminal_pe', 20))
    revenue_growth = float(request.args.get('revenue_growth', 0.20))
    terminal_ps = float(request.args.get('terminal_ps', 5))
    fcf_growth = float(request.args.get('fcf_growth', 0.10))
    discount_rate = float(request.args.get('discount_rate', 0.10))
    volatility = float(request.args.get('volatility', 0.30))

    # Get growth metrics for historical data
    growth_metrics = db.get_growth_metrics(ticker)

    # Create forecaster
    forecaster = StockForecaster(stock_data, growth_metrics)

    # Growth decay
    growth_decay = 0.1 if earnings_growth > 0.2 else 0.05
    rev_growth_decay = 0.1 if revenue_growth > 0.25 else 0.05
    terminal_growth = 0.03
    simulations = 10000

    # Calculate all models
    results = {
        "ticker": ticker,
        "current_price": stock_data.get('current_price', 0),
        "company_name": stock_data.get('company_name', 'N/A'),
        "earnings_model": forecaster.earnings_growth_model(
            growth_rate=earnings_growth,
            growth_decay=growth_decay,
            terminal_pe=terminal_pe,
            years=years
        ),
        "revenue_model": forecaster.revenue_growth_model(
            growth_rate=revenue_growth,
            growth_decay=rev_growth_decay,
            terminal_ps=terminal_ps,
            years=years
        ),
        "dcf_model": forecaster.dcf_model(
            fcf_growth=fcf_growth,
            discount_rate=discount_rate,
            terminal_growth=terminal_growth,
            years=years
        ),
        "monte_carlo": forecaster.monte_carlo_simulation(
            volatility=volatility,
            years=years,
            simulations=simulations
        ),
        "scenarios": forecaster.scenario_analysis(years=years)
    }

    return jsonify(results)


@app.route('/upside')
def upside_calculator():
    """Upside calculator - see potential upside to target market cap"""
    # Get all stocks for analysis
    stocks_df = db.get_all_stocks()

    # Get target market cap from query params (in billions)
    target_market_cap_b = request.args.get('target', 1000, type=float)
    target_market_cap = target_market_cap_b * 1_000_000_000  # Convert to actual value

    # Calculate upside for each stock
    stocks_with_upside = []
    if not stocks_df.empty:
        for _, stock in stocks_df.iterrows():
            current_mc = stock.get('market_cap', 0)
            if current_mc and current_mc > 0:
                upside_multiple = target_market_cap / current_mc
                upside_percent = (upside_multiple - 1) * 100
                target_price = stock.get('current_price', 0) * upside_multiple

                stocks_with_upside.append({
                    'ticker': stock['ticker'],
                    'company_name': stock.get('company_name', 'N/A'),
                    'current_market_cap': current_mc,
                    'current_price': stock.get('current_price', 0),
                    'target_market_cap': target_market_cap,
                    'upside_multiple': upside_multiple,
                    'upside_percent': upside_percent,
                    'target_price': target_price,
                    'sector': stock.get('sector', 'N/A')
                })

    # Sort by upside percent (highest first)
    stocks_with_upside.sort(key=lambda x: x['upside_percent'], reverse=True)

    # Get growth metrics for reverse calculation
    growth_data = []
    for stock in stocks_with_upside[:20]:  # Top 20 for performance
        growth_metrics = db.get_growth_metrics(stock['ticker'])
        if growth_metrics:
            growth_data.append({
                'ticker': stock['ticker'],
                'revenue_cagr_3y': growth_metrics.get('revenue_cagr_3y', 0),
                'earnings_cagr_3y': growth_metrics.get('earnings_cagr_3y', 0)
            })

    return render_template('upside_calculator.html',
                          stocks=stocks_with_upside,
                          target_market_cap_b=target_market_cap_b,
                          growth_data=growth_data)


@app.route('/api/calculate-required-growth', methods=['GET'])
def calculate_required_growth():
    """Calculate what growth rate is needed to reach target market cap in X years"""
    ticker = request.args.get('ticker', '').upper()
    target_mc = float(request.args.get('target_mc', 0))
    years = int(request.args.get('years', 5))

    stock = db.get_stock(ticker)
    if not stock:
        return jsonify({'error': 'Stock not found'}), 404

    current_mc = stock.get('market_cap', 0)
    if current_mc <= 0:
        return jsonify({'error': 'Invalid market cap'}), 400

    # Calculate required CAGR: (target/current)^(1/years) - 1
    required_growth = ((target_mc / current_mc) ** (1 / years)) - 1

    # Get historical growth for comparison
    growth_metrics = db.get_growth_metrics(ticker)

    # Handle None values - default to 0
    historical_revenue_growth = growth_metrics.get('revenue_cagr_3y') if growth_metrics else None
    historical_earnings_growth = growth_metrics.get('earnings_cagr_3y') if growth_metrics else None

    # Convert None to 0 for calculations
    revenue_growth_val = historical_revenue_growth if historical_revenue_growth is not None else 0
    earnings_growth_val = historical_earnings_growth if historical_earnings_growth is not None else 0

    # Calculate year-by-year progression
    progression = []
    for year in range(1, years + 1):
        projected_mc = current_mc * ((1 + required_growth) ** year)
        projected_price = stock.get('current_price', 0) * (projected_mc / current_mc)
        progression.append({
            'year': year,
            'market_cap': projected_mc,
            'price': projected_price
        })

    # Determine feasibility based on historical growth
    is_feasible = None
    if growth_metrics and (revenue_growth_val > 0 or earnings_growth_val > 0):
        max_historical_growth = max(revenue_growth_val, earnings_growth_val)
        is_feasible = required_growth <= max_historical_growth * 1.5

    return jsonify({
        'ticker': ticker,
        'company_name': stock.get('company_name', 'N/A'),
        'current_market_cap': current_mc,
        'target_market_cap': target_mc,
        'years': years,
        'required_cagr': required_growth,
        'historical_revenue_cagr': revenue_growth_val if revenue_growth_val > 0 else None,
        'historical_earnings_cagr': earnings_growth_val if earnings_growth_val > 0 else None,
        'is_feasible': is_feasible,
        'progression': progression
    })


@app.template_filter('format_number')
def format_number(value):
    """Format large numbers"""
    if value is None or value == 0:
        return 'N/A'

    try:
        value = float(value)
        if value >= 1_000_000_000_000:
            return f'${value / 1_000_000_000_000:.2f}T'
        elif value >= 1_000_000_000:
            return f'${value / 1_000_000_000:.2f}B'
        elif value >= 1_000_000:
            return f'${value / 1_000_000:.2f}M'
        elif value >= 1_000:
            return f'${value / 1_000:.2f}K'
        else:
            return f'${value:.2f}'
    except (ValueError, TypeError):
        return 'N/A'


@app.template_filter('format_percent')
def format_percent(value):
    """Format percentage"""
    if value is None:
        return 'N/A'

    try:
        value = float(value)
        return f'{value * 100:.2f}%'
    except (ValueError, TypeError):
        return 'N/A'


@app.template_filter('format_ratio')
def format_ratio(value):
    """Format ratio"""
    if value is None or value == 0:
        return 'N/A'

    try:
        value = float(value)
        return f'{value:.2f}x'
    except (ValueError, TypeError):
        return 'N/A'


if __name__ == '__main__':
    import os
    port = int(os.environ.get('PORT', 5000))
    app.run(debug=True, host='0.0.0.0', port=port)
