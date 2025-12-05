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

app = Flask(__name__)
app.config['SECRET_KEY'] = 'stock-analyzer-secret-key'

# Initialize components
fetcher = StockFetcher()

# Get absolute path to data directory (project root / data / stocks.db)
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
db_path = os.path.join(project_root, 'data', 'stocks.db')
db = StockDatabase(db_path)


@app.route('/')
def index():
    """Home page with overview"""
    stats = {
        'total_stocks': len(db.get_all_stocks()),
        'sectors': len(db.get_sectors()),
        'high_risk': len(db.get_high_risk_stocks()),
        'watchlist_count': len(db.get_watchlist())
    }
    return render_template('index.html', stats=stats)


@app.route('/fetch', methods=['GET', 'POST'])
def fetch_stocks():
    """Fetch new stock data"""
    # Indicator counts for the info section
    fetched_count = 20  # Number of metrics currently fetched
    not_fetched_count = 33  # Number of available but not fetched metrics

    if request.method == 'POST':
        tickers_input = request.form.get('tickers', '')
        tickers = [t.strip().upper() for t in tickers_input.split(',') if t.strip()]

        if tickers:
            df = fetcher.fetch_multiple_stocks(tickers)
            if not df.empty:
                saved_count = db.save_multiple_stocks(df)
                return render_template('fetch.html',
                                       stock_lists=get_all_lists(),
                                       fetched_count=fetched_count,
                                       not_fetched_count=not_fetched_count,
                                       success=True,
                                       message=f'Successfully fetched and saved {saved_count} stocks')
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

    if not stocks_df.empty:
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

        # Add comparison metrics
        stocks_df = fetcher.compare_stocks(stocks_df)

    sectors = db.get_sectors()
    risk_levels = ['LOW', 'MEDIUM', 'HIGH', 'VERY HIGH', 'EXTREME']

    return render_template('screener.html',
                           stocks=stocks_df.to_dict('records') if not stocks_df.empty else [],
                           sectors=sectors,
                           risk_levels=risk_levels,
                           available_stocks=available_stocks)


@app.route('/stock/<ticker>')
def stock_detail(ticker):
    """Stock detail page"""
    stock = db.get_stock(ticker.upper())

    if not stock:
        return render_template('error.html', message=f'Stock {ticker} not found'), 404

    return render_template('stock_detail.html', stock=stock)


@app.route('/comparison')
def comparison():
    """Compare multiple stocks"""
    tickers_param = request.args.get('tickers', '')
    tickers = [t.strip().upper() for t in tickers_param.split(',') if t.strip()]

    # Get all available stocks for search
    stocks_df = db.get_all_stocks()
    available_stocks = stocks_df['ticker'].tolist() if not stocks_df.empty else []

    stocks = []
    if tickers:
        for ticker in tickers:
            stock = db.get_stock(ticker)
            if stock:
                stocks.append(stock)

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

    # Get some curated comparison suggestions
    stock_lists = get_all_lists()
    suggestions = {
        'watchlist': {
            'name': 'My Watchlist',
            'tickers': watchlist_tickers[:10],  # Limit to 10 for comparison
            'description': 'Compare stocks from your watchlist'
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

    return render_template('comparison.html', stocks=stocks, suggestions=suggestions, available_stocks=available_stocks)


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


@app.route('/api/search')
def api_search():
    """API endpoint for stock search"""
    keyword = request.args.get('q', '')
    if keyword:
        results_df = db.search_stocks(keyword)
        return jsonify(results_df.to_dict('records'))
    return jsonify([])


@app.route('/api/refresh/<ticker>', methods=['POST'])
def api_refresh_stock(ticker):
    """API endpoint to refresh stock data"""
    data = fetcher.fetch_stock_data(ticker)
    if data:
        # Save snapshot before updating
        old_data = db.get_stock(ticker)
        if old_data:
            db.save_snapshot(ticker, old_data)

        # Update with new data
        db.save_stock(data)
        return jsonify({'success': True, 'data': data})
    return jsonify({'success': False, 'error': 'Failed to fetch data'}), 400


@app.route('/refresh-all', methods=['POST'])
def refresh_all_stocks():
    """Refresh all stocks in the database"""
    stocks_df = db.get_all_stocks()
    if stocks_df.empty:
        return render_template('fetch.html',
                               stock_lists=get_all_lists(),
                               error=True,
                               message='No stocks to refresh')

    tickers = stocks_df['ticker'].tolist()
    df = fetcher.fetch_multiple_stocks(tickers)

    if not df.empty:
        saved_count = db.save_multiple_stocks(df)
        return render_template('fetch.html',
                               stock_lists=get_all_lists(),
                               success=True,
                               message=f'Successfully refreshed {saved_count} stocks')
    else:
        return render_template('fetch.html',
                               stock_lists=get_all_lists(),
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
    """Show potential value stocks"""
    value_stocks_df = db.get_value_stocks()
    near_value_stocks_df = db.get_near_value_stocks()
    return render_template('value_plays.html',
                           stocks=value_stocks_df.to_dict('records') if not value_stocks_df.empty else [],
                           near_value_stocks=near_value_stocks_df.to_dict('records') if not near_value_stocks_df.empty else [])


@app.route('/bubble-territory')
def bubble_territory():
    """Show stocks in bubble territory"""
    bubble_stocks_df = db.get_high_risk_stocks(min_bubble_score=6)
    return render_template('bubble_territory.html',
                           stocks=bubble_stocks_df.to_dict('records') if not bubble_stocks_df.empty else [])


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
            forecaster = StockForecaster(stock_data)

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

            forecast_results = {
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
        else:
            error = f"Stock {ticker} not found in database. Please fetch it first."

    return render_template('forecast.html',
                           available_stocks=available_stocks,
                           selected_ticker=ticker,
                           stock=stock_data,
                           results=forecast_results,
                           error=error)


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
    app.run(debug=True, host='0.0.0.0', port=5000)
