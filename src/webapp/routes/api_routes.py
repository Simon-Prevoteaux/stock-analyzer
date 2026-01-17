"""
API routes - JSON endpoints for AJAX calls
"""

from flask import request, jsonify
from webapp.routes import api_bp
from webapp.extensions import db, fetcher
from webapp.utils import process_stock_fetch
from libs.forecaster import StockForecaster


@api_bp.route('/api/search')
def api_search():
    """API endpoint for stock search"""
    keyword = request.args.get('q', '')
    if keyword:
        results_df = db.search_stocks(keyword)
        return jsonify(results_df.to_dict('records'))
    return jsonify([])


@api_bp.route('/api/all-tickers')
def api_all_tickers():
    """API endpoint to get all tickers as comma-separated string"""
    stocks_df = db.get_all_stocks()
    if stocks_df.empty:
        return jsonify({'tickers': '', 'count': 0})

    tickers = sorted(stocks_df['ticker'].tolist())
    ticker_string = ', '.join(tickers)

    return jsonify({'tickers': ticker_string, 'count': len(tickers)})


@api_bp.route('/api/refresh/<ticker>', methods=['POST'])
def api_refresh_stock(ticker):
    """API endpoint to refresh stock data with historical financials"""
    try:
        # Save snapshot before updating
        old_data = db.get_stock(ticker)
        if old_data:
            db.save_snapshot(ticker, old_data)

        success = process_stock_fetch(ticker, fetcher, db)

        if success:
            current_data = db.get_stock(ticker)
            return jsonify({'success': True, 'data': current_data})
        return jsonify({'success': False, 'error': 'Failed to fetch data'}), 400
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 400


@api_bp.route('/fetch-with-history', methods=['POST'])
def fetch_with_history():
    """Fetch stock data including historical financials"""
    tickers_input = request.form.get('tickers', '')
    tickers = [t.strip().upper() for t in tickers_input.split(',') if t.strip()]

    if not tickers:
        return jsonify({'error': 'No tickers provided'}), 400

    results = []
    for ticker in tickers:
        success = process_stock_fetch(ticker, fetcher, db)
        if success:
            results.append({'ticker': ticker, 'success': True})
        else:
            results.append({'ticker': ticker, 'success': False, 'error': 'Failed to fetch data'})

    return jsonify({'results': results})


@api_bp.route('/api/forecast', methods=['GET'])
def api_forecast():
    """API endpoint for AJAX forecast updates"""
    ticker = request.args.get('ticker', '').upper()

    if not ticker:
        return jsonify({'error': 'Ticker required'}), 400

    stock_data = db.get_stock(ticker)
    if not stock_data:
        return jsonify({'error': f'Stock {ticker} not found'}), 404

    # Get parameters
    years = int(request.args.get('years', 5))
    earnings_growth = float(request.args.get('earnings_growth', 0.15))
    terminal_pe = float(request.args.get('terminal_pe', 20))
    revenue_growth = float(request.args.get('revenue_growth', 0.20))
    terminal_ps = float(request.args.get('terminal_ps', 5))
    fcf_growth = float(request.args.get('fcf_growth', 0.10))
    discount_rate = float(request.args.get('discount_rate', 0.10))
    volatility = float(request.args.get('volatility', 0.30))

    growth_metrics = db.get_growth_metrics(ticker)
    forecaster = StockForecaster(stock_data, growth_metrics)

    growth_decay = 0.1 if earnings_growth > 0.2 else 0.05
    rev_growth_decay = 0.1 if revenue_growth > 0.25 else 0.05
    terminal_growth = 0.03
    simulations = 10000

    results = {
        "ticker": ticker,
        "current_price": stock_data.get('current_price', 0),
        "company_name": stock_data.get('company_name', 'N/A'),
        "earnings_model": forecaster.earnings_growth_model(
            growth_rate=earnings_growth, growth_decay=growth_decay,
            terminal_pe=terminal_pe, years=years),
        "revenue_model": forecaster.revenue_growth_model(
            growth_rate=revenue_growth, growth_decay=rev_growth_decay,
            terminal_ps=terminal_ps, years=years),
        "dcf_model": forecaster.dcf_model(
            fcf_growth=fcf_growth, discount_rate=discount_rate,
            terminal_growth=terminal_growth, years=years),
        "monte_carlo": forecaster.monte_carlo_simulation(
            volatility=volatility, years=years, simulations=simulations),
        "scenarios": forecaster.scenario_analysis(years=years)
    }

    return jsonify(results)


@api_bp.route('/api/calculate-required-growth', methods=['GET'])
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

    required_growth = ((target_mc / current_mc) ** (1 / years)) - 1
    growth_metrics = db.get_growth_metrics(ticker)

    historical_revenue_growth = growth_metrics.get('revenue_cagr_3y') if growth_metrics else None
    historical_earnings_growth = growth_metrics.get('earnings_cagr_3y') if growth_metrics else None

    revenue_growth_val = historical_revenue_growth if historical_revenue_growth is not None else 0
    earnings_growth_val = historical_earnings_growth if historical_earnings_growth is not None else 0

    progression = []
    for year in range(1, years + 1):
        projected_mc = current_mc * ((1 + required_growth) ** year)
        projected_price = stock.get('current_price', 0) * (projected_mc / current_mc)
        progression.append({'year': year, 'market_cap': projected_mc, 'price': projected_price})

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
