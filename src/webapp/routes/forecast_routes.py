"""
Forecast routes - Price forecasting and upside calculator
"""

from flask import render_template, request, jsonify
from webapp.routes import forecast_bp
from webapp.extensions import db
from libs.forecaster import StockForecaster


@forecast_bp.route('/forecast')
def forecast():
    """Stock price forecasting page"""
    ticker = request.args.get('ticker', '').upper()
    stocks_df = db.get_all_stocks()
    available_stocks = stocks_df['ticker'].tolist() if not stocks_df.empty else []

    forecast_results = None
    stock_data = None
    growth_metrics = None
    error = None

    if ticker:
        stock_data = db.get_stock(ticker)
        if stock_data:
            growth_metrics = db.get_growth_metrics(ticker)
            forecaster = StockForecaster(stock_data, growth_metrics)

            # Get parameters
            years = request.args.get('years', 5, type=int)
            earnings_growth = request.args.get('earnings_growth', type=float)
            growth_decay = request.args.get('growth_decay', 0.10, type=float)
            terminal_pe = request.args.get('terminal_pe', type=float)
            revenue_growth = request.args.get('revenue_growth', type=float)
            rev_growth_decay = request.args.get('rev_growth_decay', 0.15, type=float)
            terminal_ps = request.args.get('terminal_ps', type=float)
            fcf_growth = request.args.get('fcf_growth', 0.10, type=float)
            discount_rate = request.args.get('discount_rate', 0.10, type=float)
            terminal_growth = request.args.get('terminal_growth', 0.03, type=float)
            volatility = request.args.get('volatility', 0.30, type=float)
            simulations = request.args.get('simulations', 1000, type=int)
            peg_growth_rate = request.args.get('peg_growth_rate', type=float)
            fair_peg = request.args.get('fair_peg', type=float)

            forecast_results = forecaster.run_all_models(years=years)

            # Override with custom parameters if provided
            if earnings_growth or terminal_pe:
                forecast_results["earnings_model"] = forecaster.earnings_growth_model(
                    growth_rate=earnings_growth, growth_decay=growth_decay,
                    terminal_pe=terminal_pe, years=years)
            if revenue_growth or terminal_ps:
                forecast_results["revenue_model"] = forecaster.revenue_growth_model(
                    growth_rate=revenue_growth, growth_decay=rev_growth_decay,
                    terminal_ps=terminal_ps, years=years)
            if fcf_growth != 0.10 or discount_rate != 0.10:
                forecast_results["dcf_model"] = forecaster.dcf_model(
                    fcf_growth=fcf_growth, discount_rate=discount_rate,
                    terminal_growth=terminal_growth, years=years)
            if volatility != 0.30 or simulations != 1000:
                forecast_results["monte_carlo"] = forecaster.monte_carlo_simulation(
                    volatility=volatility, years=years, simulations=simulations)
            if peg_growth_rate or fair_peg:
                forecast_results["peg_valuation"] = forecaster.peg_based_valuation(
                    growth_rate=peg_growth_rate, fair_peg=fair_peg)

            # Recalculate consensus if custom params provided
            forecast_params = ['earnings_growth', 'growth_decay', 'terminal_pe', 'revenue_growth',
                             'rev_growth_decay', 'terminal_ps', 'fcf_growth', 'discount_rate',
                             'terminal_growth', 'volatility', 'simulations', 'peg_growth_rate', 'fair_peg']
            if any(param in request.args for param in forecast_params):
                all_models = [
                    forecast_results["earnings_model"], forecast_results["revenue_model"],
                    forecast_results["dcf_model"], forecast_results["monte_carlo"],
                    forecast_results.get("graham_number"), forecast_results.get("gordon_growth"),
                    forecast_results.get("peg_valuation"), forecast_results.get("ps_sector")
                ]
                forecast_results["consensus"] = forecaster.calculate_consensus([m for m in all_models if m])
        else:
            error = f"Stock {ticker} not found in database. Please fetch it first."

    # Prepare growth rate info
    growth_rate_info = None
    if forecast_results and stock_data:
        growth_rate_info = _build_growth_rate_info(stock_data, growth_metrics,
                                                   StockForecaster(stock_data, growth_metrics))

    return render_template('forecast.html',
                           available_stocks=available_stocks,
                           selected_ticker=ticker,
                           stock=stock_data,
                           results=forecast_results,
                           growth_rate_info=growth_rate_info,
                           error=error)


@forecast_bp.route('/upside')
def upside_calculator():
    """Upside calculator - see potential upside to target market cap"""
    stocks_df = db.get_all_stocks()
    target_market_cap_b = request.args.get('target', 1000, type=float)
    target_market_cap = target_market_cap_b * 1_000_000_000

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

    stocks_with_upside.sort(key=lambda x: x['upside_percent'], reverse=True)

    growth_data = []
    for stock in stocks_with_upside[:20]:
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


def _build_growth_rate_info(stock_data, growth_metrics, forecaster):
    """Build growth rate information for the UI"""
    revenue_sources = []
    earnings_sources = []

    if growth_metrics and growth_metrics.get('revenue_cagr_3y'):
        revenue_sources.append({
            'key': 'cagr_3y', 'label': '3-Year CAGR (Historical)',
            'value': growth_metrics.get('revenue_cagr_3y')
        })
    if growth_metrics and growth_metrics.get('earnings_cagr_3y'):
        earnings_sources.append({
            'key': 'cagr_3y', 'label': '3-Year CAGR (Historical)',
            'value': growth_metrics.get('earnings_cagr_3y')
        })

    if growth_metrics and growth_metrics.get('avg_quarterly_revenue_growth'):
        quarterly_rev = growth_metrics.get('avg_quarterly_revenue_growth')
        annualized_rev = ((1 + quarterly_rev) ** 4 - 1) if quarterly_rev else 0
        revenue_sources.append({
            'key': 'quarterly_avg', 'label': 'Quarterly Average - Annualized (Recent Trend)',
            'value': annualized_rev
        })
    if growth_metrics and growth_metrics.get('avg_quarterly_earnings_growth'):
        quarterly_earn = growth_metrics.get('avg_quarterly_earnings_growth')
        annualized_earn = ((1 + quarterly_earn) ** 4 - 1) if quarterly_earn else 0
        earnings_sources.append({
            'key': 'quarterly_avg', 'label': 'Quarterly Average - Annualized (Recent Trend)',
            'value': annualized_earn
        })

    if stock_data.get('revenue_growth'):
        revenue_sources.append({
            'key': 'yfinance', 'label': 'yfinance (Single-Point)',
            'value': stock_data.get('revenue_growth', 0)
        })
    if stock_data.get('earnings_growth'):
        earnings_sources.append({
            'key': 'yfinance', 'label': 'yfinance (Single-Point)',
            'value': stock_data.get('earnings_growth', 0)
        })

    return {
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
