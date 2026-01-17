"""
Technical analysis routes
"""

from flask import render_template, request, jsonify
from webapp.routes import technical_bp
from webapp.extensions import db, fetcher
from libs.technical_analyzer import TechnicalAnalyzer


@technical_bp.route('/technical-analysis/basic')
def technical_analysis_basic():
    """Basic technical analysis page with support/resistance and trends"""
    ticker = request.args.get('ticker', type=str)
    all_stocks_df = db.get_all_stocks()
    available_stocks = all_stocks_df['ticker'].tolist() if not all_stocks_df.empty else []

    if not ticker:
        return render_template('technical_analysis_basic.html',
                             available_stocks=available_stocks,
                             ticker=None,
                             stock_info=None,
                             technical_data=None,
                             chart_data=None,
                             error=None)

    ticker = ticker.upper()
    stock_info = db.get_stock(ticker)
    if not stock_info:
        return render_template('technical_analysis_basic.html',
                             available_stocks=available_stocks,
                             ticker=ticker,
                             stock_info=None,
                             technical_data=None,
                             chart_data=None,
                             error=f"Stock {ticker} not found in database. Please fetch it first.")

    price_df = db.get_price_history(ticker)

    if price_df.empty:
        print(f"No price history found for {ticker}, fetching from Yahoo Finance...")
        price_data = fetcher.fetch_price_history(ticker)

        if price_data:
            db.save_price_history(ticker, price_data)
            price_df = db.get_price_history(ticker)
        else:
            return render_template('technical_analysis_basic.html',
                                 available_stocks=available_stocks,
                                 ticker=ticker,
                                 stock_info=stock_info,
                                 technical_data=None,
                                 chart_data=None,
                                 error=f"Unable to fetch price history for {ticker}")

    if len(price_df) < 30:
        return render_template('technical_analysis_basic.html',
                             available_stocks=available_stocks,
                             ticker=ticker,
                             stock_info=stock_info,
                             technical_data=None,
                             chart_data=None,
                             error=f"Insufficient price history for {ticker} (need at least 30 days)")

    analyzer = TechnicalAnalyzer(price_df)
    current_price = stock_info.get('current_price', price_df.iloc[-1]['close'])
    technical_data = analyzer.calculate_all_indicators(current_price)
    chart_data = analyzer.get_chart_data(include_indicators=True)

    # Save to cache
    cache_data = {
        'support_levels': technical_data.get('support_levels', []),
        'resistance_levels': technical_data.get('resistance_levels', []),
        'trend_slope': technical_data.get('trend', {}).get('slope'),
        'trend_r_squared': technical_data.get('trend', {}).get('r_squared'),
        'trend_target_30d': technical_data.get('trend', {}).get('target_30d'),
        'trend_target_90d': technical_data.get('trend', {}).get('target_90d')
    }
    db.save_technical_indicators(ticker, cache_data)

    return render_template('technical_analysis_basic.html',
                         available_stocks=available_stocks,
                         ticker=ticker,
                         stock_info=stock_info,
                         technical_data=technical_data,
                         chart_data=chart_data,
                         error=None)


@technical_bp.route('/api/refresh-price-history/<ticker>', methods=['POST'])
def api_refresh_price_history(ticker: str):
    """API endpoint to refresh price history for a stock"""
    try:
        ticker = ticker.upper()
        price_data = fetcher.fetch_price_history(ticker)

        if not price_data:
            return jsonify({'success': False, 'error': 'Failed to fetch price history'}), 400

        db.save_price_history(ticker, price_data)
        return jsonify({'success': True, 'data_points': len(price_data)})

    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500
