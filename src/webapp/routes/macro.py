"""
Macro signals routes - Macroeconomic analysis dashboard
"""

from flask import render_template, jsonify
from webapp.routes import macro_bp
from webapp.extensions import db, get_fred_api_key
from libs.macro_fetcher import MacroDataFetcher
from libs.macro_analyzer import MacroAnalyzer


@macro_bp.route('/macro-signals')
def macro_signals():
    """Main macro signals dashboard"""
    try:
        fred_api_key = get_fred_api_key()
        if not fred_api_key:
            return render_template('error.html',
                                 error="FRED API key not configured. Please add FRED_API_KEY to your .env file.")

        macro_fetcher = MacroDataFetcher(fred_api_key, db=db, cache_hours=24)
        analyzer = MacroAnalyzer()

        # Fetch data
        currencies = macro_fetcher.calculate_currency_returns(base='USD')
        currencies_vs_gold = macro_fetcher.calculate_currencies_vs_gold()
        sp500 = macro_fetcher.calculate_sp500_returns()

        # Gold returns
        gold_usd_df = macro_fetcher.fetch_gold_price(lookback_days=1825)
        gold_returns = {}
        for period, days in [('1d', 1), ('1w', 7), ('1m', 30), ('3m', 90), ('1y', 365), ('3y', 1095), ('5y', 1825)]:
            gold_returns[period] = macro_fetcher._calculate_period_return(gold_usd_df, days)

        # Yield curve and spreads
        yield_curve = macro_fetcher.fetch_yield_curve()
        spreads_raw = macro_fetcher.calculate_yield_spreads()
        spreads = analyzer.interpret_yield_curve(spreads_raw)
        spread_history = macro_fetcher.get_spread_history(lookback_days=365)
        recession_indicators = analyzer.get_recession_indicator_summary(spreads)

        # Credit spreads
        credit_spreads_raw = macro_fetcher.fetch_credit_spreads()
        credit_spreads = {}
        for spread_type, data in credit_spreads_raw.items():
            if data['current'] is not None:
                credit_spreads[spread_type] = {
                    'current': data['current'],
                    'percentile': data['percentile'],
                    'interpretation': analyzer.interpret_credit_spread(
                        spread_type, data['current'], data['percentile']
                    )
                }

        insights = analyzer.format_currency_comparison_insight(currencies, {'gold_returns': gold_returns})
        last_update = macro_fetcher.get_last_update_date(macro_fetcher.TREASURY_SERIES['10Y'])

        return render_template(
            'macro_signals.html',
            currencies=currencies,
            currencies_vs_gold=currencies_vs_gold,
            sp500=sp500,
            gold_returns=gold_returns,
            yield_curve=yield_curve,
            spreads=spreads,
            spread_history=spread_history,
            credit_spreads=credit_spreads,
            recession_indicators=recession_indicators,
            insights=insights,
            last_update=last_update
        )

    except Exception as e:
        print(f"Error in macro_signals route: {str(e)}")
        import traceback
        traceback.print_exc()
        return f"<h1>Error loading macro signals</h1><pre>{str(e)}\n\n{traceback.format_exc()}</pre>", 500


@macro_bp.route('/api/macro/refresh-currencies', methods=['POST'])
def refresh_currencies():
    """Refresh currency data via AJAX"""
    try:
        fred_api_key = get_fred_api_key()
        if not fred_api_key:
            return jsonify({'success': False, 'error': 'FRED API key not configured'}), 400

        macro_fetcher = MacroDataFetcher(fred_api_key)
        currencies = macro_fetcher.calculate_currency_returns(base='USD')
        gold = macro_fetcher.calculate_gold_returns()

        return jsonify({'success': True, 'currencies': currencies, 'gold': gold})

    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@macro_bp.route('/api/macro/refresh-rates', methods=['POST'])
def refresh_rates():
    """Refresh yield curve and credit spreads via AJAX"""
    try:
        fred_api_key = get_fred_api_key()
        if not fred_api_key:
            return jsonify({'success': False, 'error': 'FRED API key not configured'}), 400

        macro_fetcher = MacroDataFetcher(fred_api_key)
        analyzer = MacroAnalyzer()

        yield_curve = macro_fetcher.fetch_yield_curve()
        spreads_raw = macro_fetcher.calculate_yield_spreads()
        spreads = analyzer.interpret_yield_curve(spreads_raw)

        credit_spreads_raw = macro_fetcher.fetch_credit_spreads()
        credit_spreads = {}
        for spread_type, data in credit_spreads_raw.items():
            if data['current'] is not None:
                credit_spreads[spread_type] = {
                    'current': data['current'],
                    'percentile': data['percentile'],
                    'interpretation': analyzer.interpret_credit_spread(
                        spread_type, data['current'], data['percentile']
                    )
                }

        return jsonify({
            'success': True,
            'yield_curve': yield_curve,
            'spreads': spreads,
            'credit_spreads': credit_spreads
        })

    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500
