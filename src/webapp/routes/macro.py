"""
Macro signals routes - Macroeconomic analysis pages
Split into: Currencies, Rates & Spreads, Global Economy, Real Estate
"""

from flask import render_template, jsonify, redirect, url_for
from webapp.routes import macro_bp
from webapp.extensions import db, get_fred_api_key
from libs.macro_fetcher import MacroDataFetcher
from libs.macro_analyzer import MacroAnalyzer


def _get_fetcher_and_analyzer():
    """Helper to create fetcher and analyzer instances"""
    fred_api_key = get_fred_api_key()
    if not fred_api_key:
        return None, None, "FRED API key not configured. Please add FRED_API_KEY to your .env file."

    macro_fetcher = MacroDataFetcher(fred_api_key, db=db, cache_hours=24)
    analyzer = MacroAnalyzer()
    return macro_fetcher, analyzer, None


# =============================================================================
# REDIRECT OLD ROUTE
# =============================================================================

@macro_bp.route('/macro-signals')
def macro_signals():
    """Redirect old dashboard URL to currencies page"""
    return redirect(url_for('macro.macro_currencies'))


# =============================================================================
# CURRENCIES & GOLD PAGE
# =============================================================================

@macro_bp.route('/macro/currencies')
def macro_currencies():
    """Currency performance and gold comparisons"""
    try:
        macro_fetcher, analyzer, error = _get_fetcher_and_analyzer()
        if error:
            return render_template('error.html', error=error)

        # Fetch currency and gold data
        currencies = macro_fetcher.calculate_currency_returns(base='USD')
        currencies_vs_gold = macro_fetcher.calculate_currencies_vs_gold()
        sp500 = macro_fetcher.calculate_sp500_returns()

        # Gold returns
        gold_usd_df = macro_fetcher.fetch_gold_price(lookback_days=1825)
        gold_returns = {}
        for period, days in [('1d', 1), ('1w', 7), ('1m', 30), ('3m', 90), ('1y', 365), ('3y', 1095), ('5y', 1825)]:
            gold_returns[period] = macro_fetcher._calculate_period_return(gold_usd_df, days)

        # Generate insights
        insights = analyzer.format_currency_comparison_insight(currencies, {'1y': gold_returns.get('1y')})
        last_update = macro_fetcher.get_last_update_date(macro_fetcher.CURRENCY_SERIES['EUR'])

        return render_template(
            'macro_currencies.html',
            currencies=currencies,
            currencies_vs_gold=currencies_vs_gold,
            sp500=sp500,
            gold_returns=gold_returns,
            insights=insights,
            last_update=last_update
        )

    except Exception as e:
        print(f"Error in macro_currencies route: {str(e)}")
        import traceback
        traceback.print_exc()
        return f"<h1>Error loading currencies page</h1><pre>{str(e)}\n\n{traceback.format_exc()}</pre>", 500


# =============================================================================
# RATES & SPREADS PAGE
# =============================================================================

@macro_bp.route('/macro/rates')
def macro_rates():
    """Yield curve, spreads, and credit spreads"""
    try:
        macro_fetcher, analyzer, error = _get_fetcher_and_analyzer()
        if error:
            return render_template('error.html', error=error)

        # Fetch yield curve and spreads
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

        last_update = macro_fetcher.get_last_update_date(macro_fetcher.TREASURY_SERIES['10Y'])

        return render_template(
            'macro_rates.html',
            yield_curve=yield_curve,
            spreads=spreads,
            spread_history=spread_history,
            credit_spreads=credit_spreads,
            recession_indicators=recession_indicators,
            last_update=last_update
        )

    except Exception as e:
        print(f"Error in macro_rates route: {str(e)}")
        import traceback
        traceback.print_exc()
        return f"<h1>Error loading rates page</h1><pre>{str(e)}\n\n{traceback.format_exc()}</pre>", 500


# =============================================================================
# GLOBAL ECONOMY PAGE (NEW)
# =============================================================================

@macro_bp.route('/macro/global-economy')
def macro_global_economy():
    """Global economic indicators - Buffett Indicator, M2/GDP, Debt/GDP, M2 Velocity"""
    try:
        macro_fetcher, analyzer, error = _get_fetcher_and_analyzer()
        if error:
            return render_template('error.html', error=error)

        # Fetch global economy data
        buffett_data = macro_fetcher.fetch_buffett_indicator(lookback_years=25)
        m2_gdp_data = macro_fetcher.fetch_m2_gdp_ratio(lookback_years=25)
        debt_gdp_data = macro_fetcher.fetch_debt_to_gdp(lookback_years=25)
        velocity_data = macro_fetcher.fetch_m2_velocity(lookback_years=25)

        # Add interpretations
        buffett_interp = analyzer.interpret_buffett_indicator(
            buffett_data.get('current'),
            buffett_data.get('percentile')
        )
        m2_gdp_interp = analyzer.interpret_m2_gdp(
            m2_gdp_data.get('current'),
            m2_gdp_data.get('yoy_change')
        )
        debt_gdp_interp = analyzer.interpret_debt_gdp(
            debt_gdp_data.get('current'),
            debt_gdp_data.get('historical_comparison')
        )
        velocity_interp = analyzer.interpret_m2_velocity(
            velocity_data.get('current'),
            velocity_data.get('historical_avg')
        )

        # Generate summary
        summary = analyzer.get_global_economy_summary(
            buffett_data, m2_gdp_data, debt_gdp_data, velocity_data
        )

        # Get last update date (use GDP since Buffett indicator is now calculated from Yahoo Finance)
        last_update = macro_fetcher.get_last_update_date(
            macro_fetcher.GLOBAL_ECONOMY_SERIES['gdp']
        )

        return render_template(
            'macro_global_economy.html',
            buffett=buffett_data,
            buffett_interp=buffett_interp,
            m2_gdp=m2_gdp_data,
            m2_gdp_interp=m2_gdp_interp,
            debt_gdp=debt_gdp_data,
            debt_gdp_interp=debt_gdp_interp,
            velocity=velocity_data,
            velocity_interp=velocity_interp,
            summary=summary,
            last_update=last_update
        )

    except Exception as e:
        print(f"Error in macro_global_economy route: {str(e)}")
        import traceback
        traceback.print_exc()
        return f"<h1>Error loading global economy page</h1><pre>{str(e)}\n\n{traceback.format_exc()}</pre>", 500


# =============================================================================
# REAL ESTATE PAGE (NEW)
# =============================================================================

@macro_bp.route('/macro/real-estate')
def macro_real_estate():
    """Real estate market analysis - Case-Shiller, Housing Supply, Affordability"""
    try:
        macro_fetcher, analyzer, error = _get_fetcher_and_analyzer()
        if error:
            return render_template('error.html', error=error)

        # Fetch real estate data
        case_shiller = macro_fetcher.fetch_case_shiller(lookback_years=25)
        housing_supply = macro_fetcher.fetch_housing_supply()
        housing_activity = macro_fetcher.fetch_housing_activity(lookback_years=15)
        mortgage_rates = macro_fetcher.fetch_mortgage_rates(lookback_years=15)
        affordability = macro_fetcher.fetch_housing_affordability(lookback_years=15)
        median_price = macro_fetcher.fetch_median_home_price(lookback_years=25)
        mortgage_debt_service = macro_fetcher.fetch_mortgage_debt_service(lookback_years=20)
        price_to_income = macro_fetcher.fetch_price_to_income_ratio(lookback_years=25)

        # Add interpretations
        supply_interp = analyzer.interpret_housing_supply(
            housing_supply.get('existing_months_supply')
        )
        affordability_interp = analyzer.interpret_housing_affordability(
            affordability.get('current'),
            affordability.get('historical_avg')
        )
        case_shiller_interp = analyzer.interpret_case_shiller(
            case_shiller.get('national', {}).get('yoy_change'),
            case_shiller.get('national', {}).get('current')
        )
        mortgage_interp = analyzer.interpret_mortgage_rate(
            mortgage_rates.get('current'),
            mortgage_rates.get('historical_avg')
        )

        # Generate summary
        summary = analyzer.get_real_estate_summary(
            case_shiller, housing_supply, affordability, mortgage_rates
        )

        # Get last update date
        last_update = macro_fetcher.get_last_update_date(
            macro_fetcher.REAL_ESTATE_SERIES['case_shiller_national']
        )

        return render_template(
            'macro_real_estate.html',
            case_shiller=case_shiller,
            case_shiller_interp=case_shiller_interp,
            housing_supply=housing_supply,
            supply_interp=supply_interp,
            housing_activity=housing_activity,
            mortgage_rates=mortgage_rates,
            mortgage_interp=mortgage_interp,
            affordability=affordability,
            affordability_interp=affordability_interp,
            median_price=median_price,
            mortgage_debt_service=mortgage_debt_service,
            price_to_income=price_to_income,
            summary=summary,
            last_update=last_update
        )

    except Exception as e:
        print(f"Error in macro_real_estate route: {str(e)}")
        import traceback
        traceback.print_exc()
        return f"<h1>Error loading real estate page</h1><pre>{str(e)}\n\n{traceback.format_exc()}</pre>", 500


# =============================================================================
# API ENDPOINTS
# =============================================================================

@macro_bp.route('/api/macro/refresh-currencies', methods=['POST'])
def refresh_currencies():
    """Refresh currency data via AJAX"""
    try:
        macro_fetcher, analyzer, error = _get_fetcher_and_analyzer()
        if error:
            return jsonify({'success': False, 'error': error}), 400

        currencies = macro_fetcher.calculate_currency_returns(base='USD')
        gold = macro_fetcher.calculate_gold_returns()

        return jsonify({'success': True, 'currencies': currencies, 'gold': gold})

    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@macro_bp.route('/api/macro/refresh-rates', methods=['POST'])
def refresh_rates():
    """Refresh yield curve and credit spreads via AJAX"""
    try:
        macro_fetcher, analyzer, error = _get_fetcher_and_analyzer()
        if error:
            return jsonify({'success': False, 'error': error}), 400

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
