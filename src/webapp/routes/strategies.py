"""
Strategy routes - Value plays, quality growth, etc.
"""

from flask import render_template, request
from webapp.routes import strategies_bp
from webapp.extensions import db
import pandas as pd


@strategies_bp.route('/value-plays')
def value_plays():
    """Show potential value stocks with enhanced filtering"""
    min_consistency = request.args.get('min_consistency', 60, type=float)
    require_growth = request.args.get('require_growth', 'false').lower() == 'true'
    use_enhanced = request.args.get('use_enhanced', 'false').lower() == 'true'

    if use_enhanced:
        enhanced_stocks_df = db.get_enhanced_value_stocks(
            min_consistency=min_consistency,
            min_growth=0.05 if require_growth else 0
        )
    else:
        enhanced_stocks_df = pd.DataFrame()

    value_stocks_df = db.get_value_stocks()
    near_value_stocks_df = db.get_near_value_stocks()

    return render_template('value_plays.html',
                         stocks=value_stocks_df.to_dict('records') if not value_stocks_df.empty else [],
                         near_value_stocks=near_value_stocks_df.to_dict('records') if not near_value_stocks_df.empty else [],
                         enhanced_stocks=enhanced_stocks_df.to_dict('records') if not enhanced_stocks_df.empty else [],
                         min_consistency=min_consistency,
                         require_growth=require_growth,
                         use_enhanced=use_enhanced)


@strategies_bp.route('/bubble-territory')
def bubble_territory():
    """Show stocks in bubble territory"""
    bubble_stocks_df = db.get_high_risk_stocks(min_bubble_score=6)
    return render_template('bubble_territory.html',
                           stocks=bubble_stocks_df.to_dict('records') if not bubble_stocks_df.empty else [])


@strategies_bp.route('/quality-growth')
def quality_growth():
    """Quality growth stocks with sustainable, consistent patterns"""
    min_cagr = request.args.get('min_cagr', 20, type=float)
    min_consistency = request.args.get('min_consistency', 70, type=int)
    max_peg = request.args.get('max_peg', 2.5, type=float)

    stocks_df = db.get_quality_growth_stocks(min_cagr, min_consistency, max_peg)
    all_stocks_df = db.get_all_stocks()
    available_stocks = all_stocks_df['ticker'].tolist() if not all_stocks_df.empty else []

    return render_template('quality_growth.html',
                         stocks=stocks_df.to_dict('records') if not stocks_df.empty else [],
                         available_stocks=available_stocks,
                         min_cagr=min_cagr,
                         min_consistency=min_consistency,
                         max_peg=max_peg)


@strategies_bp.route('/growth-inflection')
def growth_inflection():
    """Stocks showing growth acceleration (inflection points)"""
    min_consistency = request.args.get('min_consistency', 60, type=int)
    max_pe = request.args.get('max_pe', 40, type=float)

    stocks_df = db.get_growth_inflection_stocks(min_consistency, max_pe)
    all_stocks_df = db.get_all_stocks()
    available_stocks = all_stocks_df['ticker'].tolist() if not all_stocks_df.empty else []

    return render_template('growth_inflection.html',
                         stocks=stocks_df.to_dict('records') if not stocks_df.empty else [],
                         available_stocks=available_stocks,
                         min_consistency=min_consistency,
                         max_pe=max_pe)


@strategies_bp.route('/rule-of-40')
def rule_of_40():
    """Stocks with efficient growth (Rule of 40 metric)"""
    min_rule_of_40 = request.args.get('min_rule_of_40', 40, type=float)
    sector_filter = request.args.get('sector', type=str)

    stocks_df = db.get_rule_of_40_stocks(min_rule_of_40, sector_filter)
    sectors = db.get_sectors()
    all_stocks_df = db.get_all_stocks()
    available_stocks = all_stocks_df['ticker'].tolist() if not all_stocks_df.empty else []

    return render_template('rule_of_40.html',
                         stocks=stocks_df.to_dict('records') if not stocks_df.empty else [],
                         available_stocks=available_stocks,
                         min_rule_of_40=min_rule_of_40,
                         sector_filter=sector_filter,
                         sectors=sectors)


@strategies_bp.route('/margin-expansion')
def margin_expansion():
    """Stocks with expanding margins and improving profitability"""
    min_revenue_growth = request.args.get('min_revenue_growth', 15, type=float)
    min_operating_leverage = request.args.get('min_operating_leverage', 1.0, type=float)

    stocks_df = db.get_margin_expansion_stocks(min_revenue_growth, min_operating_leverage)
    all_stocks_df = db.get_all_stocks()
    available_stocks = all_stocks_df['ticker'].tolist() if not all_stocks_df.empty else []

    return render_template('margin_expansion.html',
                         stocks=stocks_df.to_dict('records') if not stocks_df.empty else [],
                         available_stocks=available_stocks,
                         min_revenue_growth=min_revenue_growth,
                         min_operating_leverage=min_operating_leverage)


@strategies_bp.route('/cash-generative-growth')
def cash_generative_growth():
    """High-growth stocks that also generate positive free cash flow"""
    min_revenue_growth = request.args.get('min_revenue_growth', 20, type=float)
    min_fcf_margin = request.args.get('min_fcf_margin', 10, type=float)

    stocks_df = db.get_cash_generative_growth_stocks(min_revenue_growth, min_fcf_margin)
    all_stocks_df = db.get_all_stocks()
    available_stocks = all_stocks_df['ticker'].tolist() if not all_stocks_df.empty else []

    return render_template('cash_generative_growth.html',
                         stocks=stocks_df.to_dict('records') if not stocks_df.empty else [],
                         available_stocks=available_stocks,
                         min_revenue_growth=min_revenue_growth,
                         min_fcf_margin=min_fcf_margin)
