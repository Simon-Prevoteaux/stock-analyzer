"""
Portfolio routes - Manage user's portfolio
"""

from flask import render_template, request, redirect, url_for
from webapp.routes import portfolio_bp
from webapp.extensions import db


@portfolio_bp.route('/portfolio')
def portfolio():
    """User's portfolio"""
    portfolio_df = db.get_portfolio()
    return render_template('portfolio.html',
                           stocks=portfolio_df.to_dict('records') if not portfolio_df.empty else [])


@portfolio_bp.route('/portfolio/add/<ticker>', methods=['POST'])
def add_to_portfolio(ticker):
    """Add stock to portfolio"""
    notes = request.form.get('notes', '')
    db.add_to_portfolio(ticker, notes)
    return redirect(url_for('portfolio.portfolio'))


@portfolio_bp.route('/portfolio/remove/<ticker>', methods=['POST'])
def remove_from_portfolio(ticker):
    """Remove stock from portfolio"""
    db.remove_from_portfolio(ticker)
    return redirect(url_for('portfolio.portfolio'))
