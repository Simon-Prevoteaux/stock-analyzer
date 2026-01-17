"""
Watchlist routes - Manage user's watchlist
"""

from flask import render_template, request, redirect, url_for, jsonify
from webapp.routes import watchlist_bp
from webapp.extensions import db


@watchlist_bp.route('/watchlist')
def watchlist():
    """User's watchlist"""
    watchlist_df = db.get_watchlist()
    return render_template('watchlist.html',
                           stocks=watchlist_df.to_dict('records') if not watchlist_df.empty else [])


@watchlist_bp.route('/watchlist/add/<ticker>', methods=['POST'])
def add_to_watchlist(ticker):
    """Add stock to watchlist"""
    notes = request.form.get('notes', '')
    db.add_to_watchlist(ticker, notes)
    return redirect(url_for('watchlist.watchlist'))


@watchlist_bp.route('/watchlist/remove/<ticker>', methods=['POST'])
def remove_from_watchlist(ticker):
    """Remove stock from watchlist"""
    db.remove_from_watchlist(ticker)
    return redirect(url_for('watchlist.watchlist'))


@watchlist_bp.route('/watchlist/update-notes/<ticker>', methods=['POST'])
def update_watchlist_notes(ticker):
    """Update notes for a stock in watchlist"""
    try:
        data = request.get_json()
        notes = data.get('notes', '')
        success = db.update_watchlist_notes(ticker, notes)
        return jsonify({'success': success})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@watchlist_bp.route('/watchlist/update-ranking/<ticker>', methods=['POST'])
def update_watchlist_ranking(ticker):
    """Update ranking for a stock in watchlist"""
    try:
        data = request.get_json()
        ranking = int(data.get('ranking', 0))
        success = db.update_watchlist_ranking(ticker, ranking)
        return jsonify({'success': success})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500
