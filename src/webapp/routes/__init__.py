"""
Routes package - Flask Blueprints for Stock Analyzer

This package contains modularized route definitions:
- core: Home, fetch, screener, stock detail
- watchlist_routes: Watchlist management
- portfolio_routes: Portfolio management
- strategies: Value plays, quality growth, etc.
- forecast_routes: Price forecasting and upside calculator
- technical: Technical analysis
- macro: Macro signals dashboard
- api_routes: API endpoints
"""

from flask import Blueprint

# Create blueprints
core_bp = Blueprint('core', __name__)
watchlist_bp = Blueprint('watchlist', __name__)
portfolio_bp = Blueprint('portfolio', __name__)
strategies_bp = Blueprint('strategies', __name__)
forecast_bp = Blueprint('forecast', __name__)
technical_bp = Blueprint('technical', __name__)
macro_bp = Blueprint('macro', __name__)
api_bp = Blueprint('api', __name__)


def register_blueprints(app):
    """Register all blueprints with the Flask app"""
    # Import route modules to register their routes
    # These imports must happen after blueprints are created
    from . import core
    from . import watchlist_routes
    from . import portfolio_routes
    from . import strategies
    from . import forecast_routes
    from . import technical
    from . import macro
    from . import api_routes

    # Register blueprints with the app
    app.register_blueprint(core_bp)
    app.register_blueprint(watchlist_bp)
    app.register_blueprint(portfolio_bp)
    app.register_blueprint(strategies_bp)
    app.register_blueprint(forecast_bp)
    app.register_blueprint(technical_bp)
    app.register_blueprint(macro_bp)
    app.register_blueprint(api_bp)
