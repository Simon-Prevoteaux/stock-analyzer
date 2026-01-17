"""
Flask Web Application for Stock Analysis

This is the main application entry point. Route definitions are now modularized
in the routes/ package for better maintainability.
"""

from flask import Flask
import os
import sys
from dotenv import load_dotenv

# Load environment variables
load_dotenv(os.path.join(os.path.dirname(__file__), '..', '..', '.env'))

# Add parent directory to path to import libs
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Add webapp directory to path for absolute imports
webapp_dir = os.path.dirname(__file__)
if webapp_dir not in sys.path:
    sys.path.insert(0, os.path.dirname(webapp_dir))

# Initialize Flask app
app = Flask(__name__)
app.config['SECRET_KEY'] = 'stock-analyzer-secret-key'

# Import extensions and initialize (use absolute imports)
from webapp.extensions import init_fred_api_key, db
init_fred_api_key()

# Register template filters
from webapp.filters import register_filters
register_filters(app)

# Register blueprints
from webapp.routes import register_blueprints
register_blueprints(app)


# Context processor to make available_stocks available to all templates
@app.context_processor
def inject_available_stocks():
    """Inject available stocks list into all templates for global search"""
    stocks_df = db.get_all_stocks()
    available_stocks = stocks_df['ticker'].tolist() if not stocks_df.empty else []
    return dict(available_stocks=available_stocks)


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(debug=True, host='0.0.0.0', port=port)
