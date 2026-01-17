# Stock Analyzer - Development Documentation

## Project Overview

Stock Analyzer is a personal stock analysis and valuation tool built to help analyze stocks, compare valuations, identify value plays, and detect potentially overvalued securities.

## Architecture

The application is built with a clean separation of concerns:

### 1. Data Layer (`src/libs/`)

#### `stock_fetcher.py`
- **Purpose**: Fetch and process stock data from Yahoo Finance
- **Key Functions**:
  - `fetch_stock_data(ticker)`: Fetch comprehensive metrics for a single stock
  - `fetch_multiple_stocks(tickers)`: Batch fetch multiple stocks
  - `compare_stocks(df)`: Add comparison metrics to dataset
  - `_calculate_bubble_score(data)`: Calculate 0-10 overvaluation score
  - `_classify_stock(row)`: Classify as Value/Growth/Overvalued

- **Bubble Score Algorithm**:
  - P/E > 200: +3 points
  - P/E > 100: +2 points
  - P/E > 50: +1 point
  - P/S > 50: +3 points
  - P/S > 20: +2 points
  - P/S > 10: +1 point
  - Unprofitable: +2 points
  - High P/S with low growth: +2 points
  - Max score: 10

- **Dependencies**: yfinance, pandas

#### `database.py`
- **Purpose**: SQLite database management for persistent storage
- **Tables**:
  - `stocks`: Main stock data
  - `watchlist`: User's tracked stocks
  - `historical_snapshots`: Historical data points

- **Key Functions**:
  - `save_stock(stock_data)`: Save/update single stock
  - `save_multiple_stocks(df)`: Batch save
  - `get_all_stocks()`: Retrieve all stocks
  - `get_high_risk_stocks(min_bubble_score)`: Filter by bubble score
  - `get_value_stocks(max_pe, max_ps)`: Find value plays
  - `add_to_watchlist(ticker)`: Watchlist management
  - `search_stocks(keyword)`: Search by ticker or name

### 2. Presentation Layer (`src/webapp/`)

#### `app.py`
Flask web application with the following routes:

**Main Pages**:
- `/` - Home page with stats overview
- `/fetch` - Fetch new stock data
- `/screener` - Stock screener with filters
- `/comparison` - Side-by-side stock comparison
- `/stock/<ticker>` - Detailed stock view
- `/watchlist` - User's watchlist
- `/value-plays` - Potential value stocks
- `/bubble-territory` - Overvalued stocks

**API Endpoints**:
- `/api/search` - Search stocks by keyword
- `/api/refresh/<ticker>` - Refresh stock data

**Template Filters**:
- `format_number`: Display large numbers (K, M, B, T)
- `format_percent`: Format as percentage
- `format_ratio`: Format ratio values

#### Templates Structure
- `base.html`: Base template with navigation
- `index.html`: Home page
- `fetch.html`: Data fetching form
- `screener.html`: Stock screener with filters
- `stock_detail.html`: Individual stock view
- `comparison.html`: Multi-stock comparison
- `watchlist.html`: Personal watchlist
- `value_plays.html`: Value opportunities
- `bubble_territory.html`: Overvalued stocks

#### Styling (`static/css/style.css`)
- Responsive design
- Color-coded risk levels
- Table-based data presentation
- Clean, professional interface

### 3. Data Storage

**SQLite Database** (`data/stocks.db`):
- Local file-based storage
- No external database server required
- Portable and lightweight

## Key Metrics

### Valuation Metrics
- **Market Cap**: Total market value
- **P/E Ratio**: Price-to-Earnings (valuation vs earnings)
- **Forward P/E**: Forward-looking P/E
- **P/S Ratio**: Price-to-Sales (valuation vs revenue)
- **EPS**: Earnings Per Share

### Financial Metrics
- **Revenue**: Total revenue
- **Earnings**: Net income
- **Profit Margin**: Profitability percentage
- **Operating Margin**: Operating efficiency

### Growth Metrics
- **Revenue Growth**: YoY revenue growth rate
- **Earnings Growth**: YoY earnings growth rate

### Custom Metrics
- **Bubble Score**: 0-10 scale of overvaluation risk
- **Risk Level**: LOW, MEDIUM, HIGH, VERY HIGH, EXTREME
- **Classification**: Value Play, Growth, Fairly Valued, Overvalued

## Usage Examples

### Fetch Stock Data
```python
from libs.stock_fetcher import StockFetcher

fetcher = StockFetcher()
stock_data = fetcher.fetch_stock_data('AAPL')
```

### Save to Database
```python
from libs.database import StockDatabase

db = StockDatabase('data/stocks.db')
db.save_stock(stock_data)
```

### Screen for Value Stocks
```python
value_stocks = db.get_value_stocks(max_pe=20, max_ps=3)
```

### Find High Risk Stocks
```python
bubble_stocks = db.get_high_risk_stocks(min_bubble_score=6)
```

## Technology Stack

- **Backend**: Python 3.8+, Flask 3.0+
- **Data Source**: Yahoo Finance (via yfinance)
- **Database**: SQLite 3
- **Data Processing**: Pandas
- **Frontend**: HTML5, CSS3, Jinja2 templates

## Development Workflow

1. **Fetch Data**: Use `/fetch` route to get stock data
2. **Store**: Data automatically saved to SQLite
3. **Analyze**: Use screener to filter and compare
4. **Monitor**: Add interesting stocks to watchlist
5. **Refresh**: Periodically update stock data

## Design Decisions

### Why SQLite?
- No external database server needed
- Portable single-file database
- Perfect for personal use
- Sufficient performance for this use case

### Why Flask?
- Lightweight and simple
- Perfect for small personal projects
- Easy to extend
- Built-in development server

### Why yfinance?
- Free access to Yahoo Finance data
- No API key required
- Comprehensive stock metrics
- Reliable and well-maintained

## Future Enhancements

### ✅ COMPLETED - Phase 3: New Forecasting Models

**Status**: Implemented on 2025-12-31

#### Models Implemented:

1. **Graham Number** (Intrinsic Value) ✅
   - Benjamin Graham's formula: √(22.5 × EPS × Book Value Per Share)
   - Calculates margin of safety
   - Shows if stock is undervalued vs intrinsic value
   - Location: `forecaster.py:475-522`

2. **Gordon Growth Model** (Dividend Discount) ✅
   - Fair value based on dividends
   - Formula: D1 / (r - g) where D1 = next year dividend
   - Suitable for dividend-paying stocks
   - Location: `forecaster.py:524-580`

3. **PEG-based Valuation** ✅
   - Fair P/E = Fair PEG × Growth Rate
   - Fair Price = Fair P/E × EPS
   - Assumes fair PEG ratio (typically 1.5)
   - Location: `forecaster.py:582-633`

4. **Price-to-Sales Fair Value** ✅
   - Uses sector median P/S ratio
   - Fair Price = Sector P/S × Revenue Per Share
   - Good for comparing against sector peers
   - Location: `forecaster.py:635-679`

#### Multi-Model Consensus Feature ✅:
- Consensus target price (weighted average of all models)
- Agreement score (how aligned are the models 0-100)
- Range of outcomes visualization
- Buy/Hold/Sell recommendation based on consensus upside
- Location: `forecaster.py:681-764`

#### Implementation Details:
- **Backend**: `/src/libs/forecaster.py` - All 4 models + consensus calculation
- **Data Fetching**: `/src/libs/stock_fetcher.py` - Added book_value, dividend_rate, dividend_yield fields
- **Route**: `/src/webapp/app.py` - Updated forecast route to use `run_all_models()`
- **Template Guide**: `/FORECAST_TEMPLATE_ADDITIONS.md` - Complete HTML/CSS snippets for UI

**Note**: The forecast.html template updates are documented in FORECAST_TEMPLATE_ADDITIONS.md
due to the template's size (2212 lines). All backend functionality is complete and working.

### Other Potential Features:
- Historical price charts
- Email alerts for price changes
- Export to Excel/CSV
- Technical indicators
- Automated daily data refresh

## File Organization

```
stock-analyzer/
├── src/
│   ├── libs/
│   │   ├── __init__.py
│   │   ├── stock_fetcher.py    # Stock data fetching logic
│   │   ├── macro_fetcher.py    # Macro data fetching (FRED API, Yahoo Finance) with caching
│   │   ├── macro_analyzer.py   # Macro data interpretation and analysis
│   │   ├── forecaster.py       # Stock valuation models
│   │   └── database.py         # Database management (stocks + macro data)
│   └── webapp/
│       ├── __init__.py
│       ├── app.py              # Flask application entry point
│       ├── extensions.py       # Shared instances (db, fetcher)
│       ├── filters.py          # Template filters (format_number, etc.)
│       ├── utils.py            # Shared utility functions
│       ├── routes/             # Modular route blueprints
│       │   ├── __init__.py     # Blueprint registration
│       │   ├── core.py         # Home, fetch, screener, stock detail
│       │   ├── watchlist_routes.py  # Watchlist management
│       │   ├── portfolio_routes.py  # Portfolio management
│       │   ├── strategies.py   # Value plays, quality growth, etc.
│       │   ├── forecast_routes.py   # Forecasting & upside calculator
│       │   ├── technical.py    # Technical analysis
│       │   ├── macro.py        # Macro signals dashboard
│       │   └── api_routes.py   # JSON API endpoints
│       ├── templates/          # HTML templates
│       │   ├── base.html
│       │   ├── components/     # Reusable template components
│       │   │   ├── macros.html # Jinja2 macros for styling
│       │   │   └── financial_chart.html
│       │   └── ... (19 templates)
│       └── static/
│           ├── css/
│           │   ├── style.css   # Main styles + utility classes
│           │   ├── modal.css   # Modal dialog styles
│           │   └── search.css  # Search component styles
│           └── js/
│               └── ... (6 JS files)
├── data/
│   └── stocks.db              # SQLite database (stocks + macro cache)
├── tests/                     # Test files (future)
├── venv/                      # Virtual environment
├── requirements.txt           # Python dependencies
├── .env                       # Environment variables (FRED_API_KEY)
├── .env.example              # Environment template
├── .gitignore
├── README.md
└── CLAUDE.md                  # This file
```

## Running the Application

1. **Activate virtual environment**:
   ```bash
   source venv/bin/activate
   ```

2. **Navigate to webapp**:
   ```bash
   cd src/webapp
   ```

3. **Run Flask app**:
   ```bash
   python app.py
   ```

4. **Access in browser**:
   ```
   http://localhost:5000
   ```

## Troubleshooting

### Database locked error
- Close any other connections to the database
- Ensure only one Flask instance is running

### Stock data not fetching
- Check internet connection
- Verify ticker symbol is correct
- Yahoo Finance may rate-limit requests

### Virtual environment issues
- Ensure virtual environment is activated
- Reinstall requirements: `pip install -r requirements.txt`

## Contributing

This is a personal project. For modifications:
1. Create a feature branch
2. Test changes locally
3. Commit with descriptive messages
4. Document changes in this file

## Version History

- **v1.3.0** (2026-01-17): Codebase Modularization & Refactoring
  - **Flask Blueprint Architecture**
    - Split monolithic app.py (1,578 lines) into 8 modular blueprints
    - Blueprints: core, watchlist, portfolio, strategies, forecast, technical, macro, api
    - Each blueprint handles related routes with clear separation of concerns
    - New modules: extensions.py (shared instances), filters.py (template filters), utils.py (shared functions)
  - **CSS Utility Classes**
    - Added 40+ utility classes to style.css for consistent styling
    - Classes for: display, flex, text, colors, spacing, backgrounds
    - Semantic color classes: text-positive, text-warning, text-negative
    - Replaced inline styles with CSS classes across templates
  - **Template Components**
    - Created reusable Jinja2 macros in components/macros.html
    - Macros for: peg_color, threshold_color, percentage_color, info_card
    - Updated all templates to use blueprint-prefixed url_for calls
  - **Code Organization Benefits**
    - Easier navigation and modification of code
    - Clear module boundaries for future development
    - Reduced file sizes for better maintainability
    - Consistent styling patterns across templates

- **v1.2.0** (2026-01-17): Macro Signals Feature with Performance Optimization
  - **NEW: Macro Signals Dashboard** - Comprehensive macroeconomic analysis inspired by Ray Dalio's methodology
    - Currency performance tracking (USD, EUR, JPY, CNY, CHF) across 7 timeframes (1d to 5y)
    - Gold vs currencies analysis with dual calculation methods (inverted returns & geometric purchasing power)
    - S&P 500 performance tracking and gold-adjusted returns
    - US Treasury yield curve visualization with color-coded inversion detection
    - Yield spread analysis (10Y-2Y, 10Y-3M, 30Y-5Y) with historical trends and EXPANDING/CONTRACTING/STABLE indicators
    - Historical spread chart showing 1-year trend evolution
    - Corporate credit spread monitoring (Investment Grade, High Yield, BBB)
    - Interactive tooltips explaining all metrics and interpretations
    - Integration with FRED API for official economic data
  - **Performance Optimization: Database Caching**
    - 24-hour cache for all macro data (FRED API, Yahoo Finance)
    - First load fetches from APIs, subsequent loads use database cache
    - Significantly faster page loads (seconds vs minutes)
    - Automatic cache expiration and refresh mechanism
    - Cache applies to: currencies, gold, S&P 500, Treasury yields, credit spreads
  - **UI Enhancements**
    - Added logo icon to navigation bar
    - Compressed navigation spacing for better dropdown display
    - Dark theme styling consistent across all macro sections
    - Responsive tables with color-coded positive/negative values
  - **Backend Improvements**
    - New `MacroDataFetcher` class with caching support ([macro_fetcher.py](src/libs/macro_fetcher.py))
    - New `MacroAnalyzer` class for data interpretation ([macro_analyzer.py](src/libs/macro_analyzer.py))
    - Enhanced database schema with `macro_data` table
    - FRED API integration for official economic data
    - Yahoo Finance integration for gold and S&P 500 data

- **v1.0.0** (2025-12-04): Initial release
  - Stock data fetching
  - SQLite database
  - Flask web interface
  - Stock screener
  - Comparison tool
  - Watchlist feature
  - Value play detection
  - Bubble territory detection
