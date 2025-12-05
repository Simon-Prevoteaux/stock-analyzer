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

Potential features for future development:
- Historical price charts
- Portfolio tracking
- Email alerts for price changes
- Export to Excel/CSV
- Technical indicators
- Peer comparison within sectors
- Automated daily data refresh
- Advanced screening criteria
- Custom bubble score weights

## File Organization

```
stock-analyzer/
├── src/
│   ├── libs/
│   │   ├── __init__.py
│   │   ├── stock_fetcher.py    # Data fetching logic
│   │   └── database.py         # Database management
│   └── webapp/
│       ├── __init__.py
│       ├── app.py              # Flask application
│       ├── templates/          # HTML templates
│       │   ├── base.html
│       │   ├── index.html
│       │   ├── fetch.html
│       │   ├── screener.html
│       │   ├── stock_detail.html
│       │   ├── comparison.html
│       │   ├── watchlist.html
│       │   ├── value_plays.html
│       │   └── bubble_territory.html
│       └── static/
│           └── css/
│               └── style.css   # Styling
├── data/
│   └── stocks.db              # SQLite database
├── tests/                     # Test files (future)
├── venv/                      # Virtual environment
├── requirements.txt           # Python dependencies
├── .gitignore
├── README.md
└── claude.md                  # This file
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

- **v1.0.0** (2025-12-04): Initial release
  - Stock data fetching
  - SQLite database
  - Flask web interface
  - Stock screener
  - Comparison tool
  - Watchlist feature
  - Value play detection
  - Bubble territory detection
