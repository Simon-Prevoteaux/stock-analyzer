# Changelog

All notable changes to Stock Analyzer will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [1.1.4] - 2025-12-10

### Added
- **Portfolio Feature**: New portfolio management system alongside watchlist
  - Created `portfolio` database table for storing portfolio stocks
  - Added backend functions: `add_to_portfolio()`, `get_portfolio()`, `remove_from_portfolio()`
  - New dedicated portfolio page at `/portfolio` with full CRUD functionality
  - Portfolio stocks can be viewed, added, and removed similar to watchlist
  - Portfolio integration in comparison page: Added "My Portfolio" to Quick Comparison Groups
  - Portfolio count now displayed on home page statistics

### Changed
- **Navigation Redesign**: Replaced single "Watchlist" link with "My Stocks" dropdown menu
  - Dropdown now contains both "Watchlist" and "Portfolio" options
  - Consistent navigation pattern with "Stock Analysis" dropdown
  - Better organization of personal stock lists
- **Stock Detail Page UX Improvements**: Complete reorganization of action buttons
  - Moved action button bar from bottom to top of page (right after stock header)
  - Organized buttons into three logical groups with headers:
    - "Add to Lists": Add to Watchlist + Add to Portfolio
    - "Analysis": See Forecast
    - "Data Management": Refresh Data + Back to Screener
  - Improved visual layout with better spacing and alignment
  - Added descriptive section headers for better clarity

### Technical Details
- Database: New `portfolio` table with same schema as `watchlist` (ticker, added_date, notes)
- Routes: `/portfolio`, `/portfolio/add/<ticker>`, `/portfolio/remove/<ticker>`
- Templates: New `portfolio.html` template mirroring watchlist functionality
- CSS: Added `.action-group` styling for organized button layout on stock detail page

## [1.1.3] - 2025-12-09

### Added
- **Share Your Stock List**: New feature on the fetch data page to share your stock database with friends
  - Added "Generate Ticker List" button that retrieves all stocks from your database
  - Generates a comma-separated list of all stock tickers (e.g., "AAPL, AMD, GOOGL, MSFT, NVDA")
  - Displays count of stocks in database for quick reference
  - One-click "Copy to Clipboard" functionality with visual feedback
  - Friends can paste the list directly into the fetch form to replicate your stock database
  - New API endpoint `/api/all-tickers` that returns sorted ticker list and count
  - Tickers are automatically sorted alphabetically for consistency

## [1.1.2] - 2025-12-07

### Added
- **Upside Calculator**: New dedicated page for calculating potential upside to target market caps
  - Set custom target market cap (with quick buttons for $10B, $100B, $500B, $1T, $2T, $5T)
  - View all stocks ranked by upside potential to the target
  - Interactive selection to view detailed stock information
  - Visual market cap progression chart (current vs target)
  - Required growth calculator: Calculate what annual growth rate (CAGR) is needed to reach target over a specified timeframe
  - Historical growth comparison: Compare required growth against historical revenue/earnings CAGR
  - Feasibility assessment: Determines if the target is "Achievable" or "Ambitious" based on historical performance
  - Year-by-year progression visualization showing projected market cap and stock price growth path
  - Real-time calculations and dual-axis charts (market cap + stock price)
- **Stock Analysis Dropdown Menu**: Improved navigation with organized analysis tools
  - Added dropdown menu in navigation bar for "Stock Analysis" section
  - Grouped related analysis pages: Screener, Compare, Forecast, Upside, Value Plays, Bubble Territory
  - Cleaner navigation with better discoverability of features
  - Hover-activated dropdown with smooth animations

### Changed
- **Code Organization**: Centralized CSS and JavaScript for better maintainability
  - Moved inline CSS to centralized stylesheets in `src/webapp/static/css/`:
    - `search.css`: Search component styles (quick search, autocomplete, chips)
    - `modal.css`: Modal component styles (info modals, section headers)
  - Moved inline JavaScript to centralized files in `src/webapp/static/js/`:
    - `search.js`: Stock search functionality (autocomplete, keyboard navigation)
    - `comparison.js`: Stock comparison logic (multi-select, chip management)
    - `upside-calculator.js`: Upside calculator functionality (charts, growth calculations)
    - `modal.js`: Info modal functionality (metric explanations, tooltips)
  - Updated templates to use centralized files:
    - `screener.html`: Removed ~95 lines of inline CSS/JS
    - `comparison.html`: Removed ~580 lines of inline CSS/JS
    - `upside_calculator.html`: Removed ~290 lines of inline JS
  - Added global CSS includes in `base.html` for site-wide availability
  - **Benefits**: Easier maintenance, better caching, cleaner template code, reusable components

## [1.1.1] - 2025-12-06
### Added
- various links in the app to help navigation between pages
- new requirements to be used for more recent version of Python (3.13)
  
## [1.1.0] - 2025-12-06

### Added
- **Growth Analysis Tooltips**: Added informative tooltips to all growth analysis metrics on stock detail page
  - Revenue CAGR (3Y): Explains calculation formula and interpretation guidelines
  - Earnings CAGR (3Y): Details the compound annual growth rate calculation for earnings
  - Revenue Consistency Score: Breaks down the 0-100 scoring methodology
  - Earnings Consistency Score: Explains consistency measurement for earnings
  - Profitable Quarters: Describes consecutive profitability tracking
- **Enhanced Comparison Page**: Major improvements to stock comparison functionality
  - Added all growth analysis metrics (Revenue/Earnings CAGR, Consistency Scores, Profitable Quarters)
  - Organized comparison table into clear sections:
    - Valuation Metrics
    - Financial Metrics
    - Growth & Risk
    - Growth Analysis
  - Added tooltips for Bubble Score, Risk Level, and all growth metrics
  - Color-coded consistency scores (green for high, yellow for medium, red for low)
- **Section Headers**: Added visual section headers throughout comparison table for better organization

### Fixed
- **Historical Data Preservation (Critical)**: Changed database insert strategy from `INSERT OR REPLACE` to `INSERT OR IGNORE`
  - Historical financial data is now preserved when refreshing stock data
  - Old quarters no longer available from Yahoo Finance API are permanently archived
  - New data is appended without overwriting existing records
  - Builds a comprehensive historical dataset over time
  - Location: `src/libs/database.py:456` - `save_financial_history()` method

### Changed
- Improved comparison page UX with better visual hierarchy and section organization
- Enhanced documentation in `save_financial_history()` method explaining data preservation strategy

## [1.0.0] - 2025-12-04

### Added
- Initial release
- Stock data fetching from Yahoo Finance
- SQLite database for persistent storage
- Flask web interface
- Stock screener with filters
- Stock comparison tool
- Watchlist feature
- Value play detection
- Bubble territory detection
- Bubble score calculation (0-10 scale)
- Risk level classification
- Historical data tracking (quarterly and annual)
- Growth analysis with CAGR calculations
- Consistency scoring for revenue and earnings
- Interactive charts for historical trends
- Forecasting models (earnings growth, revenue growth, DCF, Monte Carlo)

### Features Included
- **Data Fetching**: Fetch comprehensive stock metrics from Yahoo Finance API
- **Database**: SQLite-based storage with automatic schema migration
- **Web Interface**: Clean, responsive Flask application
- **Stock Screener**: Filter stocks by various metrics
- **Comparison**: Side-by-side stock comparison
- **Watchlist**: Track your favorite stocks
- **Analysis Tools**:
  - Bubble score detection
  - Growth metrics calculation
  - Historical trend analysis
  - Forecasting models
