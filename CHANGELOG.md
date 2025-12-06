# Changelog

All notable changes to Stock Analyzer will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
