# Changelog

All notable changes to Stock Analyzer will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [1.2.4] - 2026-01-17

### Added
- **Macro Signals Dashboard**: Ray Dalio-inspired macroeconomic analysis dashboard
  - Currency tracking (USD, EUR, CHF, JPY, CNY) across 7 timeframes with dual calculation methods
  - Gold vs currencies analysis showing purchasing power erosion
  - S&P 500 performance with gold-adjusted returns
  - Interactive US Treasury yield curve with automatic inversion detection
  - Yield spread analysis (10Y-2Y, 10Y-3M, 30Y-5Y) with historical trends and time-series chart
  - Corporate credit spreads (BBB, High Yield, Master OAS) with percentile rankings
  - FRED API integration for Treasury yields, FX rates, and credit spreads
  - Yahoo Finance integration for gold futures and S&P 500 data

- **Performance Optimization**: 24-hour database caching system for all macro data
  - 90% reduction in page load time (30-60s → 2-3s on subsequent loads)
  - New `macro_data` table with cache validity checking

- **UI/UX Improvements**:
  - Logo icon in navigation bar
  - Compressed navigation spacing for better dropdown display
  - Color-coded trend badges (EXPANDING/CONTRACTING/STABLE)
  - Enhanced charts with inversion detection and interactive tooltips

### Changed
- Added `FRED_API_KEY` to environment configuration
- Updated navigation bar styling (reduced font sizes and padding)

### Fixed
- Geometric purchasing power calculation (was showing -208% instead of -40%)
- Database initialization error (`NameError` for undefined `DATABASE_PATH`)

## [1.2.3] - 2026-01-16

### Added

- Capability to add notes on stocks in the watchlist 
- Custom logo for the app

## [1.2.2] - 2026-01-03

### Added
- **Technical Analysis Feature**: Basic technical analysis with support/resistance levels and trend forecasting
  - New standalone page at `/technical-analysis/basic` with stock selector
  - 2-year OHLC price history fetching from Yahoo Finance API
  - Support & resistance level calculations using local maxima/minima clustering
  - Classical pivot points (R1/R2/R3, Pivot, S1/S2/S3)
  - Trend analysis using linear regression with 30-day and 90-day price targets
  - Interactive price chart with 20-day and 50-day moving averages
  - Beginner-friendly info modals explaining technical concepts

- **Technical Analysis Integration**: Quick summary panel on stock detail pages
  - Shows cached support/resistance levels and trend targets
  - Displays trend strength (R²) with color-coded indicators
  - Direct link to full technical analysis page

- **New Database Tables**:
  - `price_history`: Stores OHLC data with automatic accumulation over time
  - `technical_indicators`: Caches calculated support/resistance levels and trend metrics

- **New Navigation Menu**: "Technical Analysis" dropdown with "Basic" submenu item

### Changed
- Reorganized main navigation: "Technical Analysis" menu now appears before "Stock Analysis"
- Moved "Fetch Data" into "Tools" dropdown menu (first position)
- Added `scipy>=1.10.0` dependency for statistical calculations

## [1.2.1] - 2025-12-31

### Added
- **Four New Valuation Models**: Classical value investing and fundamental analysis models
  - **Graham Number**: Benjamin Graham's intrinsic value formula based on EPS and book value
  - **Gordon Growth Model**: Dividend discount model for dividend-paying stocks
  - **PEG-Based Valuation**: Fair value using PEG ratio and earnings growth
  - **P/S Sector Valuation**: Price-to-Sales comparison against sector medians

- **Multi-Model Consensus**: Intelligent aggregation of all forecasting models
  - Consensus target price from 8 valuation models
  - Agreement score (0-100) showing model alignment
  - Smart recommendations (Strong Buy, Buy, Hold, Sell) based on upside and agreement
  - Price range visualization showing min/max across all models

- **Enhanced Forecast Page**: Unified summary with individual model breakdown table
  - Merged forecast summary and consensus into single comprehensive view
  - Model-by-model results table with target prices and upside percentages
  - Visual status indicators for each model's validity

### Changed
- **Database Schema**: Added `book_value`, `dividend_rate`, and `dividend_yield` columns
- **Data Fetching**: Enhanced stock_fetcher to calculate book value per share using multiple fallback methods
- **P/S Valuation Logic**: Uses realistic sector-specific median P/S ratios instead of current stock P/S

## [1.2.0] - 2025-12-31

### Added
- **Docker Support**: Full containerization with Dockerfile, docker-compose.yml, and build/run scripts
  - Usage: `./docker-build.sh && ./docker-run.sh` or `docker-compose up -d`
  - Database persistence via volume mounting
  - Default port: 5001

- **Five New Advanced Screeners**: Investment strategy-focused filters
  - **Quality Growth** (`/quality-growth`): High CAGR with consistency and reasonable PEG ratios
  - **Growth Inflection** (`/growth-inflection`): Stocks showing growth acceleration
  - **Rule of 40** (`/rule-of-40`): SaaS efficiency metric (Growth % + Profit Margin % >= 40)
  - **Margin Expansion** (`/margin-expansion`): Companies with improving profitability
  - **Cash-Generative Growth** (`/cash-generative-growth`): High growth with positive FCF

- **Strategy Match System**: Visual cards on stock detail pages showing which investment strategies each stock qualifies for

- **New Metrics**: PEG Ratio, Operating Leverage, FCF Margin, Sector Rankings, Revenue vs Sector comparisons

### Changed
- Enhanced screener table with PEG Ratio, Sector Rank, and Revenue vs Sector columns
- Reorganized navigation menu with new screener links grouped by strategy type
- Extended database schema to support new metrics and calculations

## [1.1.5] - 2025-12-15

### Fixed
- **Zero Value Filtering**: Improved data quality by excluding zero values from Yahoo Finance API
  - Chart Rendering: Stock detail page charts now filter out zero values
  - Growth Calculations: All growth metrics exclude zero values from calculations
  - Forecast Accuracy: Forecast models benefit from cleaner growth metrics

### Impact
- More accurate revenue and earnings trend visualization
- Better growth rate calculations that aren't distorted by API errors
- Improved forecast reliability using cleaner historical data

## [1.1.4] - 2025-12-10

### Added
- **Portfolio Feature**: New portfolio management system alongside watchlist
  - Created `portfolio` database table for storing portfolio stocks
  - New dedicated portfolio page at `/portfolio` with full CRUD functionality
  - Portfolio integration in comparison page

### Changed
- **Navigation Redesign**: Replaced single "Watchlist" link with "My Stocks" dropdown menu
- **Stock Detail Page UX Improvements**: Reorganized action buttons into logical groups

## [1.1.3] - 2025-12-09

### Added
- **Share Your Stock List**: Generate comma-separated list of all stocks from database
  - One-click "Copy to Clipboard" functionality
  - Friends can paste the list to replicate your stock database

## [1.1.2] - 2025-12-07

### Added
- **Upside Calculator**: Calculate potential upside to target market caps
  - Set custom target market cap with quick buttons
  - Required growth calculator with historical comparison
  - Year-by-year progression visualization
- **Stock Analysis Dropdown Menu**: Organized analysis tools in navigation

### Changed
- **Code Organization**: Centralized CSS and JavaScript for better maintainability
  - Moved inline CSS to `src/webapp/static/css/`
  - Moved inline JavaScript to `src/webapp/static/js/`
  - Updated templates to use centralized files

## [1.1.1] - 2025-12-06
### Added
- Various navigation links between pages
- Updated requirements for Python 3.13

## [1.1.0] - 2025-12-06

### Added
- **Growth Analysis Tooltips**: Informative tooltips for all growth analysis metrics
- **Enhanced Comparison Page**: Added all growth analysis metrics with organized sections

### Fixed
- **Historical Data Preservation (Critical)**: Changed database from `INSERT OR REPLACE` to `INSERT OR IGNORE`
  - Historical financial data is now preserved when refreshing stock data
  - Builds a comprehensive historical dataset over time

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
