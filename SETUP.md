# Stock Analyzer - Setup Guide

## Prerequisites

- Python 3.13 or higher (recommended)
- Git
- Internet connection for fetching stock data

## Installation Steps

### 1. Clone the Repository

```bash
git clone https://github.com/Simon-Prevoteaux/stock-analyzer.git
cd stock-analyzer
```

### 2. Create Virtual Environment

```bash
python3 -m venv venv
source venv/bin/activate
```

### 3. Install Dependencies

```bash
pip install -r requirements.txt
```

### 4. Test the Installation

```bash
python test_basic.py
```

This will:
- Test the stock fetcher
- Fetch sample data for AAPL
- Test database operations
- Display sample stock metrics

### 5. Run the Application

Option A - Using the run script:
```bash
./run.sh
```

Option B - Manual:
```bash
source venv/bin/activate
cd src/webapp
python app.py
```

### 6. Access the Web Interface

Open your browser to: [http://localhost:5000](http://localhost:5000)

## First Steps

1. **Fetch Stock Data**
   - Navigate to "Fetch Data" in the menu
   - Enter ticker symbols (e.g., `AAPL, MSFT, GOOGL, NVDA, META`)
   - Click "Fetch Data"

2. **Use the Screener**
   - Go to "Screener"
   - Apply filters (P/E ratio, P/S ratio, sector, etc.)
   - View results in table format

3. **Compare Stocks**
   - Go to "Compare"
   - Enter multiple tickers separated by commas
   - View side-by-side comparison

4. **Find Value Plays**
   - Navigate to "Value Plays"
   - See stocks with P/E < 20 and P/S < 3

5. **Check Bubble Territory**
   - Navigate to "Bubble Territory"
   - See potentially overvalued stocks

## Troubleshooting

### Python Version Issues

If you encounter typing-related errors with Python 3.8, upgrade to Python 3.9+:

```bash
# Remove old virtual environment
rm -rf venv

# Create new with Python 3.9+
python3.9 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### Module Not Found Errors

Ensure virtual environment is activated:
```bash
source venv/bin/activate
```

### Database Issues

If you encounter database errors, delete and recreate:
```bash
rm -rf data/stocks.db
# Run the app again - it will recreate the database
```

### Yahoo Finance Rate Limiting

If stock fetching fails:
- Wait a few minutes between large batch requests
- Fetch stocks in smaller batches
- Check internet connection

## Project Structure

```
stock-analyzer/
├── src/
│   ├── libs/              # Core functionality
│   └── webapp/            # Flask application
├── data/                  # SQLite database (auto-created)
├── venv/                  # Virtual environment
├── test_basic.py          # Test script
├── run.sh                 # Startup script
└── requirements.txt       # Dependencies
```

## Next Steps

- Read [README.md](README.md) for feature overview
- Read [claude.md](claude.md) for technical documentation
- Start analyzing stocks!

## Support

This is a personal project. For issues:
1. Check this guide
2. Review error messages
3. Check [claude.md](claude.md) for architecture details
