# Stock Analyzer

Personal stock analysis and valuation tool for comparing stocks, identifying value plays, and detecting overvalued securities.

## Features

- **Real-time Stock Data**: Fetch live stock metrics from Yahoo Finance
- **Key Metrics**: Market cap, revenue, earnings, P/E, P/S, EPS, growth rates
- **Stock Screener**: Filter stocks by multiple criteria
- **Comparison Tool**: Compare multiple stocks side-by-side
- **Value Detection**: Identify potential value plays
- **Bubble Detection**: Flag overvalued stocks with high bubble scores
- **Watchlist**: Maintain a personal watchlist
- **Local Storage**: SQLite database for persistent data

## Installation

1. Clone the repository:
```bash
git clone https://github.com/Simon-Prevoteaux/stock-analyzer.git
cd stock-analyzer
```

2. Create and activate virtual environment:
```bash
python3 -m venv venv
source venv/bin/activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

## Usage

1. Start the Flask application:
```bash
cd src/webapp
python app.py
```

2. Open your browser to `http://localhost:5000`

3. Fetch stock data using ticker symbols (e.g., AAPL, MSFT, GOOGL)

4. Use the screener to filter and analyze stocks

## Project Structure

```
stock-analyzer/
├── src/
│   ├── libs/                 # Core libraries
│   │   ├── stock_fetcher.py  # Fetch stock data from Yahoo Finance
│   │   └── database.py       # SQLite database management
│   └── webapp/               # Flask web application
│       ├── app.py            # Main Flask app
│       ├── templates/        # HTML templates
│       └── static/           # CSS and static files
├── data/                     # SQLite database storage
├── tests/                    # Test files
├── requirements.txt          # Python dependencies
└── README.md
```

## Metrics Explained

- **P/E Ratio**: Price-to-Earnings ratio
- **P/S Ratio**: Price-to-Sales ratio
- **EPS**: Earnings Per Share
- **Bubble Score**: Custom metric (0-10) indicating overvaluation risk
- **Risk Level**: Classification based on bubble score

## License

Personal use only
