"""
Curated lists of popular stocks for quick fetching
"""

# Magnificent 7 (Big Tech)
MAG_7 = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'META', 'NVDA', 'TSLA']

# Top 25 S&P 500 by market cap
SP500_TOP_25 = [
    'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'NVDA', 'META', 'TSLA',
    'BRK.B', 'LLY', 'V', 'UNH', 'XOM', 'JPM', 'JNJ', 'WMT',
    'MA', 'PG', 'AVGO', 'HD', 'CVX', 'MRK', 'ABBV', 'COST',
    'ORCL', 'PEP'
]

# NASDAQ 100 - Top Tech stocks
NASDAQ_TOP_25 = [
    'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'NVDA', 'META', 'TSLA',
    'AVGO', 'COST', 'ASML', 'NFLX', 'AMD', 'ADBE', 'CSCO',
    'PEP', 'CMCSA', 'INTC', 'INTU', 'QCOM', 'TXN', 'AMAT',
    'HON', 'AMGN', 'SBUX', 'ISRG'
]

# Software & Cloud
SOFTWARE_CLOUD = [
    'MSFT', 'GOOGL', 'ORCL', 'CRM', 'ADBE', 'NOW', 'SNOW',
    'PLTR', 'DDOG', 'CRWD', 'ZS', 'PANW', 'TEAM', 'WDAY',
    'MNDY', 'U', 'NET', 'S', 'DOCN'
]

# Semiconductors
SEMICONDUCTORS = [
    'NVDA', 'AMD', 'INTC', 'TSM', 'ASML', 'AVGO', 'QCOM',
    'TXN', 'AMAT', 'MU', 'LRCX', 'KLAC', 'MCHP', 'ADI',
    'NXPI', 'ON'
]

# Electric Vehicles & Auto
EV_AUTO = [
    'TSLA', 'F', 'GM', 'RIVN', 'LCID', 'NIO', 'XPEV',
    'LI', 'TM', 'HMC'
]

# Financial Services
FINANCIAL = [
    'JPM', 'BAC', 'WFC', 'GS', 'MS', 'C', 'BLK', 'SCHW',
    'AXP', 'USB', 'PNC', 'TFC', 'COF', 'V', 'MA', 'PYPL',
    'SQ', 'COIN'
]

# Healthcare & Biotech
HEALTHCARE = [
    'UNH', 'JNJ', 'LLY', 'ABBV', 'MRK', 'TMO', 'ABT', 'PFE',
    'DHR', 'BMY', 'AMGN', 'GILD', 'VRTX', 'REGN', 'CVS',
    'ISRG', 'CI', 'MDT'
]

# E-commerce & Retail
ECOMMERCE_RETAIL = [
    'AMZN', 'WMT', 'COST', 'HD', 'TGT', 'LOW', 'NKE',
    'SBUX', 'MCD', 'BKNG', 'EBAY', 'ETSY', 'W', 'CHWY'
]

# AI & Emerging Tech
AI_EMERGING = [
    'NVDA', 'MSFT', 'GOOGL', 'META', 'ORCL', 'PLTR', 'SNOW',
    'CRWD', 'DDOG', 'NET', 'AI', 'SOUN', 'PATH', 'APP',
    'UPST', 'BBAI'
]

# Entertainment & Media
ENTERTAINMENT = [
    'DIS', 'NFLX', 'CMCSA', 'WBD', 'PARA', 'SPOT', 'ROKU',
    'RBLX', 'EA', 'TTWO', 'ATVI', 'SONY'
]

# All curated lists with descriptions
STOCK_LISTS = {
    'mag_7': {
        'name': 'Magnificent 7 (Big Tech)',
        'tickers': MAG_7,
        'description': 'The 7 largest tech companies by market cap'
    },
    'sp500_top_25': {
        'name': 'S&P 500 Top 25',
        'tickers': SP500_TOP_25,
        'description': 'Top 25 S&P 500 companies by market capitalization'
    },
    'nasdaq_top_25': {
        'name': 'NASDAQ Top 25',
        'tickers': NASDAQ_TOP_25,
        'description': 'Top 25 NASDAQ-listed companies'
    },
    'software_cloud': {
        'name': 'Software & Cloud',
        'tickers': SOFTWARE_CLOUD,
        'description': 'Leading software and cloud computing companies'
    },
    'semiconductors': {
        'name': 'Semiconductors',
        'tickers': SEMICONDUCTORS,
        'description': 'Chip makers and semiconductor equipment companies'
    },
    'ev_auto': {
        'name': 'Electric Vehicles & Auto',
        'tickers': EV_AUTO,
        'description': 'Electric vehicle manufacturers and traditional automakers'
    },
    'financial': {
        'name': 'Financial Services',
        'tickers': FINANCIAL,
        'description': 'Banks, payment processors, and fintech companies'
    },
    'healthcare': {
        'name': 'Healthcare & Biotech',
        'tickers': HEALTHCARE,
        'description': 'Healthcare providers, pharma, and biotech companies'
    },
    'ecommerce_retail': {
        'name': 'E-commerce & Retail',
        'tickers': ECOMMERCE_RETAIL,
        'description': 'Online and traditional retailers'
    },
    'ai_emerging': {
        'name': 'AI & Emerging Tech',
        'tickers': AI_EMERGING,
        'description': 'AI-focused and emerging technology companies'
    },
    'entertainment': {
        'name': 'Entertainment & Media',
        'tickers': ENTERTAINMENT,
        'description': 'Streaming, gaming, and media companies'
    }
}


def get_list(list_name: str) -> list:
    """Get a specific stock list by name"""
    return STOCK_LISTS.get(list_name, {}).get('tickers', [])


def get_all_lists() -> dict:
    """Get all stock lists"""
    return STOCK_LISTS
