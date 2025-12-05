"""
Basic test script for Stock Analyzer
Run with: source venv/bin/activate && python test_basic.py
"""

import sys
sys.path.insert(0, 'src')

from libs.stock_fetcher import StockFetcher
from libs.database import StockDatabase

def test_basic_functionality():
    """Test basic stock fetching and database operations"""

    print('='*50)
    print('Stock Analyzer - Basic Test')
    print('='*50)

    # Initialize components
    print('\n1. Initializing components...')
    fetcher = StockFetcher()
    db = StockDatabase('data/stocks.db')
    print('   ✓ StockFetcher and Database initialized')

    # Fetch sample stock
    print('\n2. Fetching AAPL stock data...')
    stock_data = fetcher.fetch_stock_data('AAPL')

    if not stock_data:
        print('   ✗ Failed to fetch stock data')
        return False

    print(f'   ✓ Fetched: {stock_data["company_name"]} ({stock_data["ticker"]})')
    print(f'   - Market Cap: ${stock_data["market_cap"]/1e9:.2f}B')
    print(f'   - Current Price: ${stock_data["current_price"]:.2f}')
    print(f'   - P/E Ratio: {stock_data["pe_ratio"]:.2f}')
    print(f'   - P/S Ratio: {stock_data["ps_ratio"]:.2f}')
    print(f'   - Revenue Growth: {(stock_data["revenue_growth"] or 0)*100:.1f}%')
    print(f'   - Bubble Score: {stock_data["bubble_score"]}/10')
    print(f'   - Risk Level: {stock_data["risk_level"]}')

    # Save to database
    print('\n3. Saving to database...')
    if db.save_stock(stock_data):
        print('   ✓ Successfully saved to database')
    else:
        print('   ✗ Failed to save to database')
        return False

    # Retrieve from database
    print('\n4. Retrieving from database...')
    retrieved = db.get_stock('AAPL')
    if retrieved:
        print(f'   ✓ Retrieved: {retrieved["company_name"]}')
    else:
        print('   ✗ Failed to retrieve from database')
        return False

    # Get all stocks
    print('\n5. Checking database contents...')
    all_stocks = db.get_all_stocks()
    print(f'   ✓ Total stocks in database: {len(all_stocks)}')

    # Clean up
    db.close()

    print('\n' + '='*50)
    print('✓ All basic tests passed!')
    print('='*50)
    print('\nYou can now run the Flask app with:')
    print('  ./run.sh')
    print('\nOr manually:')
    print('  source venv/bin/activate')
    print('  cd src/webapp')
    print('  python app.py')
    print('='*50)

    return True

if __name__ == '__main__':
    try:
        test_basic_functionality()
    except Exception as e:
        print(f'\n✗ Test failed with error: {str(e)}')
        import traceback
        traceback.print_exc()
