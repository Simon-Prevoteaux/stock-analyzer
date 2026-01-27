"""
Macro Data Fetcher - FRED API Integration
Fetches macroeconomic data from Federal Reserve Economic Data (FRED) API

This module provides the MacroDataFetcher class for fetching and caching
macroeconomic data from FRED API and Yahoo Finance.

For spread calculations, see also: libs/spread_calculator.py (SpreadCalculator)
"""

import requests
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import logging
import yfinance as yf

logger = logging.getLogger(__name__)


class MacroDataFetcher:
    """Fetches macro data from FRED API"""

    FRED_BASE_URL = "https://api.stlouisfed.org/fred/series/observations"

    # FRED Series IDs
    CURRENCY_SERIES = {
        'EUR': 'DEXUSEU',  # US / Euro FX Rate
        'JPY': 'DEXJPUS',  # Japan / US FX Rate (JPY per USD)
        'CNY': 'DEXCHUS',  # China / US FX Rate (CNY per USD)
        'CHF': 'DEXSZUS',  # Switzerland / US FX Rate (CHF per USD)
    }

    GOLD_SERIES = 'GLDPRCUSD'  # Gold Price: USD per Troy Ounce (daily)

    TREASURY_SERIES = {
        '1M': 'DGS1MO',
        '3M': 'DGS3MO',
        '6M': 'DGS6MO',
        '1Y': 'DGS1',
        '2Y': 'DGS2',
        '3Y': 'DGS3',
        '5Y': 'DGS5',
        '7Y': 'DGS7',
        '10Y': 'DGS10',
        '20Y': 'DGS20',
        '30Y': 'DGS30',
    }

    CREDIT_SPREAD_SERIES = {
        'corporate_master': 'BAMLC0A0CM',      # ICE BofA US Corporate Master OAS
        'high_yield': 'BAMLH0A0HYM2',          # ICE BofA US High Yield Master II OAS
        'bbb': 'BAMLC0A4CBBB',                 # ICE BofA BBB US Corporate Index OAS
    }

    # Global Economy Series IDs
    GLOBAL_ECONOMY_SERIES = {
        'buffett_indicator': 'DDDM01USA156NWDB',  # Stock Market Capitalization to GDP (pre-calculated Buffett Indicator)
        'gdp': 'GDP',                          # Gross Domestic Product (quarterly)
        'm2': 'M2SL',                          # M2 Money Stock (monthly)
        'm2_velocity': 'M2V',                  # Velocity of M2 Money Stock
        'debt_gdp': 'GFDEGDQ188S',            # Federal Debt: Total Public Debt as % of GDP
        'debt_public_gdp': 'FYGFGDQ188S',     # Federal Debt Held by Public as % of GDP
    }

    # Real Estate Series IDs
    REAL_ESTATE_SERIES = {
        'case_shiller_national': 'CSUSHPINSA',    # Case-Shiller US National Home Price Index
        'case_shiller_20city': 'SPCS20RSA',       # Case-Shiller 20-City Composite
        'housing_starts': 'HOUST',                 # New Housing Units Started
        'building_permits': 'PERMIT',              # Building Permits
        'existing_home_sales': 'EXHOSLUSM495N',   # Existing Home Sales
        'housing_inventory': 'HOSINVUSM495N',     # Housing Inventory
        'months_supply': 'HOSSUPUSM673N',         # Months Supply of Existing Homes
        'new_home_months_supply': 'MSACSR',       # Monthly Supply of New Houses
        'mortgage_30y': 'MORTGAGE30US',           # 30-Year Fixed Mortgage Rate
        'affordability_index': 'FIXHAI',          # Housing Affordability Index
        'median_home_price': 'MSPUS',             # Median Sales Price of Houses Sold
        'median_income': 'MEHOINUSA646N',         # Median Household Income (annual)
        'mortgage_debt_service': 'MDSP',          # Mortgage Debt Service as % of Disposable Income
    }

    # Inflation & Fed Policy Series IDs
    INFLATION_SERIES = {
        'cpi': 'CPIAUCSL',              # Consumer Price Index for All Urban Consumers
        'core_cpi': 'CPILFESL',         # CPI Less Food and Energy (Core CPI)
        'pce': 'PCEPI',                 # Personal Consumption Expenditures Price Index
        'core_pce': 'PCEPILFE',         # PCE Less Food and Energy (Core PCE - Fed's target)
        'breakeven_5y': 'T5YIE',        # 5-Year Breakeven Inflation Rate
        'breakeven_10y': 'T10YIE',      # 10-Year Breakeven Inflation Rate
        'fed_funds': 'DFEDTARU',        # Federal Funds Target Rate - Upper Bound
        'fed_assets': 'WALCL',          # Fed Total Assets (Balance Sheet)
    }

    # Consumer Sentiment Series IDs
    SENTIMENT_SERIES = {
        'consumer_sentiment': 'UMCSENT',  # University of Michigan Consumer Sentiment Index
    }

    # Money Market Funds Series IDs
    MONEY_MARKET_SERIES = {
        'total_assets': 'MMMFFAQ027S',  # Money Market Funds Total Financial Assets (quarterly)
    }

    def __init__(self, fred_api_key: str, db=None, cache_hours: int = 24):
        """
        Initialize with FRED API key and optional database for caching

        Args:
            fred_api_key: FRED API key
            db: Database connection for caching (optional)
            cache_hours: Hours before cache expires (default 24)
        """
        self.api_key = fred_api_key
        self.db = db
        self.cache_hours = cache_hours

    def _get_cached_series(self, series_id: str, start_date: Optional[str] = None) -> Optional[pd.DataFrame]:
        """
        Try to get series data from database cache

        Returns:
            DataFrame if cache is fresh, None if cache miss or stale
        """
        if not self.db:
            return None

        try:
            # Determine data type based on series_id
            if series_id in self.CURRENCY_SERIES.values():
                data_type = 'fx_rate'
            elif series_id in self.TREASURY_SERIES.values():
                data_type = 'yield'
            elif series_id in self.CREDIT_SPREAD_SERIES.values():
                data_type = 'credit_spread'
            elif series_id == 'GC=F':
                data_type = 'gold'
            elif series_id in [self.GLOBAL_ECONOMY_SERIES.get('buffett_indicator')]:
                data_type = 'buffett_indicator'
            elif series_id in [self.GLOBAL_ECONOMY_SERIES.get('gdp')]:
                data_type = 'gdp'
            elif series_id in [self.GLOBAL_ECONOMY_SERIES.get('m2'), self.GLOBAL_ECONOMY_SERIES.get('m2_velocity')]:
                data_type = 'money_supply'
            elif series_id in [self.GLOBAL_ECONOMY_SERIES.get('debt_gdp'), self.GLOBAL_ECONOMY_SERIES.get('debt_public_gdp')]:
                data_type = 'debt'
            elif series_id in [self.REAL_ESTATE_SERIES.get('case_shiller_national'),
                              self.REAL_ESTATE_SERIES.get('case_shiller_20city'),
                              self.REAL_ESTATE_SERIES.get('median_home_price')]:
                data_type = 'housing_price'
            elif series_id in [self.REAL_ESTATE_SERIES.get('housing_starts'),
                              self.REAL_ESTATE_SERIES.get('building_permits'),
                              self.REAL_ESTATE_SERIES.get('existing_home_sales')]:
                data_type = 'housing_activity'
            elif series_id in [self.REAL_ESTATE_SERIES.get('housing_inventory'),
                              self.REAL_ESTATE_SERIES.get('months_supply'),
                              self.REAL_ESTATE_SERIES.get('new_home_months_supply')]:
                data_type = 'housing_inventory'
            elif series_id in [self.REAL_ESTATE_SERIES.get('mortgage_30y')]:
                data_type = 'mortgage'
            elif series_id in [self.REAL_ESTATE_SERIES.get('affordability_index'),
                              self.REAL_ESTATE_SERIES.get('median_income'),
                              self.REAL_ESTATE_SERIES.get('mortgage_debt_service')]:
                data_type = 'affordability'
            elif series_id in [self.INFLATION_SERIES.get('cpi'),
                              self.INFLATION_SERIES.get('core_cpi'),
                              self.INFLATION_SERIES.get('pce'),
                              self.INFLATION_SERIES.get('core_pce')]:
                data_type = 'inflation'
            elif series_id in [self.INFLATION_SERIES.get('breakeven_5y'),
                              self.INFLATION_SERIES.get('breakeven_10y')]:
                data_type = 'breakeven'
            elif series_id in [self.INFLATION_SERIES.get('fed_funds')]:
                data_type = 'fed_funds'
            elif series_id in [self.INFLATION_SERIES.get('fed_assets')]:
                data_type = 'fed_assets'
            elif series_id in ['^VIX', '^VIX3M']:
                data_type = 'vix'
            elif series_id in [self.SENTIMENT_SERIES.get('consumer_sentiment')]:
                data_type = 'consumer_sentiment'
            elif series_id in [self.MONEY_MARKET_SERIES.get('total_assets')]:
                data_type = 'money_market'
            elif series_id in ['^RUT']:
                data_type = 'small_cap_index'
            elif series_id in ['BTC-USD']:
                data_type = 'crypto'
            else:
                return None

            # Check if we have recent data
            df = self.db.get_macro_data(data_type, series_id, start_date=start_date)

            if df.empty:
                return None

            # Check if cache is fresh (has data from last cache_hours)
            latest_date = pd.to_datetime(df['date'].max())
            cache_age = datetime.now() - latest_date.replace(tzinfo=None)

            # Also check if cache covers the requested start_date
            if start_date:
                earliest_date = pd.to_datetime(df['date'].min())
                requested_start = pd.to_datetime(start_date)
                # Allow 30 days tolerance for data that may not be available at exact start
                if earliest_date > requested_start + pd.Timedelta(days=30):
                    logger.info(f"Cache incomplete for {series_id}: earliest={earliest_date.date()}, requested={requested_start.date()}")
                    return None

            if cache_age.total_seconds() / 3600 <= self.cache_hours:
                logger.info(f"Using cached data for {series_id} (age: {cache_age.total_seconds()/3600:.1f}h)")
                return df
            else:
                logger.info(f"Cache stale for {series_id} (age: {cache_age.total_seconds()/3600:.1f}h)")
                return None

        except Exception as e:
            logger.error(f"Error reading cache for {series_id}: {e}")
            return None

    def _save_to_cache(self, series_id: str, df: pd.DataFrame):
        """Save series data to database cache"""
        if not self.db or df.empty:
            return

        try:
            # Determine data type
            if series_id in self.CURRENCY_SERIES.values():
                data_type = 'fx_rate'
            elif series_id in self.TREASURY_SERIES.values():
                data_type = 'yield'
            elif series_id in self.CREDIT_SPREAD_SERIES.values():
                data_type = 'credit_spread'
            elif series_id == 'GC=F':
                data_type = 'gold'
            elif series_id in [self.GLOBAL_ECONOMY_SERIES.get('buffett_indicator')]:
                data_type = 'buffett_indicator'
            elif series_id in [self.GLOBAL_ECONOMY_SERIES.get('gdp')]:
                data_type = 'gdp'
            elif series_id in [self.GLOBAL_ECONOMY_SERIES.get('m2'), self.GLOBAL_ECONOMY_SERIES.get('m2_velocity')]:
                data_type = 'money_supply'
            elif series_id in [self.GLOBAL_ECONOMY_SERIES.get('debt_gdp'), self.GLOBAL_ECONOMY_SERIES.get('debt_public_gdp')]:
                data_type = 'debt'
            elif series_id in [self.REAL_ESTATE_SERIES.get('case_shiller_national'),
                              self.REAL_ESTATE_SERIES.get('case_shiller_20city'),
                              self.REAL_ESTATE_SERIES.get('median_home_price')]:
                data_type = 'housing_price'
            elif series_id in [self.REAL_ESTATE_SERIES.get('housing_starts'),
                              self.REAL_ESTATE_SERIES.get('building_permits'),
                              self.REAL_ESTATE_SERIES.get('existing_home_sales')]:
                data_type = 'housing_activity'
            elif series_id in [self.REAL_ESTATE_SERIES.get('housing_inventory'),
                              self.REAL_ESTATE_SERIES.get('months_supply'),
                              self.REAL_ESTATE_SERIES.get('new_home_months_supply')]:
                data_type = 'housing_inventory'
            elif series_id in [self.REAL_ESTATE_SERIES.get('mortgage_30y')]:
                data_type = 'mortgage'
            elif series_id in [self.REAL_ESTATE_SERIES.get('affordability_index'),
                              self.REAL_ESTATE_SERIES.get('median_income'),
                              self.REAL_ESTATE_SERIES.get('mortgage_debt_service')]:
                data_type = 'affordability'
            elif series_id in [self.INFLATION_SERIES.get('cpi'),
                              self.INFLATION_SERIES.get('core_cpi'),
                              self.INFLATION_SERIES.get('pce'),
                              self.INFLATION_SERIES.get('core_pce')]:
                data_type = 'inflation'
            elif series_id in [self.INFLATION_SERIES.get('breakeven_5y'),
                              self.INFLATION_SERIES.get('breakeven_10y')]:
                data_type = 'breakeven'
            elif series_id in [self.INFLATION_SERIES.get('fed_funds')]:
                data_type = 'fed_funds'
            elif series_id in [self.INFLATION_SERIES.get('fed_assets')]:
                data_type = 'fed_assets'
            elif series_id in ['^VIX', '^VIX3M']:
                data_type = 'vix'
            elif series_id in [self.SENTIMENT_SERIES.get('consumer_sentiment')]:
                data_type = 'consumer_sentiment'
            elif series_id in [self.MONEY_MARKET_SERIES.get('total_assets')]:
                data_type = 'money_market'
            elif series_id in ['^RUT']:
                data_type = 'small_cap_index'
            elif series_id in ['BTC-USD']:
                data_type = 'crypto'
            else:
                return

            # Convert DataFrame to list of dicts for database
            observations = [
                {'date': row['date'].strftime('%Y-%m-%d'), 'value': row['value']}
                for _, row in df.iterrows()
            ]

            self.db.save_macro_data(data_type, series_id, observations)
            logger.info(f"Saved {len(observations)} observations for {series_id} to cache")

        except Exception as e:
            logger.error(f"Error saving cache for {series_id}: {e}")

    def _fetch_series(self, series_id: str, start_date: Optional[str] = None,
                      end_date: Optional[str] = None, use_cache: bool = True) -> pd.DataFrame:
        """
        Fetch a FRED data series with caching support

        Args:
            series_id: FRED series identifier
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            use_cache: Whether to use database cache (default True)

        Returns:
            DataFrame with columns: date, value
        """
        # Try cache first
        if use_cache:
            cached_df = self._get_cached_series(series_id, start_date)
            if cached_df is not None:
                return cached_df

        # Cache miss or disabled - fetch from API
        params = {
            'series_id': series_id,
            'api_key': self.api_key,
            'file_type': 'json',
        }

        if start_date:
            params['observation_start'] = start_date
        if end_date:
            params['observation_end'] = end_date

        try:
            response = requests.get(self.FRED_BASE_URL, params=params, timeout=10)
            response.raise_for_status()

            data = response.json()

            if 'observations' not in data:
                logger.warning(f"No observations found for series {series_id}")
                return pd.DataFrame(columns=['date', 'value'])

            observations = data['observations']

            # Filter out missing values (FRED uses '.' for missing data)
            valid_obs = [
                {'date': obs['date'], 'value': float(obs['value'])}
                for obs in observations
                if obs['value'] != '.'
            ]

            df = pd.DataFrame(valid_obs)
            if not df.empty:
                df['date'] = pd.to_datetime(df['date'])
                df = df.sort_values('date')

                # Save to cache
                if use_cache:
                    self._save_to_cache(series_id, df)

            return df

        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching FRED series {series_id}: {e}")
            return pd.DataFrame(columns=['date', 'value'])
        except (ValueError, KeyError) as e:
            logger.error(f"Error parsing FRED data for {series_id}: {e}")
            return pd.DataFrame(columns=['date', 'value'])

    def _calculate_period_return(self, df: pd.DataFrame, days_back: int) -> Optional[float]:
        """
        Calculate percentage return over a period

        Args:
            df: DataFrame with date and value columns
            days_back: Number of days to look back

        Returns:
            Percentage return or None if data not available
        """
        if df.empty or len(df) < 2:
            return None

        latest_value = df.iloc[-1]['value']
        target_date = df.iloc[-1]['date'] - timedelta(days=days_back)

        # Find closest date to target
        df_sorted = df[df['date'] <= df.iloc[-1]['date']].copy()
        df_sorted['date_diff'] = abs((df_sorted['date'] - target_date).dt.days)
        closest_row = df_sorted.nsmallest(1, 'date_diff')

        if closest_row.empty:
            return None

        previous_value = closest_row.iloc[0]['value']

        if previous_value == 0 or pd.isna(previous_value):
            return None

        return ((latest_value - previous_value) / previous_value) * 100

    def fetch_exchange_rate(self, currency: str, lookback_days: int = 1825) -> pd.DataFrame:
        """
        Fetch exchange rate data for a currency

        Args:
            currency: Currency code (EUR, JPY, CNY, CHF)
            lookback_days: Days of history to fetch (default 5 years ~1825 days)

        Returns:
            DataFrame with date and value columns
        """
        if currency not in self.CURRENCY_SERIES:
            logger.error(f"Unknown currency: {currency}")
            return pd.DataFrame(columns=['date', 'value'])

        series_id = self.CURRENCY_SERIES[currency]
        start_date = (datetime.now() - timedelta(days=lookback_days)).strftime('%Y-%m-%d')

        return self._fetch_series(series_id, start_date=start_date)

    def fetch_gold_price(self, lookback_days: int = 1825, use_cache: bool = True) -> pd.DataFrame:
        """
        Fetch gold price data from Yahoo Finance with caching

        Args:
            lookback_days: Days of history to fetch (default 5 years)
            use_cache: Whether to use database cache (default True)

        Returns:
            DataFrame with date and value columns
        """
        series_id = 'GC=F'  # Gold futures ticker
        start_date = (datetime.now() - timedelta(days=lookback_days)).strftime('%Y-%m-%d')

        # Try cache first
        if use_cache:
            cached_df = self._get_cached_series(series_id, start_date)
            if cached_df is not None:
                return cached_df

        # Cache miss - fetch from Yahoo Finance
        try:
            gold = yf.Ticker(series_id)
            end_date = datetime.now()
            start_date_dt = end_date - timedelta(days=lookback_days)

            hist = gold.history(start=start_date_dt, end=end_date)

            if hist.empty:
                logger.warning("No gold price data available from Yahoo Finance")
                return pd.DataFrame(columns=['date', 'value'])

            # Convert to FRED-like format
            df = pd.DataFrame({
                'date': hist.index,
                'value': hist['Close']
            })
            df['date'] = pd.to_datetime(df['date']).dt.tz_localize(None)  # Remove timezone for consistency with FRED
            df = df.sort_values('date').reset_index(drop=True)

            logger.info(f"Fetched {len(df)} gold price observations from Yahoo Finance")

            # Save to cache
            if use_cache:
                self._save_to_cache(series_id, df)

            return df

        except Exception as e:
            logger.error(f"Error fetching gold price from Yahoo Finance: {e}")
            return pd.DataFrame(columns=['date', 'value'])

    def calculate_currency_returns(self, base: str = 'USD') -> List[Dict]:
        """
        Calculate currency returns vs USD across multiple timeframes

        Args:
            base: Base currency (default USD)

        Returns:
            List of dicts with currency returns across timeframes
        """
        timeframes = {
            '1d': 1,
            '1w': 7,
            '1m': 30,
            '3m': 90,
            '1y': 365,
            '3y': 1095,
            '5y': 1825,
        }

        results = []

        for currency, series_id in self.CURRENCY_SERIES.items():
            df = self.fetch_exchange_rate(currency, lookback_days=1825)

            currency_data = {
                'name': currency,
                'code': currency,
            }

            for period, days in timeframes.items():
                return_pct = self._calculate_period_return(df, days)
                # FRED FX rate interpretation:
                # EUR (DEXUSEU): USD per EUR - if it goes up, EUR strengthened (positive return = EUR gained)
                # JPY/CNY/CHF (DEXJPUS/DEXCHUS/DEXSZUS): Foreign per USD - if it goes down, foreign strengthened
                if return_pct is not None:
                    if currency == 'EUR':
                        # DEXUSEU is USD per EUR, so positive return = EUR strengthened
                        # No inversion needed
                        currency_data[period] = return_pct
                    else:
                        # For JPY, CNY, CHF: series is foreign per USD
                        # If rate increases (more JPY per USD), JPY weakened, so we invert
                        # If rate decreases (less JPY per USD), JPY strengthened, so we invert
                        currency_data[period] = -return_pct
                else:
                    currency_data[period] = None

            results.append(currency_data)

        return results

    def calculate_gold_returns(self) -> Dict:
        """
        Calculate USD performance vs gold across multiple timeframes

        Ray Dalio's methodology: measure currency purchasing power vs gold
        If gold price in USD goes up, USD lost purchasing power (negative return)

        Returns:
            Dict with USD performance vs gold for each timeframe
        """
        timeframes = {
            '1d': 1,
            '1w': 7,
            '1m': 30,
            '3m': 90,
            '1y': 365,
            '3y': 1095,
            '5y': 1825,
        }

        df = self.fetch_gold_price(lookback_days=1825)

        gold_data = {
            'name': 'USD',
            'code': 'USD',
        }

        for period, days in timeframes.items():
            gold_return = self._calculate_period_return(df, days)
            # Currency vs Gold = - (Gold price return in that currency)
            # If gold price in USD +150%, USD lost 150% purchasing power → -150%
            gold_data[period] = -gold_return if gold_return is not None else None

        return gold_data

    def calculate_currencies_vs_gold(self) -> List[Dict]:
        """
        Calculate all major currencies' performance vs gold

        Ray Dalio's methodology: If gold price in a currency goes up,
        that currency lost purchasing power vs gold (negative return)

        Returns:
            List of dicts with currency performance vs gold
        """
        timeframes = {
            '1d': 1,
            '1w': 7,
            '1m': 30,
            '3m': 90,
            '1y': 365,
            '3y': 1095,
            '5y': 1825,
        }

        # Get gold price in USD
        gold_usd_df = self.fetch_gold_price(lookback_days=1825)

        results = []

        # USD vs Gold (already calculated, but include here for consistency)
        usd_data = {
            'name': 'USD',
            'code': 'USD',
        }
        for period, days in timeframes.items():
            gold_return_usd = self._calculate_period_return(gold_usd_df, days)
            # If gold price in USD +150%, USD lost 150% → -150%
            usd_data[period] = -gold_return_usd if gold_return_usd is not None else None
        results.append(usd_data)

        # Other currencies vs Gold
        # Gold price in EUR = Gold price in USD × (EUR per USD) = Gold price in USD / (USD per EUR)
        # We need to calculate: (Gold_EUR_now / Gold_EUR_then - 1) then invert

        for currency in ['EUR', 'JPY', 'CNY', 'CHF']:
            fx_df = self.fetch_exchange_rate(currency, lookback_days=1825)

            if fx_df.empty:
                continue

            # Merge FX rate with gold price on date
            merged = pd.merge(gold_usd_df, fx_df, on='date', suffixes=('_gold', '_fx'))

            if merged.empty:
                continue

            # Calculate gold price in local currency
            if currency == 'EUR':
                # DEXUSEU = USD per EUR, so Gold_EUR = Gold_USD / (USD per EUR)
                merged['gold_local'] = merged['value_gold'] / merged['value_fx']
            else:
                # For JPY/CNY/CHF: series is foreign per USD
                # Gold_JPY = Gold_USD × (JPY per USD)
                merged['gold_local'] = merged['value_gold'] * merged['value_fx']

            # Create dataframe for local gold price
            gold_local_df = pd.DataFrame({
                'date': merged['date'],
                'value': merged['gold_local']
            })

            currency_data = {
                'name': currency,
                'code': currency,
            }

            for period, days in timeframes.items():
                gold_return_local = self._calculate_period_return(gold_local_df, days)
                # Currency vs Gold = - (Gold price return in that currency)
                # If gold price in EUR +154%, EUR lost 154% purchasing power → -154%
                currency_data[period] = -gold_return_local if gold_return_local is not None else None

            results.append(currency_data)

        return results

    def calculate_sp500_returns(self, use_cache: bool = True) -> Dict:
        """
        Calculate S&P 500 returns across multiple timeframes with caching

        Args:
            use_cache: Whether to use database cache (default True)

        Returns:
            Dict with S&P 500 returns for each timeframe
        """
        timeframes = {
            '1d': 1,
            '1w': 7,
            '1m': 30,
            '3m': 90,
            '1y': 365,
            '3y': 1095,
            '5y': 1825,
        }

        series_id = '^GSPC'
        start_date_str = (datetime.now() - timedelta(days=1825)).strftime('%Y-%m-%d')

        # Try cache first
        df = None
        if use_cache:
            # For S&P 500, we'll use 'sp500' as data_type in cache check
            # Update _get_cached_series to handle this
            try:
                if self.db:
                    cached_df = self.db.get_macro_data('sp500', series_id, start_date=start_date_str)
                    if not cached_df.empty:
                        latest_date = pd.to_datetime(cached_df['date'].max())
                        cache_age = datetime.now() - latest_date.replace(tzinfo=None)
                        if cache_age.total_seconds() / 3600 <= self.cache_hours:
                            logger.info(f"Using cached S&P 500 data (age: {cache_age.total_seconds()/3600:.1f}h)")
                            df = cached_df
            except Exception as e:
                logger.error(f"Error reading S&P 500 cache: {e}")

        # Cache miss - fetch from Yahoo Finance
        if df is None:
            try:
                sp500 = yf.Ticker(series_id)
                end_date = datetime.now()
                start_date = end_date - timedelta(days=1825)

                hist = sp500.history(start=start_date, end=end_date)

                if hist.empty:
                    logger.warning("No S&P 500 data available from Yahoo Finance")
                    return {'name': 'S&P 500', 'code': 'SPX', **{p: None for p in timeframes.keys()}}

                # Convert to FRED-like format
                df = pd.DataFrame({
                    'date': hist.index,
                    'value': hist['Close']
                })
                df['date'] = pd.to_datetime(df['date']).dt.tz_localize(None)
                df = df.sort_values('date').reset_index(drop=True)

                logger.info(f"Fetched {len(df)} S&P 500 observations from Yahoo Finance")

                # Save to cache
                if use_cache and self.db:
                    try:
                        observations = [
                            {'date': row['date'].strftime('%Y-%m-%d'), 'value': row['value']}
                            for _, row in df.iterrows()
                        ]
                        self.db.save_macro_data('sp500', series_id, observations)
                        logger.info(f"Saved S&P 500 data to cache")
                    except Exception as e:
                        logger.error(f"Error saving S&P 500 cache: {e}")

            except Exception as e:
                logger.error(f"Error fetching S&P 500 from Yahoo Finance: {e}")
                return {'name': 'S&P 500', 'code': 'SPX', **{p: None for p in timeframes.keys()}}

        # Calculate returns
        sp500_data = {
            'name': 'S&P 500',
            'code': 'SPX',
        }

        for period, days in timeframes.items():
            return_pct = self._calculate_period_return(df, days)
            sp500_data[period] = return_pct

        return sp500_data

    def fetch_yield_curve(self) -> Dict:
        """
        Fetch current Treasury yields for yield curve visualization

        Returns:
            Dict with maturity: yield pairs
        """
        yields = {}

        # Use 90 days lookback to ensure we get recent data even with weekends/holidays
        for maturity, series_id in self.TREASURY_SERIES.items():
            df = self._fetch_series(series_id, start_date=(datetime.now() - timedelta(days=90)).strftime('%Y-%m-%d'))

            if not df.empty:
                yields[maturity] = df.iloc[-1]['value']
                logger.info(f"Fetched {maturity} yield: {yields[maturity]}% (date: {df.iloc[-1]['date']})")
            else:
                logger.warning(f"No data available for {maturity} Treasury (series {series_id})")
                yields[maturity] = None

        return yields

    def calculate_yield_spreads(self) -> Dict:
        """
        Calculate key yield spreads with historical context and trend analysis

        Returns:
            Dict with spread calculations, historical values, and trend indicators
        """
        # Fetch data for key maturities
        lookback_days = 1095  # 3 years for context

        yields_10y = self._fetch_series(self.TREASURY_SERIES['10Y'],
                                        start_date=(datetime.now() - timedelta(days=lookback_days)).strftime('%Y-%m-%d'))
        yields_2y = self._fetch_series(self.TREASURY_SERIES['2Y'],
                                       start_date=(datetime.now() - timedelta(days=lookback_days)).strftime('%Y-%m-%d'))
        yields_3m = self._fetch_series(self.TREASURY_SERIES['3M'],
                                       start_date=(datetime.now() - timedelta(days=lookback_days)).strftime('%Y-%m-%d'))
        yields_30y = self._fetch_series(self.TREASURY_SERIES['30Y'],
                                        start_date=(datetime.now() - timedelta(days=lookback_days)).strftime('%Y-%m-%d'))
        yields_5y = self._fetch_series(self.TREASURY_SERIES['5Y'],
                                       start_date=(datetime.now() - timedelta(days=lookback_days)).strftime('%Y-%m-%d'))

        spreads = {}

        # Helper function to calculate spread at a specific lookback period
        def get_historical_spread(df1, df2, days_back):
            """Get spread value from days_back ago"""
            if df1.empty or df2.empty:
                return None

            target_date = df1.iloc[-1]['date'] - timedelta(days=days_back)
            hist_df1 = df1[df1['date'] <= target_date]
            hist_df2 = df2[df2['date'] <= target_date]

            if not hist_df1.empty and not hist_df2.empty:
                return hist_df1.iloc[-1]['value'] - hist_df2.iloc[-1]['value']
            return None

        # 10Y-2Y spread
        if not yields_10y.empty and not yields_2y.empty:
            current_10y = yields_10y.iloc[-1]['value']
            current_2y = yields_2y.iloc[-1]['value']
            spread_current = current_10y - current_2y

            # Historical spreads
            spread_1m = get_historical_spread(yields_10y, yields_2y, 30)
            spread_3m = get_historical_spread(yields_10y, yields_2y, 90)
            spread_6m = get_historical_spread(yields_10y, yields_2y, 180)
            spread_1y = get_historical_spread(yields_10y, yields_2y, 365)

            # Calculate changes
            change_1m = (spread_current - spread_1m) if spread_1m is not None else None
            change_3m = (spread_current - spread_3m) if spread_3m is not None else None
            change_6m = (spread_current - spread_6m) if spread_6m is not None else None
            change_1y = (spread_current - spread_1y) if spread_1y is not None else None

            # Determine trend (expanding = widening, contracting = narrowing)
            trend = None
            if change_3m is not None:
                if change_3m > 0.1:
                    trend = 'EXPANDING'
                elif change_3m < -0.1:
                    trend = 'CONTRACTING'
                else:
                    trend = 'STABLE'

            spreads['10y2y'] = {
                'current': round(spread_current, 2),
                '1m_ago': round(spread_1m, 2) if spread_1m is not None else None,
                '3m_ago': round(spread_3m, 2) if spread_3m is not None else None,
                '6m_ago': round(spread_6m, 2) if spread_6m is not None else None,
                '1y_ago': round(spread_1y, 2) if spread_1y is not None else None,
                'change_1m': round(change_1m, 2) if change_1m is not None else None,
                'change_3m': round(change_3m, 2) if change_3m is not None else None,
                'change_6m': round(change_6m, 2) if change_6m is not None else None,
                'change_1y': round(change_1y, 2) if change_1y is not None else None,
                'trend': trend,
            }

        # 10Y-3M spread
        if not yields_10y.empty and not yields_3m.empty:
            current_10y = yields_10y.iloc[-1]['value']
            current_3m = yields_3m.iloc[-1]['value']
            spread_current = current_10y - current_3m

            spread_1m = get_historical_spread(yields_10y, yields_3m, 30)
            spread_3m = get_historical_spread(yields_10y, yields_3m, 90)
            spread_6m = get_historical_spread(yields_10y, yields_3m, 180)
            spread_1y = get_historical_spread(yields_10y, yields_3m, 365)

            change_1m = (spread_current - spread_1m) if spread_1m is not None else None
            change_3m = (spread_current - spread_3m) if spread_3m is not None else None
            change_6m = (spread_current - spread_6m) if spread_6m is not None else None
            change_1y = (spread_current - spread_1y) if spread_1y is not None else None

            trend = None
            if change_3m is not None:
                if change_3m > 0.1:
                    trend = 'EXPANDING'
                elif change_3m < -0.1:
                    trend = 'CONTRACTING'
                else:
                    trend = 'STABLE'

            spreads['10y3m'] = {
                'current': round(spread_current, 2),
                '1m_ago': round(spread_1m, 2) if spread_1m is not None else None,
                '3m_ago': round(spread_3m, 2) if spread_3m is not None else None,
                '6m_ago': round(spread_6m, 2) if spread_6m is not None else None,
                '1y_ago': round(spread_1y, 2) if spread_1y is not None else None,
                'change_1m': round(change_1m, 2) if change_1m is not None else None,
                'change_3m': round(change_3m, 2) if change_3m is not None else None,
                'change_6m': round(change_6m, 2) if change_6m is not None else None,
                'change_1y': round(change_1y, 2) if change_1y is not None else None,
                'trend': trend,
            }

        # 30Y-5Y spread
        if not yields_30y.empty and not yields_5y.empty:
            current_30y = yields_30y.iloc[-1]['value']
            current_5y = yields_5y.iloc[-1]['value']
            spread_current = current_30y - current_5y

            spread_1m = get_historical_spread(yields_30y, yields_5y, 30)
            spread_3m = get_historical_spread(yields_30y, yields_5y, 90)
            spread_6m = get_historical_spread(yields_30y, yields_5y, 180)
            spread_1y = get_historical_spread(yields_30y, yields_5y, 365)

            change_1m = (spread_current - spread_1m) if spread_1m is not None else None
            change_3m = (spread_current - spread_3m) if spread_3m is not None else None
            change_6m = (spread_current - spread_6m) if spread_6m is not None else None
            change_1y = (spread_current - spread_1y) if spread_1y is not None else None

            trend = None
            if change_3m is not None:
                if change_3m > 0.05:
                    trend = 'EXPANDING'
                elif change_3m < -0.05:
                    trend = 'CONTRACTING'
                else:
                    trend = 'STABLE'

            spreads['30y5y'] = {
                'current': round(spread_current, 2),
                '1m_ago': round(spread_1m, 2) if spread_1m is not None else None,
                '3m_ago': round(spread_3m, 2) if spread_3m is not None else None,
                '6m_ago': round(spread_6m, 2) if spread_6m is not None else None,
                '1y_ago': round(spread_1y, 2) if spread_1y is not None else None,
                'change_1m': round(change_1m, 2) if change_1m is not None else None,
                'change_3m': round(change_3m, 2) if change_3m is not None else None,
                'change_6m': round(change_6m, 2) if change_6m is not None else None,
                'change_1y': round(change_1y, 2) if change_1y is not None else None,
                'trend': trend,
            }

        return spreads

    def get_spread_history(self, lookback_days: int = 365, sample_interval: int = None) -> Dict:
        """
        Get historical spread data for charting

        Args:
            lookback_days: Days of history to fetch (default 1 year)
            sample_interval: Sample every N days to reduce data points (default: auto-calculated)
                            If None, will auto-calculate based on lookback to keep ~500 points

        Returns:
            Dict with dates and spread values for each spread type
        """
        # Auto-calculate sample interval if not provided
        # Target ~500 data points for good chart performance
        if sample_interval is None:
            # ~252 trading days per year, so lookback_days / 252 = years
            # For 1 year: daily (1), for 20 years: weekly (7)
            if lookback_days <= 365:
                sample_interval = 1  # Daily for 1 year or less
            elif lookback_days <= 730:
                sample_interval = 2  # Every other day for 2 years
            elif lookback_days <= 1825:
                sample_interval = 5  # ~Weekly for up to 5 years
            else:
                sample_interval = 7  # Weekly for longer periods

        start_date = (datetime.now() - timedelta(days=lookback_days)).strftime('%Y-%m-%d')

        yields_10y = self._fetch_series(self.TREASURY_SERIES['10Y'], start_date=start_date)
        yields_2y = self._fetch_series(self.TREASURY_SERIES['2Y'], start_date=start_date)
        yields_3m = self._fetch_series(self.TREASURY_SERIES['3M'], start_date=start_date)
        yields_30y = self._fetch_series(self.TREASURY_SERIES['30Y'], start_date=start_date)
        yields_5y = self._fetch_series(self.TREASURY_SERIES['5Y'], start_date=start_date)

        history = {
            'dates': [],
            '10y2y': [],
            '10y3m': [],
            '30y5y': []
        }

        # Merge all dataframes on date to ensure consistent timeline
        if not yields_10y.empty and not yields_2y.empty:
            merged = pd.merge(yields_10y, yields_2y, on='date', suffixes=('_10y', '_2y'))

            # Sample data at specified interval
            sampled = merged.iloc[::sample_interval].copy()

            for _, row in sampled.iterrows():
                date_str = row['date'].strftime('%Y-%m-%d')
                if date_str not in history['dates']:
                    history['dates'].append(date_str)

                spread_10y2y = row['value_10y'] - row['value_2y']
                history['10y2y'].append(round(spread_10y2y, 2))

        # 10Y-3M spread
        if not yields_10y.empty and not yields_3m.empty:
            merged = pd.merge(yields_10y, yields_3m, on='date', suffixes=('_10y', '_3m'))
            sampled = merged.iloc[::sample_interval].copy()

            # Reset if we're starting fresh
            if not history['dates']:
                for _, row in sampled.iterrows():
                    history['dates'].append(row['date'].strftime('%Y-%m-%d'))

            for _, row in sampled.iterrows():
                spread_10y3m = row['value_10y'] - row['value_3m']
                history['10y3m'].append(round(spread_10y3m, 2))

        # 30Y-5Y spread
        if not yields_30y.empty and not yields_5y.empty:
            merged = pd.merge(yields_30y, yields_5y, on='date', suffixes=('_30y', '_5y'))
            sampled = merged.iloc[::sample_interval].copy()

            # Reset if we're starting fresh
            if not history['dates']:
                for _, row in sampled.iterrows():
                    history['dates'].append(row['date'].strftime('%Y-%m-%d'))

            for _, row in sampled.iterrows():
                spread_30y5y = row['value_30y'] - row['value_5y']
                history['30y5y'].append(round(spread_30y5y, 2))

        return history

    def fetch_credit_spreads(self) -> Dict:
        """
        Fetch corporate credit spreads with historical context

        Returns:
            Dict with current spreads and percentile rankings
        """
        lookback_days = 3650  # 10 years for percentile calc

        spreads = {}

        for spread_type, series_id in self.CREDIT_SPREAD_SERIES.items():
            df = self._fetch_series(series_id,
                                   start_date=(datetime.now() - timedelta(days=lookback_days)).strftime('%Y-%m-%d'))

            if not df.empty:
                current_value = df.iloc[-1]['value']

                # Calculate percentile
                percentile = (df['value'] <= current_value).sum() / len(df) * 100

                spreads[spread_type] = {
                    'current': round(current_value, 0),
                    'percentile': round(percentile, 1),
                }
            else:
                spreads[spread_type] = {
                    'current': None,
                    'percentile': None,
                }

        return spreads

    def get_last_update_date(self, series_id: str) -> Optional[str]:
        """
        Get the last update date for a series

        Args:
            series_id: FRED series ID

        Returns:
            Last update date as string or None
        """
        df = self._fetch_series(series_id, start_date=(datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d'))

        if not df.empty:
            return df.iloc[-1]['date'].strftime('%Y-%m-%d')
        return None

    # =========================================================================
    # GLOBAL ECONOMY INDICATORS
    # =========================================================================

    def fetch_buffett_indicator(self, lookback_years: int = 25) -> Dict:
        """
        Calculate the Buffett Indicator (Total Market Cap / GDP)

        Warren Buffett called this "probably the best single measure of
        where valuations stand at any given moment."

        Calculates by fetching:
        - Wilshire 5000 Total Market Index from Yahoo Finance (^W5000)
        - GDP from FRED

        The Wilshire 5000 index value represents approximately $1 billion per point
        (this approximation has evolved over time but remains useful for the ratio).

        Args:
            lookback_years: Years of history to fetch

        Returns:
            Dict with current value, historical series, percentile, and interpretation
        """
        lookback_days = lookback_years * 365
        start_date = (datetime.now() - timedelta(days=lookback_days)).strftime('%Y-%m-%d')

        try:
            # Fetch Wilshire 5000 from Yahoo Finance (FRED removed this data in June 2024)
            wilshire = yf.Ticker("^W5000")
            wilshire_df = wilshire.history(period=f"{lookback_years}y", interval="1mo")

            if wilshire_df.empty:
                logger.warning("Could not fetch Wilshire 5000 data from Yahoo Finance")
                return {
                    'current': None,
                    'percentile': None,
                    'history': {'dates': [], 'values': []}
                }

            # Process Wilshire data - use Close prices
            wilshire_df = wilshire_df.reset_index()
            wilshire_df = wilshire_df[['Date', 'Close']].copy()
            wilshire_df.columns = ['date', 'wilshire']
            wilshire_df['date'] = pd.to_datetime(wilshire_df['date']).dt.tz_localize(None)

            # Fetch GDP from FRED (quarterly, in billions)
            gdp_df = self._fetch_series(
                self.GLOBAL_ECONOMY_SERIES['gdp'],
                start_date=start_date
            )

            if gdp_df.empty:
                logger.warning("Could not fetch GDP data from FRED")
                return {
                    'current': None,
                    'percentile': None,
                    'history': {'dates': [], 'values': []}
                }

            # Forward-fill GDP to monthly frequency to match Wilshire
            gdp_df = gdp_df.set_index('date')
            gdp_monthly = gdp_df.reindex(wilshire_df['date'], method='ffill').reset_index()
            gdp_monthly.columns = ['date', 'gdp']

            # Merge datasets
            merged = pd.merge(wilshire_df, gdp_monthly, on='date', how='inner')
            merged = merged.dropna()

            if merged.empty:
                logger.warning("No overlapping data between Wilshire and GDP")
                return {
                    'current': None,
                    'percentile': None,
                    'history': {'dates': [], 'values': []}
                }

            # Calculate Buffett Indicator
            # Wilshire 5000 index value ≈ total market cap in billions (roughly 1:1)
            # GDP is in billions, so ratio * 100 gives percentage
            merged['buffett'] = (merged['wilshire'] / merged['gdp']) * 100

            # Get current value
            current_value = merged.iloc[-1]['buffett']

            # Calculate percentile
            percentile = (merged['buffett'] <= current_value).sum() / len(merged) * 100

            # Prepare history for charting
            history = {
                'dates': merged['date'].dt.strftime('%Y-%m-%d').tolist(),
                'values': merged['buffett'].round(1).tolist()
            }

            return {
                'current': round(current_value, 1),
                'percentile': round(percentile, 1),
                'history': history
            }

        except Exception as e:
            logger.error(f"Error calculating Buffett Indicator: {e}")
            return {
                'current': None,
                'percentile': None,
                'history': {'dates': [], 'values': []}
            }

    def fetch_m2_gdp_ratio(self, lookback_years: int = 25) -> Dict:
        """
        Fetch and calculate M2 Money Supply to GDP ratio

        This ratio indicates liquidity in the economy. A rising ratio means
        more money is chasing the same economic output, which can lead to
        asset inflation.

        Args:
            lookback_years: Years of history to fetch

        Returns:
            Dict with current value, historical series, and year-over-year change
        """
        lookback_days = lookback_years * 365
        start_date = (datetime.now() - timedelta(days=lookback_days)).strftime('%Y-%m-%d')

        # Fetch M2 Money Supply (monthly, in billions)
        m2_df = self._fetch_series(
            self.GLOBAL_ECONOMY_SERIES['m2'],
            start_date=start_date
        )

        # Fetch GDP (quarterly, in billions)
        gdp_df = self._fetch_series(
            self.GLOBAL_ECONOMY_SERIES['gdp'],
            start_date=start_date
        )

        if m2_df.empty or gdp_df.empty:
            logger.warning("Could not fetch M2/GDP ratio data")
            return {
                'current': None,
                'yoy_change': None,
                'history': {'dates': [], 'values': []}
            }

        # Forward-fill GDP to monthly frequency
        gdp_df = gdp_df.set_index('date')
        gdp_monthly = gdp_df.reindex(m2_df['date']).ffill().reset_index()
        gdp_monthly.columns = ['date', 'gdp']

        # Merge and calculate ratio
        merged = pd.merge(m2_df, gdp_monthly, on='date')
        merged['ratio'] = (merged['value'] / merged['gdp']) * 100

        # Get current value
        current_value = merged.iloc[-1]['ratio'] if not merged.empty else None

        # Calculate YoY change
        yoy_change = None
        if len(merged) > 12:
            year_ago_value = merged.iloc[-13]['ratio'] if len(merged) >= 13 else None
            if year_ago_value:
                yoy_change = current_value - year_ago_value

        # Prepare history for charting
        history = {
            'dates': merged['date'].dt.strftime('%Y-%m-%d').tolist(),
            'values': merged['ratio'].round(1).tolist()
        }

        return {
            'current': round(current_value, 1) if current_value else None,
            'yoy_change': round(yoy_change, 1) if yoy_change else None,
            'history': history
        }

    def fetch_debt_to_gdp(self, lookback_years: int = 25) -> Dict:
        """
        Fetch Federal Debt to GDP ratio

        This shows the government's debt burden relative to economic output.
        High levels (>100%) can lead to concerns about debt sustainability.

        Args:
            lookback_years: Years of history to fetch

        Returns:
            Dict with current value, historical series, and key historical comparisons
        """
        lookback_days = lookback_years * 365
        start_date = (datetime.now() - timedelta(days=lookback_days)).strftime('%Y-%m-%d')

        # Fetch total public debt as % of GDP (already calculated by FRED)
        debt_df = self._fetch_series(
            self.GLOBAL_ECONOMY_SERIES['debt_gdp'],
            start_date=start_date
        )

        if debt_df.empty:
            logger.warning("Could not fetch Debt/GDP ratio data")
            return {
                'current': None,
                'history': {'dates': [], 'values': []},
                'historical_comparison': {}
            }

        # Get current value
        current_value = debt_df.iloc[-1]['value'] if not debt_df.empty else None

        # Historical comparisons
        historical_comparison = {}
        for years_back, label in [(5, '5y_ago'), (10, '10y_ago'), (20, '20y_ago')]:
            target_date = datetime.now() - timedelta(days=years_back * 365)
            past_data = debt_df[debt_df['date'] <= target_date]
            if not past_data.empty:
                historical_comparison[label] = round(past_data.iloc[-1]['value'], 1)

        # Prepare history for charting
        history = {
            'dates': debt_df['date'].dt.strftime('%Y-%m-%d').tolist(),
            'values': debt_df['value'].round(1).tolist()
        }

        return {
            'current': round(current_value, 1) if current_value else None,
            'history': history,
            'historical_comparison': historical_comparison
        }

    def fetch_m2_velocity(self, lookback_years: int = 25) -> Dict:
        """
        Fetch M2 Money Velocity (GDP / M2)

        Money velocity shows how quickly money circulates in the economy.
        Low velocity means money is being held rather than spent, which can
        indicate economic weakness or increased saving.

        Args:
            lookback_years: Years of history to fetch

        Returns:
            Dict with current value, historical series, and trend
        """
        lookback_days = lookback_years * 365
        start_date = (datetime.now() - timedelta(days=lookback_days)).strftime('%Y-%m-%d')

        # Fetch M2 Velocity (already calculated by FRED as GDP/M2)
        velocity_df = self._fetch_series(
            self.GLOBAL_ECONOMY_SERIES['m2_velocity'],
            start_date=start_date
        )

        if velocity_df.empty:
            logger.warning("Could not fetch M2 Velocity data")
            return {
                'current': None,
                'history': {'dates': [], 'values': []},
                'historical_avg': None
            }

        # Get current value
        current_value = velocity_df.iloc[-1]['value'] if not velocity_df.empty else None

        # Calculate historical average
        historical_avg = velocity_df['value'].mean() if not velocity_df.empty else None

        # Prepare history for charting
        history = {
            'dates': velocity_df['date'].dt.strftime('%Y-%m-%d').tolist(),
            'values': velocity_df['value'].round(2).tolist()
        }

        return {
            'current': round(current_value, 2) if current_value else None,
            'history': history,
            'historical_avg': round(historical_avg, 2) if historical_avg else None
        }

    # =========================================================================
    # REAL ESTATE INDICATORS
    # =========================================================================

    def fetch_case_shiller(self, lookback_years: int = 25) -> Dict:
        """
        Fetch Case-Shiller Home Price Index data

        The S&P/Case-Shiller Index measures changes in the value of residential
        real estate using repeat-sales methodology.

        Args:
            lookback_years: Years of history to fetch

        Returns:
            Dict with national and 20-city indices, historical series, and YoY change
        """
        lookback_days = lookback_years * 365
        start_date = (datetime.now() - timedelta(days=lookback_days)).strftime('%Y-%m-%d')

        # Fetch national index
        national_df = self._fetch_series(
            self.REAL_ESTATE_SERIES['case_shiller_national'],
            start_date=start_date
        )

        # Fetch 20-city composite
        city20_df = self._fetch_series(
            self.REAL_ESTATE_SERIES['case_shiller_20city'],
            start_date=start_date
        )

        result = {
            'national': {'current': None, 'yoy_change': None, 'history': {'dates': [], 'values': []}},
            'city20': {'current': None, 'yoy_change': None, 'history': {'dates': [], 'values': []}}
        }

        # Process national index
        if not national_df.empty:
            current = national_df.iloc[-1]['value']
            result['national']['current'] = round(current, 1)

            # YoY change
            if len(national_df) >= 13:
                year_ago = national_df.iloc[-13]['value']
                result['national']['yoy_change'] = round(((current - year_ago) / year_ago) * 100, 1)

            result['national']['history'] = {
                'dates': national_df['date'].dt.strftime('%Y-%m-%d').tolist(),
                'values': national_df['value'].round(1).tolist()
            }

        # Process 20-city index
        if not city20_df.empty:
            current = city20_df.iloc[-1]['value']
            result['city20']['current'] = round(current, 1)

            # YoY change
            if len(city20_df) >= 13:
                year_ago = city20_df.iloc[-13]['value']
                result['city20']['yoy_change'] = round(((current - year_ago) / year_ago) * 100, 1)

            result['city20']['history'] = {
                'dates': city20_df['date'].dt.strftime('%Y-%m-%d').tolist(),
                'values': city20_df['value'].round(1).tolist()
            }

        return result

    def fetch_housing_supply(self) -> Dict:
        """
        Fetch housing inventory and months supply data

        Months supply indicates how long it would take to sell all homes
        at the current sales pace. 4-6 months is considered balanced.

        Returns:
            Dict with inventory, months supply, and trends
        """
        lookback_days = 10 * 365  # 10 years
        start_date = (datetime.now() - timedelta(days=lookback_days)).strftime('%Y-%m-%d')

        # Fetch months supply of existing homes
        months_supply_df = self._fetch_series(
            self.REAL_ESTATE_SERIES['months_supply'],
            start_date=start_date
        )

        # Fetch housing inventory
        inventory_df = self._fetch_series(
            self.REAL_ESTATE_SERIES['housing_inventory'],
            start_date=start_date
        )

        # Fetch new home months supply
        new_months_df = self._fetch_series(
            self.REAL_ESTATE_SERIES['new_home_months_supply'],
            start_date=start_date
        )

        result = {
            'existing_months_supply': None,
            'new_home_months_supply': None,
            'inventory': None,
            'months_supply_history': {'dates': [], 'values': []},
            'inventory_history': {'dates': [], 'values': []}
        }

        # Use existing home months supply for current value if available
        if not months_supply_df.empty:
            result['existing_months_supply'] = round(months_supply_df.iloc[-1]['value'], 1)

        if not inventory_df.empty:
            # Inventory is in thousands
            result['inventory'] = round(inventory_df.iloc[-1]['value'], 0)
            result['inventory_history'] = {
                'dates': inventory_df['date'].dt.strftime('%Y-%m-%d').tolist(),
                'values': inventory_df['value'].round(0).tolist()
            }

        if not new_months_df.empty:
            result['new_home_months_supply'] = round(new_months_df.iloc[-1]['value'], 1)
            # Use new home months supply for chart since it has full historical data
            # NAR existing home data is limited to 13 months on FRED
            result['months_supply_history'] = {
                'dates': new_months_df['date'].dt.strftime('%Y-%m-%d').tolist(),
                'values': new_months_df['value'].round(1).tolist()
            }

        return result

    def fetch_housing_activity(self, lookback_years: int = 15) -> Dict:
        """
        Fetch housing starts, permits, and sales data

        These are leading indicators of housing market activity and
        overall economic health.

        Args:
            lookback_years: Years of history to fetch

        Returns:
            Dict with current values and historical series
        """
        lookback_days = lookback_years * 365
        start_date = (datetime.now() - timedelta(days=lookback_days)).strftime('%Y-%m-%d')

        # Fetch housing starts (thousands of units, SAAR)
        starts_df = self._fetch_series(
            self.REAL_ESTATE_SERIES['housing_starts'],
            start_date=start_date
        )

        # Fetch building permits
        permits_df = self._fetch_series(
            self.REAL_ESTATE_SERIES['building_permits'],
            start_date=start_date
        )

        # Fetch existing home sales
        sales_df = self._fetch_series(
            self.REAL_ESTATE_SERIES['existing_home_sales'],
            start_date=start_date
        )

        result = {
            'housing_starts': {'current': None, 'yoy_change': None, 'history': {'dates': [], 'values': []}},
            'building_permits': {'current': None, 'yoy_change': None, 'history': {'dates': [], 'values': []}},
            'existing_sales': {'current': None, 'yoy_change': None, 'history': {'dates': [], 'values': []}}
        }

        # Process housing starts
        if not starts_df.empty:
            current = starts_df.iloc[-1]['value']
            result['housing_starts']['current'] = round(current, 0)

            if len(starts_df) >= 13:
                year_ago = starts_df.iloc[-13]['value']
                result['housing_starts']['yoy_change'] = round(((current - year_ago) / year_ago) * 100, 1)

            result['housing_starts']['history'] = {
                'dates': starts_df['date'].dt.strftime('%Y-%m-%d').tolist(),
                'values': starts_df['value'].round(0).tolist()
            }

        # Process building permits
        if not permits_df.empty:
            current = permits_df.iloc[-1]['value']
            result['building_permits']['current'] = round(current, 0)

            if len(permits_df) >= 13:
                year_ago = permits_df.iloc[-13]['value']
                result['building_permits']['yoy_change'] = round(((current - year_ago) / year_ago) * 100, 1)

            result['building_permits']['history'] = {
                'dates': permits_df['date'].dt.strftime('%Y-%m-%d').tolist(),
                'values': permits_df['value'].round(0).tolist()
            }

        # Process existing home sales
        if not sales_df.empty:
            current = sales_df.iloc[-1]['value']
            result['existing_sales']['current'] = round(current, 0)

            if len(sales_df) >= 13:
                year_ago = sales_df.iloc[-13]['value']
                result['existing_sales']['yoy_change'] = round(((current - year_ago) / year_ago) * 100, 1)

            result['existing_sales']['history'] = {
                'dates': sales_df['date'].dt.strftime('%Y-%m-%d').tolist(),
                'values': sales_df['value'].round(0).tolist()
            }

        return result

    def fetch_mortgage_rates(self, lookback_years: int = 15) -> Dict:
        """
        Fetch 30-year mortgage rate data

        Mortgage rates directly impact housing affordability and demand.

        Args:
            lookback_years: Years of history to fetch

        Returns:
            Dict with current rate, historical series, and comparisons
        """
        lookback_days = lookback_years * 365
        start_date = (datetime.now() - timedelta(days=lookback_days)).strftime('%Y-%m-%d')

        mortgage_df = self._fetch_series(
            self.REAL_ESTATE_SERIES['mortgage_30y'],
            start_date=start_date
        )

        if mortgage_df.empty:
            return {
                'current': None,
                'history': {'dates': [], 'values': []},
                'historical_avg': None,
                'yoy_change': None
            }

        current = mortgage_df.iloc[-1]['value']

        # Calculate YoY change
        yoy_change = None
        if len(mortgage_df) >= 53:  # Weekly data
            year_ago = mortgage_df.iloc[-53]['value']
            yoy_change = current - year_ago

        # Historical average
        historical_avg = mortgage_df['value'].mean()

        return {
            'current': round(current, 2),
            'history': {
                'dates': mortgage_df['date'].dt.strftime('%Y-%m-%d').tolist(),
                'values': mortgage_df['value'].round(2).tolist()
            },
            'historical_avg': round(historical_avg, 2),
            'yoy_change': round(yoy_change, 2) if yoy_change else None
        }

    def fetch_housing_affordability(self, lookback_years: int = 15) -> Dict:
        """
        Fetch Housing Affordability Index

        An index value of 100 means a median-income family has exactly enough
        income to qualify for a mortgage on a median-priced home. Higher values
        indicate greater affordability.

        Args:
            lookback_years: Years of history to fetch

        Returns:
            Dict with current value, historical series, and interpretation
        """
        lookback_days = lookback_years * 365
        start_date = (datetime.now() - timedelta(days=lookback_days)).strftime('%Y-%m-%d')

        affordability_df = self._fetch_series(
            self.REAL_ESTATE_SERIES['affordability_index'],
            start_date=start_date
        )

        if affordability_df.empty:
            return {
                'current': None,
                'history': {'dates': [], 'values': []},
                'historical_avg': None,
                'percentile': None
            }

        current = affordability_df.iloc[-1]['value']

        # Historical average
        historical_avg = affordability_df['value'].mean()

        # Percentile (lower percentile = less affordable historically)
        percentile = (affordability_df['value'] >= current).sum() / len(affordability_df) * 100

        return {
            'current': round(current, 1),
            'history': {
                'dates': affordability_df['date'].dt.strftime('%Y-%m-%d').tolist(),
                'values': affordability_df['value'].round(1).tolist()
            },
            'historical_avg': round(historical_avg, 1),
            'percentile': round(percentile, 1)
        }

    def fetch_median_home_price(self, lookback_years: int = 25) -> Dict:
        """
        Fetch median home sales price

        Args:
            lookback_years: Years of history to fetch

        Returns:
            Dict with current price, historical series, and YoY change
        """
        lookback_days = lookback_years * 365
        start_date = (datetime.now() - timedelta(days=lookback_days)).strftime('%Y-%m-%d')

        price_df = self._fetch_series(
            self.REAL_ESTATE_SERIES['median_home_price'],
            start_date=start_date
        )

        if price_df.empty:
            return {
                'current': None,
                'history': {'dates': [], 'values': []},
                'yoy_change': None
            }

        current = price_df.iloc[-1]['value']

        # YoY change (quarterly data)
        yoy_change = None
        if len(price_df) >= 5:
            year_ago = price_df.iloc[-5]['value']
            yoy_change = ((current - year_ago) / year_ago) * 100

        return {
            'current': round(current, 0),
            'history': {
                'dates': price_df['date'].dt.strftime('%Y-%m-%d').tolist(),
                'values': price_df['value'].round(0).tolist()
            },
            'yoy_change': round(yoy_change, 1) if yoy_change else None
        }

    def fetch_mortgage_debt_service(self, lookback_years: int = 20) -> Dict:
        """
        Fetch Mortgage Debt Service Payments as % of Disposable Personal Income

        This metric shows what percentage of household income goes to mortgage payments.
        Lower values indicate more affordable housing relative to income.

        Args:
            lookback_years: Years of history to fetch

        Returns:
            Dict with current value, historical series, and comparisons
        """
        lookback_days = lookback_years * 365
        start_date = (datetime.now() - timedelta(days=lookback_days)).strftime('%Y-%m-%d')

        mdsp_df = self._fetch_series(
            self.REAL_ESTATE_SERIES['mortgage_debt_service'],
            start_date=start_date
        )

        if mdsp_df.empty:
            logger.warning("Could not fetch Mortgage Debt Service data")
            return {
                'current': None,
                'history': {'dates': [], 'values': []},
                'historical_avg': None,
                'historical_high': None,
                'historical_low': None
            }

        current = mdsp_df.iloc[-1]['value']

        # Historical stats
        historical_avg = mdsp_df['value'].mean()
        historical_high = mdsp_df['value'].max()
        historical_low = mdsp_df['value'].min()

        return {
            'current': round(current, 2),
            'history': {
                'dates': mdsp_df['date'].dt.strftime('%Y-%m-%d').tolist(),
                'values': mdsp_df['value'].round(2).tolist()
            },
            'historical_avg': round(historical_avg, 2),
            'historical_high': round(historical_high, 2),
            'historical_low': round(historical_low, 2)
        }

    def fetch_price_to_income_ratio(self, lookback_years: int = 25) -> Dict:
        """
        Calculate Home Price to Income Ratio

        This is a simple affordability metric: Median Home Price / Median Household Income.
        Higher values indicate less affordable housing.

        Note: Income data is annual, so the ratio is calculated at annual frequency.

        Args:
            lookback_years: Years of history to fetch

        Returns:
            Dict with current ratio, historical series, and comparisons
        """
        lookback_days = lookback_years * 365
        start_date = (datetime.now() - timedelta(days=lookback_days)).strftime('%Y-%m-%d')

        # Fetch median home price (quarterly)
        price_df = self._fetch_series(
            self.REAL_ESTATE_SERIES['median_home_price'],
            start_date=start_date
        )

        # Fetch median household income (annual)
        income_df = self._fetch_series(
            self.REAL_ESTATE_SERIES['median_income'],
            start_date=start_date
        )

        if price_df.empty or income_df.empty:
            logger.warning("Could not fetch Price-to-Income ratio data")
            return {
                'current': None,
                'history': {'dates': [], 'values': []},
                'historical_avg': None
            }

        # Convert price to annual (use Q4 of each year for consistency)
        price_df['year'] = price_df['date'].dt.year
        annual_prices = price_df.groupby('year')['value'].last().reset_index()
        annual_prices.columns = ['year', 'price']

        # Prepare income data
        income_df['year'] = income_df['date'].dt.year
        income_annual = income_df[['year', 'value']].copy()
        income_annual.columns = ['year', 'income']

        # Merge and calculate ratio
        merged = pd.merge(annual_prices, income_annual, on='year')
        merged['ratio'] = merged['price'] / merged['income']

        if merged.empty:
            return {
                'current': None,
                'history': {'dates': [], 'values': []},
                'historical_avg': None
            }

        current = merged.iloc[-1]['ratio']
        historical_avg = merged['ratio'].mean()

        return {
            'current': round(current, 2),
            'history': {
                'dates': [f"{int(y)}-01-01" for y in merged['year'].tolist()],
                'values': merged['ratio'].round(2).tolist()
            },
            'historical_avg': round(historical_avg, 2)
        }

    # =========================================================================
    # INFLATION & FED POLICY INDICATORS
    # =========================================================================

    def _calculate_yoy_change(self, df: pd.DataFrame, periods_back: int = 12) -> Optional[float]:
        """
        Calculate year-over-year percentage change for index data

        Args:
            df: DataFrame with date and value columns
            periods_back: Number of periods to look back (12 for monthly, 4 for quarterly)

        Returns:
            YoY percentage change or None
        """
        if df.empty or len(df) < periods_back + 1:
            return None

        current = df.iloc[-1]['value']
        year_ago = df.iloc[-(periods_back + 1)]['value']

        if year_ago == 0 or pd.isna(year_ago):
            return None

        return ((current - year_ago) / year_ago) * 100

    def fetch_inflation_data(self, lookback_years: int = 10) -> Dict:
        """
        Fetch all inflation metrics: CPI, Core CPI, PCE, Core PCE

        Returns YoY changes (inflation rates) for each metric along with
        historical series for charting.

        Args:
            lookback_years: Years of history to fetch

        Returns:
            Dict with current inflation rates, historical series, and trends
        """
        lookback_days = lookback_years * 365
        start_date = (datetime.now() - timedelta(days=lookback_days)).strftime('%Y-%m-%d')

        result = {
            'cpi': {'current': None, 'previous': None, 'history': {'dates': [], 'values': []}},
            'core_cpi': {'current': None, 'previous': None, 'history': {'dates': [], 'values': []}},
            'pce': {'current': None, 'previous': None, 'history': {'dates': [], 'values': []}},
            'core_pce': {'current': None, 'previous': None, 'history': {'dates': [], 'values': []}},
        }

        # Fetch each inflation series
        for metric in ['cpi', 'core_cpi', 'pce', 'core_pce']:
            df = self._fetch_series(
                self.INFLATION_SERIES[metric],
                start_date=start_date
            )

            if df.empty:
                continue

            # Calculate YoY inflation rate for each data point
            df_copy = df.copy()
            df_copy['yoy'] = df_copy['value'].pct_change(periods=12) * 100

            # Get current and previous month YoY
            valid_yoy = df_copy.dropna(subset=['yoy'])
            if not valid_yoy.empty:
                result[metric]['current'] = round(valid_yoy.iloc[-1]['yoy'], 2)
                if len(valid_yoy) >= 2:
                    result[metric]['previous'] = round(valid_yoy.iloc[-2]['yoy'], 2)

                # Historical YoY rates for charting
                result[metric]['history'] = {
                    'dates': valid_yoy['date'].dt.strftime('%Y-%m-%d').tolist(),
                    'values': valid_yoy['yoy'].round(2).tolist()
                }

        return result

    def fetch_breakeven_inflation(self, lookback_years: int = 10) -> Dict:
        """
        Fetch market inflation expectations from TIPS breakeven rates

        The breakeven rate is the difference between nominal Treasury yields
        and TIPS yields - it represents the market's inflation expectation.

        Args:
            lookback_years: Years of history to fetch

        Returns:
            Dict with 5Y and 10Y breakeven rates and history
        """
        lookback_days = lookback_years * 365
        start_date = (datetime.now() - timedelta(days=lookback_days)).strftime('%Y-%m-%d')

        result = {
            '5y': {'current': None, 'history': {'dates': [], 'values': []}},
            '10y': {'current': None, 'history': {'dates': [], 'values': []}},
        }

        # Fetch 5Y breakeven
        be5_df = self._fetch_series(
            self.INFLATION_SERIES['breakeven_5y'],
            start_date=start_date
        )
        if not be5_df.empty:
            result['5y']['current'] = round(be5_df.iloc[-1]['value'], 2)
            result['5y']['history'] = {
                'dates': be5_df['date'].dt.strftime('%Y-%m-%d').tolist(),
                'values': be5_df['value'].round(2).tolist()
            }

        # Fetch 10Y breakeven
        be10_df = self._fetch_series(
            self.INFLATION_SERIES['breakeven_10y'],
            start_date=start_date
        )
        if not be10_df.empty:
            result['10y']['current'] = round(be10_df.iloc[-1]['value'], 2)
            result['10y']['history'] = {
                'dates': be10_df['date'].dt.strftime('%Y-%m-%d').tolist(),
                'values': be10_df['value'].round(2).tolist()
            }

        return result

    def fetch_fed_funds_rate(self, lookback_years: int = 20) -> Dict:
        """
        Fetch Fed Funds target rate (upper bound)

        Args:
            lookback_years: Years of history to fetch

        Returns:
            Dict with current rate and historical series
        """
        lookback_days = lookback_years * 365
        start_date = (datetime.now() - timedelta(days=lookback_days)).strftime('%Y-%m-%d')

        df = self._fetch_series(
            self.INFLATION_SERIES['fed_funds'],
            start_date=start_date
        )

        if df.empty:
            return {
                'current': None,
                'history': {'dates': [], 'values': []},
                'yoy_change': None
            }

        current = df.iloc[-1]['value']

        # YoY change (look back ~252 trading days)
        yoy_change = None
        if len(df) >= 253:
            year_ago = df.iloc[-253]['value']
            yoy_change = current - year_ago

        return {
            'current': round(current, 2),
            'history': {
                'dates': df['date'].dt.strftime('%Y-%m-%d').tolist(),
                'values': df['value'].round(2).tolist()
            },
            'yoy_change': round(yoy_change, 2) if yoy_change is not None else None
        }

    def fetch_fed_balance_sheet(self, lookback_years: int = 20) -> Dict:
        """
        Fetch Fed Total Assets (balance sheet size)

        This shows QE (increasing) or QT (decreasing) activity.
        Values are in millions of dollars.

        Args:
            lookback_years: Years of history to fetch

        Returns:
            Dict with current size, YoY change, and historical series
        """
        lookback_days = lookback_years * 365
        start_date = (datetime.now() - timedelta(days=lookback_days)).strftime('%Y-%m-%d')

        df = self._fetch_series(
            self.INFLATION_SERIES['fed_assets'],
            start_date=start_date
        )

        if df.empty:
            return {
                'current': None,
                'current_trillions': None,
                'yoy_change_pct': None,
                'history': {'dates': [], 'values': []},
                'trend': None
            }

        current = df.iloc[-1]['value']  # In millions
        current_trillions = current / 1_000_000  # Convert to trillions

        # YoY change (weekly data, ~52 weeks back)
        yoy_change_pct = None
        trend = None
        if len(df) >= 53:
            year_ago = df.iloc[-53]['value']
            if year_ago > 0:
                yoy_change_pct = ((current - year_ago) / year_ago) * 100
                if yoy_change_pct > 5:
                    trend = 'QE'  # Quantitative Easing
                elif yoy_change_pct < -5:
                    trend = 'QT'  # Quantitative Tightening
                else:
                    trend = 'STABLE'

        # Sample weekly data to reduce chart points
        sampled = df.iloc[::4].copy()  # Every 4th point (~monthly)

        return {
            'current': round(current, 0),
            'current_trillions': round(current_trillions, 2),
            'yoy_change_pct': round(yoy_change_pct, 1) if yoy_change_pct is not None else None,
            'history': {
                'dates': sampled['date'].dt.strftime('%Y-%m-%d').tolist(),
                'values': (sampled['value'] / 1_000_000).round(2).tolist()  # In trillions
            },
            'trend': trend
        }

    def calculate_real_rate(self, lookback_years: int = 10) -> Dict:
        """
        Calculate Real Interest Rate = Fed Funds Rate - Core PCE Inflation

        The real rate indicates the true cost of borrowing after inflation.
        Negative real rates are stimulative, positive rates are restrictive.

        Args:
            lookback_years: Years of history to fetch

        Returns:
            Dict with current real rate, historical series, and interpretation
        """
        lookback_days = lookback_years * 365
        start_date = (datetime.now() - timedelta(days=lookback_days)).strftime('%Y-%m-%d')

        # Fetch Fed Funds rate
        fed_df = self._fetch_series(
            self.INFLATION_SERIES['fed_funds'],
            start_date=start_date
        )

        # Fetch Core PCE index for YoY calculation
        pce_df = self._fetch_series(
            self.INFLATION_SERIES['core_pce'],
            start_date=start_date
        )

        if fed_df.empty or pce_df.empty:
            return {
                'current': None,
                'fed_funds': None,
                'core_pce': None,
                'history': {'dates': [], 'values': []},
            }

        # Calculate Core PCE YoY
        pce_df_copy = pce_df.copy()
        pce_df_copy['yoy'] = pce_df_copy['value'].pct_change(periods=12) * 100

        # Merge on date (need to align monthly PCE with daily Fed Funds)
        # Use month-end for Fed Funds to match PCE
        fed_df['month'] = fed_df['date'].dt.to_period('M')
        fed_monthly = fed_df.groupby('month').last().reset_index()
        fed_monthly['date'] = fed_monthly['month'].dt.to_timestamp()

        pce_df_copy['month'] = pce_df_copy['date'].dt.to_period('M')
        pce_monthly = pce_df_copy.groupby('month').last().reset_index()
        pce_monthly['date'] = pce_monthly['month'].dt.to_timestamp()

        merged = pd.merge(
            fed_monthly[['date', 'value']],
            pce_monthly[['date', 'yoy']],
            on='date',
            suffixes=('_fed', '_pce')
        )

        if merged.empty:
            return {
                'current': None,
                'fed_funds': None,
                'core_pce': None,
                'history': {'dates': [], 'values': []},
            }

        # Calculate real rate
        merged['real_rate'] = merged['value'] - merged['yoy']
        merged = merged.dropna()

        if merged.empty:
            return {
                'current': None,
                'fed_funds': None,
                'core_pce': None,
                'history': {'dates': [], 'values': []},
            }

        current_real = merged.iloc[-1]['real_rate']
        current_fed = merged.iloc[-1]['value']
        current_pce = merged.iloc[-1]['yoy']

        return {
            'current': round(current_real, 2),
            'fed_funds': round(current_fed, 2),
            'core_pce': round(current_pce, 2),
            'history': {
                'dates': merged['date'].dt.strftime('%Y-%m-%d').tolist(),
                'values': merged['real_rate'].round(2).tolist()
            },
        }

    # =========================================================================
    # MARKET SENTIMENT INDICATORS
    # =========================================================================

    def fetch_vix_data(self, lookback_years: int = 5, use_cache: bool = True) -> Dict:
        """
        Fetch VIX and VIX3M data from Yahoo Finance

        VIX measures 30-day expected volatility, VIX3M measures 3-month.
        The term structure (VIX3M - VIX) indicates market stress:
        - Contango (VIX3M > VIX): Normal, complacent market
        - Backwardation (VIX < VIX3M): Stressed, fearful market

        Args:
            lookback_years: Years of history to fetch
            use_cache: Whether to use database cache

        Returns:
            Dict with VIX data, term structure, and historical series
        """
        lookback_days = lookback_years * 365
        start_date_str = (datetime.now() - timedelta(days=lookback_days)).strftime('%Y-%m-%d')

        result = {
            'vix': {'current': None, 'percentile': None, 'history': {'dates': [], 'values': []}},
            'vix3m': {'current': None, 'history': {'dates': [], 'values': []}},
            'term_structure': None,
            'term_structure_status': None,
        }

        # Fetch VIX
        vix_df = self._fetch_vix_ticker('^VIX', lookback_days, use_cache)
        if not vix_df.empty:
            current_vix = vix_df.iloc[-1]['value']
            result['vix']['current'] = round(current_vix, 2)

            # Calculate percentile over lookback period
            percentile = (vix_df['value'] <= current_vix).sum() / len(vix_df) * 100
            result['vix']['percentile'] = round(percentile, 1)

            result['vix']['history'] = {
                'dates': vix_df['date'].dt.strftime('%Y-%m-%d').tolist(),
                'values': vix_df['value'].round(2).tolist()
            }

        # Fetch VIX3M
        vix3m_df = self._fetch_vix_ticker('^VIX3M', lookback_days, use_cache)
        if not vix3m_df.empty:
            result['vix3m']['current'] = round(vix3m_df.iloc[-1]['value'], 2)
            result['vix3m']['history'] = {
                'dates': vix3m_df['date'].dt.strftime('%Y-%m-%d').tolist(),
                'values': vix3m_df['value'].round(2).tolist()
            }

        # Calculate term structure
        if result['vix']['current'] and result['vix3m']['current']:
            term_structure = result['vix3m']['current'] - result['vix']['current']
            result['term_structure'] = round(term_structure, 2)

            if term_structure > 2:
                result['term_structure_status'] = 'CONTANGO'
            elif term_structure < -2:
                result['term_structure_status'] = 'BACKWARDATION'
            else:
                result['term_structure_status'] = 'FLAT'

        return result

    def _fetch_vix_ticker(self, ticker: str, lookback_days: int, use_cache: bool = True) -> pd.DataFrame:
        """
        Fetch VIX-related ticker from Yahoo Finance with caching

        Args:
            ticker: Yahoo Finance ticker (^VIX, ^VIX3M)
            lookback_days: Days of history
            use_cache: Whether to use cache

        Returns:
            DataFrame with date and value columns
        """
        start_date_str = (datetime.now() - timedelta(days=lookback_days)).strftime('%Y-%m-%d')

        # Try cache first
        if use_cache and self.db:
            try:
                cached_df = self.db.get_macro_data('vix', ticker, start_date=start_date_str)
                if not cached_df.empty:
                    latest_date = pd.to_datetime(cached_df['date'].max())
                    cache_age = datetime.now() - latest_date.replace(tzinfo=None)
                    if cache_age.total_seconds() / 3600 <= self.cache_hours:
                        logger.info(f"Using cached {ticker} data (age: {cache_age.total_seconds()/3600:.1f}h)")
                        return cached_df
            except Exception as e:
                logger.error(f"Error reading {ticker} cache: {e}")

        # Fetch from Yahoo Finance
        try:
            vix = yf.Ticker(ticker)
            end_date = datetime.now()
            start_date = end_date - timedelta(days=lookback_days)

            hist = vix.history(start=start_date, end=end_date)

            if hist.empty:
                logger.warning(f"No {ticker} data available from Yahoo Finance")
                return pd.DataFrame(columns=['date', 'value'])

            df = pd.DataFrame({
                'date': hist.index,
                'value': hist['Close']
            })
            df['date'] = pd.to_datetime(df['date']).dt.tz_localize(None)
            df = df.sort_values('date').reset_index(drop=True)

            logger.info(f"Fetched {len(df)} {ticker} observations from Yahoo Finance")

            # Save to cache
            if use_cache and self.db:
                try:
                    observations = [
                        {'date': row['date'].strftime('%Y-%m-%d'), 'value': row['value']}
                        for _, row in df.iterrows()
                    ]
                    self.db.save_macro_data('vix', ticker, observations)
                    logger.info(f"Saved {ticker} data to cache")
                except Exception as e:
                    logger.error(f"Error saving {ticker} cache: {e}")

            return df

        except Exception as e:
            logger.error(f"Error fetching {ticker} from Yahoo Finance: {e}")
            return pd.DataFrame(columns=['date', 'value'])

    def fetch_sp500_moving_averages(self, use_cache: bool = True) -> Dict:
        """
        Fetch S&P 500 price relative to moving averages

        Shows whether the market is in an uptrend or downtrend.

        Args:
            use_cache: Whether to use cache

        Returns:
            Dict with current price, MAs, and position relative to MAs
        """
        # Get S&P 500 data (reuse existing method logic)
        series_id = '^GSPC'
        lookback_days = 400  # Need ~1 year for 200-day MA

        # Try cache first
        df = None
        if use_cache and self.db:
            try:
                cached_df = self.db.get_macro_data('sp500', series_id)
                if not cached_df.empty:
                    latest_date = pd.to_datetime(cached_df['date'].max())
                    cache_age = datetime.now() - latest_date.replace(tzinfo=None)
                    if cache_age.total_seconds() / 3600 <= self.cache_hours:
                        df = cached_df
            except Exception:
                pass

        if df is None:
            try:
                sp500 = yf.Ticker(series_id)
                hist = sp500.history(period="2y")
                if not hist.empty:
                    df = pd.DataFrame({
                        'date': hist.index,
                        'value': hist['Close']
                    })
                    df['date'] = pd.to_datetime(df['date']).dt.tz_localize(None)
                    df = df.sort_values('date').reset_index(drop=True)
            except Exception as e:
                logger.error(f"Error fetching S&P 500: {e}")
                return {
                    'current': None,
                    'ma_50': None,
                    'ma_200': None,
                    'above_50': None,
                    'above_200': None,
                    'trend': None
                }

        if df is None or df.empty or len(df) < 200:
            return {
                'current': None,
                'ma_50': None,
                'ma_200': None,
                'above_50': None,
                'above_200': None,
                'trend': None
            }

        # Calculate MAs
        current = df.iloc[-1]['value']
        ma_50 = df['value'].tail(50).mean()
        ma_200 = df['value'].tail(200).mean()

        above_50 = current > ma_50
        above_200 = current > ma_200

        # Determine trend
        if above_50 and above_200:
            trend = 'UPTREND'
        elif not above_50 and not above_200:
            trend = 'DOWNTREND'
        else:
            trend = 'MIXED'

        return {
            'current': round(current, 2),
            'ma_50': round(ma_50, 2),
            'ma_200': round(ma_200, 2),
            'above_50': above_50,
            'above_200': above_200,
            'trend': trend
        }

    def calculate_fear_greed_components(self) -> Dict:
        """
        Calculate components for a Fear & Greed index

        Components (each scored 0-100, 50 = neutral):
        1. VIX level (inverted - lower VIX = more greed)
        2. VIX term structure (contango = greed, backwardation = fear)
        3. Credit spreads vs 1Y average (tight = greed, wide = fear)
        4. S&P 500 vs 200-day MA (above = greed, below = fear)

        Returns:
            Dict with component scores and overall index
        """
        components = {}

        # 1. VIX Level Score (0-100)
        # VIX 10 = 100 (extreme greed), VIX 40 = 0 (extreme fear)
        vix_data = self.fetch_vix_data(lookback_years=1)
        if vix_data['vix']['current']:
            vix = vix_data['vix']['current']
            # Scale: 10 → 100, 25 → 50, 40 → 0
            vix_score = max(0, min(100, 100 - ((vix - 10) / 30) * 100))
            components['vix_level'] = {
                'score': round(vix_score, 1),
                'value': vix,
                'label': 'VIX Level'
            }

        # 2. VIX Term Structure Score
        if vix_data['term_structure'] is not None:
            term = vix_data['term_structure']
            # Contango (+5) = 100 (greed), Flat (0) = 50, Backwardation (-5) = 0 (fear)
            term_score = max(0, min(100, 50 + (term / 5) * 50))
            components['vix_term'] = {
                'score': round(term_score, 1),
                'value': term,
                'status': vix_data['term_structure_status'],
                'label': 'VIX Term Structure'
            }

        # 3. Credit Spreads Score
        credit_data = self.fetch_credit_spreads()
        if credit_data.get('high_yield', {}).get('percentile'):
            # Low percentile = tight spreads = greed
            # High percentile = wide spreads = fear
            percentile = credit_data['high_yield']['percentile']
            credit_score = 100 - percentile  # Invert so tight = high score
            components['credit_spreads'] = {
                'score': round(credit_score, 1),
                'value': credit_data['high_yield']['current'],
                'percentile': percentile,
                'label': 'Credit Spreads'
            }

        # 4. S&P 500 vs 200-day MA
        ma_data = self.fetch_sp500_moving_averages()
        if ma_data['current'] and ma_data['ma_200']:
            # Calculate % above/below 200-day MA
            pct_vs_ma = ((ma_data['current'] - ma_data['ma_200']) / ma_data['ma_200']) * 100
            # +10% above = 100 (greed), at MA = 50, -10% below = 0 (fear)
            ma_score = max(0, min(100, 50 + (pct_vs_ma / 10) * 50))
            components['sp500_trend'] = {
                'score': round(ma_score, 1),
                'value': round(pct_vs_ma, 2),
                'trend': ma_data['trend'],
                'label': 'S&P 500 Trend'
            }

        # Calculate overall index (equal weighted)
        scores = [c['score'] for c in components.values() if c.get('score') is not None]
        overall = sum(scores) / len(scores) if scores else None

        # Determine status
        status = None
        if overall is not None:
            if overall >= 80:
                status = 'EXTREME GREED'
            elif overall >= 60:
                status = 'GREED'
            elif overall >= 40:
                status = 'NEUTRAL'
            elif overall >= 20:
                status = 'FEAR'
            else:
                status = 'EXTREME FEAR'

        return {
            'overall': round(overall, 1) if overall else None,
            'status': status,
            'components': components
        }

    # =========================================================================
    # CONSUMER SENTIMENT METHODS
    # =========================================================================

    def fetch_consumer_sentiment(self, lookback_years: int = 10) -> Dict:
        """
        Fetch University of Michigan Consumer Sentiment Index

        The sentiment index is a monthly survey. Values above 100 indicate
        optimism, below 100 indicate pessimism. Historical average is around 85.

        Args:
            lookback_years: Years of history to fetch

        Returns:
            Dict with current value, historical series, percentile, and 1-year ago value
        """
        lookback_days = lookback_years * 365
        start_date = (datetime.now() - timedelta(days=lookback_days)).strftime('%Y-%m-%d')

        sentiment_df = self._fetch_series(
            self.SENTIMENT_SERIES['consumer_sentiment'],
            start_date=start_date
        )

        if sentiment_df.empty:
            return {
                'current': None,
                'percentile': None,
                'one_year_ago': None,
                'history': {'dates': [], 'values': []}
            }

        current = sentiment_df.iloc[-1]['value']
        percentile = (sentiment_df['value'] <= current).sum() / len(sentiment_df) * 100

        # Get value from 1 year ago (approximately 12 months back)
        one_year_ago = None
        if len(sentiment_df) >= 13:
            one_year_ago = sentiment_df.iloc[-13]['value']

        return {
            'current': round(current, 1),
            'percentile': round(percentile, 1),
            'one_year_ago': round(one_year_ago, 1) if one_year_ago else None,
            'history': {
                'dates': sentiment_df['date'].dt.strftime('%Y-%m-%d').tolist(),
                'values': sentiment_df['value'].round(1).tolist()
            }
        }

    def fetch_sp500_vs_sentiment(self, lookback_years: int = 10, adjust_for_inflation: bool = True) -> Dict:
        """
        Fetch S&P 500 and Consumer Sentiment for correlation chart

        Args:
            lookback_years: Years of history
            adjust_for_inflation: Whether to adjust S&P 500 for CPI

        Returns:
            Dict with aligned dates, S&P 500 values, sentiment values, and correlation
        """
        lookback_days = lookback_years * 365
        start_date = (datetime.now() - timedelta(days=lookback_days)).strftime('%Y-%m-%d')

        # Fetch Consumer Sentiment (monthly)
        sentiment_df = self._fetch_series(
            self.SENTIMENT_SERIES['consumer_sentiment'],
            start_date=start_date
        )

        if sentiment_df.empty:
            return {
                'dates': [],
                'sp500': [],
                'sp500_label': 'S&P 500',
                'sentiment': [],
                'correlation': None
            }

        # Fetch S&P 500 from Yahoo Finance
        try:
            sp500 = yf.Ticker('^GSPC')
            hist = sp500.history(start=start_date)

            if hist.empty:
                return {
                    'dates': [],
                    'sp500': [],
                    'sp500_label': 'S&P 500',
                    'sentiment': [],
                    'correlation': None
                }

            sp500_df = pd.DataFrame({
                'date': hist.index,
                'value': hist['Close']
            })
            sp500_df['date'] = pd.to_datetime(sp500_df['date']).dt.tz_localize(None)

            # Resample S&P 500 to monthly (end of month)
            sp500_df = sp500_df.set_index('date')
            sp500_monthly = sp500_df.resample('ME').last().reset_index()
            sp500_monthly.columns = ['date', 'sp500']

        except Exception as e:
            logger.error(f"Error fetching S&P 500: {e}")
            return {
                'dates': [],
                'sp500': [],
                'sp500_label': 'S&P 500',
                'sentiment': [],
                'correlation': None
            }

        # Adjust for inflation if requested
        sp500_label = 'S&P 500'
        if adjust_for_inflation:
            cpi_df = self._fetch_series(
                self.INFLATION_SERIES['cpi'],
                start_date=start_date
            )

            if not cpi_df.empty:
                # Resample CPI to monthly if needed
                cpi_df = cpi_df.set_index('date')
                cpi_monthly = cpi_df.resample('ME').last().reset_index()
                cpi_monthly.columns = ['date', 'cpi']

                # Merge and calculate real S&P 500
                sp500_monthly = sp500_monthly.merge(cpi_monthly, on='date', how='inner')
                latest_cpi = sp500_monthly['cpi'].iloc[-1]
                sp500_monthly['sp500_real'] = sp500_monthly['sp500'] * (latest_cpi / sp500_monthly['cpi'])
                sp500_monthly['sp500'] = sp500_monthly['sp500_real']
                sp500_label = 'S&P 500 (Inflation-Adjusted)'

        # Prepare sentiment data for merge
        sentiment_df = sentiment_df.copy()
        sentiment_df['date'] = sentiment_df['date'].dt.to_period('M').dt.to_timestamp('M')
        sentiment_df.columns = ['date', 'sentiment']

        # Merge on month
        merged = sp500_monthly.merge(sentiment_df, on='date', how='inner')

        if merged.empty:
            return {
                'dates': [],
                'sp500': [],
                'sp500_label': sp500_label,
                'sentiment': [],
                'correlation': None
            }

        # Calculate correlation
        correlation = merged['sp500'].corr(merged['sentiment'])

        return {
            'dates': merged['date'].dt.strftime('%Y-%m').tolist(),
            'sp500': merged['sp500'].round(0).tolist(),
            'sp500_label': sp500_label,
            'sentiment': merged['sentiment'].round(1).tolist(),
            'correlation': round(correlation, 2) if not pd.isna(correlation) else None
        }

    # =========================================================================
    # MONEY MARKET FUNDS METHODS
    # =========================================================================

    def fetch_money_market_funds(self, lookback_years: int = 15) -> Dict:
        """
        Fetch Money Market Fund Total Assets

        Shows liquidity/cash on sidelines - indicator of potential money to deploy.
        Rising MMF assets often seen during uncertainty, falling assets may indicate
        risk-on behavior as investors move money into equities.

        Args:
            lookback_years: Years of history to fetch

        Returns:
            Dict with current value in trillions, historical series, YoY change
        """
        lookback_days = lookback_years * 365
        start_date = (datetime.now() - timedelta(days=lookback_days)).strftime('%Y-%m-%d')

        mmf_df = self._fetch_series(
            self.MONEY_MARKET_SERIES['total_assets'],
            start_date=start_date
        )

        if mmf_df.empty:
            return {
                'current_trillions': None,
                'yoy_change_pct': None,
                'all_time_high': None,
                'at_all_time_high': False,
                'history': {'dates': [], 'values': []}
            }

        # Values are in millions, convert to trillions
        mmf_df['value_trillions'] = mmf_df['value'] / 1_000_000

        current = mmf_df.iloc[-1]['value_trillions']
        all_time_high = mmf_df['value_trillions'].max()

        # YoY change (quarterly data, so 4 periods back)
        yoy_change = None
        if len(mmf_df) >= 5:
            year_ago = mmf_df.iloc[-5]['value_trillions']
            yoy_change = ((current - year_ago) / year_ago) * 100

        return {
            'current_trillions': round(current, 2),
            'yoy_change_pct': round(yoy_change, 1) if yoy_change else None,
            'all_time_high': round(all_time_high, 2),
            'at_all_time_high': current >= all_time_high * 0.98,  # Within 2% of ATH
            'history': {
                'dates': mmf_df['date'].dt.strftime('%Y-%m-%d').tolist(),
                'values': mmf_df['value_trillions'].round(2).tolist()
            }
        }

    # =========================================================================
    # SMALL CAP VS LARGE CAP METHODS
    # =========================================================================

    def fetch_small_large_cap_ratio(self, lookback_years: int = 10) -> Dict:
        """
        Calculate Small Cap (Russell 2000) vs Large Cap (S&P 500) price ratio

        Shows relative performance:
        - Rising ratio: Small caps outperforming (risk-on, economic optimism)
        - Falling ratio: Large caps outperforming (risk-off, quality preference)

        Args:
            lookback_years: Years of history to fetch

        Returns:
            Dict with current ratio, historical series, percentile, trend
        """
        lookback_days = lookback_years * 365
        start_date = (datetime.now() - timedelta(days=lookback_days)).strftime('%Y-%m-%d')

        try:
            # Fetch Russell 2000 (small cap)
            rut = yf.Ticker('^RUT')
            rut_hist = rut.history(start=start_date)

            # Fetch S&P 500 (large cap)
            sp500 = yf.Ticker('^GSPC')
            sp500_hist = sp500.history(start=start_date)

            if rut_hist.empty or sp500_hist.empty:
                return {
                    'current_ratio': None,
                    'percentile': None,
                    'trend': None,
                    'change_3m': None,
                    'rut_current': None,
                    'sp500_current': None,
                    'history': {'dates': [], 'values': []}
                }

            # Create DataFrames
            rut_df = pd.DataFrame({
                'date': rut_hist.index,
                'rut': rut_hist['Close']
            })
            rut_df['date'] = pd.to_datetime(rut_df['date']).dt.tz_localize(None)

            sp500_df = pd.DataFrame({
                'date': sp500_hist.index,
                'sp500': sp500_hist['Close']
            })
            sp500_df['date'] = pd.to_datetime(sp500_df['date']).dt.tz_localize(None)

            # Merge on date
            merged = rut_df.merge(sp500_df, on='date', how='inner')

            if merged.empty:
                return {
                    'current_ratio': None,
                    'percentile': None,
                    'trend': None,
                    'change_3m': None,
                    'rut_current': None,
                    'sp500_current': None,
                    'history': {'dates': [], 'values': []}
                }

            # Calculate ratio
            merged['ratio'] = merged['rut'] / merged['sp500']

            current_ratio = merged.iloc[-1]['ratio']
            percentile = (merged['ratio'] <= current_ratio).sum() / len(merged) * 100

            # Calculate 3-month trend (~63 trading days)
            trend = None
            change_3m = None
            if len(merged) > 63:
                ratio_3m_ago = merged.iloc[-63]['ratio']
                change_3m = (current_ratio - ratio_3m_ago) / ratio_3m_ago * 100
                if change_3m > 3:
                    trend = 'SMALL CAPS GAINING'
                elif change_3m < -3:
                    trend = 'LARGE CAPS GAINING'
                else:
                    trend = 'STABLE'

            # Resample to weekly for chart (less noise)
            merged_weekly = merged.set_index('date').resample('W').last().reset_index()
            merged_weekly = merged_weekly.dropna()

            return {
                'current_ratio': round(current_ratio, 4),
                'percentile': round(percentile, 1),
                'trend': trend,
                'change_3m': round(change_3m, 2) if change_3m else None,
                'rut_current': round(merged.iloc[-1]['rut'], 2),
                'sp500_current': round(merged.iloc[-1]['sp500'], 2),
                'history': {
                    'dates': merged_weekly['date'].dt.strftime('%Y-%m-%d').tolist(),
                    'ratios': merged_weekly['ratio'].round(4).tolist()
                }
            }

        except Exception as e:
            logger.error(f"Error fetching small/large cap ratio: {e}")
            return {
                'current_ratio': None,
                'percentile': None,
                'trend': None,
                'change_3m': None,
                'rut_current': None,
                'sp500_current': None,
                'history': {'dates': [], 'ratios': []}
            }

    # =========================================================================
    # CRYPTOCURRENCY METHODS
    # =========================================================================

    def fetch_btc_price(self, lookback_days: int = 1825, use_cache: bool = True) -> pd.DataFrame:
        """
        Fetch Bitcoin price data from Yahoo Finance with caching

        Args:
            lookback_days: Days of history to fetch (default 5 years)
            use_cache: Whether to use database cache

        Returns:
            DataFrame with date and value columns
        """
        series_id = 'BTC-USD'
        start_date = (datetime.now() - timedelta(days=lookback_days)).strftime('%Y-%m-%d')

        # Try cache first
        if use_cache:
            cached_df = self._get_cached_series(series_id, start_date)
            if cached_df is not None:
                return cached_df

        try:
            btc = yf.Ticker(series_id)
            hist = btc.history(start=start_date)

            if hist.empty:
                return pd.DataFrame(columns=['date', 'value'])

            df = pd.DataFrame({
                'date': hist.index,
                'value': hist['Close']
            })
            df['date'] = pd.to_datetime(df['date']).dt.tz_localize(None)
            df = df.sort_values('date').reset_index(drop=True)

            # Save to cache
            if use_cache:
                self._save_to_cache(series_id, df)

            return df

        except Exception as e:
            logger.error(f"Error fetching BTC price: {e}")
            return pd.DataFrame(columns=['date', 'value'])

    def calculate_btc_returns(self) -> Dict:
        """
        Calculate BTC returns across multiple timeframes

        Returns:
            Dict with returns for each timeframe
        """
        timeframes = {
            '1d': 1, '1w': 7, '1m': 30, '3m': 90, '1y': 365, '3y': 1095, '5y': 1825
        }

        df = self.fetch_btc_price(lookback_days=1825)

        btc_data = {'name': 'BTC/USD', 'code': 'BTC'}

        for period, days in timeframes.items():
            return_pct = self._calculate_period_return(df, days)
            btc_data[period] = round(return_pct, 2) if return_pct is not None else None

        return btc_data

    def calculate_btc_vs_currencies(self) -> List[Dict]:
        """
        Calculate BTC performance vs major currencies

        Shows how each currency has performed vs Bitcoin.
        Negative % means currency lost purchasing power vs BTC.

        Returns:
            List of dicts with currency performance vs BTC
        """
        timeframes = {
            '1d': 1, '1w': 7, '1m': 30, '3m': 90, '1y': 365, '3y': 1095, '5y': 1825
        }

        btc_usd_df = self.fetch_btc_price(lookback_days=1825)

        if btc_usd_df.empty:
            return []

        results = []

        # USD vs BTC
        usd_data = {'name': 'USD', 'code': 'USD'}
        for period, days in timeframes.items():
            btc_return = self._calculate_period_return(btc_usd_df, days)
            # Invert: if BTC went up 10%, USD lost 10% purchasing power vs BTC
            usd_data[period] = round(-btc_return, 2) if btc_return is not None else None
        results.append(usd_data)

        # Other currencies vs BTC
        for currency in ['EUR', 'JPY', 'CNY', 'CHF']:
            fx_df = self.fetch_exchange_rate(currency, lookback_days=1825)

            if fx_df.empty:
                continue

            # Merge BTC and FX data
            merged = pd.merge(btc_usd_df, fx_df, on='date', suffixes=('_btc', '_fx'))

            if merged.empty:
                continue

            # Calculate BTC price in local currency
            if currency == 'EUR':
                # EUR: DEXUSEU = USD per EUR, so BTC_EUR = BTC_USD / DEXUSEU
                merged['btc_local'] = merged['value_btc'] / merged['value_fx']
            else:
                # JPY/CNY/CHF: Foreign per USD, so BTC_LOCAL = BTC_USD * FX_RATE
                merged['btc_local'] = merged['value_btc'] * merged['value_fx']

            btc_local_df = pd.DataFrame({
                'date': merged['date'],
                'value': merged['btc_local']
            })

            currency_data = {'name': currency, 'code': currency}
            for period, days in timeframes.items():
                btc_return_local = self._calculate_period_return(btc_local_df, days)
                # Invert: positive BTC return = negative currency performance vs BTC
                currency_data[period] = round(-btc_return_local, 2) if btc_return_local is not None else None

            results.append(currency_data)

        return results

    def fetch_btc_market_data(self) -> Dict:
        """
        Fetch BTC market cap and additional data from Yahoo Finance

        Returns:
            Dict with market cap, volume, current price, 52-week high/low
        """
        try:
            btc = yf.Ticker('BTC-USD')
            info = btc.info

            market_cap = info.get('marketCap')

            return {
                'market_cap': market_cap,
                'market_cap_trillions': round(market_cap / 1e12, 2) if market_cap else None,
                'volume_24h': info.get('volume24Hr') or info.get('volume'),
                'circulating_supply': info.get('circulatingSupply'),
                'current_price': info.get('regularMarketPrice') or info.get('previousClose'),
                'high_52w': info.get('fiftyTwoWeekHigh'),
                'low_52w': info.get('fiftyTwoWeekLow'),
            }

        except Exception as e:
            logger.error(f"Error fetching BTC market data: {e}")
            return {
                'market_cap': None,
                'market_cap_trillions': None,
                'volume_24h': None,
                'circulating_supply': None,
                'current_price': None,
                'high_52w': None,
                'low_52w': None,
            }

    def fetch_asset_market_caps(self) -> Dict:
        """
        Fetch market caps for BTC, Gold, and Silver for comparison

        Gold: ~$16T (estimated from total above-ground gold ~210,000 tonnes * price/oz)
        Silver: ~$1.5T (estimated from total above-ground silver)
        BTC: From Yahoo Finance

        Returns:
            Dict with market caps and percentages
        """
        try:
            # BTC market cap from Yahoo Finance
            btc = yf.Ticker('BTC-USD')
            btc_info = btc.info
            btc_market_cap = btc_info.get('marketCap', 0)

            # Gold price from Yahoo Finance
            gold = yf.Ticker('GC=F')
            gold_info = gold.history(period='1d')
            gold_price = gold_info['Close'].iloc[-1] if not gold_info.empty else 2650

            # Silver price from Yahoo Finance
            silver = yf.Ticker('SI=F')
            silver_info = silver.history(period='1d')
            silver_price = silver_info['Close'].iloc[-1] if not silver_info.empty else 30

            # Estimated total above-ground gold: ~210,000 metric tonnes
            # 1 metric tonne = 32,150.75 troy ounces
            gold_tonnes = 210000
            gold_oz = gold_tonnes * 32150.75
            gold_market_cap = gold_oz * gold_price

            # Estimated total above-ground silver: ~1.7 million metric tonnes (investment grade ~50,000)
            # Using investable silver estimate
            silver_tonnes = 50000
            silver_oz = silver_tonnes * 32150.75
            silver_market_cap = silver_oz * silver_price

            total = btc_market_cap + gold_market_cap + silver_market_cap

            return {
                'btc': {
                    'name': 'Bitcoin',
                    'market_cap': btc_market_cap,
                    'market_cap_trillions': round(btc_market_cap / 1e12, 2) if btc_market_cap else None,
                    'percentage': round((btc_market_cap / total) * 100, 1) if total else None,
                    'color': '#f7931a'
                },
                'gold': {
                    'name': 'Gold',
                    'market_cap': gold_market_cap,
                    'market_cap_trillions': round(gold_market_cap / 1e12, 2),
                    'percentage': round((gold_market_cap / total) * 100, 1) if total else None,
                    'price_per_oz': round(gold_price, 2),
                    'color': '#ffd700'
                },
                'silver': {
                    'name': 'Silver',
                    'market_cap': silver_market_cap,
                    'market_cap_trillions': round(silver_market_cap / 1e12, 2),
                    'percentage': round((silver_market_cap / total) * 100, 1) if total else None,
                    'price_per_oz': round(silver_price, 2),
                    'color': '#c0c0c0'
                },
                'total_trillions': round(total / 1e12, 2) if total else None
            }

        except Exception as e:
            logger.error(f"Error fetching asset market caps: {e}")
            return {
                'btc': {'name': 'Bitcoin', 'market_cap_trillions': None, 'percentage': None},
                'gold': {'name': 'Gold', 'market_cap_trillions': None, 'percentage': None},
                'silver': {'name': 'Silver', 'market_cap_trillions': None, 'percentage': None},
                'total_trillions': None
            }

    def fetch_normalized_asset_comparison(self, lookback_years: int = 5) -> Dict:
        """
        Fetch BTC, Gold, Silver, and S&P 500 prices normalized to 100 at start date.
        Allows comparison of relative performance from same starting point.

        Args:
            lookback_years: Years of history (default 5)

        Returns:
            Dict with dates and normalized values for each asset
        """
        lookback_days = lookback_years * 365
        start_date = (datetime.now() - timedelta(days=lookback_days)).strftime('%Y-%m-%d')

        try:
            # Fetch all assets
            assets = {
                'btc': {'ticker': 'BTC-USD', 'name': 'Bitcoin', 'color': '#f7931a'},
                'gold': {'ticker': 'GC=F', 'name': 'Gold', 'color': '#ffd700'},
                'silver': {'ticker': 'SI=F', 'name': 'Silver', 'color': '#c0c0c0'},
                'sp500': {'ticker': '^GSPC', 'name': 'S&P 500', 'color': '#00ff88'},
            }

            all_data = {}

            for key, info in assets.items():
                ticker = yf.Ticker(info['ticker'])
                hist = ticker.history(start=start_date)

                if not hist.empty:
                    df = pd.DataFrame({
                        'date': hist.index,
                        'price': hist['Close']
                    })
                    df['date'] = pd.to_datetime(df['date']).dt.tz_localize(None)
                    df = df.sort_values('date').reset_index(drop=True)
                    all_data[key] = df

            if not all_data:
                return {'dates': [], 'datasets': []}

            # Find common date range (weekly resampling for cleaner chart)
            # Use BTC as reference since it might have shortest history
            ref_key = 'btc' if 'btc' in all_data else list(all_data.keys())[0]
            ref_df = all_data[ref_key].set_index('date').resample('W').last().reset_index()
            ref_df = ref_df.dropna()

            dates = ref_df['date'].tolist()
            result = {
                'dates': [d.strftime('%Y-%m-%d') for d in dates],
                'datasets': []
            }

            for key, info in assets.items():
                if key not in all_data:
                    continue

                df = all_data[key].set_index('date').resample('W').last().reset_index()
                df = df.dropna()

                # Align with reference dates
                df = df[df['date'].isin(dates)]

                if len(df) < 2:
                    continue

                # Normalize to 100 at start
                start_price = df.iloc[0]['price']
                df['normalized'] = (df['price'] / start_price) * 100

                result['datasets'].append({
                    'key': key,
                    'name': info['name'],
                    'color': info['color'],
                    'data': df['normalized'].round(2).tolist()
                })

            return result

        except Exception as e:
            logger.error(f"Error fetching normalized asset comparison: {e}")
            return {'dates': [], 'datasets': []}
