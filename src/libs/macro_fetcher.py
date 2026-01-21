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
