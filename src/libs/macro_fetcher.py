"""
Macro Data Fetcher - FRED API Integration
Fetches macroeconomic data from Federal Reserve Economic Data (FRED) API
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
            else:
                return None

            # Check if we have recent data
            df = self.db.get_macro_data(data_type, series_id, start_date=start_date)

            if df.empty:
                return None

            # Check if cache is fresh (has data from last cache_hours)
            latest_date = pd.to_datetime(df['date'].max())
            cache_age = datetime.now() - latest_date.replace(tzinfo=None)

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

    def get_spread_history(self, lookback_days: int = 365) -> Dict:
        """
        Get historical spread data for charting

        Args:
            lookback_days: Days of history to fetch (default 1 year)

        Returns:
            Dict with dates and spread values for each spread type
        """
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

        history = {
            'dates': [],
            '10y2y': [],
            '10y3m': [],
            '30y5y': []
        }

        # Merge all dataframes on date to ensure consistent timeline
        if not yields_10y.empty and not yields_2y.empty:
            merged = pd.merge(yields_10y, yields_2y, on='date', suffixes=('_10y', '_2y'))

            for _, row in merged.iterrows():
                date_str = row['date'].strftime('%Y-%m-%d')
                if date_str not in history['dates']:
                    history['dates'].append(date_str)

                spread_10y2y = row['value_10y'] - row['value_2y']
                history['10y2y'].append(round(spread_10y2y, 2))

        # 10Y-3M spread
        if not yields_10y.empty and not yields_3m.empty:
            merged = pd.merge(yields_10y, yields_3m, on='date', suffixes=('_10y', '_3m'))

            # Reset if we're starting fresh
            if not history['dates']:
                for _, row in merged.iterrows():
                    history['dates'].append(row['date'].strftime('%Y-%m-%d'))

            for _, row in merged.iterrows():
                spread_10y3m = row['value_10y'] - row['value_3m']
                history['10y3m'].append(round(spread_10y3m, 2))

        # 30Y-5Y spread
        if not yields_30y.empty and not yields_5y.empty:
            merged = pd.merge(yields_30y, yields_5y, on='date', suffixes=('_30y', '_5y'))

            # Reset if we're starting fresh
            if not history['dates']:
                for _, row in merged.iterrows():
                    history['dates'].append(row['date'].strftime('%Y-%m-%d'))

            for _, row in merged.iterrows():
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
