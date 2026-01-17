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

    def __init__(self, fred_api_key: str):
        """Initialize with FRED API key"""
        self.api_key = fred_api_key

    def _fetch_series(self, series_id: str, start_date: Optional[str] = None,
                      end_date: Optional[str] = None) -> pd.DataFrame:
        """
        Fetch a FRED data series

        Args:
            series_id: FRED series identifier
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format

        Returns:
            DataFrame with columns: date, value
        """
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

    def fetch_gold_price(self, lookback_days: int = 1825) -> pd.DataFrame:
        """
        Fetch gold price data from Yahoo Finance (FRED series discontinued)

        Args:
            lookback_days: Days of history to fetch (default 5 years)

        Returns:
            DataFrame with date and value columns
        """
        try:
            # Use Yahoo Finance for gold futures (GC=F) as FRED gold series are discontinued
            gold = yf.Ticker("GC=F")
            end_date = datetime.now()
            start_date = end_date - timedelta(days=lookback_days)

            hist = gold.history(start=start_date, end=end_date)

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

    def calculate_sp500_returns(self) -> Dict:
        """
        Calculate S&P 500 returns across multiple timeframes using Yahoo Finance

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

        try:
            sp500 = yf.Ticker("^GSPC")
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
            df['date'] = pd.to_datetime(df['date']).dt.tz_localize(None)  # Remove timezone for consistency
            df = df.sort_values('date').reset_index(drop=True)

            sp500_data = {
                'name': 'S&P 500',
                'code': 'SPX',
            }

            for period, days in timeframes.items():
                return_pct = self._calculate_period_return(df, days)
                sp500_data[period] = return_pct

            logger.info(f"Fetched {len(df)} S&P 500 observations from Yahoo Finance")
            return sp500_data

        except Exception as e:
            logger.error(f"Error fetching S&P 500 from Yahoo Finance: {e}")
            return {'name': 'S&P 500', 'code': 'SPX', **{p: None for p in timeframes.keys()}}

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
        Calculate key yield spreads with historical context

        Returns:
            Dict with spread calculations and interpretations
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

        # 10Y-2Y spread
        if not yields_10y.empty and not yields_2y.empty:
            current_10y = yields_10y.iloc[-1]['value']
            current_2y = yields_2y.iloc[-1]['value']
            spread_10y2y = current_10y - current_2y

            # Get year ago value
            year_ago_date = yields_10y.iloc[-1]['date'] - timedelta(days=365)
            year_ago_10y = yields_10y[yields_10y['date'] <= year_ago_date]
            year_ago_2y = yields_2y[yields_2y['date'] <= year_ago_date]

            if not year_ago_10y.empty and not year_ago_2y.empty:
                year_ago_spread = year_ago_10y.iloc[-1]['value'] - year_ago_2y.iloc[-1]['value']
            else:
                year_ago_spread = None

            spreads['10y2y'] = {
                'current': round(spread_10y2y, 2),
                'year_ago': round(year_ago_spread, 2) if year_ago_spread is not None else None,
            }

        # 10Y-3M spread
        if not yields_10y.empty and not yields_3m.empty:
            current_10y = yields_10y.iloc[-1]['value']
            current_3m = yields_3m.iloc[-1]['value']
            spread_10y3m = current_10y - current_3m

            year_ago_date = yields_10y.iloc[-1]['date'] - timedelta(days=365)
            year_ago_10y = yields_10y[yields_10y['date'] <= year_ago_date]
            year_ago_3m = yields_3m[yields_3m['date'] <= year_ago_date]

            if not year_ago_10y.empty and not year_ago_3m.empty:
                year_ago_spread = year_ago_10y.iloc[-1]['value'] - year_ago_3m.iloc[-1]['value']
            else:
                year_ago_spread = None

            spreads['10y3m'] = {
                'current': round(spread_10y3m, 2),
                'year_ago': round(year_ago_spread, 2) if year_ago_spread is not None else None,
            }

        # 30Y-5Y spread
        if not yields_30y.empty and not yields_5y.empty:
            current_30y = yields_30y.iloc[-1]['value']
            current_5y = yields_5y.iloc[-1]['value']
            spread_30y5y = current_30y - current_5y

            year_ago_date = yields_30y.iloc[-1]['date'] - timedelta(days=365)
            year_ago_30y = yields_30y[yields_30y['date'] <= year_ago_date]
            year_ago_5y = yields_5y[yields_5y['date'] <= year_ago_date]

            if not year_ago_30y.empty and not year_ago_5y.empty:
                year_ago_spread = year_ago_30y.iloc[-1]['value'] - year_ago_5y.iloc[-1]['value']
            else:
                year_ago_spread = None

            spreads['30y5y'] = {
                'current': round(spread_30y5y, 2),
                'year_ago': round(year_ago_spread, 2) if year_ago_spread is not None else None,
            }

        return spreads

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
