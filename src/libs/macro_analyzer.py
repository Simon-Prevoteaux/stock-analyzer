"""
Macro Data Analyzer
Provides interpretation and context for macroeconomic data
"""

from typing import Dict
import pandas as pd


class MacroAnalyzer:
    """Analyzes macro data and provides interpretation"""

    @staticmethod
    def interpret_yield_curve(spreads: Dict) -> Dict:
        """
        Interpret yield curve shape and spreads

        Args:
            spreads: Dict with spread data (10y2y, 10y3m, 30y5y)

        Returns:
            Dict with interpretations for each spread
        """
        interpretations = {}

        # 10Y-2Y Spread Interpretation
        if '10y2y' in spreads and spreads['10y2y']['current'] is not None:
            spread_10y2y = spreads['10y2y']['current']

            if spread_10y2y < -0.5:
                status = 'DEEPLY INVERTED'
                interpretation = 'Strong recession warning. Short rates significantly exceed long rates.'
                status_class = 'danger'
            elif -0.5 <= spread_10y2y < -0.1:
                status = 'INVERTED'
                interpretation = 'Recession warning. Short rates exceed long rates.'
                status_class = 'danger'
            elif -0.1 <= spread_10y2y < 0:
                status = 'SLIGHTLY INVERTED'
                interpretation = 'Mild inversion. Heightened recession risk.'
                status_class = 'warning'
            elif 0 <= spread_10y2y < 0.25:
                status = 'FLAT'
                interpretation = 'Flat curve signals uncertainty about growth outlook.'
                status_class = 'warning'
            elif 0.25 <= spread_10y2y < 1:
                status = 'NORMAL'
                interpretation = 'Normal curve. Healthy economic expectations.'
                status_class = 'normal'
            elif 1 <= spread_10y2y < 2:
                status = 'STEEP'
                interpretation = 'Steep curve. Strong growth expectations or rising inflation concerns.'
                status_class = 'normal'
            else:
                status = 'VERY STEEP'
                interpretation = 'Very steep curve. Aggressive monetary easing or inflation fears.'
                status_class = 'warning'

            interpretations['10y2y'] = {
                'status': status,
                'interpretation': interpretation,
                'status_class': status_class,
                **spreads['10y2y']
            }

        # 10Y-3M Spread Interpretation
        if '10y3m' in spreads and spreads['10y3m']['current'] is not None:
            spread_10y3m = spreads['10y3m']['current']

            if spread_10y3m < -0.3:
                status = 'DEEPLY INVERTED'
                interpretation = 'Strong recession signal. Very high short-term rates.'
                status_class = 'danger'
            elif -0.3 <= spread_10y3m < 0:
                status = 'INVERTED'
                interpretation = 'Recession warning. Short-term rates exceed long-term.'
                status_class = 'danger'
            elif 0 <= spread_10y3m < 0.5:
                status = 'FLAT'
                interpretation = 'Compressed spread. Tight monetary policy or growth concerns.'
                status_class = 'warning'
            elif 0.5 <= spread_10y3m < 2:
                status = 'NORMAL'
                interpretation = 'Healthy spread. Normal monetary conditions.'
                status_class = 'normal'
            else:
                status = 'STEEP'
                interpretation = 'Wide spread. Accommodative policy or inflation expectations.'
                status_class = 'normal'

            interpretations['10y3m'] = {
                'status': status,
                'interpretation': interpretation,
                'status_class': status_class,
                **spreads['10y3m']
            }

        # 30Y-5Y Spread Interpretation
        if '30y5y' in spreads and spreads['30y5y']['current'] is not None:
            spread_30y5y = spreads['30y5y']['current']

            if spread_30y5y < 0.3:
                status = 'FLAT'
                interpretation = 'Long-end compression. Limited long-term growth expectations.'
                status_class = 'warning'
            elif 0.3 <= spread_30y5y < 0.8:
                status = 'NORMAL'
                interpretation = 'Normal long-end spread. Balanced expectations.'
                status_class = 'normal'
            else:
                status = 'STEEP'
                interpretation = 'Wide long-end spread. Higher term premium or inflation concerns.'
                status_class = 'normal'

            interpretations['30y5y'] = {
                'status': status,
                'interpretation': interpretation,
                'status_class': status_class,
                **spreads['30y5y']
            }

        return interpretations

    @staticmethod
    def interpret_credit_spread(spread_type: str, spread_bps: float, percentile: float) -> str:
        """
        Interpret credit spread level

        Args:
            spread_type: Type of spread (corporate_master, high_yield, bbb)
            spread_bps: Current spread in basis points
            percentile: Historical percentile (0-100)

        Returns:
            Interpretation string
        """
        # Interpretation based on percentile rank
        if percentile is None or spread_bps is None:
            return 'Data not available'

        if percentile < 15:
            level = 'Very Tight'
            interpretation = 'Extremely compressed spreads. High risk appetite, complacency. Limited downside protection, elevated risk of widening.'
        elif percentile < 35:
            level = 'Tight'
            interpretation = 'Below average spreads. Investors comfortable with credit risk. Limited margin of safety.'
        elif percentile < 65:
            level = 'Moderate'
            interpretation = 'Average spreads. Balanced risk assessment. Fair compensation for credit risk.'
        elif percentile < 85:
            level = 'Wide'
            interpretation = 'Above average spreads. Rising credit concerns or risk aversion. Better entry point for credit.'
        else:
            level = 'Very Wide'
            interpretation = 'Extremely elevated spreads. Significant credit stress or crisis conditions. Potential value opportunity if fundamental strength exists.'

        # Add context based on spread type
        if spread_type == 'high_yield':
            if percentile < 35:
                interpretation += ' High yield particularly vulnerable to widening.'
            elif percentile > 65:
                interpretation += ' High yield stress potentially indicating economic weakness.'
        elif spread_type == 'bbb':
            if percentile < 35:
                interpretation += ' Investment grade spreads leaving little room for deterioration.'
            elif percentile > 65:
                interpretation += ' BBB spreads widening - watch for fallen angels.'

        return f"{level} - {interpretation}"

    @staticmethod
    def interpret_currency_move(currency: str, return_pct: float, timeframe: str) -> str:
        """
        Interpret currency movement

        Args:
            currency: Currency code (EUR, JPY, etc.)
            return_pct: Return percentage
            timeframe: Timeframe (1d, 1w, 1m, etc.)

        Returns:
            Interpretation string
        """
        if return_pct is None:
            return "Data not available"

        abs_return = abs(return_pct)

        # Thresholds depend on timeframe
        if timeframe == '1d':
            threshold_small = 0.5
            threshold_large = 2.0
        elif timeframe == '1w':
            threshold_small = 1.0
            threshold_large = 3.0
        elif timeframe in ['1m', '3m']:
            threshold_small = 2.0
            threshold_large = 5.0
        elif timeframe == '1y':
            threshold_small = 5.0
            threshold_large = 10.0
        else:  # 3y, 5y
            threshold_small = 10.0
            threshold_large = 20.0

        if return_pct > threshold_large:
            magnitude = "Significant strengthening"
        elif return_pct > threshold_small:
            magnitude = "Moderate strengthening"
        elif abs_return <= threshold_small:
            magnitude = "Relatively stable"
        elif return_pct < -threshold_large:
            magnitude = "Significant weakening"
        else:
            magnitude = "Moderate weakening"

        return f"{magnitude} vs USD"

    @staticmethod
    def calculate_percentile(current_value: float, historical_data: pd.Series) -> float:
        """
        Calculate percentile rank of current value in historical distribution

        Args:
            current_value: Current value
            historical_data: Series of historical values

        Returns:
            Percentile (0-100)
        """
        if historical_data.empty or pd.isna(current_value):
            return None

        percentile = (historical_data <= current_value).sum() / len(historical_data) * 100
        return round(percentile, 1)

    @staticmethod
    def get_recession_indicator_summary(spreads: Dict) -> Dict:
        """
        Generate summary of recession indicators based on yield spreads

        Args:
            spreads: Dict with spread interpretations

        Returns:
            Dict with recession risk assessment
        """
        risk_score = 0
        signals = []

        # Check 10Y-2Y spread
        if '10y2y' in spreads:
            spread_10y2y = spreads['10y2y'].get('current')
            if spread_10y2y is not None:
                if spread_10y2y < -0.5:
                    risk_score += 3
                    signals.append("10Y-2Y deeply inverted (strong recession signal)")
                elif spread_10y2y < 0:
                    risk_score += 2
                    signals.append("10Y-2Y inverted (recession warning)")
                elif spread_10y2y < 0.25:
                    risk_score += 1
                    signals.append("10Y-2Y very flat (growth concerns)")

        # Check 10Y-3M spread
        if '10y3m' in spreads:
            spread_10y3m = spreads['10y3m'].get('current')
            if spread_10y3m is not None:
                if spread_10y3m < -0.3:
                    risk_score += 3
                    signals.append("10Y-3M deeply inverted (strong recession signal)")
                elif spread_10y3m < 0:
                    risk_score += 2
                    signals.append("10Y-3M inverted (recession warning)")

        # Determine overall risk level
        if risk_score >= 5:
            risk_level = "HIGH"
            risk_class = "danger"
            summary = "Multiple strong recession signals present"
        elif risk_score >= 3:
            risk_level = "ELEVATED"
            risk_class = "warning"
            summary = "Some recession warning signs present"
        elif risk_score >= 1:
            risk_level = "MODERATE"
            risk_class = "caution"
            summary = "Economic growth concerns but no strong recession signals"
        else:
            risk_level = "LOW"
            risk_class = "normal"
            summary = "Yield curve suggests healthy economic expectations"

        return {
            'risk_level': risk_level,
            'risk_class': risk_class,
            'risk_score': risk_score,
            'summary': summary,
            'signals': signals
        }

    @staticmethod
    def format_currency_comparison_insight(currencies: list, gold: dict) -> Dict:
        """
        Generate insights comparing currency and gold performance

        Args:
            currencies: List of currency performance dicts
            gold: Gold performance dict

        Returns:
            Dict with key insights
        """
        insights = []

        # Check if gold is outperforming significantly (1 year)
        gold_1y = gold.get('1y')
        if gold_1y is not None and gold_1y > 15:
            insights.append({
                'type': 'gold_strength',
                'message': f"Gold up {gold_1y:.1f}% over past year, signaling inflation/devaluation concerns",
                'severity': 'warning'
            })

        # Check for broad dollar weakness
        if currencies:
            currencies_1y = [c.get('1y') for c in currencies if c.get('1y') is not None]
            if currencies_1y and len(currencies_1y) >= 2:
                avg_1y = sum(currencies_1y) / len(currencies_1y)
                if avg_1y > 5:
                    insights.append({
                        'type': 'dollar_weakness',
                        'message': f"Broad dollar weakness (avg {avg_1y:.1f}% vs major currencies)",
                        'severity': 'info'
                    })
                elif avg_1y < -5:
                    insights.append({
                        'type': 'dollar_strength',
                        'message': f"Broad dollar strength (avg {avg_1y:.1f}% vs major currencies)",
                        'severity': 'info'
                    })

        return {'insights': insights}

    # =========================================================================
    # GLOBAL ECONOMY INTERPRETATIONS
    # =========================================================================

    @staticmethod
    def interpret_buffett_indicator(value: float, percentile: float = None) -> Dict:
        """
        Interpret Buffett Indicator value (Market Cap / GDP)

        Historical thresholds based on long-term data:
        - < 75%: Significantly Undervalued
        - 75-90%: Modestly Undervalued
        - 90-115%: Fair Value
        - 115-140%: Modestly Overvalued
        - 140-180%: Significantly Overvalued
        - > 180%: Extreme Overvaluation

        Args:
            value: Current Buffett Indicator value (percentage)
            percentile: Historical percentile (optional)

        Returns:
            Dict with status, interpretation, and status_class
        """
        if value is None:
            return {
                'status': 'N/A',
                'interpretation': 'Data not available',
                'status_class': 'neutral'
            }

        if value < 75:
            status = 'SIGNIFICANTLY UNDERVALUED'
            interpretation = 'Market is deeply undervalued relative to GDP. Historically rare opportunity.'
            status_class = 'positive'
        elif value < 90:
            status = 'MODESTLY UNDERVALUED'
            interpretation = 'Market trading below historical fair value. Potential buying opportunity.'
            status_class = 'positive'
        elif value < 115:
            status = 'FAIR VALUE'
            interpretation = 'Market valuation aligned with economic output. Reasonable entry point.'
            status_class = 'neutral'
        elif value < 140:
            status = 'MODESTLY OVERVALUED'
            interpretation = 'Market somewhat expensive relative to GDP. Exercise caution.'
            status_class = 'warning'
        elif value < 180:
            status = 'SIGNIFICANTLY OVERVALUED'
            interpretation = 'Market expensive. Historical periods at these levels often preceded corrections.'
            status_class = 'danger'
        else:
            status = 'EXTREME OVERVALUATION'
            interpretation = 'Market at historically extreme levels. Buffett warns this signals danger ahead.'
            status_class = 'danger'

        return {
            'status': status,
            'interpretation': interpretation,
            'status_class': status_class,
            'percentile': percentile
        }

    @staticmethod
    def interpret_m2_gdp(value: float, yoy_change: float = None) -> Dict:
        """
        Interpret M2/GDP ratio

        Args:
            value: Current M2/GDP ratio (percentage)
            yoy_change: Year-over-year change in the ratio

        Returns:
            Dict with status, interpretation, and status_class
        """
        if value is None:
            return {
                'status': 'N/A',
                'interpretation': 'Data not available',
                'status_class': 'neutral'
            }

        # Historical context: Pre-2020, M2/GDP was typically 60-70%
        # Post-2020 QE pushed it to 90%+
        if value < 70:
            status = 'NORMAL'
            interpretation = 'Money supply in line with historical norms relative to economic output.'
            status_class = 'neutral'
        elif value < 85:
            status = 'ELEVATED'
            interpretation = 'Money supply elevated vs GDP. Some inflationary pressure possible.'
            status_class = 'warning'
        else:
            status = 'HIGHLY ELEVATED'
            interpretation = 'Money supply significantly exceeds historical norms. Risk of asset inflation and currency devaluation.'
            status_class = 'danger'

        # Add trend context
        trend_note = ''
        if yoy_change is not None:
            if yoy_change > 5:
                trend_note = ' Ratio rising rapidly - aggressive monetary expansion.'
            elif yoy_change < -3:
                trend_note = ' Ratio declining - monetary tightening taking effect.'

        return {
            'status': status,
            'interpretation': interpretation + trend_note,
            'status_class': status_class
        }

    @staticmethod
    def interpret_debt_gdp(value: float, historical_comparison: Dict = None) -> Dict:
        """
        Interpret Federal Debt/GDP ratio

        Thresholds:
        - < 60%: Sustainable
        - 60-90%: Elevated
        - 90-120%: High
        - > 120%: Very High / Concerning

        Args:
            value: Current Debt/GDP ratio (percentage)
            historical_comparison: Dict with historical values

        Returns:
            Dict with status, interpretation, and status_class
        """
        if value is None:
            return {
                'status': 'N/A',
                'interpretation': 'Data not available',
                'status_class': 'neutral'
            }

        if value < 60:
            status = 'SUSTAINABLE'
            interpretation = 'Debt levels manageable relative to economic output. Fiscal flexibility exists.'
            status_class = 'positive'
        elif value < 90:
            status = 'ELEVATED'
            interpretation = 'Debt approaching concerning levels. Limited fiscal flexibility for future crises.'
            status_class = 'warning'
        elif value < 120:
            status = 'HIGH'
            interpretation = 'Debt burden heavy. Interest payments consuming significant government revenue.'
            status_class = 'danger'
        else:
            status = 'VERY HIGH'
            interpretation = 'Debt at historically extreme levels. Risk of debt spiral if rates rise further.'
            status_class = 'danger'

        return {
            'status': status,
            'interpretation': interpretation,
            'status_class': status_class,
            'historical_comparison': historical_comparison
        }

    @staticmethod
    def interpret_m2_velocity(value: float, historical_avg: float = None) -> Dict:
        """
        Interpret M2 Velocity (GDP / M2)

        Velocity shows how quickly money circulates. Lower velocity means
        money is being saved rather than spent.

        Args:
            value: Current M2 velocity
            historical_avg: Historical average for context

        Returns:
            Dict with status, interpretation, and status_class
        """
        if value is None:
            return {
                'status': 'N/A',
                'interpretation': 'Data not available',
                'status_class': 'neutral'
            }

        # Historical context: Pre-2008 velocity was ~1.9-2.0
        # Post-2008 declined to ~1.4-1.5
        # Post-2020 dropped to ~1.1-1.2
        if value > 1.7:
            status = 'NORMAL'
            interpretation = 'Money circulating at healthy pace. Active economic activity.'
            status_class = 'positive'
        elif value > 1.4:
            status = 'BELOW AVERAGE'
            interpretation = 'Money moving slower than historical norms. Cautious spending/saving behavior.'
            status_class = 'warning'
        elif value > 1.2:
            status = 'LOW'
            interpretation = 'Money velocity depressed. Significant savings or weak demand.'
            status_class = 'warning'
        else:
            status = 'VERY LOW'
            interpretation = 'Historically low velocity. Money being hoarded, not circulating. Deflationary pressure despite money printing.'
            status_class = 'danger'

        # Add historical context
        if historical_avg is not None:
            pct_below = ((historical_avg - value) / historical_avg) * 100
            if pct_below > 20:
                interpretation += f' Currently {pct_below:.0f}% below historical average.'

        return {
            'status': status,
            'interpretation': interpretation,
            'status_class': status_class
        }

    @staticmethod
    def get_global_economy_summary(buffett: Dict, m2_gdp: Dict, debt_gdp: Dict, velocity: Dict) -> Dict:
        """
        Generate overall economic assessment summary

        Args:
            buffett: Buffett Indicator data and interpretation
            m2_gdp: M2/GDP data and interpretation
            debt_gdp: Debt/GDP data and interpretation
            velocity: M2 Velocity data and interpretation

        Returns:
            Dict with overall assessment
        """
        risk_score = 0
        signals = []

        # Score based on Buffett Indicator
        if buffett.get('current'):
            bi = buffett['current']
            if bi > 180:
                risk_score += 3
                signals.append("Buffett Indicator at extreme levels (>180%)")
            elif bi > 140:
                risk_score += 2
                signals.append("Buffett Indicator signals overvaluation (>140%)")
            elif bi > 115:
                risk_score += 1
                signals.append("Buffett Indicator modestly elevated")

        # Score based on M2/GDP
        if m2_gdp.get('current'):
            m2 = m2_gdp['current']
            if m2 > 90:
                risk_score += 2
                signals.append("M2/GDP ratio at historic highs - excess liquidity")
            elif m2 > 80:
                risk_score += 1
                signals.append("M2/GDP elevated above historical norms")

        # Score based on Debt/GDP
        if debt_gdp.get('current'):
            debt = debt_gdp['current']
            if debt > 120:
                risk_score += 2
                signals.append("Federal debt exceeds 120% of GDP")
            elif debt > 100:
                risk_score += 1
                signals.append("Federal debt exceeds 100% of GDP")

        # Determine overall risk level
        if risk_score >= 6:
            risk_level = "HIGH"
            risk_class = "danger"
            summary = "Multiple indicators signal significant macro risks. Caution warranted."
        elif risk_score >= 4:
            risk_level = "ELEVATED"
            risk_class = "warning"
            summary = "Several macro indicators showing warning signs."
        elif risk_score >= 2:
            risk_level = "MODERATE"
            risk_class = "caution"
            summary = "Some macro concerns but not at extreme levels."
        else:
            risk_level = "LOW"
            risk_class = "normal"
            summary = "Macro indicators within normal ranges."

        return {
            'risk_level': risk_level,
            'risk_class': risk_class,
            'risk_score': risk_score,
            'summary': summary,
            'signals': signals
        }

    # =========================================================================
    # REAL ESTATE INTERPRETATIONS
    # =========================================================================

    @staticmethod
    def interpret_housing_supply(months_supply: float) -> Dict:
        """
        Interpret housing months supply

        Thresholds:
        - < 3 months: Strong seller's market
        - 3-4 months: Seller's market
        - 4-6 months: Balanced market
        - 6-8 months: Buyer's market
        - > 8 months: Strong buyer's market

        Args:
            months_supply: Current months supply

        Returns:
            Dict with status, interpretation, and status_class
        """
        if months_supply is None:
            return {
                'status': 'N/A',
                'interpretation': 'Data not available',
                'status_class': 'neutral',
                'market_type': 'unknown'
            }

        if months_supply < 3:
            status = 'VERY LOW'
            interpretation = 'Severe inventory shortage. Strong seller\'s market with bidding wars likely.'
            status_class = 'danger'
            market_type = "Strong Seller's Market"
        elif months_supply < 4:
            status = 'LOW'
            interpretation = 'Limited inventory. Seller\'s market with upward price pressure.'
            status_class = 'warning'
            market_type = "Seller's Market"
        elif months_supply < 6:
            status = 'BALANCED'
            interpretation = 'Healthy supply-demand balance. Neither buyers nor sellers have clear advantage.'
            status_class = 'neutral'
            market_type = 'Balanced Market'
        elif months_supply < 8:
            status = 'ELEVATED'
            interpretation = 'Buyer\'s market. More negotiating power for purchasers.'
            status_class = 'positive'
            market_type = "Buyer's Market"
        else:
            status = 'HIGH'
            interpretation = 'Significant oversupply. Strong buyer\'s market with potential price declines.'
            status_class = 'positive'
            market_type = "Strong Buyer's Market"

        return {
            'status': status,
            'interpretation': interpretation,
            'status_class': status_class,
            'market_type': market_type
        }

    @staticmethod
    def interpret_housing_affordability(index: float, historical_avg: float = None) -> Dict:
        """
        Interpret Housing Affordability Index

        Index value of 100 = median income family can afford median home
        Higher = more affordable, Lower = less affordable

        Args:
            index: Current affordability index
            historical_avg: Historical average for context

        Returns:
            Dict with status, interpretation, and status_class
        """
        if index is None:
            return {
                'status': 'N/A',
                'interpretation': 'Data not available',
                'status_class': 'neutral'
            }

        if index > 150:
            status = 'VERY AFFORDABLE'
            interpretation = 'Housing highly affordable for median-income families.'
            status_class = 'positive'
        elif index > 120:
            status = 'AFFORDABLE'
            interpretation = 'Housing reasonably affordable. Most families can qualify for median home.'
            status_class = 'positive'
        elif index > 100:
            status = 'MODERATELY AFFORDABLE'
            interpretation = 'Housing somewhat stretched but still accessible for median earners.'
            status_class = 'neutral'
        elif index > 80:
            status = 'UNAFFORDABLE'
            interpretation = 'Housing becoming unaffordable. Median family cannot afford median home.'
            status_class = 'warning'
        else:
            status = 'VERY UNAFFORDABLE'
            interpretation = 'Severe affordability crisis. Significant income gap to homeownership.'
            status_class = 'danger'

        # Add historical context
        if historical_avg is not None:
            pct_diff = ((index - historical_avg) / historical_avg) * 100
            if pct_diff < -20:
                interpretation += f' Currently {abs(pct_diff):.0f}% less affordable than historical average.'
            elif pct_diff > 20:
                interpretation += f' Currently {pct_diff:.0f}% more affordable than historical average.'

        return {
            'status': status,
            'interpretation': interpretation,
            'status_class': status_class
        }

    @staticmethod
    def interpret_case_shiller(yoy_change: float, index_value: float = None) -> Dict:
        """
        Interpret Case-Shiller home price changes

        Args:
            yoy_change: Year-over-year percentage change
            index_value: Current index value (optional)

        Returns:
            Dict with status, interpretation, and status_class
        """
        if yoy_change is None:
            return {
                'status': 'N/A',
                'interpretation': 'Data not available',
                'status_class': 'neutral'
            }

        if yoy_change > 15:
            status = 'RAPID APPRECIATION'
            interpretation = 'Home prices rising at unsustainable pace. Potential bubble concerns.'
            status_class = 'danger'
        elif yoy_change > 8:
            status = 'STRONG APPRECIATION'
            interpretation = 'Home prices rising faster than historical norms.'
            status_class = 'warning'
        elif yoy_change > 3:
            status = 'MODERATE APPRECIATION'
            interpretation = 'Healthy price growth in line with inflation and income growth.'
            status_class = 'neutral'
        elif yoy_change > 0:
            status = 'SLIGHT APPRECIATION'
            interpretation = 'Home prices relatively flat. Stable market conditions.'
            status_class = 'neutral'
        elif yoy_change > -5:
            status = 'SLIGHT DECLINE'
            interpretation = 'Home prices softening. Market cooling or correction underway.'
            status_class = 'warning'
        else:
            status = 'SIGNIFICANT DECLINE'
            interpretation = 'Home prices falling notably. Potential distressed market.'
            status_class = 'danger'

        return {
            'status': status,
            'interpretation': interpretation,
            'status_class': status_class
        }

    @staticmethod
    def interpret_mortgage_rate(rate: float, historical_avg: float = None) -> Dict:
        """
        Interpret 30-year mortgage rate

        Args:
            rate: Current 30-year mortgage rate
            historical_avg: Historical average for context

        Returns:
            Dict with status, interpretation, and status_class
        """
        if rate is None:
            return {
                'status': 'N/A',
                'interpretation': 'Data not available',
                'status_class': 'neutral'
            }

        # Historical context: Long-term average ~7-8%, 2010s average ~4%
        if rate < 4:
            status = 'VERY LOW'
            interpretation = 'Historically low rates. Excellent borrowing conditions.'
            status_class = 'positive'
        elif rate < 5.5:
            status = 'LOW'
            interpretation = 'Below-average rates. Favorable financing environment.'
            status_class = 'positive'
        elif rate < 7:
            status = 'MODERATE'
            interpretation = 'Rates near historical norms. Standard financing conditions.'
            status_class = 'neutral'
        elif rate < 8:
            status = 'ELEVATED'
            interpretation = 'Above-average rates. Higher monthly payments impacting affordability.'
            status_class = 'warning'
        else:
            status = 'HIGH'
            interpretation = 'High rates significantly impacting affordability and housing demand.'
            status_class = 'danger'

        return {
            'status': status,
            'interpretation': interpretation,
            'status_class': status_class
        }

    @staticmethod
    def get_real_estate_summary(case_shiller: Dict, supply: Dict, affordability: Dict, mortgage: Dict) -> Dict:
        """
        Generate overall real estate market assessment

        Args:
            case_shiller: Case-Shiller data
            supply: Housing supply data
            affordability: Affordability data
            mortgage: Mortgage rate data

        Returns:
            Dict with overall assessment
        """
        risk_score = 0
        signals = []
        opportunities = []

        # Analyze price trends
        if case_shiller.get('national', {}).get('yoy_change'):
            yoy = case_shiller['national']['yoy_change']
            if yoy > 15:
                risk_score += 2
                signals.append("Home prices rising at unsustainable pace")
            elif yoy < -5:
                risk_score += 1
                signals.append("Home prices declining - potential distress")
                opportunities.append("Price correction may create buying opportunities")

        # Analyze supply
        if supply.get('existing_months_supply'):
            months = supply['existing_months_supply']
            if months < 3:
                risk_score += 1
                signals.append("Very low inventory constraining market")
            elif months > 6:
                opportunities.append("Elevated inventory favors buyers")

        # Analyze affordability
        if affordability.get('current'):
            aff = affordability['current']
            if aff < 100:
                risk_score += 2
                signals.append("Housing unaffordable for median families")
            elif aff < 120:
                risk_score += 1
                signals.append("Affordability stretched")

        # Analyze mortgage rates
        if mortgage.get('current'):
            rate = mortgage['current']
            if rate > 7:
                risk_score += 1
                signals.append("Elevated mortgage rates dampening demand")

        # Determine overall assessment
        if risk_score >= 5:
            risk_level = "STRESSED"
            risk_class = "danger"
            summary = "Multiple stress indicators in housing market."
        elif risk_score >= 3:
            risk_level = "CHALLENGING"
            risk_class = "warning"
            summary = "Housing market facing headwinds."
        elif risk_score >= 1:
            risk_level = "MIXED"
            risk_class = "caution"
            summary = "Housing market showing some concerns but stable overall."
        else:
            risk_level = "HEALTHY"
            risk_class = "normal"
            summary = "Housing market conditions appear stable."

        return {
            'risk_level': risk_level,
            'risk_class': risk_class,
            'risk_score': risk_score,
            'summary': summary,
            'signals': signals,
            'opportunities': opportunities
        }
