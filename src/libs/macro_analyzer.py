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

    # =========================================================================
    # INFLATION & FED POLICY INTERPRETATIONS
    # =========================================================================

    @staticmethod
    def interpret_inflation(cpi: float, core_cpi: float, pce: float, core_pce: float) -> Dict:
        """
        Interpret inflation metrics and Fed target alignment

        The Fed targets 2% Core PCE inflation.

        Args:
            cpi: CPI YoY %
            core_cpi: Core CPI YoY %
            pce: PCE YoY %
            core_pce: Core PCE YoY % (Fed's primary target)

        Returns:
            Dict with status, interpretation, and status_class
        """
        # Use Core PCE as primary metric (Fed's target)
        target = core_pce if core_pce is not None else core_cpi

        if target is None:
            return {
                'status': 'N/A',
                'interpretation': 'Data not available',
                'status_class': 'neutral',
                'vs_target': None
            }

        vs_target = target - 2.0  # How far from 2% target

        if target < 1.0:
            status = 'DEFLATION RISK'
            interpretation = 'Inflation well below target. Risk of deflationary spiral. Fed likely to ease.'
            status_class = 'warning'
        elif target < 1.5:
            status = 'BELOW TARGET'
            interpretation = 'Inflation running below Fed\'s 2% target. Accommodative policy likely.'
            status_class = 'positive'
        elif target < 2.5:
            status = 'AT TARGET'
            interpretation = 'Inflation near Fed\'s 2% target. Policy likely stable.'
            status_class = 'positive'
        elif target < 3.5:
            status = 'ABOVE TARGET'
            interpretation = 'Inflation above target. Fed may maintain restrictive stance.'
            status_class = 'warning'
        elif target < 5.0:
            status = 'ELEVATED'
            interpretation = 'Inflation significantly above target. Fed tightening likely to continue.'
            status_class = 'danger'
        else:
            status = 'HIGH'
            interpretation = 'Inflation at concerning levels. Aggressive Fed action expected.'
            status_class = 'danger'

        return {
            'status': status,
            'interpretation': interpretation,
            'status_class': status_class,
            'vs_target': round(vs_target, 2),
            'primary_metric': 'Core PCE' if core_pce is not None else 'Core CPI'
        }

    @staticmethod
    def interpret_real_rate(real_rate: float) -> Dict:
        """
        Interpret real interest rate (Fed Funds - Core PCE)

        Real rate indicates actual monetary policy stance:
        - Negative: Stimulative (borrowing costs below inflation)
        - Positive: Restrictive (borrowing costs above inflation)

        Args:
            real_rate: Current real rate (Fed Funds - Core PCE)

        Returns:
            Dict with status, interpretation, and status_class
        """
        if real_rate is None:
            return {
                'status': 'N/A',
                'interpretation': 'Data not available',
                'status_class': 'neutral'
            }

        if real_rate < -2:
            status = 'VERY ACCOMMODATIVE'
            interpretation = 'Deeply negative real rates. Strongly stimulative policy encouraging risk-taking.'
            status_class = 'warning'
        elif real_rate < 0:
            status = 'ACCOMMODATIVE'
            interpretation = 'Negative real rates. Monetary policy still stimulative despite nominal rate level.'
            status_class = 'positive'
        elif real_rate < 1:
            status = 'NEUTRAL'
            interpretation = 'Real rates near neutral. Policy neither stimulating nor restricting.'
            status_class = 'neutral'
        elif real_rate < 2:
            status = 'RESTRICTIVE'
            interpretation = 'Positive real rates. Policy is tightening financial conditions.'
            status_class = 'warning'
        else:
            status = 'VERY RESTRICTIVE'
            interpretation = 'Highly positive real rates. Aggressive tightening affecting economy.'
            status_class = 'danger'

        return {
            'status': status,
            'interpretation': interpretation,
            'status_class': status_class
        }

    @staticmethod
    def interpret_fed_balance_sheet(current_trillions: float, yoy_change_pct: float, trend: str) -> Dict:
        """
        Interpret Fed balance sheet size and trend

        QE (expansion) adds liquidity, supports asset prices
        QT (contraction) removes liquidity, can pressure assets

        Args:
            current_trillions: Current balance sheet size in $T
            yoy_change_pct: Year-over-year change percentage
            trend: QE/QT/STABLE trend indicator

        Returns:
            Dict with status, interpretation, and status_class
        """
        if current_trillions is None:
            return {
                'status': 'N/A',
                'interpretation': 'Data not available',
                'status_class': 'neutral'
            }

        if trend == 'QE':
            status = 'EXPANDING (QE)'
            interpretation = 'Fed actively adding liquidity. Supports asset prices and risk appetite.'
            status_class = 'positive'
        elif trend == 'QT':
            status = 'CONTRACTING (QT)'
            interpretation = 'Fed reducing balance sheet. Draining liquidity, headwind for assets.'
            status_class = 'warning'
        else:
            status = 'STABLE'
            interpretation = 'Balance sheet roughly stable. Neutral liquidity impact.'
            status_class = 'neutral'

        # Add size context
        if current_trillions > 8:
            interpretation += f' At ${current_trillions:.1f}T, balance sheet remains historically elevated.'
        elif current_trillions > 5:
            interpretation += f' At ${current_trillions:.1f}T, balance sheet moderating from peak.'

        return {
            'status': status,
            'interpretation': interpretation,
            'status_class': status_class,
            'yoy_change': yoy_change_pct
        }

    @staticmethod
    def interpret_breakeven_inflation(be_5y: float, be_10y: float) -> Dict:
        """
        Interpret market inflation expectations from TIPS breakevens

        Breakeven rates show what inflation the market is pricing in.

        Args:
            be_5y: 5-year breakeven inflation rate
            be_10y: 10-year breakeven inflation rate

        Returns:
            Dict with status, interpretation, and status_class
        """
        # Use 5Y as primary (more responsive to near-term expectations)
        be = be_5y if be_5y is not None else be_10y

        if be is None:
            return {
                'status': 'N/A',
                'interpretation': 'Data not available',
                'status_class': 'neutral'
            }

        if be < 1.5:
            status = 'LOW EXPECTATIONS'
            interpretation = 'Market expects inflation well below Fed target. Deflationary concerns.'
            status_class = 'warning'
        elif be < 2.0:
            status = 'BELOW TARGET'
            interpretation = 'Market expects inflation below Fed\'s 2% target.'
            status_class = 'neutral'
        elif be < 2.5:
            status = 'ANCHORED'
            interpretation = 'Market expectations well-anchored near Fed\'s 2% target. Healthy sign.'
            status_class = 'positive'
        elif be < 3.0:
            status = 'ABOVE TARGET'
            interpretation = 'Market expects inflation above target. Some inflation concerns priced in.'
            status_class = 'warning'
        else:
            status = 'ELEVATED EXPECTATIONS'
            interpretation = 'Market pricing in persistently high inflation. Fed credibility in question.'
            status_class = 'danger'

        return {
            'status': status,
            'interpretation': interpretation,
            'status_class': status_class,
            '5y_expectation': be_5y,
            '10y_expectation': be_10y
        }

    @staticmethod
    def get_fed_policy_summary(inflation: Dict, real_rate: Dict, balance_sheet: Dict, breakevens: Dict) -> Dict:
        """
        Generate overall Fed policy assessment

        Args:
            inflation: Inflation data and interpretation
            real_rate: Real rate data and interpretation
            balance_sheet: Balance sheet data and interpretation
            breakevens: Breakeven inflation data and interpretation

        Returns:
            Dict with overall hawkish/dovish assessment
        """
        hawkish_score = 0  # Positive = hawkish, Negative = dovish
        signals = []

        # Inflation vs target
        if inflation.get('vs_target') is not None:
            vs_target = inflation['vs_target']
            if vs_target > 1.5:
                hawkish_score += 2
                signals.append("Inflation well above target - hawkish pressure")
            elif vs_target > 0.5:
                hawkish_score += 1
                signals.append("Inflation above target")
            elif vs_target < -0.5:
                hawkish_score -= 1
                signals.append("Inflation below target - dovish pressure")

        # Real rate stance
        if real_rate.get('current') is not None:
            rr = real_rate['current']
            if rr > 1.5:
                hawkish_score += 1
                signals.append("Real rates restrictive")
            elif rr < 0:
                hawkish_score -= 1
                signals.append("Real rates still accommodative")

        # Balance sheet
        if balance_sheet.get('trend'):
            if balance_sheet['trend'] == 'QT':
                hawkish_score += 1
                signals.append("QT in progress - tightening liquidity")
            elif balance_sheet['trend'] == 'QE':
                hawkish_score -= 2
                signals.append("QE in progress - adding liquidity")

        # Determine overall stance
        if hawkish_score >= 3:
            stance = 'HAWKISH'
            stance_class = 'danger'
            summary = 'Fed policy firmly in tightening mode. Risk assets face headwinds.'
        elif hawkish_score >= 1:
            stance = 'MODERATELY HAWKISH'
            stance_class = 'warning'
            summary = 'Fed maintaining restrictive stance but may be nearing pause.'
        elif hawkish_score >= -1:
            stance = 'NEUTRAL'
            stance_class = 'neutral'
            summary = 'Fed policy in wait-and-see mode. Data dependent.'
        elif hawkish_score >= -3:
            stance = 'MODERATELY DOVISH'
            stance_class = 'positive'
            summary = 'Fed leaning toward easing. Supportive for risk assets.'
        else:
            stance = 'DOVISH'
            stance_class = 'positive'
            summary = 'Fed in active easing mode. Strong tailwind for assets.'

        return {
            'stance': stance,
            'stance_class': stance_class,
            'hawkish_score': hawkish_score,
            'summary': summary,
            'signals': signals
        }

    # =========================================================================
    # MARKET SENTIMENT INTERPRETATIONS
    # =========================================================================

    @staticmethod
    def interpret_vix(vix_level: float, percentile: float = None) -> Dict:
        """
        Interpret VIX level

        VIX thresholds:
        - <12: Extreme complacency (contrarian sell)
        - 12-20: Low volatility (bullish environment)
        - 20-30: Elevated (caution warranted)
        - 30-40: High fear (contrarian buy zone)
        - >40: Panic (extreme contrarian buy)

        Args:
            vix_level: Current VIX value
            percentile: Historical percentile

        Returns:
            Dict with status, interpretation, and status_class
        """
        if vix_level is None:
            return {
                'status': 'N/A',
                'interpretation': 'Data not available',
                'status_class': 'neutral'
            }

        if vix_level < 12:
            status = 'EXTREME COMPLACENCY'
            interpretation = 'VIX at extreme lows. Market very complacent. Contrarian warning sign.'
            status_class = 'warning'
            contrarian = 'SELL SIGNAL'
        elif vix_level < 16:
            status = 'LOW'
            interpretation = 'Low volatility environment. Bullish but watch for complacency.'
            status_class = 'positive'
            contrarian = None
        elif vix_level < 20:
            status = 'NORMAL'
            interpretation = 'Volatility in normal range. Typical market conditions.'
            status_class = 'neutral'
            contrarian = None
        elif vix_level < 25:
            status = 'ELEVATED'
            interpretation = 'Above-average volatility. Some caution warranted.'
            status_class = 'warning'
            contrarian = None
        elif vix_level < 30:
            status = 'HIGH'
            interpretation = 'High volatility indicating market stress. Fear rising.'
            status_class = 'warning'
            contrarian = 'POSSIBLE BUY'
        elif vix_level < 40:
            status = 'VERY HIGH'
            interpretation = 'Significant fear in markets. Historically good contrarian buy zone.'
            status_class = 'danger'
            contrarian = 'BUY SIGNAL'
        else:
            status = 'PANIC'
            interpretation = 'Extreme panic levels. Major market stress. Historic buying opportunity.'
            status_class = 'danger'
            contrarian = 'STRONG BUY'

        return {
            'status': status,
            'interpretation': interpretation,
            'status_class': status_class,
            'contrarian_signal': contrarian,
            'percentile': percentile
        }

    @staticmethod
    def interpret_vix_term_structure(term_structure: float, status: str) -> Dict:
        """
        Interpret VIX term structure (VIX3M - VIX)

        - Contango (positive): Normal market, VIX3M > VIX
        - Backwardation (negative): Stressed market, VIX > VIX3M

        Args:
            term_structure: VIX3M - VIX difference
            status: CONTANGO/FLAT/BACKWARDATION

        Returns:
            Dict with interpretation
        """
        if term_structure is None:
            return {
                'status': 'N/A',
                'interpretation': 'Data not available',
                'status_class': 'neutral'
            }

        if status == 'CONTANGO':
            interpretation = 'Normal term structure. Market expects current volatility to persist or decline.'
            status_class = 'positive'
        elif status == 'BACKWARDATION':
            interpretation = 'Inverted term structure. Near-term fear exceeds longer-term. Stress signal.'
            status_class = 'danger'
        else:
            interpretation = 'Flat term structure. Market uncertain about volatility direction.'
            status_class = 'warning'

        return {
            'status': status,
            'interpretation': interpretation,
            'status_class': status_class,
            'spread': term_structure
        }

    @staticmethod
    def interpret_fear_greed(score: float, status: str) -> Dict:
        """
        Interpret Fear & Greed index score

        Args:
            score: Overall index (0-100)
            status: EXTREME FEAR/FEAR/NEUTRAL/GREED/EXTREME GREED

        Returns:
            Dict with interpretation and contrarian signal
        """
        if score is None:
            return {
                'status': 'N/A',
                'interpretation': 'Data not available',
                'status_class': 'neutral',
                'contrarian': None
            }

        if score < 20:
            interpretation = 'Extreme fear. Markets deeply pessimistic. Historic buying opportunity.'
            status_class = 'danger'
            contrarian = 'STRONG BUY'
        elif score < 40:
            interpretation = 'Fear dominates. Investors cautious. Potential buying opportunity.'
            status_class = 'warning'
            contrarian = 'BUY'
        elif score < 60:
            interpretation = 'Neutral sentiment. Neither fear nor greed prevailing.'
            status_class = 'neutral'
            contrarian = None
        elif score < 80:
            interpretation = 'Greed rising. Markets optimistic. Consider taking profits.'
            status_class = 'warning'
            contrarian = 'CAUTION'
        else:
            interpretation = 'Extreme greed. Euphoria in markets. Contrarian warning sign.'
            status_class = 'danger'
            contrarian = 'SELL'

        return {
            'status': status,
            'interpretation': interpretation,
            'status_class': status_class,
            'contrarian': contrarian,
            'score': score
        }

    @staticmethod
    def get_sentiment_summary(fear_greed: Dict, vix: Dict, credit_spreads: Dict, sp500_trend: Dict) -> Dict:
        """
        Generate overall market sentiment summary

        Args:
            fear_greed: Fear & Greed index data
            vix: VIX data and interpretation
            credit_spreads: Credit spread data
            sp500_trend: S&P 500 trend data

        Returns:
            Dict with overall sentiment assessment
        """
        signals = []
        risk_appetite_score = 0  # Positive = risk-on, Negative = risk-off

        # Fear & Greed contribution
        if fear_greed.get('overall') is not None:
            fg = fear_greed['overall']
            if fg > 70:
                risk_appetite_score += 2
                signals.append("Fear & Greed showing elevated greed")
            elif fg > 55:
                risk_appetite_score += 1
                signals.append("Sentiment leaning bullish")
            elif fg < 30:
                risk_appetite_score -= 2
                signals.append("Fear & Greed showing significant fear")
            elif fg < 45:
                risk_appetite_score -= 1
                signals.append("Sentiment leaning cautious")

        # VIX contribution
        if vix.get('vix', {}).get('current'):
            vix_val = vix['vix']['current']
            if vix_val < 15:
                risk_appetite_score += 1
                signals.append("VIX low - complacency")
            elif vix_val > 25:
                risk_appetite_score -= 1
                signals.append("VIX elevated - fear rising")
            elif vix_val > 35:
                risk_appetite_score -= 2
                signals.append("VIX very high - significant fear")

        # Credit spreads contribution
        if credit_spreads.get('high_yield', {}).get('percentile'):
            pct = credit_spreads['high_yield']['percentile']
            if pct < 25:
                risk_appetite_score += 1
                signals.append("Credit spreads tight - risk appetite strong")
            elif pct > 75:
                risk_appetite_score -= 1
                signals.append("Credit spreads wide - credit stress")

        # S&P trend contribution
        if sp500_trend.get('trend'):
            trend = sp500_trend['trend']
            if trend == 'UPTREND':
                risk_appetite_score += 1
                signals.append("S&P 500 in uptrend")
            elif trend == 'DOWNTREND':
                risk_appetite_score -= 1
                signals.append("S&P 500 in downtrend")

        # Determine overall sentiment
        if risk_appetite_score >= 4:
            sentiment = 'RISK-ON'
            sentiment_class = 'positive'
            summary = 'Strong risk appetite across indicators. Bullish environment.'
        elif risk_appetite_score >= 2:
            sentiment = 'MODERATELY RISK-ON'
            sentiment_class = 'positive'
            summary = 'Positive sentiment. Markets generally optimistic.'
        elif risk_appetite_score >= -1:
            sentiment = 'NEUTRAL'
            sentiment_class = 'neutral'
            summary = 'Mixed signals. Neither strong fear nor greed.'
        elif risk_appetite_score >= -3:
            sentiment = 'MODERATELY RISK-OFF'
            sentiment_class = 'warning'
            summary = 'Cautious sentiment. Some defensive positioning warranted.'
        else:
            sentiment = 'RISK-OFF'
            sentiment_class = 'danger'
            summary = 'Significant fear across indicators. Defensive stance appropriate.'

        return {
            'sentiment': sentiment,
            'sentiment_class': sentiment_class,
            'risk_score': risk_appetite_score,
            'summary': summary,
            'signals': signals
        }

    # =========================================================================
    # CONSUMER SENTIMENT INTERPRETATION
    # =========================================================================

    @staticmethod
    def interpret_sp500_sentiment_divergence(
        sp500_1y_return: float,
        sentiment_current: float,
        sentiment_1y_ago: float
    ) -> Dict:
        """
        Analyze divergence between S&P 500 performance and consumer sentiment

        A divergence can signal:
        - Market rising + sentiment falling = potential exhaustion
        - Market falling + sentiment rising = potential bottoming

        Args:
            sp500_1y_return: S&P 500 1-year return percentage
            sentiment_current: Current consumer sentiment value
            sentiment_1y_ago: Consumer sentiment 1 year ago

        Returns:
            Dict with status, interpretation, and status_class
        """
        if sp500_1y_return is None or sentiment_current is None or sentiment_1y_ago is None:
            return {
                'status': 'INSUFFICIENT DATA',
                'interpretation': 'Not enough data to analyze divergence.',
                'status_class': 'neutral'
            }

        sentiment_change = sentiment_current - sentiment_1y_ago
        sentiment_change_pct = (sentiment_change / sentiment_1y_ago) * 100

        # Bullish divergence: market up, sentiment down
        if sp500_1y_return > 10 and sentiment_change < -5:
            status = 'BULLISH DIVERGENCE'
            interpretation = (
                f'Market up {sp500_1y_return:.1f}% while consumer sentiment fell {-sentiment_change:.1f} pts. '
                'This disconnect may signal late-cycle exhaustion - consumers are not feeling the market gains.'
            )
            status_class = 'warning'

        # Bearish divergence: market down, sentiment up
        elif sp500_1y_return < -10 and sentiment_change > 5:
            status = 'BEARISH DIVERGENCE'
            interpretation = (
                f'Market down {-sp500_1y_return:.1f}% while consumer sentiment rose {sentiment_change:.1f} pts. '
                'Consumers may be underestimating economic headwinds, or market overreacted.'
            )
            status_class = 'warning'

        # Contrarian buy signal: both very negative
        elif sp500_1y_return < -15 and sentiment_current < 65:
            status = 'CONTRARIAN BUY SIGNAL'
            interpretation = (
                f'Both market ({sp500_1y_return:.1f}%) and sentiment ({sentiment_current:.0f}) deeply negative. '
                'Historically, extreme pessimism has preceded market rebounds.'
            )
            status_class = 'positive'

        # Contrarian sell signal: both very positive
        elif sp500_1y_return > 25 and sentiment_current > 95:
            status = 'CONTRARIAN SELL SIGNAL'
            interpretation = (
                f'Both market ({sp500_1y_return:.1f}%) and sentiment ({sentiment_current:.0f}) very elevated. '
                'Extreme optimism often precedes corrections.'
            )
            status_class = 'warning'

        # Aligned positive
        elif sp500_1y_return > 5 and sentiment_change > 0:
            status = 'ALIGNED POSITIVE'
            interpretation = (
                f'Market and sentiment moving together positively. '
                'Healthy bull market confirmation.'
            )
            status_class = 'positive'

        # Aligned negative
        elif sp500_1y_return < -5 and sentiment_change < 0:
            status = 'ALIGNED NEGATIVE'
            interpretation = (
                f'Market and sentiment moving together negatively. '
                'Economic concerns reflected in both indicators.'
            )
            status_class = 'warning'

        else:
            status = 'NEUTRAL'
            interpretation = 'Market and sentiment showing no significant divergence.'
            status_class = 'neutral'

        return {
            'status': status,
            'interpretation': interpretation,
            'status_class': status_class,
            'sp500_return': sp500_1y_return,
            'sentiment_change': round(sentiment_change, 1),
            'sentiment_change_pct': round(sentiment_change_pct, 1)
        }

    # =========================================================================
    # MONEY MARKET FUNDS INTERPRETATION
    # =========================================================================

    @staticmethod
    def interpret_money_market_funds(
        current_trillions: float,
        yoy_change: float,
        at_ath: bool
    ) -> Dict:
        """
        Interpret Money Market Fund levels as liquidity indicator

        High MMF assets = cash on sidelines (potential buying power)
        Low/declining MMF = money flowing into risk assets

        Args:
            current_trillions: Current MMF total assets in trillions
            yoy_change: Year-over-year change percentage
            at_ath: Whether currently at or near all-time high

        Returns:
            Dict with status, interpretation, and status_class
        """
        if current_trillions is None:
            return {
                'status': 'NO DATA',
                'interpretation': 'Money market fund data not available.',
                'status_class': 'neutral'
            }

        if at_ath and yoy_change and yoy_change > 10:
            status = 'RECORD HIGH LIQUIDITY'
            interpretation = (
                f'Money market funds at record ${current_trillions:.1f}T (+{yoy_change:.0f}% YoY). '
                'Unprecedented cash on sidelines. Investors defensive, but this represents '
                'significant potential buying power if sentiment shifts.'
            )
            status_class = 'warning'

        elif yoy_change and yoy_change > 15:
            status = 'RAPIDLY RISING'
            interpretation = (
                f'MMF assets growing fast (+{yoy_change:.0f}% YoY to ${current_trillions:.1f}T). '
                'Cash accumulating quickly - investors seeking safety. '
                'Could signal fear or preparation for opportunities.'
            )
            status_class = 'warning'

        elif yoy_change and yoy_change > 5:
            status = 'ELEVATED'
            interpretation = (
                f'MMF assets at ${current_trillions:.1f}T (+{yoy_change:.0f}% YoY). '
                'Above-average cash levels. Some caution in the market.'
            )
            status_class = 'neutral'

        elif yoy_change and yoy_change < -10:
            status = 'DECLINING FAST'
            interpretation = (
                f'MMF assets falling ({yoy_change:.0f}% YoY to ${current_trillions:.1f}T). '
                'Cash leaving money markets - likely flowing into risk assets. '
                'Strong risk-on behavior.'
            )
            status_class = 'positive'

        elif yoy_change and yoy_change < -5:
            status = 'DECLINING'
            interpretation = (
                f'MMF assets declining ({yoy_change:.0f}% YoY to ${current_trillions:.1f}T). '
                'Money moving from cash to investments. Modest risk-on signal.'
            )
            status_class = 'positive'

        else:
            status = 'STABLE'
            interpretation = (
                f'MMF assets at ${current_trillions:.1f}T. '
                'Normal cash levels with no strong directional signal.'
            )
            status_class = 'neutral'

        return {
            'status': status,
            'interpretation': interpretation,
            'status_class': status_class
        }

    # =========================================================================
    # SMALL CAP VS LARGE CAP INTERPRETATION
    # =========================================================================

    @staticmethod
    def interpret_small_large_cap_ratio(
        current_ratio: float,
        percentile: float,
        trend: str
    ) -> Dict:
        """
        Interpret small cap vs large cap ratio

        Low ratio = small caps underperforming (risk-off, quality preference)
        High ratio = small caps outperforming (risk-on, economic optimism)

        Args:
            current_ratio: Current Russell 2000 / S&P 500 ratio
            percentile: Historical percentile (0-100)
            trend: 3-month trend description

        Returns:
            Dict with status, interpretation, and status_class
        """
        if current_ratio is None or percentile is None:
            return {
                'status': 'NO DATA',
                'interpretation': 'Small/large cap ratio data not available.',
                'status_class': 'neutral'
            }

        trend_text = ""
        if trend == 'SMALL CAPS GAINING':
            trend_text = " Small caps gaining momentum recently."
        elif trend == 'LARGE CAPS GAINING':
            trend_text = " Large caps gaining momentum recently."

        if percentile < 15:
            status = 'SMALL CAPS VERY CHEAP'
            interpretation = (
                f'Russell 2000/S&P 500 ratio at {percentile:.0f}th percentile (10Y). '
                'Small caps trading at historically low valuations relative to large caps. '
                'Could signal opportunity if economic outlook improves, '
                'or continued risk-off/quality preference.' + trend_text
            )
            status_class = 'warning'

        elif percentile < 35:
            status = 'SMALL CAPS UNDERPERFORMING'
            interpretation = (
                f'Ratio at {percentile:.0f}th percentile - below average small cap valuations. '
                'Investors preferring large cap quality/safety. '
                'Value investors may find opportunities in smaller companies.' + trend_text
            )
            status_class = 'neutral'

        elif percentile > 85:
            status = 'SMALL CAPS EXPENSIVE'
            interpretation = (
                f'Ratio at {percentile:.0f}th percentile - small caps at premium vs large caps. '
                'Strong risk-on sentiment. '
                'Be cautious of overheating in speculative names.' + trend_text
            )
            status_class = 'warning'

        elif percentile > 65:
            status = 'FAVORING SMALL CAPS'
            interpretation = (
                f'Ratio at {percentile:.0f}th percentile - above average small cap performance. '
                'Risk appetite elevated. Economic optimism supporting smaller companies.' + trend_text
            )
            status_class = 'positive'

        else:
            status = 'BALANCED'
            interpretation = (
                f'Ratio at {percentile:.0f}th percentile - normal relationship. '
                'No extreme preference for small or large caps.' + trend_text
            )
            status_class = 'neutral'

        return {
            'status': status,
            'interpretation': interpretation,
            'status_class': status_class,
            'trend': trend
        }
