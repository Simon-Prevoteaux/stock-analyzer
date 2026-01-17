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
