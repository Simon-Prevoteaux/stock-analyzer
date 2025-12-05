"""
Stock Price Forecasting Module
Provides multiple valuation and forecasting methods
"""

import numpy as np
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass


@dataclass
class ForecastResult:
    """Container for forecast results"""
    method: str
    current_price: float
    target_price: float
    upside_percent: float
    years: int
    annual_return: float
    details: Dict


class StockForecaster:
    """Stock price forecasting using multiple valuation methods"""

    def __init__(self, stock_data: Dict):
        """
        Initialize forecaster with stock data

        Args:
            stock_data: Dictionary containing stock metrics from database
        """
        self.data = stock_data
        self.ticker = stock_data.get('ticker', 'N/A')
        self.current_price = stock_data.get('current_price', 0) or 0
        self.eps = stock_data.get('eps', 0) or 0
        self.pe_ratio = stock_data.get('pe_ratio', 0) or 0
        self.forward_pe = stock_data.get('forward_pe', 0) or 0
        self.ps_ratio = stock_data.get('ps_ratio', 0) or 0
        self.revenue = stock_data.get('revenue', 0) or 0
        self.revenue_growth = stock_data.get('revenue_growth', 0) or 0
        self.earnings_growth = stock_data.get('earnings_growth', 0) or 0
        self.market_cap = stock_data.get('market_cap', 0) or 0
        self.profit_margin = stock_data.get('profit_margin', 0) or 0

        # Calculate shares outstanding
        if self.current_price > 0 and self.market_cap > 0:
            self.shares_outstanding = self.market_cap / self.current_price
        else:
            self.shares_outstanding = 0

    def earnings_growth_model(
        self,
        growth_rate: float = None,
        growth_decay: float = 0.1,
        terminal_pe: float = None,
        years: int = 5
    ) -> ForecastResult:
        """
        Earnings-based valuation using EPS growth projections

        Args:
            growth_rate: Annual EPS growth rate (default: use current growth)
            growth_decay: Annual reduction in growth rate (e.g., 0.1 = 10% decay)
            terminal_pe: P/E ratio at end of forecast period
            years: Number of years to forecast

        Returns:
            ForecastResult with projected price
        """
        if growth_rate is None:
            growth_rate = self.earnings_growth if self.earnings_growth else 0.15

        if terminal_pe is None:
            terminal_pe = min(self.pe_ratio if self.pe_ratio > 0 else 20, 25)

        if self.eps <= 0:
            return ForecastResult(
                method="Earnings Growth Model",
                current_price=self.current_price,
                target_price=0,
                upside_percent=0,
                years=years,
                annual_return=0,
                details={"error": "Company has no positive earnings"}
            )

        # Project EPS forward with decaying growth
        eps_projections = [self.eps]
        current_growth = growth_rate

        for year in range(1, years + 1):
            projected_eps = eps_projections[-1] * (1 + current_growth)
            eps_projections.append(projected_eps)
            # Decay the growth rate
            current_growth = max(current_growth * (1 - growth_decay), 0.03)

        final_eps = eps_projections[-1]
        target_price = final_eps * terminal_pe

        upside = ((target_price / self.current_price) - 1) * 100 if self.current_price > 0 else 0
        annual_return = ((target_price / self.current_price) ** (1 / years) - 1) * 100 if self.current_price > 0 else 0

        return ForecastResult(
            method="Earnings Growth Model",
            current_price=self.current_price,
            target_price=round(target_price, 2),
            upside_percent=round(upside, 2),
            years=years,
            annual_return=round(annual_return, 2),
            details={
                "starting_eps": round(self.eps, 2),
                "ending_eps": round(final_eps, 2),
                "eps_projections": [round(e, 2) for e in eps_projections],
                "initial_growth_rate": round(growth_rate * 100, 1),
                "growth_decay": round(growth_decay * 100, 1),
                "terminal_pe": round(terminal_pe, 1)
            }
        )

    def revenue_growth_model(
        self,
        growth_rate: float = None,
        growth_decay: float = 0.15,
        terminal_ps: float = None,
        target_margin: float = None,
        years: int = 5
    ) -> ForecastResult:
        """
        Revenue-based valuation for growth companies

        Args:
            growth_rate: Annual revenue growth rate
            growth_decay: Annual reduction in growth rate
            terminal_ps: P/S ratio at end of forecast period
            target_margin: Expected profit margin at maturity
            years: Number of years to forecast

        Returns:
            ForecastResult with projected price
        """
        if growth_rate is None:
            growth_rate = self.revenue_growth if self.revenue_growth else 0.20

        if terminal_ps is None:
            terminal_ps = min(self.ps_ratio if self.ps_ratio > 0 else 5, 10)

        if target_margin is None:
            target_margin = max(self.profit_margin if self.profit_margin else 0.15, 0.15)

        if self.revenue <= 0:
            return ForecastResult(
                method="Revenue Growth Model",
                current_price=self.current_price,
                target_price=0,
                upside_percent=0,
                years=years,
                annual_return=0,
                details={"error": "Company has no revenue data"}
            )

        # Project revenue forward with decaying growth
        revenue_projections = [self.revenue]
        current_growth = growth_rate

        for year in range(1, years + 1):
            projected_revenue = revenue_projections[-1] * (1 + current_growth)
            revenue_projections.append(projected_revenue)
            current_growth = max(current_growth * (1 - growth_decay), 0.03)

        final_revenue = revenue_projections[-1]

        # Calculate target market cap and price
        target_market_cap = final_revenue * terminal_ps
        target_price = target_market_cap / self.shares_outstanding if self.shares_outstanding > 0 else 0

        upside = ((target_price / self.current_price) - 1) * 100 if self.current_price > 0 else 0
        annual_return = ((target_price / self.current_price) ** (1 / years) - 1) * 100 if self.current_price > 0 else 0

        # Also calculate implied earnings at target margin
        implied_earnings = final_revenue * target_margin
        implied_eps = implied_earnings / self.shares_outstanding if self.shares_outstanding > 0 else 0

        return ForecastResult(
            method="Revenue Growth Model",
            current_price=self.current_price,
            target_price=round(target_price, 2),
            upside_percent=round(upside, 2),
            years=years,
            annual_return=round(annual_return, 2),
            details={
                "starting_revenue": self.revenue,
                "ending_revenue": round(final_revenue, 0),
                "revenue_cagr": round((((final_revenue / self.revenue) ** (1/years)) - 1) * 100, 1),
                "initial_growth_rate": round(growth_rate * 100, 1),
                "growth_decay": round(growth_decay * 100, 1),
                "terminal_ps": round(terminal_ps, 2),
                "target_margin": round(target_margin * 100, 1),
                "implied_eps": round(implied_eps, 2)
            }
        )

    def dcf_model(
        self,
        fcf_growth: float = 0.10,
        discount_rate: float = 0.10,
        terminal_growth: float = 0.03,
        years: int = 10
    ) -> ForecastResult:
        """
        Simplified Discounted Cash Flow model

        Args:
            fcf_growth: Annual free cash flow growth rate
            discount_rate: Required rate of return (WACC)
            terminal_growth: Perpetual growth rate after forecast period
            years: Number of years to project

        Returns:
            ForecastResult with intrinsic value
        """
        # Estimate FCF from earnings or revenue
        if self.eps > 0 and self.shares_outstanding > 0:
            # Rough FCF estimate: ~80% of net income for mature companies
            net_income = self.eps * self.shares_outstanding
            base_fcf = net_income * 0.8
        elif self.revenue > 0:
            # For unprofitable companies, estimate FCF as % of revenue
            base_fcf = self.revenue * 0.05
        else:
            return ForecastResult(
                method="DCF Model",
                current_price=self.current_price,
                target_price=0,
                upside_percent=0,
                years=years,
                annual_return=0,
                details={"error": "Insufficient data for DCF analysis"}
            )

        # Project FCF
        fcf_projections = []
        for year in range(1, years + 1):
            fcf = base_fcf * ((1 + fcf_growth) ** year)
            fcf_projections.append(fcf)

        # Calculate present value of projected FCFs
        pv_fcf = sum(
            fcf / ((1 + discount_rate) ** (i + 1))
            for i, fcf in enumerate(fcf_projections)
        )

        # Calculate terminal value (Gordon Growth Model)
        terminal_fcf = fcf_projections[-1] * (1 + terminal_growth)
        terminal_value = terminal_fcf / (discount_rate - terminal_growth)
        pv_terminal = terminal_value / ((1 + discount_rate) ** years)

        # Total intrinsic value
        intrinsic_value = pv_fcf + pv_terminal
        target_price = intrinsic_value / self.shares_outstanding if self.shares_outstanding > 0 else 0

        upside = ((target_price / self.current_price) - 1) * 100 if self.current_price > 0 else 0
        annual_return = ((target_price / self.current_price) ** (1 / years) - 1) * 100 if self.current_price > 0 else 0

        return ForecastResult(
            method="DCF Model",
            current_price=self.current_price,
            target_price=round(target_price, 2),
            upside_percent=round(upside, 2),
            years=years,
            annual_return=round(annual_return, 2),
            details={
                "base_fcf": round(base_fcf / 1e9, 2),
                "fcf_growth": round(fcf_growth * 100, 1),
                "discount_rate": round(discount_rate * 100, 1),
                "terminal_growth": round(terminal_growth * 100, 1),
                "pv_fcf": round(pv_fcf / 1e9, 2),
                "terminal_value": round(terminal_value / 1e9, 2),
                "pv_terminal": round(pv_terminal / 1e9, 2),
                "intrinsic_value": round(intrinsic_value / 1e9, 2)
            }
        )

    def monte_carlo_simulation(
        self,
        expected_return: float = None,
        volatility: float = 0.30,
        years: int = 5,
        simulations: int = 1000
    ) -> Dict:
        """
        Monte Carlo simulation for probabilistic price forecasting

        Args:
            expected_return: Expected annual return (default: based on growth)
            volatility: Annual volatility (standard deviation)
            years: Number of years to simulate
            simulations: Number of simulation runs

        Returns:
            Dictionary with simulation results and percentiles
        """
        if expected_return is None:
            # Estimate expected return from growth rates
            if self.earnings_growth and self.earnings_growth > 0:
                expected_return = min(self.earnings_growth, 0.25)
            elif self.revenue_growth and self.revenue_growth > 0:
                expected_return = min(self.revenue_growth * 0.7, 0.20)
            else:
                expected_return = 0.08

        if self.current_price <= 0:
            return {
                "method": "Monte Carlo Simulation",
                "error": "No current price available"
            }

        # Daily parameters
        trading_days = 252
        total_days = years * trading_days
        daily_return = expected_return / trading_days
        daily_vol = volatility / np.sqrt(trading_days)

        # Run simulations
        np.random.seed(42)  # For reproducibility
        final_prices = []

        for _ in range(simulations):
            prices = [self.current_price]
            for _ in range(total_days):
                random_return = np.random.normal(daily_return, daily_vol)
                new_price = prices[-1] * (1 + random_return)
                prices.append(max(new_price, 0.01))  # Price can't go below 0.01
            final_prices.append(prices[-1])

        final_prices = np.array(final_prices)

        # Calculate percentiles
        percentiles = {
            "p10": round(np.percentile(final_prices, 10), 2),
            "p25": round(np.percentile(final_prices, 25), 2),
            "p50": round(np.percentile(final_prices, 50), 2),
            "p75": round(np.percentile(final_prices, 75), 2),
            "p90": round(np.percentile(final_prices, 90), 2)
        }

        # Calculate probability of different outcomes
        prob_double = (final_prices >= self.current_price * 2).mean() * 100
        prob_50_up = (final_prices >= self.current_price * 1.5).mean() * 100
        prob_profit = (final_prices >= self.current_price).mean() * 100
        prob_loss_50 = (final_prices <= self.current_price * 0.5).mean() * 100

        return {
            "method": "Monte Carlo Simulation",
            "current_price": self.current_price,
            "years": years,
            "simulations": simulations,
            "expected_return": round(expected_return * 100, 1),
            "volatility": round(volatility * 100, 1),
            "percentiles": percentiles,
            "median_target": percentiles["p50"],
            "median_upside": round(((percentiles["p50"] / self.current_price) - 1) * 100, 2),
            "probabilities": {
                "profit": round(prob_profit, 1),
                "up_50_percent": round(prob_50_up, 1),
                "double": round(prob_double, 1),
                "down_50_percent": round(prob_loss_50, 1)
            },
            "histogram_data": {
                "bins": [round(x, 2) for x in np.histogram(final_prices, bins=20)[1].tolist()],
                "counts": [int(x) for x in np.histogram(final_prices, bins=20)[0].tolist()]
            }
        }

    def scenario_analysis(
        self,
        years: int = 5
    ) -> Dict:
        """
        Generate bull, base, and bear case scenarios

        Args:
            years: Forecast horizon

        Returns:
            Dictionary with three scenarios
        """
        scenarios = {}

        # Base case - current growth continues with some decay
        base_growth = self.earnings_growth if self.earnings_growth else 0.12
        base_growth = max(min(base_growth, 0.30), 0.05)  # Cap between 5-30%

        # Bull case - optimistic scenario
        bull_growth = base_growth * 1.5
        bull_pe = (self.pe_ratio if self.pe_ratio > 0 else 20) * 1.2

        # Bear case - pessimistic scenario
        bear_growth = base_growth * 0.5
        bear_pe = (self.pe_ratio if self.pe_ratio > 0 else 20) * 0.7

        if self.eps > 0:
            # Earnings-based scenarios
            for name, growth, pe in [
                ("bear", bear_growth, bear_pe),
                ("base", base_growth, self.pe_ratio if self.pe_ratio > 0 else 18),
                ("bull", bull_growth, bull_pe)
            ]:
                final_eps = self.eps * ((1 + growth) ** years)
                target = final_eps * pe
                upside = ((target / self.current_price) - 1) * 100 if self.current_price > 0 else 0

                scenarios[name] = {
                    "target_price": round(target, 2),
                    "upside_percent": round(upside, 2),
                    "growth_rate": round(growth * 100, 1),
                    "terminal_pe": round(pe, 1),
                    "final_eps": round(final_eps, 2)
                }
        else:
            # Revenue-based scenarios for unprofitable companies
            base_rev_growth = self.revenue_growth if self.revenue_growth else 0.20

            for name, growth, ps in [
                ("bear", base_rev_growth * 0.5, 2),
                ("base", base_rev_growth, 5),
                ("bull", base_rev_growth * 1.3, 8)
            ]:
                final_revenue = self.revenue * ((1 + growth) ** years)
                target_mcap = final_revenue * ps
                target = target_mcap / self.shares_outstanding if self.shares_outstanding > 0 else 0
                upside = ((target / self.current_price) - 1) * 100 if self.current_price > 0 else 0

                scenarios[name] = {
                    "target_price": round(target, 2),
                    "upside_percent": round(upside, 2),
                    "growth_rate": round(growth * 100, 1),
                    "terminal_ps": round(ps, 1)
                }

        return {
            "method": "Scenario Analysis",
            "current_price": self.current_price,
            "years": years,
            "scenarios": scenarios
        }

    def run_all_models(self, years: int = 5) -> Dict:
        """
        Run all forecasting models with default parameters

        Returns:
            Dictionary containing results from all models
        """
        return {
            "ticker": self.ticker,
            "current_price": self.current_price,
            "company_name": self.data.get('company_name', 'N/A'),
            "earnings_model": self.earnings_growth_model(years=years),
            "revenue_model": self.revenue_growth_model(years=years),
            "dcf_model": self.dcf_model(years=years),
            "monte_carlo": self.monte_carlo_simulation(years=years),
            "scenarios": self.scenario_analysis(years=years)
        }
