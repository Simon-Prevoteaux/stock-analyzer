/**
 * Modal Functionality
 * Info modals for displaying metric explanations
 */

const infoContent = {
    'bubble-score': {
        title: 'Bubble Score',
        content: `<p><strong>What it is:</strong> A custom 0-10 scale that measures how overvalued a stock might be. Higher scores indicate higher risk of overvaluation.</p>
        <p><strong>How it's calculated:</strong> The score is computed based on several factors:</p>
        <ul>
            <li><strong>P/E Ratio:</strong>
                <ul>
                    <li>P/E > 200: +3 points</li>
                    <li>P/E > 100: +2 points</li>
                    <li>P/E > 50: +1 point</li>
                </ul>
            </li>
            <li><strong>P/S Ratio:</strong>
                <ul>
                    <li>P/S > 50: +3 points</li>
                    <li>P/S > 20: +2 points</li>
                    <li>P/S > 10: +1 point</li>
                </ul>
            </li>
            <li><strong>Profitability:</strong>
                <ul>
                    <li>Not profitable: +2 points</li>
                </ul>
            </li>
            <li><strong>Growth vs Valuation:</strong>
                <ul>
                    <li>High P/S (>15) with low revenue growth (<20%): +2 points</li>
                </ul>
            </li>
        </ul>`
    },
    'risk-level': {
        title: 'Risk Level',
        content: `<p><strong>What it is:</strong> A classification of the stock's overvaluation risk based on its Bubble Score.</p>
        <ul>
            <li><strong>LOW (0-1):</strong> Generally reasonable valuations</li>
            <li><strong>MEDIUM (2-3):</strong> Some valuation concerns but not extreme</li>
            <li><strong>HIGH (4-5):</strong> Elevated valuations that may not be sustainable</li>
            <li><strong>VERY HIGH (6-7):</strong> Significant overvaluation concerns</li>
            <li><strong>EXTREME (8-10):</strong> Potentially in bubble territory</li>
        </ul>`
    },
    'revenue-cagr': {
        title: 'Revenue CAGR (3Y)',
        content: `<p><strong>What it is:</strong> Compound Annual Growth Rate - the year-over-year growth rate of revenue over a 3-year period.</p>
        <p><strong>How it's calculated:</strong></p>
        <ul>
            <li>Uses annual financial data from the past 3 years</li>
            <li>Formula: <code>(End Value / Start Value)^(1 / Years) - 1</code></li>
        </ul>
        <p><strong>Interpretation:</strong></p>
        <ul>
            <li><strong>Negative:</strong> Revenue is declining - major red flag</li>
            <li><strong>0-10%:</strong> Slow growth - mature company or struggling business</li>
            <li><strong>10-20%:</strong> Moderate growth - healthy for established companies</li>
            <li><strong>20-40%:</strong> High growth - strong performing growth company</li>
            <li><strong>40%+:</strong> Very high growth - exceptional but may not be sustainable</li>
        </ul>`
    },
    'earnings-cagr': {
        title: 'Earnings CAGR (3Y)',
        content: `<p><strong>What it is:</strong> Compound Annual Growth Rate of earnings (net income) over a 3-year period.</p>
        <p><strong>How it's calculated:</strong></p>
        <ul>
            <li>Uses annual earnings data from the past 3 years</li>
            <li>Formula: <code>(End Earnings / Start Earnings)^(1 / Years) - 1</code></li>
            <li>Only calculated if both start and end values are positive</li>
        </ul>
        <p><strong>Key insights:</strong></p>
        <ul>
            <li>Earnings CAGR higher than revenue CAGR = improving margins (good sign)</li>
            <li>Earnings CAGR lower than revenue CAGR = deteriorating margins (warning sign)</li>
        </ul>`
    },
    'revenue-consistency': {
        title: 'Revenue Consistency Score',
        content: `<p><strong>What it is:</strong> A 0-100 score measuring how consistent and predictable revenue growth has been over recent quarters.</p>
        <p><strong>How it's calculated:</strong> Based on quarterly data (up to 12 quarters), the score combines three factors:</p>
        <ul>
            <li><strong>Positive Growth Frequency (40 points):</strong> Percentage of quarters with revenue growth</li>
            <li><strong>Low Volatility (30 points):</strong> Rewards steady, predictable growth rates</li>
            <li><strong>Positive Average Growth (30 points):</strong> Bonus if average growth is positive</li>
        </ul>
        <p><strong>Score interpretation:</strong></p>
        <ul>
            <li><strong>70-100 (High):</strong> Very consistent revenue growth - predictable business model</li>
            <li><strong>50-69 (Medium):</strong> Moderately consistent - some volatility but overall growing</li>
            <li><strong>0-49 (Low):</strong> Inconsistent revenue - unpredictable or declining</li>
        </ul>`
    },
    'earnings-consistency': {
        title: 'Earnings Consistency Score',
        content: `<p><strong>What it is:</strong> A 0-100 score measuring how consistent and reliable quarterly earnings have been.</p>
        <p><strong>How it's calculated:</strong> Same methodology as Revenue Consistency but applied to quarterly earnings:</p>
        <ul>
            <li><strong>Positive Growth Frequency (40 points):</strong> Percentage of quarters with earnings growth</li>
            <li><strong>Low Volatility (30 points):</strong> Reward for steady, predictable earnings growth rates</li>
            <li><strong>Positive Average Growth (30 points):</strong> Bonus if average quarterly growth is positive</li>
        </ul>
        <p><strong>Why it matters:</strong></p>
        <ul>
            <li>More important than revenue consistency for mature companies</li>
            <li>High scores suggest good management execution and pricing power</li>
            <li>For growth companies, low scores are common and not necessarily bad - focus on revenue consistency instead</li>
        </ul>`
    },
    'profitable-quarters': {
        title: 'Consecutive Profitable Quarters',
        content: `<p><strong>What it is:</strong> The number of consecutive quarters with positive net income, counting backwards from the most recent quarter.</p>
        <p><strong>Interpretation:</strong></p>
        <ul>
            <li><strong>0 quarters:</strong> Currently unprofitable - may be early-stage or struggling</li>
            <li><strong>1-4 quarters:</strong> Newly profitable - watch to see if it's sustainable</li>
            <li><strong>4-8 quarters (1-2 years):</strong> Establishing profitability - positive trend</li>
            <li><strong>8-12 quarters (2-3 years):</strong> Proven profitability - reliable earnings</li>
            <li><strong>12+ quarters (3+ years):</strong> Consistently profitable - mature business model</li>
        </ul>`
    }
};

/**
 * Show info modal with specific content
 * @param {string} key - The content key to display
 */
function showInfo(key) {
    const info = infoContent[key];
    if (!info) return;

    document.getElementById('info-modal-title').textContent = info.title;
    document.getElementById('info-modal-content').innerHTML = info.content;
    document.getElementById('info-modal-overlay').classList.add('active');
    document.body.style.overflow = 'hidden';
}

/**
 * Close the info modal
 */
function closeInfo() {
    document.getElementById('info-modal-overlay').classList.remove('active');
    document.body.style.overflow = '';
}

// Initialize modal close on Escape key
document.addEventListener('keydown', function(e) {
    if (e.key === 'Escape') closeInfo();
});
