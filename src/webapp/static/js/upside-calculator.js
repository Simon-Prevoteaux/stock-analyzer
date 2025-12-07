/**
 * Upside Calculator Functionality
 * Calculates potential upside and required growth rates for stocks
 */

// Global variables
let currentTicker = '';
let currentMC = 0;
let targetMC = 0;
let upsideChart = null;
let progressionChart = null;

/**
 * Set target value and submit form
 * @param {number} value - Target market cap in billions
 */
function setTarget(value) {
    document.getElementById('target').value = value;
    document.querySelector('.upside-form').submit();
}

/**
 * Select a stock and update the details panel
 */
function selectStock(ticker, currentMarketCap, upsideMultiple, currentPrice, targetPrice, companyName, targetMarketCap) {
    // Remove previous selection
    document.querySelectorAll('.stock-row').forEach(row => row.classList.remove('selected'));

    // Add selection to clicked row
    const row = document.querySelector(`tr[data-ticker="${ticker}"]`);
    if (row) row.classList.add('selected');

    // Update global variables
    currentTicker = ticker;
    currentMC = currentMarketCap;
    targetMC = targetMarketCap;

    // Update details panel
    document.getElementById('selectedTicker').textContent = ticker;
    document.getElementById('selectedCompany').textContent = companyName;
    document.getElementById('selectedCurrentMC').textContent = formatNumber(currentMarketCap);
    document.getElementById('selectedCurrentPrice').textContent = '$' + currentPrice.toFixed(2);
    document.getElementById('selectedTargetMC').textContent = formatNumber(targetMarketCap);
    document.getElementById('selectedTargetPrice').textContent = '$' + targetPrice.toFixed(2);
    document.getElementById('selectedUpside').textContent = '+' + ((upsideMultiple - 1) * 100).toFixed(0) + '%';
    document.getElementById('selectedMultiple').textContent = upsideMultiple.toFixed(2) + 'x';

    // Update chart
    updateUpsideChart(ticker, currentMarketCap, targetMarketCap);

    // Hide growth results (will recalculate when user clicks)
    document.getElementById('growthResults').style.display = 'none';
}

/**
 * Update upside visualization chart
 */
function updateUpsideChart(ticker, current, target) {
    const ctx = document.getElementById('upsideChart').getContext('2d');

    // Destroy existing chart
    if (upsideChart) {
        upsideChart.destroy();
    }

    // Create new chart
    upsideChart = new Chart(ctx, {
        type: 'bar',
        data: {
            labels: ['Current Market Cap', 'Target Market Cap'],
            datasets: [{
                label: ticker,
                data: [current, target],
                backgroundColor: ['#4a90e2', '#00ff88'],
                borderColor: ['#357abd', '#00cc6a'],
                borderWidth: 2
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: true,
            plugins: {
                legend: {
                    display: false
                },
                tooltip: {
                    callbacks: {
                        label: function(context) {
                            return formatNumber(context.parsed.y);
                        }
                    }
                }
            },
            scales: {
                y: {
                    beginAtZero: true,
                    ticks: {
                        callback: function(value) {
                            return formatNumberShort(value);
                        }
                    }
                }
            }
        }
    });
}

/**
 * Calculate required growth rate
 */
async function calculateRequiredGrowth() {
    const years = parseInt(document.getElementById('timeframe').value);

    if (!currentTicker) {
        alert('Please select a stock first');
        return;
    }

    if (!targetMC || targetMC <= 0) {
        alert('Invalid target market cap. Please set a target.');
        return;
    }

    try {
        const url = `/api/calculate-required-growth?ticker=${currentTicker}&target_mc=${targetMC}&years=${years}`;
        console.log('Fetching:', url);

        const response = await fetch(url);

        if (!response.ok) {
            const errorData = await response.json();
            alert(errorData.error || 'Failed to calculate required growth rate');
            return;
        }

        const data = await response.json();

        if (data.error) {
            alert(data.error);
            return;
        }

        // Display results
        document.getElementById('growthResults').style.display = 'block';
        document.getElementById('requiredCAGR').textContent = (data.required_cagr * 100).toFixed(1) + '%';

        // Historical comparison - handle null, undefined, and 0
        document.getElementById('historicalRevenue').textContent =
            (data.historical_revenue_cagr !== null && data.historical_revenue_cagr !== undefined && data.historical_revenue_cagr > 0)
                ? (data.historical_revenue_cagr * 100).toFixed(1) + '%'
                : 'N/A';
        document.getElementById('historicalEarnings').textContent =
            (data.historical_earnings_cagr !== null && data.historical_earnings_cagr !== undefined && data.historical_earnings_cagr > 0)
                ? (data.historical_earnings_cagr * 100).toFixed(1) + '%'
                : 'N/A';

        // Feasibility assessment
        const feasibilityEl = document.getElementById('feasibility');
        if (data.is_feasible === true) {
            feasibilityEl.textContent = 'Achievable';
            feasibilityEl.className = 'value feasible';
        } else if (data.is_feasible === false) {
            feasibilityEl.textContent = 'Ambitious';
            feasibilityEl.className = 'value ambitious';
        } else {
            feasibilityEl.textContent = 'Unknown';
            feasibilityEl.className = 'value';
        }

        // Update progression chart
        updateProgressionChart(data.progression, data.company_name);

    } catch (error) {
        console.error('Error calculating required growth:', error);
        alert('Failed to calculate required growth rate. Check console for details.');
    }
}

/**
 * Update year-by-year progression chart
 */
function updateProgressionChart(progression, companyName) {
    const ctx = document.getElementById('progressionChart').getContext('2d');

    // Destroy existing chart
    if (progressionChart) {
        progressionChart.destroy();
    }

    const labels = progression.map(p => `Year ${p.year}`);
    const mcData = progression.map(p => p.market_cap);
    const priceData = progression.map(p => p.price);

    progressionChart = new Chart(ctx, {
        type: 'line',
        data: {
            labels: labels,
            datasets: [
                {
                    label: 'Market Cap',
                    data: mcData,
                    borderColor: '#00ff88',
                    backgroundColor: 'rgba(0, 255, 136, 0.1)',
                    yAxisID: 'y',
                    tension: 0.3,
                    fill: true
                },
                {
                    label: 'Stock Price',
                    data: priceData,
                    borderColor: '#ffd700',
                    backgroundColor: 'rgba(255, 215, 0, 0.1)',
                    yAxisID: 'y1',
                    tension: 0.3,
                    fill: true
                }
            ]
        },
        options: {
            responsive: true,
            maintainAspectRatio: true,
            interaction: {
                mode: 'index',
                intersect: false,
            },
            plugins: {
                title: {
                    display: true,
                    text: `${companyName} - Projected Growth Path`
                }
            },
            scales: {
                y: {
                    type: 'linear',
                    display: true,
                    position: 'left',
                    title: {
                        display: true,
                        text: 'Market Cap'
                    },
                    ticks: {
                        callback: function(value) {
                            return formatNumberShort(value);
                        }
                    }
                },
                y1: {
                    type: 'linear',
                    display: true,
                    position: 'right',
                    title: {
                        display: true,
                        text: 'Stock Price ($)'
                    },
                    grid: {
                        drawOnChartArea: false,
                    },
                    ticks: {
                        callback: function(value) {
                            return '$' + value.toFixed(2);
                        }
                    }
                }
            }
        }
    });
}

/**
 * Format number helper
 */
function formatNumber(value) {
    if (!value) return 'N/A';

    if (value >= 1e12) {
        return '$' + (value / 1e12).toFixed(2) + 'T';
    } else if (value >= 1e9) {
        return '$' + (value / 1e9).toFixed(2) + 'B';
    } else if (value >= 1e6) {
        return '$' + (value / 1e6).toFixed(2) + 'M';
    } else if (value >= 1e3) {
        return '$' + (value / 1e3).toFixed(2) + 'K';
    }
    return '$' + value.toFixed(2);
}

/**
 * Format number short helper
 */
function formatNumberShort(value) {
    if (value >= 1e12) {
        return (value / 1e12).toFixed(1) + 'T';
    } else if (value >= 1e9) {
        return (value / 1e9).toFixed(1) + 'B';
    } else if (value >= 1e6) {
        return (value / 1e6).toFixed(1) + 'M';
    }
    return value.toFixed(0);
}

/**
 * Initialize upside calculator
 */
function initializeUpsideCalculator(firstStockData) {
    if (firstStockData) {
        currentTicker = firstStockData.ticker;
        currentMC = firstStockData.currentMarketCap;
        targetMC = firstStockData.targetMarketCap;
        updateUpsideChart(firstStockData.ticker, firstStockData.currentMarketCap, firstStockData.targetMarketCap);
    }
}
