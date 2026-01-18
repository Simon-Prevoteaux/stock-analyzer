/**
 * Macro Signals Dashboard JavaScript
 * Handles yield curve visualization and data refresh
 */

/**
 * Render the US Treasury yield curve chart
 */
function renderYieldCurve(yieldData) {
    const ctx = document.getElementById('yieldCurveChart');

    if (!ctx) {
        console.error('Yield curve canvas element not found');
        return;
    }

    // Check if Chart.js is loaded
    if (typeof Chart === 'undefined') {
        console.error('Chart.js library not loaded');
        ctx.parentElement.innerHTML = '<p style="color: #ff4444; text-align: center; padding: 2rem;">Chart.js library failed to load. Please refresh the page.</p>';
        return;
    }

    console.log('Yield curve data received:', yieldData);

    // Filter out null values and corresponding labels
    const validData = yieldData.maturities.map((maturity, index) => {
        return {
            maturity: maturity,
            yield: yieldData.yields[index]
        };
    }).filter(item => item.yield !== null && item.yield !== undefined);

    console.log('Valid data points:', validData);

    const labels = validData.map(item => item.maturity);
    const yields = validData.map(item => item.yield);

    // If no valid data, show error message
    if (yields.length === 0) {
        console.error('No valid yield data to display');
        ctx.parentElement.innerHTML = '<p style="color: #ff4444; text-align: center; padding: 2rem;">No yield curve data available. Please refresh the page.</p>';
        return;
    }

    // Destroy existing chart if it exists
    if (window.yieldCurveChart && typeof window.yieldCurveChart.destroy === 'function') {
        window.yieldCurveChart.destroy();
    }

    // Determine curve shape for color coding
    let curveColor = '#00ff88';  // normal - theme green
    let backgroundColor = 'rgba(0, 255, 136, 0.1)';

    // Check if curve is inverted (short-term > long-term)
    if (yields.length >= 3) {
        const shortTerm = yields[0];  // 1M
        const longTerm = yields[yields.length - 1];  // 30Y

        if (shortTerm > longTerm) {
            // Inverted curve - use red
            curveColor = '#ff4444';
            backgroundColor = 'rgba(255, 68, 68, 0.1)';
        }
    }

    window.yieldCurveChart = new Chart(ctx, {
        type: 'line',
        data: {
            labels: labels,
            datasets: [{
                label: 'Treasury Yield (%)',
                data: yields,
                borderColor: curveColor,
                backgroundColor: backgroundColor,
                fill: true,
                tension: 0.4,
                borderWidth: 3,
                pointRadius: 5,
                pointHoverRadius: 7,
                pointBackgroundColor: curveColor,
                pointBorderColor: '#1a1d2e',
                pointBorderWidth: 2
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            scales: {
                y: {
                    beginAtZero: false,
                    title: {
                        display: true,
                        text: 'Yield (%)',
                        font: {
                            size: 14,
                            weight: 'bold'
                        },
                        color: '#e0e6ed'
                    },
                    ticks: {
                        color: '#e0e6ed'
                    },
                    grid: {
                        color: 'rgba(0, 255, 136, 0.1)'
                    }
                },
                x: {
                    title: {
                        display: true,
                        text: 'Maturity',
                        font: {
                            size: 14,
                            weight: 'bold'
                        },
                        color: '#e0e6ed'
                    },
                    ticks: {
                        color: '#e0e6ed'
                    },
                    grid: {
                        display: false
                    }
                }
            },
            plugins: {
                title: {
                    display: true,
                    text: 'US Treasury Yield Curve',
                    font: {
                        size: 18,
                        weight: 'bold'
                    },
                    color: '#00ff88',
                    padding: {
                        top: 10,
                        bottom: 20
                    }
                },
                legend: {
                    display: false
                },
                tooltip: {
                    callbacks: {
                        label: function(context) {
                            return 'Yield: ' + context.parsed.y.toFixed(2) + '%';
                        }
                    },
                    backgroundColor: 'rgba(30, 33, 57, 0.95)',
                    borderColor: 'rgba(0, 255, 136, 0.3)',
                    borderWidth: 1,
                    titleColor: '#00ff88',
                    bodyColor: '#e0e6ed',
                    padding: 12,
                    titleFont: {
                        size: 14
                    },
                    bodyFont: {
                        size: 13
                    }
                }
            },
            interaction: {
                intersect: false,
                mode: 'index'
            }
        }
    });
}

/**
 * Refresh all macro data
 */
document.addEventListener('DOMContentLoaded', function() {
    const refreshButton = document.getElementById('refresh-data');
    const refreshStatus = document.getElementById('refresh-status');

    if (refreshButton) {
        refreshButton.addEventListener('click', async function() {
            refreshButton.disabled = true;
            refreshButton.textContent = 'Refreshing...';
            refreshStatus.textContent = 'Fetching latest data from FRED...';
            refreshStatus.className = 'refresh-status loading';

            try {
                // Note: Full refresh would reload the page
                // For now, just reload the page to get fresh data
                refreshStatus.textContent = 'Reloading page with fresh data...';

                setTimeout(() => {
                    window.location.reload();
                }, 1000);

            } catch (error) {
                console.error('Error refreshing data:', error);
                refreshStatus.textContent = 'Error refreshing data. Please try again.';
                refreshStatus.className = 'refresh-status error';
                refreshButton.disabled = false;
                refreshButton.textContent = 'Refresh All Data';
            }
        });
    }
});

/**
 * Switch between gold calculation methods
 */
function switchGoldCalcMethod(method) {
    const invertedTable = document.getElementById('gold-table-inverted');
    const geometricTable = document.getElementById('gold-table-geometric');
    const description = document.getElementById('gold-calc-description');
    const tabs = document.querySelectorAll('.calc-tab');

    // Update tab active states
    tabs.forEach(tab => {
        if (tab.dataset.method === method) {
            tab.classList.add('active');
        } else {
            tab.classList.remove('active');
        }
    });

    // Show/hide tables
    if (method === 'inverted') {
        invertedTable.style.display = 'table';
        geometricTable.style.display = 'none';
        description.textContent = '📊 Method 1: Showing inverted gold price returns | Positive % = Currency gained vs Gold | Negative % = Currency lost vs Gold';
    } else {
        invertedTable.style.display = 'none';
        geometricTable.style.display = 'table';
        description.textContent = '📊 Method 2: Showing geometric purchasing power loss | Shows actual % of wealth lost in gold terms';
    }
}

/**
 * Render historical spread trends chart
 */
function renderSpreadHistory(spreadData) {
    const ctx = document.getElementById('spreadHistoryChart');

    if (!ctx) {
        console.error('Spread history canvas element not found');
        return;
    }

    if (typeof Chart === 'undefined') {
        console.error('Chart.js library not loaded');
        return;
    }

    // Destroy existing chart if it exists
    if (window.spreadHistoryChart && typeof window.spreadHistoryChart.destroy === 'function') {
        window.spreadHistoryChart.destroy();
    }

    const datasets = [];

    // 10Y-2Y Spread
    if (spreadData.spreads['10y2y'] && spreadData.spreads['10y2y'].length > 0) {
        datasets.push({
            label: '10Y-2Y Spread',
            data: spreadData.spreads['10y2y'],
            borderColor: '#00ff88',
            backgroundColor: 'rgba(0, 255, 136, 0.1)',
            fill: false,
            tension: 0.3,
            borderWidth: 2,
            pointRadius: 0,
            pointHoverRadius: 5
        });
    }

    // 10Y-3M Spread
    if (spreadData.spreads['10y3m'] && spreadData.spreads['10y3m'].length > 0) {
        datasets.push({
            label: '10Y-3M Spread',
            data: spreadData.spreads['10y3m'],
            borderColor: '#64b5f6',
            backgroundColor: 'rgba(100, 181, 246, 0.1)',
            fill: false,
            tension: 0.3,
            borderWidth: 2,
            pointRadius: 0,
            pointHoverRadius: 5
        });
    }

    // 30Y-5Y Spread
    if (spreadData.spreads['30y5y'] && spreadData.spreads['30y5y'].length > 0) {
        datasets.push({
            label: '30Y-5Y Spread',
            data: spreadData.spreads['30y5y'],
            borderColor: '#ffc107',
            backgroundColor: 'rgba(255, 193, 7, 0.1)',
            fill: false,
            tension: 0.3,
            borderWidth: 2,
            pointRadius: 0,
            pointHoverRadius: 5
        });
    }

    window.spreadHistoryChart = new Chart(ctx, {
        type: 'line',
        data: {
            labels: spreadData.dates,
            datasets: datasets
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            scales: {
                y: {
                    beginAtZero: false,
                    title: {
                        display: true,
                        text: 'Spread (%)',
                        color: '#e0e6ed'
                    },
                    ticks: {
                        color: '#e0e6ed'
                    },
                    grid: {
                        color: 'rgba(168, 178, 209, 0.1)'
                    }
                },
                x: {
                    title: {
                        display: true,
                        text: 'Date',
                        color: '#e0e6ed'
                    },
                    ticks: {
                        color: '#e0e6ed',
                        maxTicksLimit: 12,
                        maxRotation: 45
                    },
                    grid: {
                        display: false
                    }
                }
            },
            plugins: {
                title: {
                    display: true,
                    text: 'Yield Spread History (1 Year)',
                    font: {
                        size: 16,
                        weight: 'bold'
                    },
                    color: '#00ff88',
                    padding: {
                        bottom: 20
                    }
                },
                legend: {
                    display: true,
                    labels: {
                        color: '#e0e6ed'
                    }
                },
                tooltip: {
                    backgroundColor: 'rgba(30, 33, 57, 0.95)',
                    borderColor: 'rgba(0, 255, 136, 0.3)',
                    borderWidth: 1,
                    titleColor: '#00ff88',
                    bodyColor: '#e0e6ed'
                },
                annotation: {
                    annotations: {
                        zeroLine: {
                            type: 'line',
                            yMin: 0,
                            yMax: 0,
                            borderColor: 'rgba(255, 68, 68, 0.7)',
                            borderWidth: 2,
                            borderDash: [5, 5],
                            label: {
                                content: 'Inversion Threshold',
                                enabled: true,
                                position: 'end',
                                color: '#ff4444',
                                backgroundColor: 'rgba(30, 33, 57, 0.8)'
                            }
                        }
                    }
                }
            }
        }
    });
}

/**
 * Smooth scroll to anchors
 */
document.addEventListener('DOMContentLoaded', function() {
    // Handle anchor links for smooth scrolling
    document.querySelectorAll('a[href^="#"]').forEach(anchor => {
        anchor.addEventListener('click', function(e) {
            const href = this.getAttribute('href');
            if (href === '#') return;

            e.preventDefault();
            const target = document.querySelector(href);
            if (target) {
                target.scrollIntoView({
                    behavior: 'smooth',
                    block: 'start'
                });
            }
        });
    });
});
