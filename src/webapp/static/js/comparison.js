/**
 * Stock Comparison Functionality
 * Handles multi-stock selection and comparison
 */

function initializeComparisonSearch(availableStocks) {
    const searchInput = document.getElementById('ticker-search');
    const hiddenInput = document.getElementById('tickers-hidden');
    const resultsDiv = document.getElementById('search-results');
    const chipsContainer = document.getElementById('selected-chips');
    const selectedContainer = document.getElementById('selected-stocks');
    const compareBtn = document.getElementById('compare-btn');
    const clearBtn = document.getElementById('clear-btn');

    if (!searchInput || !resultsDiv || !chipsContainer) {
        console.warn('Comparison search elements not found');
        return;
    }

    let selectedStocks = [];

    // Pre-populate from URL if present
    const urlParams = new URLSearchParams(window.location.search);
    const existingTickers = urlParams.get('tickers');
    if (existingTickers) {
        existingTickers.split(',').forEach(ticker => {
            const trimmed = ticker.trim().toUpperCase();
            if (trimmed && !selectedStocks.includes(trimmed)) {
                selectedStocks.push(trimmed);
            }
        });
        updateUI();
    }

    function updateUI() {
        // Update chips
        chipsContainer.innerHTML = '';
        selectedStocks.forEach(stock => {
            const chip = document.createElement('div');
            chip.className = 'stock-chip';
            chip.innerHTML = `
                <span class="chip-text">${stock}</span>
                <span class="chip-remove" data-stock="${stock}">&times;</span>
            `;
            chipsContainer.appendChild(chip);
        });

        // Update hidden input
        hiddenInput.value = selectedStocks.join(',');

        // Update buttons
        const hasSelection = selectedStocks.length > 0;
        compareBtn.disabled = selectedStocks.length < 2;
        clearBtn.style.display = hasSelection ? 'inline-block' : 'none';
        selectedContainer.style.display = hasSelection ? 'block' : 'none';

        // Add remove listeners
        document.querySelectorAll('.chip-remove').forEach(btn => {
            btn.addEventListener('click', function() {
                removeStock(this.dataset.stock);
            });
        });
    }

    function addStock(stock) {
        if (!selectedStocks.includes(stock)) {
            selectedStocks.push(stock);
            updateUI();
        }
        searchInput.value = '';
        resultsDiv.style.display = 'none';
    }

    function removeStock(stock) {
        selectedStocks = selectedStocks.filter(s => s !== stock);
        updateUI();
    }

    searchInput.addEventListener('input', function() {
        const query = this.value.toUpperCase().trim();
        resultsDiv.innerHTML = '';

        if (query.length === 0) {
            resultsDiv.style.display = 'none';
            return;
        }

        const matches = availableStocks
            .filter(stock => stock.toUpperCase().includes(query) && !selectedStocks.includes(stock))
            .slice(0, 10);

        if (matches.length > 0) {
            resultsDiv.style.display = 'block';
            matches.forEach(stock => {
                const div = document.createElement('div');
                div.className = 'search-result-item';
                div.textContent = stock;
                div.addEventListener('click', function() {
                    addStock(stock);
                });
                resultsDiv.appendChild(div);
            });
        } else {
            resultsDiv.style.display = 'none';
        }
    });

    searchInput.addEventListener('keydown', function(e) {
        const items = resultsDiv.querySelectorAll('.search-result-item');
        const active = resultsDiv.querySelector('.search-result-item.active');

        if (e.key === 'ArrowDown') {
            e.preventDefault();
            if (!active && items.length > 0) {
                items[0].classList.add('active');
            } else if (active && active.nextElementSibling) {
                active.classList.remove('active');
                active.nextElementSibling.classList.add('active');
            }
        } else if (e.key === 'ArrowUp') {
            e.preventDefault();
            if (active && active.previousElementSibling) {
                active.classList.remove('active');
                active.previousElementSibling.classList.add('active');
            }
        } else if (e.key === 'Enter') {
            e.preventDefault();
            if (active) {
                addStock(active.textContent);
            }
        } else if (e.key === 'Escape') {
            resultsDiv.style.display = 'none';
        }
    });

    clearBtn.addEventListener('click', function() {
        selectedStocks = [];
        updateUI();
        searchInput.focus();
    });

    document.addEventListener('click', function(e) {
        if (!searchInput.contains(e.target) && !resultsDiv.contains(e.target)) {
            resultsDiv.style.display = 'none';
        }
    });
}

/**
 * Compare stocks by tickers
 * @param {string} tickers - Comma-separated ticker symbols
 */
function compareTickers(tickers) {
    window.location.href = '/comparison?tickers=' + encodeURIComponent(tickers);
}
