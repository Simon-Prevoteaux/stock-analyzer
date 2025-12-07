/**
 * Stock Search Functionality
 * Provides quick search and navigation for stock tickers
 */

function initializeStockSearch(availableStocks, options = {}) {
    const {
        searchInputId = 'ticker-search',
        searchResultsId = 'search-results',
        redirectToDetail = true
    } = options;

    const searchInput = document.getElementById(searchInputId);
    const resultsDiv = document.getElementById(searchResultsId);

    if (!searchInput || !resultsDiv) {
        console.warn('Search elements not found');
        return;
    }

    searchInput.addEventListener('input', function() {
        const query = this.value.toUpperCase().trim();
        resultsDiv.innerHTML = '';

        if (query.length === 0) {
            resultsDiv.style.display = 'none';
            return;
        }

        const matches = availableStocks.filter(stock =>
            stock.toUpperCase().includes(query)
        ).slice(0, 10);

        if (matches.length > 0) {
            resultsDiv.style.display = 'block';
            matches.forEach(stock => {
                const div = document.createElement('div');
                div.className = 'search-result-item';
                div.textContent = stock;
                div.addEventListener('click', function() {
                    if (redirectToDetail) {
                        window.location.href = '/stock/' + stock;
                    }
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
                if (redirectToDetail) {
                    window.location.href = '/stock/' + active.textContent;
                }
            } else if (availableStocks.includes(searchInput.value.toUpperCase())) {
                if (redirectToDetail) {
                    window.location.href = '/stock/' + searchInput.value.toUpperCase();
                }
            }
        } else if (e.key === 'Escape') {
            resultsDiv.style.display = 'none';
        }
    });

    document.addEventListener('click', function(e) {
        if (!searchInput.contains(e.target) && !resultsDiv.contains(e.target)) {
            resultsDiv.style.display = 'none';
        }
    });
}
