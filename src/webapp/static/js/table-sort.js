/**
 * Table Sorting Functionality
 * Adds sorting capability to tables with class 'sortable-table'
 */

document.addEventListener('DOMContentLoaded', function() {
    const tables = document.querySelectorAll('.sortable-table');

    tables.forEach(table => {
        const headers = table.querySelectorAll('th[data-sortable]');

        headers.forEach((header, index) => {
            header.style.cursor = 'pointer';
            header.style.userSelect = 'none';

            // Add sort indicator
            const indicator = document.createElement('span');
            indicator.className = 'sort-indicator';
            indicator.innerHTML = ' ⇅';
            header.appendChild(indicator);

            header.addEventListener('click', () => {
                sortTable(table, index, header);
            });
        });
    });
});

function sortTable(table, columnIndex, header) {
    const tbody = table.querySelector('tbody');
    const rows = Array.from(tbody.querySelectorAll('tr'));
    const isNumeric = header.getAttribute('data-type') === 'number';
    const currentDirection = header.getAttribute('data-direction') || 'asc';
    const newDirection = currentDirection === 'asc' ? 'desc' : 'asc';

    // Remove all sort indicators and directions
    table.querySelectorAll('th[data-sortable]').forEach(th => {
        th.removeAttribute('data-direction');
        const indicator = th.querySelector('.sort-indicator');
        if (indicator) {
            indicator.innerHTML = ' ⇅';
        }
    });

    // Set new direction
    header.setAttribute('data-direction', newDirection);
    const indicator = header.querySelector('.sort-indicator');
    if (indicator) {
        indicator.innerHTML = newDirection === 'asc' ? ' ↑' : ' ↓';
    }

    // Sort rows
    rows.sort((a, b) => {
        let aValue = a.cells[columnIndex].textContent.trim();
        let bValue = b.cells[columnIndex].textContent.trim();

        if (isNumeric) {
            // Extract numeric value (remove $, %, B, M, K, x, N/A)
            aValue = parseNumericValue(aValue);
            bValue = parseNumericValue(bValue);

            // Handle N/A or null values
            if (isNaN(aValue)) aValue = -Infinity;
            if (isNaN(bValue)) bValue = -Infinity;

            return newDirection === 'asc' ? aValue - bValue : bValue - aValue;
        } else {
            // String comparison
            if (aValue === 'N/A') aValue = '';
            if (bValue === 'N/A') bValue = '';

            return newDirection === 'asc'
                ? aValue.localeCompare(bValue)
                : bValue.localeCompare(aValue);
        }
    });

    // Reorder rows in DOM
    rows.forEach(row => tbody.appendChild(row));
}

function parseNumericValue(value) {
    // Remove common formatting
    value = value.replace(/[$,%]/g, '');

    // Handle shorthand notation (B, M, K)
    if (value.includes('B')) {
        return parseFloat(value) * 1e9;
    } else if (value.includes('M')) {
        return parseFloat(value) * 1e6;
    } else if (value.includes('K')) {
        return parseFloat(value) * 1e3;
    } else if (value.includes('x')) {
        return parseFloat(value);
    }

    return parseFloat(value);
}
