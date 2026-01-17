"""
Template filters for Jinja2

This module contains custom template filters for formatting data in templates.
"""


def format_number(value):
    """Format large numbers with K, M, B, T suffixes"""
    if value is None or value == 0:
        return 'N/A'

    try:
        value = float(value)
        if value >= 1_000_000_000_000:
            return f'${value / 1_000_000_000_000:.2f}T'
        elif value >= 1_000_000_000:
            return f'${value / 1_000_000_000:.2f}B'
        elif value >= 1_000_000:
            return f'${value / 1_000_000:.2f}M'
        elif value >= 1_000:
            return f'${value / 1_000:.2f}K'
        else:
            return f'${value:.2f}'
    except (ValueError, TypeError):
        return 'N/A'


def format_percent(value):
    """Format value as percentage"""
    if value is None:
        return 'N/A'

    try:
        value = float(value)
        return f'{value * 100:.2f}%'
    except (ValueError, TypeError):
        return 'N/A'


def format_ratio(value):
    """Format value as ratio with x suffix"""
    if value is None or value == 0:
        return 'N/A'

    try:
        value = float(value)
        return f'{value:.2f}x'
    except (ValueError, TypeError):
        return 'N/A'


def register_filters(app):
    """Register all template filters with the Flask app"""
    app.template_filter('format_number')(format_number)
    app.template_filter('format_percent')(format_percent)
    app.template_filter('format_ratio')(format_ratio)
