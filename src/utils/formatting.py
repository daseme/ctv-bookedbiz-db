"""Shared formatting helpers for revenue display and name parsing."""


def fmt_revenue(val):
    """Format revenue for signal labels: $1.2M / $145K / $800."""
    if val is None:
        return "$0"
    v = abs(val)
    if v >= 1_000_000:
        return f"${v/1_000_000:.1f}M"
    elif v >= 1_000:
        return f"${v/1_000:.0f}K"
    else:
        return f"${v:,.0f}"


def client_portion(name):
    """Strip agency prefix: 'Misfit:CA Colleges' -> 'CA Colleges'."""
    if not name:
        return name
    idx = name.find(":")
    return name[idx + 1:] if idx >= 0 else name
