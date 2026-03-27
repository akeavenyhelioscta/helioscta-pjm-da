"""Structured view models — domain interpretation layer.

Each module takes raw pipeline output and returns a structured dict
with explicit domain knowledge: thresholds, percentiles, outlier flags,
annotations. Consumed by HTML reporting, API endpoints, and agents.
"""
