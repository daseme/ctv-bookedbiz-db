-- Migration 014: Entity Health Signals cache
-- Materialized at import time alongside entity_metrics.
-- Stores per-entity CRM signals (churned, declining, gone_quiet, new_account, growing).

CREATE TABLE IF NOT EXISTS entity_signals (
    entity_type TEXT NOT NULL,
    entity_id INTEGER NOT NULL,
    signal_type TEXT NOT NULL,
    signal_label TEXT NOT NULL,
    signal_priority INTEGER NOT NULL,
    trailing_revenue REAL,
    prior_revenue REAL,
    computed_at TEXT DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (entity_type, entity_id, signal_type)
);
