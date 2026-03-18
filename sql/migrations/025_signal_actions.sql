-- 025_signal_actions.sql
-- Signal action queue: tracks lifecycle of entity signals as work items

CREATE TABLE IF NOT EXISTS signal_actions (
    action_id INTEGER PRIMARY KEY AUTOINCREMENT,
    entity_type TEXT NOT NULL CHECK (entity_type IN ('customer', 'agency')),
    entity_id INTEGER NOT NULL,
    signal_type TEXT NOT NULL,
    assigned_ae TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'new'
        CHECK (status IN ('new', 'acknowledged', 'snoozed', 'dismissed')),
    reason TEXT,
    snooze_until DATE,
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_by TEXT
);

CREATE INDEX IF NOT EXISTS idx_signal_actions_ae_status
    ON signal_actions(assigned_ae, status);
CREATE INDEX IF NOT EXISTS idx_signal_actions_entity
    ON signal_actions(entity_type, entity_id, signal_type);
