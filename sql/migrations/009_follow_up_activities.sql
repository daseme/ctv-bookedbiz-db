-- Migration 009: Add follow_up activity type with due_date and completion tracking
-- SQLite cannot ALTER CHECK constraints, so we recreate the table

BEGIN TRANSACTION;

-- 1. Create new table with updated schema
CREATE TABLE entity_activity_new (
    activity_id INTEGER PRIMARY KEY AUTOINCREMENT,
    entity_type TEXT NOT NULL CHECK (entity_type IN ('customer', 'agency')),
    entity_id INTEGER NOT NULL,
    activity_type TEXT NOT NULL CHECK (activity_type IN ('note', 'call', 'email', 'meeting', 'status_change', 'follow_up')),
    activity_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    description TEXT,
    created_by TEXT NOT NULL,
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    contact_id INTEGER,
    due_date TEXT,
    is_completed INTEGER DEFAULT 0,
    completed_date TIMESTAMP,
    FOREIGN KEY (contact_id) REFERENCES entity_contacts(contact_id) ON DELETE SET NULL
);

-- 2. Copy existing data
INSERT INTO entity_activity_new
    (activity_id, entity_type, entity_id, activity_type, activity_date,
     description, created_by, created_date, contact_id)
SELECT activity_id, entity_type, entity_id, activity_type, activity_date,
       description, created_by, created_date, contact_id
FROM entity_activity;

-- 3. Drop old table and rename
DROP TABLE entity_activity;
ALTER TABLE entity_activity_new RENAME TO entity_activity;

-- 4. Recreate indexes
CREATE INDEX idx_entity_activity_lookup
ON entity_activity(entity_type, entity_id, activity_date DESC);

CREATE INDEX idx_entity_activity_followups
ON entity_activity(activity_type, is_completed, due_date)
WHERE activity_type = 'follow_up';

COMMIT;
