-- Create spot_language_assignments table
CREATE TABLE spot_language_assignments (
    assignment_id INTEGER PRIMARY KEY AUTOINCREMENT,
    spot_id INTEGER NOT NULL UNIQUE,
    language_code TEXT NOT NULL,
    language_status TEXT NOT NULL CHECK (language_status IN ('determined', 'undetermined', 'default', 'invalid')),
    confidence REAL DEFAULT 1.0,
    assignment_method TEXT DEFAULT 'direct_mapping',
    assigned_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    requires_review BOOLEAN DEFAULT 0,
    notes TEXT,
    FOREIGN KEY (spot_id) REFERENCES spots(spot_id) ON DELETE CASCADE
);

-- Create indexes
CREATE INDEX idx_language_assignments_review 
ON spot_language_assignments(requires_review, language_status) 
WHERE requires_review = 1;

CREATE INDEX idx_language_assignments_undetermined 
ON spot_language_assignments(language_status) 
WHERE language_status = 'undetermined';

CREATE INDEX idx_language_assignments_spot 
ON spot_language_assignments(spot_id);
