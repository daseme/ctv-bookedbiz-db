#!/usr/bin/env python3
"""
Migrate agency_canonical_map to entity_aliases.

This script:
1. For each row in agency_canonical_map:
   - Ensures the canonical_name exists in agencies table
   - Creates entity_aliases record with entity_type='agency'
2. Logs conflicts to alias_conflicts table
3. Records audit trail in canon_audit

Usage:
    python scripts/migrate_agency_canonical_to_entity_aliases.py [--db-path PATH] [--dry-run]

Options:
    --db-path PATH  Path to SQLite database (default: ./data/database/production.db)
    --dry-run       Preview changes without committing
"""

import argparse
import sqlite3
from datetime import datetime
from contextlib import contextmanager


@contextmanager
def get_connection(db_path: str, readonly: bool = False):
    """Get database connection."""
    if readonly:
        uri = f"file:{db_path}?mode=ro"
        conn = sqlite3.connect(uri, uri=True, timeout=5.0)
    else:
        conn = sqlite3.connect(db_path, timeout=10.0)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
    finally:
        conn.close()


def migrate_agency_canonical_map(db_path: str, dry_run: bool = False):
    """
    Migrate agency_canonical_map entries to entity_aliases.

    Returns dict with migration stats.
    """
    stats = {
        "total_mappings": 0,
        "agencies_created": 0,
        "aliases_created": 0,
        "aliases_skipped_existing": 0,
        "conflicts_logged": 0,
        "errors": [],
    }

    with get_connection(db_path) as conn:
        # Get all mappings from agency_canonical_map
        mappings = conn.execute("""
            SELECT alias_name, canonical_name, updated_date
            FROM agency_canonical_map
            ORDER BY canonical_name, alias_name
        """).fetchall()

        stats["total_mappings"] = len(mappings)
        print(f"Found {len(mappings)} mappings in agency_canonical_map")

        for mapping in mappings:
            alias_name = mapping["alias_name"]
            canonical_name = mapping["canonical_name"]

            try:
                # 1. Check if agency exists with canonical_name
                agency = conn.execute("""
                    SELECT agency_id FROM agencies
                    WHERE agency_name = ? AND is_active = 1
                """, [canonical_name]).fetchone()

                if agency:
                    agency_id = agency["agency_id"]
                else:
                    # Create agency
                    if not dry_run:
                        conn.execute("""
                            INSERT INTO agencies (agency_name, is_active, notes)
                            VALUES (?, 1, 'Created by migration from agency_canonical_map')
                        """, [canonical_name])
                        agency_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
                    else:
                        agency_id = -1  # Placeholder for dry run
                    stats["agencies_created"] += 1
                    print(f"  + Created agency: {canonical_name}")

                # 2. Check if alias already exists in entity_aliases
                existing_alias = conn.execute("""
                    SELECT alias_id, target_entity_id
                    FROM entity_aliases
                    WHERE alias_name = ? AND entity_type = 'agency'
                """, [alias_name]).fetchone()

                if existing_alias:
                    # Check if it points to same agency
                    if existing_alias["target_entity_id"] != agency_id and not dry_run:
                        # Conflict - log it
                        conn.execute("""
                            INSERT INTO alias_conflicts
                            (entity_type, alias_name, normalized_name,
                             existing_target_entity_id, proposed_target_entity_id, notes)
                            VALUES ('agency', ?, ?, ?, ?,
                                    'Conflict during migration from agency_canonical_map')
                        """, [alias_name, canonical_name,
                              existing_alias["target_entity_id"], agency_id])
                        stats["conflicts_logged"] += 1
                        print(f"  ! Conflict: {alias_name} -> existing points to "
                              f"{existing_alias['target_entity_id']}, migration wants {agency_id}")
                    else:
                        stats["aliases_skipped_existing"] += 1

                else:
                    # Create alias
                    if not dry_run:
                        conn.execute("""
                            INSERT INTO entity_aliases
                            (alias_name, entity_type, target_entity_id, confidence_score,
                             created_by, notes, is_active)
                            VALUES (?, 'agency', ?, 100, 'migration',
                                    'Migrated from agency_canonical_map', 1)
                        """, [alias_name, agency_id])
                    stats["aliases_created"] += 1
                    if alias_name != canonical_name:
                        print(f"  + Created alias: {alias_name} -> {canonical_name}")

            except Exception as e:
                error_msg = f"Error processing {alias_name} -> {canonical_name}: {e}"
                stats["errors"].append(error_msg)
                print(f"  X {error_msg}")

        # Log migration to audit
        if not dry_run:
            conn.execute("""
                INSERT INTO canon_audit (actor, action, key, value, extra)
                VALUES ('migration', 'BULK_MIGRATE', 'agency_canonical_map', 'entity_aliases', ?)
            """, [
                f"total={stats['total_mappings']}|"
                f"agencies_created={stats['agencies_created']}|"
                f"aliases_created={stats['aliases_created']}|"
                f"skipped={stats['aliases_skipped_existing']}|"
                f"conflicts={stats['conflicts_logged']}"
            ])
            conn.commit()
            print("\nMigration committed.")
        else:
            print("\n[DRY RUN] No changes committed.")

    return stats


def verify_migration(db_path: str):
    """Verify migration results."""
    with get_connection(db_path, readonly=True) as conn:
        # Count canonical map entries
        canonical_count = conn.execute("""
            SELECT COUNT(*) as cnt FROM agency_canonical_map
        """).fetchone()["cnt"]

        # Count entity_aliases for agencies
        alias_count = conn.execute("""
            SELECT COUNT(*) as cnt FROM entity_aliases
            WHERE entity_type = 'agency' AND is_active = 1
        """).fetchone()["cnt"]

        # Count agencies
        agency_count = conn.execute("""
            SELECT COUNT(*) as cnt FROM agencies WHERE is_active = 1
        """).fetchone()["cnt"]

        # Count conflicts
        conflict_count = conn.execute("""
            SELECT COUNT(*) as cnt FROM alias_conflicts
            WHERE entity_type = 'agency'
        """).fetchone()["cnt"]

        print("\n=== Verification ===")
        print(f"agency_canonical_map entries: {canonical_count}")
        print(f"entity_aliases (agency): {alias_count}")
        print(f"Active agencies: {agency_count}")
        print(f"Logged conflicts: {conflict_count}")


def main():
    parser = argparse.ArgumentParser(
        description="Migrate agency_canonical_map to entity_aliases"
    )
    parser.add_argument(
        "--db-path",
        default="./data/database/production.db",
        help="Path to SQLite database"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview changes without committing"
    )
    parser.add_argument(
        "--verify-only",
        action="store_true",
        help="Only verify current state, don't migrate"
    )

    args = parser.parse_args()

    print(f"Database: {args.db_path}")
    print(f"Dry run: {args.dry_run}")
    print()

    if args.verify_only:
        verify_migration(args.db_path)
        return

    stats = migrate_agency_canonical_map(args.db_path, dry_run=args.dry_run)

    print("\n=== Migration Summary ===")
    print(f"Total mappings processed: {stats['total_mappings']}")
    print(f"Agencies created: {stats['agencies_created']}")
    print(f"Aliases created: {stats['aliases_created']}")
    print(f"Aliases skipped (existing): {stats['aliases_skipped_existing']}")
    print(f"Conflicts logged: {stats['conflicts_logged']}")
    if stats["errors"]:
        print(f"Errors: {len(stats['errors'])}")
        for err in stats["errors"][:5]:
            print(f"  - {err}")

    if not args.dry_run:
        verify_migration(args.db_path)


if __name__ == "__main__":
    main()
