"""Define the version 1 schema of a source database and the groups that read it."""

from __future__ import annotations

from sg_event_daemon._sqlite import Schema

BUMP_INDEX_REVISION = (
    'UPDATE source_meta SET index_revision = index_revision + 1 WHERE singleton = 1'
)
BUMP_RANGE_REVISION = (
    'UPDATE source_meta SET range_revision = range_revision + 1 WHERE singleton = 1'
)
SOURCE_SCHEMA = Schema(
    version=1,
    statements=(
        """
        CREATE TABLE source_meta (
            singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
            site TEXT NOT NULL,
            source_name TEXT NOT NULL,
            stream_fingerprint TEXT NOT NULL,
            stream_incarnation TEXT,
            source_generation TEXT NOT NULL,
            initialized INTEGER NOT NULL,
            frontier INTEGER NOT NULL,
            successful_clock REAL NOT NULL,
            condition TEXT,
            index_revision INTEGER NOT NULL DEFAULT 0,
            range_revision INTEGER NOT NULL DEFAULT 0
        )
        """,
        """
        CREATE TABLE groups (
            group_name TEXT PRIMARY KEY,
            group_generation INTEGER NOT NULL,
            ordering_mode TEXT NOT NULL,
            site_server_url TEXT NOT NULL,
            verified_incarnation TEXT NOT NULL,
            holdback_secs REAL NOT NULL,
            active INTEGER NOT NULL CHECK (active IN (0, 1)),
            lifecycle TEXT NOT NULL
                CHECK (lifecycle IN ('active', 'orphaned', 'readded-parked'))
        )
        """,
        """
        CREATE TABLE group_state (
            group_name TEXT NOT NULL PRIMARY KEY REFERENCES groups(group_name),
            revision INTEGER NOT NULL,
            cursor INTEGER NOT NULL,
            delivery_floor INTEGER NOT NULL,
            in_flight_event_id INTEGER,
            in_flight_obligation_id TEXT,
            manifest_json TEXT,
            required_event_id INTEGER,
            retry_event_id INTEGER,
            failure_code TEXT,
            failure_text TEXT,
            failure_byte_count INTEGER,
            failure_digest TEXT,
            failure_ordinal INTEGER,
            parked INTEGER NOT NULL DEFAULT 0,
            is_operator_replay INTEGER NOT NULL DEFAULT 0,
            replay_required_from INTEGER,
            history_truncated_floor INTEGER,
            ordering_anomaly_id INTEGER,
            strict_capacity_abort_streak INTEGER NOT NULL DEFAULT 0,
            strict_capacity_attempt_token TEXT,
            strict_capacity_last_caused INTEGER,
            strict_capacity_exhausted INTEGER NOT NULL DEFAULT 0,
            shape_mismatch INTEGER NOT NULL DEFAULT 0,
            orphaned INTEGER NOT NULL DEFAULT 0,
            stream_identity_changed INTEGER NOT NULL DEFAULT 0,
            ordering_changed INTEGER NOT NULL DEFAULT 0,
            reader_clock_checkpoint REAL NOT NULL DEFAULT 0,
            last_epoch_capture REAL,
            latest_join_boundary INTEGER,
            operational_late_id_count INTEGER NOT NULL DEFAULT 0
                CHECK (operational_late_id_count >= 0),
            last_delivered_id INTEGER NOT NULL
        )
        """,
        """
        CREATE TABLE callback_marks (
            group_name TEXT NOT NULL,
            event_id INTEGER NOT NULL,
            ordinal INTEGER NOT NULL,
            plugin_id TEXT NOT NULL,
            callback_id TEXT NOT NULL,
            outcome TEXT NOT NULL,
            delivery_attempt INTEGER NOT NULL,
            PRIMARY KEY (group_name, event_id, ordinal)
        )
        """,
        """
        CREATE TABLE discovery_ranges (
            obligation_id TEXT PRIMARY KEY,
            low_id INTEGER NOT NULL,
            high_id INTEGER NOT NULL,
            opened_successful_at REAL NOT NULL,
            visibility_deadline REAL NOT NULL,
            source_revision INTEGER NOT NULL,
            close_generation INTEGER NOT NULL,
            range_state TEXT NOT NULL,
            scan_started_at REAL,
            scan_next_id INTEGER NOT NULL,
            scan_pages INTEGER NOT NULL,
            degraded INTEGER NOT NULL,
            scan_cycle_pages INTEGER NOT NULL DEFAULT 0
        )
        """,
        """
        CREATE TABLE range_groups (
            obligation_id TEXT NOT NULL REFERENCES discovery_ranges(obligation_id)
                ON DELETE CASCADE,
            group_name TEXT NOT NULL,
            group_generation INTEGER NOT NULL,
            delivery_floor INTEGER NOT NULL,
            eligibility_point REAL NOT NULL,
            obligation_state TEXT NOT NULL CHECK (
                obligation_state IN ('pending', 'satisfied-by-floor', 'scan-complete')
            ),
            PRIMARY KEY (obligation_id, group_name, group_generation)
        )
        """,
        """
        CREATE TABLE source_late_ids (
            obligation_id TEXT NOT NULL REFERENCES discovery_ranges(obligation_id)
                ON DELETE CASCADE,
            event_id INTEGER NOT NULL,
            PRIMARY KEY (obligation_id, event_id)
        )
        """,
        """
        CREATE TABLE late_ids (
            group_name TEXT NOT NULL,
            obligation_id TEXT NOT NULL REFERENCES discovery_ranges(obligation_id)
                ON DELETE CASCADE,
            event_id INTEGER NOT NULL,
            terminal_state TEXT NOT NULL,
            capacity_revision INTEGER,
            PRIMARY KEY (group_name, obligation_id, event_id)
        )
        """,
        """
        CREATE TABLE replay_markers (
            group_name TEXT NOT NULL,
            group_generation INTEGER NOT NULL,
            source_generation TEXT NOT NULL,
            marker_id TEXT NOT NULL,
            from_event_id INTEGER NOT NULL,
            command_digest TEXT NOT NULL,
            PRIMARY KEY (group_name, group_generation)
        )
        """,
        """
        CREATE TABLE epochs (
            epoch_id INTEGER PRIMARY KEY AUTOINCREMENT,
            group_name TEXT NOT NULL,
            low_id INTEGER NOT NULL,
            high_id INTEGER NOT NULL,
            captured_at REAL NOT NULL,
            eligibility_point REAL NOT NULL,
            reader_clock_checkpoint REAL NOT NULL,
            source_generation TEXT NOT NULL,
            group_generation INTEGER NOT NULL,
            state TEXT NOT NULL
        )
        """,
        """
        CREATE TABLE join_commits (
            group_name TEXT NOT NULL,
            attempt_token TEXT NOT NULL,
            identity_json TEXT NOT NULL,
            result_revision INTEGER NOT NULL,
            snapshot_json TEXT NOT NULL,
            PRIMARY KEY (group_name, attempt_token)
        )
        """,
        """
        CREATE TABLE audit (
            audit_id INTEGER PRIMARY KEY AUTOINCREMENT,
            site TEXT NOT NULL,
            group_name TEXT NOT NULL,
            event_id INTEGER,
            plugin_id TEXT,
            callback_id TEXT,
            command TEXT NOT NULL,
            wall_clock_time REAL NOT NULL,
            prior_revision INTEGER NOT NULL,
            new_revision INTEGER NOT NULL,
            reason TEXT NOT NULL,
            operator_label TEXT,
            local_uid INTEGER NOT NULL,
            username TEXT NOT NULL,
            pid INTEGER NOT NULL,
            disposition_id TEXT,
            command_fingerprint TEXT,
            terminal_result TEXT,
            result_snapshot_json TEXT
        )
        """,
        """
        CREATE TABLE conditional_park_results (
            group_name TEXT NOT NULL,
            evidence_id TEXT NOT NULL,
            group_generation INTEGER NOT NULL,
            evidence_fingerprint TEXT NOT NULL,
            outcome TEXT NOT NULL CHECK (outcome IN ('parked', 'stale')),
            resulting_revision INTEGER NOT NULL,
            decided_at_wall_clock REAL NOT NULL,
            PRIMARY KEY (group_name, evidence_id)
        )
        """,
        """
        CREATE TABLE disposition_audit (
            disposition_id TEXT PRIMARY KEY,
            group_name TEXT NOT NULL,
            command TEXT
                CHECK (command IS NULL OR command IN ('replay', 'reset-to-latest')),
            command_fingerprint TEXT NOT NULL,
            old_generation INTEGER,
            new_generation INTEGER,
            cursor INTEGER,
            frontier INTEGER,
            stream_incarnation TEXT,
            terminal_result TEXT NOT NULL,
            CHECK (
                (
                    command IS NULL
                    AND old_generation IS NULL
                    AND new_generation IS NULL
                    AND cursor IS NULL
                    AND frontier IS NULL
                    AND stream_incarnation IS NULL
                )
                OR (
                    command IS NOT NULL
                    AND old_generation IS NOT NULL
                    AND new_generation IS NOT NULL
                    AND cursor IS NOT NULL
                    AND frontier IS NOT NULL
                    AND stream_incarnation IS NOT NULL
                )
            )
        )
        """,
        """
        CREATE TABLE stream_adoptions (
            adoption_id TEXT PRIMARY KEY,
            command_fingerprint TEXT NOT NULL,
            stream_fingerprint TEXT NOT NULL,
            reason TEXT NOT NULL,
            retired_range_count INTEGER NOT NULL,
            retired_late_id_count INTEGER NOT NULL,
            retired_marker_count INTEGER NOT NULL
        )
        """,
        """
        CREATE TABLE pending_adoptions (
            adoption_id TEXT PRIMARY KEY,
            command_fingerprint TEXT NOT NULL,
            prior_incarnation TEXT NOT NULL,
            prior_stream_fingerprint TEXT NOT NULL,
            target_stream_fingerprint TEXT NOT NULL,
            reason TEXT NOT NULL,
            UNIQUE (
                command_fingerprint,
                prior_incarnation,
                prior_stream_fingerprint,
                target_stream_fingerprint
            )
        )
        """,
        """
        CREATE INDEX range_groups_by_group
        ON range_groups(group_name, group_generation, obligation_state)
        """,
        'CREATE INDEX late_ids_by_obligation ON late_ids(obligation_id)',
        """
        CREATE INDEX late_ids_capacity_pending
        ON late_ids(group_name, capacity_revision, obligation_id, event_id)
        WHERE terminal_state = 'capacity-pending'
        """,
        'CREATE INDEX epochs_by_group ON epochs(group_name, epoch_id)',
        'CREATE INDEX audit_by_group ON audit(group_name, audit_id)',
        """
        CREATE INDEX audit_by_disposition
        ON audit(group_name, disposition_id)
        WHERE disposition_id IS NOT NULL
        """,
    ),
)
UNRESOLVED_LATE_ID_COUNT_SQL = """SELECT COUNT(*) FROM (
    SELECT DISTINCT sl.event_id
    FROM source_late_ids AS sl
    JOIN discovery_ranges AS dr USING (obligation_id)
    JOIN range_groups AS rg USING (obligation_id)
    JOIN groups AS gm USING (group_name, group_generation)
    WHERE rg.group_name = ? AND rg.obligation_state = 'pending'
        AND dr.range_state = 'closed'
        AND NOT EXISTS (
            SELECT 1 FROM late_ids AS li
            WHERE li.group_name = rg.group_name
                AND li.obligation_id = rg.obligation_id
                AND li.event_id = sl.event_id
                AND li.terminal_state = 'done'
        )
)"""
UNRESOLVED_LATE_ID_PREFIX_SQL = """SELECT DISTINCT sl.event_id
    FROM source_late_ids AS sl
    JOIN discovery_ranges AS dr USING (obligation_id)
    JOIN range_groups AS rg USING (obligation_id)
    JOIN groups AS gm USING (group_name, group_generation)
    WHERE rg.group_name = ? AND rg.obligation_state = 'pending'
        AND dr.range_state = 'closed'
        AND NOT EXISTS (
            SELECT 1 FROM late_ids AS li
            WHERE li.group_name = rg.group_name
                AND li.obligation_id = rg.obligation_id
                AND li.event_id = sl.event_id
                AND li.terminal_state = 'done'
        )
    ORDER BY sl.event_id LIMIT ?"""


def source_identity(site: str, source: str) -> str:
    """Return the identity string stored in one source database.

    The existing identity check rejects a database file moved between sources.

    Args:
        site: Configured site name.
        source: Configured source name.

    Returns:
        The identity `source:<site>:<source>`.
    """
    return f'source:{site}:{source}'
