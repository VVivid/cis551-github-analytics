CREATE OR REPLACE VIEW "${database}"."completed_openings" AS
SELECT e.event_id, e.repository, e.event_created_at, e.archive_day, e.archive_hour
FROM "${database}"."opening_events" e
-- The ledger selects the current batch; orphaned and superseded files stay hidden.
JOIN "${database}"."hour_completions" c
  ON e.archive_day = c.archive_day
 AND e.archive_hour = c.archive_hour
 AND e.batch_id = c.batch_id
WHERE e.archive_day = '${archive_day}'
