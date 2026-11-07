CREATE OR REPLACE VIEW "${database}"."hourly_status" AS
WITH hours(archive_hour) AS (
  VALUES (0), (1), (2), (3), (4), (5), (6), (7), (8), (9), (10), (11),
         (12), (13), (14), (15), (16), (17), (18), (19), (20), (21), (22), (23)
)
SELECT '${archive_day}' AS archive_day, h.archive_hour,
       lpad(cast(h.archive_hour AS varchar), 2, '0') || ':00' AS hour_utc,
       CASE WHEN c.batch_id IS NULL THEN 'MISSING' ELSE 'COMPLETED' END AS processing_status,
       c.opening_count, c.raw_events, c.rejected_openings, c.duplicate_openings, c.completed_at
FROM hours h
LEFT JOIN "${database}"."hour_completions" c
  ON c.archive_day = '${archive_day}' AND c.archive_hour = h.archive_hour
