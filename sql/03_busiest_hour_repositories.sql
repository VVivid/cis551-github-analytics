CREATE OR REPLACE VIEW "${database}"."busiest_hour_repositories" AS
WITH busiest AS (
  SELECT archive_hour, hour_utc
  FROM "${database}"."hourly_status"
  WHERE processing_status = 'COMPLETED'
  ORDER BY opening_count DESC, archive_hour ASC
  LIMIT 1
), counts AS (
  SELECT b.hour_utc, e.repository, count(*) AS opening_count
  FROM "${database}"."completed_openings" e
  JOIN busiest b ON e.archive_hour = b.archive_hour
  GROUP BY b.hour_utc, e.repository
)
SELECT hour_utc, repository, opening_count,
       row_number() OVER (ORDER BY opening_count DESC, repository ASC) AS repository_rank,
       100.0 * opening_count / sum(opening_count) OVER () AS share_percent
FROM counts
