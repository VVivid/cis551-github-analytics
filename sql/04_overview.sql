CREATE OR REPLACE VIEW "${database}"."overview" AS
WITH coverage AS (
  SELECT count(CASE WHEN processing_status = 'COMPLETED' THEN 1 END) AS completed_hours,
         coalesce(sum(opening_count), 0) AS total_openings
  FROM "${database}"."hourly_status"
), busiest AS (
  SELECT hour_utc, opening_count
  FROM "${database}"."hourly_status"
  WHERE processing_status = 'COMPLETED'
  ORDER BY opening_count DESC, archive_hour ASC
  LIMIT 1
), top_ten AS (
  SELECT sum(opening_count) AS openings
  FROM "${database}"."repository_totals"
  WHERE repository_rank <= 10
), busiest_top AS (
  SELECT repository, share_percent
  FROM "${database}"."busiest_hour_repositories"
  WHERE repository_rank = 1
)
SELECT '${archive_day}' AS archive_day,
       CASE WHEN c.completed_hours = 24 THEN 'FULL DAY' ELSE 'PROVISIONAL' END AS report_status,
       c.completed_hours, 24 - c.completed_hours AS missing_hours, c.total_openings,
       b.hour_utc AS busiest_hour_utc, b.opening_count AS busiest_hour_openings,
       100.0 * t.openings / nullif(c.total_openings, 0) AS top_10_share_percent,
       bt.repository AS busiest_top_repository,
       bt.share_percent AS busiest_top_repository_share_percent
FROM coverage c
CROSS JOIN top_ten t
LEFT JOIN busiest b ON true
LEFT JOIN busiest_top bt ON true
