CREATE OR REPLACE VIEW "${database}"."repository_totals" AS
WITH counts AS (
  SELECT repository, count(*) AS opening_count
  FROM "${database}"."completed_openings"
  GROUP BY repository
)
SELECT repository, opening_count,
       row_number() OVER (ORDER BY opening_count DESC, repository ASC) AS repository_rank,
       100.0 * opening_count / sum(opening_count) OVER () AS share_percent
FROM counts
