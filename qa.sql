USE wqp;
GO

-- Orphan results (site_id missing from sites)
SELECT TOP 50 r.site_id, COUNT(*) AS n
FROM wqp.results r
LEFT JOIN wqp.sites s ON s.site_id = r.site_id
WHERE s.site_id IS NULL
GROUP BY r.site_id
ORDER BY n DESC;

-- Duplicate primary keys (should be zero rows)
SELECT result_id, COUNT(*) n
FROM wqp.results
GROUP BY result_id
HAVING COUNT(*) > 1;

SELECT
  MIN(sample_date) AS min_date,
  MAX(sample_date) AS max_date,
  COUNT(*) AS n_results
FROM wqp.results;

SELECT chemical, std_unit, COUNT(*) n
FROM wqp.results
GROUP BY chemical, std_unit
ORDER BY chemical, n DESC;

SELECT
  chemical,
  water_type,
  AVG(CAST(is_nondetect AS float)) AS nd_frac_calc,
  COUNT(*) AS n
FROM wqp.results
GROUP BY chemical, water_type
ORDER BY n DESC;


SELECT chemical, is_nondetect, COUNT(*) AS n
FROM wqp.results
GROUP BY chemical, is_nondetect
ORDER BY chemical, is_nondetect;
