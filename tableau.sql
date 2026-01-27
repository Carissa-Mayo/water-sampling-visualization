USE wqp;
GO

CREATE OR ALTER VIEW wqp.v_results_enriched AS
SELECT
  r.result_id,
  r.site_id,
  s.site_name,
  s.lat,
  s.lon,
  s.huc8,
  s.state_fips,
  s.county_fips5,
  s.provider,
  r.sample_date,
  r.chemical,
  r.water_type,
  r.location_type,
  r.activity_media,
  r.activity_media_sub,
  r.is_nondetect,
  r.value_for_stats,
  r.value_std,
  r.std_unit,
  r.rl_raw,
  r.detect_condition
FROM wqp.results r
LEFT JOIN wqp.sites s
  ON s.site_id = r.site_id;
GO

CREATE OR ALTER VIEW wqp.v_monthly_summary AS
SELECT
  chemical,
  water_type,
  month_start,
  metric,
  n,
  n_sites,
  nd_frac,
  mean_value,
  median_value,
  ci_low,
  ci_high
FROM wqp.agg_month;
GO