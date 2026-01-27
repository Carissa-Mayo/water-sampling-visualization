USE wqp;
GO

/* 0) DROP TABLES */
DROP TABLE IF EXISTS wqp.results;
DROP TABLE IF EXISTS wqp.sites;
DROP TABLE IF EXISTS wqp.agg_month;
DROP TABLE IF EXISTS wqp.test_results;

DROP TABLE IF EXISTS wqp.stg_sites_tsv;
DROP TABLE IF EXISTS wqp.stg_results_tsv;
DROP TABLE IF EXISTS wqp.stg_agg_month_tsv;
DROP TABLE IF EXISTS wqp.stg_test_results_tsv;
GO

/* 1) STAGING TABLES (all NVARCHAR) */
CREATE TABLE wqp.stg_sites_tsv (
  site_id        NVARCHAR(200)  NULL,
  site_name      NVARCHAR(4000) NULL,
  lat            NVARCHAR(100)  NULL,
  lon            NVARCHAR(100)  NULL,
  huc8           NVARCHAR(20)   NULL,
  state_fips     NVARCHAR(10)   NULL,
  county_fips5   NVARCHAR(10)   NULL,
  provider       NVARCHAR(200)  NULL
);
GO

CREATE TABLE wqp.stg_results_tsv (
  result_id           NVARCHAR(200)  NULL,
  site_id             NVARCHAR(200)  NULL,
  sample_date         NVARCHAR(50)   NULL,
  chemical            NVARCHAR(50)   NULL,
  unit_raw            NVARCHAR(200)  NULL,
  value_raw           NVARCHAR(100)  NULL,
  rl_raw              NVARCHAR(100)  NULL,
  detect_condition    NVARCHAR(200)  NULL,
  activity_media      NVARCHAR(200)  NULL,
  activity_media_sub  NVARCHAR(200)  NULL,
  location_type       NVARCHAR(400)  NULL,
  is_nondetect        NVARCHAR(20)   NULL,
  value_for_stats     NVARCHAR(100)  NULL,
  water_type          NVARCHAR(50)   NULL,
  value_std           NVARCHAR(100)  NULL,
  std_unit            NVARCHAR(50)   NULL
);
GO

CREATE TABLE wqp.stg_agg_month_tsv (
  chemical     NVARCHAR(50)  NULL,
  water_type   NVARCHAR(50)  NULL,
  month_start  NVARCHAR(50)  NULL,
  n            NVARCHAR(50)  NULL,
  n_sites      NVARCHAR(50)  NULL,
  nd_frac      NVARCHAR(50)  NULL,
  mean_value   NVARCHAR(100) NULL,
  median_value NVARCHAR(100) NULL,
  ci_low       NVARCHAR(100) NULL,
  ci_high      NVARCHAR(100) NULL,
  metric       NVARCHAR(20)  NULL
);
GO

CREATE TABLE wqp.stg_test_results_tsv (
  chemical    NVARCHAR(50)  NULL,
  water_type  NVARCHAR(50)  NULL,
  test_name   NVARCHAR(100) NULL,
  n_a         NVARCHAR(50)  NULL,
  n_b         NVARCHAR(50)  NULL,
  effect      NVARCHAR(100) NULL,
  ci_low      NVARCHAR(100) NULL,
  ci_high     NVARCHAR(100) NULL,
  p_value     NVARCHAR(100) NULL
);
GO

/* 2) TYPED TABLES (Tableau-ready) */
CREATE TABLE wqp.sites (
  site_id      NVARCHAR(200)  NOT NULL PRIMARY KEY,
  site_name    NVARCHAR(4000) NULL,
  lat          FLOAT          NULL,
  lon          FLOAT          NULL,
  huc8         NVARCHAR(20)   NULL,
  state_fips   NVARCHAR(10)   NULL,
  county_fips5 NVARCHAR(10)   NULL,
  provider     NVARCHAR(200)  NULL
);
GO

CREATE TABLE wqp.results (
  result_id          NVARCHAR(200) NOT NULL PRIMARY KEY,
  site_id            NVARCHAR(200) NOT NULL,
  sample_date        DATE          NULL,
  chemical           NVARCHAR(50)  NULL,
  unit_raw           NVARCHAR(200) NULL,
  value_raw          FLOAT         NULL,
  rl_raw             FLOAT         NULL,
  detect_condition   NVARCHAR(200) NULL,
  activity_media     NVARCHAR(200) NULL,
  activity_media_sub NVARCHAR(200) NULL,
  location_type      NVARCHAR(400) NULL,
  is_nondetect       BIT           NULL,
  value_for_stats    FLOAT         NULL,
  water_type         NVARCHAR(50)  NULL,
  value_std          FLOAT         NULL,
  std_unit           NVARCHAR(50)  NULL
);
GO

CREATE TABLE wqp.agg_month (
  chemical     NVARCHAR(50) NOT NULL,
  water_type   NVARCHAR(50) NOT NULL,
  month_start  DATE         NOT NULL,
  n            INT          NULL,
  n_sites      INT          NULL,
  nd_frac      FLOAT        NULL,
  mean_value   FLOAT        NULL,
  median_value FLOAT        NULL,
  ci_low       FLOAT        NULL,
  ci_high      FLOAT        NULL,
  metric       NVARCHAR(20) NOT NULL,
  CONSTRAINT PK_agg_month PRIMARY KEY (chemical, water_type, month_start, metric)
);
GO

CREATE TABLE wqp.test_results (
  chemical    NVARCHAR(50)  NOT NULL,
  water_type  NVARCHAR(50)  NOT NULL,
  test_name   NVARCHAR(100) NOT NULL,
  n_a         INT           NULL,
  n_b         INT           NULL,
  effect      FLOAT         NULL,
  ci_low      FLOAT         NULL,
  ci_high     FLOAT         NULL,
  p_value     FLOAT         NULL,
  CONSTRAINT PK_test_results PRIMARY KEY (chemical, water_type, test_name)
);
GO

/* 3) BULK INSERT (staging to SQL server) */
TRUNCATE TABLE wqp.stg_sites_tsv;
TRUNCATE TABLE wqp.stg_results_tsv;
TRUNCATE TABLE wqp.stg_agg_month_tsv;
TRUNCATE TABLE wqp.stg_test_results_tsv;
GO

BULK INSERT wqp.stg_sites_tsv
FROM '/var/opt/mssql/data/sites.tsv'
WITH (FIRSTROW=2, FIELDTERMINATOR='0x09', ROWTERMINATOR='0x0a', TABLOCK);
GO

BULK INSERT wqp.stg_results_tsv
FROM '/var/opt/mssql/data/results.tsv'
WITH (FIRSTROW=2, FIELDTERMINATOR='0x09', ROWTERMINATOR='0x0a', TABLOCK);
GO

BULK INSERT wqp.stg_agg_month_tsv
FROM '/var/opt/mssql/data/agg_month.tsv'
WITH (FIRSTROW=2, FIELDTERMINATOR='0x09', ROWTERMINATOR='0x0a', TABLOCK);
GO

BULK INSERT wqp.stg_test_results_tsv
FROM '/var/opt/mssql/data/test_results.tsv'
WITH (FIRSTROW=2, FIELDTERMINATOR='0x09', ROWTERMINATOR='0x0a', TABLOCK);
GO

/* 4) LOAD TYPED TABLES */
-- sites
INSERT INTO wqp.sites (site_id, site_name, lat, lon, huc8, state_fips, county_fips5, provider)
SELECT
  site_id,
  site_name,
  TRY_CONVERT(float, NULLIF(lat, '')) AS lat,
  TRY_CONVERT(float, NULLIF(lon, '')) AS lon,
  NULLIF(huc8, ''),
  NULLIF(state_fips, ''),
  NULLIF(county_fips5, ''),
  NULLIF(provider, '')
FROM wqp.stg_sites_tsv
WHERE NULLIF(site_id,'') IS NOT NULL;
GO

-- results
INSERT INTO wqp.results (
  result_id, site_id, sample_date, chemical, unit_raw,
  value_raw, rl_raw, detect_condition, is_nondetect,
  value_for_stats, value_std, std_unit,
  activity_media, activity_media_sub, location_type, water_type
)
SELECT
  result_id,
  site_id,
  TRY_CONVERT(date, sample_date) AS sample_date,
  NULLIF(chemical,'') AS chemical,
  NULLIF(unit_raw,'') AS unit_raw,
  TRY_CONVERT(float, NULLIF(value_raw, '')) AS value_raw,
  TRY_CONVERT(float, NULLIF(rl_raw, '')) AS rl_raw,
  NULLIF(detect_condition,'') AS detect_condition,
  CASE
    WHEN LTRIM(RTRIM(LOWER(ISNULL(is_nondetect,'')))) IN
        ('1','true','t','yes','y','nd','nondetect','non-detect')
    THEN 1 ELSE 0
  END,
  TRY_CONVERT(float, NULLIF(REPLACE(REPLACE(LOWER(ISNULL(value_for_stats,'')),'nan',''),'na',''), '')) AS value_for_stats,
  TRY_CONVERT(float, NULLIF(REPLACE(REPLACE(LOWER(ISNULL(value_std,'')),'nan',''),'na',''), '')) AS value_std,
  NULLIF(std_unit,'') AS std_unit,
  NULLIF(activity_media,'') AS activity_media,
  NULLIF(activity_media_sub,'') AS activity_media_sub,
  NULLIF(location_type,'') AS location_type,
  NULLIF(water_type,'') AS water_type
FROM wqp.stg_results_tsv
WHERE NULLIF(result_id,'') IS NOT NULL
  AND NULLIF(site_id,'') IS NOT NULL;
GO

-- agg_month
INSERT INTO wqp.agg_month (
  chemical, water_type, month_start, metric, n, n_sites, nd_frac, mean_value, median_value, ci_low, ci_high
)
SELECT
  chemical,
  water_type,
  TRY_CONVERT(date, month_start) AS month_start,
  metric,
  TRY_CONVERT(int, NULLIF(n, '')) AS n,
  TRY_CONVERT(int, NULLIF(n_sites, '')) AS n_sites,
  TRY_CONVERT(float, NULLIF(nd_frac, '')) AS nd_frac,
  TRY_CONVERT(float, NULLIF(mean_value, '')) AS mean_value,
  TRY_CONVERT(float, NULLIF(median_value, '')) AS median_value,
  TRY_CONVERT(float, NULLIF(ci_low, '')) AS ci_low,
  TRY_CONVERT(float, NULLIF(ci_high, '')) AS ci_high
FROM wqp.stg_agg_month_tsv
WHERE NULLIF(chemical,'') IS NOT NULL
  AND NULLIF(water_type,'') IS NOT NULL
  AND TRY_CONVERT(date, month_start) IS NOT NULL
  AND NULLIF(metric,'') IS NOT NULL;
GO

-- test_results
INSERT INTO wqp.test_results (chemical, water_type, test_name, n_a, n_b, effect, ci_low, ci_high, p_value)
SELECT
  chemical,
  water_type,
  test_name,
  TRY_CONVERT(int, NULLIF(n_a, '')) AS n_a,
  TRY_CONVERT(int, NULLIF(n_b, '')) AS n_b,
  TRY_CONVERT(float, NULLIF(effect, '')) AS effect,
  TRY_CONVERT(float, NULLIF(ci_low, '')) AS ci_low,
  TRY_CONVERT(float, NULLIF(ci_high, '')) AS ci_high,
  TRY_CONVERT(float, NULLIF(p_value, '')) AS p_value
FROM wqp.stg_test_results_tsv
WHERE NULLIF(chemical,'') IS NOT NULL
  AND NULLIF(water_type,'') IS NOT NULL
  AND NULLIF(test_name,'') IS NOT NULL;
GO

/* 5) COUNTS */
SELECT 'sites' tbl, COUNT(*) n FROM wqp.sites
UNION ALL SELECT 'results', COUNT(*) FROM wqp.results
UNION ALL SELECT 'agg_month', COUNT(*) FROM wqp.agg_month
UNION ALL SELECT 'test_results', COUNT(*) FROM wqp.test_results;
GO
