-- Redshift Spectrum: query the curated Parquet in S3 through the Glue Data Catalog, without loading it
CREATE EXTERNAL SCHEMA IF NOT EXISTS lake
FROM DATA CATALOG DATABASE '{{curated_database}}'
IAM_ROLE default;
