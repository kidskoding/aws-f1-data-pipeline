-- Replace every table's contents. Runs inside the same transaction as the schema step.

-- Replace f1.drivers from curated Parquet
DELETE FROM f1.drivers;
COPY f1.drivers FROM 's3://{{bucket}}/{{curated_prefix}}/drivers/' IAM_ROLE default FORMAT AS PARQUET;

-- Replace f1.constructors from curated Parquet
DELETE FROM f1.constructors;
COPY f1.constructors FROM 's3://{{bucket}}/{{curated_prefix}}/constructors/' IAM_ROLE default FORMAT AS PARQUET;

-- Replace f1.circuits from curated Parquet
DELETE FROM f1.circuits;
COPY f1.circuits FROM 's3://{{bucket}}/{{curated_prefix}}/circuits/' IAM_ROLE default FORMAT AS PARQUET;

-- Replace f1.races from curated Parquet
DELETE FROM f1.races;
COPY f1.races FROM 's3://{{bucket}}/{{curated_prefix}}/races/' IAM_ROLE default FORMAT AS PARQUET;

-- Replace f1.results from curated Parquet
DELETE FROM f1.results;
COPY f1.results FROM 's3://{{bucket}}/{{curated_prefix}}/results/' IAM_ROLE default FORMAT AS PARQUET;

-- Replace f1.official_standings from curated Parquet
DELETE FROM f1.official_standings;
COPY f1.official_standings FROM 's3://{{bucket}}/{{curated_prefix}}/official_standings/' IAM_ROLE default FORMAT AS PARQUET;
