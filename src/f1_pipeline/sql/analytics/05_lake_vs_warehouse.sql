-- The same data read two ways: loaded into Redshift, and in place on S3 through Redshift Spectrum
SELECT 'warehouse: f1.results' AS source, COUNT(*) AS row_count, SUM(points) AS total_points FROM f1.results
UNION ALL
SELECT 'lake via Spectrum: lake.results', COUNT(*), SUM(points) FROM lake.results;
