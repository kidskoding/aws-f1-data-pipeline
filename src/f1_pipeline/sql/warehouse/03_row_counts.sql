-- Rows per warehouse table
SELECT 'drivers' AS table_name, COUNT(*) AS row_count FROM f1.drivers
UNION ALL SELECT 'constructors' AS table_name, COUNT(*) AS row_count FROM f1.constructors
UNION ALL SELECT 'circuits' AS table_name, COUNT(*) AS row_count FROM f1.circuits
UNION ALL SELECT 'races' AS table_name, COUNT(*) AS row_count FROM f1.races
UNION ALL SELECT 'results' AS table_name, COUNT(*) AS row_count FROM f1.results
UNION ALL SELECT 'official_standings' AS table_name, COUNT(*) AS row_count FROM f1.official_standings
ORDER BY table_name;
