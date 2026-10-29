-- Reconciliation: points we computed from race and sprint results vs the official standings
WITH computed AS (
  SELECT season, driver_id, SUM(points) AS points
  FROM f1.results
  GROUP BY season, driver_id
)
SELECT o.season,
       COUNT(*)                                                          AS drivers,
       SUM(CASE WHEN o.points = COALESCE(c.points, 0) THEN 1 ELSE 0 END)  AS matching,
       SUM(CASE WHEN o.points <> COALESCE(c.points, 0) THEN 1 ELSE 0 END) AS mismatched
FROM f1.official_standings o
LEFT JOIN computed c ON c.season = o.season AND c.driver_id = o.driver_id
GROUP BY o.season
ORDER BY o.season;
