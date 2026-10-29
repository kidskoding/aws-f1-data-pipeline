-- Average places gained from grid to finish, classified race finishes only (at least 30 starts)
SELECT driver, starts, avg_places_gained
FROM (
  SELECT d.given_name || ' ' || d.family_name                         AS driver,
         COUNT(*)                                                     AS starts,
         ROUND(AVG(CAST(r.grid - r.position AS DECIMAL(8,2))), 2)      AS avg_places_gained
  FROM f1.results r
  JOIN f1.drivers d ON d.driver_id = r.driver_id
  WHERE r.session = 'race'
    AND r.classified
    AND r.grid > 0  -- grid 0 means a pit lane start, which would inflate the gain
  GROUP BY d.given_name, d.family_name
  HAVING COUNT(*) >= 30
) ranked
ORDER BY avg_places_gained DESC
LIMIT 10;
