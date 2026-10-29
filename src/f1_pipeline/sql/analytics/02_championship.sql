-- Championship top 10 for the latest season, computed from race and sprint results
SELECT ROW_NUMBER() OVER (ORDER BY SUM(r.points) DESC)                     AS pos,
       d.given_name || ' ' || d.family_name                                AS driver,
       t.name                                                              AS team,
       SUM(r.points)                                                       AS points,
       SUM(CASE WHEN r.session = 'race' AND r.position = 1 THEN 1 ELSE 0 END)  AS wins,
       SUM(CASE WHEN r.session = 'race' AND r.position <= 3 THEN 1 ELSE 0 END) AS podiums
FROM f1.results r
JOIN f1.drivers d ON d.driver_id = r.driver_id
JOIN f1.official_standings s ON s.season = r.season AND s.driver_id = r.driver_id
JOIN f1.constructors t ON t.constructor_id = s.constructor_id
WHERE r.season = (SELECT MAX(season) FROM f1.results)
GROUP BY d.given_name, d.family_name, t.name
ORDER BY points DESC
LIMIT 10;
