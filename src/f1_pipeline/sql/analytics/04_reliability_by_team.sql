-- Reliability: share of race starts that ended in a DNF, by team, latest season
SELECT t.name                                                                  AS team,
       COUNT(*)                                                                AS starts,
       SUM(CASE WHEN r.finish_category = 'dnf' THEN 1 ELSE 0 END)              AS dnfs,
       ROUND(100.0 * SUM(CASE WHEN r.finish_category = 'dnf' THEN 1 ELSE 0 END) / COUNT(*), 1) AS dnf_pct
FROM f1.results r
JOIN f1.constructors t ON t.constructor_id = r.constructor_id
WHERE r.session = 'race'
  AND r.finish_category <> 'did_not_start'
  AND r.season = (SELECT MAX(season) FROM f1.results)
GROUP BY t.name
ORDER BY dnf_pct DESC, team;
