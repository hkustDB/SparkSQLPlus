SELECT g1.src AS src, g1.dst AS via1, g3.src AS via2, g3.dst AS dst,
    c1.cnt AS cnt1, c2.cnt AS cnt2
FROM Graph AS g1, Graph AS g2, Graph AS g3,
    (SELECT src, COUNT(*) AS cnt FROM Graph GROUP BY src) AS c1,
    (SELECT src, COUNT(*) AS cnt FROM Graph GROUP BY src) AS c2
WHERE c1.src = g1.src AND g1.dst = g2.src AND g2.dst = g3.src AND g3.dst = c2.src
    AND c1.cnt < g3.dst