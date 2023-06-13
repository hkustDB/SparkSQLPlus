SELECT g1.src AS src, g1.dst AS via1, g3.src AS via2, g3.dst AS dst,
    c1.cnt AS cnt1, c2.cnt AS cnt2, c3.cnt AS cnt3, c4.cnt AS cnt4
FROM Graph AS g1, Graph AS g2, Graph AS g3,
    (SELECT src, COUNT(*) AS cnt FROM Graph GROUP BY src) AS c1,
    (SELECT src, COUNT(*) AS cnt FROM Graph GROUP BY src) AS c2,
    (SELECT src, COUNT(*) AS cnt FROM Graph GROUP BY src) AS c3,
    (SELECT dst, COUNT(*) AS cnt FROM Graph GROUP BY dst) AS c4
WHERE g1.dst = g2.src AND g2.dst = g3.src
    AND c1.src = g1.src AND c2.src = g3.dst
    AND c3.src = g2.src AND c4.dst = g3.dst
    AND c1.cnt < c2.cnt AND c3.cnt < c4.cnt