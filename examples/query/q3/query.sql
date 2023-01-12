SELECT P1.src AS src, P1.dst AS via1, P3.src AS via2, P3.dst AS dst,
    C1.cnt AS cnt1, C2.cnt AS cnt2, C3.cnt AS cnt3, C4.cnt AS cnt4
FROM path AS P1, path AS P2, path AS P3,
(SELECT src, COUNT(*) AS cnt FROM path GROUP BY src) AS C1,
(SELECT src, COUNT(*) AS cnt FROM path GROUP BY src) AS C2,
(SELECT src, COUNT(*) AS cnt FROM path GROUP BY src) AS C3,
(SELECT dst, COUNT(*) AS cnt FROM path GROUP BY dst) AS C4
WHERE P1.dst = P2.src AND P2.dst = P3.src
AND C1.src = P1.src AND C2.src = P3.dst AND C3.src = P2.src AND C4.dst = P3.dst
AND C1.cnt < C2.cnt AND C3.cnt < C4.cnt