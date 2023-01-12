SELECT P3.src AS src, P3.dst AS dst
FROM path AS P1, path AS P2, path AS P3,
(SELECT src, COUNT(*) AS cnt FROM path GROUP BY src) AS C1,
(SELECT src, COUNT(*) AS cnt FROM path GROUP BY src) AS C2
WHERE C1.src = P1.src AND P1.dst = P2.src AND P2.dst = P3.src AND P3.dst = C2.src
AND C1.cnt < C2.cnt