SELECT C1.cnt AS cnt1, P1.src AS src, P1.dst AS via1, P3.src AS via2, P3.dst AS dst, C2.cnt AS cnt2
FROM path AS P1, path AS P2, path AS P3,
(SELECT src, COUNT(*) AS cnt FROM path GROUP BY src) AS C1,
(SELECT src, COUNT(*) AS cnt FROM path GROUP BY src) AS C2
WHERE C1.src = P1.src AND P1.dst = P2.src AND P2.dst = P3.src AND P3.dst = C2.src
AND C1.cnt < P3.dst