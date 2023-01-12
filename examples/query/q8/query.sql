SELECT P1.src AS src, P1.dst AS via1, P2.dst AS via2, P3.dst AS via3, P4.dst AS dst
FROM path AS P1, path AS P2, path AS P3, path AS P4
WHERE P1.dst = P2.src AND P2.dst = P3.src AND P3.dst = P4.src
AND P2.src < P2.dst AND P3.src < P3.dst