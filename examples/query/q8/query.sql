SELECT g1.src AS src, g1.dst AS via1, g2.dst AS via2, g3.dst AS via3, g4.dst AS dst
FROM Graph AS g1, Graph AS g2, Graph AS g3, Graph AS g4
WHERE g1.dst = g2.src AND g2.dst = g3.src AND g3.dst = g4.src
    AND g2.src < g2.dst AND g3.src < g3.dst