SELECT g7.src, g7.dst
FROM Graph AS g1, Graph AS g2, Graph AS g3,
    Graph AS g4, Graph AS g5, Graph AS g6, Graph AS g7
WHERE g1.dst = g2.src AND g2.dst = g3.src AND g3.dst = g1.src
    AND g4.dst = g5.src AND g5.dst = g6.src AND g6.dst = g4.src
    AND g1.dst = g7.src AND g7.dst = g4.src
    AND g1.src + g2.src + g3.src < g4.src + g5.src + g6.src