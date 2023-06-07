SELECT g7.src, g7.dst
From Graph g1, Graph g2, Graph g3, Graph g4, Graph g5, Graph g6, Graph g7
where g1.src = g3.dst and g2.src = g1.dst and g3.src=g2.dst
and g4.src = g6.dst and g5.src = g4.dst and g6.src = g5.dst
and g1.dst = g7.src and g4.src = g7.dst and
g1.src+g2.src+g3.src  < g4.src+g5.src+g6.src