SELECT g1.a, g1.b, g3.c, g3.f
FROM g1, g2, g3
WHERE g1.b = g2.b AND g2.c = g3.c AND g1.a < g3.e