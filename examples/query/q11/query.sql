SELECT r.a, s.b, t.c, t.f
FROM R AS r, S AS s, T AS t
WHERE r.b = s.b AND s.c = t.c AND r.a < t.e