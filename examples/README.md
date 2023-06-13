## Examples
Q1
```sql
SELECT g1.src AS src, g1.dst AS via1, g3.src AS via2, g3.dst AS dst,
    c1.cnt AS cnt1, c2.cnt AS cnt2
FROM Graph AS g1, Graph AS g2, Graph AS g3,
    (SELECT src, COUNT(*) AS cnt FROM Graph GROUP BY src) AS c1,
    (SELECT src, COUNT(*) AS cnt FROM Graph GROUP BY src) AS c2
WHERE c1.src = g1.src AND g1.dst = g2.src AND g2.dst = g3.src AND g3.dst = c2.src
    AND c1.cnt < c2.cnt
```

Q2
```sql
SELECT *
FROM Graph AS g1, Graph AS g2, Graph AS g3,
    Graph AS g4, Graph AS g5, Graph AS g6, Graph AS g7
WHERE g1.dst = g2.src AND g2.dst = g3.src AND g3.dst = g1.src
    AND g4.dst = g5.src AND g5.dst = g6.src AND g6.dst = g4.src
    AND g1.dst = g7.src AND g7.dst = g4.src
    AND g1.src + g2.src + g3.src < g4.src + g5.src + g6.src
```

Q3
```sql
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
```

Q4
```sql
SELECT g3.src AS src, g3.dst AS dst
FROM Graph AS g1, Graph AS g2, Graph AS g3,
    (SELECT src, COUNT(*) AS cnt FROM Graph GROUP BY src) AS c1,
    (SELECT src, COUNT(*) AS cnt FROM Graph GROUP BY src) AS c2
WHERE c1.src = g1.src AND g1.dst = g2.src AND g2.dst = g3.src AND g3.dst = c2.src
    AND c1.cnt < c2.cnt
```

Q5
```sql
SELECT g2.src, g2.dst
FROM Graph AS g1, Graph AS g2, Graph AS g3, Graph AS g4, Graph AS g5,
    (SELECT src, COUNT(*) AS cnt FROM Graph GROUP BY src) AS c1,
    (SELECT src, COUNT(*) AS cnt FROM Graph GROUP BY src) AS c2,
    (SELECT dst, COUNT(*) AS cnt FROM Graph GROUP BY dst) AS c3,
    (SELECT dst, COUNT(*) AS cnt FROM Graph GROUP BY dst) AS c4
WHERE g1.dst = g2.src AND g2.dst = g3.src AND g1.src = c1.src
    AND g3.dst = c2.src AND c1.cnt < c2.cnt
    AND g4.dst = g2.src AND g2.dst = g5.src AND g4.src = c3.dst
    AND g5.dst = c4.dst AND c3.cnt < c4.cnt
```

Q6
```sql
SELECT g1.src AS src, g1.dst AS via1, g3.src AS via2, g3.dst AS dst,
    c1.cnt AS cnt1, c2.cnt AS cnt2
FROM Graph AS g1, Graph AS g2, Graph AS g3,
    (SELECT src, COUNT(*) AS cnt FROM Graph GROUP BY src) AS c1,
    (SELECT src, COUNT(*) AS cnt FROM Graph GROUP BY src) AS c2
WHERE c1.src = g1.src AND g1.dst = g2.src AND g2.dst = g3.src AND g3.dst = c2.src
    AND c1.cnt < g3.dst
```

Q7
```sql
SELECT g1.src AS src, g1.dst AS via1, g3.src AS via2, g3.dst AS dst,
    c1.cnt AS cnt1, c2.cnt AS cnt2
FROM Graph AS g1, Graph AS g2, Graph AS g3,
    (SELECT src, COUNT(*) AS cnt FROM Graph GROUP BY src) AS c1,
    (SELECT src, COUNT(*) AS cnt FROM Graph GROUP BY src) AS c2
WHERE c1.src = g1.src AND g1.dst = g2.src AND g2.dst = g3.src AND g3.dst = c2.src
    AND c1.cnt < g1.dst AND c2.cnt < g3.src
```

Q8
```sql
SELECT g1.src AS src, g1.dst AS via1, g2.dst AS via2, g3.dst AS via3, g4.dst AS dst
FROM Graph AS g1, Graph AS g2, Graph AS g3, Graph AS g4
WHERE g1.dst = g2.src AND g2.dst = g3.src AND g3.dst = g4.src
    AND g2.src < g2.dst AND g3.src < g3.dst
```

Q9
```sql
SELECT t1.T_ID, t1.T_DTS, t1.T_TT_ID, t1.T_TRADE_PRICE,
    t2.T_ID, t2.T_DTS, t2.T_TT_ID, t2.T_TRADE_PRICE,
    t1.T_S_SYMB, t1.T_CA_ID
FROM Trade t1, Trade t2
WHERE t1.T_TT_ID LIKE '%B%' AND t2.T_TT_ID LIKE '%S%'
    AND t1.T_CA_ID = t2.T_CA_ID AND t1.T_S_SYMB = t2.T_S_SYMB
    AND t1.T_DTS <= t2.T_DTS AND t1.T_DTS + interval '90' day >= t2.T_DTS
    AND t1.T_TRADE_PRICE * 1.2 < t2.T_TRADE_PRICE
```

Q10
```sql
SELECT g7.src, g7.dst
FROM Graph AS g1, Graph AS g2, Graph AS g3,
    Graph AS g4, Graph AS g5, Graph AS g6, Graph AS g7
WHERE g1.dst = g2.src AND g2.dst = g3.src AND g3.dst = g1.src
    AND g4.dst = g5.src AND g5.dst = g6.src AND g6.dst = g4.src
    AND g1.dst = g7.src AND g7.dst = g4.src
    AND g1.src + g2.src + g3.src < g4.src + g5.src + g6.src
```

Q11
```sql
SELECT r.a, s.b, t.c, t.f
FROM R AS r, S AS s, T AS t
WHERE r.b = s.b AND s.c = t.c AND r.a < t.e
```