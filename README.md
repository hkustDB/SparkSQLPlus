# SparkSQL+

## Example queries
Q1
```
SELECT C1.cnt AS cnt1, P1.src AS src, P1.dst AS via1, P3.src AS via2, P3.dst AS dst, C2.cnt AS cnt2
FROM path AS P1, path AS P2, path AS P3,
(SELECT src, COUNT(*) AS cnt FROM path GROUP BY src) AS C1,
(SELECT src, COUNT(*) AS cnt FROM path GROUP BY src) AS C2
WHERE C1.src = P1.src AND P1.dst = P2.src AND P2.dst = P3.src AND P3.dst = C2.src
AND C1.cnt < C2.cnt
```

Q2
```
SELECT *
From Graph g1, Graph g2, Graph g3, Graph g4, Graph g5, Graph g6, Graph g7
where g1.src = g3.dst and g2.src = g1.dst and g3.src=g2.dst
and g4.src = g6.dst and g5.src = g4.dst and g6.src = g5.dst
and g1.dst = g7.src and g4.src = g7.dst and
g1.weight+g2.weight+g3.weight < g4.weight+g5.weight+g6.weight
```

Q3
```
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
```

Q4
```
SELECT P3.src AS src, P3.dst AS dst
FROM path AS P1, path AS P2, path AS P3,
(SELECT src, COUNT(*) AS cnt FROM path GROUP BY src) AS C1,
(SELECT src, COUNT(*) AS cnt FROM path GROUP BY src) AS C2
WHERE C1.src = P1.src AND P1.dst = P2.src AND P2.dst = P3.src AND P3.dst = C2.src
AND C1.cnt < C2.cnt
```

Q5
```
select g2.src, g2.dst
from path g1, path g2, path g3, path g4, path g5,
     (select src, count(*) as cnt from path group by src) as c1,
     (select src, count(*) as cnt from path group by src) as c2,
     (select dst, count(*) as cnt from path group by dst) as c3,
     (select dst, count(*) as cnt from path group by dst) as c4
where g1.dst = g2.src and g2.dst = g3.src and g1.src = c1.src
  and g3.dst = c2.src and c1.cnt < c2.cnt
  and g4.dst = g2.src and g2.dst = g5.src and g4.src = c3.dst
  and g5.dst = c4.dst and c3.cnt < c4.cnt
```

Q6
```
SELECT C1.cnt AS cnt1, P1.src AS src, P1.dst AS via1, P3.src AS via2, P3.dst AS dst, C2.cnt AS cnt2
FROM path AS P1, path AS P2, path AS P3,
(SELECT src, COUNT(*) AS cnt FROM path GROUP BY src) AS C1,
(SELECT src, COUNT(*) AS cnt FROM path GROUP BY src) AS C2
WHERE C1.src = P1.src AND P1.dst = P2.src AND P2.dst = P3.src AND P3.dst = C2.src
AND C1.cnt < P3.dst
```

Q7
```
SELECT C1.cnt AS cnt1, P1.src AS src, P1.dst AS via1, P3.src AS via2, P3.dst AS dst, C2.cnt AS cnt2
FROM path AS P1, path AS P2, path AS P3,
(SELECT src, COUNT(*) AS cnt FROM path GROUP BY src) AS C1,
(SELECT src, COUNT(*) AS cnt FROM path GROUP BY src) AS C2
WHERE C1.src = P1.src AND P1.dst = P2.src AND P2.dst = P3.src AND P3.dst = C2.src
AND C1.cnt < P1.dst AND C2.cnt < P3.src
```

Q8
```
SELECT P1.src AS src, P1.dst AS via1, P2.dst AS via2, P3.dst AS via3, P4.dst AS dst
FROM path AS P1, path AS P2, path AS P3, path AS P4
WHERE P1.dst = P2.src AND P2.dst = P3.src AND P3.dst = P4.src
AND P2.src < P2.dst AND P3.src < P3.dst
```

## usage
```
    sparksql-plus compiles the input SQL file into SparkSQL+ code.
    
    syntax: sparksql-plus [OPTIONS] <query>
      options:
         -d,--ddl <path>           Set the path to the ddl file.
         -h,--help                 Show the help message.
         -n,--name <object name>   Set the object name for the output object.
         -o,--output <path>        Set the path to the output file.
         -p,--pkg <package name>   Set the package name for the output object.

```
