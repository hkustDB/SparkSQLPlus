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