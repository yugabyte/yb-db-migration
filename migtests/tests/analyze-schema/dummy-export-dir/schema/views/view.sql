--view ddl having WITH CHECK OPTION
CREATE VIEW v1 AS SELECT * FROM t1 WHERE a < 2
WITH CHECK OPTION;

--view ddl having WITH LOCAL CHECK OPTION
CREATE VIEW v2 AS SELECT * FROM t1 WHERE a < 2
WITH LOCAL CHECK OPTION;

--dropping multiple objects
DROP VIEW IF EXISTS view1,view2,view3;

--alter view
ALTER VIEW view_name TO select * from test;

--JSON_ARRAYAGG() not available
CREATE OR REPLACE VIEW test AS (
                            select x , JSON_ARRAYAGG(trunc(b, 2) order by t desc) as agg
                            FROM test1
                            where t = '1DAY' group by x
                            );