
-- Requirement 1: Table Creation & Data Insertion

-- Feed1: 10 columns, 10 rows
CREATE TABLE Feed1 (
  id SERIAL PRIMARY KEY,
  col1 TEXT, col2 TEXT, col3 TEXT, col4 TEXT, col5 TEXT,
  col6 TEXT, col7 TEXT, col8 TEXT, col9 TEXT, col10 TEXT
);

INSERT INTO Feed1 (col1, col2, col3, col4, col5, col6, col7, col8, col9, col10)
SELECT CAST((random()*100)::INT AS TEXT),
       CAST((random()*100)::INT AS TEXT),
       CAST((random()*100)::INT AS TEXT),
       CAST((random()*100)::INT AS TEXT),
       CAST((random()*100)::INT AS TEXT),
       CAST((random()*100)::INT AS TEXT),
       CAST((random()*100)::INT AS TEXT),
       CAST((random()*100)::INT AS TEXT),
       CAST((random()*100)::INT AS TEXT),
       CAST((random()*100)::INT AS TEXT)
FROM generate_series(1,10);

-- Feed2: 15 columns, 15 rows
CREATE TABLE Feed2 (
  id SERIAL PRIMARY KEY,
  col1 TEXT, col2 TEXT, col3 TEXT, col4 TEXT, col5 TEXT,
  col6 TEXT, col7 TEXT, col8 TEXT, col9 TEXT, col10 TEXT,
  col11 TEXT, col12 TEXT, col13 TEXT, col14 TEXT, col15 TEXT
);

INSERT INTO Feed2 (col1, col2, col3, col4, col5, col6, col7, col8, col9, col10,
                   col11, col12, col13, col14, col15)
SELECT CAST((random()*100)::INT AS TEXT),
       CAST((random()*100)::INT AS TEXT),
       CAST((random()*100)::INT AS TEXT),
       CAST((random()*100)::INT AS TEXT),
       CAST((random()*100)::INT AS TEXT),
       CAST((random()*100)::INT AS TEXT),
       CAST((random()*100)::INT AS TEXT),
       CAST((random()*100)::INT AS TEXT),
       CAST((random()*100)::INT AS TEXT),
       CAST((random()*100)::INT AS TEXT),
       CAST((random()*100)::INT AS TEXT),
       CAST((random()*100)::INT AS TEXT),
       CAST((random()*100)::INT AS TEXT),
       CAST((random()*100)::INT AS TEXT),
       CAST((random()*100)::INT AS TEXT)
FROM generate_series(1,15);

-- Feed3: 20 columns, 20 rows
CREATE TABLE Feed3 (
  id SERIAL PRIMARY KEY,
  col1 TEXT, col2 TEXT, col3 TEXT, col4 TEXT, col5 TEXT,
  col6 TEXT, col7 TEXT, col8 TEXT, col9 TEXT, col10 TEXT,
  col11 TEXT, col12 TEXT, col13 TEXT, col14 TEXT, col15 TEXT,
  col16 TEXT, col17 TEXT, col18 TEXT, col19 TEXT, col20 TEXT
);

INSERT INTO Feed3 (col1, col2, col3, col4, col5, col6, col7, col8, col9, col10,
                   col11, col12, col13, col14, col15,
                   col16, col17, col18, col19, col20)
SELECT CAST((random()*100)::INT AS TEXT), CAST((random()*100)::INT AS TEXT),
       CAST((random()*100)::INT AS TEXT), CAST((random()*100)::INT AS TEXT),
       CAST((random()*100)::INT AS TEXT), CAST((random()*100)::INT AS TEXT),
       CAST((random()*100)::INT AS TEXT), CAST((random()*100)::INT AS TEXT),
       CAST((random()*100)::INT AS TEXT), CAST((random()*100)::INT AS TEXT),
       CAST((random()*100)::INT AS TEXT), CAST((random()*100)::INT AS TEXT),
       CAST((random()*100)::INT AS TEXT), CAST((random()*100)::INT AS TEXT),
       CAST((random()*100)::INT AS TEXT), CAST((random()*100)::INT AS TEXT),
       CAST((random()*100)::INT AS TEXT), CAST((random()*100)::INT AS TEXT),
       CAST((random()*100)::INT AS TEXT), CAST((random()*100)::INT AS TEXT)
FROM generate_series(1,20);

-- Requirement 2
CREATE OR REPLACE PROCEDURE generate_feed(feed_name TEXT, rows_no INT, cols_no INT)
LANGUAGE plpgsql AS $$
DECLARE
  i INT;
  create_sql TEXT;
  insert_sql TEXT;
BEGIN
  -- Drop table if exists
  EXECUTE format('DROP TABLE IF EXISTS %I', feed_name);

  create_sql := 'CREATE TABLE ' || feed_name || ' (id SERIAL PRIMARY KEY, ';
  FOR i IN 1..cols_no LOOP
    create_sql := create_sql || format('col%s TEXT', i);
    IF i < cols_no THEN
      create_sql := create_sql || ', ';
    END IF;
  END LOOP;
  create_sql := create_sql || ')';
  EXECUTE create_sql;

  insert_sql := 'INSERT INTO ' || feed_name || ' (';
  FOR i IN 1..cols_no LOOP
    insert_sql := insert_sql || format('col%s', i);
    IF i < cols_no THEN
      insert_sql := insert_sql || ', ';
    END IF;
  END LOOP;
  insert_sql := insert_sql || ') SELECT ';
  FOR i IN 1..cols_no LOOP
    insert_sql := insert_sql || 'CAST((random()*100)::INT AS TEXT)';
    IF i < cols_no THEN
      insert_sql := insert_sql || ', ';
    END IF;
  END LOOP;
  insert_sql := insert_sql || format(' FROM generate_series(1,%s)', rows_no);

  EXECUTE insert_sql;
END;
$$;

-- Procedure calls
CALL generate_feed('Feed1', 10, 10);
CALL generate_feed('Feed2', 15, 15);
CALL generate_feed('Feed3', 20, 20);

-- Requirement 3

SELECT * FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY col1,col2,col3,col4,col5,col6,col7,col8,col9,col10 ORDER BY id) rn
  FROM Feed1
) t
WHERE rn > 1;

-- Requirement 4
CREATE TABLE Feed1_Duplicates AS
SELECT * FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY col1,col2,col3,col4,col5,col6,col7,col8,col9,col10 ORDER BY id) rn
  FROM Feed1
) t
WHERE rn > 1;

-- Requirement 5 & 6: Remove Duplicates and Validate

WITH cte AS (
  SELECT id, ROW_NUMBER() OVER (PARTITION BY col1,col2,col3,col4,col5,col6,col7,col8,col9,col10 ORDER BY id) rn
  FROM Feed1
)
DELETE FROM Feed1 WHERE id IN (SELECT id FROM cte WHERE rn > 1);

-- Validation: ensure no duplicates remain (should return 0)
SELECT * FROM (
  SELECT *, COUNT(*) OVER (PARTITION BY col1,col2,col3,col4,col5,col6,col7,col8,col9,col10) cnt
  FROM Feed1
) t
WHERE cnt > 1;

-- Requirement 7

-- Compare Feed2 vs Feed1 (first 10 columns)
SELECT f2.*, f1.*
FROM Feed2 f2
JOIN Feed1 f1
  ON (f2.col1,f2.col2,f2.col3,f2.col4,f2.col5,f2.col6,f2.col7,f2.col8,f2.col9,f2.col10)
   = (f1.col1,f1.col2,f1.col3,f1.col4,f1.col5,f1.col6,f1.col7,f1.col8,f1.col9,f1.col10);

-- Compare Feed3 vs Feed1 (first 10 columns)
SELECT f3.*, f1.*
FROM Feed3 f3
JOIN Feed1 f1
  ON (f3.col1,f3.col2,f3.col3,f3.col4,f3.col5,f3.col6,f3.col7,f3.col8,f3.col9,f3.col10)
   = (f1.col1,f1.col2,f1.col3,f1.col4,f1.col5,f1.col6,f1.col7,f1.col8,f1.col9,f1.col10);

