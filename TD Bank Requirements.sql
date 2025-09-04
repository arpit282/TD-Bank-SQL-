--- Requirement 1
-- a) Feed-1 (10 columns, 10 rows)
DROP TABLE IF EXISTS feed1;
CREATE TABLE feed1 (
    id SERIAL PRIMARY KEY,
    col1 INT,
    col2 TEXT,
    col3 NUMERIC(10,2),
    col4 DATE,
    col5 BOOLEAN,
    col6 TEXT,
    col7 INT,
    col8 NUMERIC(8,3),
    col9 TIMESTAMP,
    col10 TEXT
);

INSERT INTO feed1 (col1, col2, col3, col4, col5, col6, col7, col8, col9, col10)
SELECT
    (random()*100)::INT,
    md5(random()::text),
    round(random()*1000, 2),
    CURRENT_DATE - ((random()*365)::INT),
    (random() > 0.5),
    substr(md5(random()::text), 1, 5),
    (random()*50)::INT,
    round(random()*500, 3),
    NOW() - ((random()*100000)::INT || ' seconds')::interval,
    substr(md5(random()::text), 1, 8)
FROM generate_series(1, 10);


-- b) Feed-2 (15 columns, 15 rows)
DROP TABLE IF EXISTS feed2;
CREATE TABLE feed2 (
    id SERIAL PRIMARY KEY,
    col1 INT,
    col2 TEXT,
    col3 NUMERIC(10,2),
    col4 DATE,
    col5 BOOLEAN,
    col6 TEXT,
    col7 INT,
    col8 NUMERIC(8,3),
    col9 TIMESTAMP,
    col10 TEXT,
    col11 INT,
    col12 NUMERIC(12,4),
    col13 BOOLEAN,
    col14 DATE,
    col15 TEXT
);

INSERT INTO feed2 (col1, col2, col3, col4, col5, col6, col7, col8, col9, col10,
                   col11, col12, col13, col14, col15)
SELECT
    (random()*100)::INT,
    md5(random()::text),
    round(random()*1000, 2),
    CURRENT_DATE - ((random()*365)::INT),
    (random() > 0.5),
    substr(md5(random()::text), 1, 5),
    (random()*50)::INT,
    round(random()*500, 3),
    NOW() - ((random()*100000)::INT || ' seconds')::interval,
    substr(md5(random()::text), 1, 8),
    (random()*200)::INT,
    round(random()*9999, 4),
    (random() > 0.5),
    CURRENT_DATE - ((random()*1000)::INT),
    substr(md5(random()::text), 1, 12)
FROM generate_series(1, 15);


-- c) Feed-3 (20 columns, 20 rows)
DROP TABLE IF EXISTS feed3;
CREATE TABLE feed3 (
    id SERIAL PRIMARY KEY,
    col1 INT,
    col2 TEXT,
    col3 NUMERIC(10,2),
    col4 DATE,
    col5 BOOLEAN,
    col6 TEXT,
    col7 INT,
    col8 NUMERIC(8,3),
    col9 TIMESTAMP,
    col10 TEXT,
    col11 INT,
    col12 NUMERIC(12,4),
    col13 BOOLEAN,
    col14 DATE,
    col15 TEXT,
    col16 INT,
    col17 NUMERIC(10,3),
    col18 TEXT,
    col19 BOOLEAN,
    col20 DATE
);

INSERT INTO feed3 (col1, col2, col3, col4, col5, col6, col7, col8, col9, col10,
                   col11, col12, col13, col14, col15, col16, col17, col18, col19, col20)
SELECT
    (random()*100)::INT,
    md5(random()::text),
    round(random()*1000, 2),
    CURRENT_DATE - ((random()*365)::INT),
    (random() > 0.5),
    substr(md5(random()::text), 1, 5),
    (random()*50)::INT,
    round(random()*500, 3),
    NOW() - ((random()*100000)::INT || ' seconds')::interval,
    substr(md5(random()::text), 1, 8),
    (random()*200)::INT,
    round(random()*9999, 4),
    (random() > 0.5),
    CURRENT_DATE - ((random()*1000)::INT),
    substr(md5(random()::text), 1, 12),
    (random()*500)::INT,
    round(random()*12345, 3),
    substr(md5(random()::text), 1, 15),
    (random() > 0.5),
    CURRENT_DATE + ((random()*500)::INT)
FROM generate_series(1, 20); 
--- Requirement 2:
DROP FUNCTION IF EXISTS generate_feed(feed_name text, num_rows int, num_cols int);
CREATE OR REPLACE FUNCTION generate_feed(feed_name text, num_rows int, num_cols int)
RETURNS void AS
$$
DECLARE
    col_defs TEXT := '';
    insert_cols TEXT := '';
    i INT;
    dyn_sql TEXT;
BEGIN
    -- Build dynamic CREATE TABLE statement
    col_defs := 'id SERIAL PRIMARY KEY';
    FOR i IN 1..num_cols LOOP
        col_defs := col_defs || ', col' || i || ' TEXT';
        insert_cols := insert_cols || 'col' || i;
        IF i < num_cols THEN
            insert_cols := insert_cols || ', ';
        END IF;
    END LOOP;

    dyn_sql := 'DROP TABLE IF EXISTS ' || feed_name || '; ' ||
               'CREATE TABLE ' || feed_name || ' (' || col_defs || ');';
    EXECUTE dyn_sql;

    -- Populate with random data
    dyn_sql := 'INSERT INTO ' || feed_name || '(' || insert_cols || ') ' ||
               'SELECT ';

    FOR i IN 1..num_cols LOOP
        dyn_sql := dyn_sql || 'substr(md5(random()::text),1,8)';
        IF i < num_cols THEN
            dyn_sql := dyn_sql || ', ';
        END IF;
    END LOOP;

    dyn_sql := dyn_sql || ' FROM generate_series(1,' || num_rows || ');';

    EXECUTE dyn_sql;

END;
$$ LANGUAGE plpgsql;

-- Feed-1 with 10 cols × 10 rows
SELECT generate_feed('feed1', 10, 10);

-- Feed-2 with 15 cols × 15 rows
SELECT generate_feed('feed2', 15, 15);

-- Feed-3 with 20 cols × 20 rows
SELECT generate_feed('feed3', 20, 20);


---- Requirement 3
-- Feed-1
SELECT col1, col2, col3, col4, col5, col6, col7, col8, col9, col10,
       COUNT(*) AS dup_count
FROM feed1
GROUP BY col1, col2, col3, col4, col5, col6, col7, col8, col9, col10
HAVING COUNT(*) > 1;


-- Feed-2
SELECT col1, col2, col3, col4, col5, col6, col7, col8, col9, col10,
       col11, col12, col13, col14, col15,
       COUNT(*) AS dup_count
FROM feed2
GROUP BY col1, col2, col3, col4, col5, col6, col7, col8, col9, col10,
         col11, col12, col13, col14, col15
HAVING COUNT(*) > 1;


-- Feed-3
SELECT col1, col2, col3, col4, col5, col6, col7, col8, col9, col10,
       col11, col12, col13, col14, col15, col16, col17, col18, col19, col20,
       COUNT(*) AS dup_count
FROM feed3
GROUP BY col1, col2, col3, col4, col5, col6, col7, col8, col9, col10,
         col11, col12, col13, col14, col15, col16, col17, col18, col19, col20
HAVING COUNT(*) > 1;


--- Requirement 4
-- duplicates from feed1
COPY (
    SELECT col1, col2, col3, col4, col5, col6, col7, col8, col9, col10,
           COUNT(*) AS dup_count
    FROM feed1
    GROUP BY col1, col2, col3, col4, col5, col6, col7, col8, col9, col10
    HAVING COUNT(*) > 1
) TO '/tmp/duplicates_feed1.csv' WITH CSV HEADER;


-- duplicates from feed2
COPY (
    SELECT col1, col2, col3, col4, col5, col6, col7, col8, col9, col10,
           col11, col12, col13, col14, col15,
           COUNT(*) AS dup_count
    FROM feed2
    GROUP BY col1, col2, col3, col4, col5, col6, col7, col8, col9, col10,
             col11, col12, col13, col14, col15
    HAVING COUNT(*) > 1
) TO '/tmp/duplicates_feed2.csv' WITH CSV HEADER;


-- duplicates from feed3
COPY (
    SELECT col1, col2, col3, col4, col5, col6, col7, col8, col9, col10,
           col11, col12, col13, col14, col15, col16, col17, col18, col19, col20,
           COUNT(*) AS dup_count
    FROM feed3
    GROUP BY col1, col2, col3, col4, col5, col6, col7, col8, col9, col10,
             col11, col12, col13, col14, col15, col16, col17, col18, col19, col20
    HAVING COUNT(*) > 1
) TO '/tmp/duplicates_feed3.csv' WITH CSV HEADER;

--- Requirement 5
-- Removing duplicates from Feed-1
DELETE FROM feed1 f
USING (
    SELECT id
    FROM (
        SELECT id,
               ROW_NUMBER() OVER (PARTITION BY col1, col2, col3, col4, col5,
                                               col6, col7, col8, col9, col10
                                  ORDER BY id) AS rn
        FROM feed1
    ) t
    WHERE t.rn > 1
) d
WHERE f.id = d.id;

-- Remove duplicates from Feed-2
DELETE FROM feed2 f
USING (
    SELECT id
    FROM (
        SELECT id,
               ROW_NUMBER() OVER (PARTITION BY col1, col2, col3, col4, col5,
                                               col6, col7, col8, col9, col10,
                                               col11, col12, col13, col14, col15
                                  ORDER BY id) AS rn
        FROM feed2
    ) t
    WHERE t.rn > 1
) d
WHERE f.id = d.id;


-- Remove duplicates from Feed-3
DELETE FROM feed3 f
USING (
    SELECT id
    FROM (
        SELECT id,
               ROW_NUMBER() OVER (PARTITION BY col1, col2, col3, col4, col5,
                                               col6, col7, col8, col9, col10,
                                               col11, col12, col13, col14, col15,
                                               col16, col17, col18, col19, col20
                                  ORDER BY id) AS rn
        FROM feed3
    ) t
    WHERE t.rn > 1
) d
WHERE f.id = d.id;

--- Requirement 6
-- Checking duplicates in Feed-1
SELECT COUNT(*) AS dup_count
FROM (
    SELECT col1, col2, col3, col4, col5, col6, col7, col8, col9, col10
    FROM feed1
    GROUP BY col1, col2, col3, col4, col5, col6, col7, col8, col9, col10
    HAVING COUNT(*) > 1
) t;


-- Checking duplicates in Feed-2
SELECT COUNT(*) AS dup_count
FROM (
    SELECT col1, col2, col3, col4, col5, col6, col7, col8, col9, col10,
           col11, col12, col13, col14, col15
    FROM feed2
    GROUP BY col1, col2, col3, col4, col5, col6, col7, col8, col9, col10,
             col11, col12, col13, col14, col15
    HAVING COUNT(*) > 1
) t;


-- Checking duplicates in Feed-3
SELECT COUNT(*) AS dup_count
FROM (
    SELECT col1, col2, col3, col4, col5, col6, col7, col8, col9, col10,
           col11, col12, col13, col14, col15, col16, col17, col18, col19, col20
    FROM feed3
    GROUP BY col1, col2, col3, col4, col5, col6, col7, col8, col9, col10,
             col11, col12, col13, col14, col15, col16, col17, col18, col19, col20
    HAVING COUNT(*) > 1
) t;


--- Requirement 7
-- Rows in Feed-2 but not in Feed-1
SELECT 'Feed2_ONLY' AS source, f2.*
FROM feed2 f2
LEFT JOIN feed1 f1
ON f2.col1 = f1.col1  
AND f2.col2 = f1.col2
AND f2.col3 = f1.col3
WHERE f1.col1 IS NULL;

-- Rows in Feed-1 but not in Feed-2
SELECT 'Feed1_ONLY' AS source, f1.*
FROM feed1 f1
LEFT JOIN feed2 f2
ON f1.col1 = f2.col1
AND f1.col2 = f2.col2
AND f1.col3 = f2.col3
WHERE f2.col1 IS NULL;

-- Rows in Feed-3 but not in Feed-1
SELECT 'Feed3_ONLY' AS source, f3.*
FROM feed3 f3
LEFT JOIN feed1 f1
ON f3.col1 = f1.col1
AND f3.col2 = f1.col2
AND f3.col3 = f1.col3
WHERE f1.col1 IS NULL;

-- Rows in Feed-1 but not in Feed-3
SELECT 'Feed1_ONLY' AS source, f1.*
FROM feed1 f1
LEFT JOIN feed3 f3
ON f1.col1 = f3.col1
AND f1.col2 = f3.col2
AND f1.col3 = f3.col3
-- add as many as needed
WHERE f3.col1 IS NULL;

-- Feed-2 vs Feed-1 and exporting differences
COPY (
    SELECT 'Feed2_ONLY' AS source, f2.*
    FROM feed2 f2
    LEFT JOIN feed1 f1
    ON f2.col1 = f1.col1 AND f2.col2 = f1.col2
    WHERE f1.col1 IS NULL
    UNION ALL
    SELECT 'Feed1_ONLY' AS source, f1.*
    FROM feed1 f1
    LEFT JOIN feed2 f2
    ON f1.col1 = f2.col1 AND f1.col2 = f2.col2
    WHERE f2.col1 IS NULL
) TO '/tmp/feed2_vs_feed1_diff.csv' CSV HEADER;


-- Feed-3 vs Feed-1 and exporting differences
COPY (
    SELECT 'Feed3_ONLY' AS source, f3.*
    FROM feed3 f3
    LEFT JOIN feed1 f1
    ON f3.col1 = f1.col1 AND f3.col2 = f1.col2
    WHERE f1.col1 IS NULL
    UNION ALL
    SELECT 'Feed1_ONLY' AS source, f1.*
    FROM feed1 f1
    LEFT JOIN feed3 f3
    ON f1.col1 = f3.col1 AND f1.col2 = f3.col2
    WHERE f3.col1 IS NULL
) TO '/tmp/feed3_vs_feed1_diff.csv' CSV HEADER;

Requirement 9:

CREATE TABLE IF NOT EXISTS test_runs (
  run_id        BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
  started_at    TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  finished_at   TIMESTAMP NULL,
  overall_status VARCHAR(10) DEFAULT 'PENDING', 
  notes         TEXT
);

CREATE TABLE IF NOT EXISTS test_results (
  id           BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
  run_id       BIGINT UNSIGNED NOT NULL,
  case_id      VARCHAR(20) NOT NULL,
  step_no      INT NOT NULL,
  test_name    VARCHAR(255) NOT NULL,
  expected     TEXT,
  actual       TEXT,
  status       VARCHAR(10) NOT NULL,
  details      TEXT,
  created_at   TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  KEY (run_id),
  CONSTRAINT fk_results_run FOREIGN KEY (run_id) REFERENCES test_runs(run_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS duplicates_log (
  id            BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
  run_id        BIGINT UNSIGNED NOT NULL,
  table_name    VARCHAR(255) NOT NULL,
  group_hash    VARCHAR(64) NOT NULL,
  group_size    INT NOT NULL,
  sample        JSON,
  created_at    TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  KEY (run_id)
);

CREATE TABLE IF NOT EXISTS compare_results (
  id            BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
  run_id        BIGINT UNSIGNED NOT NULL,
  left_table    VARCHAR(255) NOT NULL,
  right_table   VARCHAR(255) NOT NULL,
  side          VARCHAR(12) NOT NULL,
  row_hash      VARCHAR(64) NOT NULL,
  row_json      JSON,
  created_at    TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  KEY (run_id)
);

DELIMITER $$



CREATE OR REPLACE PROCEDURE sp_log_result(
  IN p_run_id BIGINT UNSIGNED,
  IN p_case   VARCHAR(20),
  IN p_step   INT,
  IN p_name   VARCHAR(255),
  IN p_exp    TEXT,
  IN p_act    TEXT,
  IN p_pass   BOOLEAN,
  IN p_details TEXT
)
BEGIN
  INSERT INTO test_results(run_id, case_id, step_no, test_name, expected, actual, status, details)
  VALUES(p_run_id, p_case, p_step, p_name, p_exp, p_act, IF(p_pass,'PASS','FAIL'), p_details);
END$$

CREATE OR REPLACE PROCEDURE sp_generate_feed(
  IN p_table    VARCHAR(255),
  IN p_num_cols INT,
  IN p_num_rows INT,
  IN p_dup_rows INT
)
BEGIN
  DECLARE i INT DEFAULT 1;
  DECLARE col_defs TEXT DEFAULT 'row_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY';
  DECLARE col_list TEXT DEFAULT '';
  DECLARE expr_list TEXT DEFAULT '';
  DECLARE col_name VARCHAR(255);
  DECLARE typ INT;

  SET @sql := CONCAT('DROP TABLE IF EXISTS `', p_table, '`');
  PREPARE s FROM @sql; EXECUTE s; DEALLOCATE PREPARE s;

  WHILE i <= p_num_cols DO
    SET typ = ((i - 1) % 5) + 1;
    CASE typ
      WHEN 1 THEN SET col_name = CONCAT('c', i, '_text');   SET col_defs = CONCAT(col_defs, ', `', col_name, '` VARCHAR(50)');      SET expr_list = CONCAT(expr_list, "CONCAT('T', LPAD(FLOOR(RAND()*999999),6,'0'))");
      WHEN 2 THEN SET col_name = CONCAT('c', i, '_int');    SET col_defs = CONCAT(col_defs, ', `', col_name, '` INT');              SET expr_list = CONCAT(expr_list, "FLOOR(RAND()*1000)");
      WHEN 3 THEN SET col_name = CONCAT('c', i, '_date');   SET col_defs = CONCAT(col_defs, ', `', col_name, '` DATE');             SET expr_list = CONCAT(expr_list, "DATE_ADD('2020-01-01', INTERVAL FLOOR(RAND()*1825) DAY)");
      WHEN 4 THEN SET col_name = CONCAT('c', i, '_num');    SET col_defs = CONCAT(col_defs, ', `', col_name, '` DECIMAL(10,2)');    SET expr_list = CONCAT(expr_list, "ROUND(RAND()*10000,2)");
      WHEN 5 THEN SET col_name = CONCAT('c', i, '_bool');   SET col_defs = CONCAT(col_defs, ', `', col_name, '` TINYINT(1)');       SET expr_list = CONCAT(expr_list, "FLOOR(RAND()*2)");
    END CASE;

    SET col_list = CONCAT(col_list, '`', col_name, '`');
    IF i < p_num_cols THEN
      SET col_list = CONCAT(col_list, ', ');
      SET expr_list = CONCAT(expr_list, ', ');
    END IF;
    SET i = i + 1;
  END WHILE;

  
  SET @sql = CONCAT('CREATE TABLE `', p_table, '` (', col_defs, ') ENGINE=InnoDB');
  PREPARE s FROM @sql; EXECUTE s; DEALLOCATE PREPARE s;

  
SET @sql = CONCAT(
    'WITH RECURSIVE seq(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM seq WHERE n < ', p_num_rows, ') ',
    'INSERT INTO `', p_table, '` (', col_list, ') ',
    'SELECT ', expr_list, ' FROM seq'
  );
  PREPARE s FROM @sql; EXECUTE s; DEALLOCATE PREPARE s;

  
  IF p_dup_rows > 0 THEN
    SET @sql = CONCAT(
      'INSERT INTO `', p_table, '` (', col_list, ') ',
      'SELECT ', col_list, ' FROM `', p_table, '` ORDER BY RAND() LIMIT ', p_dup_rows
    );
    PREPARE s FROM @sql; EXECUTE s; DEALLOCATE PREPARE s;
  END IF;
END$$

CREATE OR REPLACE PROCEDURE sp_find_duplicates(
  IN p_run_id BIGINT UNSIGNED,
  IN p_table  VARCHAR(255)
)
BEGIN
  DECLARE v_concat_expr TEXT;
  DECLARE v_json_pairs  TEXT;

  
  SELECT GROUP_CONCAT(CONCAT('COALESCE(CAST(`', COLUMN_NAME, '` AS CHAR),''<NULL>'')') 
                      ORDER BY ORDINAL_POSITION SEPARATOR ', ')
    INTO v_concat_expr
  FROM information_schema.columns
  WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = p_table AND COLUMN_NAME <> 'row_id';

  SELECT GROUP_CONCAT(CONCAT("'", COLUMN_NAME, "', MIN(`", COLUMN_NAME, '`)')
                      ORDER BY ORDINAL_POSITION SEPARATOR ', ')
    INTO v_json_pairs
  FROM information_schema.columns
  WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = p_table AND COLUMN_NAME <> 'row_id';

  SET @dup_sql = CONCAT(
    'INSERT INTO duplicates_log(run_id, table_name, group_hash, group_size, sample) ',
    'SELECT ', p_run_id, ', ''', p_table, ''', ',
    'MD5(CONCAT_WS(''||'', ', v_concat_expr, ')) AS grp_hash, ',
    'COUNT(*) AS grp_sz, ',
    'JSON_OBJECT(', v_json_pairs, ') AS sample ',
    'FROM `', p_table, '` ',
    'GROUP BY MD5(CONCAT_WS(''||'', ', v_concat_expr, ')) ',
    'HAVING COUNT(*) > 1'
  );
  PREPARE s FROM @dup_sql; EXECUTE s; DEALLOCATE PREPARE s;
END$$

CREATE OR REPLACE PROCEDURE sp_dedup_table(
  IN p_table VARCHAR(255)
)
BEGIN
  DECLARE v_part_cols TEXT;
  SELECT GROUP_CONCAT(CONCAT('`', COLUMN_NAME, '`') ORDER BY ORDINAL_POSITION SEPARATOR ', ')
    INTO v_part_cols
  FROM information_schema.columns
  WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = p_table AND COLUMN_NAME <> 'row_id';

  SET @sql = CONCAT(
    'DELETE t FROM `', p_table, '` t ',
    'JOIN (',
      'SELECT row_id, ROW_NUMBER() OVER(PARTITION BY ', v_part_cols, ' ORDER BY row_id) rn ',
      'FROM `', p_table, '`',
    ') x ON t.row_id = x.row_id ',
    'WHERE x.rn > 1'
  );
  PREPARE s FROM @sql; EXECUTE s; DEALLOCATE PREPARE s;
END$$

CREATE OR REPLACE PROCEDURE sp_count_dup_groups(
  IN  p_table VARCHAR(255),
  OUT p_groups INT
)
BEGIN
  DECLARE v_concat_expr TEXT;
  SET p_groups = 0;

  SELECT GROUP_CONCAT(CONCAT('COALESCE(CAST(`', COLUMN_NAME, '` AS CHAR),''<NULL>'')')
                      ORDER BY ORDINAL_POSITION SEPARATOR ', ')
    INTO v_concat_expr
  FROM information_schema.columns
  WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = p_table AND COLUMN_NAME <> 'row_id';

  SET @g := NULL;
  SET @sql = CONCAT(
    'SELECT COUNT(*) INTO @g FROM (',
      'SELECT MD5(CONCAT_WS(''||'', ', v_concat_expr, ')) h ',
      'FROM `', p_table, '` ',
      'GROUP BY MD5(CONCAT_WS(''||'', ', v_concat_expr, ')) ',
      'HAVING COUNT(*) > 1',
    ') z'
  );
  PREPARE s FROM @sql; EXECUTE s; DEALLOCATE PREPARE s;
  SET p_groups = IFNULL(@g,0);
END$$

CREATE OR REPLACE PROCEDURE sp_compare_tables(
  IN p_run_id BIGINT UNSIGNED,
  IN p_left   VARCHAR(255),
  IN p_right  VARCHAR(255)
)
BEGIN
  DECLARE v_concat_l TEXT; DECLARE v_concat_r TEXT;
  DECLARE v_json_l   TEXT; DECLARE v_json_r   TEXT;

  
  SELECT GROUP_CONCAT(CONCAT('COALESCE(CAST(a.`', a.COLUMN_NAME, '` AS CHAR),''<NULL>'')')
                      ORDER BY a.ORDINAL_POSITION SEPARATOR ', ')
    INTO v_concat_l
  FROM information_schema.columns a
  JOIN information_schema.columns b
    ON a.TABLE_SCHEMA=DATABASE() AND b.TABLE_SCHEMA=DATABASE()
   AND a.TABLE_NAME=p_left AND b.TABLE_NAME=p_right
   AND a.COLUMN_NAME=b.COLUMN_NAME
  WHERE a.COLUMN_NAME <> 'row_id';

  SELECT GROUP_CONCAT(CONCAT('COALESCE(CAST(b.`', b.COLUMN_NAME, '` AS CHAR),''<NULL>'')')
                      ORDER BY b.ORDINAL_POSITION SEPARATOR ', ')
    INTO v_concat_r
  FROM information_schema.columns a
  JOIN information_schema.columns b
    ON a.TABLE_SCHEMA=DATABASE() AND b.TABLE_SCHEMA=DATABASE()
   AND a.TABLE_NAME=p_left AND b.TABLE_NAME=p_right
   AND a.COLUMN_NAME=b.COLUMN_NAME
  WHERE b.COLUMN_NAME <> 'row_id';

  SELECT GROUP_CONCAT(CONCAT("'", a.COLUMN_NAME, "', a.`", a.COLUMN_NAME, '`')
                      ORDER BY a.ORDINAL_POSITION SEPARATOR ', ')
    INTO v_json_l
  FROM information_schema.columns a
  JOIN information_schema.columns b
    ON a.TABLE_SCHEMA=DATABASE() AND b.TABLE_SCHEMA=DATABASE()
   AND a.TABLE_NAME=p_left AND b.TABLE_NAME=p_right
   AND a.COLUMN_NAME=b.COLUMN_NAME
  WHERE a.COLUMN_NAME <> 'row_id';

  SELECT GROUP_CONCAT(CONCAT("'", b.COLUMN_NAME, "', b.`", b.COLUMN_NAME, '`')
                      ORDER BY b.ORDINAL_POSITION SEPARATOR ', ')
    INTO v_json_r
  FROM information_schema.columns a
  JOIN information_schema.columns b
    ON a.TABLE_SCHEMA=DATABASE() AND b.TABLE_SCHEMA=DATABASE()
   AND a.TABLE_NAME=p_left AND b.TABLE_NAME=p_right
   AND a.COLUMN_NAME=b.COLUMN_NAME
  WHERE b.COLUMN_NAME <> 'row_id';

 
  IF v_concat_l IS NULL OR v_concat_r IS NULL THEN
    LEAVE proc;
  END IF;

  
  SET @sql = CONCAT(
    'CREATE TEMPORARY TABLE tmp_l AS ',
    'SELECT MD5(CONCAT_WS(''||'', ', v_concat_l, ')) AS h, JSON_OBJECT(', v_json_l, ') AS js FROM `', p_left, '` a'
  );
  PREPARE s FROM @sql; EXECUTE s; DEALLOCATE PREPARE s;

  SET @sql = CONCAT(
    'CREATE TEMPORARY TABLE tmp_r AS ',
    'SELECT MD5(CONCAT_WS(''||'', ', v_concat_r, ')) AS h, JSON_OBJECT(', v_json_r, ') AS js FROM `', p_right, '` b'
  );
  PREPARE s FROM @sql; EXECUTE s; DEALLOCATE PREPARE s;

  
  INSERT INTO compare_results(run_id, left_table, right_table, side, row_hash, row_json)
  SELECT p_run_id, p_left, p_right, 'LEFT_ONLY', l.h, l.js
  FROM tmp_l l LEFT JOIN tmp_r r ON l.h=r.h
  WHERE r.h IS NULL;

  
  INSERT INTO compare_results(run_id, left_table, right_table, side, row_hash, row_json)
  SELECT p_run_id, p_left, p_right, 'RIGHT_ONLY', r.h, r.js
  FROM tmp_r r LEFT JOIN tmp_l l ON l.h=r.h
  WHERE l.h IS NULL;

  DROP TEMPORARY TABLE IF EXISTS tmp_l;
  DROP TEMPORARY TABLE IF EXISTS tmp_r;

  proc: END;
END$$

CREATE OR REPLACE PROCEDURE sp_run_suite(
  IN p_emit_csv BOOLEAN,
  IN p_csv_dir  VARCHAR(500)
)
BEGIN
  DECLARE v_run BIGINT UNSIGNED;
  DECLARE v_cnt INT; DECLARE v_ok BOOLEAN;
  DECLARE v_groups INT;

  INSERT INTO test_runs() VALUES(); 
  SET v_run = LAST_INSERT_ID();

  
  CALL sp_generate_feed('Feed1', 10, 30, 3);
  CALL sp_generate_feed('Feed2', 15, 40, 5);
  CALL sp_generate_feed('Feed3', 20, 50, 7);

  -- Row count checks
  SELECT COUNT(*) INTO v_cnt FROM Feed1;
  SET v_ok = (v_cnt = 33); 
  CALL sp_log_result(v_run,'TC01',1,'Feed1 rows','33', CONCAT(v_cnt), v_ok, NULL);

  SELECT COUNT(*) INTO v_cnt FROM Feed2;
  SET v_ok = (v_cnt = 45); 
  CALL sp_log_result(v_run,'TC02',1,'Feed2 rows','45', CONCAT(v_cnt), v_ok, NULL);

  SELECT COUNT(*) INTO v_cnt FROM Feed3;
  SET v_ok = (v_cnt = 57); 
  CALL sp_log_result(v_run,'TC03',1,'Feed3 rows','57', CONCAT(v_cnt), v_ok, NULL);

  CALL sp_find_duplicates(v_run, 'Feed1');
  CALL sp_find_duplicates(v_run, 'Feed2');
  CALL sp_find_duplicates(v_run, 'Feed3');

  CALL sp_count_dup_groups('Feed1', v_groups);
  CALL sp_log_result(v_run,'TC04',2,'Dup groups Feed1','> 0', CONCAT(v_groups), (v_groups > 0), NULL);

  CALL sp_count_dup_groups('Feed2', v_groups);
  CALL sp_log_result(v_run,'TC05',2,'Dup groups Feed2','> 0', CONCAT(v_groups), (v_groups > 0), NULL);

  CALL sp_count_dup_groups('Feed3', v_groups);
  CALL sp_log_result(v_run,'TC06',2,'Dup groups Feed3','> 0', CONCAT(v_groups), (v_groups > 0), NULL);

  CALL sp_dedup_table('Feed1');
  CALL sp_dedup_table('Feed2');
  CALL sp_dedup_table('Feed3');

  CALL sp_count_dup_groups('Feed1', v_groups);
  CALL sp_log_result(v_run,'TC07',3,'Verify no dups Feed1','0', CONCAT(v_groups), (v_groups = 0), NULL);

  CALL sp_count_dup_groups('Feed2', v_groups);
  CALL sp_log_result(v_run,'TC08',3,'Verify no dups Feed2','0', CONCAT(v_groups), (v_groups = 0), NULL);

  CALL sp_count_dup_groups('Feed3', v_groups);
  CALL sp_log_result(v_run,'TC09',3,'Verify no dups Feed3','0', CONCAT(v_groups), (v_groups = 0), NULL);

  DELETE FROM compare_results WHERE run_id = v_run;
  CALL sp_compare_tables(v_run, 'Feed2', 'Feed1');
  CALL sp_compare_tables(v_run, 'Feed3', 'Feed1');

  SELECT COUNT(*) INTO v_cnt FROM compare_results WHERE run_id = v_run AND left_table='Feed2' AND right_table='Feed1';
  CALL sp_log_result(v_run,'TC10',4,'Compare Feed2 vs Feed1','> 0 diffs', CONCAT(v_cnt), (v_cnt > 0), NULL);

  SELECT COUNT(*) INTO v_cnt FROM compare_results WHERE run_id = v_run AND left_table='Feed3' AND right_table='Feed1';
  CALL sp_log_result(v_run,'TC11',4,'Compare Feed3 vs Feed1','> 0 diffs', CONCAT(v_cnt), (v_cnt > 0), NULL);


