-- CREATE TABLESPACE app_data
--     DATAFILE '/opt/app/oracle/oradata/orcl/app_data01.dbf'
--     SIZE 200M
--     AUTOEXTEND ON NEXT 50M MAXSIZE UNLIMITED;
SELECT con_id, name, open_mode FROM v$pdbs;
--查看表
SELECT COUNT(*) FROM user_tables;
--创建数据库
CREATE DATABASE TEST
    USER SYS IDENTIFIED BY SYSPWD
    USER SYSTEM IDENTIFIED BY SYSTEMPWD
    CONTROLFILE REUSE
    MAXINSTANCES 1
    MAXLOGFILES 5
    MAXLOGMENBERS 5
MAXLOGHISTOYR 1
MAXDATAFILES 100
LOGFILE GROUP 1 (‘/app/oracle/testdb/redo01.log’) SIZE 10M,
    GROUP 2(‘/app/oracle/testdb/redo01.log’) SIZE 10M,
DATAFILE ‘/ Aapp/oracle/testdb/system01.dbf’ SIZE 100M REUSE
EXTENT MANAGEMENT LOCAL
DEFAULT TABLESPACE tbs1
DEFAULT TEMPORARY TABLESPACE tempts1
TEMPFILE ‘temp1.dbf’ SIZE 10M REUSE
CHARACTER SET US7ASCII;



--   切换数据库
ALTER SESSION SET CURRENT_SCHEMA = SYSTEM;
--创建
CREATE PLUGGABLE DATABASE orclpdb FROM PDB$SEED;


CREATE TABLE student_scores (
                                student_id NUMBER,
                                subject VARCHAR2(20),
                                score NUMBER
);

INSERT INTO student_scores VALUES (1, '数学', 90);
INSERT INTO student_scores VALUES (1, '语文', 85);
INSERT INTO student_scores VALUES (1, '英语', 92);
INSERT INTO student_scores VALUES (2, '数学', 78);
INSERT INTO student_scores VALUES (2, '语文', 88);
INSERT INTO student_scores VALUES (2, '英语', 80);
COMMIT;
/**
列转行（Unpivot）
*/

CREATE PLUGGABLE DATABASE target_pdb
    FROM source_pdb
    COPY FILES;
SELECT
    student_id,
    MAX(CASE WHEN subject = '数学' THEN score END) AS "数学",
    MAX(CASE WHEN subject = '语文' THEN score END) AS "语文",
    MAX(CASE WHEN subject = '英语' THEN score END) AS "英语"
FROM student_scores
GROUP BY student_id;

CREATE TABLE student_scores_pivot (
                                      student_id NUMBER,
                                      "数学" NUMBER,
                                      "语文" NUMBER,
                                      "英语" NUMBER
);

INSERT INTO student_scores_pivot VALUES (1, 90, 85, 92);
INSERT INTO student_scores_pivot VALUES (2, 78, 88, 80);
COMMIT;

/**
标准 SQL（UNION ALL）
列转行（Unpivot）
*/
SELECT
    student_id,
    '数学' AS subject,
    "数学" AS score
FROM student_scores_pivot
UNION ALL
SELECT
    student_id,
    '语文' AS subject,
    "语文" AS score
FROM student_scores_pivot
UNION ALL
SELECT
    student_id,
    '英语' AS subject,
    "英语" AS score
FROM student_scores_pivot;
/*
Oracle UNPIVOT 语法（11g+）
*/

SELECT *
FROM student_scores_pivot
         UNPIVOT (
                  score FOR subject IN ("数学", "语文", "英语")
        );
