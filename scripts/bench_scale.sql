-- bench_scale.sql - reproduce the RESULTS.md section 13 performance table
--   <CONSUMER_SCHEMA> <CONSUMER_WAREHOUSE>
--
-- COST WARNING. This builds a 10,000,000-row table and creates a MEDIUM warehouse
-- for the build. On a demo account that is small but it is not free. The final
-- statement drops everything it created, including the warehouse. Read it before
-- running it.
--
-- WHY THIS IS SEPARATE FROM sql/
-- sql/01 generates 200,000 rows, which is enough to prove CORRECTNESS but actively
-- misleading for PERFORMANCE: at 200k the ordering of the three methods is
-- different (DECFLOAT 139ms, limb SUM 155ms, UDAF 1,412ms) and the UDAF looks far
-- more competitive than it is. The gap only opens up with scale and with group
-- count, so this file exists to measure at a size where the answer is stable.
--
-- ===========================================================================
-- TWO MEASUREMENT TRAPS THIS FILE AVOIDS
-- ===========================================================================
--
-- 1. DO NOT benchmark with SELECT COUNT(*) FROM (SELECT ..., AGG(x) ... GROUP BY).
--    Snowflake prunes the unused aggregate, so the UDAF is never actually
--    evaluated and appears FASTER at 50x the rows. Force materialisation with
--    MAX() over the aggregate plus a checksum, as below. The checksum doubles as
--    an exactness proof, since the two methods must produce identical values.
--
-- 2. Comments are stripped from QUERY_HISTORY.QUERY_TEXT, so /* tags */ cannot be
--    used to classify runs. Use a literal string column ('MRKA') instead, and
--    note that underscores are LIKE wildcards -- prefer CONTAINS().

USE WAREHOUSE <CONSUMER_WAREHOUSE>;
USE SCHEMA <CONSUMER_SCHEMA>;

CREATE OR REPLACE WAREHOUSE U256_SCALE_WH
  WITH WAREHOUSE_SIZE='MEDIUM' AUTO_SUSPEND=60 INITIALLY_SUSPENDED=FALSE;

-- Scalar decode, used ONLY to generate test data. This is the provider-side
-- decode; a consumer never needs it.
CREATE OR REPLACE FUNCTION U256_DEC(B BINARY) RETURNS VARCHAR
LANGUAGE PYTHON RUNTIME_VERSION='3.11' HANDLER='f'
AS $$
def f(b):
    return None if b is None else str(int.from_bytes(b,'big'))
$$;

USE WAREHOUSE U256_SCALE_WH;

-- 10M rows, 5% infinite approvals, 2,000 token groups.
CREATE OR REPLACE TABLE SCALE_RAW AS
SELECT
  TO_BINARY(CASE WHEN MOD(seq8(),20) = 0 THEN REPEAT('F',64)
                 ELSE LPAD(LEFT(SHA2(TO_VARCHAR(seq8())||'a',256),40),64,'0') END,'HEX') AS value_raw,
  TO_BINARY(LEFT(SHA2(TO_VARCHAR(MOD(seq8(),2000)),256),40),'HEX') AS token_addr
FROM TABLE(GENERATOR(ROWCOUNT=>10000000));

-- Provider-side decode + chunk materialisation. Measured at 13s on MEDIUM.
CREATE OR REPLACE TABLE SCALE_DECODED AS
WITH d AS (SELECT value_raw, token_addr, U256_DEC(value_raw) AS s FROM SCALE_RAW),
p AS (SELECT value_raw, token_addr, s, LPAD(s,80,'0') AS z FROM d)
SELECT value_raw, token_addr, s AS value_dec_exact,
  TO_DECIMAL(SUBSTR(z, 1,20),38,0) AS d0,
  TO_DECIMAL(SUBSTR(z,21,20),38,0) AS d1,
  TO_DECIMAL(SUBSTR(z,41,20),38,0) AS d2,
  TO_DECIMAL(SUBSTR(z,61,20),38,0) AS d3
FROM p;

SELECT COUNT(*) AS rows_decoded, MAX(LENGTH(value_dec_exact)) AS max_digits FROM SCALE_DECODED;
-- expect: 10000000, 78

-- Exact per-group total, pure SQL. Note (t - MOD(t,base))/base, NOT FLOOR(t/base)
-- -- see RESULTS.md section 14.
CREATE OR REPLACE VIEW V_BENCH_EXACT AS
WITH t AS (SELECT token_addr, SUM(d0) t0, SUM(d1) t1, SUM(d2) t2, SUM(d3) t3
           FROM SCALE_DECODED GROUP BY token_addr),
c  AS (SELECT token_addr,t0,t1,t2, MOD(t3,100000000000000000000) r3,
              (t3-MOD(t3,100000000000000000000))/100000000000000000000 k3 FROM t),
c2 AS (SELECT token_addr,t0,t1,r3, MOD(t2+k3,100000000000000000000) r2,
              ((t2+k3)-MOD(t2+k3,100000000000000000000))/100000000000000000000 k2 FROM c),
c3 AS (SELECT token_addr,t0,r3,r2, MOD(t1+k2,100000000000000000000) r1,
              ((t1+k2)-MOD(t1+k2,100000000000000000000))/100000000000000000000 k1 FROM c2)
SELECT token_addr,
       LTRIM(TO_VARCHAR(FLOOR(t0+k1))||LPAD(TO_VARCHAR(FLOOR(r1)),20,'0')
       ||LPAD(TO_VARCHAR(FLOOR(r2)),20,'0')||LPAD(TO_VARCHAR(FLOOR(r3)),20,'0'),'0') AS exact_total
FROM c3;

-- EXACTNESS at 2,000 groups. Expect mismatched = 0.
WITH s AS (SELECT token_addr, exact_total FROM V_BENCH_EXACT),
u AS (SELECT token_addr, U256_SUM(value_raw) ut FROM SCALE_DECODED GROUP BY token_addr)
SELECT COUNT(*) groups, COUNT_IF(s.exact_total=u.ut) matching,
       COUNT_IF(s.exact_total<>u.ut) mismatched, MAX(LENGTH(u.ut)) max_digits
FROM s JOIN u USING (token_addr);

-- EXACTNESS at ~1,000,000 groups. This is the run that catches carry bugs; a
-- 2,000-group test passes while the arithmetic is wrong. Expect mismatched = 0.
WITH t AS (SELECT MOD(ABS(HASH(value_raw)),1000000) g, SUM(d0) t0, SUM(d1) t1, SUM(d2) t2, SUM(d3) t3
           FROM SCALE_DECODED GROUP BY 1),
c  AS (SELECT g,t0,t1,t2, MOD(t3,100000000000000000000) r3,
              (t3-MOD(t3,100000000000000000000))/100000000000000000000 k3 FROM t),
c2 AS (SELECT g,t0,t1,r3, MOD(t2+k3,100000000000000000000) r2,
              ((t2+k3)-MOD(t2+k3,100000000000000000000))/100000000000000000000 k2 FROM c),
c3 AS (SELECT g,t0,r3,r2, MOD(t1+k2,100000000000000000000) r1,
              ((t1+k2)-MOD(t1+k2,100000000000000000000))/100000000000000000000 k1 FROM c2),
s  AS (SELECT g, LTRIM(TO_VARCHAR(FLOOR(t0+k1))||LPAD(TO_VARCHAR(FLOOR(r1)),20,'0')
       ||LPAD(TO_VARCHAR(FLOOR(r2)),20,'0')||LPAD(TO_VARCHAR(FLOOR(r3)),20,'0'),'0') AS exact_total FROM c3),
u  AS (SELECT MOD(ABS(HASH(value_raw)),1000000) g, U256_SUM(value_raw) ut FROM SCALE_DECODED GROUP BY 1)
SELECT COUNT(*) groups, COUNT_IF(s.exact_total=u.ut) matching,
       COUNT_IF(s.exact_total<>u.ut) mismatched
FROM s JOIN u USING (g);

-- ---------------------------------------------------------------------------
-- TIMED RUNS. Back on the XSMALL, caching off, 3 passes each.
-- ---------------------------------------------------------------------------
USE WAREHOUSE <CONSUMER_WAREHOUSE>;
ALTER SESSION SET USE_CACHED_RESULT=FALSE;

SELECT 'MRKA' m, MAX(exact_total) v, SUM(LENGTH(exact_total)) c FROM V_BENCH_EXACT;
SELECT 'MRKA' m, MAX(exact_total) v, SUM(LENGTH(exact_total)) c FROM V_BENCH_EXACT;
SELECT 'MRKA' m, MAX(exact_total) v, SUM(LENGTH(exact_total)) c FROM V_BENCH_EXACT;

SELECT 'MRKB' m, MAX(ut) v, SUM(LENGTH(ut)) c
FROM (SELECT token_addr, U256_SUM(value_raw) ut FROM SCALE_DECODED GROUP BY token_addr);
SELECT 'MRKB' m, MAX(ut) v, SUM(LENGTH(ut)) c
FROM (SELECT token_addr, U256_SUM(value_raw) ut FROM SCALE_DECODED GROUP BY token_addr);
SELECT 'MRKB' m, MAX(ut) v, SUM(LENGTH(ut)) c
FROM (SELECT token_addr, U256_SUM(value_raw) ut FROM SCALE_DECODED GROUP BY token_addr);

SELECT 'MRKC' m, MAX(x) v, COUNT(x) c
FROM (SELECT token_addr, SUM(TO_DECFLOAT(value_dec_exact)) x FROM SCALE_DECODED GROUP BY token_addr);
SELECT 'MRKC' m, MAX(x) v, COUNT(x) c
FROM (SELECT token_addr, SUM(TO_DECFLOAT(value_dec_exact)) x FROM SCALE_DECODED GROUP BY token_addr);
SELECT 'MRKC' m, MAX(x) v, COUNT(x) c
FROM (SELECT token_addr, SUM(TO_DECFLOAT(value_dec_exact)) x FROM SCALE_DECODED GROUP BY token_addr);

-- MRKA and MRKB must return the IDENTICAL v and c. That is the exactness proof;
-- the timings below are only meaningful if those two columns match.
SELECT CASE WHEN CONTAINS(QUERY_TEXT,'MRKA') THEN 'A_exact_pure_SQL'
            WHEN CONTAINS(QUERY_TEXT,'MRKB') THEN 'B_python_udaf'
            WHEN CONTAINS(QUERY_TEXT,'MRKC') THEN 'C_decfloat_approx' END AS method,
       COUNT(*) n, MEDIAN(TOTAL_ELAPSED_TIME)::INT p50_ms,
       MIN(TOTAL_ELAPSED_TIME) min_ms, MAX(TOTAL_ELAPSED_TIME) max_ms
FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY(
        END_TIME_RANGE_START=>DATEADD('minute',-30,CURRENT_TIMESTAMP()), RESULT_LIMIT=>10000))
WHERE (CONTAINS(QUERY_TEXT,'MRKA') OR CONTAINS(QUERY_TEXT,'MRKB') OR CONTAINS(QUERY_TEXT,'MRKC'))
  AND QUERY_TYPE='SELECT' AND NOT CONTAINS(QUERY_TEXT,'CASE WHEN')
GROUP BY 1 ORDER BY 1;

-- ---------------------------------------------------------------------------
-- CLEAN UP. The MEDIUM warehouse in particular should not be left behind.
-- ---------------------------------------------------------------------------
DROP VIEW     IF EXISTS V_BENCH_EXACT;
DROP TABLE    IF EXISTS SCALE_DECODED;
DROP TABLE    IF EXISTS SCALE_RAW;
DROP FUNCTION IF EXISTS U256_DEC(BINARY);
DROP WAREHOUSE IF EXISTS U256_SCALE_WH;
SHOW WAREHOUSES LIKE 'U256_SCALE_WH';   -- expect zero rows
