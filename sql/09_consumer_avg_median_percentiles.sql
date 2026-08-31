-- 09: AVG, MEDIAN and percentiles over the full 256-bit range, exact, pure SQL.
-- Placeholders:
--   <CONSUMER_DB> <SCHEMA> <CONSUMER_SCHEMA> <CONSUMER_WAREHOUSE>
--
-- These were previously documented as "not solved". They are solvable, and this
-- script is the verified proof. Two observations do the work:
--
--   1. ORDER STATISTICS ARE NOT AN ARITHMETIC PROBLEM. Ordering by the four
--      base-10^20 chunk columns is identical to ordering by the true numeric
--      value, because the chunks are big-endian positional digits. It is also
--      identical to ordering the raw fixed(32) bytes. Verified on 200,000 rows:
--      RANK() OVER (ORDER BY value_raw) and RANK() OVER (ORDER BY d0,d1,d2,d3)
--      disagreed on ZERO rows. So median and percentiles need no decode at all.
--
--   2. DIVISION DOES DECOMPOSE, the same way the carry does. Long division over
--      base-10^20 digits keeps every intermediate inside decimal(38,0), because
--      the running remainder is < n and n <= 10^18 (the same row-count bound
--      that already caps the chunk sums), so remainder * 10^20 < 10^38.
--
-- READ THIS BEFORE CHANGING THE DIVISION ARITHMETIC
-- Every quotient below is written (x - MOD(x,d)) / d, never FLOOR(x / d).
-- Snowflake division returns a scale-6 result and ROUNDS rather than truncates,
-- so FLOOR(x/d) is silently off by one on a fraction of inputs. This is the
-- identical trap documented for the carry in sql/07 -- it applies to the
-- division here for exactly the same reason. The FLOOR() calls in the final
-- SELECT are cosmetic only: they strip a trailing ".000000" before string
-- concatenation. They are not performing the division.

USE WAREHOUSE <CONSUMER_WAREHOUSE>;
USE SCHEMA <CONSUMER_SCHEMA>;

-- ---------------------------------------------------------------------------
-- A. Ordering agreement. Establishes that the chunk columns are a valid sort key.
-- Expect disagreements = 0.
-- ---------------------------------------------------------------------------
WITH r AS (
  SELECT RANK() OVER (ORDER BY value_raw)      AS rk_bin,
         RANK() OVER (ORDER BY d0, d1, d2, d3) AS rk_chunk
  FROM <CONSUMER_DB>.<SCHEMA>.APPROVALS_CHUNKED
)
SELECT 'ORDER_AGREEMENT' AS test, COUNT(*) AS n_rows,
       COUNT_IF(rk_bin <> rk_chunk) AS disagreements
FROM r;

-- ---------------------------------------------------------------------------
-- B. Percentiles and discrete median, nearest-rank. Returns the exact value.
-- No arithmetic at all -- it selects an existing row, so it is exact by
-- construction at any magnitude. p50 here IS the discrete median.
-- ---------------------------------------------------------------------------
WITH o AS (
  SELECT value_dec_exact,
         ROW_NUMBER() OVER (ORDER BY d0, d1, d2, d3, tx_hash, log_index) AS rn,
         COUNT(*)     OVER ()                                            AS n
  FROM <CONSUMER_DB>.<SCHEMA>.APPROVALS_CHUNKED
)
SELECT p.p AS pctl, o.value_dec_exact AS exact_value
FROM (SELECT column1 AS p FROM VALUES (0.01),(0.25),(0.50),(0.75),(0.90),(0.99)) p
JOIN o ON o.rn = GREATEST(1, CEIL(p.p * o.n))
ORDER BY p.p;

-- Per-group form: add PARTITION BY token_addr to both window functions and
-- join on (token_addr, rn).

-- ---------------------------------------------------------------------------
-- C. Exact AVG. Carry-normalise the chunk sums into base-10^20 digits e3..e0,
-- then long-divide by n. Emits 20 fractional digits; chain another step off R0
-- for 20 more.
-- ---------------------------------------------------------------------------
WITH t AS (
  SELECT COUNT(*) n, SUM(d0) t0, SUM(d1) t1, SUM(d2) t2, SUM(d3) t3
  FROM <CONSUMER_DB>.<SCHEMA>.APPROVALS_CHUNKED
),
c  AS (SELECT n,t0,t1,t2, MOD(t3,100000000000000000000) r3,
              (t3-MOD(t3,100000000000000000000))/100000000000000000000 k3 FROM t),
c2 AS (SELECT n,t0,t1,r3, MOD(t2+k3,100000000000000000000) r2,
              ((t2+k3)-MOD(t2+k3,100000000000000000000))/100000000000000000000 k2 FROM c),
c3 AS (SELECT n,t0,r3,r2, MOD(t1+k2,100000000000000000000) r1,
              ((t1+k2)-MOD(t1+k2,100000000000000000000))/100000000000000000000 k1 FROM c2),
d  AS (SELECT n, t0+k1 AS e3, r1 AS e2, r2 AS e1, r3 AS e0 FROM c3),
s3 AS (SELECT n,e2,e1,e0,(e3-MOD(e3,n))/n Q3, MOD(e3,n) R3 FROM d),
s2 AS (SELECT n,e1,e0,Q3,(R3*100000000000000000000+e2) a2 FROM s3),
s2b AS (SELECT n,e1,e0,Q3,(a2-MOD(a2,n))/n Q2, MOD(a2,n) R2 FROM s2),
s1 AS (SELECT n,e0,Q3,Q2,(R2*100000000000000000000+e1) a1 FROM s2b),
s1b AS (SELECT n,e0,Q3,Q2,(a1-MOD(a1,n))/n Q1, MOD(a1,n) R1 FROM s1),
s0 AS (SELECT n,Q3,Q2,Q1,(R1*100000000000000000000+e0) a0 FROM s1b),
s0b AS (SELECT n,Q3,Q2,Q1,(a0-MOD(a0,n))/n Q0, MOD(a0,n) R0 FROM s0),
fr AS (SELECT n,Q3,Q2,Q1,Q0,(R0*100000000000000000000) af FROM s0b),
frb AS (SELECT n,Q3,Q2,Q1,Q0,(af-MOD(af,n))/n QF FROM fr)
SELECT n AS rows_in_group,
       COALESCE(NULLIF(LTRIM(
           TO_VARCHAR(FLOOR(Q3))
        || LPAD(TO_VARCHAR(FLOOR(Q2)),20,'0')
        || LPAD(TO_VARCHAR(FLOOR(Q1)),20,'0')
        || LPAD(TO_VARCHAR(FLOOR(Q0)),20,'0'), '0'), ''), '0')
       || '.' || LPAD(TO_VARCHAR(FLOOR(QF)),20,'0') AS exact_avg
FROM frb;

-- ---------------------------------------------------------------------------
-- D. PERCENTILE_CONT-style interpolated median: the two middle rows, summed,
-- divided by 2. Nothing new -- the carry from sql/07 and the long division from
-- section C, over a two-row group.
-- ---------------------------------------------------------------------------
WITH o AS (
  SELECT d0,d1,d2,d3,
         ROW_NUMBER() OVER (ORDER BY d0,d1,d2,d3, tx_hash, log_index) AS rn,
         COUNT(*)     OVER ()                                        AS nn
  FROM <CONSUMER_DB>.<SCHEMA>.APPROVALS_CHUNKED
),
mid AS (SELECT d0,d1,d2,d3 FROM o WHERE rn IN (FLOOR(nn/2), FLOOR(nn/2)+1)),
t  AS (SELECT 2 AS n, SUM(d0) t0, SUM(d1) t1, SUM(d2) t2, SUM(d3) t3 FROM mid),
c  AS (SELECT n,t0,t1,t2, MOD(t3,100000000000000000000) r3,
              (t3-MOD(t3,100000000000000000000))/100000000000000000000 k3 FROM t),
c2 AS (SELECT n,t0,t1,r3, MOD(t2+k3,100000000000000000000) r2,
              ((t2+k3)-MOD(t2+k3,100000000000000000000))/100000000000000000000 k2 FROM c),
c3 AS (SELECT n,t0,r3,r2, MOD(t1+k2,100000000000000000000) r1,
              ((t1+k2)-MOD(t1+k2,100000000000000000000))/100000000000000000000 k1 FROM c2),
d  AS (SELECT n, t0+k1 AS e3, r1 AS e2, r2 AS e1, r3 AS e0 FROM c3),
s3 AS (SELECT n,e2,e1,e0,(e3-MOD(e3,n))/n Q3, MOD(e3,n) R3 FROM d),
s2 AS (SELECT n,e1,e0,Q3,(R3*100000000000000000000+e2) a2 FROM s3),
s2b AS (SELECT n,e1,e0,Q3,(a2-MOD(a2,n))/n Q2, MOD(a2,n) R2 FROM s2),
s1 AS (SELECT n,e0,Q3,Q2,(R2*100000000000000000000+e1) a1 FROM s2b),
s1b AS (SELECT n,e0,Q3,Q2,(a1-MOD(a1,n))/n Q1, MOD(a1,n) R1 FROM s1),
s0 AS (SELECT n,Q3,Q2,Q1,(R1*100000000000000000000+e0) a0 FROM s1b),
s0b AS (SELECT n,Q3,Q2,Q1,(a0-MOD(a0,n))/n Q0, MOD(a0,n) R0 FROM s0),
fr AS (SELECT n,Q3,Q2,Q1,Q0,(R0*100000000000000000000) af FROM s0b),
frb AS (SELECT Q3,Q2,Q1,Q0,(af-MOD(af,n))/n QF FROM fr)
SELECT COALESCE(NULLIF(LTRIM(
           TO_VARCHAR(FLOOR(Q3))
        || LPAD(TO_VARCHAR(FLOOR(Q2)),20,'0')
        || LPAD(TO_VARCHAR(FLOOR(Q1)),20,'0')
        || LPAD(TO_VARCHAR(FLOOR(Q0)),20,'0'), '0'), ''), '0')
       || '.' || LPAD(TO_VARCHAR(FLOOR(QF)),20,'0') AS exact_interpolated_median
FROM frb;

-- ---------------------------------------------------------------------------
-- Limits that remain real:
--   * Snowflake's BUILT-IN MEDIAN / PERCENTILE_CONT / PERCENTILE_DISC still
--     cannot be used, because they take a numeric argument and uint256 does not
--     fit decimal(38,0). The rank-based form above replaces them.
--   * The row-count bound is unchanged: n <= 10^18, the same cap the chunk sums
--     already carry.
--   * Everything here is arithmetic on the chunk columns, so it inherits the
--     10^18-row overflow bound and nothing more.
-- ---------------------------------------------------------------------------
