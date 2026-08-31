-- 07 - Consumer: exact uint256 aggregation in pure SQL, including GROUP BY
--   <CONSUMER_DB> <SCHEMA> <CONSUMER_SCHEMA> <CONSUMER_WAREHOUSE>
--
-- No UDF anywhere in this file. Everything here runs against the shared Iceberg
-- table using only SUM, MOD, arithmetic and string functions, which is the whole
-- point: Java and Python UDFs cannot be granted to a share, so a consumer who
-- wants an exact total has to get it from SQL or create their own function.
--
-- The four D columns are base-10^20 chunks materialised by 06. Summing them is
-- ordinary numeric aggregation. Reassembling the four subtotals into the exact
-- total is carry propagation in base 10^20, which stays inside decimal(38,0) at
-- every step and therefore works per group inside the query.
--
-- ===========================================================================
-- READ THIS BEFORE CHANGING THE CARRY ARITHMETIC
-- ===========================================================================
--
-- Do NOT write FLOOR(t / 100000000000000000000) to extract a carry.
--
-- Snowflake division returns a scale-6 result and ROUNDS rather than truncating
-- (see docs, "Arithmetic operators": scale = max(S1, min(S1+6, 12)), and "If the
-- result of the division operation exceeds the output scale, Snowflake rounds
-- the output rather than truncating"). So:
--
--   SELECT 399999999999999999999999999 / 100000000000000000000;
--   -> 4000000.000000                      -- rounded UP from 3999999.99999...
--   SELECT FLOOR(399999999999999999999999999 / 100000000000000000000);
--   -> 4000000                             -- WRONG, true floor is 3999999
--
-- The carry comes out one too high and the reassembled total lands exactly
-- 10^20 out. Use (t - MOD(t, base)) / base, which is exact because the
-- numerator is already a whole multiple of the base:
--
--   SELECT (399999999999999999999999999 - MOD(399999999999999999999999999,100000000000000000000))
--          / 100000000000000000000;
--   -> 3999999                             -- correct
--
-- DEGENERATE GROUPS. LTRIM(...,'0') on an all-zero string returns the EMPTY STRING,
-- not '0', and an all-NULL group returns NULL. The UDAF returns '0' for both, so
-- either case shows up as a spurious mismatch. Hence COALESCE(NULLIF(...,''),'0').
-- No group in the 200k or 10M sets summed to zero, so this never surfaced in the
-- headline validation -- it was found by constructing the case deliberately.
--
-- FAILURE RATE, AND WHY THAT MATTERS FOR HOW YOU TEST THIS.
-- The FLOOR version corrupted 1 group in 999,934. A 2,000-group test passed
-- clean while the code was wrong. If you modify this, validate against a
-- big-integer implementation over at least a million groups, not a handful.

USE WAREHOUSE <CONSUMER_WAREHOUSE>;

-- ---------------------------------------------------------------------------
-- Path A: exact grand total, pure SQL.
-- ---------------------------------------------------------------------------
WITH t AS (
  SELECT SUM(d0) AS t0, SUM(d1) AS t1, SUM(d2) AS t2, SUM(d3) AS t3
  FROM <CONSUMER_DB>.<SCHEMA>.APPROVALS_CHUNKED
),
c  AS (SELECT t0, t1, t2,
              MOD(t3, 100000000000000000000) AS r3,
              (t3 - MOD(t3, 100000000000000000000)) / 100000000000000000000 AS k3
       FROM t),
c2 AS (SELECT t0, t1, r3,
              MOD(t2 + k3, 100000000000000000000) AS r2,
              ((t2 + k3) - MOD(t2 + k3, 100000000000000000000)) / 100000000000000000000 AS k2
       FROM c),
c3 AS (SELECT t0, r3, r2,
              MOD(t1 + k2, 100000000000000000000) AS r1,
              ((t1 + k2) - MOD(t1 + k2, 100000000000000000000)) / 100000000000000000000 AS k1
       FROM c2)
SELECT COALESCE(NULLIF(LTRIM(
         TO_VARCHAR(FLOOR(t0 + k1))
         || LPAD(TO_VARCHAR(FLOOR(r1)),20,'0')
         || LPAD(TO_VARCHAR(FLOOR(r2)),20,'0')
         || LPAD(TO_VARCHAR(FLOOR(r3)),20,'0'), '0'),''),'0') AS exact_total_wei
FROM c3;
-- The FLOOR() calls here are cosmetic only: MOD and the exact division above
-- yield whole numbers, but Snowflake carries a scale on them, so FLOOR strips
-- the trailing ".000000" before string assembly. They are NOT doing the carry.

-- ---------------------------------------------------------------------------
-- Path B: exact total PER GROUP. This is the one that needs base 10^20 --
-- there is no client-side reassembly step, the exact number comes back per row.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW <CONSUMER_SCHEMA>.V_EXACT_BY_TOKEN AS
WITH t AS (
  SELECT token_addr, COUNT(*) AS n,
         SUM(d0) AS t0, SUM(d1) AS t1, SUM(d2) AS t2, SUM(d3) AS t3
  FROM <CONSUMER_DB>.<SCHEMA>.APPROVALS_CHUNKED
  GROUP BY token_addr
),
c  AS (SELECT token_addr, n, t0, t1, t2,
              MOD(t3, 100000000000000000000) AS r3,
              (t3 - MOD(t3, 100000000000000000000)) / 100000000000000000000 AS k3
       FROM t),
c2 AS (SELECT token_addr, n, t0, t1, r3,
              MOD(t2 + k3, 100000000000000000000) AS r2,
              ((t2 + k3) - MOD(t2 + k3, 100000000000000000000)) / 100000000000000000000 AS k2
       FROM c),
c3 AS (SELECT token_addr, n, t0, r3, r2,
              MOD(t1 + k2, 100000000000000000000) AS r1,
              ((t1 + k2) - MOD(t1 + k2, 100000000000000000000)) / 100000000000000000000 AS k1
       FROM c2)
SELECT token_addr, n,
       COALESCE(NULLIF(LTRIM(TO_VARCHAR(FLOOR(t0 + k1))
             || LPAD(TO_VARCHAR(FLOOR(r1)),20,'0')
             || LPAD(TO_VARCHAR(FLOOR(r2)),20,'0')
             || LPAD(TO_VARCHAR(FLOOR(r3)),20,'0'), '0'),''),'0') AS exact_total_wei
FROM c3;

SELECT * FROM <CONSUMER_SCHEMA>.V_EXACT_BY_TOKEN ORDER BY n DESC LIMIT 10;

-- ---------------------------------------------------------------------------
-- Path C: DECFLOAT. One number, cannot overflow, never NULL, 38 significant
-- digits. NOT exact -- see RESULTS.md section 3. Reasonable for a BI tile,
-- wrong for anything reconciling to the wei.
-- ---------------------------------------------------------------------------
SELECT token_addr, SUM(TO_DECFLOAT(value_dec_exact)) AS approx_total_wei
FROM <CONSUMER_DB>.<SCHEMA>.APPROVALS_CHUNKED
GROUP BY token_addr
ORDER BY 2 DESC
LIMIT 10;

-- ---------------------------------------------------------------------------
-- Validation. Compare pure SQL against a big-integer implementation.
-- Requires the consumer-side Python UDAF from 05. Expect mismatched = 0.
-- ---------------------------------------------------------------------------
WITH s AS (SELECT token_addr, exact_total_wei FROM <CONSUMER_SCHEMA>.V_EXACT_BY_TOKEN),
u AS (SELECT token_addr, <CONSUMER_SCHEMA>.U256_SUM(value_raw) AS exact_udaf
      FROM <CONSUMER_DB>.<SCHEMA>.APPROVALS_CHUNKED GROUP BY token_addr)
SELECT COUNT(*) AS groups,
       COUNT_IF(s.exact_total_wei =  u.exact_udaf) AS matching,
       COUNT_IF(s.exact_total_wei <> u.exact_udaf) AS mismatched,
       MAX(LENGTH(u.exact_udaf))                   AS max_digits
FROM s JOIN u USING (token_addr);

-- ---------------------------------------------------------------------------
-- AVG, MEDIAN and percentiles: see sql/09. An earlier version of this comment
-- said they were not solvable because "SUM decomposes across chunks, division
-- does not." That was wrong and untested. Percentiles are an ordering problem,
-- not an arithmetic one -- ordering by (d0,d1,d2,d3) is ordering by the true
-- value -- and long division DOES decompose over base-10^20 digits inside
-- decimal(38,0). Both are exact and pure SQL in sql/09. MIN and MAX need
-- neither: they work directly on the raw fixed(32) bytes (RESULTS.md §2).
-- ---------------------------------------------------------------------------
