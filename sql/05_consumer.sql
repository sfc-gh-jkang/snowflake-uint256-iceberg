-- 05 - Consumer: request, import, and aggregate the shared uint256 column
--   <LISTING_GLOBAL_NAME> <CONSUMER_DB> <CONSUMER_SCHEMA>
--
-- Run this in the CONSUMER account, in a different region from the provider.

-- Find the listing. is_ready_for_import = false means it has not been replicated
-- into your region yet.
USE WAREHOUSE <CONSUMER_WAREHOUSE>;

SHOW AVAILABLE LISTINGS IS_SHARED_WITH_ME = TRUE;

-- Request fulfillment. This is the step that breaks the deadlock, and it is
-- explicitly usable when is_ready_for_import is FALSE. No UI required.
--   timeout_mins = 0 registers demand and returns immediately.
--   omit it to block and poll (default 240 minutes).
-- https://docs.snowflake.com/en/sql-reference/stored-procedures/system_request_listing_and_wait
CALL SYSTEM$REQUEST_LISTING_AND_WAIT('<LISTING_GLOBAL_NAME>', 0);

-- Once is_ready_for_import flips to true:
CREATE DATABASE <CONSUMER_DB> FROM LISTING '<LISTING_GLOBAL_NAME>';

-- fixed(32) / fixed(20) arrive as BINARY(32) / BINARY(20). Full fidelity preserved.
DESC TABLE <CONSUMER_DB>.<SCHEMA>.APPROVALS_RAW;


-- ---------------------------------------------------------------------------
-- Path 1: aggregate the pre-decoded columns. What 99% of consumers should do.
-- ---------------------------------------------------------------------------
-- Note the filter. Summing an unlimited allowance is meaningless -- 2^256-1 is a
-- sentinel, not a quantity -- so the rows that cannot fit NUMBER(38,0) are exactly
-- the rows you exclude.
SELECT
  COUNT(*)                        AS rows_shared,
  COUNT_IF(is_infinite_approval)  AS infinite_approvals,
  SUM(value_token)                AS total_token
FROM <CONSUMER_DB>.<SCHEMA>.APPROVALS_DECODED
WHERE NOT is_infinite_approval;


-- ---------------------------------------------------------------------------
-- Path 2: aggregate the raw fixed(32) in pure SQL. No UDF, no setup.
-- ---------------------------------------------------------------------------
-- Four 64-bit limbs off the hex, each decoded with a 16-char format model, then
-- reassembled. The guard yields NULL rather than a wrong number when out of range.
WITH l AS (
  SELECT
    TRY_TO_DECIMAL(SUBSTR(TO_VARCHAR(value_raw,'HEX'), 1,16), REPEAT('X',16),38,0) AS l0,
    TRY_TO_DECIMAL(SUBSTR(TO_VARCHAR(value_raw,'HEX'),17,16), REPEAT('X',16),38,0) AS l1,
    TRY_TO_DECIMAL(SUBSTR(TO_VARCHAR(value_raw,'HEX'),33,16), REPEAT('X',16),38,0) AS l2,
    TRY_TO_DECIMAL(SUBSTR(TO_VARCHAR(value_raw,'HEX'),49,16), REPEAT('X',16),38,0) AS l3
  FROM <CONSUMER_DB>.<SCHEMA>.APPROVALS_RAW
)
SELECT
  TO_VARCHAR(SUM(CASE WHEN l0=0 AND l1=0 AND l2 < 5000000000000000000
                      THEN l2 * 18446744073709551616 + l3 END))  AS total_wei_exact,
  COUNT_IF(NOT (l0=0 AND l1=0 AND l2 < 5000000000000000000))     AS out_of_range
FROM l;

-- Or, if the provider shared the secure SQL UDF, the same thing as a one-liner:
-- SELECT SUM(<CONSUMER_DB>.<SCHEMA>.U256_TO_NUMBER(value_raw))
-- FROM <CONSUMER_DB>.<SCHEMA>.APPROVALS_RAW;


-- ---------------------------------------------------------------------------
-- Path 3: the consumer's OWN Python UDAF, for full 256-bit exact sums.
-- ---------------------------------------------------------------------------
-- The imported database is read-only, but nothing stops you creating a function in
-- your OWN database and pointing it at a shared column.
CREATE SCHEMA IF NOT EXISTS <CONSUMER_SCHEMA>;

CREATE OR REPLACE AGGREGATE FUNCTION <CONSUMER_SCHEMA>.U256_SUM(B BINARY)
RETURNS VARCHAR
LANGUAGE PYTHON RUNTIME_VERSION='3.11' HANDLER='S'
AS $$
class S:
    def __init__(self):        self._t = 0
    @property
    def aggregate_state(self): return str(self._t)
    def accumulate(self, b):
        if b is not None:      self._t += int.from_bytes(b, 'big')
    def merge(self, other):    self._t += int(other)
    def finish(self):          return str(self._t)
$$;

-- Full 256-bit exact sum, including every infinite approval. No NUMBER type can
-- hold this result, which is the point.
SELECT
  <CONSUMER_SCHEMA>.U256_SUM(value_raw)                                    AS full_exact_sum,
  LENGTH(<CONSUMER_SCHEMA>.U256_SUM(value_raw))                            AS digits,
  <CONSUMER_SCHEMA>.U256_SUM(CASE WHEN NOT is_infinite_approval
                                  THEN value_raw END)                      AS sum_excl_infinite
FROM <CONSUMER_DB>.<SCHEMA>.APPROVALS_DECODED;
