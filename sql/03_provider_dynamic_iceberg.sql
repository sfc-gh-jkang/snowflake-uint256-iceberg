-- 03 - Provider: dynamic ICEBERG table carrying the decoded columns
--   <DB> <SCHEMA> <WAREHOUSE> <EXTERNAL_VOLUME>
--
-- This is the load-bearing piece of the design. Python cannot cross a share
-- boundary, so the decode runs HERE, provider-side, on refresh. The output is
-- still an Iceberg table on your own external volume, so the share stays open
-- format and remains readable by Spark / Trino / PyIceberg.
--
-- Consumers then write plain SQL and never touch a UDF.

USE WAREHOUSE <WAREHOUSE>;

CREATE OR REPLACE DYNAMIC ICEBERG TABLE <DB>.<SCHEMA>.APPROVALS_DECODED
  TARGET_LAG = '1 hour'
  WAREHOUSE = <WAREHOUSE>
  EXTERNAL_VOLUME = '<EXTERNAL_VOLUME>'
  CATALOG = 'SNOWFLAKE'
  BASE_LOCATION = 'eth_u256_approvals_decoded'
AS
WITH l AS (
  SELECT
    block_number, log_index, tx_hash, token_addr, owner_addr, spender_addr, value_raw,
    TRY_TO_DECIMAL(SUBSTR(TO_VARCHAR(value_raw,'HEX'), 1,16), REPEAT('X',16),38,0) AS l0,
    TRY_TO_DECIMAL(SUBSTR(TO_VARCHAR(value_raw,'HEX'),17,16), REPEAT('X',16),38,0) AS l1,
    TRY_TO_DECIMAL(SUBSTR(TO_VARCHAR(value_raw,'HEX'),33,16), REPEAT('X',16),38,0) AS l2,
    TRY_TO_DECIMAL(SUBSTR(TO_VARCHAR(value_raw,'HEX'),49,16), REPEAT('X',16),38,0) AS l3
  FROM <DB>.<SCHEMA>.APPROVALS_RAW
)
SELECT
  block_number, log_index, tx_hash, token_addr, owner_addr, spender_addr,

  -- 1. Full fidelity. fixed(32) survives into the derived table unchanged.
  value_raw,

  -- 2. Exact decimal string, decoded by the Python UDF at refresh time.
  <DB>.<SCHEMA>.U256_DEC(value_raw)                                   AS value_dec_exact,

  -- 3. Pre-scaled to token units. This is what most consumers actually query.
  CASE WHEN l0=0 AND l1=0 AND l2 < 5000000000000000000
       THEN (l2 * 18446744073709551616 + l3) / 1000000000000000000
       END                                                            AS value_token,

  -- 4. Cheap binary comparison, no decode. Lets a consumer exclude unlimited
  --    allowances with one boolean instead of tripping over a 78-digit number.
  value_raw = TO_BINARY(REPEAT('F',64),'HEX')                         AS is_infinite_approval,

  -- 5. Honest range flag rather than a silently wrong number.
  NOT (l0=0 AND l1=0 AND l2 < 5000000000000000000)                    AS exceeds_number38
FROM l;
