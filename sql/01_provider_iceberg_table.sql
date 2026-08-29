-- 01 - Provider: Iceberg table holding uint256 as fixed(32)
--
-- Replace the <PLACEHOLDER> tokens before running. See README "Running it".
--   <DB> <SCHEMA> <EXTERNAL_VOLUME>
--
-- Why fixed(32): 32 bytes x 8 = 256 bits, the same width as an EVM word.
-- Snowflake NUMBER caps at 38 decimal digits (~126 bits), and the Apache Iceberg
-- spec caps decimal(P,S) at P <= 38, so no numeric type in the Iceberg ecosystem
-- can hold a full uint256. Binary can, exactly.
--   https://docs.snowflake.com/en/user-guide/tables-iceberg-data-types
--   https://docs.snowflake.com/en/sql-reference/data-types-numeric

USE WAREHOUSE <WAREHOUSE>;

CREATE SCHEMA IF NOT EXISTS <DB>.<SCHEMA>;

-- GOTCHA: BINARY(32) is REJECTED in Iceberg DDL --
--   "For Iceberg tables, only max length (67,108,864) is supported for 'BINARY(L)'"
-- Use the Iceberg type name fixed(32). DESC TABLE then reports it as BINARY(32).
CREATE OR REPLACE ICEBERG TABLE <DB>.<SCHEMA>.APPROVALS_RAW (
  block_number  NUMBER(19,0),
  log_index     NUMBER(10,0),
  tx_hash       fixed(32),   -- bytes32
  token_addr    fixed(20),   -- address
  owner_addr    fixed(20),   -- address
  spender_addr  fixed(20),   -- address
  value_raw     fixed(32)    -- uint256, exact, no loss
)
CATALOG = 'SNOWFLAKE'
EXTERNAL_VOLUME = '<EXTERNAL_VOLUME>'
BASE_LOCATION = 'eth_u256_approvals_raw'
ICEBERG_VERSION = 3;   -- v3 required for consumer-side streams / dynamic tables on the share

-- Synthetic ERC-20 Approval events. 5% are "infinite approvals" (2^256-1), which is
-- the standard unlimited-allowance idiom and the reason fixed(32) is load-bearing.
INSERT INTO <DB>.<SCHEMA>.APPROVALS_RAW
SELECT
  21000000 + FLOOR(SEQ8()/50)                                             AS block_number,
  MOD(SEQ8(), 50)                                                          AS log_index,
  TO_BINARY(SHA2(SEQ8()::VARCHAR, 256), 'HEX')                             AS tx_hash,
  TO_BINARY(LEFT(SHA2('token'||MOD(SEQ8(),12)::VARCHAR,256),40),'HEX')     AS token_addr,
  TO_BINARY(LEFT(SHA2('own'||MOD(SEQ8(),9000)::VARCHAR,256),40),'HEX')     AS owner_addr,
  TO_BINARY(LEFT(SHA2('spend'||MOD(SEQ8(),40)::VARCHAR,256),40),'HEX')     AS spender_addr,
  CASE
    WHEN MOD(SEQ8(), 20) = 0 THEN TO_BINARY(REPEAT('F',64),'HEX')          -- infinite approval
    WHEN MOD(SEQ8(), 97) = 0 THEN TO_BINARY(REPEAT('0',64),'HEX')          -- revoke
    ELSE TO_BINARY(LPAD(RIGHT(SHA2(SEQ8()::VARCHAR,256),18),64,'0'),'HEX') -- normal wei
  END                                                                      AS value_raw
FROM TABLE(GENERATOR(ROWCOUNT => 200000));

-- Proof that byte order equals unsigned numeric order, so ORDER BY / MIN / MAX /
-- range predicates / clustering keys all work on the raw bytes with NO decode.
SELECT TO_VARCHAR(value_raw,'HEX') AS hex, COUNT(*) AS n
FROM <DB>.<SCHEMA>.APPROVALS_RAW
GROUP BY 1 ORDER BY TO_BINARY(hex,'HEX') LIMIT 5;
