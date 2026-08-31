-- 06 - Provider: base-10^20 chunk columns so consumers can aggregate with plain SUM
--   <DB> <SCHEMA> <WAREHOUSE> <EXTERNAL_VOLUME>
--
-- WHY THIS FILE EXISTS
--
-- 03 materialises VALUE_DEC_EXACT, the full decimal string. That string is
-- lossless but you cannot SUM a string, and the pre-scaled decimal column is
-- NULL on anything above 38 digits. So on its own it does not give a consumer
-- an aggregation path. Measured on the 200k-row set:
--
--   SELECT SUM(TO_DECIMAL(value_dec_exact,38,0)) FROM ...APPROVALS_DECODED;
--   -> 100038 (22018): Numeric value '1157920892...639935' is not recognized
--
-- Java and Python UDFs cannot be granted to a share, so the only compute you
-- can hand a consumer is SQL, which caps at 38 digits. The fix is to split the
-- value across four decimal(38,0) columns and let ordinary SUM do the work.
--
-- WHY BASE 10^20 AND NOT BASE 2^64
--
-- Both work for a single grand total. Only a power-of-ten base lets the four
-- subtotals be reassembled into the exact number INSIDE SQL, because carry
-- propagation in base 10^20 is MOD plus string concatenation (see 07).
-- Base-2^64 limbs need big-integer arithmetic outside SQL, which is fine for
-- one number and useless for a GROUP BY. Choose the base for the aggregation
-- you want, not for the decode cost.
--
-- HEADROOM
--
-- Each chunk is at most 20 digits. A chunk SUM gains one digit per power of ten
-- of row count, so decimal(38,0) carries roughly 10^18 rows before overflow.
-- At 10^7 rows the widest chunk sum observed was 27 digits.
--
-- ICEBERG TYPE GOTCHA
--
-- In Iceberg DDL the type is FIXED(32), NOT BINARY(32). BINARY(32) is rejected:
--   099209 (42601): For Iceberg tables, only max length (67,108,864) is
--   supported for 'BINARY(L)'. Alternatively, use BINARY directly.
-- Do not take that advice literally: unqualified BINARY gives you an unbounded
-- binary, not a 32-byte fixed. Use FIXED(32). Confusingly, DESC TABLE afterwards
-- reports the column back as BINARY(32).
--
-- decimal(38,0) is inside the Iceberg spec's own 38-digit cap, so these are
-- ordinary Iceberg decimals. Spark and Trino read them with no custom type.
--
-- FIXED(32) IS EXACT-LENGTH, AND THE PLATFORM NOW ENFORCES IT
--
-- Inserting a value that is not exactly 32 bytes is an error:
--   100041 (22000): Binary value has length 31, but Iceberg fixed[32] type
--   requires exactly 32 bytes for column ''
-- Before release 10.7 (Mar 2026, BCR-2246) Snowflake accepted values SHORTER
-- than L and left-padded them, which meant a truncated hex string could land
-- silently. It now fails loudly, so the 32-byte invariant is enforced by the
-- engine rather than by convention. The decode in sql/01 pads with
-- LPAD(...,64,'0') to 64 hex characters = 32 bytes, satisfying old and new.
--   https://docs.snowflake.com/en/user-guide/tables-iceberg-data-types
--   https://docs.snowflake.com/en/release-notes/bcr-bundles/2026_02/bcr-2246
--
-- CHANGE TRACKING IS ALREADY ON, AND CANNOT BE TURNED OFF
--
-- Sharing a table that a consumer will build a stream or dynamic table on
-- normally requires the PROVIDER to enable change tracking first, because
-- consumers cannot enable it on a shared object:
--   https://docs.snowflake.com/en/user-guide/dynamic-tables/sharing
-- For Iceberg tables that step is automatic. Every table below reports
-- change_tracking = ON with no clause present, and an explicit attempt to
-- disable it is rejected:
--   001435 (22023): invalid value 'false' for property 'CHANGE_TRACKING',
--   Reason: Change Tracking cannot be turned off for Iceberg tables
-- So no ALTER is needed here, and a consumer-side stream on the shared
-- APPROVALS_CHUNKED will build. Note this is Iceberg-specific: on a standard
-- Snowflake table you would still have to set it yourself before sharing.

USE WAREHOUSE <WAREHOUSE>;

-- ---------------------------------------------------------------------------
-- Static form. Use this to test the shape before committing to a refresh cadence.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE ICEBERG TABLE <DB>.<SCHEMA>.APPROVALS_CHUNKED (
  TX_HASH              FIXED(32),
  LOG_INDEX            DECIMAL(10,0),
  TOKEN_ADDR           FIXED(20),
  VALUE_RAW            FIXED(32),      -- keep the raw bytes: the only definitionally lossless column
  VALUE_DEC_EXACT      STRING,         -- exact, but not summable
  D0                   DECIMAL(38,0),  -- most significant 20 digits
  D1                   DECIMAL(38,0),
  D2                   DECIMAL(38,0),
  D3                   DECIMAL(38,0),  -- least significant 20 digits
  IS_INFINITE_APPROVAL BOOLEAN
)
  EXTERNAL_VOLUME = '<EXTERNAL_VOLUME>'
  CATALOG = 'SNOWFLAKE'
  BASE_LOCATION = 'eth_u256_approvals_chunked'
  ICEBERG_VERSION = 3;

INSERT INTO <DB>.<SCHEMA>.APPROVALS_CHUNKED
SELECT
  tx_hash, log_index, token_addr, value_raw, value_dec_exact,
  -- LPAD to 80 so the four 20-digit windows are positionally stable.
  -- uint256 max is 78 digits, so the top window always has two leading zeros.
  TO_DECIMAL(SUBSTR(LPAD(value_dec_exact,80,'0'), 1,20),38,0),
  TO_DECIMAL(SUBSTR(LPAD(value_dec_exact,80,'0'),21,20),38,0),
  TO_DECIMAL(SUBSTR(LPAD(value_dec_exact,80,'0'),41,20),38,0),
  TO_DECIMAL(SUBSTR(LPAD(value_dec_exact,80,'0'),61,20),38,0),
  is_infinite_approval
FROM <DB>.<SCHEMA>.APPROVALS_DECODED;

-- ---------------------------------------------------------------------------
-- Production form: dynamic ICEBERG table, so the decode is paid once per refresh.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE DYNAMIC ICEBERG TABLE <DB>.<SCHEMA>.APPROVALS_CHUNKED_DT
  TARGET_LAG = '1 hour'
  WAREHOUSE = <WAREHOUSE>
  EXTERNAL_VOLUME = '<EXTERNAL_VOLUME>'
  CATALOG = 'SNOWFLAKE'
  BASE_LOCATION = 'eth_u256_approvals_chunked_dt'
  ICEBERG_VERSION = 3
AS
SELECT
  tx_hash, log_index, token_addr, value_raw, value_dec_exact,
  TO_DECIMAL(SUBSTR(LPAD(value_dec_exact,80,'0'), 1,20),38,0) AS d0,
  TO_DECIMAL(SUBSTR(LPAD(value_dec_exact,80,'0'),21,20),38,0) AS d1,
  TO_DECIMAL(SUBSTR(LPAD(value_dec_exact,80,'0'),41,20),38,0) AS d2,
  TO_DECIMAL(SUBSTR(LPAD(value_dec_exact,80,'0'),61,20),38,0) AS d3,
  is_infinite_approval
FROM <DB>.<SCHEMA>.APPROVALS_DECODED;

-- Confirm the Iceberg schema really is native Iceberg types, not Snowflake ones.
SELECT GET_DDL('TABLE','<DB>.<SCHEMA>.APPROVALS_CHUNKED');
--   VALUE_RAW FIXED(32), VALUE_DEC_EXACT STRING, D0 DECIMAL(38, 0), ...

-- Confirm real Iceberg metadata exists on your own external volume.
SELECT SYSTEM$GET_ICEBERG_TABLE_INFORMATION('<DB>.<SCHEMA>.APPROVALS_CHUNKED');
--   {"metadataLocation":"s3://.../eth_u256_approvals_chunked.<suffix>/metadata/00001-....metadata.json","status":"success"}

-- ---------------------------------------------------------------------------
-- Add to the share.
-- ---------------------------------------------------------------------------
GRANT SELECT ON ICEBERG TABLE <DB>.<SCHEMA>.APPROVALS_CHUNKED TO SHARE ETH_U256_SHARE;

-- CROSS-REGION GOTCHA. Adding a table to an existing cross-region listing is
-- NOT immediate. Until the listing refreshes, the consumer sees:
--   002003 (42S02): Table '...APPROVALS_CHUNKED' does not exist or not authorized
-- even though the grant succeeded and SHOW GRANTS TO SHARE lists the object.
-- Do not read that as a broken grant. Trigger the refresh -- note it takes TWO
-- arguments; the single-argument form fails with "expected 2, got 1".
SELECT SYSTEM$TRIGGER_LISTING_REFRESH('LISTING', 'ETH_U256_PRIVATE');
--   Successfully triggered refresh for LISTING '...' in 1 region(s).
-- Measured: visible on the consumer roughly 30 seconds later.
