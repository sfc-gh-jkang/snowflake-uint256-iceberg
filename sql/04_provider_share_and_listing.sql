-- 04 - Provider: share + cross-region private listing
--   <DB> <SCHEMA> <CONSUMER_ACCOUNT>
--
-- Grant order matters. Database, then schema, then objects.

USE WAREHOUSE <WAREHOUSE>;

CREATE OR REPLACE SHARE ETH_U256_SHARE
  COMMENT = 'Ethereum uint256 Iceberg pattern';

GRANT USAGE ON DATABASE <DB>                    TO SHARE ETH_U256_SHARE;
GRANT USAGE ON SCHEMA   <DB>.<SCHEMA>           TO SHARE ETH_U256_SHARE;

GRANT SELECT ON ICEBERG TABLE <DB>.<SCHEMA>.APPROVALS_RAW      TO SHARE ETH_U256_SHARE;
GRANT SELECT ON DYNAMIC TABLE <DB>.<SCHEMA>.APPROVALS_DECODED  TO SHARE ETH_U256_SHARE;

-- A SECURE SQL UDF can be shared. A Python UDF cannot.
GRANT USAGE ON FUNCTION <DB>.<SCHEMA>.U256_TO_NUMBER(BINARY)   TO SHARE ETH_U256_SHARE;

-- Same-region consumers can stop here and use ALTER SHARE ... ADD ACCOUNTS.
-- Cross-region requires a listing with auto-fulfillment.
--
-- GOTCHA: refresh_schedule_override is REQUIRED whenever another listing already
-- exists on the same database, even when your schedule matches theirs exactly.
CREATE EXTERNAL LISTING ETH_U256_PRIVATE
SHARE ETH_U256_SHARE AS
$$
title: "Ethereum uint256 Iceberg Pattern"
subtitle: "fixed(32) approvals with decoded aggregation columns"
description: |
  Apache Iceberg tables holding uint256 values as fixed(32), plus a dynamic
  Iceberg table carrying provider-side decoded exact decimal and pre-scaled
  token columns.
listing_terms:
  type: "OFFLINE"
targets:
  accounts: ["<CONSUMER_ACCOUNT>"]
auto_fulfillment:
  refresh_type: "SUB_DATABASE"
  refresh_schedule: "1440 MINUTE"
  refresh_schedule_override: TRUE
$$
PUBLISH = TRUE;

-- Diagnostic. "in 0 region(s)" means the listing has never been fulfilled anywhere.
-- This function only refreshes ALREADY-fulfilled regions, so it cannot bootstrap
-- the first one. Fulfillment is demand-driven -- the consumer must request it (05).
SELECT SYSTEM$TRIGGER_LISTING_REFRESH('LISTING', 'ETH_U256_PRIVATE');

DESCRIBE LISTING ETH_U256_PRIVATE;   -- read global_name for the consumer step
