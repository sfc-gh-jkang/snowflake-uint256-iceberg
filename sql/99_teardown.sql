-- 99 - Teardown
--   <DB> <SCHEMA> <WAREHOUSE> <CONSUMER_DB> <CONSUMER_SCHEMA>
--
-- Every step is tolerant of the object not existing, because a partial run is the
-- normal case: someone who only ran 01-03 has no share and no listing, and a
-- teardown that aborts on the first missing object silently leaves the schema
-- behind. That is worse than no teardown at all, since you believe you cleaned up.
--
-- snow CLI stops at the first failing statement, which is why the listing and share
-- steps are wrapped rather than written as bare DDL.

USE WAREHOUSE <WAREHOUSE>;

-- Consumer side. These already tolerate absence.
DROP DATABASE IF EXISTS <CONSUMER_DB>;
DROP SCHEMA   IF EXISTS <CONSUMER_SCHEMA> CASCADE;

-- Provider side, in dependency order: listing, then share, then objects.
-- A PUBLISHED listing cannot be dropped, so it must be unpublished first, and
-- neither statement supports IF EXISTS.
-- GOTCHA: the snow CLI splits input on ';', so a bare BEGIN ... END; block gets cut
-- at the first inner semicolon and fails with "unexpected '<EOF>'". Wrapping the
-- block in $$ ... $$ inside EXECUTE IMMEDIATE keeps it intact.
EXECUTE IMMEDIATE $$
BEGIN
  ALTER LISTING ETH_U256_PRIVATE UNPUBLISH;
EXCEPTION
  WHEN OTHER THEN NULL;   -- not published, or does not exist
END;
$$;

EXECUTE IMMEDIATE $$
BEGIN
  DROP LISTING ETH_U256_PRIVATE;
EXCEPTION
  WHEN OTHER THEN NULL;   -- never created
END;
$$;

EXECUTE IMMEDIATE $$
BEGIN
  DROP SHARE ETH_U256_SHARE;
EXCEPTION
  WHEN OTHER THEN NULL;   -- never created
END;
$$;

DROP SCHEMA IF EXISTS <DB>.<SCHEMA> CASCADE;

-- Verify. Use SHOW, not ACCOUNT_USAGE -- the latter lags hours and will report
-- stale state right after a drop. All three should return zero rows.
SHOW SCHEMAS  LIKE '<SCHEMA>' IN DATABASE <DB>;
SHOW SHARES   LIKE 'ETH_U256_SHARE';
SHOW LISTINGS LIKE 'ETH_U256_PRIVATE';
