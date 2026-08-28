-- 02 - Provider: decode functions
--   <DB> <SCHEMA>
--
-- Python has arbitrary-precision ints, so uint256 arithmetic is exact and free there.
-- IMPORTANT: Python UDFs CANNOT be granted to a share ("Python UDFs may not be shared").
-- They run provider-side only. See 03 for how their output reaches consumers.

-- Scalar decode: fixed(32) -> exact decimal string.
USE WAREHOUSE <WAREHOUSE>;

CREATE OR REPLACE FUNCTION <DB>.<SCHEMA>.U256_DEC(B BINARY)
RETURNS VARCHAR
LANGUAGE PYTHON RUNTIME_VERSION='3.11' HANDLER='f'
AS $$
def f(b):
    return None if b is None else str(int.from_bytes(b, 'big'))
$$;

-- Exact aggregate. Faster than per-row decode: state is one Python int and
-- Snowflake parallelises via merge().
CREATE OR REPLACE AGGREGATE FUNCTION <DB>.<SCHEMA>.U256_SUM(B BINARY)
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

-- A SECURE SQL UDF *can* be shared, unlike Python. Hand this to consumers so they
-- do not have to write the limb decode themselves.
-- NOTE the guard: returns NULL rather than a wrong number when the value exceeds
-- NUMBER(38,0). See README "The silent-corruption trap".
CREATE OR REPLACE SECURE FUNCTION <DB>.<SCHEMA>.U256_TO_NUMBER(B BINARY)
RETURNS NUMBER(38,0)
LANGUAGE SQL
AS
$$
  WITH h AS (SELECT TO_VARCHAR(B,'HEX') AS x)
  SELECT CASE
           WHEN TRY_TO_DECIMAL(SUBSTR(x, 1,16), REPEAT('X',16),38,0) = 0
            AND TRY_TO_DECIMAL(SUBSTR(x,17,16), REPEAT('X',16),38,0) = 0
            AND TRY_TO_DECIMAL(SUBSTR(x,33,16), REPEAT('X',16),38,0) < 5000000000000000000
           THEN TRY_TO_DECIMAL(SUBSTR(x,33,16), REPEAT('X',16),38,0) * 18446744073709551616
              + TRY_TO_DECIMAL(SUBSTR(x,49,16), REPEAT('X',16),38,0)
         END
  FROM h
$$;
