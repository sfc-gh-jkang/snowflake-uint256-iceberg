# Measured results

Every number here was produced by executing the scripts in `sql/`. Nothing is estimated.

**Environment.** Provider: a Snowflake demo account in `AWS_US_EAST_1`, Business Critical.
Consumer: a separate account in `AWS_US_WEST_2` — different region, real full account, not a
reader account. Warehouse: X-Small class. Iceberg format version 3, Snowflake-managed storage on
an external volume. Measured 2026-08-28.

**Caveat on the benchmarks.** Single account pair, single warehouse size, synthetic data. Useful
for relative comparison between approaches; not a published benchmark.

---

## 1. `fixed(32)` holds `uint256` exactly

Byte width and value capacity, decoded natively where the hex format model is safe:

| `fixed(L)` | bits | bytes stored | exact max value | digits |
|---|---|---|---|---|
| `fixed(1)` | 8 | 1 | 255 | 3 |
| `fixed(2)` | 16 | 2 | 65,535 | 5 |
| `fixed(4)` | 32 | 4 | 4,294,967,295 | 10 |
| `fixed(8)` | 64 | 8 | 18,446,744,073,709,551,615 | 20 |
| `fixed(12)` | 96 | 12 | 79,228,162,514,264,337,593,543,950,335 | 29 |
| `fixed(16)` | 128 | 16 | *exceeds `NUMBER(38,0)`* | 39 |
| `fixed(32)` | 256 | 32 | 2²⁵⁶−1 | 78 |

`bytes stored` equalled `L` on every row. The last two rows show the **decoder** failing, not the
storage — the 32 bytes hold the value perfectly; converting it to a Snowflake `NUMBER` is what
cannot work above ~126 bits.

Hex round-trip of `uint256_max` through `BINARY(32)` returned byte-identical output:

```
FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF
```

## 2. Byte order equals unsigned numeric order

`ORDER BY` applied directly to the `BINARY(32)` column, no decode:

```
zero         0000000000000000000000000000000000000000000000000000000000000000
one          0000000000000000000000000000000000000000000000000000000000000001
two          0000000000000000000000000000000000000000000000000000000000000002
2^128        0000000000000000000000000000000100000000000000000000000000000000
uint256_max  FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF
```

Correct ordering. This is what makes `MIN`/`MAX`, range predicates and clustering keys work at
full fidelity with no conversion.

## 3. `DECFLOAT` is not a substitute

`DECFLOAT` carries the magnitude but only 38 significant digits, so it is lossy for `uint256`:

```
uint256_max + 1  →  1.1579208923731619542357098500868790785e77   (unchanged)
```

Adding one produced no change. Do not use it for exact values.

## 4. `SUM` overflows before you expect it to

Ten rows of `99999999999999999999999999999999999999` (38 digits, each individually valid):

```
SUM(v) → Value overflow in a SUM aggregate
```

Individual values fitting `NUMBER(38,0)` does not mean their sum does.

## 5. The `TRY_TO_DECIMAL` silent-corruption trap

| input width | result | correct? |
|---|---|---|
| 32-bit (8 hex) | `4294967295` | yes |
| 64-bit (16 hex) | `18446744073709551615` | yes |
| 96-bit (24 hex) | `79228162514264337593543950335` | yes |
| **128-bit (32 hex)** | **`-1`** | **no — silently wrong** |
| 256-bit (64 hex) | `NULL` | safe |

A `TRY_` function returning `-1` instead of `NULL` is the worst failure mode available. Safe only
to 96 bits.

## 6. Decode and aggregation performance

5,000,000 full-range `uint256` values, same warm warehouse, two passes each:

| Approach | exec pass 1 | exec pass 2 |
|---|---|---|
| Native SQL hex decode (≤96-bit only) | 0.42s | — |
| **Python UDAF, exact 256-bit `SUM`** | **2.92s** | **3.34s** |
| Scalar Python UDF decode | 3.32s | 4.23s |
| Vectorized UDF (`.map`) | 6.45s | 6.52s |
| Vectorized UDF (list comprehension, 50k batch) | 6.41s | 6.41s |

**Vectorized is consistently ~2× slower than scalar**, and rewriting it properly changed nothing
(6.406 → 6.407 — stable, not noise). There is no numpy dtype for 256-bit integers, so `int(h,16)`
is a Python-object loop either way; the vectorized path adds DataFrame construction and Arrow
marshalling on top of the identical loop.

The UDAF is fastest for aggregation because state is a single Python `int` and Snowflake
parallelises through `merge()`.

The 0.42s native figure is **not like-for-like** — it is the "value fits in 96 bits" path, not a
full 256-bit decode.

## 7. Provider-side dynamic Iceberg table

200,000 rows, 5% seeded as infinite approvals:

| metric | value |
|---|---|
| total rows | 200,000 |
| infinite approvals | 10,000 |
| `exceeds_number38` | 10,000 |
| token-decodable | 190,000 |
| max `value_dec_exact` length | 78 digits |
| `SUM(value_token)` excl. infinite | 442,761,746.715663 |

`exceeds_number38` matching `infinite_approvals` exactly is the correctness signal: the
out-of-range set is precisely the unlimited-allowance set, nothing else.

## 8. Cross-account, cross-region share

`AWS_US_EAST_1` → private listing → `AWS_US_WEST_2`, entirely from SQL.

**Fidelity preserved.** Consumer-side `DESC TABLE` after replication:

| column | provider | consumer |
|---|---|---|
| `value_raw` | `fixed(32)` | `BINARY(32)` |
| `token_addr` | `fixed(20)` | `BINARY(20)` |

Both tables arrived with `is_iceberg = Y`, 200,000 rows each; the derived table also
`is_dynamic = Y`. `SHOW ICEBERG TABLES` reported both as `MANAGED`, format version 3.

**Consumer-side streams and dynamic tables on the share.** Because both shared tables are
`ICEBERG_VERSION = 3`, the consumer can build its own pipeline objects directly on the shared
Iceberg table — not supported on v2. Verified on the consumer account:

| Object created by the consumer | On | Result |
|---|---|---|
| `STREAM ... ON TABLE ETH_U256_CONSUMED.ETH_SHARE.APPROVALS_RAW` | shared v3 Iceberg table | created |
| `DYNAMIC TABLE` selecting from the same shared table | shared v3 Iceberg table | created, returned **exactly 10,000 rows** |

The dynamic table filtered `value_raw >= 0xFF..FF` and returned 10,000 rows, matching the
infinite-approval count in the source data — so it computed the right answer, not merely created
successfully. Note `ON ICEBERG TABLE` is not valid stream syntax; it is `ON TABLE` regardless of
the source table's type.

**Fulfillment sequence.** `SYSTEM$TRIGGER_LISTING_REFRESH` → `in 0 region(s)`;
`is_ready_for_import: false`; `CREATE DATABASE … FROM LISTING` →
`Listing … is not fulfilled to your current region`. After
`SYSTEM$REQUEST_LISTING_AND_WAIT(…, 0)`, `is_ready_for_import` flipped to `true` in under
30 minutes and the import succeeded.

## 9. Four independent aggregation methods agree

All computed on the shared cross-region data:

| Method | Total wei, excluding infinite approvals |
|---|---|
| Provider dynamic Iceberg table (`value_token`) | `442761746.715663` (token units) |
| Consumer pure-SQL limb decode | `442761746715760125099787987` |
| Consumer Python UDAF | `442761746715760125099787987` |
| Shared secure SQL UDF | `442761746715760125099787987` |

Three exact-wei methods identical to the digit, and the token-unit figure matched the provider
value exactly. A consumer's SQL limb arithmetic and a consumer's Python big-int arithmetic
agreeing exactly is the strongest correctness evidence available here.

## 10. Full 256-bit exact sum

Including all 10,000 infinite approvals, computed by a **consumer-owned** Python UDAF over the
**shared, cross-region** column:

```
1157920892373161954235709850086879078532699846656405640837337586794891421499137987
```

**82 digits.** Independently recomputed as `10000 × (2²⁵⁶−1) + 442761746715760125099787987` —
exact match. No `NUMBER` type on any engine can hold this, which is the point: it is genuine
256-bit arithmetic, executed by the consumer, over shared data, across regions.

## 11. Share grant matrix

| Object | Grantable to a share? |
|---|---|
| `ICEBERG TABLE` (Snowflake-managed) | yes |
| `DYNAMIC ICEBERG TABLE` | yes |
| Secure SQL UDF | yes |
| Non-secure SQL UDF | no — needs `SECURE` |
| **Python UDF / UDAF** | **no** — `Python UDFs may not be shared` |

## 12. You cannot `SUM` the exact decimal string

Section 3 established that `DECFLOAT` is not exact. This is the other half: the exact decimal
string is lossless at rest but is not an aggregation path.

```sql
SELECT SUM(TO_DECIMAL(value_dec_exact,38,0)) FROM <shared>.APPROVALS_DECODED;
-- 100038 (22018): Numeric value
-- '115792089237316195423570985008687907853269984665640564039457584007913129639935'
-- is not recognized
```

And the pre-scaled `decimal(38,18)` column is `NULL` on precisely the rows that matter:

| Measure | 200,000-row set |
|---|---|
| Rows | 200,000 |
| `EXCEEDS_NUMBER38` | 10,000 |
| Pre-scaled column `NULL` | 10,000 |
| `MAX(LENGTH(value_dec_exact))` | 78 |

So `SUM` on the decimal column **silently skips** the infinite approvals, while `SUM` on the exact
string **errors outright**. Neither is a usable default, which is what section 13 fixes.

## 13. Exact `uint256` aggregation in pure SQL, including `GROUP BY`

Materialise the value as four base-10^20 chunks of `decimal(38,0)` (`sql/06`). Consumers then
`SUM` four ordinary numeric columns, and because the base is a power of ten the four subtotals
reassemble into the exact total **inside SQL** via `MOD` and string concatenation (`sql/07`).

Base-2^64 limbs also work but reassembly then needs big-integer arithmetic outside SQL — fine for
one grand total, useless for a `GROUP BY`. **The base is chosen for the aggregation, not the decode.**

Headroom: a chunk `SUM` gains one digit per power of ten of row count, so `decimal(38,0)` carries
roughly 10^18 rows. At 10^7 rows the widest chunk sum observed was 27 digits.

Validated against the Python UDAF (a real big-integer implementation) on 10,000,000 rows:

| Grouping | Groups | Matching | Mismatched | Max digits |
|---|---|---|---|---|
| by token | 2,000 | 2,000 | 0 | 81 |
| high cardinality | 999,934 | 999,934 | 0 | 81 |
| by token, **off the shared Iceberg table**, cross-region | 12 | 12 | 0 | 81 |

Matched on the largest total *and* on a length checksum across every group, not just on a spot check.

### Performance, XSMALL warehouse, 10,000,000 rows

| Aggregation | 2,000 groups | 1,000,000 groups |
|---|---|---|
| **Exact, pure SQL carry** | **304 ms** | **1,171 ms** |
| Python UDAF (exact, not shareable) | 5,296 ms | 29,202 ms |
| `DECFLOAT` (approximate) | 1,079 ms | — |

**The gap widens with group count** — 17× at 2,000 groups, 25× at 1,000,000. The pure-SQL path is
both the exact one and the fast one, which is the opposite of the usual trade-off.

Provider-side decode of all 10,000,000 rows to produce the chunk columns: **13 s on a MEDIUM**, once
per refresh.

Reproduce with `scripts/bench_scale.sql`. It builds the table, asserts exactness at both group
counts, prints this table and drops everything including its warehouse. Timings move run to run --
a second run gave 420 / 5,639 / 1,251 ms for the three methods at 2,000 groups. The ordering and
the order of magnitude hold; treat the individual numbers as indicative.

At 200,000 rows the ordering is different and misleading — `DECFLOAT` 139 ms vs limb `SUM` 155 ms vs
UDAF 1,412 ms. Do not size this decision on a small table.

## 14. The `FLOOR(t / base)` carry trap

Found while building section 13, and it is the most dangerous thing in this repository because the
failure rate is low enough to pass a small test.

Snowflake division returns a **scale-6 result and rounds rather than truncates**
([arithmetic operators](https://docs.snowflake.com/en/sql-reference/operators-arithmetic)), so
`FLOOR` of a quotient sitting just under an integer overshoots:

```sql
SELECT 399999999999999999999999999 / 100000000000000000000;
-- 4000000.000000        -- rounded up from 3999999.99999999999999999999
SELECT FLOOR(399999999999999999999999999 / 100000000000000000000);
-- 4000000               -- WRONG. True floor is 3999999.
```

The carry lands one too high and the reassembled total is out by exactly 10^20. Correct form:

```sql
SELECT (399999999999999999999999999 - MOD(399999999999999999999999999,100000000000000000000))
       / 100000000000000000000;
-- 3999999               -- exact: the numerator is a whole multiple of the base
```

| Carry expression | Groups | Mismatched |
|---|---|---|
| `FLOOR(t / base)` | 999,934 | **1** |
| `(t - MOD(t,base)) / base` | 999,934 | 0 |

**One group in 999,934.** The same code passed a 2,000-group test cleanly while it was wrong. If you
modify the carry arithmetic, validate against a big-integer implementation over at least a million
groups.

## 15. Iceberg mechanics for the chunk columns

`decimal(38,0)` is inside the Iceberg spec's own 38-digit cap, so the chunks are ordinary Iceberg
decimals readable by Spark or Trino with no custom type. `GET_DDL` on the created table shows
Iceberg-native type names:

```
TX_HASH FIXED(32), VALUE_RAW FIXED(32), VALUE_DEC_EXACT STRING,
D0 DECIMAL(38, 0), D1 DECIMAL(38, 0), D2 DECIMAL(38, 0), D3 DECIMAL(38, 0)
```

with real metadata on the provider's own external volume:

```
s3://<bucket>/eth_u256_approvals_chunked.<suffix>/metadata/00001-....metadata.json
```

Verified in all four shapes: plain Iceberg table (200,000 rows), **dynamic** Iceberg table
(200,000 rows), cross-region share (`FIXED(32)` → `BINARY(32)`, `DECIMAL(38,0)` → `NUMBER(38,0)`,
unchanged), and exact aggregation executed by the consumer off the shared Iceberg table.

Two mechanics that cost time:

**Iceberg DDL wants `FIXED(32)`, not `BINARY(32)`.**

```
099209 (42601): For Iceberg tables, only max length (67,108,864) is supported for
'BINARY(L)'. Alternatively, use BINARY directly.
```

Do not follow that suggestion literally — unqualified `BINARY` gives an unbounded binary, not a
32-byte fixed. Use `FIXED(32)`. `DESC TABLE` afterwards reports it back as `BINARY(32)`.

**Adding a table to an existing cross-region listing is not immediate.** The consumer returns
`002003 ... does not exist or not authorized` even though the grant succeeded and
`SHOW GRANTS TO SHARE` lists the object. That is replication lag, not a broken grant. Trigger
`SYSTEM$TRIGGER_LISTING_REFRESH('LISTING','<name>')` and it appeared roughly 30 seconds later.

## 16. Degenerate groups, and what the chunk path does *not* solve

Found by deliberately constructing the cases, not by the headline validation — no group in either
the 200,000-row or 10,000,000-row set summed to zero, so a million-group run would not have caught
this either.

`LTRIM(<all zeros>, '0')` returns the **empty string**, not `'0'`, and an all-NULL group returns
`NULL`. The UDAF returns `'0'` for both, so either case reports as a spurious mismatch:

| Group | Before fix | After fix | UDAF |
|---|---|---|---|
| total = 0 | `''` | `'0'` | `'0'` |
| all values NULL | `NULL` | `'0'` | `'0'` |
| total = 1 | `'1'` | `'1'` | `'1'` |

Fix is `COALESCE(NULLIF(LTRIM(...,'0'),''),'0')`. Applied in `sql/07` and `scripts/bench_scale.sql`.

**NULL handling is otherwise consistent.** `SUM` skips NULL chunks and the UDAF skips `None`, so a
partially-NULL group agrees between the two without special handling.

**`DECFLOAT` was stable across the runs tested** — four repeated full-table sums returned one
distinct value. That is weak evidence: decimal-float addition is order-sensitive when magnitudes
differ wildly and parallel aggregation order is not guaranteed, so this should not be read as a
determinism guarantee.

### Not solved by this approach

| Aggregate | Status |
|---|---|
| `SUM` | exact, pure SQL, per group |
| `MIN` / `MAX` | exact, works directly on raw `fixed(32)` bytes, no decode |
| `COUNT` | trivially fine |
| **`AVG`** | **not solved** — `SUM` decomposes across chunks, division does not |
| `MEDIAN`, percentiles | not solved, same reason |

`AVG` needs the exact total divided by the count, and an 80-digit dividend is not expressible in
`decimal(38,0)`. Three workarounds, none implemented here: accept `DECFLOAT`'s 38 significant
digits, restrict `AVG` to rows known to fit 38 digits, or divide client-side from the exact total
and the count. The README claim that "only `SUM` and `AVG` need a number" is accurate about *which*
aggregates need a decode, but only `SUM` has an exact full-range answer in this repo.

## 17. Systematic boundary testing of the chunk arithmetic (`sql/08`)

Two bugs, two different discovery methods, and neither method would have found the other:

| Bug | Class | Found by | Would volume have found it? |
|---|---|---|---|
| `FLOOR(t/base)` carry | distribution | 999,934 groups | yes, and only at volume |
| zero-total → `''` | degenerate input | constructed case | **no** — no group ever summed to 0 |

So `sql/08` enumerates the input space instead of sampling it. **43 groups / 205 rows**, each totalled
by the chunk SQL and independently by the Python UDAF, with expected values also computed offline in
Python — a three-way check. Result: **43/43 agreement, 0 mismatches.**

Coverage, chosen from where the boundaries actually are (powers of 10^20, since the chunks are
base-10^20 — *not* powers of two):

| Class | Cases |
|---|---|
| Single boundary values | 0, 1, 2, 9, 10, 10^20±1, 10^40±1, 10^60±1, 2^64±1, 2^128±1, 2^192±1, 2^255, 2^256−2, 2^256−1 |
| Carry exactly at the base | chunk sum = base−1 (no carry), = base (carry, remainder 0), = base+1 (carry, remainder 1) |
| Multi-unit carry | seven rows of base−1 → carry of 6 |
| **Cascading carry** | 10^60−1 has d1=d2=d3=base−1, so two such rows cascade a carry d3→d2→d1→d0 |
| Interior zero chunks | 10^40+1 (d2=0), 10^60+1 (d1=d2=0) |
| Widest totals | 100 × (2^256−1) = **80 digits** |
| Degenerate | total=0, all rows NULL, partially NULL |

The cascading-carry case is the one worth stealing: a single value whose lower three chunks are all
`base−1` turns one addition into a three-step carry chain, and it is not a case anyone guesses.

### Limits of the math, derived rather than measured

- A chunk holds < 10^20; `decimal(38,0)` holds < 10^38. So chunk sums overflow at roughly **10^18
  rows**. Not testable directly — stated as arithmetic, not as a measurement.
- The top chunk is emitted unpadded, so output width is not fixed: at 10^7 rows the widest possible
  total is ~85 digits and the assembly handles it. Verified to 80 digits.
- `d0 < 10^18`, not 10^20, because `LPAD` to 80 of a 78-digit number always leaves two leading zeros.

### Harness bug worth recording

Group names contain an uppercase letter (`d3_sum_eq_B`). A `grep -E '[a-z_0-9]+'` over the output
silently dropped exactly those three rows and looked like missing data — and they were the three
carry-at-exactly-base cases, i.e. the most important ones. **A verification harness that filters its
own input can manufacture a false alarm as easily as a false pass.** Same lesson as the `COUNT(*)`
aggregate-pruning trap in `scripts/bench_scale.sql`.

## 18. Incremental refresh through the two-level dynamic-table chain

`APPROVALS_RAW` (Iceberg) → `APPROVALS_DECODED` (dynamic, decode) → `APPROVALS_CHUNKED_DT`
(dynamic, chunk columns). Appending to the base table and refreshing both levels was verified to
propagate correctly, and — more importantly — the chunk values were checked against the known
inputs rather than only the row counts.

Three rows appended, chosen to exercise the boundaries rather than to be representative:

| `log_index` | value | why this value |
|---|---|---|
| 7001 | 2^256−1 | maximum, 78 digits |
| 7002 | **0** | degenerate zero (the case section 16 fixed) |
| 7003 | **10^20** | lands exactly on a chunk boundary |

Both levels went 200,000 → 200,003 incrementally, and the materialised chunks were exact:

```
7001  d0=115792089237316195  d1=42357098500868790785  d2=32699846656405640394  d3=57584007913129639935
7002  d0=0                   d1=0                     d2=0                     d3=0
7003  d0=0                   d1=0                     d2=1                     d3=0
```

`7003` is the informative one: 10^20 correctly becomes `d2=1, d3=0` — one whole unit of the base
with a zero remainder. And `7001` confirms the derived bound that **`d0 < 10^18`**, not 10^20,
because `LPAD` to 80 of a 78-digit value always leaves two leading zeros (`d0` is 18 digits).

The exact aggregation then ran over the refreshed dynamic table for that token group and returned
the independently Python-computed total, to the digit:

```
115792089237316195423570985008687907853269984665640564039557584007913129639935   (78 digits)
```

So the chunk pipeline is correct on **newly arrived** data, not just on the seeded set — including
a zero row and a chunk-boundary row flowing through a dynamic table rather than being constructed
in a test harness. Test rows were then deleted and all three tables verified back to 200,000.
