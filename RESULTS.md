# Measured results

Every number here was produced by executing the scripts in `sql/`. Nothing is estimated.

**Environment.** Provider: a Snowflake demo account in `AWS_US_EAST_1`, Business Critical.
Consumer: a separate account in `AWS_US_WEST_2` — different region, real full account, not a
reader account. Warehouse: X-Small class. Iceberg format version 2, Snowflake-managed storage on
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
`is_dynamic = Y`. `SHOW ICEBERG TABLES` reported both as `MANAGED`, format version 2.

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
