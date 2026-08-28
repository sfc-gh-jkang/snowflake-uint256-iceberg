# Ethereum `uint256` on Snowflake, in Apache Iceberg

How to store EVM `uint256` values in Apache Iceberg tables with **zero loss**, share them
across accounts and regions, and let consumers aggregate them — including the values that no
`NUMBER` type on any engine can hold.

Everything here was executed end to end. The numbers in [RESULTS.md](RESULTS.md) are measured,
not estimated, and each carries its provenance. [TESTING.md](TESTING.md) records the account matrix
it has been run on, the defects a cold start surfaced, and what remains untested.

## The problem

`uint256` is 256 bits, which is **78 decimal digits**:

```
115792089237316195423570985008687907853269984665640564039457584007913129639935
```

Two independent ceilings sit below that:

- Snowflake `NUMBER` caps at **38 digits** (~126 bits) —
  [numeric data types](https://docs.snowflake.com/en/sql-reference/data-types-numeric)
- The Apache Iceberg spec caps `decimal(P,S)` at **P ≤ 38** —
  [Iceberg data types](https://docs.snowflake.com/en/user-guide/tables-iceberg-data-types)

So this is **not a Snowflake limitation**. No numeric type anywhere in the Iceberg ecosystem —
Snowflake, Spark, Trino — can hold a full `uint256`. Anyone hitting this on Snowflake would hit
it identically on Spark.

## The answer: `fixed(32)`

Iceberg's `fixed(32)` is 32 bytes. 32 × 8 = **256 bits exactly** — the same width as an EVM
word, bit for bit. Not a conversion, not an approximation.

The confusion is that Iceberg's numeric types measure *decimal digits* while `fixed(L)` measures
*bytes*, and binary is a far denser encoding. Capacity multiplies by 256 per byte:

| `fixed(L)` | bits | max value | decimal digits needed |
|---|---|---|---|
| `fixed(8)` | 64 | 18,446,744,073,709,551,615 | 20 |
| `fixed(12)` | 96 | 79,228,162,514,264,337,593,543,950,335 | 29 |
| `fixed(16)` | 128 | *(exceeds `NUMBER`)* | 39 |
| **`fixed(32)`** | **256** | **2²⁵⁶−1** | **78** |

Storage footprint, for the same value:

```
as decimal string  → 78 bytes
as hex string      → 64 bytes
as fixed(32)       → 32 bytes   ← and sortable
```

### The property that makes it practical

**Big-endian bytes sort in unsigned numeric order.** Verified — `ORDER BY` directly on the
binary yields `0 < 1 < 2 < 2^128 < 2^256−1`.

So at full 256-bit fidelity with **no decode at all**: `ORDER BY`, `MIN`/`MAX`, range predicates,
equality, joins, dedup, and **clustering keys**. Only additive aggregation needs decoding.

## Why this matters specifically for Ethereum

Split `uint256` columns three ways:

**Quantities** — almost always fit `decimal(38,0)`. All ETH ever is ~1.2 × 10²⁶ wei = 27 digits,
leaving 11 orders of magnitude of headroom.

**Infinite approvals — these genuinely need `fixed(32)`, and they are routine.**
`approve(spender, 2**256-1)` is the standard unlimited-allowance idiom, so `Approval` events
carry literal 2²⁵⁶−1 as normal traffic. A schema capped at 38 digits corrupts or nulls **every
one of them** — and those are exactly the rows a risk or security analyst cares about. This
single fact is the argument for `fixed(32)`.

**Identifiers** — `tx_hash`, `block_hash`, storage slots are `bytes32` → `fixed(32)`; addresses
are `fixed(20)`. Never summed, so they cost nothing.

## Design

Three representations of the same value, in one Iceberg table:

| Column | Iceberg type | Role |
|---|---|---|
| `value_raw` | `fixed(32)` | System of record. 256 bits, zero loss. |
| `value_dec_exact` | `string` | Exact full-precision decimal, materialised provider-side. |
| `value_token` | `decimal(38,18)` | Pre-scaled. What most consumers query. |

Plus two derived flags computed with no decode: `is_infinite_approval` (a binary comparison) and
`exceeds_number38` (honest range flag rather than a silently wrong number).

### The constraint that shapes everything

**Python UDFs cannot be granted to a share** — `GRANT USAGE ON FUNCTION … TO SHARE` fails with
`Python UDFs may not be shared`. Secure *SQL* UDFs share fine; Python does not, at all.

So the decode cannot live in the consumer's query. It runs provider-side inside a
**`DYNAMIC ICEBERG TABLE`**, and the share stays open-format Iceberg throughout:

```
ICEBERG TABLE (fixed(32))                  ← ingest, full fidelity, your external volume
        ↓  DYNAMIC ICEBERG TABLE refresh — Python UDF runs HERE, provider-side
ICEBERG TABLE (raw + exact decimal + pre-scaled + flags)
        ↓  GRANT SELECT … TO SHARE  →  private listing (cross-region)
consumer: plain SQL, no UDF. External engines still read the same Parquet.
```

This is better than exposing a function anyway: the decode is paid once per refresh rather than
by every consumer on every query.

## Three consumer paths, all verified cross-region

| Path | What the consumer writes | Setup needed |
|---|---|---|
| **1** | `SUM(value_token)` on the shared column | none |
| **2** | Pure-SQL limb decode on raw `fixed(32)`, or the shared secure SQL UDF | none |
| **3** | Their own Python UDAF for full 256-bit exact sums | own UDF |

Path 3 works because the restriction is on creating objects *inside* the read-only imported
database, and on *granting* Python to a share — neither blocks a consumer-owned function reading
a shared column.

## Claim → evidence matrix

Every headline claim, with how it was verified. See [RESULTS.md](RESULTS.md) for the values.

| Claim | Evidence |
|---|---|
| `fixed(32)` holds `uint256` exactly | 32 bytes stored confirmed via `LENGTH()`; hex round-trip byte-identical for 2²⁵⁶−1 |
| Byte order = numeric order | `ORDER BY` on `BINARY(32)` returned `0 < 1 < 2 < 2^128 < max` |
| Objects are genuinely Iceberg | `SHOW ICEBERG TABLES` reports both as `MANAGED`, format version 2, on the external volume |
| Decode is exact to 78 digits | `value_dec_exact` max length 78; equals 2²⁵⁶−1 for infinite approvals |
| Cross-region share preserves fidelity | Consumer `DESC TABLE` shows `BINARY(32)` / `BINARY(20)` after replication |
| All aggregation paths agree | Four independent methods returned the identical exact wei total |
| Full 256-bit arithmetic really happens | 82-digit sum, independently recomputed in Python; exact match |
| Python UDFs cannot be shared | `GRANT USAGE ON FUNCTION … TO SHARE` → `Python UDFs may not be shared` |
| Cross-region is fully scriptable | `SYSTEM$REQUEST_LISTING_AND_WAIT` then `CREATE DATABASE … FROM LISTING`, no UI |

## Gotchas worth knowing before you start

**`BINARY(32)` is rejected in Iceberg DDL.** You must write the Iceberg type name:

```
CREATE ICEBERG TABLE … value_raw BINARY(32)
→ For Iceberg tables, only max length (67,108,864) is supported for 'BINARY(L)'
```

Use `fixed(32)`. `DESC TABLE` then reports it as `BINARY(32)`.

**`TRY_TO_DECIMAL` with a hex format model silently corrupts above 96 bits.** This is the
sharpest trap here — a `TRY_` function returning a *wrong value* instead of `NULL`:

| input width | result |
|---|---|
| 32-bit (8 hex) | `4294967295` ✓ |
| 64-bit (16 hex) | `18446744073709551615` ✓ |
| 96-bit (24 hex) | `79228162514264337593543950335` ✓ |
| **128-bit (32 hex)** | **`-1`** ✗ |
| 256-bit (64 hex) | `NULL` |

Only use it with a hard `≤ 24 hex chars` guard. The limb decode in this repo works in 16-char
chunks for exactly this reason.

**Cross-region fulfillment is demand-driven.** `SYSTEM$TRIGGER_LISTING_REFRESH` returning
`in 0 region(s)` means the listing has never been fulfilled anywhere — that function only
refreshes *already-fulfilled* regions, so it cannot bootstrap the first one. The consumer must
call `SYSTEM$REQUEST_LISTING_AND_WAIT`, which is
[documented as usable when `is_ready_for_import` is FALSE](https://docs.snowflake.com/en/sql-reference/stored-procedures/system_request_listing_and_wait).

**`refresh_schedule_override: TRUE` is required** whenever another listing already exists on the
same database — even when your schedule matches theirs exactly.

**Vectorized UDFs are slower here, not faster.** Measured ~2× slower than a scalar UDF, and
rewriting the vectorized version properly changed nothing. There is no numpy dtype for 256-bit
integers, so it is a Python-object loop either way and the batching is pure overhead. Use the
UDAF for aggregation instead.

**Consider Iceberg v3.** Per
[auto-fulfillment with open table formats](https://docs.snowflake.com/en/collaboration/use-auto-fulfillment-with-open-table-formats),
streams and dynamic tables on shared Iceberg **v2** tables are not supported, while **v3** is. If
consumers will build their own dynamic tables on the share, create these as v3. The tables in
this test were v2, so the consumer-side dynamic-table path is **untested here**.

## Running it

Scripts use `<PLACEHOLDER>` tokens rather than real object names. Render them for your own
account:

```bash
cp .env.example .env    # then edit
./scripts/render.sh     # writes ./rendered
```

`render.sh` refuses to run if any required variable is unset or empty, and fails if any
placeholder survives into executable SQL. That matters: substituting an empty string produces
SQL that *looks* fine and is not — `CREATE SCHEMA IF NOT EXISTS .;` and `..APPROVALS_RAW` — and
you would get a confusing Snowflake error rather than a clear local one. Do not hand-roll the
substitution with a `sed` loop.

Then, from `rendered/`:

| Step | Where | Notes |
|---|---|---|
| `01` | provider | Iceberg table + 200k synthetic rows |
| `02` | provider | Python UDF/UDAF + shareable secure SQL UDF |
| `03` | provider | dynamic Iceberg decode table |
| `04` | provider | share + private listing. Read `global_name` from `DESCRIBE LISTING` |
| — | — | put that `global_name` in `.env` as `LISTING_GLOBAL_NAME`, re-run `render.sh` |
| `05` | consumer | request, import, and the three aggregation paths |
| `99` | both | teardown |

`LISTING_GLOBAL_NAME` is unknown until `04` has run, so the first render warns about it rather
than failing. That is expected.

You need an **external volume** the provider account can write to, and for the cross-region path,
`MANAGE LISTING AUTO FULFILLMENT` on the provider account.

If that volume is **GCS-backed**, the Snowflake service account needs `storage.buckets.get` in
addition to object permissions. `roles/storage.objectAdmin` does not include it, and
`SYSTEM$VERIFY_EXTERNAL_VOLUME` reports success without it while `CREATE ICEBERG TABLE` hangs on
`Query needs to be retried to setup external volume`. Grant `roles/storage.admin`, or a custom
role carrying `storage.buckets.get`. This repo has been run on AWS, Azure and GCP compute — see
[TESTING.md](TESTING.md) — but not yet against a GCS-backed volume.

`99_teardown.sql` is real and verified. Note that a **published listing cannot be dropped** —
unpublish first, and the listing must go before the share.

## Limitations and provenance

- Data is **synthetic**, generated by `01_provider_iceberg_table.sql`. Shapes and magnitudes
  mirror ERC-20 `Approval` events; the values are not real chain data.
- Benchmarks were measured on the author's own Snowflake demo account on an X-Small–class
  warehouse, one region pair (`AWS_US_EAST_1` → `AWS_US_WEST_2`), at the row counts stated in
  RESULTS.md. Treat them as order-of-magnitude, not as a published benchmark.
- Iceberg format version 2. The consumer-side streams/dynamic-tables path is untested.
- Cross-Cloud Auto-Fulfillment replicates an external-volume Iceberg table as a
  **Snowflake-managed** Iceberg table in the target region, and the provider is billed for
  egress, storage and compute.

## Repository Owner

- **Owner:** John Kang (john.kang@snowflake.com / [@sfc-gh-jkang](https://github.com/sfc-gh-jkang))
- **Access requests:** Email the owner, or open an issue
- **License:** Apache-2.0
