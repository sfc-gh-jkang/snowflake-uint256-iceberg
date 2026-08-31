# Portability testing

What has been run, where, and what remains untested. A demo that works on the account it was
built on is not a portable demo — the build account already satisfies every dependency, so it
cannot reveal any of them.

## Matrix

| Account | Cloud / region | Role | Scripts run | Date | Outcome |
|---|---|---|---|---|---|
| provider A | AWS `us-east-1` | ACCOUNTADMIN | 01–04 | 2026-08-28 | pass (build account) |
| consumer A | AWS `us-west-2` | ACCOUNTADMIN | 05 | 2026-08-28 | pass, cross-region |
| **provider B** | **Azure `eastus2`** | ACCOUNTADMIN | 01–03, 99 | 2026-08-28 | **pass after 5 fixes** |
| **provider C** | **GCP `us-east4`** | ACCOUNTADMIN | 01–03, 99 | 2026-08-28 | **pass, zero new defects** |

Providers B and C were genuine cold starts: fresh `git clone`, rendered from `.env`, torn down,
then set up again. Five defects surfaced on Azure that the AWS build account could not have shown.
GCP then passed first try, which is the evidence that those five fixes were the right ones rather
than Azure-specific patches.

### Cross-cloud result

All three clouds reproduced the same figures **exactly**, from a fresh clone:

| metric | AWS `us-east-1` | Azure `eastus2` | GCP `us-east4` |
|---|---|---|---|
| rows | 200,000 | 200,000 | 200,000 |
| infinite approvals | 10,000 | 10,000 | 10,000 |
| `exceeds_number38` | 10,000 | 10,000 | 10,000 |
| token-decodable | 190,000 | 190,000 | 190,000 |
| max exact digits | 78 | 78 | 78 |
| `SUM(value_token)` | 442761746.715663 | 442761746.715663 | 442761746.715663 |
| full 256-bit sum (82 digits) | `…891421499137987` | identical | identical |

`fixed(32)` behaves identically on all three, including the byte-ordering property.

**Storage caveat on GCP.** That account's external volume is **S3-backed**
(`s3://…`, `us-west-2`), not GCS. So the GCP run exercised GCP *compute* against cross-cloud
storage, and did **not** exercise a GCS-backed volume. That distinction matters, because GCS
volumes carry their own IAM prerequisite — see the untested list below.

### Idempotency

On both Azure and GCP: teardown → setup → verify → teardown → setup again. Every second setup
returned all-zero exit codes and identical figures, and the final teardown left a residual schema
count of zero. `CREATE OR REPLACE` on the Iceberg and dynamic Iceberg tables is safe to re-run —
Snowflake appends a unique suffix to `BASE_LOCATION`, so re-creating after a drop does not collide.

## Findings

Split by class, because they have different fixes.

| # | Class | Finding | Fix |
|---|---|---|---|
| 1 | account state | `01` failed with `No active warehouse selected`. The build account's connection had a default warehouse; this one did not. | Every script now begins `USE WAREHOUSE`. |
| 2 | account state | `02` failed with `391528 An active warehouse is required for creating Python UDFs`. Non-obvious — the statement is pure DDL. | Same fix, applied to **all** scripts rather than the ones guessed to need it. |
| 3 | cascade | `03` failed with `Unknown function … U256_DEC`. Not independent — a consequence of `02` failing. | Resolved by fixing 2. |
| 4 | **genuine bug** | `99_teardown.sql` aborted on `ALTER LISTING … UNPUBLISH` when no listing existed, so **the schema was never dropped**. Verified via `INFORMATION_SCHEMA.SCHEMATA` that the schema survived a "completed" teardown. Anyone running only `01`–`03` could never clean up. | Listing and share drops wrapped in `EXCEPTION WHEN OTHER THEN NULL`. |
| 5 | **genuine bug** | The wrapped blocks then failed with `unexpected '<EOF>'` — the snow CLI splits input on `;`, cutting `BEGIN … END;` at the first inner semicolon. | Wrapped in `EXECUTE IMMEDIATE $$ … $$`. |

Findings 1 and 2 are the classic shape: an invisible dependency on build-account state. Neither
could have been found without an account whose connection lacks a default warehouse.

## Untested surfaces

Stated plainly, because these are the honest gaps:

- **A second person cloning and running it.** Everything above ran on the machine that holds the
  `.env`, the connections and the credentials. That is the single largest remaining gap.
- **`04` and `05` on Azure or GCP.** The share and cross-region listing were exercised only on the
  AWS pair. Azure and GCP covered `01`–`03` and `99`.
- **Cross-cloud auto-fulfillment** (Azure → AWS, GCP → AWS). Only same-cloud, cross-region
  (AWS → AWS) was proven.
- **A GCS-backed external volume.** The GCP run used an S3-backed volume, so this is still open.
  It is the case most likely to bite: the Snowflake service account needs `storage.buckets.get`
  in addition to object permissions, `roles/storage.objectAdmin` does **not** include it, and
  `SYSTEM$VERIFY_EXTERNAL_VOLUME` reports success without it while `CREATE ICEBERG TABLE` hangs
  on `Query needs to be retried to setup external volume`. Grant `roles/storage.admin` or a
  custom role carrying `storage.buckets.get`.
- **Iceberg v3 on Azure and GCP.** The v3 rebuild and the consumer-side stream and dynamic table
  were exercised on the AWS pair only. Azure and GCP ran v3 DDL but no share.
- **A reader account as consumer.** Reader accounts cannot run DML and are not supported with
  organizational listings; whether they can create a UDF is unverified.
- **Scale.** 200,000 rows on all three clouds. The 5,000,000-row and 10,000,000-row performance
  figures in RESULTS.md were measured on the AWS account only.
- **`06` and `07` on Azure or GCP.** The chunk columns and the pure-SQL exact aggregation were
  exercised on the AWS pair only: static and dynamic Iceberg table, cross-region share, and the
  consumer running the exact `GROUP BY` off the shared table. The arithmetic is engine-side and
  cloud-independent, but that is reasoning, not a measurement.
- **The chunk columns read by an external engine.** `GET_DDL` reports Iceberg-native
  `DECIMAL(38, 0)` and real `metadata.json` exists on the external volume, so Spark or Trino
  *should* read them as ordinary decimals. No external engine has actually been pointed at them.
  This is the weakest open-format claim in the repo.
- **Chunk-sum overflow at ~10^18 rows.** Derived arithmetically (chunk < 10^20, decimal(38,0)
  < 10^38); not reachable on any test dataset, so it is reasoning rather than a measurement.
- ~~**`AVG`, `MEDIAN` and percentiles over the full 256-bit range.**~~ **VERIFIED exact and pure
  SQL** — see RESULTS.md section 19 and `sql/09`. The previous "not solved" claim here was reasoned,
  not measured, and was wrong.
- **`DECFLOAT` determinism.** Four repeated sums agreed, which is not enough to claim
  determinism for an order-sensitive decimal-float aggregate.
- ~~An incremental refresh of the chunked dynamic table.~~ **VERIFIED** — see RESULTS.md section 18.
- **A consumer-side stream or dynamic table on `APPROVALS_CHUNKED`.** Proven on
  `APPROVALS_RAW`, not re-proven on the chunked table.
- **Teardown with the chunked table present in the share.** `99_teardown.sql` drops the schema
  `CASCADE`, which should cover it, but has not been run since `06` added a shared table.
- **`bench_scale.sql` on a non-AWS account.** Ran clean end to end on the AWS consumer
  (10,000,000 rows, 0 mismatches at 2,000 and 999,934 groups, self-cleaned). Untested elsewhere.
