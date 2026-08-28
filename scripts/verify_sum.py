#!/usr/bin/env python3
"""Independently recompute the exact 256-bit sum from RESULTS.md section 10.

The point of this script is that it does not touch Snowflake. It recomputes the
82-digit total from first principles so the SQL result can be checked against
something with no shared code path.

Usage:
    python3 scripts/verify_sum.py
    python3 scripts/verify_sum.py --infinite 10000 --rest 442761746715760125099787987
"""
import argparse
import sys

# Measured on the provider dynamic Iceberg table, 200,000 synthetic rows.
DEFAULT_INFINITE = 10_000
DEFAULT_REST = 442_761_746_715_760_125_099_787_987

# Reported by the consumer-owned Python UDAF over the shared cross-region column.
REPORTED = int(
    "1157920892373161954235709850086879078532699846656405640"
    "837337586794891421499137987"
)


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--infinite", type=int, default=DEFAULT_INFINITE,
                   help="count of infinite-approval rows (value 2**256-1)")
    p.add_argument("--rest", type=int, default=DEFAULT_REST,
                   help="exact wei sum of all other rows")
    p.add_argument("--reported", type=int, default=REPORTED,
                   help="the value the UDAF returned, to check against")
    args = p.parse_args()

    u256_max = 2**256 - 1
    expected = args.infinite * u256_max + args.rest

    print(f"uint256 max            : {u256_max}")
    print(f"infinite-approval rows : {args.infinite}")
    print(f"  x uint256 max        : {args.infinite * u256_max}")
    print(f"+ sum of other rows    : {args.rest}")
    print()
    print(f"expected               : {expected}")
    print(f"reported by UDAF       : {args.reported}")
    print(f"digits                 : {len(str(expected))}")
    print()

    if expected == args.reported:
        print("MATCH")
        return 0

    print("MISMATCH")
    print(f"  difference           : {args.reported - expected}")
    return 1


if __name__ == "__main__":
    sys.exit(main())
