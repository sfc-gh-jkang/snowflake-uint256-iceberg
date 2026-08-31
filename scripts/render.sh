#!/usr/bin/env bash
# Render the SQL templates with your own object names.
#
# The scripts in sql/ carry <PLACEHOLDER> tokens rather than real identifiers.
# This script substitutes them from .env and FAILS LOUDLY if anything is missing,
# because an empty substitution produces SQL that looks fine and is not
# (e.g. "CREATE SCHEMA IF NOT EXISTS .;").
#
# Usage:
#   cp .env.example .env    # then edit
#   ./scripts/render.sh [outdir]     # default outdir: ./rendered

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUTDIR="${1:-$REPO_ROOT/rendered}"
ENV_FILE="$REPO_ROOT/.env"

if [[ ! -f "$ENV_FILE" ]]; then
  echo "ERROR: $ENV_FILE not found. Run: cp .env.example .env  (then edit it)" >&2
  exit 1
fi

# shellcheck disable=SC1090
set -a; source "$ENV_FILE"; set +a

REQUIRED=(DB SCHEMA WAREHOUSE EXTERNAL_VOLUME CONSUMER_ACCOUNT CONSUMER_WAREHOUSE CONSUMER_DB CONSUMER_SCHEMA)

missing=()
for v in "${REQUIRED[@]}"; do
  if [[ -z "${!v:-}" ]]; then missing+=("$v"); fi
done
if (( ${#missing[@]} > 0 )); then
  echo "ERROR: these variables are unset or empty in .env: ${missing[*]}" >&2
  echo "Refusing to render, because an empty value silently corrupts the SQL." >&2
  exit 1
fi

# LISTING_GLOBAL_NAME is only known after 04 runs, so it is optional on the first pass.
# When empty we deliberately LEAVE the token in place rather than substituting an empty
# string, so 05 cannot be run with CALL ...('' , 0).
LISTING_GLOBAL_NAME="${LISTING_GLOBAL_NAME:-}"

mkdir -p "$OUTDIR"

for f in "$REPO_ROOT"/sql/*.sql "$REPO_ROOT"/scripts/*.sql; do
  [[ -e "$f" ]] || continue
  base="$(basename "$f")"
  sed -e "s|<DB>|$DB|g" \
      -e "s|<SCHEMA>|$SCHEMA|g" \
      -e "s|<WAREHOUSE>|$WAREHOUSE|g" \
      -e "s|<EXTERNAL_VOLUME>|$EXTERNAL_VOLUME|g" \
      -e "s|<CONSUMER_ACCOUNT>|$CONSUMER_ACCOUNT|g" \
      -e "s|<CONSUMER_WAREHOUSE>|$CONSUMER_WAREHOUSE|g" \
      -e "s|<CONSUMER_DB>|$CONSUMER_DB|g" \
      -e "s|<CONSUMER_SCHEMA>|$CONSUMER_SCHEMA|g" \
      "$f" > "$OUTDIR/$base"
  if [[ -n "$LISTING_GLOBAL_NAME" ]]; then
    sed -i.bak -e "s|<LISTING_GLOBAL_NAME>|$LISTING_GLOBAL_NAME|g" "$OUTDIR/$base"
    rm -f "$OUTDIR/$base.bak"
  fi
done

# Guard. Any surviving <TOKEN> in executable SQL means an unsubstituted placeholder.
# Comment lines are excluded so the explanatory header in 01 does not trip this.
fail=0
for f in "$OUTDIR"/*.sql; do
  leftover="$(grep -vE '^\s*--' "$f" | grep -oE '<[A-Z_]+>' | sort -u || true)"
  # LISTING_GLOBAL_NAME is legitimately unknown until 04 has run, so warn rather than fail.
  if [[ -n "$leftover" ]] && echo "$leftover" | grep -q '^<LISTING_GLOBAL_NAME>$'; then
    echo "WARN: $(basename "$f") awaits LISTING_GLOBAL_NAME — set it in .env after 04, then re-render."
  fi
  hard="$(echo "$leftover" | grep -v '^<LISTING_GLOBAL_NAME>$' || true)"
  if [[ -n "$hard" ]]; then
    echo "ERROR: $(basename "$f") still contains: $(echo "$hard" | tr '\n' ' ')" >&2
    fail=1
  fi
done

# Catch the empty-substitution signature too: '..IDENT' or a bare '.' object ref.
for f in "$OUTDIR"/*.sql; do
  if grep -vE '^\s*--' "$f" | grep -qE '\.\.[A-Z_]|NOT EXISTS \.\s*;'; then
    echo "ERROR: $(basename "$f") has an empty-identifier artifact (e.g. '..TABLE')." >&2
    fail=1
  fi
done

if (( fail )); then
  echo "Render FAILED. Nothing in $OUTDIR should be run." >&2
  exit 1
fi

echo "Rendered $(ls -1 "$OUTDIR"/*.sql | wc -l | tr -d ' ') files to $OUTDIR"
if [[ -z "$LISTING_GLOBAL_NAME" ]]; then
  echo "Note: LISTING_GLOBAL_NAME was empty (expected on first run)."
  echo "      After 04 runs, put the global_name in .env and re-render for 05."
fi
