#!/usr/bin/env bash
# Run from the existing scheduler. Dry-run by default; pass --write to persist.
set -euo pipefail

repo_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
state_dir="${NBA_STATE_DIR:-${repo_dir}/.nba-run}"
python_bin="${NBA_PYTHON:-${repo_dir}/.venv/bin/python}"
if [[ ! -x "$python_bin" ]]; then
    python_bin="$(command -v python3)"
fi
for dependency in xvfb-run flock; do
    if ! command -v "$dependency" >/dev/null 2>&1; then
        echo "Missing ${dependency}; install Xvfb/xauth and util-linux on the EC2 host." >&2
        exit 2
    fi
done
mkdir -p -- "$state_dir"
exec 9>"${state_dir}/nba_props.lock"
if ! flock -n 9; then
    echo "NBA scrape already running; skipped overlapping invocation."
    exit 0
fi

# Exclude EC2-blocked Sportsbet, unverified SurgeBet, and access-denied Picklebet.
read -r -a bookmakers <<< "${NBA_BOOKMAKERS:-Pointsbet Unibet Palmerbet Betright Betr TAB Betdeluxe}"
exec xvfb-run -a "$python_bin" "${repo_dir}/nba_scraper.py" \
    --headed-browser --bookmakers "${bookmakers[@]}" \
    --output "${state_dir}/latest.json" "$@"
