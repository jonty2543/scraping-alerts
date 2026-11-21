#!/usr/bin/env bash
# Run from the existing scheduler. Dry-run by default; pass --write to persist.
set -euo pipefail

repo_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
state_dir="${NBA_STATE_DIR:-${repo_dir}/.nba-run}"
python_bin="${NBA_PYTHON:-${repo_dir}/.venv/bin/python}"
if [[ ! -x "$python_bin" ]]; then
    python_bin="$(command -v python3)"
fi
if ! command -v flock >/dev/null 2>&1; then
    echo "Missing flock. On Amazon Linux: sudo yum install -y util-linux" >&2
    exit 2
fi
mkdir -p -- "$state_dir"
exec 9>"${state_dir}/nba_props.lock"
if ! flock -n 9; then
    echo "NBA scrape already running; skipped overlapping invocation."
    exit 0
fi

# Exclude EC2-blocked Sportsbet, unverified SurgeBet, and access-denied Picklebet.
read -r -a bookmakers <<< "${NBA_BOOKMAKERS:-Pointsbet Unibet Palmerbet Betright Betr TAB Betdeluxe}"
scraper_command=("$python_bin" "${repo_dir}/nba_scraper.py" \
    --headed-browser --bookmakers "${bookmakers[@]}" \
    --output "${state_dir}/latest.json" "$@")

if [[ -n "${DISPLAY:-}" ]]; then
    exec "${scraper_command[@]}"
fi
if command -v xvfb-run >/dev/null 2>&1 && command -v xauth >/dev/null 2>&1; then
    exec xvfb-run -a "${scraper_command[@]}"
fi
if ! command -v Xvfb >/dev/null 2>&1 || ! command -v xauth >/dev/null 2>&1; then
    echo "Missing virtual display packages. On Amazon Linux run:" >&2
    echo "  sudo yum install -y xorg-x11-server-Xvfb xorg-x11-xauth util-linux" >&2
    exit 2
fi

# Amazon Linux ships Xvfb without Debian's xvfb-run wrapper. Start a private,
# authenticated display, and keep this shell alive to clean it up on exit.
umask 077
display_dir="$(mktemp -d "${state_dir}/xvfb.XXXXXX")"
xvfb_pid=""
cleanup_display() {
    if [[ -n "$xvfb_pid" ]]; then
        kill "$xvfb_pid" 2>/dev/null || true
        wait "$xvfb_pid" 2>/dev/null || true
    fi
    rm -rf -- "$display_dir"
}
trap cleanup_display EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

display_number=""
for ((candidate = 99; candidate < 200; candidate++)); do
    if [[ ! -e "/tmp/.X${candidate}-lock" && ! -e "/tmp/.X11-unix/X${candidate}" ]]; then
        display_number="$candidate"
        break
    fi
done
if [[ -z "$display_number" ]]; then
    echo "No free X display found in :99 through :199." >&2
    exit 2
fi
export DISPLAY=":${display_number}"
export XAUTHORITY="${display_dir}/Xauthority"
touch "$XAUTHORITY"
cookie="$("$python_bin" -c 'import secrets; print(secrets.token_hex(16))')"
xauth -f "$XAUTHORITY" add "$DISPLAY" MIT-MAGIC-COOKIE-1 "$cookie"
Xvfb "$DISPLAY" -screen 0 1280x720x24 -nolisten tcp -auth "$XAUTHORITY" \
    >"${state_dir}/xvfb.log" 2>&1 &
xvfb_pid=$!
display_ready=false
for ((attempt = 0; attempt < 50; attempt++)); do
    if ! kill -0 "$xvfb_pid" 2>/dev/null; then
        break
    fi
    if [[ -S "/tmp/.X11-unix/X${display_number}" ]]; then
        display_ready=true
        break
    fi
    sleep 0.1
done
if [[ "$display_ready" != true ]]; then
    echo "Xvfb failed to start; see ${state_dir}/xvfb.log" >&2
    exit 2
fi
"${scraper_command[@]}"
