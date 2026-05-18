#!/usr/bin/env bash
# sync_atlas_to_repo.sh
# Mirrors the Manuscript Portfolio Atlas markdown from the user's Desktop
# into the THYROID_2026 repo at docs/atlas/MANUSCRIPT_PORTFOLIO_ATLAS.md
# and pushes the change to GitHub. Only commits if the Atlas file actually
# changed; only touches that one file (other dirty paths in the repo are
# left alone — they are committed by their own workflows).
#
# Triggered by launchd:
#   - WatchPaths on the source Atlas markdown (event-driven)
#   - StartCalendarInterval daily at 06:00 (safety net if WatchPaths missed)
#
# Logs to ~/Library/Logs/thyroid-atlas-mirror.log

set -euo pipefail

# --- Config ---
SOURCE="/Users/lgm5maxmac/Desktop/Finalized manuscript drafts/MANUSCRIPT_PORTFOLIO_ATLAS.md"
REPO="/Users/lgm5maxmac/code/THYROID_2026"
DEST_REL="docs/atlas/MANUSCRIPT_PORTFOLIO_ATLAS.md"
DEST="$REPO/$DEST_REL"
LOG="$HOME/Library/Logs/thyroid-atlas-mirror.log"

# --- PATH for launchd (it strips PATH by default) ---
export PATH="/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin:$PATH"

# --- Logging helpers ---
log() {
  printf '[%s] %s\n' "$(date -u +'%Y-%m-%dT%H:%M:%SZ')" "$*" | tee -a "$LOG" >&2
}

mkdir -p "$(dirname "$LOG")"
mkdir -p "$(dirname "$DEST")"

log "atlas-mirror: starting"

# --- Sanity checks ---
if [[ ! -f "$SOURCE" ]]; then
  log "atlas-mirror: source file missing at $SOURCE — abort"
  exit 0
fi

if [[ ! -d "$REPO/.git" ]]; then
  log "atlas-mirror: repo missing at $REPO — abort"
  exit 1
fi

cd "$REPO"

# --- Copy the file (only if different) ---
if [[ -f "$DEST" ]] && diff -q "$SOURCE" "$DEST" >/dev/null 2>&1; then
  log "atlas-mirror: no content change, nothing to do"
  exit 0
fi

cp -p "$SOURCE" "$DEST"
log "atlas-mirror: copied $SOURCE -> $DEST"

# --- Stage ONLY the atlas mirror file (don't touch other dirty paths) ---
git add -- "$DEST_REL"

# --- If staging produced nothing (e.g., file unchanged after copy), bail ---
if git diff --cached --quiet -- "$DEST_REL"; then
  log "atlas-mirror: no staged change for $DEST_REL — nothing to commit"
  exit 0
fi

# --- Commit ---
TS=$(date -u +'%Y-%m-%dT%H:%M:%SZ')
git \
  -c user.name="Atlas Mirror" \
  -c user.email="atlas-mirror@noreply.local" \
  commit -m "[atlas-mirror] sync $TS

Mirrored from /Users/lgm5maxmac/Desktop/Finalized manuscript drafts/MANUSCRIPT_PORTFOLIO_ATLAS.md
Triggered by launchd job com.logan.thyroid-atlas-mirror." \
  -- "$DEST_REL" >>"$LOG" 2>&1

log "atlas-mirror: committed (sha $(git rev-parse --short HEAD))"

# --- Push ---
if git push origin main >>"$LOG" 2>&1; then
  log "atlas-mirror: pushed to origin/main"
else
  log "atlas-mirror: push FAILED — commit is local; will retry on next change"
  exit 2
fi

log "atlas-mirror: done"
