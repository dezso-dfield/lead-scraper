#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
#  Lead Scraper — Update script (macOS / Linux)
#  Pulls latest code from GitHub and reinstalls dependencies if needed.
# ─────────────────────────────────────────────────────────────────────────────
set -e

BOLD='\033[1m'
GREEN='\033[0;32m'
CYAN='\033[0;36m'
YELLOW='\033[0;33m'
RED='\033[0;31m'
RESET='\033[0m'
REPO_URL="https://github.com/dezso-dfield/lead-scraper"
BRANCH="main"

echo ""
echo -e "${BOLD}╔══════════════════════════════════════╗${RESET}"
echo -e "${BOLD}║      Lead Scraper  —  Updater        ║${RESET}"
echo -e "${BOLD}╚══════════════════════════════════════╝${RESET}"
echo ""

# ── Git check ─────────────────────────────────────────────────────────────────
if ! command -v git &>/dev/null; then
    echo -e "${RED}✗ git not found. Please install Git.${RESET}"
    exit 1
fi

# ── Ensure we're inside the repo ─────────────────────────────────────────────
if [ ! -d ".git" ]; then
    echo -e "${RED}✗ Run this script from the lead-scraper directory.${RESET}"
    exit 1
fi

# ── Fetch remote info ─────────────────────────────────────────────────────────
echo -e "${CYAN}[1/4] Checking for updates…${RESET}"
git fetch origin "$BRANCH" --quiet

LOCAL=$(git rev-parse HEAD)
REMOTE=$(git rev-parse "origin/$BRANCH")

if [ "$LOCAL" = "$REMOTE" ]; then
    echo -e "      ${GREEN}✓ Already up to date.${RESET}"
    echo ""
    CURRENT_VER=$(grep '^version' pyproject.toml 2>/dev/null | head -1 | sed 's/.*= *"\(.*\)"/\1/')
    echo -e "  Current version: ${BOLD}${CURRENT_VER:-unknown}${RESET}"
    echo -e "  Repository:      ${CYAN}${REPO_URL}${RESET}"
    echo ""
    exit 0
fi

# Show what changed
COMMITS_BEHIND=$(git rev-list --count HEAD..origin/"$BRANCH")
echo -e "      ${YELLOW}⬆  ${COMMITS_BEHIND} new commit(s) available${RESET}"
echo ""
echo -e "  ${BOLD}Changelog:${RESET}"
git log --oneline HEAD..origin/"$BRANCH" | sed 's/^/    • /'
echo ""

# Warn about local changes
if ! git diff --quiet || ! git diff --cached --quiet; then
    echo -e "  ${YELLOW}⚠  You have local uncommitted changes.${RESET}"
    read -p "  Continue and stash them? [y/N] " -n 1 -r
    echo ""
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        git stash push -m "lead-scraper update stash $(date +%Y%m%d-%H%M%S)"
        echo -e "      ${GREEN}✓ Changes stashed${RESET}"
        STASHED=true
    else
        echo -e "  ${RED}Aborted.${RESET}"
        exit 1
    fi
fi

# ── Pull ──────────────────────────────────────────────────────────────────────
echo -e "${CYAN}[2/4] Pulling latest code…${RESET}"
git pull origin "$BRANCH" --quiet
echo -e "      ${GREEN}✓ Code updated${RESET}"

# ── Restore stash ─────────────────────────────────────────────────────────────
if [ "${STASHED:-false}" = "true" ]; then
    git stash pop || echo -e "  ${YELLOW}⚠  Stash apply had conflicts — resolve manually with: git stash pop${RESET}"
fi

# ── Reinstall deps if requirements or pyproject changed ──────────────────────
echo -e "${CYAN}[3/4] Checking dependencies…${RESET}"
CHANGED=$(git diff HEAD@{1} HEAD --name-only 2>/dev/null | grep -E 'requirements\.txt|pyproject\.toml' || true)

if [ -n "$CHANGED" ]; then
    echo -e "      ${YELLOW}Dependencies file changed — reinstalling…${RESET}"
    if [ -f ".venv/bin/pip" ]; then
        .venv/bin/pip install -r requirements.txt -q
        .venv/bin/pip install -e . -q
        echo -e "      ${GREEN}✓ Dependencies updated${RESET}"
    else
        echo -e "      ${YELLOW}Virtual env not found — run setup.sh first.${RESET}"
    fi
else
    echo -e "      ${GREEN}✓ No dependency changes${RESET}"
fi

# ── Done ──────────────────────────────────────────────────────────────────────
echo -e "${CYAN}[4/4] Done!${RESET}"
echo ""
NEW_VER=$(grep '^version' pyproject.toml 2>/dev/null | head -1 | sed 's/.*= *"\(.*\)"/\1/')
echo -e "${GREEN}${BOLD}✓ Lead Scraper updated to v${NEW_VER:-latest}${RESET}"
echo ""
echo -e "  Run ${BOLD}./start.sh${RESET} to launch."
echo ""
