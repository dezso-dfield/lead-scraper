#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
#  Lead Scraper — One-click setup for macOS / Linux
# ─────────────────────────────────────────────────────────────────────────────
set -e

BOLD='\033[1m'
GREEN='\033[0;32m'
CYAN='\033[0;36m'
YELLOW='\033[0;33m'
RED='\033[0;31m'
RESET='\033[0m'

echo ""
echo -e "${BOLD}╔══════════════════════════════════════╗${RESET}"
echo -e "${BOLD}║      Lead Scraper  —  Setup          ║${RESET}"
echo -e "${BOLD}╚══════════════════════════════════════╝${RESET}"
echo ""

# ── 1. Python check ──────────────────────────────────────────────────────────
echo -e "${CYAN}[1/5] Checking Python…${RESET}"
if command -v python3 &>/dev/null; then
    PY=$(python3 --version 2>&1)
    echo -e "      ${GREEN}✓ Found ${PY}${RESET}"
    PYTHON=python3
elif command -v python &>/dev/null; then
    PY=$(python --version 2>&1)
    echo -e "      ${GREEN}✓ Found ${PY}${RESET}"
    PYTHON=python
else
    echo -e "      ${RED}✗ Python 3.10+ not found.${RESET}"
    echo ""
    echo "  Please install Python from https://python.org/downloads"
    exit 1
fi

# Check version >= 3.10
PYVER=$($PYTHON -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')")
PYMAJ=$($PYTHON -c "import sys; print(sys.version_info.major)")
PYMIN=$($PYTHON -c "import sys; print(sys.version_info.minor)")
if [ "$PYMAJ" -lt 3 ] || { [ "$PYMAJ" -eq 3 ] && [ "$PYMIN" -lt 10 ]; }; then
    echo -e "      ${RED}✗ Python 3.10+ required, found ${PYVER}${RESET}"
    exit 1
fi

# ── 2. Virtual environment ────────────────────────────────────────────────────
echo -e "${CYAN}[2/5] Setting up virtual environment…${RESET}"
if [ ! -d ".venv" ]; then
    $PYTHON -m venv .venv
    echo -e "      ${GREEN}✓ Created .venv${RESET}"
else
    echo -e "      ${GREEN}✓ .venv already exists${RESET}"
fi

# Activate
source .venv/bin/activate
PIP=".venv/bin/pip"

# ── 3. Install dependencies ───────────────────────────────────────────────────
echo -e "${CYAN}[3/5] Installing dependencies…${RESET}"
$PIP install --upgrade pip -q
$PIP install -r requirements.txt -q
$PIP install -e . -q
echo -e "      ${GREEN}✓ All packages installed${RESET}"

# ── 4. Optional Playwright ────────────────────────────────────────────────────
echo -e "${CYAN}[4/5] Optional: Google Maps (Playwright)${RESET}"
echo -e "      ${YELLOW}Playwright adds ~300MB Chromium download for Google Maps scraping.${RESET}"
read -p "      Install Playwright? [y/N] " -n 1 -r
echo ""
if [[ $REPLY =~ ^[Yy]$ ]]; then
    $PIP install playwright -q
    .venv/bin/playwright install chromium --with-deps
    echo -e "      ${GREEN}✓ Playwright installed${RESET}"
else
    echo -e "      ${YELLOW}Skipped. Run later: pip install playwright && playwright install chromium${RESET}"
fi

# ── 5. Launch ─────────────────────────────────────────────────────────────────
echo -e "${CYAN}[5/5] Done!${RESET}"
echo ""
echo -e "${BOLD}╔══════════════════════════════════════════════╗${RESET}"
echo -e "${BOLD}║  Setup complete! How to start:               ║${RESET}"
echo -e "${BOLD}║                                              ║${RESET}"
echo -e "${BOLD}║  Web UI (browser):  ./start.sh               ║${RESET}"
echo -e "${BOLD}║  Terminal TUI:      ./start.sh --tui         ║${RESET}"
echo -e "${BOLD}║  CLI scrape:        ./start.sh \"query\" -l loc ║${RESET}"
echo -e "${BOLD}╚══════════════════════════════════════════════╝${RESET}"
echo ""

# Create start script
cat > start.sh << 'STARTEOF'
#!/usr/bin/env bash
source .venv/bin/activate
python -m scraper "$@"
STARTEOF
chmod +x start.sh

echo -e "${GREEN}Starting web UI now…${RESET}"
echo ""
.venv/bin/python -m scraper
