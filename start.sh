#!/bin/bash
# ============================================================
# start.sh — RunPod auto-start script for AI-Finance MCP server
# ============================================================
# Set this as the "Start Command" in your RunPod pod settings.
# On first boot: git clone + install deps + start server.
# On restarts: deps already installed, just starts the server.
#
# Usage (manual): bash /workspace/start.sh
# Usage (RunPod):  Set pod start command to: bash /workspace/start.sh
# ============================================================

set -e

REPO_URL="https://github.com/AtharvaMaskarQuanthm/ai-finance-mcp.git"
WORKSPACE="/workspace"
APP_DIR="$WORKSPACE/AI-Finance"
LOG_FILE="$WORKSPACE/mcp_server.log"
PORT=8000

echo "========================================"
echo "  AI-Finance MCP Server — Start Script  "
echo "========================================"

# ── Step 1: Clone repo if not already present ──────────────
if [ ! -d "$APP_DIR" ]; then
  echo "[1/4] Cloning repo..."
  git clone "$REPO_URL" "$APP_DIR"
else
  echo "[1/4] Repo already present — pulling latest..."
  cd "$APP_DIR" && git pull --ff-only || echo "  (git pull skipped — local changes present)"
fi

cd "$APP_DIR"

# ── Step 2: Check .env exists (must be created manually once) ──
if [ ! -f ".env" ]; then
  echo ""
  echo "ERROR: .env file not found at $APP_DIR/.env"
  echo "Please create it manually:"
  echo "  nano $APP_DIR/.env"
  echo "See .env.example for the required variables."
  echo ""
  exit 1
fi

# ── Step 3: Install Python dependencies ────────────────────
echo "[2/4] Installing dependencies..."
python3 -m pip install -r requirements.txt

echo "  Verifying mcp import..."
python3 -c "from mcp.server.fastmcp import FastMCP; print('  mcp OK')" || {
  echo "ERROR: mcp import failed. Python: $(which python3) $(python3 --version)"
  echo "  Installed packages:"
  python3 -m pip list | grep -i mcp
  exit 1
}

# ── Step 4: Start the MCP server ───────────────────────────
echo "[3/4] Killing any existing MCP server process..."
pkill -f "mcp_server.py" 2>/dev/null || true
sleep 1

echo "[4/4] Starting MCP server on port $PORT..."
nohup python3 mcp_server.py --transport sse --port $PORT \
  > "$LOG_FILE" 2>&1 &

MCP_PID=$!
sleep 2

# Verify it started
if kill -0 $MCP_PID 2>/dev/null; then
  echo ""
  echo "  MCP server running (PID $MCP_PID)"
  echo "  Logs: $LOG_FILE"
  echo "  URL:  https://YOUR_POD_ID-${PORT}.proxy.runpod.net"
  echo ""
  echo "  Claude Desktop config:"
  echo "  {"
  echo "    \"mcpServers\": {"
  echo "      \"ai-finance\": {"
  echo "        \"url\": \"https://YOUR_POD_ID-${PORT}.proxy.runpod.net/sse\""
  echo "      }"
  echo "    }"
  echo "  }"
else
  echo "ERROR: Server failed to start. Check logs: $LOG_FILE"
  tail -20 "$LOG_FILE"
  exit 1
fi
