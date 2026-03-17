#!/bin/bash
# Start the API server

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Check Python dependencies
if ! python3 -c "import fastapi, uvicorn" 2>/dev/null; then
    echo "Installing dependencies..."
    pip3 install fastapi uvicorn pydantic
fi

echo "Starting API server on http://localhost:8000"
echo "Open http://localhost:8000 in your browser"
echo ""

cd "$SCRIPT_DIR"
python3 api_server.py
