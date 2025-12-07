#!/bin/bash
echo "=== Launching Python microservices ==="

PYTHON_SCRIPTS=(
    "uv run market_data_collector_service.py"
    "uv run data_storage_service.py"
    "uv run signal_service.py"
    "uv run trading_engine_service.py"
)

# Launch each script in a new macOS Terminal window
for script in "${PYTHON_SCRIPTS[@]}"; do
    echo "Launching: $script"

    osascript <<EOF
tell application "Terminal"
    do script "cd $(pwd); $script"
end tell
EOF

done

echo "=== All Python services launched in separate Terminal windows ==="
