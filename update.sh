#!/bin/bash
# update.sh - Automatic update script
# Add this to crontab: 0 * * * * /path/to/distributed-encodes/update.sh >> /tmp/update.log 2>&1

# Navigate to the project directory (directory of this script)
cd "$(dirname "$0")" || exit

# Fetch latest changes without merging
git fetch origin

# Check if there are updates
LOCAL=$(git rev-parse HEAD)
REMOTE=$(git rev-parse @{u})

if [ "$LOCAL" != "$REMOTE" ]; then
    echo "[$(date)] Update found. Pulling changes..."
    git pull origin main

    # Install/Update dependencies
    if [ -f requirements.txt ]; then
        pip3 install -r requirements.txt
    fi

    # Restart the service (Adjust if using a different service name or system/user scope)
    # Using 'systemctl --user' is recommended for non-root deployments
    if systemctl --user is-active --quiet distributed-encodes.service; then
        systemctl --user restart distributed-encodes.service
        echo "[$(date)] Service restarted successfully."
    else
        # Fallback for system-level service or if not running
        echo "[$(date)] Service not running or restart command failed. Please check manually."
    fi
else
    echo "[$(date)] No updates available."
fi
