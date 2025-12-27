# update.ps1 - Windows Update Script
# Usage: Right-click > "Run with PowerShell" or run from terminal: .\update.ps1

# Navigate to the script's directory
Set-Location $PSScriptRoot

Write-Host "Checking for updates..."

# Fetch latest changes
git fetch origin

# Compare local and remote hashes
$local = git rev-parse HEAD
$remote = git rev-parse "@{u}"

if ($local -ne $remote) {
    Write-Host "Update found! Pulling changes..." -ForegroundColor Cyan
    git pull origin main

    if (Test-Path "requirements.txt") {
        Write-Host "Updating dependencies..."
        pip install -r requirements.txt
    }

    Write-Host "Update complete." -ForegroundColor Green
    Write-Host "NOTE: You must manually restart 'manager.py' for changes to take effect." -ForegroundColor Yellow
} else {
    Write-Host "No updates available. You are on the latest version." -ForegroundColor Green
}

# Keep window open briefly so user can read output
Start-Sleep -Seconds 5
