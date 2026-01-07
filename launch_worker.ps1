<#
.SYNOPSIS
    Fractum Worker Launcher & Installer (User Friendly Version)
    Installs Python, asks for User/Worker details via Popup, and runs the worker.
#>

$ErrorActionPreference = "Stop"

# Configuration
$ManagerUrl = "https://encode.fractumseraph.net/"
$WorkerUrl = "$($ManagerUrl)dl/worker"
$ConfigFile = "worker_config.json"

# Load Visual Basic (Required for Input Boxes)
Add-Type -AssemblyName Microsoft.VisualBasic

Write-Host "=========================================" -ForegroundColor Cyan
Write-Host "   Fractum Distributed Encoder Setup" -ForegroundColor Cyan
Write-Host "=========================================" -ForegroundColor Cyan

# --------------------------------------------------------------------------
# 1. SETUP & CONFIGURATION (The User-Friendly Part)
# --------------------------------------------------------------------------

# Check if we already have saved settings
if (Test-Path $ConfigFile) {
    Write-Host "[*] Loading saved configuration..." -ForegroundColor Gray
    try {
        $Config = Get-Content $ConfigFile -Raw | ConvertFrom-Json
    } catch {
        Write-Warning "Config file corrupted. Resetting."
        Remove-Item $ConfigFile -ErrorAction SilentlyContinue
    }
}

# If no config exists (First Run), ask the user via Popup Windows
if (-not $Config) {
    Write-Host "[*] First time setup: Asking for user details..." -ForegroundColor Yellow

    # Prompt 1: Username
    # Explanation: The person running the program.
    $MsgUser = "Please enter the USERNAME of the person running the program." + `
               "`n`nExamples: 'FractumSeraph', 'John Smith', 'JaneDoe'"
    
    $InputUser = [Microsoft.VisualBasic.Interaction]::InputBox(
        $MsgUser, 
        "Fractum Setup - Step 1 of 2", 
        "Anonymous"
    )
    if ([string]::IsNullOrWhiteSpace($InputUser)) { $InputUser = "Anonymous" }

    # Prompt 2: Worker Name
    # Explanation: The name of the specific computer.
    $MsgWorker = "Please enter a name for THIS COMPUTER." + `
                 "`n`nExamples: 'Fractums Laptop', 'Johns Gaming PC', 'LivingRoom-PC'"
    
    $DefaultWorker = "Node-" + (Get-Random -Minimum 1000 -Maximum 9999)
    $InputWorker = [Microsoft.VisualBasic.Interaction]::InputBox(
        $MsgWorker, 
        "Fractum Setup - Step 2 of 2", 
        $DefaultWorker
    )
    if ([string]::IsNullOrWhiteSpace($InputWorker)) { $InputWorker = $DefaultWorker }

    # Save to file so the worker remembers this next time
    $Config = @{
        username   = $InputUser
        workername = $InputWorker
    }
    $Config | ConvertTo-Json | Out-File $ConfigFile -Encoding UTF8
    
    Write-Host "    Settings saved to $ConfigFile" -ForegroundColor Green
}

# --------------------------------------------------------------------------
# 2. CHECK PYTHON INSTALLATION
# --------------------------------------------------------------------------
Write-Host "[*] Checking for Python..."
try {
    $pyVersion = python --version 2>&1
    if ($pyVersion -match "Python 3") {
        Write-Host "    Found $pyVersion" -ForegroundColor Green
    } else { throw "Missing" }
} catch {
    Write-Host "    Python not found. Downloading installer..." -ForegroundColor Yellow
    
    $installerPath = "$env:TEMP\python_installer.exe"
    $pyUrl = "https://www.python.org/ftp/python/3.11.9/python-3.11.9-amd64.exe"
    
    try {
        Invoke-WebRequest -Uri $pyUrl -OutFile $installerPath -UseBasicParsing
        Write-Host "    Installing Python (This may take a minute)..." -ForegroundColor Yellow
        # Silent install
        Start-Process -FilePath $installerPath -ArgumentList "/quiet InstallAllUsers=1 PrependPath=1 Include_test=0" -Wait
        # Refresh environment variables
        $env:Path = [System.Environment]::GetEnvironmentVariable("Path","Machine") + ";" + [System.Environment]::GetEnvironmentVariable("Path","User")
        Write-Host "    Python installed successfully!" -ForegroundColor Green
    } catch {
        [Microsoft.VisualBasic.Interaction]::MsgBox("Failed to install Python. Please Run as Administrator.", "Critical Error", 0)
        exit
    }
}

# --------------------------------------------------------------------------
# 3. INSTALL DEPENDENCIES & DOWNLOAD WORKER
# --------------------------------------------------------------------------
Write-Host "[*] Checking dependencies..."
try {
    pip install requests --disable-pip-version-check | Out-Null
} catch {
    python -m pip install requests
}

if (-not (Test-Path "worker.py")) {
    Write-Host "[*] Downloading worker script..."
    try {
        Invoke-WebRequest -Uri $WorkerUrl -OutFile "worker.py" -UseBasicParsing
    } catch {
        Write-Error "Could not download worker.py from $WorkerUrl"
        exit
    }
}

# --------------------------------------------------------------------------
# 4. LAUNCH THE WORKER
# --------------------------------------------------------------------------
Write-Host "`n[*] Launching Worker..." -ForegroundColor Cyan
Write-Host "    User:   $($Config.username)" -ForegroundColor Gray
Write-Host "    Worker: $($Config.workername)" -ForegroundColor Gray
Write-Host "`n[!] Press Ctrl+C to stop the worker." -ForegroundColor Yellow

# Pass the configured variables to the python script
python worker.py --manager $ManagerUrl --username "$($Config.username)" --workername "$($Config.workername)" --jobs 1

Write-Host "`nWorker exited."
Start-Sleep -Seconds 5
