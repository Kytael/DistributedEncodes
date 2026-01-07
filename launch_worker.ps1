$ErrorActionPreference = "Stop"

# Load required UI assemblies for InputBoxes
Add-Type -AssemblyName Microsoft.VisualBasic
Add-Type -AssemblyName System.Windows.Forms

# --- Configuration ---
$ManagerUrl = "https://encode.fractumseraph.net/"
$WorkerUrl  = "$($ManagerUrl)dl/worker"
$ConfigFile = "worker_config.json"
$Config     = $null

# --- Helper Function for Errors ---
function Show-Error($msg) {
    [System.Windows.Forms.MessageBox]::Show($msg, "Error", [System.Windows.Forms.MessageBoxButtons]::OK, [System.Windows.Forms.MessageBoxIcon]::Error)
}

# --- 1. Load or Create Config ---
# We try to load the config silently.
if (Test-Path $ConfigFile) {
    try {
        $Config = Get-Content $ConfigFile -Raw | ConvertFrom-Json
    } catch {
        Remove-Item $ConfigFile -ErrorAction SilentlyContinue
    }
}

# If config is missing, we ask the user ONE time.
if (-not $Config) {
    # Prompt 1: Username
    $defUser = "Anonymous"
    $uPrompt = "Please enter the USERNAME of the person running the program.`n(e.g. 'FractumSeraph', 'John Smith')`n`n[Click Cancel to Quit]"
    $uInput  = [Microsoft.VisualBasic.Interaction]::InputBox($uPrompt, "Fractum Encodes Setup (1/2)", $defUser)
    
    # FIX: Check if user hit Cancel (Empty String)
    if ($uInput -eq "") { exit } 

    # Prompt 2: Worker Name
    $defWorker = "Node-" + (Get-Random -Minimum 1000 -Maximum 9999)
    $wPrompt   = "Please enter a name for THIS COMPUTER.`n(e.g. 'LivingRoom-PC')`n`n[Click Cancel to Quit]"
    $wInput    = [Microsoft.VisualBasic.Interaction]::InputBox($wPrompt, "Fractum Encodes Setup (2/2)", $defWorker)

    # FIX: Check if user hit Cancel
    if ($wInput -eq "") { exit }

    # Save to file
    $Config = @{ username = $uInput; workername = $wInput }
    $Config | ConvertTo-Json | Out-File $ConfigFile -Encoding UTF8
}

# --- 2. Check Python (Silent) ---
$pythonExists = $false
try {
    # We redirect output to $null so it doesn't cause a popup
    $ver = python --version 2>&1
    if ($ver -match "Python 3") { $pythonExists = $true }
} catch {}

if (-not $pythonExists) {
    # Only show this popup if we actually need to install Python
    [System.Windows.Forms.MessageBox]::Show("Python is missing.`n`nClick OK to install it automatically.`n(This may take 1-2 minutes. Please wait.)", "Fractum Setup", [System.Windows.Forms.MessageBoxButtons]::OK, [System.Windows.Forms.MessageBoxIcon]::Information)
    
    try {
        $installer = "$env:TEMP\python_installer.exe"
        Invoke-WebRequest "https://www.python.org/ftp/python/3.11.9/python-3.11.9-amd64.exe" -OutFile $installer -UseBasicParsing
        # Install silently
        Start-Process -FilePath $installer -ArgumentList "/quiet InstallAllUsers=1 PrependPath=1 Include_test=0" -Wait
        # Refresh Path
        $env:Path = [System.Environment]::GetEnvironmentVariable("Path","Machine") + ";" + [System.Environment]::GetEnvironmentVariable("Path","User")
    } catch {
        Show-Error "Failed to install Python. Please run this installer as Administrator."
        exit
    }
}

# --- 3. Check Dependencies (Silent) ---
try {
    pip install requests --disable-pip-version-check | Out-Null
} catch {
    python -m pip install requests | Out-Null
}

# --- 4. Download Worker (Silent) ---
if (-not (Test-Path "worker.py")) {
    try {
        Invoke-WebRequest $WorkerUrl -OutFile "worker.py" -UseBasicParsing
    } catch {
        Show-Error "Could not download worker script from server.`nCheck your internet connection."
        exit
    }
}

# --- 5. Launch the Worker ---
# This opens the black console window for the worker itself so the user can see progress.
# The Launcher (.exe) will close immediately after this.
Start-Process python -ArgumentList "worker.py --manager $ManagerUrl --username `"$($Config.username)`" --workername `"$($Config.workername)`" --jobs 1"
