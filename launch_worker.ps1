$ErrorActionPreference = "Stop"
$DebugLog = "launcher_debug.txt"

# --- Start Logging ---
Start-Transcript -Path $DebugLog -Force

Add-Type -AssemblyName Microsoft.VisualBasic
Add-Type -AssemblyName System.Windows.Forms

# --- Configuration ---
$ManagerUrl = "https://encode.fractumseraph.net/"
$WorkerUrl  = "$($ManagerUrl)dl/worker"
$ConfigFile = "worker_config.json"
$Config     = $null

function Show-Error($msg) {
    Write-Error $msg
    [System.Windows.Forms.MessageBox]::Show($msg, "Error", [System.Windows.Forms.MessageBoxButtons]::OK, [System.Windows.Forms.MessageBoxIcon]::Error)
}

# --- 1. Load or Create Config ---
if (Test-Path $ConfigFile) {
    try {
        $Config = Get-Content $ConfigFile -Raw | ConvertFrom-Json
        Write-Host "Loaded existing config."
    } catch {
        Remove-Item $ConfigFile -ErrorAction SilentlyContinue
    }
}

if (-not $Config) {
    $defUser = "Anonymous"
    $uPrompt = "Please enter the USERNAME of the person running the program.`n(e.g. 'FractumSeraph')`n`n[Click Cancel to Quit]"
    $uInput  = [Microsoft.VisualBasic.Interaction]::InputBox($uPrompt, "Setup (1/2)", $defUser)
    if ($uInput -eq "") { Stop-Transcript; exit } 

    $defWorker = "Node-" + (Get-Random -Minimum 1000 -Maximum 9999)
    $wPrompt   = "Please enter a name for THIS COMPUTER.`n(e.g. 'LivingRoom-PC')`n`n[Click Cancel to Quit]"
    $wInput    = [Microsoft.VisualBasic.Interaction]::InputBox($wPrompt, "Setup (2/2)", $defWorker)
    if ($wInput -eq "") { Stop-Transcript; exit }

    $Config = @{ username = $uInput; workername = $wInput }
    $Config | ConvertTo-Json | Out-File $ConfigFile -Encoding UTF8
    Write-Host "Config saved to $ConfigFile"
}

# --- 2. Check Python ---
$pythonExists = $false
try {
    $ver = python --version 2>&1
    if ($ver -match "Python 3") { $pythonExists = $true }
} catch {}

if (-not $pythonExists) {
    [System.Windows.Forms.MessageBox]::Show("Python is missing.`n`nClick OK to install it automatically.`n(This may take 1-2 minutes.)", "Fractum Setup", [System.Windows.Forms.MessageBoxButtons]::OK, [System.Windows.Forms.MessageBoxIcon]::Information)
    try {
        $installer = "$env:TEMP\python_installer.exe"
        Invoke-WebRequest "https://www.python.org/ftp/python/3.11.9/python-3.11.9-amd64.exe" -OutFile $installer -UseBasicParsing
        Start-Process -FilePath $installer -ArgumentList "/quiet InstallAllUsers=1 PrependPath=1 Include_test=0" -Wait
        $env:Path = [System.Environment]::GetEnvironmentVariable("Path","Machine") + ";" + [System.Environment]::GetEnvironmentVariable("Path","User")
    } catch {
        Show-Error "Failed to install Python."
        Stop-Transcript; exit
    }
}

# --- 3. Dependencies ---
try { pip install requests --disable-pip-version-check | Out-Null } catch { python -m pip install requests | Out-Null }

# --- 4. Download Worker ---
try {
    Write-Host "Updating worker script..."
    Invoke-WebRequest $WorkerUrl -OutFile "worker.py" -UseBasicParsing
} catch {
    if (-not (Test-Path "worker.py")) {
        Show-Error "Could not download worker script.`nCheck internet connection."
        Stop-Transcript; exit
    }
}

# --- 5. Launch Worker ---
$CurrentDir = Get-Location
$LaunchCmd = "python worker.py --manager $ManagerUrl --jobs 1 2>&1 | Tee-Object -FilePath 'worker_log.txt'"

Write-Host "Starting Worker..."
Stop-Transcript

Start-Process powershell -ArgumentList "-NoExit", "-Command", "$LaunchCmd" -WorkingDirectory $CurrentDir
