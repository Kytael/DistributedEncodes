# FractumSeraph's Distributed Video Encoder

A distributed video encoding system that uses a central manager to coordinate tasks and multiple remote workers to process video files. 

The system uses HTTP for all data transfer, meaning Workers do not need to mount network drives, install programs, or anything special. They simply need an internet connectiontand the rest of the setup is automatic.

## System Architecture

1.  **Manager:** Hosts the source files, the database, and a web dashboard.
2.  **Workers:** Download a raw video, encode it locally (using FFmpeg), and upload the result back to the Manager.

## Prerequisites

* **Manager:** 
  * OS: Ubuntu 22.04/24.04 LTS (Recommended)
  * Software: Python 3.8+, pip, git, ffmpeg (for verification only)
* **Worker:** 
  * OS: Linux/macOS/Windows
  * Software: Python 3. FFmpeg with SVT-AV1 support (auto-installed on Linux).

## Server Deployment (The Manager)

### 1. Installation
Clone the repository to your server (e.g., in your home directory).
```bash
cd ~
git clone https://github.com/FractumSeraph/DistributedEncodes.git distributed-encodes
cd distributed-encodes
pip3 install -r requirements.txt
```

### 2. Configuration
*   **Create Directories:**
    ```bash
    mkdir source_media
    ```
    *Place your raw video files here (e.g., `source_media/Movie.mkv`).*

*   **Edit `manager.py`:**
    *   Set `SERVER_URL_DISPLAY` to your public domain/IP.
    *   **CRITICAL:** Change `ADMIN_PASS` to a secure password.

### 3. Service Setup (Systemd)
To keep the server running in the background and starting on boot, use the provided systemd service file.

1.  **Install the Service:**
    ```bash
    mkdir -p ~/.config/systemd/user/
    # If you cloned to a different path, edit distributed-encodes.service first!
    cp distributed-encodes.service ~/.config/systemd/user/
    systemctl --user daemon-reload
    systemctl --user enable --now distributed-encodes.service
    ```
2.  **Enable Lingering** (Required so the service stays active when you logout):
    ```bash
    loginctl enable-linger $USER
    ```
3.  **Check Status:**
    ```bash
    systemctl --user status distributed-encodes.service
    ```

### 4. Automatic Updates
The manager includes an `update.sh` script that pulls the latest code from GitHub and restarts the service if changes are found.

1.  **Make executable:** `chmod +x update.sh`
2.  **Add to Crontab** (Runs hourly):
    ```bash
    crontab -e
    ```
    Add this line (replace `YOUR_USER` with your actual username):
    ```bash
    0 * * * * /home/YOUR_USER/distributed-encodes/update.sh >> /home/YOUR_USER/distributed-encodes/update.log 2>&1
    ```

### 5. Managing Jobs
*   **Populate Queue:** To add new files from `source_media/` to the database:
    ```bash
    python3 populate.py
    ```
    *(Note: You do not need to restart the server; the script updates the DB directly.)*

## How Updates Work

*   **Manager:** Updates are handled by the server's OS. The `update.sh` script (triggered by cron) checks the Git repository for new commits. If a new version exists, it pulls the code, installs any new dependencies, and restarts the systemd service automatically.
*   **Workers:** When a worker starts, it checks the Manager's `/dl/worker` endpoint. If the server has a newer version of the worker script, the worker can be instructed to update itself (logic handled within the worker script or manually by re-running the install command).

## Worker Setup

Workers can be set up with a single command. The Manager hosts a dynamic installer script.

Windows users can install python and run the worker.py file. I Will occasionally build .exe releases as well.
Run this on any Linux machine for a one liner download and run. Replace username and workername with your preferences:

```curl -s "https://encode.fractumseraph.net/install?username=YourName&workername=GamingPC" | sudo bash```
