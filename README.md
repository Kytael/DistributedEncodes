# FractumSeraph's Distributed Video Encoder

A distributed video encoding swarm. This system allows multiple computers (nodes) to fetch video chunks from a central manager, encode them into AV1, and upload the results automatically.

This Program is built to help others encode videos to add to my website at https://vsv.fractumseraph.net/
The videos are specifically made with very extreme encoding settings.

The system uses HTTP for all data transfer, meaning Workers do not need to mount network drives or perform complex VPN setups. They simply need an internet connection.

---

## How to Help (Run a Worker)
You can contribute your CPU power to the swarm using one of the methods below. 

### Option 1: Docker (Recommended)
The easiest way to run a worker. It handles all dependencies automatically and isolates the process.

**If you have cloned the repository:**
1. Navigate to the `docker` folder:
   ```bash
   cd docker
   ```
2. Run the worker:
   ```bash
   docker-compose up -d
   ```

**If you do not have the repository:**
1. Create a file named `docker-compose.yml`following and paste the following (Change the username if you want to be on the scoreboard!):
   ```yaml
   version: '3.8'
   services:
     fractum-worker:
       image: python:3.11-slim-bookworm
       container_name: fractum_worker_node
       restart: unless-stopped
       stop_grace_period: 30s
       # This entrypoint installs ffmpeg/python and downloads the latest worker script automatically
       entrypoint: ["/bin/sh", "-c", "apt-get update && apt-get install -y ffmpeg curl && pip install requests && curl -fsSL -o worker.py [https://encode.fractumseraph.net/dl/worker](https://encode.fractumseraph.net/dl/worker) && exec python worker.py \"$@\"", "--"]
       command: >
         --manager "[https://encode.fractumseraph.net/](https://encode.fractumseraph.net/)"
         --username "DockerUser"
         --workername "DockerNode"
         --jobs 1
   ```
2. Run it:
   ```bash
   docker-compose up -d
   ```

---

### Option 2: Linux / WSL (Auto-Installer)
If you are on Linux or Windows Subsystem for Linux (WSL), use this one-line installer. It will install FFmpeg and Python automatically.

```bash
# Replace 'YourName' with your actual username
curl -s "[https://encode.fractumseraph.net/install?username=YourName&workername=LinuxNode&jobs=1](https://encode.fractumseraph.net/install?username=YourName&workername=LinuxNode&jobs=1)" | bash
```

---

### Option 3: Windows (Manual Run)

1. **Install Python 3.11+**:
   - Download from [python.org](https://www.python.org/).
   - **Important:** During install, check the box **"Add Python to PATH"**.

2. **Install Dependencies:**
   Open CMD or PowerShell and run:
   ```bash
   pip install requests
   ```

3. **Run the Worker:**
   - [Download worker.py](https://encode.fractumseraph.net/dl/worker)
   - Open a terminal in the download folder and run:
     ```bash
     python worker.py --manager "[https://encode.fractumseraph.net/](https://encode.fractumseraph.net/)" --username "MyUser" --workername "MyPC" --jobs 1
     ```
   
   **Note:** The script will automatically detect if you are missing FFmpeg and will download a portable version (approx 40MB) to the local folder.

---

## Hosting the Manager (Server Side)

**Only follow these steps if you are hosting the central server.**

### Prerequisites
* **OS:**p Ubuntu 22.04/24.04 LTS (Recommended. It probably works on other systems, but I test it on these.)
* **Software:** Python 3.11+, pip, git, ffmpeg (for verification only)
* **Network:** A public IP or domain name (e.g., `encode.fractumseraph.net`)

### 1. Installation
Clone the repository to your server.
```bash
git clone [https://github.com/FractumSeraph/DistributedEncodes.git](https://github.com/FractumSeraph/DistributedEncodes.git) distributed-encodes
cd distributed-encodes
pip3 install -r requirements.txt
```

### 2. Configuration
Create a `config.py` file in the main directory:

```python
SERVER_HOST = '0.0.0.0'
SERVER_PORT = 5000
SERVER_URL_DISPLAY = '[https://encode.fractumseraph.net](https://encode.fractumseraph.net)' # Public URL workers use
SOURCE_DIRECTORY = './source_media'
COMPLETED_DIRECTORY = './completed_media'
WORKER_TEMPLATE_FILE = 'worker_template.py'
DB_FILE = 'fractum.db'
VIDEO_EXTENSIONS = ('.mkv', '.mp4', '.avi', '.mov')
ADMIN_USER = 'admin'
ADMIN_PASS = 'ChangeMeToSomethingSecure'
WORKER_SECRET = 'OptionalSharedSecret'
```

### 3. Running the Server (Gunicorn)
To prevent database locking issues, you **MUST** run Gunicorn with a single worker process.

**Standard Run Command:**
```bash
gunicorn --workers 1 --threads 8 --bind 0.0.0.0:5000 manager:app
```

**Systemd Service Example:**
Create `~/.config/systemd/user/fractum.service`:
```ini
[Unit]
Description=Fractum Manager
After=network.target

[Service]
WorkingDirectory=%h/distributed-encodes
ExecStart=%h/.local/bin/gunicorn --workers 1 --threads 8 --bind 0.0.0.0:5000 manager:app
Restart=always

[Install]
WantedBy=default.target
```

Enable it:
```bash
systemctl --user enable --now fractum.service
loginctl enable-linger $USER
```

### 4. Admin Panel
Access the dashboard at `https://your-domain.com/admin`.
* **Login:** Defined in `config.py`.
* **Features:**
    * **Reset Failed Jobs:** Bulk reset any jobs that crashed.
    * **Scan Files:** Manually trigger a scan of the `source_media` folder.
    * **Live Logs:** Watch worker activity in real-time.
