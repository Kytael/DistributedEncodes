# FractumSeraph's Distributed Video Encoder

A distributed video encoding system that uses a central manager to coordinate tasks and multiple remote workers to process video files. 

The system uses HTTP for all data transfer, meaning Workers do not need to mount network drives, install programs, or anything special. They simply need an internet connectiontand the rest of the setup is automatic.

## System Architecture

1.  **Manager:** Hosts the source files, the database, and a web dashboard.
2.  **Workers:** Download a raw video, encode it locally (using FFmpeg), and upload the result back to the Manager.

## Prerequisites

* **Manager:** Python 3.8+, Flask, SQLite
* **Worker:** Linux/macOS/WSL with Python 3. FFmpeg with av1 (svt) support is also required, but will be downloaded automatically if required.

## Server Setup (The Manager)

1.  **Clone the repository:**
    ```bash
    git clone [https://github.com/FractumSeraph/DistributedEncodes.git](https://github.com/FractumSeraph/DistributedEncodes.git)
    cd DistributedEncodes
    ```

2.  **Install Dependencies:**
    ```bash
    pip install -r requirements.txt
    ```

3.  **Prepare Folders:**
    Create a folder named `source_media` in the root directory.
    ```bash
    mkdir source_media
    ```
    *Place your raw video files here.(e.g., `source_media/Series Name/Season 1/Episode 1.mkv`).*

4.  **Configuration:**
    Open `manager.py` and edit the `SERVER_URL_DISPLAY` to your servers ip/domain.
    ```python
    SERVER_URL_DISPLAY = "[https://encode.fractumseraph.net/](https://encode.fractumseraph.net/)"
    ```

5.  **Run the Server:**
    ```bash
    python manager.py
    ```
    The server listens on Port 80 bby default, but I recommend placing this behing a reverse proxy with https support instead. That will be required when I get the web worker finished..

## Worker Setup

Workers can be set up with a single command. The Manager hosts a dynamic installer script.

Windows users can install python and run the worker.py file. I Will occasionally build .exe releases as well.
Run this on any Linux machine for a one liner download and run. Replace username and workername with your preferences:

```curl -s "https://encode.fractumseraph.net/install?username=YourName&workername=GamingPC" | sudo bash```
