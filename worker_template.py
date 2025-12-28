import argparse
import time
import requests
import subprocess
import os
import re
import shutil
import threading
import sys
import traceback
import platform
import zipfile
import io
import json
from datetime import datetime

# ==============================================================================
# CONFIGURATION
# ==============================================================================
DEFAULT_MANAGER_URL = "https://encode.fractumseraph.net/"
DEFAULT_USERNAME = "Anonymous"
DEFAULT_WORKERNAME = f"Node-{int(time.time())}"
WORKER_VERSION = "1.0.2"

# --- UPDATE COORDINATION ---
SHUTDOWN_EVENT = threading.Event()
UPDATE_AVAILABLE = False
LAST_UPDATE_CHECK = 0
CHECK_LOCK = threading.Lock()

# ==============================================================================
# ENCODING CONFIGURATION - DO NOT MODIFY WITHOUT EXPLICIT USER INSTRUCTION
# ==============================================================================
# WARNING: These settings are critically tuned for specific requirements.
# ANY CHANGES HERE BY LLMS MUST BE EXPLICITLY REQUESTED BY THE USER.
# DO NOT "OPTIMIZE" OR "FIX" THESE SETTINGS AUTOMATICALLY.
ENCODING_CONFIG = {
    # VIDEO SETTINGS
    "VIDEO_CODEC": "libsvtav1",
    "VIDEO_PRESET": "2",
    "VIDEO_CRF": "63",           # Smallest filesize
    "VIDEO_PIX_FMT": "yuv420p",  # 8-bit
    "VIDEO_SCALE": "scale=-2:480",
    
    # AUDIO SETTINGS
    "AUDIO_CODEC": "libopus",
    "AUDIO_BITRATE": "12k",      # 12kbps
    "AUDIO_CHANNELS": "1",       # Mono
    
    # SUBTITLE SETTINGS
    "SUBTITLE_CODEC": "mov_text", # MP4 compatibility
    
    # CONTAINER
    "OUTPUT_EXT": ".mp4"
}
# ==============================================================================

# ==============================================================================
# HELPERS
# ==============================================================================

def check_version(manager_url):
    """Checks if a newer version exists. Returns True if update found."""
    global LAST_UPDATE_CHECK
    with CHECK_LOCK:
        # Debounce checks (10 minutes)
        if time.time() - LAST_UPDATE_CHECK < 600:
            return False
        LAST_UPDATE_CHECK = time.time()

    print(f"[*] Checking for updates (Current: {WORKER_VERSION})...")
    try:
        url = f"{manager_url}/dl/worker"
        r = requests.get(url, timeout=10)
        
        if r.status_code == 200:
            new_content = r.text
            match = re.search(r'WORKER_VERSION\s*=\s*"([^"]+)"', new_content)
            if match:
                remote_version = match.group(1)
                if remote_version != WORKER_VERSION:
                    print(f"[!] Update found: {WORKER_VERSION} -> {remote_version}")
                    return True
    except Exception as e:
        print(f"[!] Update check failed: {e}")
    
    return False

def apply_update(manager_url):
    """Downloads and restarts the worker."""
    print("[*] Downloading and applying update...")
    try:
        url = f"{manager_url}/dl/worker"
        r = requests.get(url, timeout=30)
        if r.status_code == 200:
            # Overwrite current script
            script_path = os.path.abspath(sys.argv[0])
            with open(script_path, 'w', encoding='utf-8') as f:
                f.write(r.text)
                
            print("[*] Restarting worker...")
            os.execv(sys.executable, [sys.executable] + sys.argv)
        else:
            print("[!] Update download failed.")
    except Exception as e:
        print(f"[!] Failed to apply update: {e}")

def log(worker_id, message, level="INFO"):
    """Thread-safe logging with timestamps."""
    timestamp = datetime.now().strftime("%H:%M:%S")
    # Use simple print which is generally thread-safe for single lines
    print(f"[{timestamp}] [{worker_id}] [{level}] {message}")

def print_progress(worker_id, current, total, prefix='', suffix=''):
    """
    Draws a progress bar. 
    NOTE: Only use this in single-thread mode to avoid console scrambling.
    """
    if total <= 0: return
    
    # Calculate percentages
    percent = 100 * (current / float(total))
    if percent > 100: percent = 100
    
    # Bar configuration
    length = 40
    filled_length = int(length * current // total)
    bar = '█' * filled_length + '-' * (length - filled_length)
    
    # Overwrite line
    sys.stdout.write(f'\r[{datetime.now().strftime("%H:%M:%S")}] [{worker_id}] {prefix} |{bar}| {percent:.1f}% {suffix}')
    sys.stdout.flush()
    
    if current >= total:
        sys.stdout.write('\n')

def get_seconds(t):
    """Converts HH:MM:SS.ms to total seconds."""
    try:
        parts = t.split(':')
        h = int(parts[0])
        m = int(parts[1])
        s = float(parts[2])
        return h*3600 + m*60 + s
    except:
        return 0

def install_ffmpeg_windows():
    """Downloads and installs FFmpeg for Windows automatically."""
    print("[*] Downloading FFmpeg for Windows (approx. 100MB)...")
    url = "https://github.com/BtbN/FFmpeg-Builds/releases/download/latest/ffmpeg-master-latest-win64-gpl.zip"
    try:
        r = requests.get(url, stream=True)
        r.raise_for_status()
        
        local_zip = "ffmpeg_temp.zip"
        total_size = int(r.headers.get('content-length', 0))
        downloaded = 0
        
        with open(local_zip, 'wb') as f:
            for chunk in r.iter_content(chunk_size=32768):
                if chunk:
                    f.write(chunk)
                    downloaded += len(chunk)
                    if total_size > 0:
                        percent = int((downloaded / total_size) * 100)
                        sys.stdout.write(f"\r[*] Downloading... {percent}%")
                        sys.stdout.flush()
        print("") # Newline

        print("[*] Extracting FFmpeg & FFprobe...")
        with zipfile.ZipFile(local_zip) as z:
            ffmpeg_path = None
            ffprobe_path = None
            for name in z.namelist():
                if name.endswith("bin/ffmpeg.exe"):
                    ffmpeg_path = name
                elif name.endswith("bin/ffprobe.exe"):
                    ffprobe_path = name
            
            if not ffmpeg_path:
                raise Exception("ffmpeg.exe not found in downloaded zip.")
            
            with open("ffmpeg.exe", "wb") as f_out:
                f_out.write(z.read(ffmpeg_path))
            
            if ffprobe_path:
                with open("ffprobe.exe", "wb") as f_out:
                    f_out.write(z.read(ffprobe_path))
            else:
                print("[!] Warning: ffprobe.exe not found in zip.")
        
        # Cleanup
        os.remove(local_zip)
        print("[*] FFmpeg & FFprobe downloaded and extracted to current directory.")
        return os.path.abspath(".")
    except Exception as e:
        print(f"[!] Failed to auto-install FFmpeg: {e}")
        if os.path.exists("ffmpeg_temp.zip"): os.remove("ffmpeg_temp.zip")
        return None

def check_ffmpeg():
    """Verifies FFmpeg installation and SVT-AV1 support."""
    print("[*] Checking FFmpeg installation...")
    
    # Windows Auto-Install Logic
    if platform.system() == 'Windows':
        # Check if we need to setup local environment (if tools are missing from global PATH)
        if not (shutil.which("ffmpeg") and shutil.which("ffprobe")):
            # Check if we have it locally from a previous run
            if os.path.exists("ffmpeg.exe") and os.path.exists("ffprobe.exe"):
                 os.environ["PATH"] += os.pathsep + os.path.abspath(".")
            else:
                 install_dir = install_ffmpeg_windows()
                 if install_dir:
                     os.environ["PATH"] += os.pathsep + install_dir

    def has_svtav1():
        try:
            res = subprocess.run(["ffmpeg", "-hide_banner", "-encoders"], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
            return "libsvtav1" in res.stdout
        except:
            return False

    if shutil.which("ffmpeg") and shutil.which("ffprobe") and has_svtav1():
        print("[*] FFmpeg with SVT-AV1 found. Good to go.")
        return

    print("[!] FFmpeg with SVT-AV1 support not found or missing (or ffprobe is missing).")

    # Linux/Mac Auto-Install Logic
    if platform.system() != 'Windows':
        print("[!] Attempting automatic installation...")
        try:
            if shutil.which("apt-get"):
                subprocess.run(["sudo", "apt-get", "update", "-qq"], check=True)
                subprocess.run(["sudo", "apt-get", "install", "-y", "ffmpeg"], check=True)
            elif shutil.which("dnf"):
                subprocess.run(["sudo", "dnf", "install", "-y", "ffmpeg"], check=True)
            elif shutil.which("brew"):
                subprocess.run(["brew", "install", "ffmpeg"], check=True)
            else:
                if not shutil.which("ffmpeg"): 
                    raise EnvironmentError("No supported package manager found.")
            
            if not has_svtav1():
                 raise EnvironmentError("Installed FFmpeg does not support libsvtav1 (SVT-AV1). Please install a custom build.")
                 
            print("[*] Installation successful.")
        except Exception as e:
            print(f"[!] CRITICAL ERROR: {e}")
            print("    Please install FFmpeg with libsvtav1 manually.")
            sys.exit(1)
    else:
        # Windows failed auto-install
        print("[!] Automatic installation failed.")
        print("    Please download FFmpeg (git-full build) from https://www.gyan.dev/ffmpeg/builds/")
        print("    and place 'ffmpeg.exe' in this directory.")
        sys.exit(1)

def verify_connection(manager_url):
    """Checks if the manager is reachable before starting."""
    print(f"[*] Testing connection to Manager: {manager_url}")
    try:
        r = requests.get(manager_url, timeout=10)
        if r.status_code < 400:
            print("[*] Connection successful.")
            return True
        else:
            print(f"[!] Server replied with error: {r.status_code}")
    except requests.exceptions.ConnectionError:
        print(f"[!] CRITICAL: Could not connect to {manager_url}")
        print("    1. Check if the URL is correct (use --manager http://IP:PORT).")
        print("    2. Check if the manager server is running.")
        print("    3. Check your firewall settings.")
    except Exception as e:
        print(f"[!] Error connecting: {e}")
    
    return False

# ==============================================================================
# WORKER LOGIC
# ==============================================================================

def worker_task(worker_id, manager_url, temp_dir, single_mode=False):
    """
    The main lifecycle of a worker thread.
    single_mode: If True, enables visual progress bars.
    """
    global UPDATE_AVAILABLE
    log(worker_id, "Thread started. Polling for jobs...")
    os.makedirs(temp_dir, exist_ok=True)
    
    while not SHUTDOWN_EVENT.is_set():
        try:
            # 0. Check for Updates periodically
            if check_version(manager_url):
                UPDATE_AVAILABLE = True
                SHUTDOWN_EVENT.set()
                break

            # 1. Request Job
            try:
                r = requests.get(f"{manager_url}/get_job", timeout=10)
            except requests.exceptions.RequestException as e:
                log(worker_id, f"Connection lost: {e}. Retrying in 30s...", "WARN")
                # Wait 30s, but check shutdown often
                for _ in range(30):
                    if SHUTDOWN_EVENT.is_set(): break
                    time.sleep(1)
                continue

            data = r.json() if r.status_code == 200 else None
            
            if data and data.get("status") == "ok":
                job = data["job"]
                job_id = job['id']
                dl_url = job['download_url']
                
                log(worker_id, f"Received Job: {job_id} ({job['filename']})")
                
                local_src = os.path.join(temp_dir, "source.tmp")
                local_dst = os.path.join(temp_dir, f"encoded{ENCODING_CONFIG['OUTPUT_EXT']}")
                
                # --- STEP 1: DOWNLOADING ---
                if not single_mode: log(worker_id, "Status: Downloading...")
                requests.post(f"{manager_url}/report_status", json={"worker_id":worker_id, "job_id":job_id, "status":"downloading", "progress":0})
                
                try:
                    with requests.get(dl_url, stream=True, timeout=30) as r:
                        r.raise_for_status()
                        total_size = int(r.headers.get('content-length', 0))
                        downloaded = 0
                        last_rep = 0
                        
                        with open(local_src, 'wb') as f:
                            for chunk in r.iter_content(chunk_size=8192): 
                                f.write(chunk)
                                downloaded += len(chunk)
                                
                                # Report to server every 5s
                                if time.time() - last_rep > 5:
                                    pct = int((downloaded/total_size)*100) if total_size > 0 else 0
                                    requests.post(f"{manager_url}/report_status", json={"worker_id":worker_id, "job_id":job_id, "status":"downloading", "progress":pct})
                                    last_rep = time.time()
                                    if not single_mode: log(worker_id, f"Downloading: {pct}%")
                                
                                # Visual Bar
                                if single_mode: print_progress(worker_id, downloaded, total_size, prefix='Downloading')

                    if single_mode: print_progress(worker_id, total_size, total_size, prefix='Downloading', suffix='Done')
                except Exception as e:
                    log(worker_id, f"Download failed: {e}", "ERROR")
                    requests.post(f"{manager_url}/report_status", json={"worker_id":worker_id, "job_id":job_id, "status":"failed"})
                    time.sleep(5)
                    continue

                # --- STEP 2: ANALYZE ---
                total_sec = 0
                total_min = 0
                audio_index = 0
                subtitle_indices = []
                
                try:
                    # Probe for Duration and Streams
                    cmd_probe = ['ffprobe', '-v', 'quiet', '-print_format', 'json', '-show_streams', '-show_format', local_src]
                    res = subprocess.run(cmd_probe, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
                    probe_data = json.loads(res.stdout)
                    
                    # 1. Duration
                    dur = probe_data.get('format', {}).get('duration')
                    if dur:
                        total_sec = float(dur)
                        total_min = int(total_sec / 60)
                    
                    # 2. Select Audio Stream (English > First)
                    audio_streams = [s for s in probe_data.get('streams', []) if s['codec_type'] == 'audio']
                    if audio_streams:
                        # Default to first
                        audio_index = audio_streams[0]['index']
                        # Try to find english
                        for s in audio_streams:
                            lang = s.get('tags', {}).get('language', '').lower()
                            # Check for 'eng', 'en', 'english'
                            if lang in ['eng', 'en', 'english']:
                                audio_index = s['index']
                                break
                    
                    # 3. Subtitles
                    for s in probe_data.get('streams', []):
                        if s['codec_type'] == 'subtitle':
                            # Whitelist text-based subtitles to reduce size (Excludes PGS/VOBSUB)
                            cname = s.get('codec_name', '').lower()
                            if cname in ['subrip', 'ass', 'webvtt', 'mov_text', 'text', 'srt', 'ssa']:
                                subtitle_indices.append(s['index'])
                            else:
                                log(worker_id, f"Skipping non-text subtitle: {cname} (Index {s['index']})", "INFO")

                except Exception as e:
                    log(worker_id, f"Probe failed: {e}. Using defaults.", "WARN")

                # --- STEP 3: ENCODING ---
                log(worker_id, f"Starting Encode. Length: {total_min} mins. Audio Index: {audio_index}")
                requests.post(f"{manager_url}/report_status", json={"worker_id":worker_id, "job_id":job_id, "status":"processing", "progress":0, "duration":total_min})
                
                # Build FFmpeg Command
                cmd = [
                    'ffmpeg', '-y', '-i', local_src,
                    '-map', '0:v:0',           # Map First Video
                    '-map', f'0:{audio_index}' # Map Selected Audio
                ]
                
                # Map Subtitles
                for idx in subtitle_indices: 
                    cmd.extend(['-map', f'0:{idx}'])

                cmd.extend([
                    '-c:v', ENCODING_CONFIG["VIDEO_CODEC"], 
                    '-preset', ENCODING_CONFIG["VIDEO_PRESET"], 
                    '-crf', ENCODING_CONFIG["VIDEO_CRF"],
                    '-pix_fmt', ENCODING_CONFIG["VIDEO_PIX_FMT"],
                    '-vf', ENCODING_CONFIG["VIDEO_SCALE"],
                    '-c:a', ENCODING_CONFIG["AUDIO_CODEC"], 
                    '-b:a', ENCODING_CONFIG["AUDIO_BITRATE"],
                    '-ac', ENCODING_CONFIG["AUDIO_CHANNELS"],
                    '-c:s', ENCODING_CONFIG["SUBTITLE_CODEC"],
                    '-progress', 'pipe:1', 
                    local_dst
                ])
                
                proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
                
                last_rep = 0
                frame_count = 0
                
                while True:
                    line = proc.stdout.readline()
                    if not line and proc.poll() is not None: break
                    
                    if "out_time=" in line and "N/A" not in line and total_sec > 0:
                        try:
                            time_str = line.split('=')[1].strip()
                            curr_sec = get_seconds(time_str)
                            pct = int((curr_sec/total_sec)*100)
                            
                            # Visual Bar
                            if single_mode: 
                                print_progress(worker_id, curr_sec, total_sec, prefix='Encoding   ')
                            
                            # Server Update (every 10s)
                            if time.time() - last_rep > 10:
                                requests.post(f"{manager_url}/report_status", json={"worker_id":worker_id, "job_id":job_id, "status":"processing", "progress":pct})
                                last_rep = time.time()
                                if not single_mode: log(worker_id, f"Encoding: {pct}%")
                                
                        except: pass
                
                if single_mode: print_progress(worker_id, total_sec, total_sec, prefix='Encoding   ', suffix='Done')

                if proc.returncode == 0 and os.path.exists(local_dst):
                    # --- STEP 4: UPLOADING ---
                    if not single_mode: log(worker_id, "Status: Uploading...")
                    requests.post(f"{manager_url}/report_status", json={"worker_id":worker_id, "job_id":job_id, "status":"uploading", "progress":0})
                    
                    # Upload wrapper for progress
                    class ProgressFileReader:
                        def __init__(self, filename, callback):
                            self._f = open(filename, 'rb')
                            self._total = os.path.getsize(filename)
                            self._read = 0
                            self._callback = callback
                            self._last_time = 0
                        def __enter__(self):
                            return self
                        def __exit__(self, exc_type, exc_val, exc_tb):
                            self._f.close()
                        def read(self, size=-1):
                            data = self._f.read(size)
                            self._read += len(data)
                            # Update visual bar immediately
                            if single_mode: 
                                print_progress(worker_id, self._read, self._total, prefix='Uploading  ')
                            
                            # Update server every 5s
                            if time.time() - self._last_time > 5:
                                pct = int((self._read / self._total) * 100)
                                self._callback(pct)
                                self._last_time = time.time()
                            return data
                        def __getattr__(self, attr): return getattr(self._f, attr)

                    def upload_server_callback(pct):
                        try: 
                            requests.post(f"{manager_url}/report_status", json={"worker_id":worker_id, "job_id":job_id, "status":"uploading", "progress":pct})
                            if not single_mode: log(worker_id, f"Uploading: {pct}%")
                        except: pass

                    with ProgressFileReader(local_dst, upload_server_callback) as f:
                        requests.post(f"{manager_url}/upload_result", files={'file': (job_id, f)}, data={'job_id': job_id, 'worker_id': worker_id})
                    
                    if single_mode: print_progress(worker_id, 100, 100, prefix='Uploading  ', suffix='Done')
                    log(worker_id, f"Job Completed: {job_id}")
                else:
                    log(worker_id, f"FFmpeg failed or file missing. Return code: {proc.returncode}", "ERROR")
                    requests.post(f"{manager_url}/report_status", json={"worker_id":worker_id, "job_id":job_id, "status":"failed"})

                # Cleanup
                if os.path.exists(local_src): os.remove(local_src)
                if os.path.exists(local_dst): os.remove(local_dst)
            else:
                # No job found
                if single_mode:
                    sys.stdout.write(f"\r[{datetime.now().strftime('%H:%M:%S')}] [{worker_id}] Idle. Waiting for jobs...   ")
                    sys.stdout.flush()
                else:
                    # In multi-thread mode, don't spam logs
                    pass 
                time.sleep(10)
                
        except KeyboardInterrupt:
            break
        except Exception as e:
            log(worker_id, f"Unexpected Error: {e}", "CRITICAL")
            traceback.print_exc()
            time.sleep(10)

def run_worker(args):
    print("==================================================")
    print(" FRACTUM DISTRIBUTED WORKER")
    print("==================================================")
    
    check_ffmpeg()
    
    manager_url = (args.manager or DEFAULT_MANAGER_URL).rstrip('/')
    username = args.username or DEFAULT_USERNAME
    base_workername = args.workername or DEFAULT_WORKERNAME
    
    # Verify URL before starting
    if not verify_connection(manager_url):
        sys.exit(1)
    
    # Initial Check
    if check_version(manager_url):
        apply_update(manager_url)
    
    num_jobs = args.jobs if args.jobs > 0 else 1
    if num_jobs > 32: num_jobs = 32
    
    print(f"[*] Starting {num_jobs} worker threads.")
    print(f"[*] ID: {username}-{base_workername} | Target: {manager_url}")
    print("==================================================\n")
    
    threads = []
    # If only 1 job, we enable the pretty progress bars
    single_mode = (num_jobs == 1)
    
    for i in range(num_jobs):
        worker_id = f"{username}-{base_workername}-{i+1}"
        temp_dir = f"./temp_encode_{base_workername}_{i+1}"
        
        if num_jobs > 1:
            t = threading.Thread(target=worker_task, args=(worker_id, manager_url, temp_dir, single_mode))
            t.daemon = True
            t.start()
            threads.append(t)
        else:
            # Run directly in main thread for better control if single job
            worker_task(worker_id, manager_url, temp_dir, single_mode)
            # If main thread returns, it means update or exit
            if UPDATE_AVAILABLE:
                apply_update(manager_url)
            return
        
    try:
        # Multi-thread Monitor Loop
        while True:
            all_dead = True
            for t in threads:
                if t.is_alive():
                    all_dead = False
                    break
            
            if all_dead:
                break

            if SHUTDOWN_EVENT.is_set():
                # Wait for threads to finish naturally (they break loop on next iteration)
                pass
            
            time.sleep(1)
            
        if UPDATE_AVAILABLE:
            apply_update(manager_url)

    except KeyboardInterrupt:
        print("\n[*] Stopping workers...")
        SHUTDOWN_EVENT.set()
