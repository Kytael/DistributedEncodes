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
import json
import signal
import zipfile
import tarfile
import io
from datetime import datetime, timedelta

# ==============================================================================
# CONFIGURATION
# ==============================================================================
DEFAULT_MANAGER_URL = "https://encode.fractumseraph.net/"
DEFAULT_USERNAME = "Anonymous"
DEFAULT_WORKERNAME = f"Node-{int(time.time())}"
WORKER_VERSION = "1.9.5" # [BUMPED] Auto-fix corrupted config files

WORKER_SECRET = os.environ.get("WORKER_SECRET", "DefaultInsecureSecret")

SHUTDOWN_EVENT = threading.Event()
UPDATE_AVAILABLE = False
LAST_UPDATE_CHECK = 0
CHECK_LOCK = threading.Lock()
CONSOLE_LOCK = threading.Lock()
PROGRESS_LOCK = threading.Lock()
WORKER_PROGRESS = {} 
PAUSE_REQUESTED = False
ACTIVE_PROCS = {}
PROC_LOCK = threading.Lock()

# Global paths for executables
FFMPEG_CMD = "ffmpeg"
FFPROBE_CMD = "ffprobe"

# Detect OS to handle Fonts
FONT_FILE = ""
if platform.system() == "Windows":
    FONT_FILE = r":fontfile='C\:\\Windows\\Fonts\\arial.ttf'" 
else:
    FONT_FILE = "" 

ENCODING_CONFIG = {
    "VIDEO_CODEC": "libsvtav1",
    "VIDEO_PRESET": "2",
    "VIDEO_CRF": "63",           
    "VIDEO_PIX_FMT": "yuv420p",
    "VIDEO_SCALE": f"scale=-2:480,drawtext=text='@FractumSeraph'{FONT_FILE}:fontcolor=white@0.2:fontsize=12:x=10:y=h-th-10",
    "AUDIO_CODEC": "libopus",
    "AUDIO_BITRATE": "12k",      
    "AUDIO_CHANNELS": "1",       
    "SUBTITLE_CODEC": "mov_text", 
    "OUTPUT_EXT": ".mp4"
}


class QuotaTracker:
    def __init__(self, limit_gb, worker_name):
        self.limit_bytes = int(limit_gb * 1024**3) if limit_gb > 0 else 0
        self.filename = f"usage_{re.sub(r'[^a-zA-Z0-9]', '', worker_name)}.json"
        self.lock = threading.Lock()
        self.current_usage = 0
        self.last_save = 0
        self._load()

    def _load(self):
        today = datetime.now().strftime("%Y-%m-%d")
        if os.path.exists(self.filename):
            try:
                with open(self.filename, 'r') as f:
                    data = json.load(f)
                    if data.get('date') == today:
                        self.current_usage = data.get('bytes', 0)
                    else:
                        self.current_usage = 0
                        self._save()
            except:
                self.current_usage = 0
        else:
            self.current_usage = 0

    def _save(self):
        today = datetime.now().strftime("%Y-%m-%d")
        try:
            with open(self.filename, 'w') as f:
                json.dump({"date": today, "bytes": self.current_usage}, f)
        except: pass

    def check_cap(self):
        if self.limit_bytes <= 0: return False
        today = datetime.now().strftime("%Y-%m-%d")
        if os.path.exists(self.filename):
            try:
                with open(self.filename, 'r') as f:
                    data = json.load(f)
                    if data.get('date') != today:
                        with self.lock:
                            self.current_usage = 0
                            self._save()
                        return False
            except: pass
        return self.current_usage >= self.limit_bytes

    def add_usage(self, num_bytes):
        if self.limit_bytes <= 0: return
        with self.lock:
            self.current_usage += num_bytes
            if time.time() - self.last_save > 30:
                self._save()
                self.last_save = time.time()
    
    def force_save(self):
        with self.lock: self._save()

    def get_remaining_str(self):
        if self.limit_bytes <= 0: return "Unlimited"
        rem = self.limit_bytes - self.current_usage
        if rem < 0: rem = 0
        return f"{rem / 1024**3:.2f} GB"
    
    def get_wait_time(self):
        now = datetime.now()
        tomorrow = now + timedelta(days=1)
        midnight = datetime(year=tomorrow.year, month=tomorrow.month, day=tomorrow.day, hour=0, minute=0, second=1)
        return (midnight - now).total_seconds()


def get_auth_headers():
    headers = {'User-Agent': f'FractumWorker/{WORKER_VERSION}'}
    if WORKER_SECRET:
        headers['X-Worker-Token'] = WORKER_SECRET
    return headers

def get_term_width():
    try: return shutil.get_terminal_size((80, 20)).columns
    except: return 80

def safe_print(message):
    with CONSOLE_LOCK:
        sys.stdout.write('\033[2K\r')
        print(message)
        sys.stdout.flush()

def log(worker_id, message, level="INFO"):
    timestamp = datetime.now().strftime("%H:%M:%S")
    safe_print(f"[{timestamp}] [{worker_id}] [{level}] {message}")

def signal_handler(sig, frame):
    global PAUSE_REQUESTED
    if not PAUSE_REQUESTED:
        PAUSE_REQUESTED = True
        sys.stdout.write('\n\n[!] PAUSE REQUESTED (Stopping gracefully...)\n')
        sys.stdout.flush()

def toggle_processes(suspend=True):
    with PROC_LOCK:
        for wid, proc in ACTIVE_PROCS.items():
            if proc.poll() is None:
                try:
                    if platform.system() == 'Windows': pass 
                    else:
                        sig = signal.SIGSTOP if suspend else signal.SIGCONT
                        os.kill(proc.pid, sig)
                except: pass

def kill_processes():
    with PROC_LOCK:
        for wid, proc in ACTIVE_PROCS.items():
            try:
                if proc.poll() is None: proc.kill()
            except: pass

def check_version(manager_url):
    global LAST_UPDATE_CHECK
    with CHECK_LOCK:
        if time.time() - LAST_UPDATE_CHECK < 600: return False
        LAST_UPDATE_CHECK = time.time()
    try:
        url = f"{manager_url}/dl/worker"
        r = requests.get(url, headers=get_auth_headers(), timeout=10)
        if r.status_code == 200:
            match = re.search(r'WORKER_VERSION\s*=\s*"([^"]+)"', r.text)
            if match and match.group(1) != WORKER_VERSION:
                safe_print(f"[!] Update found: {WORKER_VERSION} -> {match.group(1)}")
                return True
    except: pass
    return False

def apply_update(manager_url):
    safe_print("[*] Downloading and applying update...")
    try:
        url = f"{manager_url}/dl/worker"
        r = requests.get(url, headers=get_auth_headers(), timeout=30)
        if r.status_code == 200:
            with open(os.path.abspath(sys.argv[0]), 'w', encoding='utf-8') as f:
                f.write(r.text)
            safe_print("[*] Restarting worker...")
            os.execv(sys.executable, [sys.executable] + sys.argv)
    except Exception as e:
        safe_print(f"[!] Failed to apply update: {e}")

def print_progress(worker_id, current, total, prefix='', suffix=''):
    if total <= 0: return
    percent = 100 * (current / float(total))
    if percent > 100: percent = 100
    width = get_term_width()
    overhead = 12 + len(worker_id) + len(prefix) + 10 + len(suffix)
    bar_length = width - overhead - 5
    if bar_length < 10: bar_length = 10
    filled_length = int(bar_length * current // total)
    bar = '█' * filled_length + '-' * (bar_length - filled_length)
    line = f'[{datetime.now().strftime("%H:%M:%S")}] [{worker_id}] {prefix} |{bar}| {percent:.1f}% {suffix}'
    with CONSOLE_LOCK:
        if len(line) > width: line = line[:width-1]
        sys.stdout.write('\033[2K\r' + line)
        sys.stdout.flush()
    if current >= total: sys.stdout.write('\n')

def monitor_status_loop(worker_ids):
    while not SHUTDOWN_EVENT.is_set():
        if PAUSE_REQUESTED:
             time.sleep(0.5); continue
        parts = []
        with PROGRESS_LOCK:
            for wid in sorted(worker_ids, key=lambda x: x.split('-')[-1]):
                try: short_id = wid.split('-')[-1]
                except: short_id = wid
                state = WORKER_PROGRESS.get(wid, "Idle")
                parts.append(f"[{short_id}: {state}]")
        if parts:
            line = " ".join(parts)
            width = get_term_width()
            if len(line) > width - 1: line = line[:width-4] + "..."
            with CONSOLE_LOCK:
                sys.stdout.write('\033[2K\r' + line)
                sys.stdout.flush()
        time.sleep(0.5)

def get_seconds(t):
    try:
        parts = t.split(':')
        h = int(parts[0]); m = int(parts[1]); s = float(parts[2])
        return h*3600 + m*60 + s
    except: return 0

# ==============================================================================
# FFMPEG MANAGEMENT
# ==============================================================================

def download_ffmpeg_windows():
    print("[*] FFmpeg not found. Attempting download (FULL Version ~128MB)...")
    
    # 1. Primary: Gyan.dev (FULL BUILD) - Required for SVT-AV1
    # 2. Backup: BtbN (GitHub) - Also contains SVT-AV1
    urls = [
        "https://www.gyan.dev/ffmpeg/builds/ffmpeg-release-full.zip",
        "https://github.com/BtbN/FFmpeg-Builds/releases/download/latest/ffmpeg-master-latest-win64-gpl.zip"
    ]
    
    temp_zip = "ffmpeg_temp.zip"
    
    for url in urls:
        print(f"[*] Trying mirror: {url}")
        try:
            # Increased timeout for larger files
            with requests.get(url, stream=True, timeout=180) as r:
                r.raise_for_status()
                total_size = int(r.headers.get('content-length', 0))
                downloaded = 0
                
                with open(temp_zip, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        f.write(chunk)
                        downloaded += len(chunk)
                        if total_size > 0:
                            pct = int((downloaded / total_size) * 100)
                            sys.stdout.write(f"\r    Downloading... {pct}%")
                            sys.stdout.flush()
            print("\n[*] Extracting FFmpeg...")
            
            with zipfile.ZipFile(temp_zip) as z:
                ffmpeg_path = None
                ffprobe_path = None
                for file in z.namelist():
                    if file.endswith("bin/ffmpeg.exe"): ffmpeg_path = file
                    if file.endswith("bin/ffprobe.exe"): ffprobe_path = file
                
                if not ffmpeg_path or not ffprobe_path:
                    # Could not find in this zip, try next
                    print("\n[!] Binaries not found in zip.")
                    continue
                
                with open("ffmpeg.exe", "wb") as f: f.write(z.read(ffmpeg_path))
                with open("ffprobe.exe", "wb") as f: f.write(z.read(ffprobe_path))
                
            os.remove(temp_zip)
            print("[*] FFmpeg installed locally!")
            return True
            
        except Exception as e:
            print(f"\n[!] Mirror failed: {e}")
            if os.path.exists(temp_zip): os.remove(temp_zip)
            continue
            
    return False

def download_ffmpeg_linux():
    print("[*] Downloading static FFmpeg build (BtbN)...")
    arch = platform.machine().lower()
    
    if arch in ['x86_64', 'amd64']:
        url = "https://github.com/BtbN/FFmpeg-Builds/releases/download/latest/ffmpeg-master-latest-linux64-gpl.tar.xz"
    elif arch in ['aarch64', 'arm64']:
        url = "https://johnvansickle.com/ffmpeg/releases/ffmpeg-release-arm64-static.tar.xz"
    else:
        print(f"[!] Unsupported architecture for auto-download: {arch}")
        return False

    try:
        r = requests.get(url, stream=True, allow_redirects=True, timeout=30)
        r.raise_for_status()
        
        tar_name = f"ffmpeg_static_{int(time.time())}.tar.xz"
        total_size = int(r.headers.get('content-length', 0))
        downloaded = 0
        
        with open(tar_name, 'wb') as f:
             for chunk in r.iter_content(chunk_size=8192):
                 f.write(chunk)
                 downloaded += len(chunk)
                 if total_size > 0:
                     pct = int((downloaded / total_size) * 100)
                     sys.stdout.write(f"\r    Downloading... {pct}%")
                     sys.stdout.flush()
        
        print("\n[*] Extracting FFmpeg...")
        ext_dir = f"temp_ffmpeg_ext_{int(time.time())}"
        os.makedirs(ext_dir, exist_ok=True)
        
        with tarfile.open(tar_name, "r:xz") as tar:
            tar.extractall(path=ext_dir)
            
        found_ffmpeg = False
        for root, dirs, files in os.walk(ext_dir):
            for file in files:
                if file == "ffmpeg":
                    shutil.move(os.path.join(root, file), "ffmpeg")
                    found_ffmpeg = True
                elif file == "ffprobe":
                    shutil.move(os.path.join(root, file), "ffprobe")

        if os.path.exists(tar_name): os.remove(tar_name)
        if os.path.exists(ext_dir): shutil.rmtree(ext_dir)
        
        if found_ffmpeg:
            os.chmod("ffmpeg", 0o755)
            if os.path.exists("ffprobe"): os.chmod("ffprobe", 0o755)
            print("[*] FFmpeg installed locally!")
            return True
        else:
            print("[!] Could not find 'ffmpeg' binary in extracted archive.")
            return False

    except Exception as e:
        print(f"\n[!] Linux Download failed: {e}")
        return False

def has_svtav1(cmd):
    """Checks if the given ffmpeg command supports libsvtav1"""
    try:
        res = subprocess.run([cmd, "-hide_banner", "-encoders"], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        return "libsvtav1" in res.stdout
    except:
        return False

def check_ffmpeg():
    global FFMPEG_CMD, FFPROBE_CMD
    
    # 1. Check Local Static Build (Priority)
    local_ffmpeg = os.path.abspath("ffmpeg.exe" if platform.system() == "Windows" else "./ffmpeg")
    local_ffprobe = os.path.abspath("ffprobe.exe" if platform.system() == "Windows" else "./ffprobe")
    
    if os.path.exists(local_ffmpeg) and has_svtav1(local_ffmpeg):
        FFMPEG_CMD = local_ffmpeg
        if os.path.exists(local_ffprobe): FFPROBE_CMD = local_ffprobe
        return

    # 2. Check System FFmpeg
    if shutil.which("ffmpeg") and has_svtav1("ffmpeg"):
        FFMPEG_CMD = "ffmpeg"
        FFPROBE_CMD = "ffprobe"
        return

    # 3. If we are here, we have no working FFmpeg. Attempt Download.
    print("[!] Valid FFmpeg with lib
