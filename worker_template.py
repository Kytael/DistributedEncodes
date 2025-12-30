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
import ctypes
from datetime import datetime

# ==============================================================================
# CONFIGURATION
# ==============================================================================
DEFAULT_MANAGER_URL = "https://encode.fractumseraph.net/"
DEFAULT_USERNAME = "Anonymous"
DEFAULT_WORKERNAME = f"Node-{int(time.time())}"
WORKER_VERSION = "1.3.0" # Bumped version for Series Filtering

# [FIX] Read Secret from Environment (injected by install script)
WORKER_SECRET = os.environ.get("WORKER_SECRET", "")

# --- UPDATE COORDINATION ---
SHUTDOWN_EVENT = threading.Event()
UPDATE_AVAILABLE = False
LAST_UPDATE_CHECK = 0
CHECK_LOCK = threading.Lock()

# --- CONSOLE MANAGEMENT ---
CONSOLE_LOCK = threading.Lock()
PROGRESS_LOCK = threading.Lock()
WORKER_PROGRESS = {} 
PAUSE_REQUESTED = False

# --- PROCESS MANAGEMENT ---
ACTIVE_PROCS = {}
PROC_LOCK = threading.Lock()

# ==============================================================================
# ENCODING CONFIGURATION
# ==============================================================================
ENCODING_CONFIG = {
    "VIDEO_CODEC": "libsvtav1",
    "VIDEO_PRESET": "2",
    "VIDEO_CRF": "63",           
    "VIDEO_PIX_FMT": "yuv420p",  
    "VIDEO_SCALE": "scale=-2:480",
    "AUDIO_CODEC": "libopus",
    "AUDIO_BITRATE": "12k",      
    "AUDIO_CHANNELS": "1",       
    "SUBTITLE_CODEC": "mov_text", 
    "OUTPUT_EXT": ".mp4"
}

# ==============================================================================
# HELPERS
# ==============================================================================

def get_auth_headers():
    """[FIX] Add Security Token to all requests"""
    return {
        'User-Agent': f'FractumWorker/{WORKER_VERSION}',
        'X-Worker-Token': WORKER_SECRET
    }

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
        sys.stdout.write('\n\n[!] PAUSE REQUESTED. Please wait...\n')
        sys.stdout.flush()

def toggle_processes(suspend=True):
    with PROC_LOCK:
        for wid, proc in ACTIVE_PROCS.items():
            if proc.poll() is None:
                try:
                    if platform.system() == 'Windows':
                        # Windows suspend logic omitted for brevity
                        pass 
                    else:
                        sig = signal.SIGSTOP if suspend else signal.SIGCONT
                        os.kill(proc.pid, sig)
                except: pass

def kill_processes():
    with PROC_LOCK:
        for wid, proc in ACTIVE_PROCS.items():
            try:
                if proc.poll() is None:
                    proc.kill()
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

# --- FFMPEG ---
def check_ffmpeg():
    def has_svtav1():
        try:
            res = subprocess.run(["ffmpeg", "-hide_banner", "-encoders"], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
            return "libsvtav1" in res.stdout
        except: return False
    
    if shutil.which("ffmpeg") and shutil.which("ffprobe"):
        if has_svtav1(): return
        print("[!] FFmpeg found but missing libsvtav1 support.")
    
    if platform.system() != 'Windows':
        print("[!] Attempting to install ffmpeg...")
        try: subprocess.run(["sudo", "apt-get", "install", "-y", "ffmpeg"], check=False)
        except: pass

    if not (shutil.which("ffmpeg") and has_svtav1()):
        print("[!] FFmpeg missing or incompatible. Please install FFmpeg with libsvtav1.")
        sys.exit(1)

def verify_connection(manager_url):
    try:
        if requests.get(manager_url, timeout=10).status_code < 400: return True
    except: pass
    print(f"[!] Could not connect to {manager_url}"); return False

# ==============================================================================
# WORKER LOGIC
# ==============================================================================

def worker_task(worker_id, manager_url, temp_dir, single_mode=False, series_filter=None):
    global UPDATE_AVAILABLE
    log(worker_id, "Thread active.")
    os.makedirs(temp_dir, exist_ok=True)
    
    def update_status(msg):
        with PROGRESS_LOCK: WORKER_PROGRESS[worker_id] = msg
        
    def post_status(status, progress=0, duration=0):
        try:
            payload = {"worker_id":worker_id, "job_id":job_id, "status":status, "progress":progress}
            if duration > 0: payload["duration"] = duration
            requests.post(f"{manager_url}/report_status", json=payload, headers=get_auth_headers(), timeout=10)
        except: pass

    while not SHUTDOWN_EVENT.is_set():
        if PAUSE_REQUESTED:
             time.sleep(1); continue

        try:
            update_status("Idle")
            if check_version(manager_url):
                UPDATE_AVAILABLE = True; SHUTDOWN_EVENT.set(); break

            try: 
                # [CHANGED] Pass Series Filter to Manager
                params = {}
                if series_filter: params['series'] = series_filter
                
                r = requests.get(f"{manager_url}/get_job", params=params, headers=get_auth_headers(), timeout=10)
            except: time.sleep(5); continue

            data = r.json() if r.status_code == 200 else None
            
            if r.status_code == 401:
                log(worker_id, "AUTH FAILED: Worker Secret is invalid.", "CRITICAL")
                SHUTDOWN_EVENT.set(); break

            if data and data.get("status") == "ok":
                job = data["job"]; job_id = job['id']; dl_url = job['download_url']
                log(worker_id, f"Job: {job['filename']}")
                
                local_src = os.path.join(temp_dir, "source.tmp")
                local_dst = os.path.join(temp_dir, f"encoded{ENCODING_CONFIG['OUTPUT_EXT']}")
                
                # --- DOWNLOAD ---
                post_status("downloading", 0)
                
                try:
                    with requests.get(dl_url, stream=True, timeout=30) as r:
                        r.raise_for_status()
                        total_size = int(r.headers.get('content-length', 0))
                        downloaded = 0; last_rep = 0
                        with open(local_src, 'wb') as f:
                            for chunk in r.iter_content(chunk_size=8192):
                                if PAUSE_REQUESTED: 
                                    while PAUSE_REQUESTED: time.sleep(1)
                                    if SHUTDOWN_EVENT.is_set(): raise Exception("Shutdown")

                                f.write(chunk)
                                downloaded += len(chunk)
                                pct = int((downloaded/total_size)*100) if total_size > 0 else 0
                                
                                if single_mode: print_progress(worker_id, downloaded, total_size, prefix='DL')
                                else: update_status(f"DL {pct}%")

                                if time.time() - last_rep > 30:
                                    post_status("downloading", pct)
                                    last_rep = time.time()
                    if single_mode: print_progress(worker_id, total_size, total_size, prefix='DL', suffix='OK')
                except Exception as e:
                    log(worker_id, f"Download failed: {e}", "ERROR")
                    post_status("failed")
                    time.sleep(5); continue

                # --- PROBE ---
                update_status("Probing")
                total_sec = 0; total_min = 0; audio_index = 0; subtitle_indices = []
                try:
                    cmd_probe = ['ffprobe', '-v', 'quiet', '-print_format', 'json', '-show_streams', '-show_format', local_src]
                    res = subprocess.run(cmd_probe, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
                    probe_data = json.loads(res.stdout)
                    dur = probe_data.get('format', {}).get('duration')
                    if dur: total_sec = float(dur); total_min = int(total_sec / 60)
                    
                    audio_streams = [s for s in probe_data.get('streams', []) if s['codec_type'] == 'audio']
                    if audio_streams:
                        audio_index = audio_streams[0]['index']
                        for s in audio_streams:
                            if s.get('tags', {}).get('language', '').lower() in ['eng', 'en', 'english']:
                                audio_index = s['index']; break
                    for s in probe_data.get('streams', []):
                        if s['codec_type'] == 'subtitle':
                            if s.get('codec_name', '').lower() in ['subrip', 'ass', 'webvtt', 'mov_text', 'text', 'srt', 'ssa']:
                                subtitle_indices.append(s['index'])
                except: pass

                # --- ENCODE ---
                log(worker_id, f"Encoding ({total_min}m)...")
                post_status("processing", 0, total_min)
                
                cmd = ['ffmpeg', '-y', '-i', local_src, '-map', '0:v:0', '-map', f'0:{audio_index}']
                for idx in subtitle_indices: cmd.extend(['-map', f'0:{idx}'])
                cmd.extend(['-c:v', ENCODING_CONFIG["VIDEO_CODEC"], '-preset', ENCODING_CONFIG["VIDEO_PRESET"], '-crf', ENCODING_CONFIG["VIDEO_CRF"], '-pix_fmt', ENCODING_CONFIG["VIDEO_PIX_FMT"], '-vf', ENCODING_CONFIG["VIDEO_SCALE"], '-c:a', ENCODING_CONFIG["AUDIO_CODEC"], '-b:a', ENCODING_CONFIG["AUDIO_BITRATE"], '-ac', ENCODING_CONFIG["AUDIO_CHANNELS"], '-c:s', ENCODING_CONFIG["SUBTITLE_CODEC"], '-progress', 'pipe:1', local_dst])
                
                start_enc = time.time(); last_rep = 0
                
                popen_kwargs = {}
                if platform.system() == 'Windows':
                    popen_kwargs['creationflags'] = subprocess.CREATE_NEW_PROCESS_GROUP
                else:
                    popen_kwargs['start_new_session'] = True

                proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL, text=True, **popen_kwargs)
                
                with PROC_LOCK: ACTIVE_PROCS[worker_id] = proc
                
                while True:
                    if PAUSE_REQUESTED:
                        time.sleep(0.2); continue

                    line = proc.stdout.readline()
                    if not line and proc.poll() is not None: break
                    
                    if "out_time=" in line and "N/A" not in line and total_sec > 0:
                        try:
                            time_str = line.split('=')[1].strip()
                            curr_sec = get_seconds(time_str)
                            pct = int((curr_sec/total_sec)*100)
                            
                            if single_mode: print_progress(worker_id, curr_sec, total_sec, prefix='Enc')
                            else: update_status(f"Enc {pct}%")
                            
                            if time.time() - last_rep > 10:
                                post_status("processing", pct)
                                last_rep = time.time()
                        except: pass
                
                with PROC_LOCK:
                    if worker_id in ACTIVE_PROCS: del ACTIVE_PROCS[worker_id]

                enc_time = time.time() - start_enc
                if single_mode: print_progress(worker_id, total_sec, total_sec, prefix='Enc', suffix='OK')

                if proc.returncode == 0 and os.path.exists(local_dst):
                    final_size = os.path.getsize(local_dst) / 1024 / 1024
                    log(worker_id, f"Encode done ({enc_time:.0f}s, {final_size:.2f}MB). Uploading...")
                    post_status("uploading", 0)
                    
                    # --- UPLOAD ---
                    class ProgressFileReader:
                        def __init__(self, filename, callback):
                            self._f = open(filename, 'rb'); self._total = os.path.getsize(filename)
                            self._read = 0; self._callback = callback; self._last_time = 0
                        def __enter__(self): return self
                        def __exit__(self, exc_type, exc_val, exc_tb): self._f.close()
                        def read(self, size=-1):
                            if PAUSE_REQUESTED:
                                while PAUSE_REQUESTED: time.sleep(1)
                            data = self._f.read(size); self._read += len(data)
                            pct = int((self._read / self._total) * 100)
                            if single_mode: print_progress(worker_id, self._read, self._total, prefix='Up')
                            else: update_status(f"Up {pct}%")
                            if time.time() - self._last_time > 30:
                                self._callback(pct); self._last_time = time.time()
                            return data
                        def __getattr__(self, attr): return getattr(self._f, attr)

                    def upload_cb(pct): post_status("uploading", pct)

                    with ProgressFileReader(local_dst, upload_cb) as f:
                        # [FIX] Added auth headers
                        requests.post(f"{manager_url}/upload_result", 
                                      files={'file': (job_id, f)}, 
                                      data={'job_id': job_id, 'worker_id': worker_id, 'duration': total_min},
                                      headers=get_auth_headers())
                    
                    if single_mode: print_progress(worker_id, 100, 100, prefix='Up', suffix='OK')
                    log(worker_id, "Job complete.")
                else:
                    if SHUTDOWN_EVENT.is_set() and proc.returncode != 0: log(worker_id, "Aborted.", "WARN")
                    else: log(worker_id, f"FFmpeg failed (RC: {proc.returncode})", "ERROR")
                    post_status("failed")

                if os.path.exists(local_src): os.remove(local_src)
                if os.path.exists(local_dst): os.remove(local_dst)
            else:
                if single_mode:
                    with CONSOLE_LOCK:
                        sys.stdout.write(f"\033[2K\r[{datetime.now().strftime('%H:%M:%S')}] [{worker_id}] Idle. Waiting...")
                        sys.stdout.flush()
                time.sleep(10)
        except Exception as e:
            log(worker_id, f"Error: {e}", "CRITICAL")
            time.sleep(10)

def run_worker(args):
    print("==================================================")
    print(" FRACTUM DISTRIBUTED WORKER")
    print("==================================================")
    check_ffmpeg()
    
    manager_url = (args.manager or DEFAULT_MANAGER_URL).rstrip('/')
    username = args.username or DEFAULT_USERNAME
    base_workername = args.workername or DEFAULT_WORKERNAME
    
    # [FIX] Ensure WORKER_SECRET is available
    if not WORKER_SECRET:
        print("[!] ERROR: WORKER_SECRET not set. Please reinstall using the manager's install command.")
        sys.exit(1)

    if not verify_connection(manager_url): sys.exit(1)
    if check_version(manager_url): apply_update(manager_url)
    
    num_jobs = args.jobs if args.jobs > 0 else 1
    if num_jobs > 32: num_jobs = 32
    
    signal.signal(signal.SIGINT, signal_handler)
    
    threads = []
    worker_ids = []
    single_mode = (num_jobs == 1)
    
    # [NEW] Series Filter Log
    if args.series:
        print(f"[*] SERIES FILTER ACTIVE: Only processing jobs matching '{args.series}'")
    
    for i in range(num_jobs):
        worker_id = f"{username}-{base_workername}-{i+1}"
        worker_ids.append(worker_id)
        temp_dir = f"./temp_encode_{base_workername}_{i+1}"
        
        # [CHANGED] Pass Series Filter
        t = threading.Thread(target=worker_task, args=(worker_id, manager_url, temp_dir, single_mode, args.series))
        t.daemon = True
        t.start()
        threads.append(t)

    if not single_mode:
        monitor_t = threading.Thread(target=monitor_status_loop, args=(worker_ids,))
        monitor_t.daemon = True
        monitor_t.start()
        
    global PAUSE_REQUESTED
    while True:
        if not PAUSE_REQUESTED:
            all_dead = True
            for t in threads:
                if t.is_alive(): all_dead = False; break
            if all_dead: break
            if SHUTDOWN_EVENT.is_set() and not PAUSE_REQUESTED: break 
            time.sleep(0.5)
            continue
        
        toggle_processes(suspend=True)
        print("\n" + "="*40)
        print(" [!] WORKER PAUSED")
        print("="*40)
        print(" [C]ontinue  - Resume encoding")
        print(" [F]inish    - Finish active, then stop")
        print(" [S]top      - Abort immediately")
        
        while PAUSE_REQUESTED:
            try:
                choice = input("Select [c/f/s]: ").strip().lower()
                if choice == 'c':
                    print("[*] Resuming...")
                    PAUSE_REQUESTED = False
                    toggle_processes(suspend=False)
                elif choice == 'f':
                    print("[*] Draining jobs...")
                    PAUSE_REQUESTED = False
                    toggle_processes(suspend=False)
                    SHUTDOWN_EVENT.set()
                elif choice == 's':
                    print("[*] Aborting...")
                    toggle_processes(suspend=False)
                    kill_processes()
                    SHUTDOWN_EVENT.set()
                    PAUSE_REQUESTED = False
                    sys.exit(0)
            except (EOFError, KeyboardInterrupt):
                sys.stdout.write("\n")
                time.sleep(0.5)
                continue
            except Exception: time.sleep(0.5)
            
    if UPDATE_AVAILABLE: apply_update(manager_url)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--manager", default=DEFAULT_MANAGER_URL)
    parser.add_argument("--username", default=DEFAULT_USERNAME)
    parser.add_argument("--workername", default=DEFAULT_WORKERNAME)
    parser.add_argument("--jobs", type=int, default=1)
    parser.add_argument("--series", default="", help="Only process jobs from this folder name") # [NEW] Argument
    args = parser.parse_args()
    run_worker(args)
