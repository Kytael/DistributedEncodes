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
WORKER_VERSION = "1.0.5" # Bumped version for new logging features

# --- UPDATE COORDINATION ---
SHUTDOWN_EVENT = threading.Event()
UPDATE_AVAILABLE = False
LAST_UPDATE_CHECK = 0
CHECK_LOCK = threading.Lock()

# --- CONSOLE MANAGEMENT ---
CONSOLE_LOCK = threading.Lock()
PROGRESS_LOCK = threading.Lock()
WORKER_PROGRESS = {} 

# ==============================================================================
# ENCODING CONFIGURATION
# ==============================================================================
ENCODING_CONFIG = {
    # VIDEO SETTINGS
    "VIDEO_CODEC": "libsvtav1",
    "VIDEO_PRESET": "2",
    "VIDEO_CRF": "63",           
    "VIDEO_PIX_FMT": "yuv420p",  
    "VIDEO_SCALE": "scale=-2:480",
    
    # AUDIO SETTINGS
    "AUDIO_CODEC": "libopus",
    "AUDIO_BITRATE": "12k",      
    "AUDIO_CHANNELS": "1",       
    
    # SUBTITLE SETTINGS
    "SUBTITLE_CODEC": "mov_text", 
    
    # CONTAINER
    "OUTPUT_EXT": ".mp4"
}
# ==============================================================================

# ==============================================================================
# HELPERS
# ==============================================================================

def safe_print(message):
    """Thread-safe print that clears the current line (status bar) first."""
    with CONSOLE_LOCK:
        # Clear line with spaces then CR to return to start
        sys.stdout.write('\r' + ' ' * 100 + '\r')
        print(message)
        sys.stdout.flush()

def log(worker_id, message, level="INFO"):
    """Thread-safe logging with timestamps."""
    timestamp = datetime.now().strftime("%H:%M:%S")
    safe_print(f"[{timestamp}] [{worker_id}] [{level}] {message}")

def check_version(manager_url):
    """Checks if a newer version exists. Returns True if update found."""
    global LAST_UPDATE_CHECK
    with CHECK_LOCK:
        # Debounce checks (10 minutes)
        if time.time() - LAST_UPDATE_CHECK < 600:
            return False
        LAST_UPDATE_CHECK = time.time()

    # Only log if we are actually checking to avoid spam
    # safe_print(f"[*] Checking for updates (Current: {WORKER_VERSION})...")
    try:
        url = f"{manager_url}/dl/worker"
        r = requests.get(url, timeout=10)
        
        if r.status_code == 200:
            new_content = r.text
            match = re.search(r'WORKER_VERSION\s*=\s*"([^"]+)"', new_content)
            if match:
                remote_version = match.group(1)
                if remote_version != WORKER_VERSION:
                    safe_print(f"[!] Update found: {WORKER_VERSION} -> {remote_version}")
                    return True
    except Exception as e:
        safe_print(f"[!] Update check failed: {e}")
    
    return False

def apply_update(manager_url):
    """Downloads and restarts the worker."""
    safe_print("[*] Downloading and applying update...")
    try:
        url = f"{manager_url}/dl/worker"
        r = requests.get(url, timeout=30)
        if r.status_code == 200:
            script_path = os.path.abspath(sys.argv[0])
            with open(script_path, 'w', encoding='utf-8') as f:
                f.write(r.text)
                
            safe_print("[*] Restarting worker...")
            os.execv(sys.executable, [sys.executable] + sys.argv)
        else:
            safe_print("[!] Update download failed.")
    except Exception as e:
        safe_print(f"[!] Failed to apply update: {e}")

def print_progress(worker_id, current, total, prefix='', suffix=''):
    """
    Draws a progress bar. 
    Only used in single-thread mode.
    """
    if total <= 0: return
    
    percent = 100 * (current / float(total))
    if percent > 100: percent = 100
    
    length = 40
    filled_length = int(length * current // total)
    bar = '█' * filled_length + '-' * (length - filled_length)
    
    with CONSOLE_LOCK:
        sys.stdout.write(f'\r[{datetime.now().strftime("%H:%M:%S")}] [{worker_id}] {prefix} |{bar}| {percent:.1f}% {suffix}')
        sys.stdout.flush()
    
    if current >= total:
        sys.stdout.write('\n')

def monitor_status_loop(worker_ids):
    """
    Background thread that prints a combined status line for all workers.
    """
    while not SHUTDOWN_EVENT.is_set():
        parts = []
        with PROGRESS_LOCK:
            for wid in sorted(worker_ids, key=lambda x: x.split('-')[-1]):
                try:
                    short_id = wid.split('-')[-1]
                except:
                    short_id = wid
                
                state = WORKER_PROGRESS.get(wid, "Idle")
                parts.append(f"[{short_id}: {state}]")
        
        if parts:
            line = " ".join(parts)
            if len(line) > 110: line = line[:107] + "..."
            
            with CONSOLE_LOCK:
                sys.stdout.write('\r' + line.ljust(110))
                sys.stdout.flush()
        
        time.sleep(0.5)

def get_seconds(t):
    try:
        parts = t.split(':')
        h = int(parts[0]); m = int(parts[1]); s = float(parts[2])
        return h*3600 + m*60 + s
    except: return 0

# --- FFMPEG INSTALLATION (Collapsed for brevity) ---
def install_ffmpeg_windows():
    print("[*] Downloading FFmpeg for Windows...")
    url = "https://github.com/BtbN/FFmpeg-Builds/releases/download/latest/ffmpeg-master-latest-win64-gpl.zip"
    try:
        r = requests.get(url, stream=True)
        r.raise_for_status()
        local_zip = "ffmpeg_temp.zip"
        with open(local_zip, 'wb') as f:
            for chunk in r.iter_content(chunk_size=32768):
                if chunk: f.write(chunk)
        with zipfile.ZipFile(local_zip) as z:
            for name in z.namelist():
                if name.endswith("bin/ffmpeg.exe"):
                    with open("ffmpeg.exe", "wb") as f_out: f_out.write(z.read(name))
                elif name.endswith("bin/ffprobe.exe"):
                    with open("ffprobe.exe", "wb") as f_out: f_out.write(z.read(name))
        os.remove(local_zip)
        return os.path.abspath(".")
    except Exception as e:
        print(f"[!] Failed: {e}"); return None

def check_ffmpeg():
    if platform.system() == 'Windows':
        if not (shutil.which("ffmpeg") and shutil.which("ffprobe")):
            if os.path.exists("ffmpeg.exe"): os.environ["PATH"] += os.pathsep + os.path.abspath(".")
            else: 
                d = install_ffmpeg_windows()
                if d: os.environ["PATH"] += os.pathsep + d

    def has_svtav1():
        try:
            res = subprocess.run(["ffmpeg", "-hide_banner", "-encoders"], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
            return "libsvtav1" in res.stdout
        except: return False

    if shutil.which("ffmpeg") and shutil.which("ffprobe") and has_svtav1():
        return

    print("[!] FFmpeg with SVT-AV1 not found.")
    if platform.system() != 'Windows':
        try: subprocess.run(["sudo", "apt-get", "install", "-y", "ffmpeg"], check=False)
        except: pass
        if not has_svtav1():
             print("[!] Auto-install failed or no SVT-AV1. Please install manually.")
             sys.exit(1)
    else:
        sys.exit(1)

def verify_connection(manager_url):
    print(f"[*] Testing connection to: {manager_url}")
    try:
        if requests.get(manager_url, timeout=10).status_code < 400: return True
    except: pass
    print(f"[!] Could not connect to {manager_url}"); return False

# ==============================================================================
# WORKER LOGIC
# ==============================================================================

def worker_task(worker_id, manager_url, temp_dir, single_mode=False):
    global UPDATE_AVAILABLE
    log(worker_id, "Thread active.")
    os.makedirs(temp_dir, exist_ok=True)
    
    def update_status(msg):
        with PROGRESS_LOCK:
            WORKER_PROGRESS[worker_id] = msg

    while not SHUTDOWN_EVENT.is_set():
        try:
            update_status("Idle")
            if check_version(manager_url):
                UPDATE_AVAILABLE = True; SHUTDOWN_EVENT.set(); break

            try:
                r = requests.get(f"{manager_url}/get_job", timeout=10)
            except:
                time.sleep(5); continue

            data = r.json() if r.status_code == 200 else None
            
            if data and data.get("status") == "ok":
                job = data["job"]; job_id = job['id']; dl_url = job['download_url']
                
                log(worker_id, f"Accepted Job: {job['filename']} (ID: {job_id[:8]}...)")
                
                local_src = os.path.join(temp_dir, "source.tmp")
                local_dst = os.path.join(temp_dir, f"encoded{ENCODING_CONFIG['OUTPUT_EXT']}")
                
                # --- STEP 1: DOWNLOADING ---
                start_dl = time.time()
                requests.post(f"{manager_url}/report_status", json={"worker_id":worker_id, "job_id":job_id, "status":"downloading", "progress":0})
                
                try:
                    with requests.get(dl_url, stream=True, timeout=30) as r:
                        r.raise_for_status()
                        total_size = int(r.headers.get('content-length', 0))
                        log(worker_id, f"Downloading source ({total_size / 1024 / 1024:.2f} MB)...")
                        
                        downloaded = 0
                        last_rep = 0
                        
                        with open(local_src, 'wb') as f:
                            for chunk in r.iter_content(chunk_size=8192): 
                                f.write(chunk)
                                downloaded += len(chunk)
                                pct = int((downloaded/total_size)*100) if total_size > 0 else 0
                                
                                if single_mode: print_progress(worker_id, downloaded, total_size, prefix='Downloading')
                                else: update_status(f"DL {pct}%")

                                if time.time() - last_rep > 30:
                                    requests.post(f"{manager_url}/report_status", json={"worker_id":worker_id, "job_id":job_id, "status":"downloading", "progress":pct})
                                    last_rep = time.time()

                    dl_time = time.time() - start_dl
                    if single_mode: print_progress(worker_id, total_size, total_size, prefix='Downloading', suffix='Done')
                    log(worker_id, f"Download finished in {dl_time:.2f}s.")
                    
                except Exception as e:
                    log(worker_id, f"Download failed: {e}", "ERROR")
                    requests.post(f"{manager_url}/report_status", json={"worker_id":worker_id, "job_id":job_id, "status":"failed"})
                    time.sleep(5); continue

                # --- STEP 2: ANALYZE ---
                update_status("Probing")
                log(worker_id, "Probing media configuration...")
                
                total_sec = 0; total_min = 0; audio_index = 0; subtitle_indices = []
                try:
                    cmd_probe = ['ffprobe', '-v', 'quiet', '-print_format', 'json', '-show_streams', '-show_format', local_src]
                    res = subprocess.run(cmd_probe, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
                    probe_data = json.loads(res.stdout)
                    
                    dur = probe_data.get('format', {}).get('duration')
                    if dur: total_sec = float(dur); total_min = int(total_sec / 60)
                    
                    # Log video stream info just for info
                    vid_stream = next((s for s in probe_data.get('streams', []) if s['codec_type'] == 'video'), None)
                    if vid_stream:
                        log(worker_id, f"Video: {vid_stream.get('codec_name')} {vid_stream.get('width')}x{vid_stream.get('height')}")

                    audio_streams = [s for s in probe_data.get('streams', []) if s['codec_type'] == 'audio']
                    if audio_streams:
                        audio_index = audio_streams[0]['index']
                        for s in audio_streams:
                            if s.get('tags', {}).get('language', '').lower() in ['eng', 'en', 'english']:
                                audio_index = s['index']; break
                        log(worker_id, f"Audio: Selected Stream #{audio_index} (of {len(audio_streams)} tracks)")
                    
                    for s in probe_data.get('streams', []):
                        if s['codec_type'] == 'subtitle':
                            if s.get('codec_name', '').lower() in ['subrip', 'ass', 'webvtt', 'mov_text', 'text', 'srt', 'ssa']:
                                subtitle_indices.append(s['index'])
                    log(worker_id, f"Subtitles: Included {len(subtitle_indices)} text tracks.")

                except Exception as e:
                    log(worker_id, f"Probe Warning: {e}", "WARN")

                # --- STEP 3: ENCODING ---
                log(worker_id, f"Starting Encode. Duration: {total_min}m.")
                requests.post(f"{manager_url}/report_status", json={"worker_id":worker_id, "job_id":job_id, "status":"processing", "progress":0, "duration":total_min})
                
                cmd = ['ffmpeg', '-y', '-i', local_src, '-map', '0:v:0', '-map', f'0:{audio_index}']
                for idx in subtitle_indices: cmd.extend(['-map', f'0:{idx}'])
                cmd.extend(['-c:v', ENCODING_CONFIG["VIDEO_CODEC"], '-preset', ENCODING_CONFIG["VIDEO_PRESET"], '-crf', ENCODING_CONFIG["VIDEO_CRF"], '-pix_fmt', ENCODING_CONFIG["VIDEO_PIX_FMT"], '-vf', ENCODING_CONFIG["VIDEO_SCALE"], '-c:a', ENCODING_CONFIG["AUDIO_CODEC"], '-b:a', ENCODING_CONFIG["AUDIO_BITRATE"], '-ac', ENCODING_CONFIG["AUDIO_CHANNELS"], '-c:s', ENCODING_CONFIG["SUBTITLE_CODEC"], '-progress', 'pipe:1', local_dst])
                
                # Log command snippet for debugging
                log(worker_id, f"FFmpeg: {ENCODING_CONFIG['VIDEO_CODEC']} (CRF {ENCODING_CONFIG['VIDEO_CRF']})")
                
                start_enc = time.time()
                proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
                last_rep = 0
                
                while True:
                    line = proc.stdout.readline()
                    if not line and proc.poll() is not None: break
                    
                    if "out_time=" in line and "N/A" not in line and total_sec > 0:
                        try:
                            time_str = line.split('=')[1].strip()
                            curr_sec = get_seconds(time_str)
                            pct = int((curr_sec/total_sec)*100)
                            
                            if single_mode: print_progress(worker_id, curr_sec, total_sec, prefix='Encoding   ')
                            else: update_status(f"Enc {pct}%")
                            
                            if time.time() - last_rep > 10:
                                requests.post(f"{manager_url}/report_status", json={"worker_id":worker_id, "job_id":job_id, "status":"processing", "progress":pct})
                                last_rep = time.time()
                        except: pass
                
                enc_time = time.time() - start_enc
                if single_mode: print_progress(worker_id, total_sec, total_sec, prefix='Encoding   ', suffix='Done')

                if proc.returncode == 0 and os.path.exists(local_dst):
                    final_size = os.path.getsize(local_dst) / 1024 / 1024
                    log(worker_id, f"Encode success in {enc_time:.1f}s. Output size: {final_size:.2f} MB")
                    
                    # --- STEP 4: UPLOADING ---
                    log(worker_id, "Uploading result...")
                    requests.post(f"{manager_url}/report_status", json={"worker_id":worker_id, "job_id":job_id, "status":"uploading", "progress":0})
                    
                    class ProgressFileReader:
                        def __init__(self, filename, callback):
                            self._f = open(filename, 'rb'); self._total = os.path.getsize(filename)
                            self._read = 0; self._callback = callback; self._last_time = 0
                        def __enter__(self): return self
                        def __exit__(self, exc_type, exc_val, exc_tb): self._f.close()
                        def read(self, size=-1):
                            data = self._f.read(size); self._read += len(data)
                            pct = int((self._read / self._total) * 100)
                            if single_mode: print_progress(worker_id, self._read, self._total, prefix='Uploading  ')
                            else: update_status(f"Up {pct}%")
                            if time.time() - self._last_time > 30:
                                self._callback(pct); self._last_time = time.time()
                            return data
                        def __getattr__(self, attr): return getattr(self._f, attr)

                    def upload_server_callback(pct):
                        try: requests.post(f"{manager_url}/report_status", json={"worker_id":worker_id, "job_id":job_id, "status":"uploading", "progress":pct})
                        except: pass

                    with ProgressFileReader(local_dst, upload_server_callback) as f:
                        requests.post(f"{manager_url}/upload_result", files={'file': (job_id, f)}, data={'job_id': job_id, 'worker_id': worker_id})
                    
                    if single_mode: print_progress(worker_id, 100, 100, prefix='Uploading  ', suffix='Done')
                    log(worker_id, "Upload complete. Job finalized.")
                else:
                    log(worker_id, f"FFmpeg failed. Return code: {proc.returncode}", "ERROR")
                    requests.post(f"{manager_url}/report_status", json={"worker_id":worker_id, "job_id":job_id, "status":"failed"})

                if os.path.exists(local_src): os.remove(local_src)
                if os.path.exists(local_dst): os.remove(local_dst)
            else:
                if single_mode:
                    sys.stdout.write(f"\r[{datetime.now().strftime('%H:%M:%S')}] [{worker_id}] Idle. Waiting...   ")
                    sys.stdout.flush()
                time.sleep(10)
                
        except KeyboardInterrupt: break
        except Exception as e:
            log(worker_id, f"CRITICAL FAILURE: {e}", "CRITICAL")
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
    
    if not verify_connection(manager_url): sys.exit(1)
    if check_version(manager_url): apply_update(manager_url)
    
    num_jobs = args.jobs if args.jobs > 0 else 1
    if num_jobs > 32: num_jobs = 32
    
    print(f"[*] Starting {num_jobs} worker threads.")
    print(f"[*] ID: {username}-{base_workername} | Target: {manager_url}")
    print("==================================================\n")
    
    threads = []
    worker_ids = []
    single_mode = (num_jobs == 1)
    
    for i in range(num_jobs):
        worker_id = f"{username}-{base_workername}-{i+1}"
        worker_ids.append(worker_id)
        temp_dir = f"./temp_encode_{base_workername}_{i+1}"
        
        if num_jobs > 1:
            t = threading.Thread(target=worker_task, args=(worker_id, manager_url, temp_dir, single_mode))
            t.daemon = True
            t.start()
            threads.append(t)
        else:
            worker_task(worker_id, manager_url, temp_dir, single_mode)
            if UPDATE_AVAILABLE: apply_update(manager_url)
            return

    if not single_mode:
        monitor_t = threading.Thread(target=monitor_status_loop, args=(worker_ids,))
        monitor_t.daemon = True
        monitor_t.start()
        
    try:
        while True:
            all_dead = True
            for t in threads:
                if t.is_alive(): all_dead = False; break
            if all_dead: break
            if SHUTDOWN_EVENT.is_set(): pass
            time.sleep(1)
            
        if UPDATE_AVAILABLE: apply_update(manager_url)

    except KeyboardInterrupt:
        print("\n[*] Stopping workers...")
        SHUTDOWN_EVENT.set()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fractum Distributed Worker")
    parser.add_argument("--manager", help="Manager URL", default=DEFAULT_MANAGER_URL)
    parser.add_argument("--username", help="Worker Username", default=DEFAULT_USERNAME)
    parser.add_argument("--workername", help="Worker Name", default=DEFAULT_WORKERNAME)
    parser.add_argument("--jobs", type=int, help="Number of concurrent jobs", default=1)
    args = parser.parse_args()
    run_worker(args)
