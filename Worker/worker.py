import requests, subprocess, os, sys, time, json, platform, uuid, signal, threading, shutil, argparse, re
from ftplib import FTP

# --- VERSION CONTROL ---
VERSION = "1.4.0" 
REPO_URL = "https://raw.githubusercontent.com/FractumSeraph/DistributedEncodes/main/worker.py"

# [CHANGE ME] Connection Config
MANAGER_URL = "http://transcode.fractumseraph.net:5000"
API_TOKEN = "FractumSecure2025"
FTP_HOST = "transcode.fractumseraph.net"
FTP_USER = "transcode"
FTP_PASS = "transcode"

# --- ARGUMENT PARSING ---
parser = argparse.ArgumentParser()
parser.add_argument("--stealth", action="store_true", help="Enable Stealth Mode")
parser.add_argument("--jobs", type=int, default=1, help="Number of concurrent encodes")
parser.add_argument("--no-update", action="store_true", help="Skip update check")
parser.add_argument("-u", "--user", type=str, help="Set Username (Overrides config)")
parser.add_argument("-w", "--worker", type=str, help="Set Worker Name (Overrides config)")
args = parser.parse_args()

STEALTH_MODE = args.stealth
MAX_JOBS = args.jobs
SKIP_UPDATE = args.no_update

if getattr(sys, 'frozen', False): APP_DIR = os.path.dirname(sys.executable)
else: APP_DIR = os.path.dirname(os.path.abspath(__file__))

RECOVERY_DIR = os.path.join(APP_DIR, "recovery")
if not os.path.exists(RECOVERY_DIR): os.makedirs(RECOVERY_DIR)

# --- PLATFORM SETUP ---
if platform.system() == "Windows":
    HANDBRAKE_EXE = os.path.join(APP_DIR, "HandBrakeCLI.exe")
    FFPROBE_EXE = os.path.join(APP_DIR, "ffprobe.exe")
else:
    HANDBRAKE_EXE = "HandBrakeCLI"
    FFPROBE_EXE = "ffprobe"

PRESET_FILE = os.path.join(APP_DIR, "FractumAV1.json")
CONFIG_FILE = os.path.join(APP_DIR, "user_config.json")
HEADERS = {"X-Auth-Token": API_TOKEN}

# --- THREAD-SAFE GLOBAL STATE ---
ACTIVE_WORKERS = {}
WORKER_LOCK = threading.Lock()
EXIT_FLAG = threading.Event()

# State is now a dict: {'state': 'Idle', 'pct': 0.0, 'info': ''}
WORKER_STATE = {f"W{i+1}": {"state": "Idle", "pct": 0.0, "info": ""} for i in range(MAX_JOBS)}

# --- AUTO-UPDATE FUNCTION ---
def check_for_updates():
    if SKIP_UPDATE: return
    print(f":: SYSTEM :: Checking for updates (Current: {VERSION})...")
    try:
        r = requests.get(REPO_URL, timeout=5)
        if r.status_code != 200: 
            print("   [!] Could not reach GitHub. Skipping update.")
            return
        remote_code = r.text
        match = re.search(r'VERSION\s*=\s*"([^"]+)"', remote_code)
        if not match: return
        remote_version = match.group(1)
        
        if remote_version != VERSION:
            print(f"   [!] New version found: {remote_version}")
            if getattr(sys, 'frozen', False):
                print("   " + "="*50)
                print("   [!] CRITICAL: CLIENT OUT OF DATE")
                print("   " + "="*50)
                input("   Press Enter to continue...")
            else:
                print(f"   [+] Installing update...")
                script_path = os.path.abspath(__file__)
                shutil.copy2(script_path, script_path + ".bak")
                with open(script_path, 'w', encoding='utf-8') as f:
                    f.write(remote_code)
                print("   [+] Update installed. Restarting...")
                time.sleep(1)
                os.execv(sys.executable, [sys.executable] + sys.argv)
        else:
            print("   [OK] System is up to date.")
    except: pass

def update_status(tid, state, pct=0.0, info=""):
    WORKER_STATE[tid] = {"state": state, "pct": pct, "info": info}

def dashboard_loop():
    BAR_WIDTH = 30
    BLOCK = "█"
    EMPTY = "-"

    while not EXIT_FLAG.is_set():
        if STEALTH_MODE:
            active_count = sum(1 for v in WORKER_STATE.values() if v['state'] != "Idle")
            sys.stdout.write(f"\r:: FRACTUM SECURITY :: SYSTEM LOAD: {active_count} ACTIVE PROCESSES" + " "*20)
        
        elif MAX_JOBS == 1:
            data = WORKER_STATE["W1"]
            state = data['state']
            
            # Show Bar for Encoding, Downloading, and Uploading
            if state in ["Encoding", "Downloading", "Uploading"]:
                pct = data['pct']
                filled = int(BAR_WIDTH * pct / 100)
                bar = BLOCK * filled + EMPTY * (BAR_WIDTH - filled)
                # Output: [██████----] 60.5% | 24fps | ETA: 00h10m
                sys.stdout.write(f"\r[{bar}] {pct:.1f}% | {state} {data['info']}   ")
            else:
                sys.stdout.write(f"\r>> STATUS: {state} {data['info']}" + " "*20)

        else:
            status_line = ""
            for tid in sorted(WORKER_STATE.keys()):
                d = WORKER_STATE[tid]
                # Show percentage for transfer states too
                if d['state'] in ["Encoding", "Downloading", "Uploading"]:
                    val = f"{d['state'][:1]}:{d['pct']:.0f}%" 
                else:
                    val = d['state']
                status_line += f"[{tid}: {val}] "
            sys.stdout.write(f"\r{status_line}" + " "*10)
        
        sys.stdout.flush()
        time.sleep(0.5)

def log(msg):
    if not STEALTH_MODE:
        sys.stdout.write(f"\n{msg}\n")

def register_worker_activity(thread_id, proc=None, job_id=None, files=None):
    with WORKER_LOCK:
        if thread_id not in ACTIVE_WORKERS: ACTIVE_WORKERS[thread_id] = {}
        if proc: ACTIVE_WORKERS[thread_id]["proc"] = proc
        if job_id: ACTIVE_WORKERS[thread_id]["job_id"] = job_id
        if files: ACTIVE_WORKERS[thread_id]["files"] = files

def clear_worker_activity(thread_id):
    with WORKER_LOCK:
        if thread_id in ACTIVE_WORKERS:
            del ACTIVE_WORKERS[thread_id]

def graceful_exit(signum, frame):
    EXIT_FLAG.set() 
    sys.stdout.write("\n\n[!] INTERRUPT DETECTED. STOPPING...\n")
    
    with WORKER_LOCK:
        for tid, data in ACTIVE_WORKERS.items():
            if data.get("proc"):
                try: 
                    data["proc"].kill()
                except: pass
            
            if data.get("job_id"):
                try: requests.post(f"{MANAGER_URL}/fail_job", json={"id": data["job_id"]}, headers=HEADERS, timeout=1)
                except: pass
                
            if data.get("files"):
                for f in data["files"]:
                    if os.path.exists(f):
                        try: os.remove(f)
                        except: pass
    
    print(":: SYSTEM HALTED ::")
    os._exit(0)

signal.signal(signal.SIGINT, graceful_exit)
signal.signal(signal.SIGTERM, graceful_exit)

def get_config():
    config = {}
    if os.path.exists(CONFIG_FILE):
        try: config = json.load(open(CONFIG_FILE))
        except: pass

    if args.user: config['username'] = args.user
    if args.worker: config['worker_name'] = args.worker

    if 'username' in config and 'worker_name' in config: return config

    print("\n:: FIRST RUN CONFIGURATION ::")
    if 'username' not in config:
        user_input = input("Enter Username (Default: Anonymous): ").strip()
        config['username'] = user_input if user_input else "Anonymous"
    
    if 'worker_name' not in config:
        default_worker = platform.node()
        worker_input = input(f"Enter Worker Name (Default: {default_worker}): ").strip()
        config['worker_name'] = worker_input if worker_input else default_worker

    with open(CONFIG_FILE, 'w') as f: json.dump(config, f)
    return config

def get_metadata(path):
    cmd = [FFPROBE_EXE, "-v", "error", "-select_streams", "v:0", "-show_entries", "stream=height,codec_name", "-show_entries", "format=duration", "-of", "json", path]
    try:
        stderr_mode = subprocess.DEVNULL
        data = json.loads(subprocess.check_output(cmd, text=True, stderr=stderr_mode))
        return {"height": int(data['streams'][0]['height']), "codec_name": data['streams'][0]['codec_name'], "duration": float(data['format']['duration'])}
    except: return None

def stash_job(local_output, job_data):
    filename = os.path.basename(local_output)
    recovery_path = os.path.join(RECOVERY_DIR, filename)
    try:
        if os.path.exists(local_output): os.rename(local_output, recovery_path)
        with open(recovery_path + ".json", 'w') as f: json.dump(job_data, f)
        log(f"[!] Stashed Job #{job_data['id']}")
    except Exception as e:
        log(f"[CRITICAL] Stash failed: {e}")

def retry_stashed():
    stashed = [f for f in os.listdir(RECOVERY_DIR) if f.endswith(".json")]
    if not stashed: return
    print(f":: RECOVERY :: Found {len(stashed)} pending uploads.")
    for meta_file in stashed:
        json_path = os.path.join(RECOVERY_DIR, meta_file)
        try:
            with open(json_path, 'r') as f: job_data = json.load(f)
            video_path = os.path.join(RECOVERY_DIR, os.path.basename(job_data['local_path']))
            if not os.path.exists(video_path): 
                os.remove(json_path); continue

            print(f"    >> Retrying Job #{job_data['id']}...")
            ftp = FTP(FTP_HOST); ftp.login(FTP_USER, FTP_PASS)
            try: ftp.cwd("completed")
            except: pass
            with open(video_path, 'rb') as f: ftp.storbinary(f"STOR {job_data['remote_name']}", f)
            ftp.quit()
            
            requests.post(f"{MANAGER_URL}/complete_job", json=job_data, headers=HEADERS)
            os.remove(video_path); os.remove(json_path)
            print(f"    [SUCCESS] Recovered Job #{job_data['id']}")
        except Exception as e:
            print(f"    [!] Recovery failed: {e}")

def heartbeat_loop(job_id, stop_event, tid):
    while not stop_event.is_set():
        if EXIT_FLAG.is_set(): break
        try: 
            pct = int(WORKER_STATE[tid]['pct'])
            requests.post(f"{MANAGER_URL}/heartbeat", json={"id": job_id, "progress": pct}, headers=HEADERS, timeout=5)
        except: pass
        time.sleep(60)

def safe_ftp_cwd(ftp, path):
    if not path or path == ".": return
    for folder in path.split("/"):
        if folder: 
            try: ftp.cwd(folder)
            except: pass

# --- PROGRESS HELPER FOR UPLOAD ---
class ProgressReader:
    def __init__(self, path, tid):
        self.f = open(path, 'rb')
        self.size = os.path.getsize(path)
        self.sent = 0
        self.tid = tid
        
    def read(self, block_size):
        if EXIT_FLAG.is_set(): raise InterruptedError("Stop")
        chunk = self.f.read(block_size)
        self.sent += len(chunk)
        if self.size:
            pct = (self.sent / self.size) * 100
            info = f"({self.sent // (1024*1024)}/{self.size // (1024*1024)} MB)"
            update_status(self.tid, "Uploading", pct=pct, info=info)
        return chunk
        
    def close(self):
        self.f.close()

def process(job, username):
    if EXIT_FLAG.is_set(): return

    tid = threading.current_thread().name
    job_path = job['filename'].replace("\\", "/")
    real_output_name = "av1_" + job_path.replace("/", "_")
    local_input = f"{tid}_{os.path.basename(job_path)}"
    local_output = f"{tid}_{real_output_name}"

    if not STEALTH_MODE:
        log(f"[+] Job {job['id']}: {os.path.basename(job_path)}")

    register_worker_activity(tid, job_id=job['id'], files=[local_input, local_output])

    # SAFETY CHECK: Disk Space
    try:
        total, used, free = shutil.disk_usage(APP_DIR)
        if free < (10 * 1024 * 1024 * 1024): 
            log(f"[!] Low Disk Space ({free // (1024*1024)} MB free). Pausing...")
            requests.post(f"{MANAGER_URL}/fail_job", json={"id": job['id']}, headers=HEADERS)
            clear_worker_activity(tid)
            update_status(tid, "NoDisk")
            time.sleep(300)
            return 
    except: pass

    # DOWNLOAD
    update_status(tid, "Downloading")
    download_success = False
    
    for attempt in range(3):
        if EXIT_FLAG.is_set(): return
        try:
            ftp = FTP(FTP_HOST); ftp.login(FTP_USER, FTP_PASS)
            try: ftp.cwd("source") 
            except: pass 
            safe_ftp_cwd(ftp, os.path.dirname(job_path))
            
            # [NEW] Get Size and use Callback for Progress
            try: total_size = ftp.size(os.path.basename(job_path))
            except: total_size = 0
            
            downloaded = 0
            def dl_callback(data):
                nonlocal downloaded
                if EXIT_FLAG.is_set(): raise InterruptedError("Stop")
                f.write(data)
                downloaded += len(data)
                if total_size:
                    pct = (downloaded / total_size) * 100
                    info = f"({downloaded // (1024*1024)}/{total_size // (1024*1024)} MB)"
                    update_status(tid, "Downloading", pct=pct, info=info)
            
            with open(local_input, 'wb') as f: 
                ftp.retrbinary(f"RETR {os.path.basename(job_path)}", dl_callback, blocksize=8192)
            
            ftp.quit(); download_success = True; break
        except InterruptedError: return
        except: time.sleep(5)
            
    if not download_success:
        if os.path.exists(local_input): os.remove(local_input)
        if not EXIT_FLAG.is_set(): 
            requests.post(f"{MANAGER_URL}/fail_job", json={"id": job['id']}, headers=HEADERS)
        clear_worker_activity(tid)
        update_status(tid, "Idle")
        return

    if EXIT_FLAG.is_set(): return

    # ENCODE
    hb_stop = threading.Event()
    hb_thread = threading.Thread(target=heartbeat_loop, args=(job['id'], hb_stop, tid), daemon=True)
    hb_thread.start()

    try:
        update_status(tid, "Starting")
        with open(PRESET_FILE) as f: preset_name = json.load(f)['PresetList'][0]['PresetName']
        cmd = [HANDBRAKE_EXE, "--preset-import-file", PRESET_FILE, "-Z", preset_name, "-i", local_input, "-o", local_output]
        
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        register_worker_activity(tid, proc=proc)
        
        for line in proc.stdout:
            if EXIT_FLAG.is_set(): 
                proc.kill()
                break

            if "Encoding" in line:
                try:
                    pct_m = re.search(r"(\d+\.\d+) %", line)
                    fps_m = re.search(r"(\d+\.\d+) fps", line)
                    eta_m = re.search(r"eta (\w+)", line)

                    pct = float(pct_m.group(1)) if pct_m else 0.0
                    info_str = ""
                    if fps_m: info_str += f"{fps_m.group(1)} fps"
                    if eta_m: info_str += f" | ETA: {eta_m.group(1)}"
                    
                    update_status(tid, "Encoding", pct=pct, info=info_str)
                except:
                    update_status(tid, "Encoding", pct=0.0, info="...")
        
        proc.wait()
    finally:
        hb_stop.set(); hb_thread.join()

    if EXIT_FLAG.is_set(): return

    # UPLOAD
    update_status(tid, "Uploading")
    uploaded = False
    meta = get_metadata(local_output)
    job_payload = {"id": job['id'], "username": username, "metadata": meta, "encoding_log": "Log omitted", 
                   "local_path": local_output, "remote_name": real_output_name}

    if meta:
        for attempt in range(3):
            if EXIT_FLAG.is_set(): return
            try:
                ftp = FTP(FTP_HOST); ftp.login(FTP_USER, FTP_PASS)
                try: ftp.cwd("completed")
                except: pass 
                
                # [NEW] Use ProgressReader wrapper
                reader = ProgressReader(local_output, tid)
                try:
                    ftp.storbinary(f"STOR {real_output_name}", reader, blocksize=8192)
                finally:
                    reader.close()
                
                ftp.quit()
                requests.post(f"{MANAGER_URL}/complete_job", json=job_payload, headers=HEADERS)
                uploaded = True; break
            except InterruptedError: return
            except: time.sleep(10)
    else:
        log(f"[!] Encoding Failed. Output file invalid: {local_output}")

    try: os.remove(local_input)
    except: pass

    if uploaded:
        try: os.remove(local_output)
        except: pass
    elif meta and not EXIT_FLAG.is_set():
        stash_job(local_output, job_payload)
    else:
        if not EXIT_FLAG.is_set():
            requests.post(f"{MANAGER_URL}/fail_job", json={"id": job['id']}, headers=HEADERS)
        try: os.remove(local_output)
        except: pass
    
    clear_worker_activity(tid)
    update_status(tid, "Idle")

def worker_loop(config):
    tid = threading.current_thread().name
    username = config['username']
    worker_name = config['worker_name']
    display_name = f"{username} [{worker_name}]"

    while not EXIT_FLAG.is_set():
        try:
            time.sleep(1) 
            if EXIT_FLAG.is_set(): break

            r = requests.post(f"{MANAGER_URL}/get_job", json={"username": display_name}, headers=HEADERS, timeout=5)
            data = r.json()
            
            if EXIT_FLAG.is_set(): break
            
            if data['status'] == 'found': 
                process(data, username)
            else: 
                update_status(tid, "Waiting")
                time.sleep(10)
        except: 
            update_status(tid, "Error")
            time.sleep(10)

def main():
    if platform.system() == "Windows":
        if not os.path.exists(HANDBRAKE_EXE): print("Missing HandBrakeCLI.exe"); return
    else:
        if not shutil.which(HANDBRAKE_EXE): print(f"CRITICAL: '{HANDBRAKE_EXE}' not found."); return

    check_for_updates()
    retry_stashed()
    config = get_config()
    username = config['username']
    worker = config['worker_name']
    
    print(f"\n:: FRACTUM NODE :: USER: {username} :: WORKER: {worker} :: THREADS: {MAX_JOBS}")
    
    dash_t = threading.Thread(target=dashboard_loop, daemon=True)
    dash_t.start()
    
    threads = []
    for i in range(MAX_JOBS):
        t = threading.Thread(target=worker_loop, args=(config,), name=f"W{i+1}")
        t.start()
        threads.append(t)
        time.sleep(0.5)
    
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        graceful_exit(None, None)

if __name__ == "__main__": main()
