import requests, subprocess, os, sys, time, json, platform, uuid, signal, threading, shutil, argparse, re
from ftplib import FTP

# --- VERSION CONTROL ---
VERSION = "1.2.0" 
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
WORKER_STATE = {f"W{i+1}": "Idle" for i in range(MAX_JOBS)}

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
        if not match:
            print("   [!] Could not parse remote version.")
            return
        
        remote_version = match.group(1)
        
        if remote_version != VERSION:
            print(f"   [!] New version found: {remote_version}")
            if getattr(sys, 'frozen', False):
                print("   " + "="*50)
                print("   [!] CRITICAL: YOUR CLIENT IS OUT OF DATE")
                print("   [!] Automatic updates are not supported in the compiled version.")
                print(f"   [!] Local: {VERSION} | Latest: {remote_version}")
                print("   [!] Please download the new executable from GitHub.")
                print("   " + "="*50)
                input("   Press Enter to continue anyway (or Ctrl+C to exit)...")
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
    except Exception as e:
        print(f"   [!] Update check failed: {e}")

def update_status(tid, msg):
    WORKER_STATE[tid] = msg

def dashboard_loop():
    while not EXIT_FLAG.is_set():
        if STEALTH_MODE:
            active_count = sum(1 for v in WORKER_STATE.values() if v != "Idle")
            sys.stdout.write(f"\r:: FRACTUM SECURITY :: SYSTEM LOAD: {active_count} ACTIVE PROCESSES" + " "*20)
        else:
            status_line = ""
            for tid in sorted(WORKER_STATE.keys()):
                status_line += f"[{tid}: {WORKER_STATE[tid]}] "
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
    with WORKER_LOCK:
        for tid, data in ACTIVE_WORKERS.items():
            if data.get("proc"):
                try: data["proc"].kill()
                except: pass
            if data.get("job_id"):
                try: requests.post(f"{MANAGER_URL}/fail_job", json={"id": data["job_id"]}, headers=HEADERS, timeout=2)
                except: pass
            if data.get("files"):
                for f in data["files"]:
                    if os.path.exists(f):
                        try: os.remove(f)
                        except: pass
    print("\n:: SYSTEM HALTED ::")
    sys.exit(0)

signal.signal(signal.SIGINT, graceful_exit)
signal.signal(signal.SIGTERM, graceful_exit)

def get_config():
    config = {}
    if os.path.exists(CONFIG_FILE):
        try: config = json.load(open(CONFIG_FILE))
        except: pass

    if args.user: config['username'] = args.user
    if args.worker: config['worker_name'] = args.worker

    if 'username' in config and 'worker_name' in config:
        return config

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
        try: 
            # Get status, e.g., "45.25%"
            raw_status = WORKER_STATE.get(tid, "0")
            progress = 0
            
            if "%" in raw_status:
                try:
                    # 1. Remove the % sign
                    clean_str = raw_status.replace("%", "").strip()
                    # 2. Convert to Float (45.25), Round it (45), then make it Int (45)
                    progress = int(float(clean_str))
                except: 
                    pass
            
            requests.post(f"{MANAGER_URL}/heartbeat", json={"id": job_id, "progress": progress}, headers=HEADERS, timeout=5)
        except: pass
        time.sleep(60)

def safe_ftp_cwd(ftp, path):
    if not path or path == ".": return
    for folder in path.split("/"):
        if folder: 
            try: ftp.cwd(folder)
            except: pass

def process(job, username):
    tid = threading.current_thread().name
    job_path = job['filename'].replace("\\", "/")
    real_output_name = "av1_" + job_path.replace("/", "_")
    local_input = f"{tid}_{os.path.basename(job_path)}"
    local_output = f"{tid}_{real_output_name}"

    if not STEALTH_MODE:
        log(f"[+] Job {job['id']}: {os.path.basename(job_path)}")

    register_worker_activity(tid, job_id=job['id'], files=[local_input, local_output])

    # SAFETY CHECK: Disk Space (10GB Buffer)
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
    update_status(tid, "Downld")
    download_success = False
    for attempt in range(3):
        try:
            ftp = FTP(FTP_HOST); ftp.login(FTP_USER, FTP_PASS)
            try: ftp.cwd("source") 
            except: pass 
            safe_ftp_cwd(ftp, os.path.dirname(job_path))
            with open(local_input, 'wb') as f: ftp.retrbinary(f"RETR {os.path.basename(job_path)}", f.write)
            ftp.quit(); download_success = True; break
        except: time.sleep(5)
            
    if not download_success:
        if os.path.exists(local_input): os.remove(local_input)
        requests.post(f"{MANAGER_URL}/fail_job", json={"id": job['id']}, headers=HEADERS)
        clear_worker_activity(tid)
        update_status(tid, "Idle")
        return

    # ENCODE
    hb_stop = threading.Event()
    hb_thread = threading.Thread(target=heartbeat_loop, args=(job['id'], hb_stop, tid), daemon=True)
    hb_thread.start()

    try:
        update_status(tid, "Start..")
        with open(PRESET_FILE) as f: preset_name = json.load(f)['PresetList'][0]['PresetName']
        cmd = [HANDBRAKE_EXE, "--preset-import-file", PRESET_FILE, "-Z", preset_name, "-i", local_input, "-o", local_output]
        
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        register_worker_activity(tid, proc=proc)
        
        full_log = ""
        for line in proc.stdout:
            full_log += line
            if "Encoding" in line:
                try:
                    parts = line.split(',')
                    percent_part = [p for p in parts if "%" in p][0]
                    clean_percent = percent_part.strip().split()[0]
                    update_status(tid, f"{clean_percent}%")
                except:
                    update_status(tid, "Enc...")
        proc.wait()
    finally:
        hb_stop.set(); hb_thread.join()

    # UPLOAD
    update_status(tid, "Upload")
    uploaded = False
    meta = get_metadata(local_output)
    job_payload = {"id": job['id'], "username": username, "metadata": meta, "encoding_log": full_log, 
                   "local_path": local_output, "remote_name": real_output_name}

    if meta:
        for attempt in range(3):
            try:
                ftp = FTP(FTP_HOST); ftp.login(FTP_USER, FTP_PASS)
                try: ftp.cwd("completed")
                except: pass 
                with open(local_output, 'rb') as f: ftp.storbinary(f"STOR {real_output_name}", f)
                ftp.quit()
                requests.post(f"{MANAGER_URL}/complete_job", json=job_payload, headers=HEADERS)
                uploaded = True; break
            except: time.sleep(10)

    try: os.remove(local_input)
    except: pass

    if uploaded:
        try: os.remove(local_output)
        except: pass
    else:
        stash_job(local_output, job_payload)
    
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
            r = requests.post(f"{MANAGER_URL}/get_job", json={"username": display_name}, headers=HEADERS, timeout=5)
            data = r.json()
            if data['status'] == 'found': process(data, username)
            else: 
                update_status(tid, "Wait..")
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
    
    for t in threads:
        t.join()

if __name__ == "__main__": main()
