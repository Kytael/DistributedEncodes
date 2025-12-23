import requests, subprocess, os, sys, time, json, platform, signal, threading, shutil, argparse, re
import urllib.request
import zipfile
from ftplib import FTP

# --- VERSION CONTROL ---
VERSION = "2.2.1" 
GITHUB_REPO = "FractumSeraph/DistributedEncodes"
# For Script Users (Linux/Mac)
RAW_URL = f"https://raw.githubusercontent.com/{GITHUB_REPO}/main/worker.py"
# For EXE Users (Windows)
RELEASE_API = f"https://api.github.com/repos/{GITHUB_REPO}/releases/latest"

PRESET_URL = f"https://raw.githubusercontent.com/{GITHUB_REPO}/main/FractumAV1.json"

# [CHANGE ME] Connection Config
MANAGER_URL = "http://transcode.fractumseraph.net:5000"
API_TOKEN = "FractumSecure2025"
FTP_HOST = "transcode.fractumseraph.net"
FTP_USER = "transcode"
FTP_PASS = "transcode"

# --- DEPENDENCY CONFIG ---
WIN_HB_URL = "https://github.com/HandBrake/HandBrake/releases/download/1.10.2/HandBrakeCLI-1.10.2-win-x86_64.zip"
WIN_FF_URL = "https://www.gyan.dev/ffmpeg/builds/ffmpeg-release-essentials.zip"

# --- ARGUMENT PARSING ---
parser = argparse.ArgumentParser()
parser.add_argument("--stealth", action="store_true", help="Enable Stealth Mode")
parser.add_argument("--jobs", type=int, default=1, help="Number of concurrent encodes")
parser.add_argument("--no-update", action="store_true", help="Skip update check")
parser.add_argument("-u", "--user", type=str, help="Set Username (Overrides config)")
parser.add_argument("-w", "--worker", type=str, help="Set Worker Name (Overrides config)")
parser.add_argument("--force-ffmpeg", action="store_true", help="Force FFmpeg backend even if HandBrake exists")
args = parser.parse_args()

STEALTH_MODE = args.stealth
MAX_JOBS = args.jobs
SKIP_UPDATE = args.no_update

if getattr(sys, 'frozen', False): APP_DIR = os.path.dirname(sys.executable)
else: APP_DIR = os.path.dirname(os.path.abspath(__file__))

RECOVERY_DIR = os.path.join(APP_DIR, "recovery")
if not os.path.exists(RECOVERY_DIR): os.makedirs(RECOVERY_DIR)

# --- PLATFORM SETUP ---
HANDBRAKE_EXE = "HandBrakeCLI"
FFMPEG_EXE = "ffmpeg"
FFPROBE_EXE = "ffprobe"

if platform.system() == "Windows":
    HANDBRAKE_EXE = os.path.join(APP_DIR, "HandBrakeCLI.exe")
    FFMPEG_EXE = os.path.join(APP_DIR, "ffmpeg.exe") 
    FFPROBE_EXE = os.path.join(APP_DIR, "ffprobe.exe")

ENCODER_BACKEND = None
PRESET_FILE = os.path.join(APP_DIR, "FractumAV1.json")
CONFIG_FILE = os.path.join(APP_DIR, "user_config.json")
HEADERS = {"X-Auth-Token": API_TOKEN}

# --- THREAD-SAFE GLOBAL STATE ---
ACTIVE_WORKERS = {}
WORKER_LOCK = threading.Lock()
EXIT_FLAG = threading.Event()
WORKER_STATE = {f"W{i+1}": {"state": "Idle", "pct": 0.0, "info": ""} for i in range(MAX_JOBS)}

# --- BOOTSTRAP / INSTALLER LOGIC ---
def is_admin():
    try: return os.getuid() == 0
    except AttributeError:
        import ctypes
        return ctypes.windll.shell32.IsUserAnAdmin() != 0

def bootstrap_preset():
    if not os.path.exists(PRESET_FILE):
        print(":: BOOTSTRAP :: Downloading Recording Preset...")
        try:
            with urllib.request.urlopen(PRESET_URL) as r, open(PRESET_FILE, 'wb') as f:
                f.write(r.read())
            print("   [+] Preset downloaded.")
        except Exception as e:
            print(f"   [!] Failed to download preset: {e}")

def bootstrap_linux():
    print(":: BOOTSTRAP :: Checking Linux Dependencies...")
    missing = []
    if not (shutil.which("HandBrakeCLI") or shutil.which("ffmpeg")):
        missing.append("encoders")
    if not missing: return
    print("   [!] Missing dependencies detected. Attempting install...")
    if shutil.which("apt-get"): pkg_mgr, pkgs = "apt-get", ["handbrake-cli", "ffmpeg"]
    elif shutil.which("dnf"): pkg_mgr, pkgs = "dnf", ["HandBrake-cli", "ffmpeg"]
    elif shutil.which("pacman"): pkg_mgr, pkgs = "pacman", ["handbrake-cli", "ffmpeg"]
    else: print("   [!] Error: Package manager not found."); sys.exit(1)

    if not is_admin():
        print(f"   [!] Root required. Rerunning with sudo..."); 
        try: os.execvp("sudo", ["sudo", sys.executable] + sys.argv)
        except: sys.exit(1)

    subprocess.call([pkg_mgr, "update" if pkg_mgr != "pacman" else "-Sy"])
    install_cmd = [pkg_mgr, "install", "-y" if pkg_mgr != "pacman" else "-S"]
    if pkg_mgr == "pacman": install_cmd.append("--noconfirm")
    install_cmd.extend(pkgs)
    subprocess.call(install_cmd)

def bootstrap_windows():
    print(":: BOOTSTRAP :: Checking Windows Dependencies...")
    if not os.path.exists(HANDBRAKE_EXE):
        print("   [+] Downloading HandBrakeCLI...")
        try:
            with urllib.request.urlopen(WIN_HB_URL) as r, open("hb.zip", 'wb') as f: shutil.copyfileobj(r, f)
            with zipfile.ZipFile("hb.zip", 'r') as z:
                if "HandBrakeCLI.exe" in z.namelist():
                    with open(HANDBRAKE_EXE, "wb") as f: f.write(z.read("HandBrakeCLI.exe"))
            os.remove("hb.zip")
        except: pass

    if not os.path.exists(FFMPEG_EXE) or not os.path.exists(FFPROBE_EXE):
        print("   [+] Downloading FFmpeg tools...")
        try:
            with urllib.request.urlopen(WIN_FF_URL) as r, open("ff.zip", 'wb') as f: shutil.copyfileobj(r, f)
            with zipfile.ZipFile("ff.zip", 'r') as z:
                for name in z.namelist():
                    if name.endswith("bin/ffmpeg.exe"):
                        with open(FFMPEG_EXE, "wb") as f: f.write(z.read(name))
                    elif name.endswith("bin/ffprobe.exe"):
                        with open(FFPROBE_EXE, "wb") as f: f.write(z.read(name))
            os.remove("ff.zip")
        except: pass

def bootstrap_mac():
    print(":: BOOTSTRAP :: Checking macOS Dependencies...")
    if not (shutil.which("HandBrakeCLI") and shutil.which("ffmpeg")):
        if not shutil.which("brew"): print("   [!] Homebrew not found."); sys.exit(1)
        subprocess.call(["brew", "install", "handbrake", "ffmpeg"])

def run_bootstrap():
    bootstrap_preset()
    sys_os = platform.system()
    if sys_os == "Linux": bootstrap_linux()
    elif sys_os == "Windows": bootstrap_windows()
    elif sys_os == "Darwin": bootstrap_mac()

# --- AUTO-UPDATE FUNCTION ---
def check_for_updates():
    if SKIP_UPDATE: return
    
    # Clean up old executable artifacts (Windows)
    if getattr(sys, 'frozen', False):
        try:
            old_exe = sys.executable + ".old"
            if os.path.exists(old_exe): os.remove(old_exe)
        except: pass

    print(f":: SYSTEM :: Checking for updates (Current: {VERSION})...")
    
    # -- PATH 1: COMPILED EXE UPDATE --
    if getattr(sys, 'frozen', False):
        try:
            r = requests.get(RELEASE_API, timeout=5)
            if r.status_code != 200: return
            data = r.json()
            remote_tag = data['tag_name'].lstrip('v')
            
            if remote_tag != VERSION:
                print(f"   [!] New version found: {remote_tag}")
                exe_url = None
                for asset in data['assets']:
                    if asset['name'].endswith(".exe"):
                        exe_url = asset['browser_download_url']
                        break
                
                if not exe_url: print("   [!] No exe asset found."); return

                print("   [+] Downloading update...")
                new_exe = sys.executable + ".new"
                with requests.get(exe_url, stream=True) as r:
                    with open(new_exe, 'wb') as f: shutil.copyfileobj(r.raw, f)
                
                print("   [+] Installing...")
                old_exe = sys.executable + ".old"
                if os.path.exists(old_exe): os.remove(old_exe)
                os.rename(sys.executable, old_exe)
                os.rename(new_exe, sys.executable)
                
                print("   [+] Restarting...")
                subprocess.Popen([sys.executable] + sys.argv[1:])
                sys.exit(0)
            else:
                print("   [OK] System is up to date.")
        except Exception as e:
            print(f"   [!] Update check failed: {e}")

    # -- PATH 2: PYTHON SCRIPT UPDATE --
    else:
        try:
            r = requests.get(RAW_URL, timeout=5)
            if r.status_code != 200: return
            remote_code = r.text
            match = re.search(r'VERSION\s*=\s*"([^"]+)"', remote_code)
            if not match: return
            remote_version = match.group(1)
            
            if remote_version != VERSION:
                print(f"   [!] New version found: {remote_version}")
                print(f"   [+] Installing update...")
                script_path = os.path.abspath(__file__)
                shutil.copy2(script_path, script_path + ".bak")
                with open(script_path, 'w', encoding='utf-8') as f:
                    f.write(remote_code)
                print("   [+] Restarting...")
                time.sleep(1)
                os.execv(sys.executable, [sys.executable] + sys.argv)
            else:
                print("   [OK] System is up to date.")
        except: pass

# --- DASHBOARD & UTILS ---
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
            if state in ["Encoding", "Downloading", "Uploading"]:
                pct = data['pct']
                filled = int(BAR_WIDTH * pct / 100)
                bar = BLOCK * filled + EMPTY * (BAR_WIDTH - filled)
                sys.stdout.write(f"\r[{bar}] {pct:.1f}% | {state} {data['info']}   ")
            else:
                sys.stdout.write(f"\r>> STATUS: {state} {data['info']}" + " "*20)
        else:
            status_line = ""
            for tid in sorted(WORKER_STATE.keys()):
                d = WORKER_STATE[tid]
                val = f"{d['state'][:1]}:{d['pct']:.0f}%" if d['state'] in ["Encoding", "Downloading", "Uploading"] else d['state']
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
                try: data["proc"].kill()
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

def build_encode_cmd(input_file, output_file):
    try:
        with open(PRESET_FILE, 'r') as f: 
            data = json.load(f)
            preset = data['PresetList'][0]
    except:
        log("[!] Error reading Preset JSON. Using Defaults.")
        preset = {}

    height = int(preset.get('PictureHeight', 480))
    rf = int(preset.get('VideoQualitySlider', 28))
    speed = preset.get('VideoPreset', '2') 
    
    if ENCODER_BACKEND == "handbrake":
        return [HANDBRAKE_EXE, "--preset-import-file", PRESET_FILE, "-Z", preset.get('PresetName', 'FractumAV1'), "-i", input_file, "-o", output_file]
    else:
        cmd = [FFMPEG_EXE, "-y", "-v", "error", "-stats", "-i", input_file]
        cmd += ["-c:v", "libsvtav1", "-preset", str(speed), "-crf", str(rf)]
        cmd += ["-vf", f"scale=-2:{height}"] 
        cmd += ["-c:a", "libopus", "-b:a", "128k", "-ac", "2"]
        cmd += [output_file]
        return cmd

def process(job, username):
    if EXIT_FLAG.is_set(): return
    tid = threading.current_thread().name
    job_path = job['filename'].replace("\\", "/")
    
    # [FIX] Force .mp4 extension regardless of source
    flat_name = job_path.replace("/", "_")
    name_no_ext = os.path.splitext(flat_name)[0]
    real_output_name = f"av1_{name_no_ext}.mp4"
    
    local_input = f"{tid}_{os.path.basename(job_path)}"
    local_output = f"{tid}_{real_output_name}"

    if not STEALTH_MODE:
        log(f"[+] Job {job['id']}: {os.path.basename(job_path)}")

    register_worker_activity(tid, job_id=job['id'], files=[local_input, local_output])

    try:
        total, used, free = shutil.disk_usage(APP_DIR)
        if free < (10 * 1024 * 1024 * 1024): 
            log(f"[!] Low Disk Space. Pausing...")
            requests.post(f"{MANAGER_URL}/fail_job", json={"id": job['id']}, headers=HEADERS)
            clear_worker_activity(tid); update_status(tid, "NoDisk"); time.sleep(300); return 
    except: pass

    update_status(tid, "Downloading")
    download_success = False
    for attempt in range(3):
        if EXIT_FLAG.is_set(): return
        try:
            ftp = FTP(FTP_HOST); ftp.login(FTP_USER, FTP_PASS)
            try: ftp.cwd("source"); safe_ftp_cwd(ftp, os.path.dirname(job_path))
            except: pass 
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
        if not EXIT_FLAG.is_set(): requests.post(f"{MANAGER_URL}/fail_job", json={"id": job['id']}, headers=HEADERS)
        clear_worker_activity(tid); update_status(tid, "Idle"); return

    if EXIT_FLAG.is_set(): return

    total_duration_sec = 0
    if ENCODER_BACKEND == "ffmpeg":
        meta_in = get_metadata(local_input)
        if meta_in: total_duration_sec = meta_in.get('duration', 0)

    hb_stop = threading.Event()
    hb_thread = threading.Thread(target=heartbeat_loop, args=(job['id'], hb_stop, tid), daemon=True)
    hb_thread.start()

    try:
        update_status(tid, "Starting")
        cmd = build_encode_cmd(local_input, local_output)
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        register_worker_activity(tid, proc=proc)
        
        for line in proc.stdout:
            if EXIT_FLAG.is_set(): proc.kill(); break
            pct = 0.0
            info_str = "..."
            if ENCODER_BACKEND == "handbrake" and "Encoding" in line:
                try:
                    pct_m = re.search(r"(\d+\.\d+) %", line)
                    if pct_m: pct = float(pct_m.group(1))
                    fps_m = re.search(r"(\d+\.\d+) fps", line)
                    eta_m = re.search(r"eta (\w+)", line)
                    if fps_m: info_str = f"{fps_m.group(1)} fps"
                    if eta_m: info_str += f" | ETA: {eta_m.group(1)}"
                except: pass
            elif ENCODER_BACKEND == "ffmpeg":
                try:
                    time_m = re.search(r"time=(\d+):(\d+):(\d+\.\d+)", line)
                    if time_m and total_duration_sec > 0:
                        h, m, s = map(float, time_m.groups())
                        current_sec = (h * 3600) + (m * 60) + s
                        pct = (current_sec / total_duration_sec) * 100
                    fps_m = re.search(r"fps=\s*(\d+\.?\d*)", line)
                    if fps_m: info_str = f"{fps_m.group(1)} fps"
                except: pass
            if pct > 0: update_status(tid, "Encoding", pct=pct, info=info_str)
        proc.wait()
    finally:
        hb_stop.set(); hb_thread.join()

    if EXIT_FLAG.is_set(): return

    update_status(tid, "Uploading")
    uploaded = False
    meta = get_metadata(local_output)
    job_payload = {"id": job['id'], "username": username, "metadata": meta, "encoding_log": "Log omitted", "local_path": local_output, "remote_name": real_output_name}

    if meta:
        for attempt in range(3):
            if EXIT_FLAG.is_set(): return
            try:
                ftp = FTP(FTP_HOST); ftp.login(FTP_USER, FTP_PASS)
                try: ftp.cwd("completed")
                except: pass 
                reader = ProgressReader(local_output, tid)
                try: ftp.storbinary(f"STOR {real_output_name}", reader, blocksize=8192)
                finally: reader.close()
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
        if not EXIT_FLAG.is_set(): requests.post(f"{MANAGER_URL}/fail_job", json={"id": job['id']}, headers=HEADERS)
        try: os.remove(local_output)
        except: pass
    clear_worker_activity(tid); update_status(tid, "Idle")

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
            if data['status'] == 'found': process(data, username)
            else: update_status(tid, "Waiting"); time.sleep(10)
        except: update_status(tid, "Error"); time.sleep(10)

def main():
    global ENCODER_BACKEND
    run_bootstrap()
    has_hb = shutil.which(HANDBRAKE_EXE) or os.path.exists(HANDBRAKE_EXE)
    has_ff = shutil.which(FFMPEG_EXE) or os.path.exists(FFMPEG_EXE)
    if args.force_ffmpeg:
        if has_ff: ENCODER_BACKEND = "ffmpeg"
        else: print("CRITICAL: Force FFmpeg requested, but ffmpeg not found."); return
    elif has_hb: ENCODER_BACKEND = "handbrake"
    elif has_ff: ENCODER_BACKEND = "ffmpeg"
    else: print("CRITICAL: No backend found."); return

    if not (shutil.which(FFPROBE_EXE) or os.path.exists(FFPROBE_EXE)):
         print("CRITICAL: 'ffprobe' is missing."); return

    check_for_updates()
    retry_stashed()
    config = get_config()
    username = config['username']
    worker = config['worker_name']
    
    print(f"\n:: FRACTUM NODE :: BACKEND: {ENCODER_BACKEND.upper()} :: USER: {username} :: WORKER: {worker}")
    dash_t = threading.Thread(target=dashboard_loop, daemon=True)
    dash_t.start()
    threads = []
    for i in range(MAX_JOBS):
        t = threading.Thread(target=worker_loop, args=(config,), name=f"W{i+1}")
        t.start()
        threads.append(t)
        time.sleep(0.5)
    
    try:
        while True: time.sleep(1)
    except KeyboardInterrupt:
        graceful_exit(None, None)

if __name__ == "__main__": main()
