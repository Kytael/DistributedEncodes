import requests, subprocess, os, sys, time, json, platform, signal, threading, shutil, argparse, re
import urllib.request
import zipfile
from ftplib import FTP
from collections import deque

VERSION = "2.6.0-REMOTE-LOGS" 
GITHUB_REPO = "FractumSeraph/DistributedEncodes"
RAW_URL = f"https://raw.githubusercontent.com/{GITHUB_REPO}/main/worker.py"
RELEASE_API = f"https://api.github.com/repos/{GITHUB_REPO}/releases/latest"
PRESET_URL = f"https://raw.githubusercontent.com/{GITHUB_REPO}/main/FractumAV1.json"

# [CHANGE ME] Connection Config
MANAGER_URL = "http://transcode.fractumseraph.net:5000"
API_TOKEN = "FractumSecure2025"
FTP_HOST = "transcode.fractumseraph.net"
FTP_USER = "transcode"
FTP_PASS = "transcode"

WIN_HB_URL = "https://github.com/HandBrake/HandBrake/releases/download/1.10.2/HandBrakeCLI-1.10.2-win-x86_64.zip"
WIN_FF_URL = "https://www.gyan.dev/ffmpeg/builds/ffmpeg-release-full.zip"

parser = argparse.ArgumentParser()
parser.add_argument("--stealth", action="store_true", help="Enable Stealth Mode")
parser.add_argument("--debug", action="store_true", help="Enable Verbose Debug Logging")
parser.add_argument("--jobs", type=int, default=1, help="Number of concurrent encodes")
parser.add_argument("--no-update", action="store_true", help="Skip update check")
parser.add_argument("-u", "--user", type=str, help="Set Username")
parser.add_argument("-w", "--worker", type=str, help="Set Worker Name")
parser.add_argument("--force-ffmpeg", action="store_true", help="Force FFmpeg backend")
args = parser.parse_args()

STEALTH_MODE = args.stealth
DEBUG_MODE = args.debug
MAX_JOBS = args.jobs
SKIP_UPDATE = args.no_update

if getattr(sys, 'frozen', False): APP_DIR = os.path.dirname(sys.executable)
else: APP_DIR = os.path.dirname(os.path.abspath(__file__))

RECOVERY_DIR = os.path.join(APP_DIR, "recovery")
if not os.path.exists(RECOVERY_DIR): os.makedirs(RECOVERY_DIR)

HANDBRAKE_EXE = "HandBrakeCLI"
FFMPEG_EXE = "ffmpeg"
FFPROBE_EXE = "ffprobe"

if platform.system() == "Windows":
    HANDBRAKE_EXE = os.path.join(APP_DIR, "HandBrakeCLI.exe")
    FFMPEG_EXE = os.path.join(APP_DIR, "ffmpeg.exe") 
    FFPROBE_EXE = os.path.join(APP_DIR, "ffprobe.exe")
else:
    local_ff = os.path.join(APP_DIR, "ffmpeg")
    if os.path.exists(local_ff): FFMPEG_EXE = local_ff

ENCODER_BACKEND = None
PRESET_FILE = os.path.join(APP_DIR, "FractumAV1.json")
CONFIG_FILE = os.path.join(APP_DIR, "user_config.json")
HEADERS = {"X-Auth-Token": API_TOKEN}

ACTIVE_WORKERS = {}
WORKER_LOCK = threading.Lock()
EXIT_FLAG = threading.Event()
WORKER_STATE = {f"W{i+1}": {"state": "Idle", "pct": 0.0, "info": ""} for i in range(MAX_JOBS)}
GLOBAL_USERNAME = "Unknown"

# --- REMOTE LOGGER ---
def remote_log(msg, level="INFO"):
    # Always print locally
    if level == "ERROR" or DEBUG_MODE:
        ts = time.strftime("%H:%M:%S")
        print(f"[{ts}] [{level}] {msg}")
        
    # Send to server if it's an error or important
    if level in ["ERROR", "CRITICAL"]:
        try:
            requests.post(f"{MANAGER_URL}/log", json={
                "worker": GLOBAL_USERNAME,
                "message": msg,
                "level": level
            }, headers=HEADERS, timeout=2)
        except: pass

def debug(msg):
    if DEBUG_MODE:
        ts = time.strftime("%H:%M:%S")
        print(f"[{ts}] [DEBUG] {msg}")

def verify_encoder_support(binary_path, encoder_name):
    try:
        cmd = [binary_path, "-encoders"]
        output = subprocess.check_output(cmd, text=True, stderr=subprocess.STDOUT)
        return encoder_name in output
    except: return False

def is_admin():
    try: return os.getuid() == 0
    except AttributeError:
        import ctypes
        return ctypes.windll.shell32.IsUserAnAdmin() != 0

def bootstrap_preset():
    if not os.path.exists(PRESET_FILE):
        print(":: BOOTSTRAP :: Downloading Preset...")
        try:
            with urllib.request.urlopen(PRESET_URL) as r, open(PRESET_FILE, 'wb') as f:
                f.write(r.read())
        except Exception as e: remote_log(f"Preset DL Failed: {e}", "ERROR")

def bootstrap_linux():
    if shutil.which("ffmpeg"):
        if not verify_encoder_support("ffmpeg", "libsvtav1"):
            remote_log("System FFmpeg lacks AV1. Please install static build.", "ERROR")

    if not (shutil.which("HandBrakeCLI") or shutil.which("ffmpeg")):
        print("   [!] Missing encoders. Installing...")
        if shutil.which("apt-get"): pkg_mgr, pkgs = "apt-get", ["handbrake-cli", "ffmpeg"]
        elif shutil.which("dnf"): pkg_mgr, pkgs = "dnf", ["HandBrake-cli", "ffmpeg"]
        elif shutil.which("pacman"): pkg_mgr, pkgs = "pacman", ["handbrake-cli", "ffmpeg"]
        else: print("   [!] Error: No package manager found."); sys.exit(1)

        if not is_admin():
            try: os.execvp("sudo", ["sudo", sys.executable] + sys.argv)
            except: sys.exit(1)

        subprocess.call([pkg_mgr, "install", "-y", pkgs[0], pkgs[1]])

def bootstrap_windows():
    if not os.path.exists(HANDBRAKE_EXE):
        print("   [+] Downloading HandBrakeCLI...")
        try:
            with urllib.request.urlopen(WIN_HB_URL) as r, open("hb.zip", 'wb') as f: shutil.copyfileobj(r, f)
            with zipfile.ZipFile("hb.zip", 'r') as z:
                if "HandBrakeCLI.exe" in z.namelist():
                    with open(HANDBRAKE_EXE, "wb") as f: f.write(z.read("HandBrakeCLI.exe"))
            os.remove("hb.zip")
        except: pass

    need_dl = False
    if not os.path.exists(FFMPEG_EXE) or not os.path.exists(FFPROBE_EXE):
        need_dl = True
    elif not verify_encoder_support(FFMPEG_EXE, "libsvtav1"):
        print("   [!] Current FFmpeg missing AV1. Updating...")
        try: os.remove(FFMPEG_EXE)
        except: pass
        need_dl = True

    if need_dl:
        print("   [+] Downloading FFmpeg FULL (AV1 Supported)...")
        try:
            with urllib.request.urlopen(WIN_FF_URL) as r, open("ff.zip", 'wb') as f: shutil.copyfileobj(r, f)
            with zipfile.ZipFile("ff.zip", 'r') as z:
                for name in z.namelist():
                    if name.endswith("bin/ffmpeg.exe"):
                        with open(FFMPEG_EXE, "wb") as f: f.write(z.read(name))
                    elif name.endswith("bin/ffprobe.exe"):
                        with open(FFPROBE_EXE, "wb") as f: f.write(z.read(name))
            os.remove("ff.zip")
        except Exception as e: remote_log(f"FFmpeg DL Failed: {e}", "ERROR")

def run_bootstrap():
    bootstrap_preset()
    if platform.system() == "Linux": bootstrap_linux()
    elif platform.system() == "Windows": bootstrap_windows()

def check_for_updates():
    if SKIP_UPDATE: return
    if getattr(sys, 'frozen', False): return
    try:
        r = requests.get(RAW_URL, timeout=5)
        if r.status_code == 200:
            match = re.search(r'VERSION\s*=\s*"([^"]+)"', r.text)
            if match and match.group(1) != VERSION:
                print(f"   [!] New version found: {match.group(1)}. Updating...")
                with open(__file__, 'w', encoding='utf-8') as f: f.write(r.text)
                os.execv(sys.executable, [sys.executable] + sys.argv)
    except: pass

def update_status(tid, state, pct=0.0, info=""):
    WORKER_STATE[tid] = {"state": state, "pct": pct, "info": info}

def dashboard_loop():
    while not EXIT_FLAG.is_set():
        if STEALTH_MODE:
            active = sum(1 for v in WORKER_STATE.values() if v['state'] != "Idle")
            sys.stdout.write(f"\r:: FRACTUM SECURITY :: ACTIVE: {active}" + " "*20)
        else:
            line = ""
            for tid in sorted(WORKER_STATE.keys()):
                d = WORKER_STATE[tid]
                val = f"{d['state'][0]}:{d['pct']:.0f}%" if d['state'] in ["Encoding","Downloading","Uploading"] else d['state']
                line += f"[{tid}: {val}] "
            sys.stdout.write(f"\r{line}" + " "*10)
        sys.stdout.flush()
        time.sleep(0.5)

def get_config():
    config = {}
    if os.path.exists(CONFIG_FILE):
        try: config = json.load(open(CONFIG_FILE))
        except: pass
    if args.user: config['username'] = args.user
    if args.worker: config['worker_name'] = args.worker
    
    if 'username' not in config:
        config['username'] = input("Enter Username: ").strip() or "Anonymous"
        config['worker_name'] = input("Enter Worker Name: ").strip() or platform.node()
        with open(CONFIG_FILE, 'w') as f: json.dump(config, f)
    return config

def get_metadata(path):
    debug(f"Probing: {path}")
    if not os.path.exists(path) or os.path.getsize(path) == 0: return None
    
    cmd_exe = FFPROBE_EXE if platform.system() == "Windows" else "ffprobe"
    cmd = [cmd_exe, "-v", "error", "-select_streams", "v:0", "-show_entries", "stream=height,codec_name", "-show_entries", "format=duration", "-of", "json", path]
    try:
        raw = subprocess.check_output(cmd, text=True, stderr=subprocess.STDOUT)
        d = json.loads(raw)
        return {"height": int(d['streams'][0]['height']), "codec_name": d['streams'][0]['codec_name'], "duration": float(d['format']['duration'])}
    except Exception as e:
        debug(f"Probe Error: {e}")
        return None

def build_encode_cmd(input_file, output_file):
    try:
        with open(PRESET_FILE, 'r') as f: 
            data = json.load(f)
            preset = data['PresetList'][0]
    except: preset = {}

    height = int(preset.get('PictureHeight', 480))
    rf = int(preset.get('VideoQualitySlider', 28))
    speed = preset.get('VideoPreset', '2') 
    
    if ENCODER_BACKEND == "handbrake":
        v = "1" if DEBUG_MODE else "0"
        return [HANDBRAKE_EXE, "-v", v, "--preset-import-file", PRESET_FILE, "-Z", preset.get('PresetName', 'FractumAV1'), "-i", input_file, "-o", output_file]
    else:
        return [FFMPEG_EXE, "-y", "-v", "error", "-stats", "-i", input_file, 
                "-c:v", "libsvtav1", "-preset", str(speed), "-crf", str(rf), 
                "-vf", f"scale=-2:{height}", "-c:a", "libopus", "-b:a", "128k", "-ac", "2", output_file]

def process(job, username):
    tid = threading.current_thread().name
    job_path = job['filename']
    local_input = f"{tid}_{os.path.basename(job_path)}"
    local_output = f"{tid}_av1_{os.path.splitext(os.path.basename(job_path))[0]}.mp4"

    if not STEALTH_MODE: print(f"\n[+] Processing: {os.path.basename(job_path)}")
    update_status(tid, "Downloading")
    
    try:
        ftp = FTP(FTP_HOST); ftp.login(FTP_USER, FTP_PASS)
        ftp.cwd("source")
        if "/" in job_path:
            for d in os.path.dirname(job_path).split("/"): ftp.cwd(d)
        
        with open(local_input, 'wb') as f:
            ftp.retrbinary(f"RETR {os.path.basename(job_path)}", f.write)
        ftp.quit()
    except Exception as e:
        remote_log(f"Job {job['id']} DL Failed: {e}", "ERROR")
        requests.post(f"{MANAGER_URL}/fail_job", json={"id": job['id'], "reason": "DL Failed"}, headers=HEADERS)
        update_status(tid, "Idle"); return

    update_status(tid, "Starting")
    cmd = build_encode_cmd(local_input, local_output)
    
    logs = deque(maxlen=20)
    hb_stop = threading.Event()
    threading.Thread(target=lambda: [time.sleep(60), requests.post(f"{MANAGER_URL}/heartbeat", json={"id": job['id'], "progress": 50}, headers=HEADERS)] if not hb_stop.is_set() else None, daemon=True).start()

    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    for line in proc.stdout:
        logs.append(line.strip())
        if ENCODER_BACKEND=="handbrake" and "%" in line:
            m = re.search(r"(\d+\.\d+) %", line)
            if m: update_status(tid, "Encoding", float(m.group(1)))
        elif ENCODER_BACKEND=="ffmpeg" and "time=" in line:
            update_status(tid, "Encoding", 50)
            
    proc.wait()
    hb_stop.set()

    meta = get_metadata(local_output)
    if not meta:
        crash_log = "\n".join(logs)
        remote_log(f"Job {job['id']} Encoder Crash:\n{crash_log}", "ERROR")
        requests.post(f"{MANAGER_URL}/fail_job", json={"id": job['id'], "reason": "Encode Failed"}, headers=HEADERS)
        try: os.remove(local_input)
        except: pass
        update_status(tid, "Idle"); return

    update_status(tid, "Uploading")
    try:
        ftp = FTP(FTP_HOST); ftp.login(FTP_USER, FTP_PASS)
        ftp.cwd("completed")
        with open(local_output, 'rb') as f:
            ftp.storbinary(f"STOR {os.path.basename(local_output)}", f)
        ftp.quit()
        
        requests.post(f"{MANAGER_URL}/complete_job", json={
            "id": job['id'], "username": username, "metadata": meta, 
            "local_path": local_output, "remote_name": os.path.basename(local_output)
        }, headers=HEADERS)
    except Exception as e:
        remote_log(f"Job {job['id']} Upload Failed: {e}", "ERROR")
        requests.post(f"{MANAGER_URL}/fail_job", json={"id": job['id'], "reason": "Upload Failed"}, headers=HEADERS)

    try: 
        os.remove(local_input)
        os.remove(local_output)
    except: pass
    update_status(tid, "Idle")

def worker_loop(config):
    tid = threading.current_thread().name
    while not EXIT_FLAG.is_set():
        try:
            r = requests.post(f"{MANAGER_URL}/get_job", json={"username": config['username']}, headers=HEADERS, timeout=5)
            if r.json()['status'] == 'found': process(r.json(), config['username'])
            else: update_status(tid, "Waiting"); time.sleep(10)
        except: update_status(tid, "Error"); time.sleep(10)

def main():
    global ENCODER_BACKEND, GLOBAL_USERNAME
    run_bootstrap()
    
    has_hb = shutil.which(HANDBRAKE_EXE) or os.path.exists(HANDBRAKE_EXE)
    has_ff = shutil.which(FFMPEG_EXE) or os.path.exists(FFMPEG_EXE)
    
    if args.force_ffmpeg: ENCODER_BACKEND = "ffmpeg" if has_ff else None
    elif has_hb: ENCODER_BACKEND = "handbrake"
    elif has_ff: ENCODER_BACKEND = "ffmpeg"
    
    if not ENCODER_BACKEND:
        print("CRITICAL: No valid encoder found."); return
        
    print(f"\n:: FRACTUM NODE :: BACKEND: {ENCODER_BACKEND}")
    
    config = get_config()
    GLOBAL_USERNAME = f"{config['username']} [{config['worker_name']}]"
    
    check_for_updates()
    
    threading.Thread(target=dashboard_loop, daemon=True).start()
    for i in range(MAX_JOBS):
        threading.Thread(target=worker_loop, args=(config,), name=f"W{i+1}").start()
        time.sleep(0.5)
        
    try:
        while True: time.sleep(1)
    except KeyboardInterrupt:
        EXIT_FLAG.set(); print("\nStopping...")

if __name__ == "__main__": main()
