import requests, subprocess, os, sys, time, json, platform, uuid, argparse
from ftplib import FTP

# --- CONFIG ---
MANAGER_URL = "http://transcode.fractumseraph.net:5000"
API_TOKEN = "FractumSecret2025"
FTP_HOST = "transcode.fractumseraph.net"
FTP_USER = "transcode"
FTP_PASS = "transcode"
# The when connecting to the ftp server, the user should see the 'source' and 'completed' folder immediately.

# --- ARGUMENT PARSING ---
parser = argparse.ArgumentParser()
parser.add_argument("--stealth", action="store_true", help="Hide filenames and activity")
args = parser.parse_args()
STEALTH_MODE = args.stealth

if getattr(sys, 'frozen', False): APP_DIR = os.path.dirname(sys.executable)
else: APP_DIR = os.path.dirname(os.path.abspath(__file__))

if platform.system() == "Windows":
    HANDBRAKE_EXE = os.path.join(APP_DIR, "HandBrakeCLI.exe")
    FFPROBE_EXE = os.path.join(APP_DIR, "ffprobe.exe")
else:
    HANDBRAKE_EXE = "HandBrakeCLI"
    FFPROBE_EXE = "ffprobe"

PRESET_FILE = os.path.join(APP_DIR, "FractumAV1.json")
CONFIG_FILE = os.path.join(APP_DIR, "user_config.json")
HEADERS = {"X-Auth-Token": API_TOKEN}

def log(msg, stealth_alt=None):
    """Handles printing based on Stealth Mode"""
    if STEALTH_MODE:
        if stealth_alt: print(stealth_alt)
    else:
        print(msg)

def get_username():
    if os.path.exists(CONFIG_FILE): return json.load(open(CONFIG_FILE)).get('username')
    user = input("Enter Username: ").strip()
    with open(CONFIG_FILE, 'w') as f: json.dump({"username": user}, f)
    return user

def get_metadata(path):
    cmd = [FFPROBE_EXE, "-v", "error", "-select_streams", "v:0", "-show_entries", "stream=height,codec_name", "-show_entries", "format=duration", "-of", "json", path]
    try:
        # Suppress FFprobe errors in stealth mode
        stderr_mode = subprocess.DEVNULL if STEALTH_MODE else None
        data = json.loads(subprocess.check_output(cmd, text=True, stderr=stderr_mode))
        return {"height": int(data['streams'][0]['height']), "codec_name": data['streams'][0]['codec_name'], "duration": float(data['format']['duration'])}
    except: return None

def safe_ftp_cwd(ftp, path):
    if not path or path == ".": return
    for folder in path.split("/"):
        if folder: 
            try: ftp.cwd(folder)
            except: pass

def process(job, username):
    # 1. Determine Paths
    job_path = job['filename'].replace("\\", "/")
    
    # OUTPUT NAME (What gets uploaded) - Still needs to be real so Server understands it
    real_output_name = "av1_" + job_path.replace("/", "_")
    
    # LOCAL NAMES (What is on disk)
    if STEALTH_MODE:
        # Random UUIDs to hide content on disk
        local_input = str(uuid.uuid4()) + ".dat"
        local_output = str(uuid.uuid4()) + ".dat"
        log(f"\n[+] Job: {job_path}", stealth_alt=f"\n[+] Processing Unit #{job['id']}")
    else:
        local_input = os.path.basename(job_path)
        local_output = real_output_name
        print(f"\n[+] Job: {job_path}")

    # DOWNLOAD
    try:
        ftp = FTP(FTP_HOST)
        ftp.login(FTP_USER, FTP_PASS)
        try: ftp.cwd("source") 
        except: pass 
        
        safe_ftp_cwd(ftp, os.path.dirname(job_path))
        
        log(f"    Downloading {os.path.basename(job_path)}...", stealth_alt="    >> Acquiring Data Packet...")
        
        with open(local_input, 'wb') as f: 
            ftp.retrbinary(f"RETR {os.path.basename(job_path)}", f.write)
        ftp.quit()
    except Exception as e:
        log(f"[!] Download Error: {e}")
        if os.path.exists(local_input): os.remove(local_input)
        requests.post(f"{MANAGER_URL}/fail_job", json={"id": job['id']}, headers=HEADERS)
        return

    # TRANSCODE
    log("    Transcoding...", stealth_alt="    >> Processing Data...")
    
    with open(PRESET_FILE) as f: preset_name = json.load(f)['PresetList'][0]['PresetName']
    
    cmd = [HANDBRAKE_EXE, "--preset-import-file", PRESET_FILE, "-Z", preset_name, "-i", local_input, "-o", local_output]
    
    # In stealth mode, we don't pass stdout to pipe (or handle it carefully) to avoid title leaks in logs
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    
    full_log = ""
    last_stealth_update = 0
    
    for line in process.stdout:
        full_log += line
        # Parse progress
        if "Encoding" in line:
            if STEALTH_MODE:
                # Only update every few seconds to look generic
                if time.time() - last_stealth_update > 1:
                    # Extract percentage roughly
                    try:
                        parts = line.split()
                        percent = [p for p in parts if "%" in p][0]
                        sys.stdout.write(f"\r    >> Computation: {percent} complete")
                        last_stealth_update = time.time()
                    except: pass
            else:
                sys.stdout.write(f"\r    {line.strip()}")
                
    process.wait()
    print()

    # UPLOAD & CLEANUP
    try:
        log("    Verifying & Uploading...", stealth_alt="    >> Uploading Result...")
        meta = get_metadata(local_output)
        if not meta: raise Exception("Encoding Failed")
        
        ftp = FTP(FTP_HOST)
        ftp.login(FTP_USER, FTP_PASS)
        try: ftp.cwd("completed")
        except: pass 
        
        # We upload the local random file, but name it the REAL name on the server
        with open(local_output, 'rb') as f: 
            ftp.storbinary(f"STOR {real_output_name}", f)
        ftp.quit()
        
        requests.post(f"{MANAGER_URL}/complete_job", json={"id": job['id'], "username": username, "metadata": meta, "encoding_log": full_log}, headers=HEADERS)
    except Exception as e:
        log(f"[!] Upload Error: {e}")

    # Delete local random files
    try: os.remove(local_input)
    except: pass
    try: os.remove(local_output)
    except: pass

def main():
    if platform.system() == "Windows" and not os.path.exists(HANDBRAKE_EXE): 
        print("Missing HandBrakeCLI.exe"); return
        
    # Stealth Welcome
    if STEALTH_MODE:
        print(":: FRACTUM DISTRIBUTED NODE ::")
        print(":: SECURE CONNECTION ESTABLISHED ::")
    
    username = get_username()
    print(f"User: {username} ({platform.system()})")
    while True:
        try:
            r = requests.post(f"{MANAGER_URL}/get_job", json={"username": username}, headers=HEADERS, timeout=5)
            data = r.json()
            if data['status'] == 'found': process(data, username)
            else: 
                msg = "Waiting for jobs..."
                if STEALTH_MODE: msg = "Standby..."
                print(msg, end='\r')
                time.sleep(10)
        except: time.sleep(10)


if __name__ == "__main__": main()

