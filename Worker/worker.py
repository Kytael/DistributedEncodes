import requests, subprocess, os, sys, time, json, platform, uuid
from ftplib import FTP

# [CHANGE ME] Connection Config
MANAGER_URL = "http://transcode.fractumseraph.net:5000"
API_TOKEN = "FractumSecure2025"
FTP_HOST = "transcode.fractumseraph.net"
FTP_USER = "transcode"
FTP_PASS = "transcode"

# --- STEALTH CONFIG ---
STEALTH_MODE = True 

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
    if STEALTH_MODE:
        if stealth_alt: print(stealth_alt)
    else: print(msg)

def get_username():
    if os.path.exists(CONFIG_FILE): return json.load(open(CONFIG_FILE)).get('username')
    user = input("Enter Username: ").strip()
    with open(CONFIG_FILE, 'w') as f: json.dump({"username": user}, f)
    return user

def get_metadata(path):
    cmd = [FFPROBE_EXE, "-v", "error", "-select_streams", "v:0", "-show_entries", "stream=height,codec_name", "-show_entries", "format=duration", "-of", "json", path]
    try:
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
    job_path = job['filename'].replace("\\", "/")
    real_output_name = "av1_" + job_path.replace("/", "_")
    
    if STEALTH_MODE:
        local_input = str(uuid.uuid4()) + ".dat"
        local_output = str(uuid.uuid4()) + ".dat"
        log("", stealth_alt=f"\n[+] Processing Unit #{job['id']}")
    else:
        local_input = os.path.basename(job_path)
        local_output = real_output_name
        log(f"\n[+] Job: {job_path}")

    # DOWNLOAD (Added simple retry here too)
    download_success = False
    for attempt in range(3):
        try:
            ftp = FTP(FTP_HOST); ftp.login(FTP_USER, FTP_PASS)
            try: ftp.cwd("source") 
            except: pass 
            safe_ftp_cwd(ftp, os.path.dirname(job_path))
            
            log(f"    Downloading {os.path.basename(job_path)}...", stealth_alt="    >> Acquiring Data Packet...")
            with open(local_input, 'wb') as f: 
                ftp.retrbinary(f"RETR {os.path.basename(job_path)}", f.write)
            ftp.quit()
            download_success = True
            break
        except Exception as e:
            log(f"[!] Download Attempt {attempt+1} failed: {e}")
            time.sleep(5)
            
    if not download_success:
        if os.path.exists(local_input): os.remove(local_input)
        requests.post(f"{MANAGER_URL}/fail_job", json={"id": job['id']}, headers=HEADERS)
        return

    # TRANSCODE
    log("    Transcoding...", stealth_alt="    >> Processing Data...")
    with open(PRESET_FILE) as f: preset_name = json.load(f)['PresetList'][0]['PresetName']
    cmd = [HANDBRAKE_EXE, "--preset-import-file", PRESET_FILE, "-Z", preset_name, "-i", local_input, "-o", local_output]
    
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    full_log = ""; last_stealth_update = 0
    for line in process.stdout:
        full_log += line
        if "Encoding" in line:
            if STEALTH_MODE:
                if time.time() - last_stealth_update > 2:
                    sys.stdout.write(f"\r    >> Compressing... "); last_stealth_update = time.time()
            else: sys.stdout.write(f"\r    {line.strip()}")
    process.wait(); print()

    # UPLOAD with RETRY LOOP
    uploaded = False
    for attempt in range(3):
        try:
            log(f"    Verifying & Uploading (Attempt {attempt+1})...", stealth_alt="    >> Uploading Result...")
            meta = get_metadata(local_output)
            if not meta: raise Exception("Encoding Failed")
            
            ftp = FTP(FTP_HOST); ftp.login(FTP_USER, FTP_PASS)
            try: ftp.cwd("completed")
            except: pass 
            with open(local_output, 'rb') as f: ftp.storbinary(f"STOR {real_output_name}", f)
            ftp.quit()
            
            # Notify Manager
            resp = requests.post(f"{MANAGER_URL}/complete_job", json={"id": job['id'], "username": username, "metadata": meta, "encoding_log": full_log}, headers=HEADERS)
            if resp.status_code == 200:
                uploaded = True
                break
            else:
                log(f"    [!] Manager rejected: {resp.text}")
                # Don't retry if manager rejected logic, only network
                break 

        except Exception as e: 
            log(f"[!] Upload Attempt {attempt+1} Error: {e}")
            time.sleep(10) # Wait 10s before retry

    try: os.remove(local_input)
    except: pass
    try: os.remove(local_output)
    except: pass

def main():
    if platform.system() == "Windows" and not os.path.exists(HANDBRAKE_EXE): print("Missing HandBrakeCLI.exe"); return
    if STEALTH_MODE: print(":: FRACTUM DISTRIBUTED NODE :: SECURE CONNECTION ESTABLISHED ::")
    username = get_username(); print(f"User: {username} ({platform.system()})")
    while True:
        try:
            r = requests.post(f"{MANAGER_URL}/get_job", json={"username": username}, headers=HEADERS, timeout=5)
            data = r.json()
            if data['status'] == 'found': process(data, username)
            else: 
                msg = "Waiting for jobs..."
                if STEALTH_MODE: msg = "Standby..."
                print(msg, end='\r'); time.sleep(10)
        except: time.sleep(10)

if __name__ == "__main__": main()
