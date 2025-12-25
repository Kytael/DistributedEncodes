import argparse, time, requests, subprocess, os, re, shutil

# ==============================================================================
# DEFAULT CONFIGURATION
# ==============================================================================
DEFAULT_MANAGER_URL = "https://encode.fractumseraph.net/"
DEFAULT_USERNAME = "Anonymous"
DEFAULT_WORKERNAME = f"Node-{int(time.time())}"
# ==============================================================================

def get_seconds(t):
    try: h,m,s = t.split(':'); return int(h)*3600 + int(m)*60 + float(s)
    except: return 0

def check_ffmpeg():
    def has_svtav1():
        try:
            res = subprocess.run(["ffmpeg", "-hide_banner", "-encoders"], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
            return "libsvtav1" in res.stdout
        except:
            return False

    if shutil.which("ffmpeg") and has_svtav1():
        return

    print("[!] FFmpeg with SVT-AV1 support not found. Attempting to install...")
    
    # Try to install if missing or insufficient
    try:
        if shutil.which("apt-get"):
            subprocess.run(["sudo", "apt-get", "update", "-qq"], check=True)
            subprocess.run(["sudo", "apt-get", "install", "-y", "ffmpeg"], check=True)
        elif shutil.which("dnf"):
            subprocess.run(["sudo", "dnf", "install", "-y", "ffmpeg"], check=True)
        elif shutil.which("brew"):
            subprocess.run(["brew", "install", "ffmpeg"], check=True)
        elif shutil.which("choco"):
             subprocess.run(["choco", "install", "ffmpeg", "-y"], check=True)
        else:
            if not shutil.which("ffmpeg"): # Only raise if we have NO ffmpeg
                raise EnvironmentError("No supported package manager found.")
        
        if not shutil.which("ffmpeg"):
             raise EnvironmentError("Installation appeared to succeed but ffmpeg is still missing.")
             
        if not has_svtav1():
             raise EnvironmentError("Installed FFmpeg does not support libsvtav1 (SVT-AV1). Please install a build with SVT-AV1 support manually.")
             
        print("[*] FFmpeg with SVT-AV1 installed/verified successfully.")
    except Exception as e:
        print(f"[!] Critical Error: {e}")
        print("    Please install a compatible FFmpeg manually.")
        exit(1)

def worker_task(worker_id, manager_url, temp_dir):
    """Single worker thread loop."""
    print(f"[{worker_id}] Started.")
    os.makedirs(temp_dir, exist_ok=True)
    
    while True:
        try:
            r = requests.get(f"{manager_url}/get_job")
            data = r.json() if r.status_code == 200 else None
            
            if data and data.get("status") == "ok":
                job = data["job"]; job_id = job['id']; dl_url = job['download_url']
                local_src = os.path.join(temp_dir, "source.tmp"); local_dst = os.path.join(temp_dir, "encoded.mkv")
                
                # --- DOWNLOADING ---
                print(f"[{worker_id}] Downloading: {job_id}")
                requests.post(f"{manager_url}/report_status", json={"worker_id":worker_id, "job_id":job_id, "status":"downloading", "progress":0})
                
                with requests.get(dl_url, stream=True) as r:
                    r.raise_for_status()
                    total_size = int(r.headers.get('content-length', 0))
                    downloaded = 0
                    last_rep = 0
                    
                    with open(local_src, 'wb') as f:
                        for chunk in r.iter_content(chunk_size=8192): 
                            f.write(chunk)
                            downloaded += len(chunk)
                            if total_size > 0 and time.time() - last_rep > 5:
                                pct = int((downloaded/total_size)*100)
                                requests.post(f"{manager_url}/report_status", json={"worker_id":worker_id, "job_id":job_id, "status":"downloading", "progress":pct})
                                last_rep = time.time()

                total_sec = 0; total_min = 0
                try:
                    # Use list-based command
                    res = subprocess.run(['ffmpeg', '-i', local_src], stderr=subprocess.PIPE, text=True)
                    m = re.search(r"Duration: (\d{2}:\d{2}:\d{2}\.\d{2})", res.stderr)
                    if m: total_sec = get_seconds(m.group(1)); total_min = int(total_sec/60)
                except: pass

                # --- ENCODING ---
                print(f"[{worker_id}] Encoding ({total_min}m): {job_id}")
                requests.post(f"{manager_url}/report_status", json={"worker_id":worker_id, "job_id":job_id, "status":"processing", "progress":0, "duration":total_min})
                
                # Identify text-based subtitle streams to keep
                allowed_subs = []
                try:
                    for line in res.stderr.split('\n'):
                        m_sub = re.search(r"Stream #0:(\d+).*Subtitle:\s*(subrip|ass|webvtt|mov_text|text)", line, re.IGNORECASE)
                        if m_sub:
                            allowed_subs.append(m_sub.group(1))
                except: pass

                # Build command
                cmd = [
                    'ffmpeg', '-y', '-i', local_src,
                    '-map', '0:v:0', # Map first video stream
                    '-map', '0:a:0', # Map first audio stream
                ]
                
                for idx in allowed_subs:
                    cmd.extend(['-map', f'0:{idx}'])

                cmd.extend([
                    '-c:v', 'libsvtav1', '-preset', '2', '-crf', '63',
                    '-vf', 'scale=-2:480',
                    '-c:a', 'libopus', '-b:a', '12k', '-ac', '1',
                    '-c:s', 'mov_text',
                    '-progress', 'pipe:1', local_dst
                ])
                
                proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
                
                last_rep = 0
                while True:
                    line = proc.stdout.readline()
                    if not line and proc.poll() is not None: break
                    if "out_time=" in line and "N/A" not in line and total_sec > 0:
                        if time.time() - last_rep > 10:
                            try:
                                curr = get_seconds(line.split('=')[1].strip())
                                pct = int((curr/total_sec)*100)
                                requests.post(f"{manager_url}/report_status", json={"worker_id":worker_id, "job_id":job_id, "status":"processing", "progress":pct})
                                last_rep = time.time()
                            except: pass

                if proc.returncode == 0:
                    # --- UPLOADING ---
                    print(f"[{worker_id}] Uploading: {job_id}")
                    requests.post(f"{manager_url}/report_status", json={"worker_id":worker_id, "job_id":job_id, "status":"uploading", "progress":0})
                    
                    class ProgressFileReader:
                        def __init__(self, filename, callback):
                            self._f = open(filename, 'rb')
                            self._total = os.path.getsize(filename)
                            self._read = 0
                            self._callback = callback
                            self._last_time = 0
                        def read(self, size=-1):
                            data = self._f.read(size)
                            self._read += len(data)
                            if time.time() - self._last_time > 5:
                                pct = int((self._read / self._total) * 100)
                                self._callback(pct)
                                self._last_time = time.time()
                            return data
                        def __getattr__(self, attr): return getattr(self._f, attr)

                    def upload_progress(pct):
                        try: requests.post(f"{manager_url}/report_status", json={"worker_id":worker_id, "job_id":job_id, "status":"uploading", "progress":pct})
                        except: pass

                    with ProgressFileReader(local_dst, upload_progress) as f:
                        requests.post(f"{manager_url}/upload_result", files={'file': (job_id, f)}, data={'job_id': job_id, 'worker_id': worker_id})
                    
                    print(f"[{worker_id}] Done: {job_id}")
                else:
                    print(f"[{worker_id}] Failed: {job_id}"); requests.post(f"{manager_url}/report_status", json={"worker_id":worker_id, "job_id":job_id, "status":"failed"})

                if os.path.exists(local_src): os.remove(local_src)
                if os.path.exists(local_dst): os.remove(local_dst)
            else: time.sleep(10)
        except Exception as e: print(f"[{worker_id}] Error: {e}"); time.sleep(10)

def run_worker(args):
    check_ffmpeg()
    # Fallback to defaults if args are missing/None
    manager_url = (args.manager or DEFAULT_MANAGER_URL).rstrip('/')
    username = args.username or DEFAULT_USERNAME
    base_workername = args.workername or DEFAULT_WORKERNAME
    
    # Concurrency limit
    num_jobs = args.jobs if args.jobs > 0 else 1
    if num_jobs > 32: num_jobs = 32
    
    print(f"[*] Starting {num_jobs} worker threads.")
    print(f"[*] Base ID: {username}-{base_workername} | Manager: {manager_url}")
    
    threads = []
    for i in range(num_jobs):
        worker_id = f"{username}-{base_workername}-{i+1}"
        temp_dir = f"./temp_encode_{base_workername}_{i+1}"
        t = threading.Thread(target=worker_task, args=(worker_id, manager_url, temp_dir))
        t.daemon = True
        t.start()
        threads.append(t)
        
    try:
        while True: time.sleep(1)
    except KeyboardInterrupt:
        print("\n[*] Stopping workers...")

if __name__ == '__main__':
    p = argparse.ArgumentParser()
    p.add_argument('--username', default=None)
    p.add_argument('--workername', default=None)
    p.add_argument('--jobs', type=int, default=0)
    p.add_argument('--manager', default=None)
    args = p.parse_args()
    run_worker(args)
