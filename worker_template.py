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

def run_worker(args):
    # Fallback to defaults if args are missing/None
    manager_url = (args.manager or DEFAULT_MANAGER_URL).rstrip('/')
    username = args.username or DEFAULT_USERNAME
    workername = args.workername or DEFAULT_WORKERNAME
    
    full_id = f"{username}-{workername}"
    print(f"[*] Worker: {full_id} | Manager: {manager_url}")
    
    TEMP_DIR = f"./temp_encode_{workername}"
    if os.path.exists(TEMP_DIR): shutil.rmtree(TEMP_DIR)
    os.makedirs(TEMP_DIR, exist_ok=True)

    completed_count = 0
    while True:
        if args.jobs > 0 and completed_count >= args.jobs: 
            print("[*] Job limit reached."); break
        try:
            r = requests.get(f"{manager_url}/get_job")
            data = r.json() if r.status_code == 200 else None
            
            if data and data.get("status") == "ok":
                job = data["job"]; job_id = job['id']; dl_url = job['download_url']
                local_src = os.path.join(TEMP_DIR, "source.tmp"); local_dst = os.path.join(TEMP_DIR, "encoded.mkv")
                
                print(f"[*] Downloading: {job_id}")
                requests.post(f"{manager_url}/report_status", json={"worker_id":full_id, "job_id":job_id, "status":"downloading"})
                with requests.get(dl_url, stream=True) as r:
                    r.raise_for_status()
                    with open(local_src, 'wb') as f:
                        for chunk in r.iter_content(chunk_size=8192): f.write(chunk)

                total_sec = 0; total_min = 0
                try:
                    res = subprocess.run(f'ffmpeg -i "{local_src}"', shell=True, stderr=subprocess.PIPE, text=True)
                    m = re.search(r"Duration: (\d{2}:\d{2}:\d{2}\.\d{2})", res.stderr)
                    if m: total_sec = get_seconds(m.group(1)); total_min = int(total_sec/60)
                except: pass

                print(f"[*] Encoding ({total_min}m)...")
                requests.post(f"{manager_url}/report_status", json={"worker_id":full_id, "job_id":job_id, "status":"processing", "duration":total_min})
                cmd = f'ffmpeg -y -i "{local_src}" -c:v libx265 -crf 23 -c:a copy -progress pipe:1 "{local_dst}"'
                proc = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
                
                last_rep = 0
                while True:
                    line = proc.stdout.readline()
                    if not line and proc.poll() is not None: break
                    if "out_time=" in line and "N/A" not in line and total_sec > 0:
                        if time.time() - last_rep > 10:
                            try:
                                curr = get_seconds(line.split('=')[1].strip())
                                pct = int((curr/total_sec)*100)
                                requests.post(f"{manager_url}/report_status", json={"worker_id":full_id, "job_id":job_id, "status":"processing", "progress":pct})
                                last_rep = time.time()
                            except: pass

                if proc.returncode == 0:
                    print(f"[*] Uploading...")
                    requests.post(f"{manager_url}/report_status", json={"worker_id":full_id, "job_id":job_id, "status":"uploading", "progress":100})
                    with open(local_dst, 'rb') as f:
                        requests.post(f"{manager_url}/upload_result", files={'file': (job_id, f)}, data={'job_id': job_id, 'worker_id': full_id})
                    print(f"[+] Done: {job_id}")
                    completed_count += 1
                else:
                    print("[!] Failed"); requests.post(f"{manager_url}/report_status", json={"worker_id":full_id, "job_id":job_id, "status":"failed"})

                if os.path.exists(local_src): os.remove(local_src)
                if os.path.exists(local_dst): os.remove(local_dst)
            else: time.sleep(10)
        except Exception as e: print(f"[!] Error: {e}"); time.sleep(10)

if __name__ == '__main__':
    p = argparse.ArgumentParser()
    p.add_argument('--username', default=None)
    p.add_argument('--workername', default=None)
    p.add_argument('--jobs', type=int, default=0)
    p.add_argument('--manager', default=None)
    args = p.parse_args()
    run_worker(args)
