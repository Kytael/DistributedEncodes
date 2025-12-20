import requests, subprocess, os

MANAGER_URL = "http://www.yourserver.com:5000"
API_TOKEN = "SecretTokenThisMustMatchTheTokenInManagerPy"
SOURCE_ROOTS = [r"C:\Path\To\source"] 
EXTENSIONS = ('.mp4', '.mkv', '.avi', '.mov')
HEADERS = {"X-Auth-Token": API_TOKEN}

def main():
    payload = []
    print(f"Scanning {SOURCE_ROOTS}...")
    for root in SOURCE_ROOTS:
        for path, _, files in os.walk(root):
            for f in files:
                if f.lower().endswith(EXTENSIONS):
                    full = os.path.join(path, f)
                    rel = os.path.relpath(full, root).replace("\\", "/")
                    try:
                        dur = float(subprocess.check_output(["ffprobe", "-v", "error", "-show_entries", "format=duration", "-of", "default=noprint_wrappers=1:nokey=1", full], text=True))
                        payload.append({"filename": rel, "duration": dur})
                        print(f"  + Found: {rel}")
                    except: print(f"  ! Error reading: {f}")
    
    if payload:
        print(f"Uploading {len(payload)} files...")
        r = requests.post(f"{MANAGER_URL}/populate", json={"files": payload}, headers=HEADERS)
        print(f"Server replied: {r.json()}")

if __name__ == "__main__": main()