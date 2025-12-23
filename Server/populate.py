import requests, subprocess, os, re
import shutil

# [CHANGE ME] Connection Config
MANAGER_URL = "http://transcode.fractumseraph.net:5000"
API_TOKEN = os.environ.get("FRACTUM_API_TOKEN", "FractumSecure2025")
SOURCE_ROOTS = [r"C:\Path\To\source"] 
EXTENSIONS = ('.mp4', '.mkv', '.avi', '.mov')
HEADERS = {"X-Auth-Token": API_TOKEN}

def sanitize_filename(name):
    """
    Replaces unsafe characters with underscores.
    Allowed: A-Z, a-z, 0-9, ., -, _
    """
    clean = re.sub(r'[^a-zA-Z0-9_.-]', '_', name)
    clean = re.sub(r'_{2,}', '_', clean)
    return clean

def main():
    payload = []
    print(f"Scanning {SOURCE_ROOTS}...")
    
    for root in SOURCE_ROOTS:
        for path, _, files in os.walk(root):
            for f in files:
                if f.lower().endswith(EXTENSIONS):
                    
                    # Sanitize Filename on Disk
                    clean_f = sanitize_filename(f)
                    full_old = os.path.join(path, f)
                    full_new = os.path.join(path, clean_f)
                    
                    if f != clean_f:
                        try:
                            os.rename(full_old, full_new)
                            print(f"  [RENAME] '{f}' -> '{clean_f}'")
                            f = clean_f
                            full_old = full_new 
                        except Exception as e:
                            print(f"  [ERROR] Could not rename '{f}': {e}")
                            continue

                    # Process normally
                    rel = os.path.relpath(full_new, root).replace("\\", "/")
                    try:
                        dur = float(subprocess.check_output(["ffprobe", "-v", "error", "-show_entries", "format=duration", "-of", "default=noprint_wrappers=1:nokey=1", full_new], text=True))
                        payload.append({"filename": rel, "duration": dur})
                        print(f"  + Found: {rel}")
                    except: print(f"  ! Error reading: {f}")
    
    if payload:
        print(f"Uploading {len(payload)} files...")
        r = requests.post(f"{MANAGER_URL}/populate", json={"files": payload}, headers=HEADERS)
        print(f"Server replied: {r.text}")
    else:
        print("No new files found.")

if __name__ == "__main__":
    main()