import requests
import argparse
import time

# Try to load config, or fallback to defaults
try:
    from config import ADMIN_USER, ADMIN_PASS, SERVER_PORT
except:
    print("[!] Config not found, using default credentials.")
    ADMIN_USER = "admin"
    ADMIN_PASS = "password"
    SERVER_PORT = 5000

MANAGER_URL = f"http://127.0.0.1:{SERVER_PORT}"

def run_archive():
    print("==================================================")
    print(" FRACTUM MAINTENANCE: ARCHIVE HISTORY")
    print("==================================================")
    print("This will:")
    print("1. Rename all COMPLETED jobs in the database.")
    print("2. Trigger a re-scan of the Source Folder.")
    print("3. Add all files back to the Queue as NEW jobs.")
    print("4. Preserve user scores (History entries are kept).")
    print("==================================================")
    
    confirm = input("Are you sure? (Type 'yes'): ")
    if confirm.lower() != "yes":
        print("Aborted.")
        return

    print(f"[*] Connecting to {MANAGER_URL}...")
    
    # Step 1: Archive
    try:
        url = f"{MANAGER_URL}/api/admin_action"
        payload = {"action": "archive_history"}
        
        # FIXED: Added Origin header to satisfy CSRF protection
        headers = {
            'Origin': MANAGER_URL,
            'Referer': MANAGER_URL
        }
        
        r = requests.post(url, json=payload, headers=headers, auth=(ADMIN_USER, ADMIN_PASS))
        
        if r.status_code == 200:
            print("[+] Jobs successfully archived.")
        else:
            print(f"[-] Archive Failed: {r.status_code} - {r.text}")
            return
    except Exception as e:
        print(f"[!] Connection Error: {e}")
        return

    # Step 2: Rescan
    print("[*] Triggering Database Rescan...")
    try:
        url = f"{MANAGER_URL}/api/rescan_db"
        # GET requests usually don't need CSRF headers, but adding them doesn't hurt
        r = requests.get(url, auth=(ADMIN_USER, ADMIN_PASS))
        if r.status_code == 200:
            print("[+] Rescan complete. Jobs should now be queued!")
        else:
            print(f"[-] Rescan Failed: {r.status_code} - {r.text}")
    except Exception as e:
        print(f"[!] Rescan Error: {e}")

if __name__ == "__main__":
    run_archive()
