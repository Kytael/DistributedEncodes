import requests
import argparse
import time

# CONFIGURATION
# ---------------------------------------------------------
# We point directly to your public domain now.
MANAGER_URL = "https://encode.fractumseraph.net"

# If you haven't changed the default credentials in config.py, 
# you might need to update these to match what is on your server.
try:
    from config import ADMIN_USER, ADMIN_PASS
except:
    print("[!] Config not found, using default credentials.")
    ADMIN_USER = "admin"
    ADMIN_PASS = "password"
# ---------------------------------------------------------

def run_archive():
    print("==================================================")
    print(" FRACTUM MAINTENANCE: ARCHIVE HISTORY")
    print("==================================================")
    print(f"Target Server: {MANAGER_URL}")
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
        
        # FIXED: Headers must match the PUBLIC domain exactly
        headers = {
            'Origin': MANAGER_URL,
            'Referer': MANAGER_URL,
            'User-Agent': 'FractumMaintenance/1.0'
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
        r = requests.get(url, auth=(ADMIN_USER, ADMIN_PASS))
        if r.status_code == 200:
            print("[+] Rescan complete. Jobs should now be queued!")
        else:
            print(f"[-] Rescan Failed: {r.status_code} - {r.text}")
    except Exception as e:
        print(f"[!] Rescan Error: {e}")

if __name__ == "__main__":
    run_archive()
