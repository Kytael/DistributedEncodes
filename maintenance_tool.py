import requests
import argparse
import time

# CONFIGURATION
# ---------------------------------------------------------
# Point directly to your public domain
MANAGER_URL = "https://encode.fractumseraph.net"

try:
    from config import ADMIN_USER, ADMIN_PASS
except:
    print("[!] Config not found, using default credentials.")
    ADMIN_USER = "admin"
    ADMIN_PASS = "password"
# ---------------------------------------------------------

def run_tool():
    print("==================================================")
    print(" FRACTUM MAINTENANCE TOOL")
    print("==================================================")
    print(f"Target Server: {MANAGER_URL}")
    print("--------------------------------------------------")
    print("1. Archive History (Rename completed jobs, keep scores)")
    print("2. PURGE QUEUE (Delete all queued jobs, force Re-Scan)")
    print("3. Exit")
    print("--------------------------------------------------")
    
    choice = input("Select Option [1-3]: ").strip()
    
    action = None
    if choice == "1": action = "archive_history"
    elif choice == "2": action = "purge_queue"
    elif choice == "3": return
    else: print("Invalid choice."); return

    print(f"[*] Sending command: {action}...")
    
    try:
        url = f"{MANAGER_URL}/api/admin_action"
        payload = {"action": action}
        
        # Headers MUST match the PUBLIC domain to pass CSRF
        headers = {
            'Origin': MANAGER_URL,
            'Referer': MANAGER_URL,
            'User-Agent': 'FractumMaintenance/1.0'
        }
        
        r = requests.post(url, json=payload, headers=headers, auth=(ADMIN_USER, ADMIN_PASS))
        
        if r.status_code == 200:
            print(f"[+] Success! {action} completed.")
            if action == "purge_queue":
                print("[*] The database is now re-scanning remote/local files...")
        else:
            print(f"[-] Failed: {r.status_code} - {r.text}")
            
    except Exception as e:
        print(f"[!] Connection Error: {e}")

if __name__ == "__main__":
    run_tool()
