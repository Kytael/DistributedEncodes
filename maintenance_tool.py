import requests
import sys
import os

# Ensure we can find config.py in the current directory
sys.path.append(os.getcwd())

try:
    import config
except ImportError:
    print("[!] Error: 'config.py' not found.")
    print("    Please run this script from the same folder as your manager and config.py.")
    sys.exit(1)

# Pull settings dynamically from config.py
ADMIN_USER = getattr(config, 'ADMIN_USER', 'admin')
ADMIN_PASS = getattr(config, 'ADMIN_PASS', 'password')
# Use the public display URL (e.g. https://encode.fractumseraph.net)
MANAGER_URL = getattr(config, 'SERVER_URL_DISPLAY', 'http://127.0.0.1:5000').rstrip('/')

def run_tool():
    print("==================================================")
    print(" FRACTUM MAINTENANCE TOOL")
    print("==================================================")
    print(f"Target: {MANAGER_URL}")
    print(f"User:   {ADMIN_USER}")
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
        
        # Headers auto-match the MANAGER_URL to pass CSRF security
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
