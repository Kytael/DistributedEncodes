import os, sqlite3
from datetime import datetime
import shutil

# ... (Imports and Config check remain the same) ...

def populate_db():
    print(f"[*] Scanning {SOURCE_DIRECTORY}...")
    if not os.path.exists(SOURCE_DIRECTORY):
        print(f"[!] Error: {SOURCE_DIRECTORY} missing."); return
    conn = sqlite3.connect(DB_FILE); cursor = conn.cursor()
    
    # [FIXED] Added started_at to the CREATE definition matches manager.py
    cursor.execute('''CREATE TABLE IF NOT EXISTS jobs (
        id TEXT PRIMARY KEY, filename TEXT, status TEXT, worker_id TEXT, 
        progress INTEGER DEFAULT 0, duration INTEGER DEFAULT 0, 
        last_updated TIMESTAMP, started_at TIMESTAMP
    )''')
    
    count_added = 0
    for root, dirs, files in os.walk(SOURCE_DIRECTORY, topdown=True):
        dirs.sort(); files.sort()
        for file in files:
            if file.lower().endswith(VIDEO_EXTENSIONS):
                full_path = os.path.join(root, file)
                rel_path = os.path.relpath(full_path, SOURCE_DIRECTORY)
                cursor.execute("SELECT status FROM jobs WHERE id = ?", (rel_path,))
                if not cursor.fetchone():
                    # [FIXED] Insert statement remains valid (started_at defaults to Null)
                    cursor.execute("INSERT INTO jobs (id, filename, status, last_updated) VALUES (?, ?, 'queued', ?)", (rel_path, file, datetime.now()))
                    print(f"[+] Added: {rel_path}"); count_added += 1
    conn.commit(); conn.close()
    print(f"[*] Added {count_added} new jobs.")

if __name__ == "__main__":
    populate_db()
