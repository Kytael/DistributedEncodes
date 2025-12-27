import os, sqlite3
from datetime import datetime

try:
    from config import SOURCE_DIRECTORY, DB_FILE, VIDEO_EXTENSIONS
except ImportError:
    print("Error: 'config.py' not found. Please copy 'config.py.example' to 'config.py'.")
    exit(1)

def populate_db():
    print(f"[*] Scanning {SOURCE_DIRECTORY}...")
    if not os.path.exists(SOURCE_DIRECTORY):
        print(f"[!] Error: {SOURCE_DIRECTORY} missing."); return
    conn = sqlite3.connect(DB_FILE); cursor = conn.cursor()
    cursor.execute('''CREATE TABLE IF NOT EXISTS jobs (id TEXT PRIMARY KEY, filename TEXT, status TEXT, worker_id TEXT, progress INTEGER DEFAULT 0, duration INTEGER DEFAULT 0, last_updated TIMESTAMP)''')
    
    count_added = 0
    for root, dirs, files in os.walk(SOURCE_DIRECTORY, topdown=True):
        dirs.sort(); files.sort()
        for file in files:
            if file.lower().endswith(VIDEO_EXTENSIONS):
                full_path = os.path.join(root, file)
                rel_path = os.path.relpath(full_path, SOURCE_DIRECTORY)
                cursor.execute("SELECT status FROM jobs WHERE id = ?", (rel_path,))
                if not cursor.fetchone():
                    cursor.execute("INSERT INTO jobs (id, filename, status, last_updated) VALUES (?, ?, 'queued', ?)", (rel_path, file, datetime.now()))
                    print(f"[+] Added: {rel_path}"); count_added += 1
    conn.commit(); conn.close()
    print(f"[*] Added {count_added} new jobs.")

if __name__ == "__main__":
    populate_db()
