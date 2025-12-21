import sqlite3
import json
import os
import time
import shutil
import threading
from flask import Flask, jsonify, request, render_template
from flask_cors import CORS

app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": "*"}}, supports_credentials=True)

# --- CONFIGURATION ---
DB_NAME = "queue.db"
# [CHANGE ME] Shared Secret Key
API_TOKEN = "FractumSecure2025" 
PRESET_FILE = "FractumAV1.json"
TOLERANCE_HEIGHT = 10
TOLERANCE_DURATION = 5

# --- BACKGROUND BACKUP SYSTEM ---
def backup_loop():
    while True:
        time.sleep(3600) # Run every hour
        try:
            timestamp = int(time.time())
            shutil.copy2(DB_NAME, f"{DB_NAME}.bak")
            print(f"[Backup] Database backed up at {timestamp}")
        except Exception as e:
            print(f"[Backup Error] {e}")

# Start the backup thread
threading.Thread(target=backup_loop, daemon=True).start()

def load_preset_rules():
    defaults = {"codec": "av1", "height": 480, "quality_check": "63", "speed_preset": "2"}
    if not os.path.exists(PRESET_FILE): return defaults
    try:
        with open(PRESET_FILE, 'r') as f: data = json.load(f)
        preset = data.get('PresetList', [])[0]
        encoder = preset.get('VideoEncoder', 'svt_av1')
        if 'av1' in encoder: valid_codec = 'av1'
        elif '265' in encoder: valid_codec = 'hevc'
        else: valid_codec = encoder
        return {
            "codec": valid_codec,
            "height": int(preset.get('PictureHeight', 480)),
            "quality_check": str(preset.get('VideoQualitySlider', 63)),
            "speed_preset": str(preset.get('VideoPreset', '2'))
        }
    except: return defaults

RULES = load_preset_rules()

def init_db():
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    # Main Jobs Table
    c.execute('''CREATE TABLE IF NOT EXISTS jobs 
                 (id INTEGER PRIMARY KEY, filename TEXT, status TEXT, 
                  assigned_to TEXT, last_heartbeat INTEGER, source_duration REAL)''')
    
    # Legacy User Stats (For "All Time" filtering)
    c.execute('''CREATE TABLE IF NOT EXISTS users 
                 (username TEXT PRIMARY KEY, total_minutes REAL DEFAULT 0, jobs_completed INTEGER DEFAULT 0)''')
    
    # NEW: Work Log (For "24h" and "30d" filtering)
    c.execute('''CREATE TABLE IF NOT EXISTS work_log 
                 (id INTEGER PRIMARY KEY, username TEXT, duration_minutes REAL, timestamp INTEGER)''')
    
    conn.commit()
    conn.close()

def check_auth():
    return request.headers.get('X-Auth-Token') == API_TOKEN

# --- PREVENT BROWSER CACHING ---
@app.after_request
def add_header(r):
    r.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
    r.headers["Pragma"] = "no-cache"
    r.headers["Expires"] = "0"
    return r

# --- ROUTES ---

@app.route('/')
def dashboard():
    return render_template('index.html')

@app.route('/get_job', methods=['POST'])
def get_job():
    if not check_auth(): return jsonify({"status": "forbidden"}), 403
    username = request.json.get('username', 'Anonymous')
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    
    # Reset stalled jobs (>24h)
    timeout = int(time.time()) - 86400
    c.execute("UPDATE jobs SET status = 'PENDING', assigned_to = NULL WHERE status = 'PROCESSING' AND last_heartbeat < ?", (timeout,))
    conn.commit()

    # Find a job
    c.execute("SELECT id, filename FROM jobs WHERE status = 'PENDING' LIMIT 1")
    job = c.fetchone()
    if job:
        c.execute("UPDATE jobs SET status = 'PROCESSING', assigned_to = ?, last_heartbeat = ? WHERE id = ?", (username, int(time.time()), job[0]))
        conn.commit()
        conn.close()
        return jsonify({"status": "found", "id": job[0], "filename": job[1]})
    
    conn.close()
    return jsonify({"status": "empty"})

@app.route('/complete_job', methods=['POST'])
def complete_job():
    if not check_auth(): return jsonify({"status": "forbidden"}), 403
    data = request.json
    job_id = data.get('id')
    username = data.get('username')
    meta = data.get('metadata', {})

    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute("SELECT status, assigned_to, source_duration FROM jobs WHERE id = ?", (job_id,))
    row = c.fetchone()
    
    if not row:
        conn.close()
        return jsonify({"status": "error", "message": "Job not found"}), 404
        
    status, assigned, expected_dur = row

    if status == 'DONE':
        conn.close()
        return jsonify({"status": "error", "message": "Already done"}), 409
    if assigned and assigned != username:
        conn.close()
        return jsonify({"status": "error", "message": "User mismatch"}), 403

    # Verification
    if meta.get('codec_name', '').lower() != RULES['codec']: return jsonify({"status": "error", "message": "Wrong Codec"}), 400
    if abs(int(meta.get('height', 0)) - RULES['height']) > TOLERANCE_HEIGHT: return jsonify({"status": "error", "message": "Wrong Resolution"}), 400
    
    # Success Logic
    dur_min = float(meta.get('duration', 0)) / 60.0
    timestamp = int(time.time())
    
    c.execute("UPDATE jobs SET status = 'DONE' WHERE id = ?", (job_id,))
    
    # Update Legacy Stats
    c.execute("INSERT OR IGNORE INTO users (username) VALUES (?)", (username,))
    c.execute("UPDATE users SET total_minutes = total_minutes + ?, jobs_completed = jobs_completed + 1 WHERE username = ?", (dur_min, username))
    
    # Insert into Work Log (New)
    c.execute("INSERT INTO work_log (username, duration_minutes, timestamp) VALUES (?, ?, ?)", (username, dur_min, timestamp))
    
    conn.commit()
    conn.close()
    return jsonify({"status": "success"})

@app.route('/fail_job', methods=['POST'])
def fail_job():
    if not check_auth(): return jsonify({"status": "forbidden"}), 403
    conn = sqlite3.connect(DB_NAME)
    conn.execute("UPDATE jobs SET status = 'PENDING', assigned_to = NULL WHERE id = ?", (request.json.get('id'),))
    conn.commit()
    conn.close()
    return jsonify({"status": "reset"})

@app.route('/populate', methods=['POST'])
def populate():
    if not check_auth(): return jsonify({"status": "forbidden"}), 403
    files = request.json.get('files', [])
    conn = sqlite3.connect(DB_NAME)
    count = 0
    for item in files:
        if not conn.execute("SELECT id FROM jobs WHERE filename = ?", (item['filename'],)).fetchone():
            conn.execute("INSERT INTO jobs (filename, status, last_heartbeat, source_duration) VALUES (?, 'PENDING', 0, ?)", (item['filename'], item['duration']))
            count += 1
    conn.commit()
    conn.close()
    return jsonify({"added": count})

# --- THE STATS ENGINE (THIS WAS MISSING) ---
@app.route('/stats')
def stats():
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    
    timeframe = request.args.get('filter', 'all')
    users = []
    
    if timeframe == '24h':
        cutoff = int(time.time()) - 86400
        c.execute("SELECT username, SUM(duration_minutes), COUNT(*) FROM work_log WHERE timestamp > ? GROUP BY username ORDER BY SUM(duration_minutes) DESC", (cutoff,))
        users = [{"name": r[0], "time": r[1] or 0, "count": r[2]} for r in c.fetchall()]

    elif timeframe == '30d':
        cutoff = int(time.time()) - (86400 * 30)
        c.execute("SELECT username, SUM(duration_minutes), COUNT(*) FROM work_log WHERE timestamp > ? GROUP BY username ORDER BY SUM(duration_minutes) DESC", (cutoff,))
        users = [{"name": r[0], "time": r[1] or 0, "count": r[2]} for r in c.fetchall()]

    else:
        # All Time (Legacy)
        c.execute("SELECT username, total_minutes, jobs_completed FROM users ORDER BY total_minutes DESC")
        users = [{"name": r[0], "time": r[1], "count": r[2]} for r in c.fetchall()]

    # Queue Counts
    c.execute("SELECT status, COUNT(*) FROM jobs GROUP BY status")
    counts = dict(c.fetchall())
    queue_stats = {
        "pending": counts.get('PENDING', 0),
        "processing": counts.get('PROCESSING', 0),
        "done": counts.get('DONE', 0),
        "total": sum(counts.values())
    }
    
    # Active Jobs List
    c.execute("SELECT id, assigned_to, filename FROM jobs WHERE status = 'PROCESSING'")
    active_jobs = [{"id": r[0], "user": r[1], "file": r[2]} for r in c.fetchall()]
    
    conn.close()
    return jsonify({"users": users, "queue": queue_stats, "active": active_jobs})

if __name__ == '__main__':
    init_db()
    # Listen on all interfaces
    app.run(host='0.0.0.0', port=5000)
