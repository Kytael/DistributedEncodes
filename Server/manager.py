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

# --- SECURE CONFIGURATION ---
DB_NAME = "queue.db"
PRESET_FILE = "FractumAV1.json"
TOLERANCE_HEIGHT = 10
TOLERANCE_DURATION = 5
HEARTBEAT_TIMEOUT = 1800 

# Load secrets from Environment Variables
# Defaults provided for API to allow easy start, but ADMIN requires explicit setup for safety.
API_TOKEN = os.environ.get("FRACTUM_API_TOKEN", "FractumSecure2025") 
ADMIN_TOKEN = os.environ.get("FRACTUM_ADMIN_TOKEN") 

# --- BACKGROUND BACKUP SYSTEM ---
def backup_loop():
    while True:
        time.sleep(3600)
        try:
            timestamp = int(time.time())
            shutil.copy2(DB_NAME, f"{DB_NAME}.bak")
            print(f"[Backup] Database backed up at {timestamp}")
        except Exception as e:
            print(f"[Backup Error] {e}")

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
    # Enable Write-Ahead Logging for better concurrency
    conn.execute("PRAGMA journal_mode=WAL;")
    c = conn.cursor()
    
    # Create Tables
    c.execute('''CREATE TABLE IF NOT EXISTS jobs 
                 (id INTEGER PRIMARY KEY, filename TEXT, status TEXT, 
                  assigned_to TEXT, last_heartbeat INTEGER, source_duration REAL)''')
    c.execute('''CREATE TABLE IF NOT EXISTS users 
                 (username TEXT PRIMARY KEY, total_minutes REAL DEFAULT 0, jobs_completed INTEGER DEFAULT 0)''')
    c.execute('''CREATE TABLE IF NOT EXISTS work_log 
                 (id INTEGER PRIMARY KEY, username TEXT, duration_minutes REAL, timestamp INTEGER)''')
    
    # Migration: Add progress column if it doesn't exist
    try:
        c.execute("ALTER TABLE jobs ADD COLUMN progress INTEGER DEFAULT 0")
        print("[System] Database upgraded: Added 'progress' column.")
    except sqlite3.OperationalError:
        pass 
        
    conn.commit()
    conn.close()

def check_auth():
    return request.headers.get('X-Auth-Token') == API_TOKEN

@app.after_request
def add_header(r):
    r.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
    r.headers["Pragma"] = "no-cache"
    r.headers["Expires"] = "0"
    return r

@app.route('/')
def dashboard():
    return render_template('index.html')

@app.route('/heartbeat', methods=['POST'])
def heartbeat():
    if not check_auth(): return jsonify({"status": "forbidden"}), 403
    data = request.json
    job_id = data.get('id')
    progress = data.get('progress', 0)
    
    if job_id:
        conn = sqlite3.connect(DB_NAME)
        conn.execute("UPDATE jobs SET last_heartbeat = ?, progress = ? WHERE id = ? AND status = 'PROCESSING'", 
                     (int(time.time()), progress, job_id))
        conn.commit()
        conn.close()
    return jsonify({"status": "ok"})

@app.route('/get_job', methods=['POST'])
def get_job():
    if not check_auth(): return jsonify({"status": "forbidden"}), 403
    username = request.json.get('username', 'Anonymous')
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    
    # Reset Dead Workers
    dead_threshold = int(time.time()) - HEARTBEAT_TIMEOUT
    c.execute("UPDATE jobs SET status = 'PENDING', assigned_to = NULL, progress = 0 WHERE status = 'PROCESSING' AND last_heartbeat < ?", (dead_threshold,))
    conn.commit()

    # Find Job
    c.execute("SELECT id, filename FROM jobs WHERE status = 'PENDING' LIMIT 1")
    job = c.fetchone()
    if job:
        c.execute("UPDATE jobs SET status = 'PROCESSING', assigned_to = ?, last_heartbeat = ?, progress = 0 WHERE id = ?", (username, int(time.time()), job[0]))
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
    
    # Allow user mismatch if worker name is included: "User [Worker]"
    is_worker_match = assigned and assigned.startswith(username + " [")
    if assigned and assigned != username and not is_worker_match:
        conn.close()
        return jsonify({"status": "error", "message": "User mismatch"}), 403

    # Validate Encode
    if meta.get('codec_name', '').lower() != RULES['codec']: return jsonify({"status": "error", "message": "Wrong Codec"}), 400
    if abs(int(meta.get('height', 0)) - RULES['height']) > TOLERANCE_HEIGHT: return jsonify({"status": "error", "message": "Wrong Resolution"}), 400
    
    dur_min = float(meta.get('duration', 0)) / 60.0
    timestamp = int(time.time())
    
    # Finalize
    c.execute("UPDATE jobs SET status = 'DONE', progress = 100 WHERE id = ?", (job_id,))
    c.execute("INSERT OR IGNORE INTO users (username) VALUES (?)", (username,))
    c.execute("UPDATE users SET total_minutes = total_minutes + ?, jobs_completed = jobs_completed + 1 WHERE username = ?", (dur_min, username))
    c.execute("INSERT INTO work_log (username, duration_minutes, timestamp) VALUES (?, ?, ?)", (username, dur_min, timestamp))
    
    conn.commit()
    conn.close()
    return jsonify({"status": "success"})

@app.route('/fail_job', methods=['POST'])
def fail_job():
    if not check_auth(): return jsonify({"status": "forbidden"}), 403
    conn = sqlite3.connect(DB_NAME)
    conn.execute("UPDATE jobs SET status = 'PENDING', assigned_to = NULL, progress = 0 WHERE id = ?", (request.json.get('id'),))
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
            conn.execute("INSERT INTO jobs (filename, status, last_heartbeat, source_duration, progress) VALUES (?, 'PENDING', 0, ?, 0)", (item['filename'], item['duration']))
            count += 1
    conn.commit()
    conn.close()
    return jsonify({"added": count})

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
        c.execute("SELECT username, total_minutes, jobs_completed FROM users ORDER BY total_minutes DESC")
        users = [{"name": r[0], "time": r[1], "count": r[2]} for r in c.fetchall()]

    c.execute("SELECT status, COUNT(*) FROM jobs GROUP BY status")
    counts = dict(c.fetchall())
    queue_stats = {"pending": counts.get('PENDING', 0), "processing": counts.get('PROCESSING', 0), "done": counts.get('DONE', 0), "total": sum(counts.values())}
    
    c.execute("SELECT id, assigned_to, filename, progress FROM jobs WHERE status = 'PROCESSING'")
    active_jobs = [{"id": r[0], "user": r[1], "file": r[2], "progress": r[3]} for r in c.fetchall()]
    
    conn.close()
    return jsonify({"users": users, "queue": queue_stats, "active": active_jobs})

# [SECURE ADMIN ENDPOINT]
@app.route('/admin/reset', methods=['GET'])
def admin_reset():
    if not ADMIN_TOKEN:
        print("[!] Security Alert: Attempted access to /admin/reset but FRACTUM_ADMIN_TOKEN is not set.")
        return "Admin interface disabled. Please configure FRACTUM_ADMIN_TOKEN on the server.", 503

    user_token = request.args.get('token')
    if not user_token or user_token != ADMIN_TOKEN:
        print(f"[!] Unauthorized Admin Access Attempt from {request.remote_addr}")
        return "Unauthorized", 403
    
    job_id = request.args.get('id')
    if not job_id: return "Missing ID", 400
    
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.execute("SELECT id FROM jobs WHERE id = ?", (job_id,))
    if not cursor.fetchone():
        conn.close()
        return "Job ID not found", 404

    conn.execute("UPDATE jobs SET status = 'PENDING', assigned_to = NULL, last_heartbeat = 0, progress = 0 WHERE id = ?", (job_id,))
    conn.commit()
    conn.close()
    
    print(f"[ADMIN] Job {job_id} manually reset by administrator.")
    return f"Job {job_id} has been reset to PENDING."

if __name__ == '__main__':
    init_db()
    app.run(host='0.0.0.0', port=5000)
