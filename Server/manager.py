import sqlite3
import json
import os
import time
from flask import Flask, jsonify, request, render_template
from flask_cors import CORS

app = Flask(__name__)
CORS(app) # Keeps CORS enabled just in case you use external tools later

# --- CONFIGURATION ---
DB_NAME = "queue.db"
API_TOKEN = "SecretTokenThisMustMatchTheTokenInWorkerPy"
PRESET_FILE = "PlaceYourExportedHandbrakePresetHere.json"
TOLERANCE_HEIGHT = 10
TOLERANCE_DURATION = 5

def load_preset_rules():
    defaults = {"codec": "av1", "height": 480, "quality_check": "63", "speed_preset": "2"}
    if not os.path.exists(PRESET_FILE): return defaults
    try:
        with open(PRESET_FILE, 'r') as f:
            data = json.load(f)
        preset = data.get('PresetList', [])[0]
        encoder = preset.get('VideoEncoder', 'svt_av1')
        if 'av1' in encoder: valid_codec = 'av1'
        elif '265' in encoder or 'hevc' in encoder: valid_codec = 'hevc'
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
    c.execute('''CREATE TABLE IF NOT EXISTS jobs 
                 (id INTEGER PRIMARY KEY, filename TEXT, status TEXT, 
                  assigned_to TEXT, last_heartbeat INTEGER, source_duration REAL)''')
    c.execute('''CREATE TABLE IF NOT EXISTS users 
                 (username TEXT PRIMARY KEY, total_minutes REAL DEFAULT 0, jobs_completed INTEGER DEFAULT 0)''')
    conn.commit()
    conn.close()

# --- AUTH HELPER ---
def check_auth():
    token = request.headers.get('X-Auth-Token')
    return token == API_TOKEN

# --- DASHBOARD ROUTE (NEW) ---
@app.route('/')
def dashboard():
    # This serves the 'templates/index.html' file
    return render_template('index.html')

# --- WORKER API ---
@app.route('/get_job', methods=['POST'])
def get_job():
    if not check_auth(): return jsonify({"status": "forbidden"}), 403

    username = request.json.get('username', 'Anonymous')
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    timeout = int(time.time()) - 86400
    c.execute("UPDATE jobs SET status = 'PENDING', assigned_to = NULL WHERE status = 'PROCESSING' AND last_heartbeat < ?", (timeout,))
    conn.commit()

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
    log = data.get('encoding_log', '')

    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute("SELECT source_duration FROM jobs WHERE id = ?", (job_id,))
    row = c.fetchone()
    expected_dur = row[0] if row else 0

    if meta.get('codec_name', '').lower() != RULES['codec']: return jsonify({"status": "error", "message": "Wrong Codec"}), 400
    if abs(int(meta.get('height', 0)) - RULES['height']) > TOLERANCE_HEIGHT: return jsonify({"status": "error", "message": "Wrong Resolution"}), 400
    if f'"Quality": {RULES["quality_check"]}' not in log: return jsonify({"status": "error", "message": "Wrong Quality Setting"}), 400
    if expected_dur > 0 and abs(float(meta.get('duration', 0)) - expected_dur) > TOLERANCE_DURATION: return jsonify({"status": "error", "message": "Duration Mismatch"}), 400

    dur_min = float(meta.get('duration', 0)) / 60.0
    c.execute("UPDATE jobs SET status = 'DONE' WHERE id = ?", (job_id,))
    c.execute("INSERT OR IGNORE INTO users (username) VALUES (?)", (username,))
    c.execute("UPDATE users SET total_minutes = total_minutes + ?, jobs_completed = jobs_completed + 1 WHERE username = ?", (dur_min, username))
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

# --- ADMIN API ---
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

# --- STATS API (PUBLIC) ---
@app.route('/stats')
def stats():
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute("SELECT username, total_minutes, jobs_completed FROM users ORDER BY total_minutes DESC")
    users = [{"name": r[0], "time": r[1], "count": r[2]} for r in c.fetchall()]
    c.execute("SELECT status, COUNT(*) FROM jobs GROUP BY status")
    counts = dict(c.fetchall())
    queue_stats = {"pending": counts.get('PENDING', 0), "processing": counts.get('PROCESSING', 0), "done": counts.get('DONE', 0), "total": sum(counts.values())}
    c.execute("SELECT id, assigned_to, filename FROM jobs WHERE status = 'PROCESSING'")
    active_jobs = [{"id": r[0], "user": r[1], "file": r[2]} for r in c.fetchall()]
    conn.close()
    return jsonify({"users": users, "queue": queue_stats, "active": active_jobs})

if __name__ == '__main__':
    init_db()
    app.run(host='0.0.0.0', port=5000)