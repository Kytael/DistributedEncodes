from flask import Flask, request, jsonify
from flask_cors import CORS
import sqlite3, os, datetime, time, threading
from functools import wraps

app = Flask(__name__)
CORS(app) 

DB_NAME = "queue.db"
API_TOKEN = "FractumSecure2025"

# --- SECURITY: RATE LIMITER ---
REQUEST_HISTORY = {}
LIMIT_WINDOW = 60 
MAX_REQUESTS = 30 

def check_rate_limit():
    ip = request.remote_addr
    now = time.time()
    if ip in REQUEST_HISTORY:
        REQUEST_HISTORY[ip] = [t for t in REQUEST_HISTORY[ip] if t > now - LIMIT_WINDOW]
    else:
        REQUEST_HISTORY[ip] = []
    if len(REQUEST_HISTORY[ip]) >= MAX_REQUESTS: return True
    REQUEST_HISTORY[ip].append(now)
    return False

def check_auth(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if check_rate_limit(): return jsonify({"status": "error", "message": "Rate limit"}), 429
        token = request.headers.get("X-Auth-Token")
        if token != API_TOKEN: return jsonify({"status": "error", "message": "Unauthorized"}), 401
        agent = request.headers.get("User-Agent", "")
        if "python-requests" not in agent and "Fractum" not in agent:
             return jsonify({"status": "error", "message": "Invalid Client"}), 403
        return f(*args, **kwargs)
    return decorated

def init_db():
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    
    # 1. Create Tables
    c.execute('''CREATE TABLE IF NOT EXISTS jobs 
                 (id INTEGER PRIMARY KEY, filename TEXT, status TEXT, 
                  worker TEXT, start_time INTEGER, end_time INTEGER, progress INTEGER DEFAULT 0)''')
    c.execute('''CREATE TABLE IF NOT EXISTS users 
                 (username TEXT PRIMARY KEY, total_minutes REAL DEFAULT 0, jobs_completed INTEGER DEFAULT 0)''')
    c.execute('''CREATE TABLE IF NOT EXISTS work_log 
                 (id INTEGER PRIMARY KEY, username TEXT, duration_minutes REAL, timestamp INTEGER)''')
    
    # 2. COLUMNS MIGRATION
    c.execute("PRAGMA table_info(jobs)")
    columns = [info[1] for info in c.fetchall()]
    for col, dtype in {'worker': 'TEXT', 'progress': 'INTEGER DEFAULT 0', 'start_time': 'INTEGER', 'end_time': 'INTEGER'}.items():
        if col not in columns:
            try: c.execute(f"ALTER TABLE jobs ADD COLUMN {col} {dtype}")
            except: pass

    # 3. STATUS MIGRATION (Fix Uppercase/Lowercase)
    try:
        c.execute("UPDATE jobs SET status='pending' WHERE status='PENDING'")
        # Note: We do NOT reset PROCESSING here anymore to avoid hiding active workers.
        # Instead, we catch them in the heartbeat.
        c.execute("UPDATE jobs SET status='completed' WHERE status='DONE'")
        if 'assigned_to' in columns and 'worker' in columns:
            c.execute("UPDATE jobs SET worker=assigned_to WHERE worker IS NULL")
    except: pass

    conn.commit()
    conn.close()

@app.route('/get_job', methods=['POST'])
@check_auth
def get_job():
    data = request.json
    worker_name = data.get('username', 'Unknown')
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    
    # Reset stale jobs (>4 hours)
    cutoff = int(time.time()) - (4 * 3600)
    c.execute("UPDATE jobs SET status='pending', worker=NULL, progress=0 WHERE status='processing' AND start_time < ?", (cutoff,))
    conn.commit()

    c.execute("SELECT id, filename FROM jobs WHERE status='pending' ORDER BY id ASC LIMIT 1")
    job = c.fetchone()
    if job:
        job_id, filename = job
        c.execute("UPDATE jobs SET status='processing', worker=?, start_time=?, progress=0 WHERE id=?", 
                  (worker_name, int(time.time()), job_id))
        conn.commit()
        conn.close()
        return jsonify({"status": "found", "id": job_id, "filename": filename})
    
    conn.close()
    return jsonify({"status": "empty"})

@app.route('/heartbeat', methods=['POST'])
@check_auth
def heartbeat():
    data = request.json
    if not data or 'id' not in data: return jsonify({"status": "error"}), 400
    try: progress = max(0, min(100, int(data.get('progress', 0))))
    except: progress = 0
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    
    # [FIX] Revive "invisible" jobs. If a worker sends a heartbeat, force status to 'processing'.
    c.execute("""
        UPDATE jobs 
        SET start_time=?, progress=?, status='processing' 
        WHERE id=? AND status != 'completed'
    """, (int(time.time()), progress, data['id']))
    
    conn.commit()
    conn.close()
    return jsonify({"status": "ok"})

@app.route('/complete_job', methods=['POST'])
@check_auth
def complete_job():
    data = request.json
    job_id = data.get('id')
    username = data.get('username')
    metadata = data.get('metadata')
    if not job_id or not username: return jsonify({"status": "error"}), 400

    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    
    # Allow completion if status is processing OR if it was pending (revived)
    c.execute("SELECT worker, status FROM jobs WHERE id=?", (job_id,))
    row = c.fetchone()
    if not row or row[1] == 'completed':
        conn.close(); return jsonify({"status": "error", "message": "Invalid job state"}), 400

    duration_min = 0
    if metadata and 'duration' in metadata:
        try: duration_min = min(600, max(0, float(metadata['duration']) / 60.0))
        except: pass

    c.execute("UPDATE jobs SET status='completed', end_time=?, progress=100 WHERE id=?", (int(time.time()), job_id))
    c.execute("INSERT OR IGNORE INTO users (username) VALUES (?)", (username,))
    c.execute("UPDATE users SET total_minutes = total_minutes + ?, jobs_completed = jobs_completed + 1 WHERE username=?", (duration_min, username))
    c.execute("INSERT INTO work_log (username, duration_minutes, timestamp) VALUES (?, ?, ?)", (username, duration_min, int(time.time())))
    conn.commit()
    conn.close()
    return jsonify({"status": "success"})

@app.route('/fail_job', methods=['POST'])
@check_auth
def fail_job():
    data = request.json
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute("UPDATE jobs SET status='pending', worker=NULL, progress=0 WHERE id=?", (data.get('id'),))
    conn.commit()
    conn.close()
    return jsonify({"status": "reset"})

# [RESTORED] Required for adding new files
@app.route('/populate', methods=['POST'])
@check_auth
def populate():
    files = request.json.get('files', [])
    if not files: return jsonify({"added": 0})
    
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    count = 0
    for item in files:
        # Check for duplicates
        c.execute("SELECT id FROM jobs WHERE filename = ?", (item['filename'],))
        if not c.fetchone():
            c.execute("INSERT INTO jobs (filename, status, start_time, progress) VALUES (?, 'pending', 0, 0)", 
                      (item['filename'],))
            count += 1
    conn.commit()
    conn.close()
    return jsonify({"added": count})

@app.route('/stats', methods=['GET'])
def stats():
    filter_type = request.args.get('filter', 'all')
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    
    c.execute("SELECT status, COUNT(*) FROM jobs GROUP BY status")
    counts = dict(c.fetchall())
    # Handle both old Uppercase and new Lowercase keys just in case
    queue_stats = {
        "total": sum(counts.values()),
        "pending": counts.get('pending', 0) + counts.get('PENDING', 0),
        "processing": counts.get('processing', 0) + counts.get('PROCESSING', 0),
        "done": counts.get('completed', 0) + counts.get('DONE', 0)
    }

    if filter_type == '24h':
        cutoff = int(time.time()) - 86400
        c.execute('''SELECT username, SUM(duration_minutes) as time, COUNT(*) as count 
                     FROM work_log WHERE timestamp > ? GROUP BY username ORDER BY time DESC''', (cutoff,))
    elif filter_type == '30d':
        cutoff = int(time.time()) - (30 * 86400)
        c.execute('''SELECT username, SUM(duration_minutes) as time, COUNT(*) as count 
                     FROM work_log WHERE timestamp > ? GROUP BY username ORDER BY time DESC''', (cutoff,))
    else:
        c.execute("SELECT username, total_minutes, jobs_completed FROM users ORDER BY total_minutes DESC")

    users = [{"name": r[0], "time": round(r[1] or 0, 1), "count": r[2]} for r in c.fetchall()]

    try:
        # Select active workers. Coalesce worker name to 'Unknown' if NULL.
        c.execute("SELECT COALESCE(worker, 'Unknown'), filename, progress FROM jobs WHERE status='processing'")
        active = [{"user": r[0], "file": r[1], "progress": r[2]} for r in c.fetchall()]
    except:
        active = []
    
    conn.close()
    return jsonify({"queue": queue_stats, "users": users, "active": active})

if __name__ == '__main__':
    init_db()
    app.run(host='0.0.0.0', port=5000)
