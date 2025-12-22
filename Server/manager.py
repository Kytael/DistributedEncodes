from flask import Flask, request, jsonify
import sqlite3, os, datetime, time, threading
from functools import wraps

app = Flask(__name__)
DB_NAME = "queue.db"
API_TOKEN = "FractumSecure2025"

# --- SECURITY: RATE LIMITER ---
# Format: {ip_address: [timestamp1, timestamp2, ...]}
REQUEST_HISTORY = {}
LIMIT_WINDOW = 60  # seconds
MAX_REQUESTS = 30  # Max requests per minute per IP

def check_rate_limit():
    ip = request.remote_addr
    now = time.time()
    
    # 1. Clean up old history
    if ip in REQUEST_HISTORY:
        REQUEST_HISTORY[ip] = [t for t in REQUEST_HISTORY[ip] if t > now - LIMIT_WINDOW]
    else:
        REQUEST_HISTORY[ip] = []

    # 2. Check limit
    if len(REQUEST_HISTORY[ip]) >= MAX_REQUESTS:
        return True # Limited
    
    # 3. Add current request
    REQUEST_HISTORY[ip].append(now)
    return False

def check_auth(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        # 1. Check Rate Limit
        if check_rate_limit():
            return jsonify({"status": "error", "message": "Rate limit exceeded"}), 429

        # 2. Check Token
        token = request.headers.get("X-Auth-Token")
        if token != API_TOKEN:
            return jsonify({"status": "error", "message": "Unauthorized"}), 401
        
        # 3. Check User-Agent (Optional but helpful)
        agent = request.headers.get("User-Agent", "")
        if "python-requests" not in agent and "Fractum" not in agent:
             return jsonify({"status": "error", "message": "Invalid Client"}), 403
             
        return f(*args, **kwargs)
    return decorated

def init_db():
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS jobs 
                 (id INTEGER PRIMARY KEY, filename TEXT, status TEXT, 
                  worker TEXT, start_time INTEGER, end_time INTEGER, progress INTEGER DEFAULT 0)''')
    c.execute('''CREATE TABLE IF NOT EXISTS users 
                 (username TEXT PRIMARY KEY, total_minutes REAL DEFAULT 0, jobs_completed INTEGER DEFAULT 0)''')
    c.execute('''CREATE TABLE IF NOT EXISTS work_log 
                 (id INTEGER PRIMARY KEY, username TEXT, duration_minutes REAL, timestamp INTEGER)''')
    conn.commit()
    conn.close()

# ... (Previous helper functions get_db_connection, etc. remain the same) ...

@app.route('/get_job', methods=['POST'])
@check_auth
def get_job():
    data = request.json
    worker_name = data.get('username', 'Unknown')
    
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    
    # Reset stale jobs (older than 4 hours)
    cutoff = int(time.time()) - (4 * 3600)
    c.execute("UPDATE jobs SET status='pending', worker=NULL, progress=0 WHERE status='processing' AND start_time < ?", (cutoff,))
    conn.commit()

    # Get next job
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
    
    # Input Sanitization
    try:
        progress = int(data.get('progress', 0))
        if progress < 0 or progress > 100: progress = 0
    except: progress = 0

    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute("UPDATE jobs SET start_time=?, progress=? WHERE id=?", (int(time.time()), progress, data['id']))
    conn.commit()
    conn.close()
    return jsonify({"status": "ok"})

@app.route('/complete_job', methods=['POST'])
@check_auth
def complete_job():
    data = request.json
    job_id = data.get('id')
    username = data.get('username')
    metadata = data.get('metadata') # Expects {'duration': 120.5, ...}

    if not job_id or not username:
        return jsonify({"status": "error", "message": "Missing data"}), 400

    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()

    # VALIDATION: Check if job was actually assigned to this user
    c.execute("SELECT worker, status FROM jobs WHERE id=?", (job_id,))
    row = c.fetchone()
    if not row:
        conn.close()
        return jsonify({"status": "error", "message": "Job not found"}), 404
        
    db_worker, db_status = row
    # Loose match (db_worker might be "User [Worker1]", request might be just "User")
    # For tighter security, enforce exact match.
    if db_status != 'processing':
        conn.close()
        return jsonify({"status": "error", "message": "Job not in processing state"}), 400

    # Calculate Duration
    duration_min = 0
    if metadata and 'duration' in metadata:
        try:
            duration_min = float(metadata['duration']) / 60.0
            # Sanity Check: Reject unrealistic durations (e.g., > 10 hours for one file?)
            if duration_min < 0: duration_min = 0
            if duration_min > 600: duration_min = 600 # Cap at 10 hours to prevent database pollution
        except: pass

    # Update Job
    c.execute("UPDATE jobs SET status='completed', end_time=?, progress=100 WHERE id=?", (int(time.time()), job_id))
    
    # Update User Stats
    c.execute("INSERT OR IGNORE INTO users (username) VALUES (?)", (username,))
    c.execute("UPDATE users SET total_minutes = total_minutes + ?, jobs_completed = jobs_completed + 1 WHERE username=?", (duration_min, username))
    
    # Log Work
    c.execute("INSERT INTO work_log (username, duration_minutes, timestamp) VALUES (?, ?, ?)", (username, duration_min, int(time.time())))

    conn.commit()
    conn.close()
    return jsonify({"status": "success"})

@app.route('/fail_job', methods=['POST'])
@check_auth
def fail_job():
    data = request.json
    job_id = data.get('id')
    
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute("UPDATE jobs SET status='pending', worker=NULL, progress=0 WHERE id=?", (job_id,))
    conn.commit()
    conn.close()
    return jsonify({"status": "reset"})

# ... (Stats route remains the same) ...

if __name__ == '__main__':
    init_db()
    app.run(host='0.0.0.0', port=5000)
