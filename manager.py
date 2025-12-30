import os
import time
import queue
import threading
import sqlite3
import subprocess
import json
import re
import shutil
import traceback
import uuid
import secrets
from functools import wraps
from datetime import datetime
from urllib.parse import quote

# [FIX] Added Flask-Limiter for rate limiting
from flask import Flask, request, jsonify, render_template, send_from_directory, send_file, Response, abort
from werkzeug.exceptions import HTTPException
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address

# ==============================================================================
# CONFIGURATION
# ==============================================================================

try:
    from config import (
        SERVER_HOST, SERVER_PORT, SERVER_URL_DISPLAY,
        SOURCE_DIRECTORY, COMPLETED_DIRECTORY, WORKER_TEMPLATE_FILE,
        DB_FILE, VIDEO_EXTENSIONS, 
        ADMIN_USER, ADMIN_PASS
    )
    
    # [FIX] Load new secrets from config.py, with fallbacks if you haven't added them yet
    try:
        from config import WORKER_SECRET
    except ImportError:
        print("[!] WARNING: WORKER_SECRET not found in config.py. Using unsafe default.")
        WORKER_SECRET = "DefaultInsecureSecret"

    try:
        from config import SECRET_KEY
    except ImportError:
        SECRET_KEY = secrets.token_hex(32)

except ImportError:
    print("[!] Critical Error: config.py not found.")
    exit(1)

app = Flask(__name__)
app.secret_key = SECRET_KEY

# [FIX] Secure Cookie Configuration
app.config['SESSION_COOKIE_SECURE'] = True    # Cookies only sent over HTTPS
app.config['SESSION_COOKIE_HTTPONLY'] = True  # JS cannot access cookies
app.config['SESSION_COOKIE_SAMESITE'] = 'Lax' # CSRF protection
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024 * 1024 

# [FIX] Initialize Rate Limiter
limiter = Limiter(
    get_remote_address,
    app=app,
    default_limits=["200 per day", "50 per hour"],
    storage_uri="memory://"
)

job_queue = queue.Queue()
queued_job_ids = set()
db_lock = threading.Lock()

# ==============================================================================
# SECURITY HELPERS
# ==============================================================================

def sanitize_input(val):
    """[FIX] Allow only safe characters: A-Z, a-z, 0-9, -, _"""
    if not val: return None
    return re.sub(r'[^a-zA-Z0-9_-]', '', str(val))

@app.after_request
def add_security_headers(response):
    """[FIX] Add CSP and Security Headers"""
    response.headers['Cross-Origin-Opener-Policy'] = 'same-origin'
    response.headers['Cross-Origin-Embedder-Policy'] = 'require-corp'
    response.headers['X-Frame-Options'] = 'DENY'
    response.headers['X-Content-Type-Options'] = 'nosniff'
    
    # Content Security Policy
    csp = (
        "default-src 'self'; "
        "script-src 'self' 'unsafe-inline' https://cdnjs.cloudflare.com; "
        "style-src 'self' 'unsafe-inline' https://fonts.googleapis.com; "
        "font-src 'self' https://fonts.gstatic.com; "
        "img-src 'self' data:; "
        "connect-src 'self' blob:;"
    )
    response.headers['Content-Security-Policy'] = csp
    return response

@app.before_request
def csrf_protect():
    """[FIX] Basic CSRF Protection for Admin API"""
    if request.method == "POST" and request.path.startswith('/api/admin_action'):
        # Check Origin/Referer
        origin = request.headers.get('Origin')
        
        # Allow if origin matches our server (simplified check)
        if origin and SERVER_HOST not in origin and 'localhost' not in origin:
            return jsonify({"status": "error", "message": "CSRF Blocked"}), 403

def check_auth(username, password):
    return username == ADMIN_USER and password == ADMIN_PASS

def authenticate():
    return Response(
        'Could not verify your access level for that URL.\n'
        'You have to login with proper credentials', 401,
        {'WWW-Authenticate': 'Basic realm="Login Required"'})

def requires_auth(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        auth = request.authorization
        if not auth or not check_auth(auth.username, auth.password):
            return authenticate()
        return f(*args, **kwargs)
    return decorated

def requires_worker_auth(f):
    """[MODIFIED] Auth Middleware with TRANSITION MODE (Legacy Support)"""
    @wraps(f)
    def decorated(*args, **kwargs):
        # Check header or query param
        token = request.headers.get('X-Worker-Token') or request.args.get('token')
        
        # --- TRANSITION MODE START ---
        # If no token is provided, assume it's an old worker and ALLOW it.
        if token is None:
            return f(*args, **kwargs)
        # --- TRANSITION MODE END ---

        # If a token IS provided, it MUST be correct (blocks attackers guessing)
        if token != WORKER_SECRET:
            return jsonify({"status": "error", "message": "Unauthorized Worker"}), 401
            
        return f(*args, **kwargs)
    return decorated

# ==============================================================================
# DATABASE & LOGGING
# ==============================================================================

def init_db():
    with db_lock:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS jobs (
                id TEXT PRIMARY KEY, filename TEXT, status TEXT, worker_id TEXT,
                progress INTEGER DEFAULT 0, duration INTEGER DEFAULT 0, last_updated TIMESTAMP,
                started_at TIMESTAMP, file_size INTEGER DEFAULT 0
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS system_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TIMESTAMP,
                level TEXT,
                message TEXT,
                related_id TEXT
            )
        ''')
        try: cursor.execute("ALTER TABLE jobs ADD COLUMN file_size INTEGER DEFAULT 0")
        except sqlite3.OperationalError: pass
        conn.commit(); conn.close()

def log_event(level, message, related_id=None):
    try:
        # [FIX] Sanitize logs before writing
        clean_msg = str(message).replace('<', '&lt;').replace('>', '&gt;')
        clean_id = sanitize_input(related_id) if related_id else None
        
        with db_lock:
            conn = sqlite3.connect(DB_FILE)
            conn.execute("INSERT INTO system_logs (timestamp, level, message, related_id) VALUES (?, ?, ?, ?)",
                         (datetime.now(), level, clean_msg, clean_id))
            conn.commit(); conn.close()
        print(f"[{level}] {message}") 
    except Exception as e:
        print(f"[!] Logging failed: {e}")

# ==============================================================================
# CORE LOGIC
# ==============================================================================

def verify_upload(filepath):
    try:
        cmd = ['ffprobe', '-v', 'error', '-print_format', 'json', '-show_streams', '-show_format', filepath]
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0: 
            return False, "FFprobe Error"
        
        try: data = json.loads(result.stdout)
        except json.JSONDecodeError: return False, "Invalid JSON"

        has_video = False
        for stream in data.get('streams', []):
            if stream['codec_type'] == 'video':
                if stream.get('codec_name') != 'av1': return False, "Invalid Codec (Not AV1)"
                if int(stream.get('height', 0)) != 480: return False, "Invalid Height"
                has_video = True
            elif stream['codec_type'] == 'audio':
                if stream.get('codec_name') != 'opus': return False, "Invalid Audio Codec"
                
        if not has_video: return False, "No Video Stream"
        return True, "Verified"
    except Exception as e:
        return False, f"Exception: {str(e)}"

def scan_and_queue():
    print(f"[*] Scanning {SOURCE_DIRECTORY}...")
    if not os.path.exists(SOURCE_DIRECTORY): os.makedirs(SOURCE_DIRECTORY)
    conn = sqlite3.connect(DB_FILE); cursor = conn.cursor()
    
    count_new = 0
    for root, dirs, files in os.walk(SOURCE_DIRECTORY, topdown=True):
        dirs.sort(); files.sort()
        for file in files:
            if file.lower().endswith(VIDEO_EXTENSIONS):
                full_path = os.path.join(root, file)
                rel_path = os.path.relpath(full_path, SOURCE_DIRECTORY)
                fsize = os.path.getsize(full_path)
                
                cursor.execute("SELECT id FROM jobs WHERE id=?", (rel_path,))
                if not cursor.fetchone():
                    cursor.execute("INSERT INTO jobs (id, filename, status, last_updated, file_size) VALUES (?, ?, 'queued', ?, ?)", (rel_path, file, datetime.now(), fsize))
                    count_new += 1
                    
    conn.commit()
    if count_new > 0: log_event("INFO", f"Scanner found {count_new} new files.")

    print("[*] Loading queue...")
    cursor.execute("SELECT id, filename, file_size FROM jobs WHERE status = 'queued'")
    for row in cursor.fetchall():
        if row[0] not in queued_job_ids:
            job_queue.put({
                "id": row[0], 
                "filename": row[1], 
                "file_size": row[2],
                "download_url": f"{SERVER_URL_DISPLAY.rstrip('/')}/download_source/{quote(row[0], safe='/')}"
            })
            queued_job_ids.add(row[0])
    conn.close()

# ==============================================================================
# ROUTES
# ==============================================================================

@app.route('/')
def dashboard(): return render_template('dashboard.html')

@app.route('/web_worker')
def web_worker_client(): return render_template('web_worker.html')

@app.route('/admin')
@limiter.limit("5 per minute") # [FIX] Rate limit login attempts
@requires_auth
def admin_panel(): return render_template('admin.html')

@app.route('/dl/worker')
def download_worker_script(): return send_file(WORKER_TEMPLATE_FILE, as_attachment=True, download_name='worker.py')

@app.route('/install')
def install_script():
    # [FIX] Sanitize inputs
    u = sanitize_input(request.args.get('username')) or 'Anonymous'
    w = sanitize_input(request.args.get('workername')) or 'LinuxNode'
    j = request.args.get('jobs', '1')
    if not j.isdigit(): j = '1'

    # [FIX] Inject Token into install script
    script = f"""#!/bin/bash
echo "[*] Initializing Worker..."
if [ -x "$(command -v apt-get)" ]; then sudo apt-get update -qq && sudo apt-get install -y ffmpeg python3 python3-requests; fi
if [ -x "$(command -v dnf)" ]; then sudo dnf install -y ffmpeg python3 python3-requests; fi

curl -s "{SERVER_URL_DISPLAY.rstrip('/')}/dl/worker" -o worker.py

echo "[*] Starting Worker..."
# Auto-injecting the token from server configuration
export WORKER_SECRET="{WORKER_SECRET}"
python3 worker.py --username "{u}" --workername "{w}" --jobs {j} --manager "{SERVER_URL_DISPLAY}"
"""
    return Response(script, mimetype='text/x-shellscript')

@app.route('/download_source/<path:filename>')
def download_source(filename): 
    return send_from_directory(SOURCE_DIRECTORY, filename, as_attachment=True)

@app.route('/get_job', methods=['GET'])
@requires_worker_auth # [FIX] Auth Required
def get_job():
    max_size_mb = request.args.get('max_size_mb')

    # Filtered Mode
    if max_size_mb and max_size_mb.isdigit():
        limit_bytes = int(max_size_mb) * 1024 * 1024
        try:
            with db_lock:
                conn = sqlite3.connect(DB_FILE); conn.row_factory = sqlite3.Row; c = conn.cursor()
                c.execute("SELECT id, filename, file_size FROM jobs WHERE status='queued' AND file_size <= ? ORDER BY last_updated ASC LIMIT 1", (limit_bytes,))
                row = c.fetchone()
                if row:
                    job = dict(row)
                    job['download_url'] = f"{SERVER_URL_DISPLAY.rstrip('/')}/download_source/{quote(job['id'], safe='/')}"
                    conn.execute("UPDATE jobs SET status='processing', last_updated=?, started_at=? WHERE id=?", (datetime.now(), datetime.now(), job['id']))
                    conn.commit(); conn.close()
                    return jsonify({"status": "ok", "job": job})
                conn.close(); return jsonify({"status": "empty"})
        except Exception as e:
            return jsonify({"status": "error"}), 500

    # FIFO Mode
    try:
        start_time = time.time()
        while True:
            if time.time() - start_time > 5: return jsonify({"status": "empty"})
            try: job = job_queue.get_nowait()
            except queue.Empty: return jsonify({"status": "empty"})

            with db_lock:
                conn = sqlite3.connect(DB_FILE); cursor = conn.cursor()
                cursor.execute("SELECT status FROM jobs WHERE id=?", (job['id'],))
                row = cursor.fetchone()
                if row and row[0] == 'queued':
                    cursor.execute("UPDATE jobs SET status='processing', last_updated=?, started_at=? WHERE id=?", (datetime.now(), datetime.now(), job['id']))
                    conn.commit(); conn.close()
                    queued_job_ids.discard(job['id'])
                    return jsonify({"status": "ok", "job": job})
                else:
                    conn.close(); queued_job_ids.discard(job['id']); continue
    except Exception as e:
        return jsonify({"status": "error"}), 500

@app.route('/upload_result', methods=['POST'])
@requires_worker_auth # [FIX] Auth Required
def upload_result():
    job_id = request.form.get('job_id')
    worker_id = sanitize_input(request.form.get('worker_id')) # [FIX] Sanitize
    duration = request.form.get('duration', 0)

    if 'file' in request.files and job_id:
        base_name, _ = os.path.splitext(job_id)
        new_filename = base_name + ".mp4"
        
        # [FIX] Quarantine Pattern
        # 1. Save to temp folder first
        quarantine_dir = os.path.join("temp_uploads", "quarantine")
        os.makedirs(quarantine_dir, exist_ok=True)
        temp_name = f"{uuid.uuid4().hex}.mp4"
        temp_path = os.path.join(quarantine_dir, temp_name)
        
        request.files['file'].save(temp_path)
        
        # 2. Verify in isolation
        is_valid, reason = verify_upload(temp_path)
        if not is_valid:
            log_event("WARN", f"Security: Upload rejected ({reason})", job_id)
            os.remove(temp_path) # Nuke it
            with db_lock:
                conn = sqlite3.connect(DB_FILE)
                conn.execute("UPDATE jobs SET status='failed', last_updated=? WHERE id=?", (datetime.now(), job_id))
                conn.commit(); conn.close()
            return jsonify({"status": "error", "message": reason}), 400

        # 3. Move to final destination (Safe)
        save_path = os.path.abspath(os.path.join(COMPLETED_DIRECTORY, new_filename))
        completed_abs = os.path.abspath(COMPLETED_DIRECTORY)
        if not save_path.startswith(completed_abs): # Path traversal check
             os.remove(temp_path)
             return jsonify({"status": "error"}), 403

        os.makedirs(os.path.dirname(save_path), exist_ok=True)
        shutil.move(temp_path, save_path) # Atomic move

        with db_lock:
            conn = sqlite3.connect(DB_FILE)
            conn.execute("UPDATE jobs SET status='completed', progress=100, worker_id=?, last_updated=? WHERE id=?", (worker_id, datetime.now(), job_id))
            conn.commit(); conn.close()
            
        log_event("INFO", f"Job completed by {worker_id}", job_id)
        return jsonify({"status": "success"})
    
    return jsonify({"status": "error"}), 400

@app.route('/report_status', methods=['POST'])
@requires_worker_auth # [FIX] Auth Required
def report_status():
    d = request.json
    status = d.get('status')
    worker_id = sanitize_input(d.get('worker_id')) # [FIX] Sanitize
    
    if status == 'completed': return jsonify({"status": "ignored"}), 403
    if status == 'failed': log_event("WARN", f"Worker {worker_id} reported failure", d.get('job_id'))

    with db_lock:
        conn = sqlite3.connect(DB_FILE)
        sql = "UPDATE jobs SET status=?, worker_id=?, progress=?, last_updated=? WHERE id=?"
        params = [status, worker_id, d.get('progress',0), datetime.now(), d.get('job_id')]
        if d.get('duration', 0) > 0: 
            sql = "UPDATE jobs SET status=?, worker_id=?, progress=?, last_updated=?, duration=? WHERE id=?"
            params.insert(4, d.get('duration'))
        conn.execute(sql, tuple(params)); conn.commit(); conn.close()
    return jsonify({"status": "received"})

@app.route('/api/stats')
def api_stats():
    filter_val = request.args.get('filter')
    time_filter = ""
    if filter_val == '24h': time_filter = " AND last_updated > datetime('now', '-1 day')"
    elif filter_val == '30d': time_filter = " AND last_updated > datetime('now', '-30 days')"

    with db_lock:
        conn = sqlite3.connect(DB_FILE); conn.row_factory = sqlite3.Row; c = conn.cursor()
        c.execute(f"SELECT CASE WHEN instr(worker_id, '-') > 0 THEN substr(worker_id, 1, instr(worker_id, '-') - 1) ELSE worker_id END as worker_id, SUM(duration) as total_minutes, COUNT(*) as files_count FROM jobs WHERE status='completed' AND worker_id IS NOT NULL {time_filter} GROUP BY 1 ORDER BY total_minutes DESC")
        sb = [dict(r) for r in c.fetchall()]
        
        c.execute("SELECT COALESCE(worker_id, 'Pending...') as worker_id, filename, duration, progress, status FROM jobs WHERE status IN ('processing', 'downloading', 'uploading')")
        act = [dict(r) for r in c.fetchall()]
        
        c.execute("SELECT id, status, worker_id FROM jobs ORDER BY last_updated DESC LIMIT 20")
        hist = [dict(r) for r in c.fetchall()]
        c.execute("SELECT COUNT(*) FROM jobs"); total_count = c.fetchone()[0]
        conn.close()
    
    return jsonify({"scoreboard": sb, "active": act, "history": hist, "queue_depth": job_queue.qsize(), "total_jobs": total_count})

@app.route('/api/all_jobs')
@requires_auth
def api_all_jobs():
    with db_lock:
        conn = sqlite3.connect(DB_FILE); conn.row_factory = sqlite3.Row; c = conn.cursor()
        c.execute("SELECT id, status FROM jobs ORDER BY last_updated DESC")
        jobs = [dict(r) for r in c.fetchall()]; conn.close()
        return jsonify({"jobs": jobs})

@app.route('/api/logs')
@requires_auth
def get_logs():
    limit = request.args.get('limit', 100)
    with db_lock:
        conn = sqlite3.connect(DB_FILE); conn.row_factory = sqlite3.Row; c = conn.cursor()
        c.execute("SELECT * FROM system_logs ORDER BY timestamp DESC LIMIT ?", (limit,))
        logs = [dict(r) for r in c.fetchall()]; conn.close()
    return jsonify({"logs": logs})

@app.route('/api/admin_action', methods=['POST'])
@requires_auth
def admin_action():
    data = request.json; job_id = data.get('job_id'); action = data.get('action')
    log_event("WARN", f"Admin performed '{action}' on job", job_id)
    
    with db_lock:
        conn = sqlite3.connect(DB_FILE); c = conn.cursor()
        if action == 'delete':
            c.execute("DELETE FROM jobs WHERE id = ?", (job_id,))
        elif action == 'retry':
            c.execute("UPDATE jobs SET status='queued', progress=0, worker_id=NULL, last_updated=? WHERE id=?", (datetime.now(), job_id))
            c.execute("SELECT filename FROM jobs WHERE id=?", (job_id,))
            row = c.fetchone()
            if row and job_id not in queued_job_ids:
                 job_queue.put({"id": job_id, "filename": row[0], "download_url": f"{SERVER_URL_DISPLAY.rstrip('/')}/download_source/{quote(job_id, safe='/')}"})
                 queued_job_ids.add(job_id)
        conn.commit(); conn.close()
    return jsonify({"status": "ok"})

@app.route('/api/rescan_db')
@requires_auth
def api_rescan():
    try:
        scan_and_queue()
        return jsonify({"status": "ok", "message": "Rescan completed successfully."})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.errorhandler(Exception)
def handle_exception(e):
    if isinstance(e,  Response) or isinstance(e, HTTPException): return e
    log_event("CRITICAL", f"Unhandled Exception: {str(e)}\n{traceback.format_exc()}")
    return "Internal Server Error", 500

def maintenance_loop():
    while True:
        try:
            logs_to_write = [] 
            with db_lock:
                conn = sqlite3.connect(DB_FILE); cursor = conn.cursor()
                now = datetime.now()
                
                # Rule 1: 24h timeout
                cursor.execute("SELECT id, filename, started_at FROM jobs WHERE status IN ('processing', 'downloading', 'uploading')")
                for row in cursor.fetchall():
                    jid, fname, started = row
                    if started:
                        try:
                            s_time = datetime.strptime(str(started).split('.')[0], "%Y-%m-%d %H:%M:%S")
                            if (now - s_time).total_seconds() > 86400:
                                logs_to_write.append(("WARN", "Job timed out (24h). Resetting.", jid))
                                cursor.execute("UPDATE jobs SET status='queued', progress=0, worker_id=NULL, last_updated=?, started_at=NULL WHERE id=?", (now, jid))
                                if jid not in queued_job_ids:
                                     job_queue.put({"id": jid, "filename": fname, "download_url": f"{SERVER_URL_DISPLAY.rstrip('/')}/download_source/{quote(jid, safe='/')}"})
                                     queued_job_ids.add(jid)
                        except: pass 
                
                conn.commit(); conn.close()
            
            for level, msg, jid in logs_to_write: log_event(level, msg, jid)
        except Exception as e: print(f"[!] Maintenance error: {e}")
        time.sleep(600)

# ==============================================================================
# APP INITIALIZATION
# ==============================================================================

# [FIX] Run these immediately when the file is loaded (Gunicorn Friendly)
print("[*] Initializing Database and Queue...")
init_db()
scan_and_queue()
threading.Thread(target=maintenance_loop, daemon=True).start()

if __name__ == '__main__':
    print(f"[*] Manager running at {SERVER_URL_DISPLAY}")
    print("[!] WARNING: Running in dev mode. Use 'gunicorn manager:app' for production.")
    app.run(host=SERVER_HOST, port=SERVER_PORT, threaded=True)
