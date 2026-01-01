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

# Flask & Extensions
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

# Security Config
app.config['SESSION_COOKIE_SECURE'] = True    
app.config['SESSION_COOKIE_HTTPONLY'] = True  
app.config['SESSION_COOKIE_SAMESITE'] = 'Lax' 
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024 * 1024 

# [FIX] Rate Limiter - Removed default_limits to prevent blocking dashboard/workers
limiter = Limiter(
    get_remote_address,
    app=app,
    storage_uri="memory://"
)

job_queue = queue.Queue()
queued_job_ids = set()
db_lock = threading.Lock()

# ==============================================================================
# SECURITY HELPERS
# ==============================================================================

def sanitize_input(val):
    if not val: return None
    return re.sub(r'[^a-zA-Z0-9_.-]', '', str(val))

@app.after_request
def add_security_headers(response):
    response.headers['Cross-Origin-Opener-Policy'] = 'same-origin'
    response.headers['Cross-Origin-Embedder-Policy'] = 'require-corp'
    response.headers['X-Frame-Options'] = 'DENY'
    response.headers['X-Content-Type-Options'] = 'nosniff'
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
    if request.method == "POST" and request.path.startswith('/api/admin_action'):
        origin = request.headers.get('Origin')
        referer = request.headers.get('Referer')
        target = origin or referer or ""
        if request.host not in target:
             return jsonify({"status": "error", "message": "CSRF Blocked: Origin Mismatch"}), 403

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
    @wraps(f)
    def decorated(*args, **kwargs):
        token = request.headers.get('X-Worker-Token') or request.args.get('token')
        if token is None: return f(*args, **kwargs) # Legacy support
        if token != WORKER_SECRET:
            return jsonify({"status": "error", "message": "Unauthorized Worker"}), 401
        return f(*args, **kwargs)
    return decorated

# ==============================================================================
# DATABASE & LOGGING
# ==============================================================================

def init_db():
    with db_lock:
        conn = sqlite3.connect(DB_FILE, timeout=30)
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
        try: cursor.execute("ALTER TABLE jobs ADD COLUMN worker_version TEXT")
        except sqlite3.OperationalError: pass
        conn.commit(); conn.close()

def log_event(level, message, related_id=None):
    try:
        clean_msg = str(message).replace('<', '&lt;').replace('>', '&gt;')
        clean_id = sanitize_input(related_id) if related_id else None
        with db_lock:
            conn = sqlite3.connect(DB_FILE, timeout=30)
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
        if result.returncode != 0: return False, "FFprobe Error"
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
    
    # Step 1: Scan filesystem (No Lock)
    found_files = []
    try:
        for root, dirs, files in os.walk(SOURCE_DIRECTORY, topdown=True):
            dirs.sort(); files.sort()
            for file in files:
                if file.lower().endswith(VIDEO_EXTENSIONS):
                    full_path = os.path.join(root, file)
                    rel_path = os.path.relpath(full_path, SOURCE_DIRECTORY)
                    fsize = os.path.getsize(full_path)
                    found_files.append((rel_path, file, fsize))
    except Exception as e:
        print(f"[!] Scanner error: {e}")
        return

    # Step 2: Update Database (With Lock)
    count_new = 0
    with db_lock:
        conn = sqlite3.connect(DB_FILE, timeout=30)
        try:
            cursor = conn.cursor()
            for rel_path, file, fsize in found_files:
                cursor.execute("SELECT id FROM jobs WHERE id=?", (rel_path,))
                if not cursor.fetchone():
                    cursor.execute("INSERT INTO jobs (id, filename, status, last_updated, file_size) VALUES (?, ?, 'queued', ?, ?)", (rel_path, file, datetime.now(), fsize))
                    count_new += 1
            conn.commit()
        finally:
            conn.close()

    # [FIX] Log OUTSIDE the lock to prevent deadlock
    if count_new > 0: log_event("INFO", f"Scanner found {count_new} new files.")

    print("[*] Loading queue...")
    with db_lock:
        conn = sqlite3.connect(DB_FILE, timeout=30)
        try:
            cursor = conn.cursor()
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
        finally:
            conn.close()

def get_series_list():
    try:
        if not os.path.exists(SOURCE_DIRECTORY): return []
        folders = [d for d in os.listdir(SOURCE_DIRECTORY) if os.path.isdir(os.path.join(SOURCE_DIRECTORY, d))]
        folders.sort()
        mapping = {}
        if os.path.exists('series_names.json'):
            try:
                with open('series_names.json', 'r') as f: mapping = json.load(f)
            except: pass

        series_data = []
        for idx, folder in enumerate(folders):
            series_id = idx + 1
            friendly = mapping.get(folder, folder)
            series_data.append({"id": series_id, "folder": folder, "name": friendly})
        return series_data
    except Exception as e:
        print(f"[!] Error getting series list: {e}"); return []

# ==============================================================================
# ROUTES
# ==============================================================================

@app.route('/')
def dashboard(): return render_template('dashboard.html')

@app.route('/admin')
@limiter.limit("5 per minute") # Keep limit for login only
@requires_auth
def admin_panel(): return render_template('admin.html')

@app.route('/dl/worker')
def download_worker_script(): return send_file(WORKER_TEMPLATE_FILE, as_attachment=True, download_name='worker.py')

@app.route('/api/series')
def api_series_list():
    return jsonify({"series": get_series_list()})

@app.route('/install')
def install_script():
    u = sanitize_input(request.args.get('username')) or 'Anonymous'
    w = sanitize_input(request.args.get('workername')) or 'LinuxNode'
    s_id = request.args.get('series_id', '') 
    if s_id and not s_id.isdigit(): s_id = ''
    j = request.args.get('jobs', '1')
    if not j.isdigit(): j = '1'

    script = f"""#!/bin/bash
echo "[*] Initializing Worker..."
if [ -x "$(command -v apt-get)" ]; then sudo apt-get update -qq && sudo apt-get install -y ffmpeg python3 python3-requests; fi
if [ -x "$(command -v dnf)" ]; then sudo dnf install -y ffmpeg python3 python3-requests; fi
curl -s "{SERVER_URL_DISPLAY.rstrip('/')}/dl/worker" -o worker.py
echo "[*] Starting Worker..."
export WORKER_SECRET="{WORKER_SECRET}"
python3 worker.py --username "{u}" --workername "{w}" --jobs {j} --manager "{SERVER_URL_DISPLAY}" --series-id "{s_id}"
"""
    return Response(script, mimetype='text/x-shellscript')

@app.route('/download_source/<path:filename>')
def download_source(filename): 
    return send_from_directory(SOURCE_DIRECTORY, filename, as_attachment=True)

@app.route('/get_job', methods=['GET'])
@requires_worker_auth
def get_job():
    max_size_mb = request.args.get('max_size_mb')
    series_id = request.args.get('series_id')
    worker_id = sanitize_input(request.args.get('worker_id'))
    worker_version = sanitize_input(request.args.get('version'))
    
    search_attempts = []
    if series_id and series_id.isdigit(): search_attempts.append(series_id)
    search_attempts.append(None) 

    try:
        with db_lock:
            conn = sqlite3.connect(DB_FILE, timeout=30); conn.row_factory = sqlite3.Row
            try:
                c = conn.cursor()
                job = None
                for current_search_id in search_attempts:
                    folder_filter = None
                    if current_search_id:
                        series_list = get_series_list()
                        for s in series_list:
                            if s['id'] == int(current_search_id):
                                folder_filter = s['folder']; break
                    
                    params = []
                    query_parts = ["status='queued'"]
                    if max_size_mb and max_size_mb.isdigit():
                        query_parts.append("file_size <= ?")
                        params.append(int(max_size_mb) * 1024 * 1024)
                    if folder_filter:
                        query_parts.append("id LIKE ?")
                        params.append(f"{folder_filter}%")
                    
                    where_clause = " AND ".join(query_parts)
                    sql = f"SELECT id, filename, file_size FROM jobs WHERE {where_clause} ORDER BY id ASC LIMIT 1"
                    c.execute(sql, tuple(params)); row = c.fetchone()
                    if row: job = dict(row); break
                
                if job:
                    job['download_url'] = f"{SERVER_URL_DISPLAY.rstrip('/')}/download_source/{quote(job['id'], safe='/')}"
                    conn.execute(
                        "UPDATE jobs SET status='processing', worker_id=?, worker_version=?, last_updated=?, started_at=? WHERE id=?", 
                        (worker_id, worker_version, datetime.now(), datetime.now(), job['id'])
                    )
                    conn.commit()
                    return jsonify({"status": "ok", "job": job})
            finally:
                conn.close()
            return jsonify({"status": "empty"})
    except Exception as e:
        log_event("ERROR", f"get_job failed: {e}")
        return jsonify({"status": "error"}), 500

@app.route('/upload_result', methods=['POST'])
@requires_worker_auth
def upload_result():
    job_id = request.form.get('job_id')
    worker_id = sanitize_input(request.form.get('worker_id'))
    
    # [FIX] Capture duration
    try:
        duration = int(float(request.form.get('duration', 0)))
    except:
        duration = 0
    
    if 'file' in request.files and job_id:
        base_name, _ = os.path.splitext(job_id)
        new_filename = base_name + ".mp4"
        
        quarantine_dir = os.path.join("temp_uploads", "quarantine")
        os.makedirs(quarantine_dir, exist_ok=True)
        temp_name = f"{uuid.uuid4().hex}.mp4"
        temp_path = os.path.join(quarantine_dir, temp_name)
        request.files['file'].save(temp_path)
        
        is_valid, reason = verify_upload(temp_path)
        if not is_valid:
            log_event("WARN", f"Security: Upload rejected ({reason})", job_id)
            os.remove(temp_path) 
            with db_lock:
                conn = sqlite3.connect(DB_FILE, timeout=30)
                conn.execute("UPDATE jobs SET status='failed', last_updated=? WHERE id=?", (datetime.now(), job_id))
                conn.commit(); conn.close()
            return jsonify({"status": "error", "message": reason}), 400

        save_path = os.path.abspath(os.path.join(COMPLETED_DIRECTORY, new_filename))
        if not save_path.startswith(os.path.abspath(COMPLETED_DIRECTORY)):
             os.remove(temp_path); return jsonify({"status": "error"}), 403

        os.makedirs(os.path.dirname(save_path), exist_ok=True)
        shutil.move(temp_path, save_path) 

        with db_lock:
            conn = sqlite3.connect(DB_FILE, timeout=30)
            # [FIX] Save duration to DB
            conn.execute("UPDATE jobs SET status='completed', progress=100, worker_id=?, last_updated=?, duration=? WHERE id=?", (worker_id, datetime.now(), duration, job_id))
            conn.commit(); conn.close()
            
        log_event("INFO", f"Job completed by {worker_id}", job_id)
        return jsonify({"status": "success"})
    return jsonify({"status": "error"}), 400

@app.route('/report_status', methods=['POST'])
@requires_worker_auth
def report_status():
    d = request.json
    status = d.get('status')
    worker_id = sanitize_input(d.get('worker_id'))
    worker_version = sanitize_input(d.get('version'))
    
    if status == 'completed': return jsonify({"status": "ignored"}), 403
    if status == 'failed': 
        log_event("WARN", f"Worker {worker_id} (v{worker_version}) reported failure", d.get('job_id'))

    with db_lock:
        conn = sqlite3.connect(DB_FILE, timeout=30)
        try:
            sql = "UPDATE jobs SET status=?, worker_id=?, progress=?, last_updated=? WHERE id=?"
            params = [status, worker_id, d.get('progress',0), datetime.now(), d.get('job_id')]
            if d.get('duration', 0) > 0: 
                sql = "UPDATE jobs SET status=?, worker_id=?, progress=?, last_updated=?, duration=? WHERE id=?"
                params.insert(4, d.get('duration'))
            conn.execute(sql, tuple(params)); conn.commit()
        finally: conn.close()
    return jsonify({"status": "received"})

@app.route('/api/stats')
def api_stats():
    filter_val = request.args.get('filter')
    time_filter = ""
    if filter_val == '24h': time_filter = " AND last_updated > datetime('now', '-1 day')"
    elif filter_val == '30d': time_filter = " AND last_updated > datetime('now', '-30 days')"

    with db_lock:
        conn = sqlite3.connect(DB_FILE, timeout=30); conn.row_factory = sqlite3.Row
        try:
            c = conn.cursor()
            c.execute(f"SELECT CASE WHEN instr(worker_id, '-') > 0 THEN substr(worker_id, 1, instr(worker_id, '-') - 1) ELSE worker_id END as worker_id, SUM(duration) as total_minutes, COUNT(*) as files_count FROM jobs WHERE status='completed' AND worker_id IS NOT NULL {time_filter} GROUP BY 1 ORDER BY total_minutes DESC")
            sb = [dict(r) for r in c.fetchall()]
            
            c.execute("SELECT COALESCE(worker_id, 'Pending...') as worker_id, filename, duration, progress, status FROM jobs WHERE status IN ('processing', 'downloading', 'uploading')")
            act = [dict(r) for r in c.fetchall()]
            
            c.execute("SELECT id, status, worker_id FROM jobs ORDER BY last_updated DESC LIMIT 20")
            hist = [dict(r) for r in c.fetchall()]
            
            c.execute("SELECT COUNT(*) FROM jobs WHERE status='queued'")
            queue_depth = c.fetchone()[0]
            c.execute("SELECT COUNT(*) FROM jobs")
            total_count = c.fetchone()[0]
        finally:
            conn.close()
    
    return jsonify({"scoreboard": sb, "active": act, "history": hist, "queue_depth": queue_depth, "total_jobs": total_count})

@app.route('/api/all_jobs')
@requires_auth
def api_all_jobs():
    with db_lock:
        conn = sqlite3.connect(DB_FILE, timeout=30); conn.row_factory = sqlite3.Row
        try:
            c = conn.cursor()
            c.execute("SELECT id, status, worker_id, worker_version, last_updated FROM jobs ORDER BY last_updated DESC")
            jobs = [dict(r) for r in c.fetchall()]
        finally: conn.close()
        return jsonify({"jobs": jobs})

@app.route('/api/logs')
@requires_auth
def get_logs():
    limit = request.args.get('limit', 100)
    with db_lock:
        conn = sqlite3.connect(DB_FILE, timeout=30); conn.row_factory = sqlite3.Row
        try:
            c = conn.cursor()
            c.execute("SELECT * FROM system_logs ORDER BY timestamp DESC LIMIT ?", (limit,))
            logs = [dict(r) for r in c.fetchall()]
        finally: conn.close()
    return jsonify({"logs": logs})

@app.route('/api/admin_action', methods=['POST'])
@requires_auth
def admin_action():
    data = request.json; job_id = data.get('job_id'); action = data.get('action')
    log_event("WARN", f"Admin performed '{action}' on job", job_id)
    
    with db_lock:
        conn = sqlite3.connect(DB_FILE, timeout=30); c = conn.cursor()
        try:
            if action == 'delete':
                c.execute("DELETE FROM jobs WHERE id = ?", (job_id,))
            elif action == 'retry':
                c.execute("UPDATE jobs SET status='queued', progress=0, worker_id=NULL, last_updated=? WHERE id=?", (datetime.now(), job_id))
            elif action == 'retry_all_failed':
                c.execute("UPDATE jobs SET status='queued', progress=0, worker_id=NULL, last_updated=? WHERE status='failed'", (datetime.now(),))
            conn.commit()
        finally: conn.close()
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
                conn = sqlite3.connect(DB_FILE, timeout=30); cursor = conn.cursor()
                try:
                    now = datetime.now()
                    cursor.execute("SELECT id, filename, started_at FROM jobs WHERE status IN ('processing', 'downloading', 'uploading')")
                    for row in cursor.fetchall():
                        jid, fname, started = row
                        if started:
                            try:
                                s_time = datetime.strptime(str(started).split('.')[0], "%Y-%m-%d %H:%M:%S")
                                if (now - s_time).total_seconds() > 86400:
                                    logs_to_write.append(("WARN", "Job timed out (24h). Resetting.", jid))
                                    cursor.execute("UPDATE jobs SET status='queued', progress=0, worker_id=NULL, last_updated=?, started_at=NULL WHERE id=?", (now, jid))
                            except: pass 
                    conn.commit()
                finally: conn.close()
            
            for level, msg, jid in logs_to_write: log_event(level, msg, jid)
        except Exception as e: print(f"[!] Maintenance error: {e}")
        time.sleep(600)

print("[*] Initializing Database...")
init_db()
scan_and_queue()
threading.Thread(target=maintenance_loop, daemon=True).start()
print(f"[*] Manager initialized and ready. (Service URL: {SERVER_URL_DISPLAY})")

if __name__ == '__main__':
    print(f"[*] Manager running at {SERVER_URL_DISPLAY}")
    print("[!] WARNING: Running in dev mode. Use 'gunicorn manager:app' for production.")
    app.run(host=SERVER_HOST, port=SERVER_PORT, threaded=True)
