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
from datetime import datetime, timedelta
from urllib.parse import quote, urljoin

# [ADDED] Requests for remote scanning
try:
    import requests
except ImportError:
    print("[!] Error: 'requests' module not found. Please run: pip install requests")
    exit(1)

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
    
    try: from config import WORKER_SECRET
    except ImportError:
        print("[!] WARNING: WORKER_SECRET not found. Using unsafe default.")
        WORKER_SECRET = "DefaultInsecureSecret"

    try: from config import SECRET_KEY
    except ImportError: SECRET_KEY = secrets.token_hex(32)

    try: from config import USE_WAL_MODE
    except ImportError: USE_WAL_MODE = True
    
    try: from config import REMOTE_SOURCE_URL
    except ImportError: REMOTE_SOURCE_URL = None

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

limiter = Limiter(get_remote_address, app=app, storage_uri="memory://")

job_queue = queue.Queue()
queued_job_ids = set()
db_lock = threading.Lock()

# ==============================================================================
# DATABASE & LOGGING
# ==============================================================================

def init_db():
    with db_lock:
        conn = sqlite3.connect(DB_FILE, timeout=60)
        if USE_WAL_MODE:
            try: conn.execute("PRAGMA journal_mode=WAL;")
            except: pass
        else:
            try: conn.execute("PRAGMA journal_mode=DELETE;")
            except: pass
        
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
        clean_id = str(related_id) if related_id else None
        if clean_id: clean_id = re.sub(r'[^a-zA-Z0-9_.-]', '', clean_id)
        
        with db_lock:
            conn = sqlite3.connect(DB_FILE, timeout=60)
            conn.execute("INSERT INTO system_logs (timestamp, level, message, related_id) VALUES (?, ?, ?, ?)",
                         (datetime.now(), level, clean_msg, clean_id))
            conn.commit(); conn.close()
        print(f"[{level}] {message}") 
    except Exception as e:
        print(f"[!] Logging failed: {e}")

# ==============================================================================
# HELPERS
# ==============================================================================

def sanitize_input(val):
    if not val: return None
    return re.sub(r'[^a-zA-Z0-9_.-]', '', str(val))

def is_version_sufficient(client_ver, min_ver):
    """Compares two version strings (e.g., '1.9.0' vs '1.8.1'). Returns True if client >= min."""
    if not client_ver: return False
    try:
        # Simple comparison for consistent format "X.Y.Z"
        # For more complex versioning, use pkg_resources.parse_version
        c_parts = [int(x) for x in client_ver.split('.') if x.isdigit()]
        m_parts = [int(x) for x in min_ver.split('.') if x.isdigit()]
        return c_parts >= m_parts
    except:
        return False

@app.after_request
def add_security_headers(response):
    response.headers['Cross-Origin-Opener-Policy'] = 'same-origin'
    response.headers['Cross-Origin-Embedder-Policy'] = 'require-corp'
    response.headers['X-Frame-Options'] = 'DENY'
    response.headers['X-Content-Type-Options'] = 'nosniff'
    return response

@app.before_request
def csrf_protect():
    if request.method == "POST" and request.path.startswith('/api/admin_action'):
        origin = request.headers.get('Origin')
        referer = request.headers.get('Referer')
        target = origin or referer or ""
        if request.host not in target:
             return jsonify({"status": "error", "message": "CSRF Blocked"}), 403

def check_auth(u, p): return u == ADMIN_USER and p == ADMIN_PASS
def authenticate(): return Response('Login Required', 401, {'WWW-Authenticate': 'Basic realm="Login Required"'})

def requires_auth(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        auth = request.authorization
        if not auth or not check_auth(auth.username, auth.password): return authenticate()
        return f(*args, **kwargs)
    return decorated

def requires_worker_auth(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        token = request.headers.get('X-Worker-Token') or request.args.get('token')
        if token is None: return f(*args, **kwargs)
        if token != WORKER_SECRET: return jsonify({"status": "error"}), 401
        return f(*args, **kwargs)
    return decorated

def verify_upload(filepath):
    try:
        cmd = ['ffprobe', '-v', 'error', '-print_format', 'json', '-show_streams', '-show_format', filepath]
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0: return False, "FFprobe Error"
        data = json.loads(result.stdout)
        has_video = any(s['codec_type'] == 'video' and s.get('codec_name') == 'av1' and int(s.get('height', 0)) == 480 for s in data.get('streams', []))
        return (True, "Verified") if has_video else (False, "Invalid Video")
    except Exception as e: return False, str(e)

# ==============================================================================
# SCANNER LOGIC (LOCAL & REMOTE)
# ==============================================================================

def scan_remote_http(url, prefix=""):
    """Recursively scans an HTTP directory listing for video files."""
    found = []
    try:
        headers = {'User-Agent': 'FractumManager/1.0'}
        r = requests.get(url, headers=headers, timeout=10)
        if r.status_code != 200: return []
        
        links = re.findall(r'href=["\']([^"\'<>]+)["\']', r.text)
        
        for link in links:
            if link.startswith('?') or link.startswith('/') or link in ['../', './']: continue
            if "parent directory" in link.lower(): continue
            
            full_url = urljoin(url, link)
            
            if link.endswith('/'):
                new_prefix = f"{prefix}{link}"
                found.extend(scan_remote_http(full_url, prefix=new_prefix))
            elif any(link.lower().endswith(ext) for ext in VIDEO_EXTENSIONS):
                from urllib.parse import unquote
                clean_name = unquote(link)
                clean_id = f"{prefix}{clean_name}"
                
                size = 0
                try:
                    h = requests.head(full_url, headers=headers, timeout=5)
                    size = int(h.headers.get('content-length', 0))
                except: pass
                
                found.append((clean_id, clean_name, size))

    except Exception as e:
        print(f"[!] HTTP Scan Error on {url}: {e}")
        
    return found

def scan_and_queue():
    found_files = []
    
    if REMOTE_SOURCE_URL:
        print(f"[*] Scanning REMOTE Source: {REMOTE_SOURCE_URL} ...")
        found_files = scan_remote_http(REMOTE_SOURCE_URL)
    else:
        print(f"[*] Scanning LOCAL Source: {SOURCE_DIRECTORY} ...")
        if not os.path.exists(SOURCE_DIRECTORY): os.makedirs(SOURCE_DIRECTORY)
        try:
            for root, dirs, files in os.walk(SOURCE_DIRECTORY, topdown=True):
                dirs.sort(); files.sort()
                for file in files:
                    if file.lower().endswith(VIDEO_EXTENSIONS):
                        rel_path = os.path.relpath(os.path.join(root, file), SOURCE_DIRECTORY)
                        fsize = os.path.getsize(os.path.join(root, file))
                        found_files.append((rel_path, file, fsize))
        except Exception as e: print(f"[!] Scanner error: {e}"); return

    count_new = 0
    with db_lock:
        conn = sqlite3.connect(DB_FILE, timeout=60)
        try:
            cursor = conn.cursor()
            for rel_path, file, fsize in found_files:
                cursor.execute("SELECT id FROM jobs WHERE id=?", (rel_path,))
                if not cursor.fetchone():
                    cursor.execute("INSERT INTO jobs (id, filename, status, last_updated, file_size) VALUES (?, ?, 'queued', ?, ?)", (rel_path, file, datetime.now(), fsize))
                    count_new += 1
            conn.commit()
        finally: conn.close()

    if count_new > 0: log_event("INFO", f"Scanner found {count_new} new files.")

    print("[*] Loading queue...")
    with db_lock:
        conn = sqlite3.connect(DB_FILE, timeout=60)
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT id, filename, file_size FROM jobs WHERE status = 'queued'")
            for row in cursor.fetchall():
                if row[0] not in queued_job_ids:
                    if REMOTE_SOURCE_URL:
                        dl_link = urljoin(REMOTE_SOURCE_URL, quote(row[0]))
                    else:
                        dl_link = f"{SERVER_URL_DISPLAY.rstrip('/')}/download_source/{quote(row[0], safe='/')}"

                    job_queue.put({
                        "id": row[0], "filename": row[1], "file_size": row[2],
                        "download_url": dl_link
                    })
                    queued_job_ids.add(row[0])
        finally: conn.close()

def get_series_list():
    if REMOTE_SOURCE_URL: return []
    try:
        if not os.path.exists(SOURCE_DIRECTORY): return []
        folders = sorted([d for d in os.listdir(SOURCE_DIRECTORY) if os.path.isdir(os.path.join(SOURCE_DIRECTORY, d))])
        mapping = {}
        if os.path.exists('series_names.json'):
            try: mapping = json.load(open('series_names.json', 'r'))
            except: pass
        return [{"id": i+1, "folder": f, "name": mapping.get(f, f)} for i, f in enumerate(folders)]
    except: return []

# ==============================================================================
# ROUTES
# ==============================================================================

@app.route('/')
def dashboard(): return render_template('dashboard.html')

@app.route('/admin')
@limiter.limit("5 per minute") 
@requires_auth
def admin_panel(): return render_template('admin.html')

@app.route('/dl/worker')
def download_worker_script(): return send_file(WORKER_TEMPLATE_FILE, as_attachment=True, download_name='worker.py')

@app.route('/api/series')
def api_series_list(): return jsonify({"series": get_series_list()})

@app.route('/install')
def install_script():
    u = sanitize_input(request.args.get('username')) or 'Anonymous'
    w = sanitize_input(request.args.get('workername')) or 'LinuxNode'
    s_id = request.args.get('series_id', '') 
    if s_id and not s_id.isdigit(): s_id = ''
    j = request.args.get('jobs', '1')
    if not j.isdigit(): j = '1'
    script = f"""#!/bin/bash
if [ -x "$(command -v apt-get)" ]; then sudo apt-get update -qq && sudo apt-get install -y ffmpeg python3 python3-requests; fi
if [ -x "$(command -v dnf)" ]; then sudo dnf install -y ffmpeg python3 python3-requests; fi
curl -s "{SERVER_URL_DISPLAY.rstrip('/')}/dl/worker" -o worker.py
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
    
    # [NEW] Enforce version check for Remote HTTP jobs
    if REMOTE_SOURCE_URL and not is_version_sufficient(worker_version, "1.9.0"):
        return jsonify({"status": "empty"})

    search_attempts = [series_id] if series_id and series_id.isdigit() else []
    search_attempts.append(None) 

    try:
        with db_lock:
            conn = sqlite3.connect(DB_FILE, timeout=60); conn.row_factory = sqlite3.Row
            try:
                c = conn.cursor()
                job = None
                for current_search_id in search_attempts:
                    folder_filter = None
                    if current_search_id:
                        for s in get_series_list():
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
                    
                    sql = f"SELECT id, filename, file_size FROM jobs WHERE {' AND '.join(query_parts)} ORDER BY id ASC LIMIT 1"
                    c.execute(sql, tuple(params)); row = c.fetchone()
                    if row: job = dict(row); break
                
                if job:
                    if REMOTE_SOURCE_URL:
                         job['download_url'] = urljoin(REMOTE_SOURCE_URL, quote(job['id']))
                    else:
                         job['download_url'] = f"{SERVER_URL_DISPLAY.rstrip('/')}/download_source/{quote(job['id'], safe='/')}"

                    conn.execute("UPDATE jobs SET status='processing', worker_id=?, worker_version=?, last_updated=?, started_at=? WHERE id=?", 
                        (worker_id, worker_version, datetime.now(), datetime.now(), job['id']))
                    conn.commit()
                    return jsonify({"status": "ok", "job": job})
            finally: conn.close()
            return jsonify({"status": "empty"})
    except Exception as e:
        log_event("ERROR", f"get_job failed: {e}")
        return jsonify({"status": "error"}), 500

@app.route('/upload_result', methods=['POST'])
@requires_worker_auth
def upload_result():
    job_id = request.form.get('job_id')
    worker_id = sanitize_input(request.form.get('worker_id'))
    try: duration = int(float(request.form.get('duration', 0)))
    except: duration = 0
    
    if 'file' in request.files and job_id:
        new_filename = os.path.splitext(job_id)[0] + ".mp4"
        quarantine_dir = os.path.join("temp_uploads", "quarantine")
        os.makedirs(quarantine_dir, exist_ok=True)
        temp_path = os.path.join(quarantine_dir, f"{uuid.uuid4().hex}.mp4")
        request.files['file'].save(temp_path)
        
        is_valid, reason = verify_upload(temp_path)
        if not is_valid:
            log_event("WARN", f"Security: Upload rejected ({reason})", job_id)
            os.remove(temp_path)
            with db_lock:
                conn = sqlite3.connect(DB_FILE, timeout=60)
                conn.execute("UPDATE jobs SET status='failed', last_updated=? WHERE id=?", (datetime.now(), job_id))
                conn.commit(); conn.close()
            return jsonify({"status": "error", "message": reason}), 400

        save_path = os.path.abspath(os.path.join(COMPLETED_DIRECTORY, new_filename))
        if not save_path.startswith(os.path.abspath(COMPLETED_DIRECTORY)):
             os.remove(temp_path); return jsonify({"status": "error"}), 403

        os.makedirs(os.path.dirname(save_path), exist_ok=True)
        shutil.move(temp_path, save_path) 

        with db_lock:
            conn = sqlite3.connect(DB_FILE, timeout=60)
            conn.execute("UPDATE jobs SET status='completed', progress=100, worker_id=?, last_updated=?, duration=? WHERE id=?", 
                (worker_id, datetime.now(), duration, job_id))
            conn.commit(); conn.close()
        log_event("INFO", f"Job completed by {worker_id}", job_id)
        return jsonify({"status": "success"})
    return jsonify({"status": "error"}), 400

@app.route('/report_status', methods=['POST'])
@requires_worker_auth
def report_status():
    d = request.json; status = d.get('status')
    worker_id = sanitize_input(d.get('worker_id'))
    worker_version = sanitize_input(d.get('version'))
    
    if status == 'completed': return jsonify({"status": "ignored"}), 403
    if status == 'failed': log_event("WARN", f"Worker {worker_id} (v{worker_version}) reported failure", d.get('job_id'))

    with db_lock:
        conn = sqlite3.connect(DB_FILE, timeout=60)
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
    time_filter = " AND last_updated > datetime('now', '-1 day')" if filter_val == '24h' else " AND last_updated > datetime('now', '-30 days')" if filter_val == '30d' else ""

    with db_lock:
        conn = sqlite3.connect(DB_FILE, timeout=60); conn.row_factory = sqlite3.Row
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
        finally: conn.close()
    return jsonify({"scoreboard": sb, "active": act, "history": hist, "queue_depth": queue_depth, "total_jobs": total_count})

@app.route('/api/all_jobs')
@requires_auth
def api_all_jobs():
    with db_lock:
        conn = sqlite3.connect(DB_FILE, timeout=60); conn.row_factory = sqlite3.Row
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
        conn = sqlite3.connect(DB_FILE, timeout=60); conn.row_factory = sqlite3.Row
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
        conn = sqlite3.connect(DB_FILE, timeout=60); c = conn.cursor()
        try:
            if action == 'delete': c.execute("DELETE FROM jobs WHERE id = ?", (job_id,))
            elif action == 'retry': c.execute("UPDATE jobs SET status='queued', progress=0, worker_id=NULL, last_updated=? WHERE id=?", (datetime.now(), job_id))
            elif action == 'retry_all_failed': c.execute("UPDATE jobs SET status='queued', progress=0, worker_id=NULL, last_updated=? WHERE status='failed'", (datetime.now(),))
            elif action == 'clear_stale':
                cutoff = datetime.now() - timedelta(minutes=10)
                c.execute("UPDATE jobs SET status='queued', progress=0, worker_id=NULL, last_updated=?, started_at=NULL WHERE status IN ('processing', 'downloading', 'uploading') AND last_updated < ?", (datetime.now(), cutoff))
            conn.commit()
        finally: conn.close()
    return jsonify({"status": "ok"})

@app.route('/api/rescan_db')
@requires_auth
def api_rescan():
    try: scan_and_queue(); return jsonify({"status": "ok", "message": "Rescan completed."})
    except Exception as e: return jsonify({"status": "error", "message": str(e)}), 500

@app.errorhandler(Exception)
def handle_exception(e):
    if isinstance(e, (Response, HTTPException)): return e
    log_event("CRITICAL", f"Unhandled Exception: {str(e)}\n{traceback.format_exc()}")
    return "Internal Server Error", 500

def maintenance_loop():
    while True:
        try:
            logs_to_write = [] 
            with db_lock:
                conn = sqlite3.connect(DB_FILE, timeout=60); cursor = conn.cursor()
                try:
                    now = datetime.now()
                    cursor.execute("SELECT id, filename, last_updated, worker_id FROM jobs WHERE status IN ('processing', 'downloading', 'uploading')")
                    for row in cursor.fetchall():
                        jid, fname, last_up, worker_id = row
                        if last_up:
                            try:
                                l_time = datetime.strptime(str(last_up).split('.')[0], "%Y-%m-%d %H:%M:%S")
                                if (now - l_time).total_seconds() > 7200: # 2 Hours
                                    logs_to_write.append(("WARN", f"Worker {worker_id} timed out. Resetting.", jid))
                                    cursor.execute("UPDATE jobs SET status='queued', progress=0, worker_id=NULL, last_updated=?, started_at=NULL WHERE id=?", (now, jid))
                            except: pass 
                    conn.commit()
                finally: conn.close()
            for level, msg, jid in logs_to_write: log_event(level, msg, jid)
        except Exception as e: print(f"[!] Maintenance error: {e}")
        time.sleep(60)

print("[*] Initializing Database...")
init_db()
scan_and_queue()
threading.Thread(target=maintenance_loop, daemon=True).start()
print(f"[*] Manager initialized and ready. (Service URL: {SERVER_URL_DISPLAY})")

if __name__ == '__main__':
    print(f"[*] Manager running at {SERVER_URL_DISPLAY}")
    print("[!] WARNING: Running in dev mode. Use 'gunicorn manager:app' for production.")
    app.run(host=SERVER_HOST, port=SERVER_PORT, threaded=True)
