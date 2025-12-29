import os
import time
import queue
import threading
import sqlite3
import subprocess
import json
import re
from functools import wraps
from datetime import datetime
from urllib.parse import quote
from flask import Flask, request, jsonify, render_template, send_from_directory, send_file, Response
from werkzeug.exceptions import HTTPException
import shutil
import traceback

# ==============================================================================
# CONFIGURATION
# ==============================================================================

if not os.path.exists('config.py'):
    if os.path.exists('config.py.example'):
        shutil.copy('config.py.example', 'config.py')
        print("[*] Created 'config.py' from default example.")
    else:
        print("[!] Error: 'config.py' not found and 'config.py.example' missing.")
        exit(1)

try:
    from config import (
        SERVER_HOST,
        SERVER_PORT,
        SERVER_URL_DISPLAY,
        ADMIN_USER,
        ADMIN_PASS,
        SOURCE_DIRECTORY,
        COMPLETED_DIRECTORY,
        WORKER_TEMPLATE_FILE,
        DB_FILE,
        VIDEO_EXTENSIONS
    )
except ImportError:
    print("Error: Could not import settings from 'config.py'.")
    exit(1)

app = Flask(__name__)
job_queue = queue.Queue()
queued_job_ids = set() # Track IDs in queue to prevent duplicates
db_lock = threading.Lock()
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024 * 1024 

# --- WEB WORKER CONFIGURATION (CRITICAL) ---
# These headers are strictly required for Raw Emscripten WASM 
# to use SharedArrayBuffer without security errors.

@app.after_request
def add_security_headers(response):
    response.headers['Cross-Origin-Opener-Policy'] = 'same-origin'
    response.headers['Cross-Origin-Embedder-Policy'] = 'require-corp'
    return response

@app.route('/web_worker')
def web_worker_client():
    """Serves the in-browser worker page."""
    return render_template('web_worker.html')

# --- SECURITY HELPERS ---
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
        # Create logs table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS system_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TIMESTAMP,
                level TEXT,
                message TEXT,
                related_id TEXT
            )
        ''')
        
        # Migration: Add file_size to existing tables
        try:
            cursor.execute("ALTER TABLE jobs ADD COLUMN file_size INTEGER DEFAULT 0")
        except sqlite3.OperationalError:
            pass # Column likely exists
            
        conn.commit()
        conn.close()

def log_event(level, message, related_id=None):
    """Logs an event to the database and prints to console."""
    try:
        with db_lock:
            conn = sqlite3.connect(DB_FILE)
            conn.execute("INSERT INTO system_logs (timestamp, level, message, related_id) VALUES (?, ?, ?, ?)",
                         (datetime.now(), level, str(message), related_id))
            conn.commit()
            conn.close()
        print(f"[{level}] {message}") 
    except Exception as e:
        print(f"[!] Logging failed: {e}")

# [IMPROVED] Better error capturing
def verify_upload(filepath):
    """Verifies that the uploaded file matches the strict encoding rules."""
    try:
        cmd = [
            'ffprobe', '-v', 'error', '-print_format', 'json', 
            '-show_streams', '-show_format', filepath
        ]
        # Capture stderr to see why it failed
        result = subprocess.run(cmd, capture_output=True, text=True)
        
        if result.returncode != 0: 
            # Return the actual error from FFprobe (last 200 chars)
            error_msg = result.stderr.strip()[-200:] if result.stderr else "Unknown error (Exit Code != 0)"
            return False, f"FFprobe Error: {error_msg}"
        
        try:
            data = json.loads(result.stdout)
        except json.JSONDecodeError:
            return False, "FFprobe returned invalid JSON"

        has_video = False
        
        for stream in data.get('streams', []):
            if stream['codec_type'] == 'video':
                # Accept 'av1'
                if stream.get('codec_name') != 'av1': 
                    return False, f"Invalid Video Codec: {stream.get('codec_name')}"
                if int(stream.get('height', 0)) != 480:
                    return False, f"Invalid Height: {stream.get('height')}"
                has_video = True
            elif stream['codec_type'] == 'audio':
                if stream.get('codec_name') != 'opus':
                    return False, f"Invalid Audio Codec: {stream.get('codec_name')}"
                if int(stream.get('channels', 0)) != 1:
                    return False, f"Invalid Audio Channels: {stream.get('channels')}"
        
        if not has_video: return False, "No Video Stream Found"
        return True, "Verified"
    except FileNotFoundError:
        return False, "FFprobe not installed on server"
    except Exception as e:
        return False, f"Verification Exception: {str(e)}"

def scan_and_queue():
    print(f"[*] Scanning {SOURCE_DIRECTORY}...")
    if not os.path.exists(SOURCE_DIRECTORY): os.makedirs(SOURCE_DIRECTORY)
    conn = sqlite3.connect(DB_FILE); cursor = conn.cursor()
    
    # 1. Add new files
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
                else:
                    # Update size for existing records if 0 (migration fix)
                    cursor.execute("UPDATE jobs SET file_size=? WHERE id=? AND file_size=0", (fsize, rel_path))
                    
    conn.commit()
    if count_new > 0:
        log_event("INFO", f"Scanner found {count_new} new files.")

    # 2. Load queue
    print("[*] Loading queue from database...")
    cursor.execute("SELECT id, filename, file_size FROM jobs WHERE status = 'queued'")
    loaded_count = 0
    for row in cursor.fetchall():
        if row[0] not in queued_job_ids:
            job_queue.put({
                "id": row[0], 
                "filename": row[1], 
                "file_size": row[2],
                "download_url": f"{SERVER_URL_DISPLAY.rstrip('/')}/download_source/{quote(row[0], safe='/')}"
            })
            queued_job_ids.add(row[0])
            loaded_count += 1
    conn.close()
    print(f"[*] Queue ready. {job_queue.qsize()} jobs waiting.")

# --- ROUTES ---

@app.route('/')
def dashboard(): return render_template('dashboard.html')

@app.route('/admin')
@requires_auth
def admin_panel(): return render_template('admin.html')

@app.route('/dl/worker')
def download_worker_script(): return send_file(WORKER_TEMPLATE_FILE, as_attachment=True, download_name='worker.py')

@app.route('/install')
def install_script():
    # Sanitize inputs to prevent shell injection
    def sanitize(val, default):
        return val if val and re.match(r'^[a-zA-Z0-9_-]+$', val) else default

    u = sanitize(request.args.get('username'), 'Anonymous')
    w = sanitize(request.args.get('workername'), 'LinuxNode')
    
    j = request.args.get('jobs', '1')
    if not j.isdigit(): j = '1'

    script = f"""#!/bin/bash
echo "[*] Initializing Worker for {SERVER_URL_DISPLAY}..."

# 1. Install System Dependencies (ffmpeg & python3-requests)
if [ -x "$(command -v apt-get)" ]; then 
    sudo apt-get update -qq > /dev/null
    sudo apt-get install -y ffmpeg python3 python3-requests > /dev/null
elif [ -x "$(command -v dnf)" ]; then 
    sudo dnf install -y ffmpeg python3 python3-requests
fi

# 2. Download Worker
curl -s "{SERVER_URL_DISPLAY.rstrip('/')}/dl/worker" -o worker.py

# 3. Run Worker
echo "[*] Starting Worker..."
python3 worker.py --username "{u}" --workername "{w}" --jobs {j} --manager "{SERVER_URL_DISPLAY}"
"""
    return Response(script, mimetype='text/x-shellscript')

@app.route('/download_source/<path:filename>')
def download_source(filename): return send_from_directory(SOURCE_DIRECTORY, filename, as_attachment=True)

@app.route('/get_job', methods=['GET'])
def get_job():
    max_size_mb = request.args.get('max_size_mb')

    # --- FILTERED MODE (e.g., Web Workers) ---
    if max_size_mb and max_size_mb.isdigit():
        limit_bytes = int(max_size_mb) * 1024 * 1024
        try:
            with db_lock:
                conn = sqlite3.connect(DB_FILE)
                conn.row_factory = sqlite3.Row
                c = conn.cursor()
                # Find oldest queued job fitting the size limit
                c.execute("SELECT id, filename, file_size FROM jobs WHERE status='queued' AND file_size <= ? ORDER BY last_updated ASC LIMIT 1", (limit_bytes,))
                row = c.fetchone()
                
                if row:
                    job = dict(row)
                    job['download_url'] = f"{SERVER_URL_DISPLAY.rstrip('/')}/download_source/{quote(job['id'], safe='/')}"
                    
                    # Mark as processing immediately
                    conn.execute("UPDATE jobs SET status='processing', last_updated=?, started_at=? WHERE id=?", (datetime.now(), datetime.now(), job['id']))
                    conn.commit()
                    conn.close()
                    
                    # Note: We don't remove from job_queue here. 
                    # The standard workers will pop it later, check DB, see it's processing, and skip it.
                    return jsonify({"status": "ok", "job": job})
                
                conn.close()
                return jsonify({"status": "empty"}) # No jobs small enough
        except Exception as e:
            log_event("ERROR", f"Filtered get_job failed: {e}")
            return jsonify({"status": "error"}), 500

    # --- STANDARD MODE (FIFO Queue) ---
    try:
        start_time = time.time()
        while True:
            # Prevent infinite loops if DB is out of sync
            if time.time() - start_time > 5: return jsonify({"status": "empty"})

            try:
                job = job_queue.get_nowait()
            except queue.Empty:
                return jsonify({"status": "empty"})

            with db_lock:
                conn = sqlite3.connect(DB_FILE)
                cursor = conn.cursor()
                cursor.execute("SELECT status FROM jobs WHERE id=?", (job['id'],))
                row = cursor.fetchone()
                
                if row and row[0] == 'queued':
                    # Valid job, take it
                    cursor.execute("UPDATE jobs SET status='processing', last_updated=?, started_at=? WHERE id=?", (datetime.now(), datetime.now(), job['id']))
                    conn.commit()
                    conn.close()
                    queued_job_ids.discard(job['id'])
                    return jsonify({"status": "ok", "job": job})
                else:
                    # Job already taken (by filtered request) or cancelled
                    conn.close()
                    queued_job_ids.discard(job['id'])
                    continue # Try next one

    except Exception as e:
        log_event("ERROR", f"Standard get_job failed: {e}")
        return jsonify({"status": "error"}), 500

@app.route('/upload_result', methods=['POST'])
def upload_result():
    job_id = request.form.get('job_id')
    worker_id = request.form.get('worker_id')
    duration = request.form.get('duration', 0)

    if 'file' in request.files and job_id:
        base_name, _ = os.path.splitext(job_id)
        new_filename = base_name + ".mp4"

        # Security check: Prevent path traversal
        save_path = os.path.abspath(os.path.join(COMPLETED_DIRECTORY, new_filename))
        completed_abs = os.path.abspath(COMPLETED_DIRECTORY)
        if not save_path.startswith(completed_abs):
             msg = "Security Alert: Path traversal attempt detected."
             log_event("CRITICAL", msg, job_id)
             with db_lock:
                 conn = sqlite3.connect(DB_FILE)
                 conn.execute("UPDATE jobs SET status='failed', last_updated=? WHERE id=?", (datetime.now(), job_id))
                 conn.commit(); conn.close()
             return jsonify({"status": "error", "message": "Invalid path"}), 403

        os.makedirs(os.path.dirname(save_path), exist_ok=True)
        request.files['file'].save(save_path)
        
        # --- VERIFICATION ---
        is_valid, reason = verify_upload(save_path)
        if not is_valid:
            log_event("ERROR", f"Verification Failed: {reason}", job_id)
            try: os.remove(save_path) 
            except: pass
            
            # Mark as failed in DB
            with db_lock:
                conn = sqlite3.connect(DB_FILE)
                conn.execute("UPDATE jobs SET status='failed', last_updated=? WHERE id=?", (datetime.now(), job_id))
                conn.commit(); conn.close()
            return jsonify({"status": "error", "message": f"Verification failed: {reason}"}), 400

        with db_lock:
            conn = sqlite3.connect(DB_FILE)
            if worker_id:
                conn.execute("UPDATE jobs SET status='completed', progress=100, worker_id=?, last_updated=? WHERE id=?", (worker_id, datetime.now(), job_id))
            else:
                conn.execute("UPDATE jobs SET status='completed', progress=100, last_updated=? WHERE id=?", (datetime.now(), job_id))
            conn.commit(); conn.close()
            
        log_event("INFO", f"Job completed successfully by {worker_id}", job_id)
        return jsonify({"status": "success"})
    
    log_event("WARN", "Invalid upload request received (missing file or ID)")
    return jsonify({"status": "error"}), 400

@app.route('/report_status', methods=['POST'])
def report_status():
    d = request.json
    status = d.get('status')
    
    # Security: Prevent workers from marking jobs as completed directly
    if status == 'completed':
        return jsonify({"status": "ignored", "message": "Cannot set completion status manually"}), 403
    
    # [NEW] Log failures reported by workers
    if status == 'failed':
        log_event("ERROR", f"Worker {d.get('worker_id')} reported failure on job", d.get('job_id'))

    with db_lock:
        conn = sqlite3.connect(DB_FILE)
        sql = "UPDATE jobs SET status=?, worker_id=?, progress=?, last_updated=? WHERE id=?"; params = [status, d.get('worker_id'), d.get('progress',0), datetime.now(), d.get('job_id')]
        if d.get('duration', 0) > 0: sql = "UPDATE jobs SET status=?, worker_id=?, progress=?, last_updated=?, duration=? WHERE id=?"; params.insert(4, d.get('duration'))
        conn.execute(sql, tuple(params)); conn.commit(); conn.close()
    return jsonify({"status": "received"})

@app.route('/api/stats')
def api_stats():
    filter_val = request.args.get('filter')
    time_filter = ""
    if filter_val == '24h':
        time_filter = " AND last_updated > datetime('now', '-1 day')"
    elif filter_val == '30d':
        time_filter = " AND last_updated > datetime('now', '-30 days')"

    with db_lock:
        conn = sqlite3.connect(DB_FILE); conn.row_factory = sqlite3.Row; c = conn.cursor()
        c.execute(f"SELECT CASE WHEN instr(worker_id, '-') > 0 THEN substr(worker_id, 1, instr(worker_id, '-') - 1) ELSE worker_id END as worker_id, SUM(duration) as total_minutes, COUNT(*) as files_count FROM jobs WHERE status='completed' AND worker_id IS NOT NULL {time_filter} GROUP BY 1 ORDER BY total_minutes DESC")
        sb = [dict(r) for r in c.fetchall()]
        
        # [FIX] Added COALESCE to show 'Pending...' instead of null
        c.execute("SELECT COALESCE(worker_id, 'Pending...') as worker_id, filename, duration, progress, status FROM jobs WHERE status IN ('processing', 'downloading', 'uploading')")
        act = [dict(r) for r in c.fetchall()]
        
        c.execute("SELECT id, status, worker_id FROM jobs ORDER BY last_updated DESC LIMIT 20")
        hist = [dict(r) for r in c.fetchall()]
        # New Metrics
        c.execute("SELECT COUNT(*) FROM jobs")
        total_count = c.fetchone()[0]
        conn.close()
    
    return jsonify({
        "scoreboard": sb, 
        "active": act, 
        "history": hist,
        "queue_depth": job_queue.qsize(),
        "total_jobs": total_count
    })

# --- ADMIN API ---
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
        conn = sqlite3.connect(DB_FILE)
        conn.row_factory = sqlite3.Row
        c = conn.cursor()
        c.execute("SELECT * FROM system_logs ORDER BY timestamp DESC LIMIT ?", (limit,))
        logs = [dict(r) for r in c.fetchall()]
        conn.close()
    return jsonify({"logs": logs})

@app.route('/api/admin_action', methods=['POST'])
@requires_auth
def admin_action():
    data = request.json; job_id = data.get('job_id'); action = data.get('action')
    # [NEW] Log admin actions
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
    """Manually triggers the file scanner to find new files and backfill sizes."""
    try:
        scan_and_queue()
        return jsonify({"status": "ok", "message": "Rescan completed successfully."})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

# [NEW] Global Exception Handler
@app.errorhandler(Exception)
def handle_exception(e):
    # Pass through HTTP errors
    if isinstance(e,  Response): return e
    if isinstance(e, HTTPException): return e
    
    # Log the error
    log_event("CRITICAL", f"Unhandled Exception: {str(e)}\n{traceback.format_exc()}")
    return "Internal Server Error", 500

def maintenance_loop():
    """Background thread to reset stuck jobs."""
    while True:
        try:
            # List to store logs so we can write them AFTER releasing the DB lock
            logs_to_write = [] 
            
            with db_lock:
                conn = sqlite3.connect(DB_FILE)
                cursor = conn.cursor()
                now = datetime.now()
                
                # Rule 1: Reset if assigned > 24 hours ago (Stuck job)
                cursor.execute("SELECT id, filename, started_at FROM jobs WHERE status IN ('processing', 'downloading', 'uploading')")
                for row in cursor.fetchall():
                    jid, fname, started = row
                    reset = False
                    
                    if started:
                        try:
                            s_time = datetime.strptime(started, "%Y-%m-%d %H:%M:%S.%f")
                            if (now - s_time).total_seconds() > 86400: # 24 hours
                                reset = True
                                # Queue the log message instead of writing it immediately
                                logs_to_write.append(("WARN", "Job timed out (24h limit). Resetting.", jid))
                        except: pass 
                    
                    if reset:
                        cursor.execute("UPDATE jobs SET status='queued', progress=0, worker_id=NULL, last_updated=?, started_at=NULL WHERE id=?", (now, jid))
                        if jid not in queued_job_ids:
                             job_queue.put({"id": jid, "filename": fname, "download_url": f"{SERVER_URL_DISPLAY.rstrip('/')}/download_source/{quote(jid, safe='/')}"})
                             queued_job_ids.add(jid)

                # Rule 2: Reset if last_updated > 2 hours ago (Disconnected worker)
                cursor.execute("SELECT id, filename, last_updated FROM jobs WHERE status IN ('processing', 'downloading', 'uploading')")
                for row in cursor.fetchall():
                    jid, fname, last_up = row
                    reset = False
                    
                    if last_up:
                        try:
                            l_time = datetime.strptime(last_up, "%Y-%m-%d %H:%M:%S.%f")
                            if (now - l_time).total_seconds() > 7200: # 2 hours
                                reset = True
                                # Queue the log message
                                logs_to_write.append(("WARN", "Zombie worker detected (2h silence). Resetting.", jid))
                        except: pass
                    
                    if reset:
                        cursor.execute("UPDATE jobs SET status='queued', progress=0, worker_id=NULL, last_updated=?, started_at=NULL WHERE id=?", (now, jid))
                        job_queue.put({"id": jid, "filename": fname, "download_url": f"{SERVER_URL_DISPLAY.rstrip('/')}/download_source/{quote(jid, safe='/')}"})
                
                conn.commit()
                conn.close()
            
            # --- SAFE ZONE: DB lock is released ---
            # Now we can safely write the logs to the database
            for level, msg, jid in logs_to_write:
                log_event(level, msg, jid)

        except Exception as e:
            print(f"[!] Maintenance error: {e}")
        
        time.sleep(600) # Run every 10 minutes

if __name__ == '__main__':
    print(f"[*] Manager running at {SERVER_URL_DISPLAY}")
    init_db()
    scan_and_queue()
    threading.Thread(target=maintenance_loop, daemon=True).start()
    app.run(host=SERVER_HOST, port=SERVER_PORT, threaded=True)

