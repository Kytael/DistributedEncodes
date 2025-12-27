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

# ==============================================================================
# CONFIGURATION
# ==============================================================================
import shutil

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
                started_at TIMESTAMP
            )
        ''')
        conn.commit()
        conn.close()

def verify_upload(filepath):
    """Verifies that the uploaded file matches the strict encoding rules."""
    try:
        cmd = [
            'ffprobe', '-v', 'quiet', '-print_format', 'json',
            '-show_streams', '-show_format', filepath
        ]
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0: return False, "FFprobe failed"
        
        data = json.loads(result.stdout)
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
    except Exception as e:
        return False, f"Verification Exception: {str(e)}"

def scan_and_queue():
    print(f"[*] Scanning {SOURCE_DIRECTORY}...")
    if not os.path.exists(SOURCE_DIRECTORY): os.makedirs(SOURCE_DIRECTORY)
    conn = sqlite3.connect(DB_FILE); cursor = conn.cursor()
    
    # 1. Add new files
    for root, dirs, files in os.walk(SOURCE_DIRECTORY, topdown=True):
        dirs.sort(); files.sort()
        for file in files:
            if file.lower().endswith(VIDEO_EXTENSIONS):
                full_path = os.path.join(root, file)
                rel_path = os.path.relpath(full_path, SOURCE_DIRECTORY)
                cursor.execute("INSERT OR IGNORE INTO jobs (id, filename, status, last_updated) VALUES (?, ?, 'queued', ?)", (rel_path, file, datetime.now()))
    conn.commit()

    # 2. Load queue
    print("[*] Loading queue from database...")
    cursor.execute("SELECT id, filename FROM jobs WHERE status = 'queued'")
    for row in cursor.fetchall():
        if row[0] not in queued_job_ids:
            job_queue.put({"id": row[0], "filename": row[1], "download_url": f"{SERVER_URL_DISPLAY.rstrip('/')}/download_source/{quote(row[0], safe='/')}"})
            queued_job_ids.add(row[0])
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
    j = request.args.get('jobs', '0')
    if not j.isdigit(): j = '0'

    script = f"""#!/bin/bash
echo "[*] Initializing Worker for {SERVER_URL_DISPLAY}..."
if [ -x "$(command -v apt-get)" ]; then sudo apt-get update -qq > /dev/null && sudo apt-get install -y ffmpeg python3 python3-pip > /dev/null; elif [ -x "$(command -v dnf)" ]; then sudo dnf install -y ffmpeg python3 python3-pip; fi
pip3 install requests --break-system-packages 2>/dev/null || pip3 install requests > /dev/null
curl -s "{SERVER_URL_DISPLAY.rstrip('/')}/dl/worker" -o worker.py
python3 worker.py --username "{u}" --workername "{w}" --jobs {j} --manager "{SERVER_URL_DISPLAY}"
"""
    return Response(script, mimetype='text/x-shellscript')

@app.route('/download_source/<path:filename>')
def download_source(filename): return send_from_directory(SOURCE_DIRECTORY, filename, as_attachment=True)

@app.route('/get_job', methods=['GET'])
def get_job():
    try:
        with db_lock:
            job = job_queue.get_nowait()
            queued_job_ids.discard(job['id'])
            
            # Mark as processing immediately so no one else gets it (if logic changes) 
            # and to persist state across server restarts if queue was persistent (it isn't, but DB is).
            conn = sqlite3.connect(DB_FILE)
            conn.execute("UPDATE jobs SET status='processing', last_updated=?, started_at=? WHERE id=?", (datetime.now(), datetime.now(), job['id']))
            conn.commit(); conn.close()
        return jsonify({"status": "ok", "job": job})
    except queue.Empty: return jsonify({"status": "empty"})

@app.route('/upload_result', methods=['POST'])
def upload_result():
    job_id = request.form.get('job_id')
    if 'file' in request.files and job_id:
        # Security check: Prevent path traversal
        save_path = os.path.abspath(os.path.join(COMPLETED_DIRECTORY, job_id))
        completed_abs = os.path.abspath(COMPLETED_DIRECTORY)
        if not save_path.startswith(completed_abs):
             return jsonify({"status": "error", "message": "Invalid path"}), 403

        os.makedirs(os.path.dirname(save_path), exist_ok=True)
        request.files['file'].save(save_path)
        
        # --- VERIFICATION ---
        is_valid, reason = verify_upload(save_path)
        if not is_valid:
            print(f"[!] Rejected upload {job_id}: {reason}")
            os.remove(save_path) # Delete bad file
            # Mark as failed in DB
            with db_lock:
                conn = sqlite3.connect(DB_FILE)
                conn.execute("UPDATE jobs SET status='failed', last_updated=? WHERE id=?", (datetime.now(), job_id))
                conn.commit(); conn.close()
            return jsonify({"status": "error", "message": f"Verification failed: {reason}"}), 400

        with db_lock:
            conn = sqlite3.connect(DB_FILE)
            conn.execute("UPDATE jobs SET status='completed', progress=100, last_updated=? WHERE id=?", (datetime.now(), job_id))
            conn.commit(); conn.close()
        print(f"[+] Received & Verified: {job_id}")
        return jsonify({"status": "success"})
    return jsonify({"status": "error"}), 400

@app.route('/report_status', methods=['POST'])
def report_status():
    d = request.json
    status = d.get('status')
    
    # Security: Prevent workers from marking jobs as completed directly
    if status == 'completed':
        return jsonify({"status": "ignored", "message": "Cannot set completion status manually"}), 403

    with db_lock:
        conn = sqlite3.connect(DB_FILE)
        sql = "UPDATE jobs SET status=?, worker_id=?, progress=?, last_updated=? WHERE id=?"; params = [status, d.get('worker_id'), d.get('progress',0), datetime.now(), d.get('job_id')]
        if d.get('duration', 0) > 0: sql = "UPDATE jobs SET status=?, worker_id=?, progress=?, last_updated=?, duration=? WHERE id=?"; params.insert(4, d.get('duration'))
        conn.execute(sql, tuple(params)); conn.commit(); conn.close()
    return jsonify({"status": "received"})

@app.route('/api/stats')
def api_stats():
    with db_lock:
        conn = sqlite3.connect(DB_FILE); conn.row_factory = sqlite3.Row; c = conn.cursor()
        c.execute("SELECT worker_id, SUM(duration) as total_minutes, COUNT(*) as files_count FROM jobs WHERE status='completed' AND worker_id IS NOT NULL GROUP BY worker_id ORDER BY total_minutes DESC")
        sb = [dict(r) for r in c.fetchall()]
        c.execute("SELECT worker_id, filename, duration, progress, status FROM jobs WHERE status IN ('processing', 'downloading', 'uploading')")
        act = [dict(r) for r in c.fetchall()]
        c.execute("SELECT id, status, worker_id FROM jobs ORDER BY last_updated DESC LIMIT 20")
        hist = [dict(r) for r in c.fetchall()]
        conn.close()
    return jsonify({"scoreboard": sb, "active": act, "history": hist})

# --- ADMIN API ---
@app.route('/api/all_jobs')
@requires_auth
def api_all_jobs():
    with db_lock:
        conn = sqlite3.connect(DB_FILE); conn.row_factory = sqlite3.Row; c = conn.cursor()
        c.execute("SELECT id, status FROM jobs ORDER BY last_updated DESC")
        jobs = [dict(r) for r in c.fetchall()]; conn.close()
        return jsonify({"jobs": jobs})

@app.route('/api/admin_action', methods=['POST'])
@requires_auth
def admin_action():
    data = request.json; job_id = data.get('job_id'); action = data.get('action')
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

def maintenance_loop():
    """Background thread to reset stuck jobs."""
    while True:
        try:
            with db_lock:
                conn = sqlite3.connect(DB_FILE)
                cursor = conn.cursor()
                now = datetime.now()
                
                # Rule 1: Reset if assigned > 24 hours ago (Stuck job)
                # We need to handle cases where started_at might be NULL (legacy jobs)
                cursor.execute("SELECT id, filename, started_at FROM jobs WHERE status IN ('processing', 'downloading', 'uploading')")
                for row in cursor.fetchall():
                    jid, fname, started = row
                    reset = False
                    
                    if started:
                        try:
                            s_time = datetime.strptime(started, "%Y-%m-%d %H:%M:%S.%f")
                            if (now - s_time).total_seconds() > 86400: # 24 hours
                                reset = True
                                print(f"[!] Job {jid} timed out (24h limit). Resetting.")
                        except: pass # Parsing error or different format
                    
                    if reset:
                        cursor.execute("UPDATE jobs SET status='queued', progress=0, worker_id=NULL, last_updated=?, started_at=NULL WHERE id=?", (now, jid))
                        # Re-queue in memory
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
                                print(f"[!] Job {jid} zombie worker (2h silence). Resetting.")
                        except: pass
                    
                    if reset:
                        # Only reset if not already reset by Rule 1 (though redundant update is fine)
                        cursor.execute("UPDATE jobs SET status='queued', progress=0, worker_id=NULL, last_updated=?, started_at=NULL WHERE id=?", (now, jid))
                        # Re-queue if not already in queue (this is tricky with in-memory queue, but duplicate put is safer than lost job)
                        # To avoid duplicate in queue, we could check queue, but that's O(N).
                        # Simple approach: Just put it back. Workers handle duplicates if they grab same ID? 
                        # Actually manager gives jobs. If it's in queue twice, second get will be invalid status in DB?
                        # scan_and_queue checks DB status 'queued'.
                        # If we set DB status to 'queued', we must add to queue.
                        job_queue.put({"id": jid, "filename": fname, "download_url": f"{SERVER_URL_DISPLAY.rstrip('/')}/download_source/{quote(jid, safe='/')}"})
                
                conn.commit()
                conn.close()
        except Exception as e:
            print(f"[!] Maintenance error: {e}")
        
        time.sleep(600) # Run every 10 minutes

if __name__ == '__main__':
    print(f"[*] Manager running at {SERVER_URL_DISPLAY}")
    init_db()
    scan_and_queue()
    threading.Thread(target=maintenance_loop, daemon=True).start()
    app.run(host=SERVER_HOST, port=SERVER_PORT, threaded=True)
