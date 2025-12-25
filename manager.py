import os
import queue
import threading
import sqlite3
import shutil
from datetime import datetime
from flask import Flask, request, jsonify, render_template, send_from_directory, send_file, Response

# ==============================================================================
# CONFIGURATION
# ==============================================================================
SERVER_HOST = '0.0.0.0'
SERVER_PORT = 80
SERVER_URL_DISPLAY = "https://encode.fractumseraph.net/"

SOURCE_DIRECTORY = "./source_media"
COMPLETED_DIRECTORY = "./completed_media"
WORKER_TEMPLATE_FILE = "worker_template.py"
DB_FILE = "encoding_jobs.db"
VIDEO_EXTENSIONS = ('.mkv', '.mp4', '.avi', '.mov')

app = Flask(__name__)
job_queue = queue.Queue()
db_lock = threading.Lock()
# Allow uploads up to 16GB
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024 * 1024 

def init_db():
    with db_lock:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS jobs (
                id TEXT PRIMARY KEY, filename TEXT, status TEXT, worker_id TEXT,
                progress INTEGER DEFAULT 0, duration INTEGER DEFAULT 0, last_updated TIMESTAMP
            )
        ''')
        conn.commit()
        conn.close()

def scan_and_queue():
    """Scans source folder and DB to populate the memory queue on startup."""
    print(f"[*] Scanning {SOURCE_DIRECTORY}...")
    if not os.path.exists(SOURCE_DIRECTORY):
        os.makedirs(SOURCE_DIRECTORY)
        
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    # 1. Add new files to DB
    for root, dirs, files in os.walk(SOURCE_DIRECTORY, topdown=True):
        dirs.sort()
        files.sort()
        for file in files:
            if file.lower().endswith(VIDEO_EXTENSIONS):
                full_path = os.path.join(root, file)
                rel_path = os.path.relpath(full_path, SOURCE_DIRECTORY)
                
                cursor.execute('''
                    INSERT OR IGNORE INTO jobs (id, filename, status, last_updated)
                    VALUES (?, ?, 'queued', ?)
                ''', (rel_path, file, datetime.now()))
    conn.commit()

    # 2. Load 'queued' jobs into memory
    print("[*] Loading queue from database...")
    cursor.execute("SELECT id, filename FROM jobs WHERE status IN ('queued', 'processing')")
    rows = cursor.fetchall()
    for row in rows:
        job = {
            "id": row[0],
            "filename": row[1],
            "download_url": f"{SERVER_URL_DISPLAY.rstrip('/')}/download_source/{row[0]}"
        }
        job_queue.put(job)
    
    conn.close()
    print(f"[*] Queue ready. {job_queue.qsize()} jobs waiting.")

@app.route('/')
def dashboard():
    return render_template('dashboard.html')

@app.route('/dl/worker')
def download_worker_script():
    return send_file(WORKER_TEMPLATE_FILE, as_attachment=True, download_name='worker.py')

@app.route('/install')
def install_script():
    u = request.args.get('username', 'Anonymous')
    w = request.args.get('workername', 'LinuxNode')
    j = request.args.get('jobs', '0')
    
    # Bash script to auto-install dependencies and run worker
    script = f"""#!/bin/bash
echo "[*] Initializing Worker for {SERVER_URL_DISPLAY}..."
if [ -x "$(command -v apt-get)" ]; then
    sudo apt-get update -qq > /dev/null && sudo apt-get install -y ffmpeg python3 python3-pip > /dev/null
elif [ -x "$(command -v dnf)" ]; then
    sudo dnf install -y ffmpeg python3 python3-pip
fi
pip3 install requests --break-system-packages 2>/dev/null || pip3 install requests > /dev/null
curl -s "{SERVER_URL_DISPLAY.rstrip('/')}/dl/worker" -o worker.py
python3 worker.py --username "{u}" --workername "{w}" --jobs {j} --manager "{SERVER_URL_DISPLAY}"
"""
    return Response(script, mimetype='text/x-shellscript')

@app.route('/download_source/<path:filename>')
def download_source(filename):
    return send_from_directory(SOURCE_DIRECTORY, filename, as_attachment=True)

@app.route('/get_job', methods=['GET'])
def get_job():
    try:
        return jsonify({"status": "ok", "job": job_queue.get_nowait()})
    except queue.Empty:
        return jsonify({"status": "empty"})

@app.route('/upload_result', methods=['POST'])
def upload_result():
    job_id = request.form.get('job_id')
    worker_id = request.form.get('worker_id')
    if 'file' in request.files and job_id:
        save_path = os.path.join(COMPLETED_DIRECTORY, job_id)
        os.makedirs(os.path.dirname(save_path), exist_ok=True)
        request.files['file'].save(save_path)
        with db_lock:
            conn = sqlite3.connect(DB_FILE)
            conn.execute("UPDATE jobs SET status='completed', progress=100, last_updated=? WHERE id=?", (datetime.now(), job_id))
            conn.commit(); conn.close()
        print(f"[+] Received: {job_id} from {worker_id}")
        return jsonify({"status": "success"})
    return jsonify({"status": "error"}), 400

@app.route('/report_status', methods=['POST'])
def report_status():
    d = request.json
    with db_lock:
        conn = sqlite3.connect(DB_FILE)
        sql = "UPDATE jobs SET status=?, worker_id=?, progress=?, last_updated=? WHERE id=?"
        params = [d.get('status'), d.get('worker_id'), d.get('progress',0), datetime.now(), d.get('job_id')]
        if d.get('duration', 0) > 0:
            sql = "UPDATE jobs SET status=?, worker_id=?, progress=?, last_updated=?, duration=? WHERE id=?"
            params.insert(4, d.get('duration'))
        conn.execute(sql, tuple(params)); conn.commit(); conn.close()
    return jsonify({"status": "received"})

@app.route('/api/stats')
def api_stats():
    with db_lock:
        conn = sqlite3.connect(DB_FILE); conn.row_factory = sqlite3.Row; c = conn.cursor()
        c.execute("SELECT worker_id, SUM(duration) as total_minutes, COUNT(*) as files_count FROM jobs WHERE status='completed' AND worker_id IS NOT NULL GROUP BY worker_id ORDER BY total_minutes DESC")
        sb = [dict(r) for r in c.fetchall()]
        c.execute("SELECT worker_id, filename, duration, progress FROM jobs WHERE status='processing'")
        act = [dict(r) for r in c.fetchall()]
        c.execute("SELECT id, status, worker_id FROM jobs ORDER BY last_updated DESC LIMIT 20")
        hist = [dict(r) for r in c.fetchall()]
        conn.close()
    return jsonify({"scoreboard": sb, "active": act, "history": hist})

if __name__ == '__main__':
    print(f"[*] Manager running at {SERVER_URL_DISPLAY}")
    init_db()
    scan_and_queue()
    app.run(host=SERVER_HOST, port=SERVER_PORT, threaded=True)
