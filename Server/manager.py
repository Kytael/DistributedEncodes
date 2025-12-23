from flask import Flask, request, jsonify, send_from_directory, Response, stream_with_context
from flask_cors import CORS
import sqlite3, os, datetime, time, json
from functools import wraps
from ftplib import FTP

app = Flask(__name__)
CORS(app) 

DB_NAME = "queue.db"
API_TOKEN = "FractumSecure2025"
ADMIN_TOKEN = os.environ.get("FRACTUM_ADMIN_TOKEN", "FractumAdmin2025")
PRESET_FILE = "FractumAV1.json"

FTP_HOST = "transcode.fractumseraph.net"
FTP_USER = "transcode"
FTP_PASS = "transcode"

@app.after_request
def add_security_headers(response):
    response.headers['Cross-Origin-Opener-Policy'] = 'same-origin'
    response.headers['Cross-Origin-Embedder-Policy'] = 'require-corp'
    response.headers['Access-Control-Allow-Origin'] = '*' 
    response.headers['Access-Control-Allow-Headers'] = 'Content-Type, X-Auth-Token'
    return response

# --- HELPERS ---
TOLERANCE_HEIGHT = 10 
REQUEST_HISTORY = {}
LIMIT_WINDOW = 60 
MAX_REQUESTS = 100 

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
        return f(*args, **kwargs)
    return decorated

def load_validation_rules():
    # Strictly load from JSON. Default to AV1/480p if missing.
    current = {"height": 480, "codec": "av1"} 
    if os.path.exists(PRESET_FILE):
        try:
            with open(PRESET_FILE, 'r') as f:
                data = json.load(f)
                preset = data['PresetList'][0]
                current['height'] = int(preset.get('PictureHeight', 480))
                enc = preset.get('VideoEncoder', 'svt_av1')
                if 'av1' in enc: current['codec'] = 'av1'
                elif '265' in enc or 'hevc' in enc: current['codec'] = 'hevc'
        except: pass
    
    # [REVERTED] Removed VP9/H264 from valid list.
    legacy = [
        {"height": 480, "codec": "av1"}, 
        {"height": 480, "codec": "hevc"}
    ]
    return current, legacy

def validate_upload(metadata):
    if not metadata: 
        print("[DEBUG] Validation failed: No metadata dictionary provided.")
        return False, "No metadata"
    
    print(f"[DEBUG] Validating Metadata: {metadata}")
    uploaded_h = int(metadata.get('height', 0))
    uploaded_c = metadata.get('codec_name', '').lower()
    current, legacy_list = load_validation_rules()
    
    # Check Current
    print(f"[DEBUG] Checking against CURRENT: {current['codec']}/{current['height']}p (Tolerance: {TOLERANCE_HEIGHT})")
    if (uploaded_c == current['codec'] and abs(uploaded_h - current['height']) <= TOLERANCE_HEIGHT):
        return True, "Valid (Current)"
    
    # Check Legacy
    for i, rules in enumerate(legacy_list):
        print(f"[DEBUG] Checking against LEGACY[{i}]: {rules['codec']}/{rules['height']}p")
        if (uploaded_c == rules['codec'] and abs(uploaded_h - rules['height']) <= TOLERANCE_HEIGHT):
            return True, "Valid (Legacy)"
            
    return False, f"Mismatch: Got {uploaded_c}/{uploaded_h}p"

def init_db():
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS jobs (id INTEGER PRIMARY KEY, filename TEXT, status TEXT, worker TEXT, start_time INTEGER, end_time INTEGER, progress INTEGER DEFAULT 0)''')
    c.execute('''CREATE TABLE IF NOT EXISTS users (username TEXT PRIMARY KEY, total_minutes REAL DEFAULT 0, jobs_completed INTEGER DEFAULT 0)''')
    c.execute('''CREATE TABLE IF NOT EXISTS work_log (id INTEGER PRIMARY KEY, username TEXT, duration_minutes REAL, timestamp INTEGER)''')
    
    c.execute("PRAGMA table_info(jobs)")
    cols = [i[1] for i in c.fetchall()]
    if 'progress' not in cols: c.execute("ALTER TABLE jobs ADD COLUMN progress INTEGER DEFAULT 0")
    conn.commit()
    conn.close()

# --- BRIDGE ROUTES ---

@app.route('/bridge/download/<path:filename>')
def bridge_download(filename):
    def generate():
        ftp = FTP(FTP_HOST)
        ftp.login(FTP_USER, FTP_PASS)
        try:
            ftp.cwd("source")
            if "/" in filename:
                d = os.path.dirname(filename)
                if d: 
                    for folder in d.split("/"):
                        try: ftp.cwd(folder)
                        except: pass
            base_name = os.path.basename(filename)
            with ftp.transfercmd(f"RETR {base_name}") as data_sock:
                while True:
                    chunk = data_sock.recv(8192)
                    if not chunk: break
                    yield chunk
        except Exception as e:
            print(f"Bridge Download Error: {e}")
        finally:
            try: ftp.quit()
            except: pass

    return Response(stream_with_context(generate()), mimetype="video/mp4")

@app.route('/bridge/upload/<filename>', methods=['POST'])
def bridge_upload(filename):
    ftp = FTP(FTP_HOST)
    ftp.login(FTP_USER, FTP_PASS)
    try:
        ftp.cwd("completed")
        ftp.storbinary(f"STOR {filename}", request.stream)
        ftp.quit()
        return jsonify({"status": "uploaded"})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# --- API ROUTES ---

@app.route('/')
def dashboard(): return send_from_directory('.', 'index.html')

@app.route('/get_job', methods=['POST'])
@check_auth
def get_job():
    data = request.json
    worker_name = data.get('username', 'Unknown')
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    cutoff = int(time.time()) - (4 * 3600)
    c.execute("UPDATE jobs SET status='pending', worker=NULL, progress=0 WHERE status='processing' AND start_time < ?", (cutoff,))
    conn.commit()
    c.execute("SELECT id, filename FROM jobs WHERE status='pending' ORDER BY id ASC LIMIT 1")
    job = c.fetchone()
    if job:
        job_id, filename = job
        c.execute("UPDATE jobs SET status='processing', worker=?, start_time=?, progress=0 WHERE id=?", (worker_name, int(time.time()), job_id))
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
    conn = sqlite3.connect(DB_NAME)
    conn.execute("UPDATE jobs SET start_time=?, progress=?, status='processing' WHERE id=? AND status != 'completed'", 
                 (int(time.time()), int(data.get('progress', 0)), data['id']))
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
    
    is_valid, msg = validate_upload(metadata)
    if not is_valid: print(f"[!] Validation Warning for Job {job_id}: {msg}")

    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute("UPDATE jobs SET status='completed', end_time=?, progress=100 WHERE id=?", (int(time.time()), job_id))
    c.execute("INSERT OR IGNORE INTO users (username) VALUES (?)", (username,))
    duration = 5
    if metadata and 'duration' in metadata:
        try: duration = float(metadata['duration']) / 60
        except: pass
    c.execute("UPDATE users SET total_minutes = total_minutes + ?, jobs_completed = jobs_completed + 1 WHERE username=?", (duration, username))
    c.execute("INSERT INTO work_log (username, duration_minutes, timestamp) VALUES (?, ?, ?)", (username, duration, int(time.time())))
    conn.commit()
    conn.close()
    return jsonify({"status": "success"})

@app.route('/fail_job', methods=['POST'])
@check_auth
def fail_job():
    data = request.json
    reason = data.get('reason', 'Unknown')
    job_id = data.get('id')
    print(f"[!] Job {job_id} FAILED. Reason: {reason}")
    
    conn = sqlite3.connect(DB_NAME)
    conn.execute("UPDATE jobs SET status='pending', worker=NULL, progress=0 WHERE id=?", (job_id,))
    conn.commit()
    conn.close()
    return jsonify({"status": "reset"})

@app.route('/stats', methods=['GET'])
def stats():
    filter_type = request.args.get('filter', 'all')
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute("SELECT status, COUNT(*) FROM jobs GROUP BY status")
    counts = dict(c.fetchall())
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
        c.execute("SELECT COALESCE(worker, 'Unknown'), filename, progress FROM jobs WHERE status='processing'")
        active = [{"user": r[0], "file": r[1], "progress": r[2]} for r in c.fetchall()]
    except: active = []
    conn.close()
    return jsonify({"queue": queue_stats, "users": users, "active": active})

@app.route('/admin/reset', methods=['GET'])
def admin_reset():
    user_token = request.args.get('token')
    if not user_token or user_token != ADMIN_TOKEN: return "Unauthorized", 403
    job_id = request.args.get('id')
    if not job_id: return "Missing ID", 400
    conn = sqlite3.connect(DB_NAME)
    conn.execute("UPDATE jobs SET status='pending', worker=NULL, progress=0 WHERE id=?", (job_id,))
    conn.commit()
    conn.close()
    return f"Job {job_id} reset to pending."

if __name__ == '__main__':
    init_db()
    app.run(host='0.0.0.0', port=5000)
