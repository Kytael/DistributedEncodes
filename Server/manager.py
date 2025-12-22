@app.route('/stats', methods=['GET'])
def stats():
    filter_type = request.args.get('filter', 'all')
    
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    
    # 1. Queue Stats
    c.execute("SELECT status, COUNT(*) FROM jobs GROUP BY status")
    counts = dict(c.fetchall())
    queue_stats = {
        "total": sum(counts.values()),
        "pending": counts.get('pending', 0),
        "processing": counts.get('processing', 0),
        "done": counts.get('completed', 0)
    }

    # 2. Leaderboard
    time_filter = ""
    params = []
    
    if filter_type == '24h':
        # Filter logic here requires work_log table for accuracy, 
        # but for simplicity we will query the work_log if available or fallback.
        # Let's stick to the users table for totals, and work_log for timeframes.
        cutoff = int(time.time()) - 86400
        c.execute('''SELECT username, SUM(duration_minutes) as time, COUNT(*) as count 
                     FROM work_log WHERE timestamp > ? 
                     GROUP BY username ORDER BY time DESC''', (cutoff,))
    elif filter_type == '30d':
        cutoff = int(time.time()) - (30 * 86400)
        c.execute('''SELECT username, SUM(duration_minutes) as time, COUNT(*) as count 
                     FROM work_log WHERE timestamp > ? 
                     GROUP BY username ORDER BY time DESC''', (cutoff,))
    else:
        # All time (from users table)
        c.execute("SELECT username, total_minutes, jobs_completed FROM users ORDER BY total_minutes DESC")

    users = [{"name": r[0], "time": round(r[1], 1), "count": r[2]} for r in c.fetchall()]

    # 3. Active Workers
    c.execute("SELECT worker, filename, progress FROM jobs WHERE status='processing'")
    active = [{"user": r[0], "file": r[1], "progress": r[2]} for r in c.fetchall()]
    
    conn.close()
    return jsonify({"queue": queue_stats, "users": users, "active": active})
