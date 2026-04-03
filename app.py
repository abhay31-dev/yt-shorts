import os
import json
import time
import uuid
import queue
import logging
import threading
from pathlib import Path
from multiprocessing import Process, Manager
from flask import (Flask, render_template, request, Response,
                   jsonify, send_from_directory, session, redirect)

from shorts_pipeline import run_pipeline
from uploader import (get_auth_url, exchange_code, get_channel_info,
                      load_transcript_for_short, generate_titles,
                      extract_tags, upload_to_youtube)

app = Flask(__name__)

# Session secret — read from env, never hardcoded
app.secret_key = os.environ.get("FLASK_SECRET_KEY", os.urandom(24))

log = logging.getLogger("shorts-pipeline")

BASE_DIR   = Path(__file__).parent
SHORTS_DIR = BASE_DIR / "shorts"
SHORTS_DIR.mkdir(exist_ok=True)

# Google OAuth config — read from environment, never from code
GOOGLE_CLIENT_ID     = os.environ.get("GOOGLE_CLIENT_ID", "")
GOOGLE_CLIENT_SECRET = os.environ.get("GOOGLE_CLIENT_SECRET", "")

# Upload progress queues — keyed by upload_job_id
upload_queues = {}
# Completed upload results — keyed by upload_job_id for polling fallback
upload_results = {}

manager         = Manager()
progress_queues = manager.dict()
jobs            = manager.dict()


# =========================
# HELPERS
# =========================

def fmt_seconds(s):
    s = int(s)
    if s < 60:   return f"{s}s"
    m, sec = divmod(s, 60)
    if m < 60:   return f"{m}m {sec:02d}s"
    h, mn  = divmod(m, 60)
    return f"{h}h {mn:02d}m"


def get_redirect_uri():
    """Hardcoded redirect URI — must match Google Cloud Console exactly."""
    return "https://yt-studio.duckdns.org/auth/callback"


def get_user_creds():
    """Get current user's OAuth credentials from session."""
    return session.get("yt_credentials")


def oauth_configured():
    return bool(GOOGLE_CLIENT_ID and GOOGLE_CLIENT_SECRET)


# =========================
# PROGRESS LOGGER (pipeline)
# =========================

class ProgressLogger:
    def __init__(self, q):
        self.q         = q
        self.job_start = time.time()

    def _eta(self, percent):
        if not percent or percent <= 2: return None
        elapsed = time.time() - self.job_start
        total   = elapsed / (percent / 100)
        return fmt_seconds(max(0, total - elapsed))

    def push(self, stage, message, percent=None):
        elapsed = time.time() - self.job_start
        self.q.put(json.dumps({
            "stage": stage, "message": message,
            "percent": percent,
            "elapsed": fmt_seconds(elapsed),
            "eta":     self._eta(percent),
        }))

    def done(self, outputs):
        elapsed = time.time() - self.job_start
        self.q.put(json.dumps({
            "stage": "done", "outputs": outputs,
            "percent": 100,
            "elapsed": fmt_seconds(elapsed), "eta": "0s",
        }))

    def error(self, msg):
        elapsed = time.time() - self.job_start
        self.q.put(json.dumps({
            "stage": "error", "message": msg,
            "percent": 0,
            "elapsed": fmt_seconds(elapsed), "eta": None,
        }))


# =========================
# PIPELINE WORKER
# =========================

def run_pipeline_with_progress(url, count, job_id, progress_queues, jobs):
    q      = progress_queues[job_id]
    logger = ProgressLogger(q)

    def update_job(stage, percent):
        job = jobs.get(job_id)
        if job:
            job["stage"]    = stage
            job["progress"] = percent or job["progress"]
            jobs[job_id]    = job

    try:
        update_job("download", 5)
        logger.push("download", "Downloading video...", 5)

        import shorts_pipeline as sp

        class QueueHandler(logging.Handler):
            STAGE_MAP = {
                "Downloading":                 ("download",   "Downloading video...",                10),
                "REGISTRY HIT":                ("download",   "Video cached, skipping download...",  15),
                "Raw video cached":            ("download",   "Video cached, skipping download...",  15),
                "CACHE HIT: transcript":       ("transcribe", "Using cached transcript...",          35),
                "CACHE MISS: running Whisper": ("transcribe", "Transcribing audio...",               20),
                "CACHE HIT: scored":           ("analyze",    "Using cached clip scores...",         55),
                "CACHE MISS: scoring":         ("analyze",    "Scoring and selecting clips...",      45),
                "Rendering clip":              ("render",     "Rendering clips...",                  65),
                "Concatenating short":         ("concat",     "Joining clips...",                    80),
                "DONE":                        ("done_log",   "Finishing up...",                     95),
            }

            def emit(self, record):
                msg = record.getMessage()
                for key, (stage, label, pct) in self.STAGE_MAP.items():
                    if key in msg:
                        update_job(stage, pct)
                        logger.push(stage, label, pct)
                        return
                if record.levelno >= logging.WARNING:
                    logger.push("warn", f"Warning: {msg}", None)

        handler   = QueueHandler()
        sp_logger = logging.getLogger("shorts-pipeline")
        sp_logger.addHandler(handler)
        outputs   = run_pipeline(url, count)
        sp_logger.removeHandler(handler)

        job = jobs.get(job_id)
        if job:
            job["status"]      = "done"
            job["progress"]    = 100
            job["finished_at"] = time.time()
            jobs[job_id]       = job

        logger.done(outputs)

    except Exception as e:
        job = jobs.get(job_id)
        if job:
            job["status"]      = "error"
            job["finished_at"] = time.time()
            jobs[job_id]       = job
        logger.error(str(e))


# =========================
# PIPELINE ROUTES
# =========================

@app.route("/")
def index():
    return render_template("index.html")


@app.route("/generate", methods=["POST"])
def generate():
    url   = request.form.get("url", "").strip()
    count = request.form.get("count", "1")
    if not url:
        return jsonify({"error": "YouTube URL is required"}), 400
    try:
        count = int(count)
        if count < 1 or count > 10:
            return jsonify({"error": "Count must be between 1 and 10"}), 400
    except ValueError:
        return jsonify({"error": "Invalid count value"}), 400

    job_id = str(uuid.uuid4())
    q      = manager.Queue()
    progress_queues[job_id] = q
    jobs[job_id] = {
        "status": "running", "stage": "starting", "progress": 0,
        "url": url, "count": count,
        "created": time.time(), "pid": None,
    }

    p = Process(target=run_pipeline_with_progress,
                args=(url, count, job_id, progress_queues, jobs))
    p.start()
    job         = jobs[job_id]
    job["pid"]  = p.pid
    jobs[job_id] = job

    return jsonify({"job_id": job_id})


@app.route("/kill/<job_id>", methods=["POST"])
def kill_job(job_id):
    job = jobs.get(job_id)
    if not job:
        return jsonify({"error": "Job not found"}), 404
    try:
        os.kill(job["pid"], 9)
        job["status"] = "killed"; job["finished_at"] = time.time()
        jobs[job_id]  = job
        return jsonify({"status": "killed"})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/progress/<job_id>")
def progress(job_id):
    def stream():
        if job_id not in progress_queues:
            yield f"data: {json.dumps({'stage':'error','message':'Job not found'})}\n\n"
            return
        q = progress_queues[job_id]
        while True:
            try:
                msg  = q.get(timeout=60)
                yield f"data: {msg}\n\n"
                if json.loads(msg).get("stage") in ("done", "error"):
                    progress_queues.pop(job_id, None)
                    break
            except Exception:
                yield f"data: {json.dumps({'stage':'ping'})}\n\n"

    return Response(stream(), mimetype="text/event-stream",
                    headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


@app.route("/jobs")
def get_jobs():
    result = []
    for job_id, data in jobs.items():
        end_time = data.get("finished_at") or time.time()
        result.append({
            "job_id":   job_id,
            "status":   data["status"],
            "stage":    data["stage"],
            "progress": data["progress"],
            "url":      data.get("url", ""),
            "count":    data.get("count", 1),
            "elapsed":  fmt_seconds(end_time - data["created"]),
            "created":  fmt_seconds(time.time() - data["created"]),
        })
    result.sort(key=lambda x: x["created"])
    return jsonify(result)


@app.route("/videos")
def videos():
    files  = sorted(SHORTS_DIR.glob("short_*.mp4"), key=os.path.getmtime, reverse=True)
    result = []
    for f in files:
        st = f.stat()
        result.append({
            "name":    f.name,
            "size_mb": round(st.st_size / (1024 * 1024), 1),
            "created": time.strftime("%b %d, %Y %H:%M", time.localtime(st.st_mtime)),
        })
    return jsonify(result)


@app.route("/shorts/<filename>")
def serve_video(filename):
    return send_from_directory(SHORTS_DIR, filename)


# =========================
# AUTH ROUTES
# =========================

@app.route("/auth/status")
def auth_status():
    if not oauth_configured():
        return jsonify({
            "configured": False,
            "message": "GOOGLE_CLIENT_ID and GOOGLE_CLIENT_SECRET not set in environment."
        })
    creds = get_user_creds()
    if not creds:
        return jsonify({"configured": True, "logged_in": False})
    channel = get_channel_info(creds)
    return jsonify({
        "configured": True,
        "logged_in":  True,
        "channel":    channel,
    })


@app.route("/auth/login")
def auth_login():
    if not oauth_configured():
        return jsonify({"error": "OAuth not configured on server"}), 500
    auth_url, state = get_auth_url(
        GOOGLE_CLIENT_ID, GOOGLE_CLIENT_SECRET, get_redirect_uri()
    )
    session["oauth_state"] = state
    return redirect(auth_url)


@app.route("/auth/callback")
def auth_callback():
    code  = request.args.get("code")
    error = request.args.get("error")

    if error:
        return redirect("/?auth=error&reason=" + error)
    if not code:
        return redirect("/?auth=error&reason=no_code")

    try:
        creds = exchange_code(
            GOOGLE_CLIENT_ID, GOOGLE_CLIENT_SECRET,
            get_redirect_uri(), code
        )
        session["yt_credentials"] = creds
        return redirect("/?auth=success&tab=upload")
    except Exception as e:
        log.error(f"OAuth callback error: {e}")
        return redirect("/?auth=error&reason=exchange_failed")


@app.route("/auth/logout", methods=["POST"])
def auth_logout():
    session.pop("yt_credentials", None)
    return jsonify({"status": "logged_out"})


# =========================
# UPLOAD ROUTES
# =========================

@app.route("/upload/prepare", methods=["POST"])
def upload_prepare():
    """
    Given a list of short filenames, return generated titles + tags
    for each one so the user can review before uploading.
    """
    data      = request.get_json()
    filenames = data.get("filenames", [])

    if not filenames:
        return jsonify({"error": "No files provided"}), 400

    results = []
    for name in filenames:
        transcript, original_url = load_transcript_for_short(name)
        if transcript:
            titles = generate_titles(transcript, original_url)
            tags   = extract_tags(transcript)
        else:
            titles = ["Short Clip", "Must Watch", "You Need to See This"]
            tags   = ["shorts", "youtubeshorts", "viral"]

        results.append({
            "filename":     name,
            "titles":       titles,
            "tags":         tags,
            "original_url": original_url,
        })

    return jsonify({"videos": results})


@app.route("/upload/submit", methods=["POST"])
def upload_submit():
    """
    Start uploading one or more videos to YouTube.
    Expects list of {filename, title, tags, privacy}.
    Returns upload_job_id for SSE progress tracking.
    """
    creds = get_user_creds()
    if not creds:
        return jsonify({"error": "Not authenticated with YouTube"}), 401

    data   = request.get_json()
    videos = data.get("videos", [])

    if not videos:
        return jsonify({"error": "No videos to upload"}), 400

    upload_job_id = str(uuid.uuid4())
    q             = queue.Queue()
    upload_queues[upload_job_id] = q

    def do_uploads():
        time.sleep(2.5)  # wait for SSE connection to establish before sending events
        total     = len(videos)
        completed = []
        for idx, v in enumerate(videos, 1):
            filename     = v.get("filename")
            title        = v.get("title", "Short Clip")
            tags         = v.get("tags", [])
            privacy      = v.get("privacy", "private")
            original_url = v.get("original_url", "")
            video_path   = SHORTS_DIR / filename

            if not video_path.exists():
                q.put(json.dumps({
                    "type":     "video_error",
                    "filename": filename,
                    "message":  "File not found on server",
                    "index":    idx, "total": total,
                }))
                continue

            # Push start event
            q.put(json.dumps({
                "type":     "video_start",
                "filename": filename,
                "title":    title,
                "index":    idx, "total": total,
            }))

            def progress_cb(pct, status, fn=filename, i=idx, t=total):
                q.put(json.dumps({
                    "type":     "video_progress",
                    "filename": fn,
                    "percent":  pct,
                    "status":   status,
                    "index":    i, "total": t,
                }))

            try:
                video_id = upload_to_youtube(
                    creds, video_path, title, tags,
                    original_url, privacy, progress_cb
                )
                yt_url = f"https://youtube.com/watch?v={video_id}"
                completed.append({
                    "filename":    filename,
                    "video_id":    video_id,
                    "youtube_url": yt_url,
                })
                q.put(json.dumps({
                    "type":        "video_done",
                    "filename":    filename,
                    "video_id":    video_id,
                    "youtube_url": yt_url,
                    "index":       idx, "total": total,
                }))
            except Exception as e:
                q.put(json.dumps({
                    "type":     "video_error",
                    "filename": filename,
                    "message":  str(e),
                    "index":    idx, "total": total,
                }))

        # All done — save results for polling fallback
        upload_results[upload_job_id] = {
            "status":  "done",
            "videos":  completed,
        }
        q.put(json.dumps({"type": "all_done", "total": total}))

    t = threading.Thread(target=do_uploads, daemon=True)
    t.start()

    return jsonify({"upload_job_id": upload_job_id})


@app.route("/upload/progress/<upload_job_id>")
def upload_progress(upload_job_id):
    def stream():
        if upload_job_id not in upload_queues:
            yield f"data: {json.dumps({'type':'error','message':'Upload job not found'})}\n\n"
            return
        q = upload_queues[upload_job_id]
        while True:
            try:
                msg  = q.get(timeout=120)
                yield f"data: {msg}\n\n"
                if json.loads(msg).get("type") in ("all_done", "error"):
                    upload_queues.pop(upload_job_id, None)
                    break
            except Exception:
                yield f"data: {json.dumps({'type':'ping'})}\n\n"

    return Response(stream(), mimetype="text/event-stream",
                    headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})



@app.route("/upload/status/<upload_job_id>")
def upload_status(upload_job_id):
    """Polling fallback for upload progress — used if SSE misses events."""
    result = upload_results.get(upload_job_id)
    if result:
        return jsonify(result)
    return jsonify({"status": "pending"})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080, debug=False)
