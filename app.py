import os
import json
import time
import uuid
import logging
from pathlib import Path
from multiprocessing import Process, Manager
from flask import Flask, render_template, request, Response, jsonify, send_from_directory

from shorts_pipeline import run_pipeline

app = Flask(__name__)

BASE_DIR   = Path(__file__).parent
SHORTS_DIR = BASE_DIR / "shorts"
SHORTS_DIR.mkdir(exist_ok=True)

manager = Manager()

progress_queues = manager.dict()
jobs            = manager.dict()


def fmt_seconds(s):
    s = int(s)
    if s < 60:
        return f"{s}s"
    m, sec = divmod(s, 60)
    if m < 60:
        return f"{m}m {sec:02d}s"
    h, mn = divmod(m, 60)
    return f"{h}h {mn:02d}m"


class ProgressLogger:
    def __init__(self, q):
        self.q         = q
        self.job_start = time.time()

    def _eta(self, percent):
        if not percent or percent <= 2:
            return None
        elapsed = time.time() - self.job_start
        total   = elapsed / (percent / 100)
        return fmt_seconds(max(0, total - elapsed))

    def push(self, stage, message, percent=None):
        elapsed = time.time() - self.job_start
        self.q.put(json.dumps({
            "stage": stage,
            "message": message,
            "percent": percent,
            "elapsed": fmt_seconds(elapsed),
            "eta": self._eta(percent),
        }))

    def done(self, outputs):
        elapsed = time.time() - self.job_start
        self.q.put(json.dumps({
            "stage": "done",
            "outputs": outputs,
            "percent": 100,
            "elapsed": fmt_seconds(elapsed),
            "eta": "0s",
        }))

    def error(self, msg):
        elapsed = time.time() - self.job_start
        self.q.put(json.dumps({
            "stage": "error",
            "message": msg,
            "percent": 0,
            "elapsed": fmt_seconds(elapsed),
            "eta": None,
        }))


def run_pipeline_with_progress(url, count, job_id, progress_queues, jobs):
    q      = progress_queues[job_id]
    logger = ProgressLogger(q)

    def update_job(stage, percent):
        job = jobs.get(job_id)
        if job:
            job["stage"] = stage
            if percent is not None:
                job["progress"] = percent
            jobs[job_id] = job  # update back

    try:
        update_job("download", 5)
        logger.push("download", "Downloading video...", 5)

        import shorts_pipeline as sp

        class QueueHandler(logging.Handler):
            STAGE_MAP = {
                "Downloading": ("download", "Downloading video...", 10),
                "CACHE MISS": ("transcribe", "Transcribing...", 20),
                "CACHE HIT: transcript": ("transcribe", "Using transcript...", 35),
                "scoring": ("analyze", "Analyzing clips...", 45),
                "Rendering clip": ("render", "Rendering...", 65),
                "Concatenating": ("concat", "Joining clips...", 80),
            }

            def emit(self, record):
                msg = record.getMessage()
                for key, (stage, label, pct) in self.STAGE_MAP.items():
                    if key in msg:
                        update_job(stage, pct)
                        logger.push(stage, label, pct)
                        return

        handler   = QueueHandler()
        sp_logger = logging.getLogger("shorts-pipeline")
        sp_logger.addHandler(handler)

        outputs = run_pipeline(url, count)

        sp_logger.removeHandler(handler)

        job = jobs.get(job_id)
        if job:
            job["status"] = "done"
            job["progress"] = 100
            jobs[job_id] = job

        logger.done(outputs)

    except Exception as e:
        job = jobs.get(job_id)
        if job:
            job["status"] = "error"
            jobs[job_id] = job
        logger.error(str(e))


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/generate", methods=["POST"])
def generate():
    url   = request.form.get("url", "").strip()
    count = int(request.form.get("count", "1"))

    job_id = str(uuid.uuid4())

    q = manager.Queue()
    progress_queues[job_id] = q

    jobs[job_id] = {
        "status": "running",
        "stage": "starting",
        "progress": 0,
        "created": time.time(),
        "pid": None
    }

    p = Process(target=run_pipeline_with_progress,
                args=(url, count, job_id, progress_queues, jobs))
    p.start()

    job = jobs[job_id]
    job["pid"] = p.pid
    jobs[job_id] = job

    return jsonify({"job_id": job_id})


@app.route("/kill/<job_id>", methods=["POST"])
def kill_job(job_id):
    job = jobs.get(job_id)

    if not job:
        return jsonify({"error": "Job not found"}), 404

    pid = job.get("pid")

    try:
        os.kill(pid, 9)  # SIGKILL
        job["status"] = "killed"
        jobs[job_id] = job
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
                msg = q.get(timeout=60)
                yield f"data: {msg}\n\n"

                data = json.loads(msg)
                if data.get("stage") in ("done", "error"):
                    progress_queues.pop(job_id, None)
                    break

            except:
                yield f"data: {json.dumps({'stage':'ping'})}\n\n"

    return Response(stream(), mimetype="text/event-stream")


@app.route("/jobs")
def get_jobs():
    result = []

    for job_id, data in jobs.items():
        result.append({
            "job_id": job_id,
            "status": data["status"],
            "stage": data["stage"],
            "progress": data["progress"],
            "created": fmt_seconds(time.time() - data["created"])
        })

    return jsonify(result)


@app.route("/videos")
def videos():
    files  = sorted(SHORTS_DIR.glob("short_*.mp4"), key=os.path.getmtime, reverse=True)
    result = []

    for f in files:
        st = f.stat()
        result.append({
            "name": f.name,
            "size_mb": round(st.st_size / (1024 * 1024), 1),
            "created": time.strftime("%b %d, %Y %H:%M", time.localtime(st.st_mtime)),
        })

    return jsonify(result)


@app.route("/shorts/<filename>")
def serve_video(filename):
    return send_from_directory(SHORTS_DIR, filename)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)