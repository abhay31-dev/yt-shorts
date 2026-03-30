import sys
import subprocess
import logging
import hashlib
import json
import re
import random
import os
import stat
from pathlib import Path
from datetime import datetime, timezone

import cv2
from faster_whisper import WhisperModel

# =========================
# CONFIG
# =========================

BASE_DIR   = Path(__file__).parent
SHORTS_DIR = BASE_DIR / "shorts"
LOG_DIR    = BASE_DIR / "logs"
CACHE_DIR  = BASE_DIR / ".cache"

RAW_VIDEO  = SHORTS_DIR / "raw.mp4"

# Secure minimal cookies file — only 3 YouTube auth tokens, nothing else.
# Must contain only: __Secure-1PSID, __Secure-1PAPISID, CONSENT
# File permissions enforced to 600 (owner read/write only) at runtime.
COOKIES_FILE     = BASE_DIR / "yt_cookies.txt"
COOKIE_WARN_DAYS = 14   # warn in logs if cookies older than this

SHORTS_DIR.mkdir(exist_ok=True)
LOG_DIR.mkdir(exist_ok=True)
CACHE_DIR.mkdir(exist_ok=True)

LOG_FILE = LOG_DIR / "pipeline.log"

# Short duration targets (seconds)
SHORT_MIN_LEN  = 45
SHORT_MAX_LEN  = 60

# Individual clip length range (seconds)
CLIP_MIN       = 5
CLIP_MAX       = 12

# How many candidate clips to score before selecting
MAX_CANDIDATES = 80

# =========================
# LOGGING
# =========================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(sys.stdout)
    ]
)

log = logging.getLogger("shorts-pipeline")

# =========================
# UTIL
# =========================

def run(cmd, desc):
    log.info(desc)
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        log.error(f"Command failed: {' '.join(cmd)}")
        log.error(result.stderr)
        raise RuntimeError(f"{desc} failed: {result.stderr[-300:]}")

def sha256_file(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()

def load_json(path, default=None):
    if path.exists():
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return default
    return default

def save_json(path, data):
    path.write_text(json.dumps(data, indent=2), encoding="utf-8")

def overlaps(a_start, a_end, b_start, b_end, buffer=1.0):
    """Check overlap with a small buffer to avoid jarring cuts."""
    return not (a_end + buffer <= b_start or a_start >= b_end + buffer)

def timestamp():
    return int(datetime.utcnow().timestamp())

# =========================
# DOWNLOAD
# =========================

# =========================
# SECURE COOKIE MANAGEMENT
# =========================

# Required cookies — only these 3 are needed for YouTube auth.
# Nothing else from your browser is used or stored.
REQUIRED_COOKIES = {"__Secure-1PSID", "__Secure-1PAPISID", "CONSENT"}

def validate_cookies():
    """
    Checks that:
    1. yt_cookies.txt exists
    2. File permissions are 600 (owner only) — enforced automatically
    3. Only contains the 3 required YouTube tokens, nothing else
    4. Warns if the file is older than COOKIE_WARN_DAYS days
    Returns True if cookies are usable, False otherwise.
    """
    if not COOKIES_FILE.exists():
        log.error(
            "yt_cookies.txt not found. Create it with only these 3 cookies:\n"
            "  __Secure-1PSID, __Secure-1PAPISID, CONSENT\n"
            f"  Expected path: {COOKIES_FILE}"
        )
        return False

    # Enforce 600 permissions automatically — owner read/write only
    current_mode = stat.S_IMODE(os.stat(COOKIES_FILE).st_mode)
    if current_mode != 0o600:
        os.chmod(COOKIES_FILE, 0o600)
        log.info("Cookie file permissions set to 600 (owner only)")

    # Check age — warn if stale
    age_days = (datetime.now() - datetime.fromtimestamp(
        COOKIES_FILE.stat().st_mtime
    )).days
    if age_days >= COOKIE_WARN_DAYS:
        log.warning(
            f"yt_cookies.txt is {age_days} days old — YouTube cookies typically "
            f"expire after 2-4 weeks. If downloads fail, refresh your cookies."
        )

    # Validate only required cookies are present — no extra data
    found   = set()
    extras  = []
    for line in COOKIES_FILE.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        parts = line.split("\t")
        if len(parts) >= 6:
            name = parts[5]
            if name in REQUIRED_COOKIES:
                found.add(name)
            else:
                extras.append(name)

    missing = REQUIRED_COOKIES - found
    if missing:
        log.error(f"yt_cookies.txt is missing required cookies: {missing}")
        return False

    if extras:
        log.warning(
            f"yt_cookies.txt contains extra cookies: {extras}. "
            "For security, keep only __Secure-1PSID, __Secure-1PAPISID, CONSENT."
        )

    log.info(f"Cookie validation passed | age={age_days}d | required tokens present")
    return True


def build_minimal_cookies(psid, papisid, consent="YES+cb"):
    """
    Helper: programmatically write a minimal secure cookies file
    from just the 3 required values. Call this instead of manually
    editing the file if you prefer.

    Usage:
        from shorts_pipeline import build_minimal_cookies
        build_minimal_cookies("g.a000...", "AbCd12.../AbCd...")
    """
    far_future = "1800000000"
    lines = [
        "# Netscape HTTP Cookie File",
        "# Minimal YouTube auth — 3 tokens only",
        f".youtube.com\tTRUE\t/\tTRUE\t{far_future}\t__Secure-1PSID\t{psid}",
        f".youtube.com\tTRUE\t/\tTRUE\t{far_future}\t__Secure-1PAPISID\t{papisid}",
        f".youtube.com\tTRUE\t/\tFALSE\t{far_future}\tCONSENT\t{consent}",
    ]
    COOKIES_FILE.write_text("\n".join(lines) + "\n", encoding="utf-8")
    os.chmod(COOKIES_FILE, 0o600)
    log.info(f"Minimal cookie file written → {COOKIES_FILE}")


# =========================
# DOWNLOAD
# =========================

def download_video(url):
    """
    Download video using minimal secure cookies.
    Skips download if already cached for this URL.
    Cookie file is validated before every download attempt.
    """
    url_hash    = hashlib.md5(url.encode()).hexdigest()[:8]
    cached_path = SHORTS_DIR / f"raw_{url_hash}.mp4"

    if cached_path.exists():
        log.info("Raw video cached — skipping download")
        return cached_path

    # Validate cookies before attempting download
    if not validate_cookies():
        raise RuntimeError(
            "Cookie validation failed. See logs for details.\n"
            "Create yt_cookies.txt with only: __Secure-1PSID, __Secure-1PAPISID, CONSENT\n"
            f"Or call build_minimal_cookies(psid, papisid) from a Python shell."
        )

    # Remove old raw videos to save space
    for old in SHORTS_DIR.glob("raw_*.mp4"):
        log.info(f"Removing old raw video: {old.name}")
        old.unlink(missing_ok=True)

    run([
        "yt-dlp", url,
        "-f",    "bv*[height<=1080]+ba/b[height<=1080]",
        "-o",    str(cached_path),
        "--cookies", str(COOKIES_FILE),
        "--extractor-args", "youtube:player_client=web,default",
        "--js-runtimes", "node",
        "--retries", "10",
        "--fragment-retries", "10",
        "--no-abort-on-error",
        "--merge-output-format", "mp4",
        "--no-playlist",
    ], "Downloading video")

    return cached_path

# =========================
# TRANSCRIPTION
# Cached per video file hash — NOT per URL
# So same video = reuse transcript, new video = fresh transcript
# =========================

def transcribe(video, cache_dir):
    path = cache_dir / "transcript.json"
    cached = load_json(path)

    if cached:
        log.info("CACHE HIT: transcript")
        return cached

    log.info("CACHE MISS: running Whisper")
    model = WhisperModel("small", device="cpu", compute_type="int8")
    segments, info = model.transcribe(
        str(video),
        vad_filter=True,
        vad_parameters={"min_silence_duration_ms": 500},
        word_timestamps=True,
    )

    transcript = []
    for s in segments:
        transcript.append({
            "start": round(s.start, 2),
            "end":   round(s.end,   2),
            "text":  s.text.strip(),
        })

    log.info(f"Transcribed {len(transcript)} segments | lang={info.language} | duration={info.duration:.0f}s")
    save_json(path, transcript)
    return transcript

# =========================
# HOOK / ENGAGEMENT SCORING
# =========================

HOOK_PATTERNS = [
    (r"\?$",                    3),   # ends with question
    (r"\bthis is why\b",        3),
    (r"\byou (need|have) to\b", 3),
    (r"\bmost people\b",        2),
    (r"\bno one tells you\b",   4),
    (r"\bthe truth\b",          3),
    (r"\bbig mistake\b",        3),
    (r"\bsecret\b",             2),
    (r"\bnever\b",              2),
    (r"\balways\b",             1),
    (r"\bactually\b",           1),
    (r"\bwait\b",               2),
    (r"\bwhat if\b",            3),
    (r"\bimagine\b",            2),
    (r"\bhonestly\b",           1),
    (r"\bthe problem is\b",     3),
    (r"\bhere's the thing\b",   3),
    (r"\blet me show you\b",    3),
    (r"\bwatch this\b",         3),
]

def hook_score(text):
    t = text.lower()
    return sum(w for p, w in HOOK_PATTERNS if re.search(p, t))

# =========================
# FACE DETECTION
# =========================

face_cascade = cv2.CascadeClassifier(
    cv2.data.haarcascades + "haarcascade_frontalface_default.xml"
)

def face_score(video, t):
    """Returns 0, 1, or 2 based on face presence and size."""
    cap = cv2.VideoCapture(str(video))
    cap.set(cv2.CAP_PROP_POS_MSEC, t * 1000)
    ok, frame = cap.read()
    cap.release()

    if not ok:
        return 0

    gray  = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
    faces = face_cascade.detectMultiScale(gray, 1.3, 5)

    if len(faces) == 0:
        return 0
    # Bonus if face is large (close-up = more engaging)
    h, w = frame.shape[:2]
    largest = max(faces, key=lambda f: f[2] * f[3])
    face_area_ratio = (largest[2] * largest[3]) / (w * h)
    return 2 if face_area_ratio > 0.05 else 1

# =========================
# CANDIDATE CLIP EXTRACTION
# =========================

def extract_candidates(video, transcript, cache_dir):
    """
    Build a rich pool of scored candidate clips from transcript segments.
    Cached per video hash — reused across multiple run_pipeline calls
    for the same video file (saves Whisper + CV time).
    """
    scored_path = cache_dir / "scored_clips.json"
    cached = load_json(scored_path)

    if cached:
        log.info("CACHE HIT: scored clips")
        return cached

    log.info("CACHE MISS: scoring clips")

    # Merge short adjacent segments into coherent chunks
    merged = []
    i = 0
    while i < len(transcript):
        seg = dict(transcript[i])
        # Try to extend segment by merging with next ones
        while i + 1 < len(transcript):
            nxt = transcript[i + 1]
            gap = nxt["start"] - seg["end"]
            combined_dur = nxt["end"] - seg["start"]
            if gap < 1.5 and combined_dur <= CLIP_MAX:
                seg["end"]  = nxt["end"]
                seg["text"] += " " + nxt["text"].strip()
                i += 1
            else:
                break
        merged.append(seg)
        i += 1

    scored = []
    for seg in merged:
        dur = seg["end"] - seg["start"]
        if dur < CLIP_MIN or dur > CLIP_MAX:
            continue

        score = hook_score(seg["text"])
        score += face_score(video, seg["start"] + dur * 0.3)  # sample 30% in
        score += face_score(video, seg["start"] + dur * 0.7)  # sample 70% in

        # Length bonus — prefer clips closer to CLIP_MAX
        length_bonus = round((dur / CLIP_MAX) * 1.5, 2)
        score += length_bonus

        scored.append({
            "score":  round(score, 2),
            "start":  round(seg["start"], 2),
            "end":    round(seg["end"],   2),
            "dur":    round(dur, 2),
            "text":   seg["text"][:120],
        })

    scored.sort(key=lambda x: x["score"], reverse=True)
    scored = scored[:MAX_CANDIDATES]

    log.info(f"Scored {len(scored)} candidate clips")
    save_json(scored_path, scored)
    return scored

# =========================
# SHORT ASSEMBLY
# Build each short from multiple best non-overlapping clips
# targeting 45–60 seconds total
# =========================

def assemble_shorts(candidates, shorts_count, used_ranges):
    """
    For each short, greedily pick the highest-scored clips that:
    - Don't overlap each other
    - Don't overlap previously used ranges
    - Fill 45–60 seconds total
    - Come from diverse parts of the video (spread > 60s apart preferred)
    """
    shorts = []

    # Work from a fresh copy so we don't mutate the original
    pool = [c for c in candidates
            if not any(overlaps(c["start"], c["end"], u[0], u[1])
                       for u in used_ranges)]

    if not pool:
        log.warning("No unused clips available")
        return []

    for i in range(shorts_count):
        selected = []
        total_dur = 0
        used_in_this = []

        # Shuffle top clips slightly for variety between shorts
        top_pool = pool[:max(20, len(pool))]
        random.shuffle(top_pool[:10])  # inject some randomness at the top

        for clip in top_pool:
            if total_dur >= SHORT_MAX_LEN:
                break

            # Skip if overlaps anything already selected
            if any(overlaps(clip["start"], clip["end"], s["start"], s["end"])
                   for s in selected):
                continue

            # Skip if overlaps used ranges from previous shorts
            if any(overlaps(clip["start"], clip["end"], u[0], u[1])
                   for u in used_ranges):
                continue

            # Diversity bonus — prefer clips spread across the video
            if selected:
                min_distance = min(abs(clip["start"] - s["start"]) for s in selected)
                if min_distance < 30:
                    continue  # too close to another selected clip

            remaining = SHORT_MAX_LEN - total_dur
            if clip["dur"] > remaining + 5:
                continue  # would push too far over max

            selected.append(clip)
            used_in_this.append(clip)
            total_dur += clip["dur"]

            if total_dur >= SHORT_MIN_LEN:
                break

        if total_dur < SHORT_MIN_LEN:
            log.warning(f"Short #{i+1}: only {total_dur:.1f}s of content — below target, including anyway")

        if not selected:
            log.warning(f"Short #{i+1}: no clips available, skipping")
            continue

        # Sort clips chronologically for natural flow
        selected.sort(key=lambda x: x["start"])

        log.info(f"Short #{i+1}: {len(selected)} clips | {total_dur:.1f}s total | "
                 f"scores: {[c['score'] for c in selected]}")

        shorts.append([(c["start"], c["end"]) for c in selected])

        # Mark these clips as used
        for c in used_in_this:
            used_ranges.append([c["start"], c["end"]])

        # Remove used clips from pool
        pool = [c for c in pool if c not in used_in_this]

    return shorts

# =========================
# VIDEO RENDERING
# =========================

def render_short(video, clips, video_hash, index):
    """
    Render each clip individually then concatenate.
    Adds smooth fade transitions between clips.
    """
    temp_files  = []
    list_file   = SHORTS_DIR / f"concat_{index}.txt"
    output      = SHORTS_DIR / f"short_{video_hash[:6]}_{index}_{timestamp()}.mp4"

    total = len(clips)
    log.info(f"Rendering short #{index} with {total} clips → {output.name}")

    with open(list_file, "w", encoding="utf-8") as f:
        for i, (s, e) in enumerate(clips):
            temp = SHORTS_DIR / f"_clip_{index}_{i}.mp4"
            temp_files.append(temp)

            duration = round(e - s, 3)

            # Add tiny fade-in/out on each clip for smooth transitions
            fade_dur   = 0.3
            fade_start = max(0, duration - fade_dur)

            vf = (
                "scale=1920:-2,"
                "crop=w='min(iw,ih*9/16)':h='min(ih,iw*16/9)',"
                "scale=1080:1920,"
                f"fade=t=in:st=0:d={fade_dur},"
                f"fade=t=out:st={fade_start}:d={fade_dur}"
            )

            run([
                "ffmpeg", "-y",
                "-ss",     str(s),
                "-i",      str(video),
                "-t",      str(duration),
                "-vf",     vf,
                "-af",     f"afade=t=in:st=0:d={fade_dur},afade=t=out:st={fade_start}:d={fade_dur}",
                "-c:v",    "libx264",
                "-pix_fmt","yuv420p",
                "-preset", "fast",
                "-crf",    "23",
                "-c:a",    "aac",
                "-b:a",    "128k",
                "-movflags","+faststart",
                str(temp)
            ], f"Rendering clip {index}.{i+1}/{total} ({s:.1f}s → {e:.1f}s)")

            f.write(f"file '{temp.as_posix()}'\n")

    # Concatenate all clips
    run([
        "ffmpeg", "-y",
        "-f",     "concat",
        "-safe",  "0",
        "-i",     str(list_file),
        "-c",     "copy",
        "-movflags", "+faststart",
        str(output)
    ], f"Concatenating short #{index}")

    # Cleanup temp files
    for t in temp_files:
        t.unlink(missing_ok=True)
    list_file.unlink(missing_ok=True)

    log.info(f"Short #{index} ready → {output.name}")
    return output

# =========================
# URL REGISTRY
# Maps URL → video_hash so we never re-scan a large video file.
# Transcript + scored clips are reused on repeat URLs.
# Only the final short (rendered video) is always freshly generated.
# =========================

URL_REGISTRY = CACHE_DIR / "url_registry.json"

def get_registry():
    return load_json(URL_REGISTRY, {})

def register_url(url, video_hash):
    registry = get_registry()
    if url not in registry:
        registry[url] = {
            "video_hash": video_hash,
            "first_seen": datetime.utcnow().isoformat(),
            "runs": 1,
        }
        log.info(f"URL registered with tag: {video_hash[:12]}")
    else:
        registry[url]["runs"] += 1
        log.info(f"URL seen before (run #{registry[url]['runs']}) — reusing transcript tag: {video_hash[:12]}")
    save_json(URL_REGISTRY, registry)

def lookup_url(url):
    """Returns video_hash if URL was seen before, else None."""
    registry = get_registry()
    entry = registry.get(url)
    if entry:
        return entry["video_hash"]
    return None

# =========================
# PIPELINE ENTRYPOINT
# =========================

def run_pipeline(youtube_url: str, shorts_count: int):
    log.info("===== SHORTS PIPELINE STARTED =====")
    log.info(f"URL: {youtube_url} | Requested: {shorts_count} shorts")

    # ── Check URL registry first ──
    known_hash = lookup_url(youtube_url)

    if known_hash:
        # URL seen before — check if video file still exists
        cache_dir = CACHE_DIR / known_hash
        url_hash  = hashlib.md5(youtube_url.encode()).hexdigest()[:8]
        video     = SHORTS_DIR / f"raw_{url_hash}.mp4"

        if video.exists() and cache_dir.exists():
            log.info(f"REGISTRY HIT — skipping download + re-hash | tag: {known_hash[:12]}")
            video_hash = known_hash
        else:
            # Files were deleted — re-download and re-register
            log.info("Registry hit but files missing — re-downloading")
            video      = download_video(youtube_url)
            video_hash = sha256_file(video)
            known_hash = None  # force re-register below
    else:
        # New URL — download and hash
        video      = download_video(youtube_url)
        video_hash = sha256_file(video)

    # ── Register URL → hash mapping ──
    register_url(youtube_url, video_hash)
    log.info(f"Video tag: {video_hash[:12]}...")

    # ── Cache dir per video hash ──
    # Same video → reuse transcript + scored clips (saves Whisper time)
    # New video  → fresh transcript + scores
    cache_dir = CACHE_DIR / video_hash
    cache_dir.mkdir(exist_ok=True)

    if not (cache_dir / "meta.json").exists():
        save_json(cache_dir / "meta.json", {
            "source":  youtube_url,
            "created": datetime.utcnow().isoformat()
        })

    # ── Transcribe ──
    transcript = transcribe(video, cache_dir)
    if not transcript:
        raise RuntimeError("Transcription returned no segments. Is there audio in this video?")

    # ── Score candidates ──
    candidates = extract_candidates(video, transcript, cache_dir)
    if not candidates:
        raise RuntimeError("No scoreable clips found. Video may be too short or have no speech.")

    # ── Load used ranges — always fresh list per pipeline run ──
    # KEY CHANGE: we do NOT reuse used_ranges across runs.
    # Every call to run_pipeline gets a clean slate so the same
    # URL always produces new shorts from the best available clips.
    used_ranges = []

    # ── Assemble clip groups ──
    clip_groups = assemble_shorts(candidates, shorts_count, used_ranges)
    if not clip_groups:
        raise RuntimeError("Could not assemble any shorts. Try a longer video.")

    # ── Render ──
    outputs = []
    for i, clips in enumerate(clip_groups, 1):
        out = render_short(video, clips, video_hash, i)
        outputs.append(out.name)

    log.info(f"===== DONE: {len(outputs)} shorts generated =====")
    return outputs

# =========================
# CLI
# =========================

if __name__ == "__main__":
    url   = input("Enter YouTube link: ").strip()
    count = int(input("Number of shorts to generate: ").strip())
    results = run_pipeline(url, count)
    print("\nGenerated:")
    for r in results:
        print(f"  → {r}")
