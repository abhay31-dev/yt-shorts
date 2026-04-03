import os
import json
import re
import time
import logging
import secrets
import urllib.parse
import requests as req
from pathlib import Path
from collections import Counter

from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from googleapiclient.errors import HttpError

# Suppress noisy file_cache warning
logging.getLogger("googleapiclient.discovery_cache").setLevel(logging.ERROR)

log = logging.getLogger("shorts-pipeline")

BASE_DIR  = Path(__file__).parent
CACHE_DIR = BASE_DIR / ".cache"

SCOPES = [
    "https://www.googleapis.com/auth/youtube.upload",
    "https://www.googleapis.com/auth/youtube.readonly",
]

CATEGORY_PEOPLE_BLOGS = "22"

STOP_WORDS = {
    "the","a","an","and","or","but","in","on","at","to","for","of","with",
    "is","was","are","were","be","been","being","have","has","had","do",
    "does","did","will","would","could","should","may","might","shall",
    "that","this","it","its","i","you","he","she","we","they","them",
    "their","our","your","my","his","her","what","when","where","who",
    "how","why","just","like","get","got","so","if","not","no","yes",
    "very","really","also","even","more","most","some","any","all","up",
    "out","about","from","into","than","then","there","here","now","can",
}

HOOK_PATTERNS = [
    r"\bthis is why\b", r"\byou need to\b", r"\bmost people\b",
    r"\bno one tells you\b", r"\bthe truth\b", r"\bbig mistake\b",
    r"\bsecret\b", r"\bwhat if\b", r"\bthe problem is\b",
    r"\bhere's the thing\b", r"\blet me show you\b", r"\bwatch this\b",
    r"\byou have to\b", r"\bactually\b", r"\bhonestly\b",
]


# =========================
# OAUTH HELPERS
# =========================

def get_auth_url(client_id, client_secret, redirect_uri):
    """Build Google OAuth URL — offline access for refresh token support."""
    state = secrets.token_urlsafe(24)
    params = {
        "client_id":     client_id,
        "redirect_uri":  redirect_uri,
        "response_type": "code",
        "scope":         " ".join(SCOPES),
        "state":         state,
        "prompt":        "consent",
        "access_type":   "offline",  # gets refresh token
    }
    auth_url = "https://accounts.google.com/o/oauth2/v2/auth?" + urllib.parse.urlencode(params)
    return auth_url, state


def exchange_code(client_id, client_secret, redirect_uri, code):
    """Exchange auth code for tokens via direct POST — avoids PKCE requirement."""
    resp = req.post("https://oauth2.googleapis.com/token", data={
        "code":          code,
        "client_id":     client_id,
        "client_secret": client_secret,
        "redirect_uri":  redirect_uri,
        "grant_type":    "authorization_code",
    })
    resp.raise_for_status()
    token_data = resp.json()
    if "error" in token_data:
        raise RuntimeError(f"Token exchange failed: {token_data}")
    return {
        "token":         token_data["access_token"],
        "refresh_token": token_data.get("refresh_token"),
        "token_uri":     "https://oauth2.googleapis.com/token",
        "client_id":     client_id,
        "client_secret": client_secret,
        "scopes":        SCOPES,
    }


def dict_to_creds(d):
    """Convert stored dict to Credentials, auto-refreshing if expired."""
    creds = Credentials(
        token=         d.get("token"),
        refresh_token= d.get("refresh_token"),
        token_uri=     d.get("token_uri", "https://oauth2.googleapis.com/token"),
        client_id=     d.get("client_id"),
        client_secret= d.get("client_secret"),
        scopes=        d.get("scopes"),
    )
    # Auto-refresh if expired and we have a refresh token
    if creds.expired and creds.refresh_token:
        try:
            creds.refresh(Request())
            log.info("OAuth token refreshed successfully")
        except Exception as e:
            log.warning(f"Token refresh failed: {e} — user may need to re-login")
    return creds


def get_channel_info(creds_dict):
    """Return channel title and id for the authenticated user."""
    try:
        creds   = dict_to_creds(creds_dict)
        youtube = build("youtube", "v3", credentials=creds)
        resp    = youtube.channels().list(part="snippet", mine=True).execute()
        items   = resp.get("items", [])
        if items:
            return {
                "id":    items[0]["id"],
                "title": items[0]["snippet"]["title"],
            }
    except Exception as e:
        log.error(f"Failed to get channel info: {e}")
    return None


# =========================
# TRANSCRIPT HELPERS
# =========================

def load_transcript_for_short(short_name):
    """
    Given short_a3f9c1_1_1234567890.mp4, find transcript from cache.
    Returns (transcript, original_url) or (None, "").
    """
    parts = short_name.replace(".mp4", "").split("_")
    if len(parts) < 2:
        return None, ""

    hash_prefix = parts[1]

    if not CACHE_DIR.exists():
        return None, ""

    for cache_dir in CACHE_DIR.iterdir():
        if cache_dir.is_dir() and cache_dir.name.startswith(hash_prefix):
            transcript_path = cache_dir / "transcript.json"
            if transcript_path.exists():
                try:
                    data = json.loads(transcript_path.read_text(encoding="utf-8"))
                    meta_path = cache_dir / "meta.json"
                    meta = {}
                    if meta_path.exists():
                        meta = json.loads(meta_path.read_text(encoding="utf-8"))
                    return data, meta.get("source", "")
                except Exception:
                    pass
    return None, ""


def extract_tags(transcript, max_tags=15):
    """Extract keyword tags from transcript."""
    text  = " ".join(s["text"] for s in transcript).lower()
    text  = re.sub(r"[^\w\s]", " ", text)
    words = text.split()

    keywords = [w for w in words if w not in STOP_WORDS and len(w) > 3]
    freq     = Counter(keywords)
    tags     = [word for word, _ in freq.most_common(max_tags * 2)]

    bigrams = []
    for i in range(len(words) - 1):
        w1, w2 = words[i], words[i+1]
        if w1 not in STOP_WORDS and w2 not in STOP_WORDS and len(w1) > 3 and len(w2) > 3:
            bigrams.append(f"{w1} {w2}")

    bigram_freq = Counter(bigrams)
    top_bigrams = [bg for bg, count in bigram_freq.most_common(5) if count >= 2]
    combined    = top_bigrams + [t for t in tags if t not in " ".join(top_bigrams)]
    return combined[:max_tags]


def generate_titles(transcript, original_url=""):
    """Generate 3 distinct title options from transcript."""
    if not transcript:
        return ["Short Clip #1", "Must Watch This", "You Need to See This"]

    sentences = [s["text"].strip() for s in transcript if len(s["text"].strip()) > 20]

    scored = []
    for sent in sentences:
        score = sum(1 for p in HOOK_PATTERNS if re.search(p, sent.lower()))
        scored.append((score, sent))
    scored.sort(reverse=True)

    top_sents = [s for _, s in scored[:10]] if scored else sentences[:10]
    titles    = []

    # Title 1 — best hook sentence
    if top_sents:
        clean = re.sub(r'\s+', ' ', top_sents[0]).strip()
        if len(clean) > 60:
            clean = clean[:57].rsplit(' ', 1)[0] + "..."
        titles.append(clean)

    # Title 2 — question format
    if len(top_sents) > 1:
        clean = re.sub(r'\s+', ' ', top_sents[1]).strip()
        if len(clean) > 55:
            clean = clean[:52].rsplit(' ', 1)[0] + "..."
        if not clean.endswith("?"):
            clean = clean.rstrip(".!,") + "?"
        titles.append(clean)
    else:
        titles.append("You Need to Watch This")

    # Title 3 — keyword driven
    tags = extract_tags(transcript, max_tags=5)
    if tags:
        kw = " ".join(t.title() for t in tags[:3])
        titles.append(f"{kw} — Must Watch")
    else:
        titles.append("This Changes Everything")

    # Deduplicate and pad to exactly 3
    seen, unique = set(), []
    for t in titles:
        if t not in seen:
            seen.add(t)
            unique.append(t)

    fallbacks = ["You Won't Believe This", "Watch Before It's Gone", "This Is Important"]
    while len(unique) < 3:
        fb = fallbacks.pop(0)
        if fb not in seen:
            unique.append(fb)

    return unique[:3]


# =========================
# YOUTUBE UPLOAD
# =========================

def upload_to_youtube(creds_dict, video_path, title, tags, original_url,
                      privacy="private", progress_callback=None):
    """
    Upload video to YouTube with resumable upload + retry.
    progress_callback(percent, status) called during upload.
    Returns YouTube video ID on success.
    """
    creds   = dict_to_creds(creds_dict)
    youtube = build("youtube", "v3", credentials=creds)

    description = (
        f"{title}\n\n"
        f"✂️ This short was clipped from:\n{original_url}\n\n"
        f"#Shorts #YouTubeShorts"
    )

    body = {
        "snippet": {
            "title":       title,
            "description": description,
            "tags":        tags,
            "categoryId":  CATEGORY_PEOPLE_BLOGS,
        },
        "status": {
            "privacyStatus":           privacy,
            "selfDeclaredMadeForKids": False,
        }
    }

    media = MediaFileUpload(
        str(video_path),
        mimetype="video/mp4",
        resumable=True,
        chunksize=5 * 1024 * 1024,
    )

    insert_request = youtube.videos().insert(
        part=",".join(body.keys()),
        body=body,
        media_body=media,
    )

    response  = None
    retry     = 0
    max_retry = 5

    # Send initial progress
    if progress_callback:
        progress_callback(5, "starting")

    while response is None:
        try:
            status, response = insert_request.next_chunk()
            if status:
                pct = int(status.progress() * 100)
                if progress_callback:
                    progress_callback(pct, "uploading")
        except HttpError as e:
            if e.resp.status in [500, 502, 503, 504]:
                retry += 1
                if retry > max_retry:
                    raise RuntimeError(f"Upload failed after {max_retry} retries: {e}")
                wait = 2 ** retry
                log.warning(f"Retryable error {e.resp.status} — retrying in {wait}s")
                time.sleep(wait)
            else:
                raise RuntimeError(f"YouTube upload error: {e}")

    if progress_callback:
        progress_callback(100, "done")

    video_id = response.get("id")
    log.info(f"Uploaded → https://youtube.com/watch?v={video_id}")
    return video_id
