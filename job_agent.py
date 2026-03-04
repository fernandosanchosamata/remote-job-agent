#!/usr/bin/env python3
import os
import re
import sys
import json
import time
import hashlib
import sqlite3
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

import requests

# ---------------------------
# Config (env vars)
# ---------------------------
KEYWORDS = [k.strip() for k in os.getenv("KEYWORDS", "java,spring,spring boot,microservices,kafka,elasticsearch").split(",") if k.strip()]
NEGATIVE_KEYWORDS = [k.strip() for k in os.getenv("NEGATIVE_KEYWORDS", "onsite,on-site,hybrid,relocation,visa sponsorship required").split(",") if k.strip()]

# If provided, we'll keep only jobs mentioning at least one of these.
MUST_HAVE = [k.strip() for k in os.getenv("MUST_HAVE", "java,spring").split(",") if k.strip()]

# Regions / hiring
ALLOW_LATAM_HINTS = [s.strip() for s in os.getenv("ALLOW_LATAM_HINTS", "latam,latin america,south america,anywhere,global,worldwide,remote").split(",") if s.strip()]
ALLOW_EU_HINTS = [s.strip() for s in os.getenv("ALLOW_EU_HINTS", "europe,emea,eu,uk,remote").split(",") if s.strip()]

# Salary / currency signals
CURRENCY_HINTS = [s.strip() for s in os.getenv("CURRENCY_HINTS", "usd,$,eur,€,salary,compensation").split(",") if s.strip()]

# Basic thresholds (optional)
MIN_USD = float(os.getenv("MIN_USD", "0"))  # set e.g. 5000 for monthly or 90000 yearly; depends on source
MAX_RESULTS = int(os.getenv("MAX_RESULTS", "25"))

# Telegram
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

# Email (optional)
SMTP_HOST = os.getenv("SMTP_HOST", "")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER = os.getenv("SMTP_USER", "")
SMTP_PASS = os.getenv("SMTP_PASS", "")
EMAIL_TO = os.getenv("EMAIL_TO", "")
EMAIL_FROM = os.getenv("EMAIL_FROM", SMTP_USER)

DB_PATH = os.getenv("DB_PATH", "jobs.db")

# ---------------------------
# Sources
# ---------------------------
REMOTEOK_URL = "https://remoteok.com/api"  # public JSON
REMOTIVE_URL = "https://remotive.com/api/remote-jobs"  # public JSON
WWR_RSS = "https://weworkremotely.com/categories/remote-programming-jobs.rss"  # RSS

UA = {"User-Agent": "remote-job-agent/1.0 (+github actions)"}


def norm(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip()).lower()


def sha_id(*parts: str) -> str:
    h = hashlib.sha256()
    for p in parts:
        h.update((p or "").encode("utf-8"))
        h.update(b"|")
    return h.hexdigest()[:24]


def contains_any(text: str, needles: List[str]) -> bool:
    t = norm(text)
    return any(norm(n) in t for n in needles)


def parse_salary_to_usd(salary_text: str) -> Optional[float]:
    """
    Very rough heuristic: tries to extract a number like 120k, 90000, 6000/mo.
    Returns None if not parseable. We do not convert currencies; we only use it for a minimal threshold if it looks USD.
    """
    if not salary_text:
        return None
    t = salary_text.lower()

    # If it doesn't look like USD, skip parsing for threshold purposes
    if "$" not in t and "usd" not in t:
        return None

    # Find numbers
    # Examples: "$120k", "USD 100,000", "$6,000 / month"
    nums = re.findall(r"(\d[\d,\.]*)\s*(k)?", t)
    if not nums:
        return None

    # take the first reasonable one
    for num_str, kflag in nums:
        try:
            val = float(num_str.replace(",", ""))
            if kflag:
                val *= 1000.0
            # if it looks like monthly and small-ish, we leave it as-is (user decides MIN_USD)
            return val
        except Exception:
            continue
    return None


# ---------------------------
# DB
# ---------------------------
def db_init(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS seen_jobs (
            id TEXT PRIMARY KEY,
            source TEXT,
            title TEXT,
            company TEXT,
            url TEXT,
            first_seen_utc TEXT
        )
        """
    )
    conn.commit()


def db_is_new(conn: sqlite3.Connection, job_id: str) -> bool:
    cur = conn.execute("SELECT 1 FROM seen_jobs WHERE id=?", (job_id,))
    return cur.fetchone() is None


def db_mark_seen(conn: sqlite3.Connection, job_id: str, source: str, title: str, company: str, url: str) -> None:
    conn.execute(
        "INSERT OR IGNORE INTO seen_jobs (id, source, title, company, url, first_seen_utc) VALUES (?,?,?,?,?,?)",
        (job_id, source, title, company, url, datetime.now(timezone.utc).isoformat()),
    )
    conn.commit()


# ---------------------------
# Fetchers
# ---------------------------
def fetch_remoteok() -> List[Dict]:
    r = requests.get(REMOTEOK_URL, headers=UA, timeout=30)
    r.raise_for_status()
    data = r.json()
    jobs = []
    for item in data:
        if not isinstance(item, dict):
            continue
        # first entry is usually a "legal" note / metadata
        if "position" not in item and "title" not in item:
            continue
        title = item.get("position") or item.get("title") or ""
        company = item.get("company") or ""
        url = item.get("url") or ""
        desc = item.get("description") or ""
        tags = " ".join(item.get("tags") or [])
        salary = item.get("salary") or ""
        location = item.get("location") or ""

        jobs.append(
            {
                "source": "remoteok",
                "title": title,
                "company": company,
                "url": url,
                "description": desc,
                "tags": tags,
                "location": location,
                "salary": salary,
            }
        )
    return jobs


def fetch_remotive() -> List[Dict]:
    r = requests.get(REMOTIVE_URL, headers=UA, timeout=30)
    r.raise_for_status()
    data = r.json()
    jobs = []
    for item in data.get("jobs", []):
        title = item.get("title") or ""
        company = item.get("company_name") or ""
        url = item.get("url") or item.get("job_url") or ""
        desc = item.get("description") or ""
        tags = " ".join(item.get("tags") or [])
        salary = item.get("salary") or ""
        location = item.get("candidate_required_location") or ""
        jobs.append(
            {
                "source": "remotive",
                "title": title,
                "company": company,
                "url": url,
                "description": desc,
                "tags": tags,
                "location": location,
                "salary": salary,
            }
        )
    return jobs


def fetch_wwr_rss() -> List[Dict]:
    # Simple RSS parsing without extra deps
    r = requests.get(WWR_RSS, headers=UA, timeout=30)
    r.raise_for_status()
    xml = r.text

    # naive parse items
    items = re.findall(r"<item>(.*?)</item>", xml, flags=re.S)
    jobs = []
    for it in items:
        title = re.findall(r"<title><!\[CDATA\[(.*?)\]\]></title>", it, flags=re.S)
        link = re.findall(r"<link>(.*?)</link>", it, flags=re.S)
        desc = re.findall(r"<description><!\[CDATA\[(.*?)\]\]></description>", it, flags=re.S)

        t = title[0] if title else ""
        url = link[0] if link else ""
        d = desc[0] if desc else ""

        # WWR title often like "Company: Role"
        company = ""
        role = t
        if ":" in t:
            company, role = [x.strip() for x in t.split(":", 1)]

        jobs.append(
            {
                "source": "weworkremotely",
                "title": role,
                "company": company,
                "url": url,
                "description": d,
                "tags": "",
                "location": "",  # not reliable in RSS
                "salary": "",
            }
        )
    return jobs

def fetch_greenhouse_board(board: str, company_label: str) -> List[Dict]:
    # Greenhouse Job Board API (public)
    # docs: https://developers.greenhouse.io/job-board.html
    url = f"https://boards-api.greenhouse.io/v1/boards/{board}/jobs?content=true"
    r = requests.get(url, headers=UA, timeout=30)
    r.raise_for_status()
    data = r.json()

    jobs = []
    for item in data.get("jobs", []):
        title = item.get("title") or ""
        # public apply URL comes as absoluteUrl
        job_url = item.get("absoluteUrl") or item.get("url") or ""
        desc = item.get("content") or ""
        location = (item.get("location") or {}).get("name") or ""
        departments = " ".join([d.get("name","") for d in (item.get("departments") or [])])
        offices = " ".join([o.get("name","") for o in (item.get("offices") or [])])

        jobs.append({
            "source": f"greenhouse:{board}",
            "title": title,
            "company": company_label,
            "url": job_url,
            "description": desc,
            "tags": " ".join([departments, offices]).strip(),
            "location": location,
            "salary": "",  # Greenhouse usualmente no publica salary en el API
        })
    return jobs

# ---------------------------
# Filtering / Scoring
# ---------------------------
def job_text(j: Dict) -> str:
    return " ".join([j.get("title",""), j.get("company",""), j.get("location",""), j.get("tags",""), j.get("description",""), j.get("salary","")])


def passes_filters(j: Dict) -> Tuple[bool, List[str]]:
    title = norm(j.get("title",""))
    role_ok = any(x in title for x in [
        "backend", "back-end", "platform", "server", "distributed",
        "software engineer", "java", "api", "systems", "infrastructure"
    ])
    if not role_ok:
        return False, ["title_not_backend_platform"]

    reasons = []
    text = job_text(j)

    # Must have (at least one)
    if MUST_HAVE and not contains_any(text, MUST_HAVE):
        return False, ["missing_must_have"]

    # Negative keywords
    if NEGATIVE_KEYWORDS and contains_any(text, NEGATIVE_KEYWORDS):
        return False, ["negative_keyword"]

    # Remote / geo: allow LATAM or global or EU remote
    geo_ok = contains_any(text, ALLOW_LATAM_HINTS) or contains_any(text, ALLOW_EU_HINTS)
    # Many postings don't explicitly say LATAM; still, if "remote" is present, we keep it
    if not geo_ok and "remote" not in norm(text):
        return False, ["not_remote_or_geo_unclear"]

    # Currency hint
    if not contains_any(text, CURRENCY_HINTS):
        # Not a hard fail; just warn (some sources omit salary)
        reasons.append("no_currency_hint")

    # Salary threshold (only if parsable USD)
    sal_usd = parse_salary_to_usd(j.get("salary",""))
    if sal_usd is not None and MIN_USD > 0 and sal_usd < MIN_USD:
        return False, [f"salary_below_min({sal_usd}<{MIN_USD})"]

    return True, reasons


def score_job(j: Dict) -> int:
    text = norm(job_text(j))
    score = 0

    # match keywords
    for k in KEYWORDS:
        if norm(k) in text:
            score += 3

    # extra boosts
    boosts = ["senior", "staff", "principal", "backend", "platform", "distributed", "microservices", "kubernetes", "gcp", "aws"]
    for b in boosts:
        if b in text:
            score += 2

    # salary present
    if j.get("salary"):
        score += 2

    # "latam" or "worldwide"
    if contains_any(text, ["latam", "latin america", "worldwide", "global"]):
        score += 3

    return score


# ---------------------------
# Notifications
# ---------------------------
def telegram_send(message: str) -> None:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": message, "disable_web_page_preview": True}
    r = requests.post(url, json=payload, timeout=30)
    r.raise_for_status()


def email_send(subject: str, body: str) -> None:
    if not (SMTP_HOST and SMTP_USER and SMTP_PASS and EMAIL_TO and EMAIL_FROM):
        return
    import smtplib
    from email.mime.text import MIMEText

    msg = MIMEText(body, "plain", "utf-8")
    msg["Subject"] = subject
    msg["From"] = EMAIL_FROM
    msg["To"] = EMAIL_TO

    with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=30) as s:
        s.starttls()
        s.login(SMTP_USER, SMTP_PASS)
        s.sendmail(EMAIL_FROM, [EMAIL_TO], msg.as_string())


def format_digest(jobs: List[Dict]) -> str:
    lines = []
    lines.append(f"Daily Remote Jobs Digest ({len(jobs)} new)\n")
    for j in jobs[:MAX_RESULTS]:
        salary = j.get("salary","").strip()
        loc = j.get("location","").strip()
        extra = []
        if salary:
            extra.append(salary)
        if loc:
            extra.append(loc)
        extra_txt = (" | " + " | ".join(extra)) if extra else ""
        lines.append(f"- {j['title']} @ {j['company']}{extra_txt}\n  {j['url']}")
    return "\n".join(lines)


# ---------------------------
# Main
# ---------------------------
def main() -> int:
    all_jobs: List[Dict] = []

    # Fetch with best-effort (don’t fail whole run if one source fails)
    for fn in (
        fetch_remoteok,
        fetch_remotive,
        fetch_wwr_rss,
        lambda: fetch_greenhouse_board("gitlab", "GitLab"),
        lambda: fetch_greenhouse_board("elastic", "Elastic"),
        ):
        try:
            all_jobs.extend(fn())
        except Exception as e:
            print(f"[warn] source failed: {fn.__name__}: {e}", file=sys.stderr)

    # Filter + score
    candidates = []
    for j in all_jobs:
        ok, reasons = passes_filters(j)
        if not ok:
            continue
        j["_score"] = score_job(j)
        j["_reasons"] = reasons
        candidates.append(j)

    candidates.sort(key=lambda x: x["_score"], reverse=True)

    # Dedup + persist
    conn = sqlite3.connect(DB_PATH)
    db_init(conn)

    new_jobs = []
    for j in candidates:
        job_id = sha_id(j.get("source",""), j.get("url",""), j.get("title",""), j.get("company",""))
        if db_is_new(conn, job_id):
            db_mark_seen(conn, job_id, j["source"], j["title"], j["company"], j["url"])
            new_jobs.append(j)

    if not new_jobs:
        print("No new matching jobs today.")
        return 0

    digest = format_digest(new_jobs)

    # Send notifications
    try:
        telegram_send(digest)
    except Exception as e:
        print(f"[warn] telegram failed: {e}", file=sys.stderr)

    try:
        email_send(subject=f"Remote Jobs Digest ({len(new_jobs)} new)", body=digest)
    except Exception as e:
        print(f"[warn] email failed: {e}", file=sys.stderr)

    # Also print to logs
    print(digest)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())