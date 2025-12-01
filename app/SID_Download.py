import os
import re
import csv
import time
import random
import urllib.parse
from datetime import datetime

import requests
from bs4 import BeautifulSoup

BASE = "https://www.sebi.gov.in"
START_URL = (
    "https://www.sebi.gov.in/sebiweb/other/OtherAction.do"
    "?doMutualFund=yes&mftype=2"
)
USER_AGENT = "MF-SID-Downloader/1.0 (research use; contact: you@example.com)"

session = requests.Session()
session.headers["User-Agent"] = USER_AGENT

# ----------------- helpers -----------------

def abs_url(href: str | None) -> str | None:
    if not href:
        return None
    return urllib.parse.urljoin(BASE, href)

def polite_sleep(base=1.0, jitter=0.75):
    """Sleep with jitter to avoid hammering the server."""
    delay = base + random.random() * jitter
    time.sleep(delay)

def get_with_retries(url, max_retries=3, timeout=30):
    """GET with simple retry/backoff."""
    for attempt in range(1, max_retries + 1):
        try:
            resp = session.get(url, timeout=timeout)
            resp.raise_for_status()
            return resp
        except Exception as e:
            if attempt == max_retries:
                print(f"[ERROR] {url} failed after {max_retries} attempts: {e}")
                return None
            backoff = 2 ** (attempt - 1)
            print(f"[WARN] {url} attempt {attempt} failed ({e}), retrying in {backoff}s")
            time.sleep(backoff)

def sanitize_filename(name: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]", "_", name)

# ----------------- robots.txt (very basic) -----------------

def allowed_by_robots(path="/sebiweb/other/OtherAction.do"):
    try:
        r = session.get(f"{BASE}/robots.txt", timeout=10)
        if r.ok:
            txt = r.text.lower()
            if "disallow: /sebiweb/other/" in txt:
                print("[INFO] robots.txt appears to disallow /sebiweb/other/. "
                      "Review before running at scale.")
        return True
    except Exception:
        return True

allowed_by_robots()

# ----------------- 1) fetch AMC -> SID list links -----------------

resp = get_with_retries(START_URL)
if resp is None:
    raise SystemExit("Could not fetch SID AMC list page")

soup = BeautifulSoup(resp.text, "html.parser")

amc_sid_links: list[tuple[str, str]] = []

# Find all <a> whose text looks like "SID (xxx)"
for a in soup.find_all("a"):
    text = a.get_text(strip=True)
    upper = text.upper()
    if not (upper.startswith("SID(") or upper.startswith("SID (")):
        continue

    href = abs_url(a.get("href"))
    if not href:
        continue

    # Try to infer AMC name from nearby text
    amc_name = None

    # 1) previous sibling text, if any
    sib = a.previous_sibling
    while sib and (not isinstance(sib, str) or not sib.strip()):
        sib = sib.previous_sibling
    if isinstance(sib, str) and sib.strip():
        amc_name = sib.strip()

    # 2) fall back to parent text (strip out SID part)
    if not amc_name:
        parent = a.parent
        if parent:
            full = parent.get_text(" ", strip=True)
            # e.g. "ICICI Prudential Mutual Fund SID (938)"
            parts = full.rsplit("SID", 1)
            if parts:
                amc_name = parts[0].strip()

    if not amc_name:
        continue

    amc_sid_links.append((amc_name, href))

print(f"[INFO] Found {len(amc_sid_links)} AMCs with SID links")

# ----------------- 2) crawl each AMC SID page & collect PDF URLs -----------------

sid_urls: set[tuple[str, str]] = set()  # (amc_name, url)

for amc_name, amc_url in amc_sid_links:
    print(f"[AMC] {amc_name} -> {amc_url}")
    polite_sleep()

    res = get_with_retries(amc_url)
    if res is None:
        continue

    ctype = res.headers.get("Content-Type", "").lower()

    # Some AMCs might link directly to a single consolidated SID PDF
    if "application/pdf" in ctype or amc_url.lower().endswith(".pdf"):
        sid_urls.add((amc_name, amc_url))
        print("  [PDF] AMC URL is a PDF (single SID bundle)")
        continue

    sub = BeautifulSoup(res.text, "html.parser")

    for a in sub.find_all("a"):
        href = a.get("href", "")
        text = a.get_text(" ", strip=True)
        url = abs_url(href)
        if not url:
            continue

        lower_url = url.lower()
        lower_text = text.lower()

        # Heuristics: target links which clearly look like SIDs
        if lower_url.endswith(".pdf"):
            if ("scheme information document" in lower_text
                or "sid" in lower_text
                or "scheme-information-document" in lower_url
                or "sid-" in lower_url):
                sid_urls.add((amc_name, url))
        elif "scheme information document" in lower_text:
            # Sometimes text says SID but the URL lacks .pdf suffix
            sid_urls.add((amc_name, url))

print(f"[INFO] Total unique SID URLs collected: {len(sid_urls)}")

# ----------------- 3) download PDFs + metadata logging -----------------

os.makedirs("sid_all", exist_ok=True)
meta_path = os.path.join("sid_all", "sid_metadata.csv")

# create metadata file with header if not exists
if not os.path.exists(meta_path):
    with open(meta_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "amc_name",
            "sid_url",
            "local_filename",
            "download_timestamp_utc"
        ])

# Load already-downloaded file names to skip duplicates
downloaded = set(os.listdir("sid_all"))

with open(meta_path, "a", newline="", encoding="utf-8") as f_meta:
    writer = csv.writer(f_meta)

    for amc_name, url in sorted(sid_urls, key=lambda x: (x[0].lower(), x[1])):
        parsed = urllib.parse.urlparse(url)
        fname = urllib.parse.unquote(os.path.basename(parsed.path) or "sid.pdf")
        if not fname.lower().endswith(".pdf"):
            fname += ".pdf"

        # prepend AMC for easier grouping and uniqueness
        prefix = sanitize_filename(amc_name)[:40]
        safe_fname = sanitize_filename(f"{prefix}__{fname}")
        if safe_fname in downloaded:
            continue

        local_path = os.path.join("sid_all", safe_fname)
        print(f"[DL] {amc_name}: {url} -> {safe_fname}")
        polite_sleep(base=1.0, jitter=1.5)

        resp = get_with_retries(url, timeout=60)
        if resp is None or not resp.ok:
            print(f"  [FAIL] {url}")
            continue

        ctype = resp.headers.get("Content-Type", "").lower()
        if "pdf" not in ctype:
            print(f"  [WARN] Content-Type not PDF ({ctype}) for {url}, saving anyway")

        try:
            with open(local_path, "wb") as f_out:
                for chunk in resp.stream(1024 * 64):
                    if not chunk:
                        break
                    f_out.write(chunk)
            downloaded.add(safe_fname)

            writer.writerow([
                amc_name,
                url,
                safe_fname,
                datetime.utcnow().isoformat()
            ])
            f_meta.flush()
        except Exception as e:
            print(f"  [ERROR] writing {safe_fname}: {e}")
            if os.path.exists(local_path):
                os.remove(local_path)
