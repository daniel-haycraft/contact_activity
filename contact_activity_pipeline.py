import argparse
import json
import os
import re
import sys
import threading
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from html import unescape

import requests

import requests
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(Path(__file__).with_name(".env"))

HUBSPOT_TOKEN = os.environ["HUBSPOT_TOKEN"]
INPUT_FILE = "contacts.xlsx"           # Excel with an email column
EMAIL_COLUMN = "E-mail Address"                 # None = auto-detect a column named like "email"
OUTPUT_FILE = "contact_activities.json"
CHUNK_SIZE = 25                               # contacts per HubSpot batch; file saved after each
HUBSPOT_REQUESTS_PER_10S = 90                 # private app limit is 100 per 10 seconds
TRANSCRIPT_WORKERS = 6                        # parallel HubSpot transcript fetches
TRANSCRIPT_GIVE_UP_AFTER = 25                 # stop trying transcripts after this many straight failures
KEEP_SAKARI_RAW = True     

# Sakari (hub.sakari.io > cog icon > API Credentials)
SAKARI_CLIENT_ID = os.environ["SAKARI_CLIENT_ID"]
SAKARI_CLIENT_SECRET = os.environ["SAKARI_CLIENT_SECRET"]
SAKARI_ACCOUNT_ID = os.environ["SAKARI_ACCOUNT_ID"]
SAKARI_PAGE_SIZE = 100

"""
contact_activity_export.py

Builds one JSON file containing the complete activity history for a list of
HubSpot contacts:

  Stage 1  HubSpot   calls (with HubSpot transcripts where available), emails,
                     notes, and HubSpot-logged SMS
  Stage 2  Sakari    every text sent or received through Sakari

Setup (one time):
    pip install requests pandas openpyxl

Run:
    python contact_activity_export.py                  # both stages
    python contact_activity_export.py --stage hubspot  # HubSpot only
    python contact_activity_export.py --stage sakari   # Sakari only, into an existing JSON
    python contact_activity_export.py --sakari-sample  # print one raw Sakari message and exit
    python contact_activity_export.py --discover       # list object types attached to the first contact and exit
    python contact_activity_export.py --test-transcript  # show HubSpot's raw reply for one transcript and exit

The JSON is saved after every batch, so it can be opened while the script runs.
"""






# ----------------------------------------------------------------------------
# Shared helpers
# ----------------------------------------------------------------------------
_t0 = time.time()


def log(msg):
    print(f"[{time.time() - _t0:7.1f}s] {msg}", flush=True)


def fmt(seconds):
    seconds = int(seconds)
    m, s = divmod(seconds, 60)
    h, m = divmod(m, 60)
    return f"{h}h {m:02d}m" if h else f"{m}m {s:02d}s"


def chunks(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def load_json(path):
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def save_json(data, path):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    os.replace(tmp, path)


def ms_to_iso(ms):
    if ms in (None, ""):
        return None
    try:
        return datetime.fromtimestamp(int(ms) / 1000, tz=timezone.utc).isoformat()
    except (ValueError, TypeError):
        return str(ms)


def strip_html(html):
    if not html:
        return None
    text = re.sub(r"<(br|/p|/div|/tr|/li|/h\d)[^>]*>", "\n", html, flags=re.I)
    text = re.sub(r"<[^>]+>", "", text)
    text = unescape(text).replace("\u2028", "\n").replace("\u2029", "\n\n")
    text = re.sub(r"[ \t]+\n", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip() or None


def to_e164(raw):
    if not raw:
        return None
    digits = re.sub(r"\D", "", str(raw))
    if len(digits) == 10:
        digits = "1" + digits
    return "+" + digits if 11 <= len(digits) <= 15 else None


def sort_activities(acts):
    acts.sort(key=lambda a: str(a.get("timestamp") or ""))


# ============================================================================
# STAGE 1: HUBSPOT
# ============================================================================
HS_BASE = "https://api.hubapi.com"
HS_HEADERS = {"Authorization": f"Bearer {HUBSPOT_TOKEN}", "Content-Type": "application/json"}

CONTACT_PROPS = ["email", "firstname", "lastname", "phone", "mobilephone", "hubspot_owner_id",
                 "lifecyclestage", "createdate"]

# object type -> properties to pull
ENGAGEMENTS = {
    "calls": ["hs_timestamp", "hs_call_title", "hs_call_body", "hs_call_direction", "hs_call_disposition",
              "hs_call_duration", "hs_call_status", "hs_call_recording_url", "hs_call_from_number",
              "hs_call_to_number", "hs_call_has_transcript", "hs_call_transcription_id", "hubspot_owner_id"],
    "emails": ["hs_timestamp", "hs_email_subject", "hs_email_text", "hs_email_html", "hs_email_direction",
               "hs_email_status", "hs_email_from_email", "hs_email_to_email", "hs_email_cc_email",
               "hubspot_owner_id"],
    "notes": ["hs_timestamp", "hs_note_body", "hubspot_owner_id"],
    "communications": ["hs_timestamp", "hs_communication_channel_type", "hs_communication_body",
                       "hs_communication_logged_from", "hubspot_owner_id"],
}

TRANSCRIPT_ENDPOINTS = [
    "/crm/extensions/calling/2026-09/transcripts/{id}",
    "/crm/extensions/calling/2026-03/transcripts/{id}",
    "/crm/v3/extensions/calling/transcripts/{id}",
]

_owners = {}
_dispositions = {}
_transcripts_disabled = False
_transcript_endpoint = None   # once one version works, stick with it


class RateLimiter:
    def __init__(self, max_calls, period):
        self.max_calls, self.period = max_calls, period
        self.calls, self.lock = deque(), threading.Lock()

    def wait(self):
        while True:
            with self.lock:
                now = time.monotonic()
                while self.calls and now - self.calls[0] > self.period:
                    self.calls.popleft()
                if len(self.calls) < self.max_calls:
                    self.calls.append(now)
                    return
                sleep_for = self.period - (now - self.calls[0]) + 0.01
            time.sleep(max(sleep_for, 0.01))


_hs_limiter = RateLimiter(HUBSPOT_REQUESTS_PER_10S, 10)
_hs_session = requests.Session()


def hs_request(method, path, **kwargs):
    for attempt in range(6):
        _hs_limiter.wait()
        r = _hs_session.request(method, HS_BASE + path, headers=HS_HEADERS, timeout=60, **kwargs)
        if r.status_code == 429:
            wait = float(r.headers.get("Retry-After", 2))
            log(f"    rate limited by HubSpot, waiting {wait:.0f}s")
            time.sleep(wait)
            continue
        if r.status_code >= 500:
            log(f"    HubSpot {r.status_code}, retry {attempt + 1}/6 ({method} {path})")
            time.sleep(1.5 * (attempt + 1))
            continue
        if r.status_code >= 400:
            raise RuntimeError(f"{method} {path} -> {r.status_code}: {r.text[:400]}")
        return r.json() if r.text else {}
    raise RuntimeError(f"Gave up on {method} {path}")


def load_emails(path, column):
    import pandas as pd
    df = pd.read_excel(path)
    if column is None:
        matches = [c for c in df.columns if "email" in str(c).lower()]
        if not matches:
            raise ValueError(f"No email column found. Columns: {list(df.columns)}")
        column = matches[0]
    emails = df[column].dropna().astype(str).str.strip().str.lower()
    return list(dict.fromkeys(e for e in emails if "@" in e))


def fetch_contacts(emails):
    found = {}
    batches = list(chunks(emails, 100))
    for n, batch in enumerate(batches, 1):
        log(f"  contact lookup {n}/{len(batches)}")
        data = hs_request("POST", "/crm/v3/objects/contacts/batch/read", json={
            "idProperty": "email", "inputs": [{"id": e} for e in batch], "properties": CONTACT_PROPS})
        for r in data.get("results", []):
            p = r.get("properties", {})
            found[(p.get("email") or "").lower()] = {"id": r["id"], "props": p}
    return found


def preload_owners():
    log("Loading owners")
    after = None
    while True:
        params = {"limit": 500, **({"after": after} if after else {})}
        try:
            data = hs_request("GET", "/crm/v3/owners", params=params)
        except Exception as e:
            log(f"  could not load owners: {str(e)[:120]}")
            return
        for o in data.get("results", []):
            name = f"{o.get('firstName', '')} {o.get('lastName', '')}".strip() or o.get("email")
            _owners[str(o["id"])] = {"id": str(o["id"]), "name": name, "email": o.get("email")}
        after = data.get("paging", {}).get("next", {}).get("after")
        if not after:
            break
    log(f"  {len(_owners)} owners")


def preload_dispositions():
    log("Loading call outcome labels")
    try:
        _dispositions.update({d["id"]: d["label"] for d in hs_request("GET", "/calling/v1/dispositions")})
        log(f"  {len(_dispositions)} outcomes")
    except Exception as e:
        log(f"  could not load outcomes, will show raw IDs ({str(e)[:80]})")


def owner(owner_id):
    if not owner_id:
        return None
    return _owners.get(str(owner_id), {"id": str(owner_id), "name": None, "email": None})


def fetch_associations(contact_ids, to_object):
    """{contact_id: [engagement ids]} for many contacts in as few calls as possible."""
    out = {str(c): [] for c in contact_ids}
    for batch in chunks(contact_ids, 100):
        log(f"  {to_object}: association lookup for {len(batch)} contacts")
        data = hs_request("POST", f"/crm/v4/associations/contacts/{to_object}/batch/read",
                          json={"inputs": [{"id": str(c)} for c in batch]})
        for r in data.get("results", []):
            cid = str(r["from"]["id"])
            out[cid].extend(str(t["toObjectId"]) for t in r.get("to", []))
            after = r.get("paging", {}).get("next", {}).get("after")
            if after:
                log(f"    contact {cid} has 500+ {to_object}, paging")
            while after:
                page = hs_request("GET", f"/crm/v4/objects/contacts/{cid}/associations/{to_object}",
                                  params={"limit": 500, "after": after})
                out[cid].extend(str(t["toObjectId"]) for t in page.get("results", []))
                after = page.get("paging", {}).get("next", {}).get("after")
    log(f"    {sum(len(v) for v in out.values())} {to_object} found")
    return out


def fetch_records(object_type, ids, properties):
    results = {}
    batches = list(chunks(list(dict.fromkeys(ids)), 100))
    for n, batch in enumerate(batches, 1):
        log(f"  {object_type}: reading {n}/{len(batches)} ({len(batch)} records)")
        data = hs_request("POST", f"/crm/v3/objects/{object_type}/batch/read",
                          json={"inputs": [{"id": i} for i in batch], "properties": properties})
        for r in data.get("results", []):
            results[str(r["id"])] = r
    return results


def fetch_transcript(transcript_id):
    global _transcript_endpoint
    endpoints = [_transcript_endpoint] if _transcript_endpoint else TRANSCRIPT_ENDPOINTS
    last_err = None
    for ep in endpoints:
        try:
            data = hs_request("GET", ep.format(id=transcript_id))
            utterances = []
            for u in data.get("transcriptUtterances", []):
                spk = u.get("speaker") or {}
                utterances.append({"speaker": spk.get("name") or spk.get("id"),
                                   "start_ms": u.get("startTimeMillis"), "end_ms": u.get("endTimeMillis"),
                                   "text": u.get("text")})
            text = "\n".join(f"{u['speaker'] or 'Speaker'}: {u['text']}" for u in utterances if u.get("text"))
            _transcript_endpoint = ep
            return {"id": transcript_id, "text": text or None, "utterances": utterances, "error": None}
        except Exception as e:
            last_err = str(e)[:200]
    return {"id": transcript_id, "text": None, "utterances": [], "error": last_err}


def normalize(object_type, rec):
    p = rec.get("properties", {})
    base = {"id": rec["id"], "source": "hubspot", "timestamp": ms_to_iso(p.get("hs_timestamp")),
            "owner": owner(p.get("hubspot_owner_id"))}

    if object_type == "calls":
        dur = p.get("hs_call_duration")
        base.update({
            "type": "call",
            "title": p.get("hs_call_title"),
            "direction": p.get("hs_call_direction"),
            "status": p.get("hs_call_status"),
            "outcome": _dispositions.get(p.get("hs_call_disposition"), p.get("hs_call_disposition")),
            "duration_seconds": round(int(dur) / 1000) if dur else None,
            "from_number": p.get("hs_call_from_number"),
            "to_number": p.get("hs_call_to_number"),
            "recording_url": p.get("hs_call_recording_url"),
            "notes": strip_html(p.get("hs_call_body")),
            "has_transcript": str(p.get("hs_call_has_transcript")).lower() == "true",
            "transcript_id": p.get("hs_call_transcription_id"),
            "transcript": None,
        })
    elif object_type == "emails":
        base.update({
            "type": "email",
            "subject": p.get("hs_email_subject"),
            "direction": p.get("hs_email_direction"),
            "status": p.get("hs_email_status"),
            "from": p.get("hs_email_from_email"),
            "to": p.get("hs_email_to_email"),
            "cc": p.get("hs_email_cc_email"),
            "body": p.get("hs_email_text") or strip_html(p.get("hs_email_html")),
        })
    elif object_type == "notes":
        base.update({"type": "note", "body": strip_html(p.get("hs_note_body"))})
    elif object_type == "communications":
        channel = (p.get("hs_communication_channel_type") or "").upper()
        base.update({
            "type": "sms" if channel == "SMS" else (channel.lower() or "communication"),
            "channel": channel or None,
            "logged_from": p.get("hs_communication_logged_from"),
            "body": strip_html(p.get("hs_communication_body")),
        })
    return base


def fetch_transcripts(calls):
    global _transcripts_disabled
    if _transcripts_disabled:
        log("Step 4: transcripts skipped (API not returning them)")
        return
    if not calls:
        log("Step 4: no calls with transcripts in this chunk")
        return
    log(f"Step 4: fetching {len(calls)} HubSpot transcripts")
    ok = failed = 0
    with ThreadPoolExecutor(max_workers=TRANSCRIPT_WORKERS) as pool:
        for i in range(0, len(calls), TRANSCRIPT_WORKERS):
            group = calls[i:i + TRANSCRIPT_WORKERS]
            for act, tr in zip(group, pool.map(lambda a: fetch_transcript(a["transcript_id"]), group)):
                act["transcript"] = tr
                if tr.get("error"):
                    failed += 1
                else:
                    ok += 1
            done = i + len(group)
            if done % 30 < TRANSCRIPT_WORKERS or done == len(calls):
                log(f"    transcripts {done}/{len(calls)} (ok {ok}, failed {failed})")
            if ok == 0 and failed >= TRANSCRIPT_GIVE_UP_AFTER:
                log(f"    {failed} failed with no successes. Last error: {tr.get('error')}")
                log("    Skipping transcripts for the rest of the run.")
                _transcripts_disabled = True
                for a in calls[done:]:
                    a["transcript"] = {"id": a["transcript_id"], "text": None, "utterances": [], "error": "skipped"}
                return


def process_chunk(records):
    contact_ids = [c["id"] for _, c in records]

    log("Step 1: finding activities")
    assoc = {t: fetch_associations(contact_ids, t) for t in ENGAGEMENTS}

    log("Step 2: reading activity content")
    objects = {}
    for t, props in ENGAGEMENTS.items():
        ids = [i for lst in assoc[t].values() for i in lst]
        objects[t] = fetch_records(t, ids, props) if ids else {}

    log("Step 3: assembling per contact")
    entries, calls_with_transcripts = [], []
    for email, c in records:
        p = c["props"]
        activities, counts = [], {}
        for t in ENGAGEMENTS:
            ids = assoc[t].get(str(c["id"]), [])
            counts[t] = len(ids)
            for i in ids:
                rec = objects[t].get(i)
                if not rec:
                    continue
                act = normalize(t, rec)
                if t == "calls" and act["transcript_id"]:
                    calls_with_transcripts.append(act)
                activities.append(act)
        sort_activities(activities)
        entries.append({
            "email": email, "found": True, "contact_id": c["id"],
            "first_name": p.get("firstname"), "last_name": p.get("lastname"),
            "phone": p.get("phone"), "mobile_phone": p.get("mobilephone"),
            "lifecycle_stage": p.get("lifecyclestage"),
            "contact_created": ms_to_iso(p.get("createdate")),
            "owner": owner(p.get("hubspot_owner_id")),
            "activity_counts": counts, "activity_count": len(activities),
            "activities": activities,
        })

    fetch_transcripts(calls_with_transcripts)
    return entries


def test_transcript(args):
    """Find one call with a transcript ID (from the JSON if it exists, otherwise from the
    first matched contact) and print HubSpot's raw response for each endpoint version."""
    transcript_id = call_id = None
    if os.path.exists(args.output):
        for c in load_json(args.output).get("contacts", []):
            for a in c.get("activities", []):
                if a.get("type") == "call" and a.get("transcript_id"):
                    transcript_id, call_id = a["transcript_id"], a["id"]
                    break
            if transcript_id:
                break
    if not transcript_id:
        emails = load_emails(args.input, args.column)
        contacts = fetch_contacts(emails)
        for e, c in contacts.items():
            ids = fetch_associations([c["id"]], "calls")[str(c["id"])]
            for rec in fetch_records("calls", ids, ENGAGEMENTS["calls"]).values():
                tid = rec["properties"].get("hs_call_transcription_id")
                if tid:
                    transcript_id, call_id = tid, rec["id"]
                    break
            if transcript_id:
                break
    if not transcript_id:
        print("Could not find any call with a transcript ID.")
        return
    print(f"\nCall {call_id}, transcript_id {transcript_id}\n")
    for ep in TRANSCRIPT_ENDPOINTS:
        path = ep.format(id=transcript_id)
        _hs_limiter.wait()
        r = _hs_session.get(HS_BASE + path, headers=HS_HEADERS, timeout=60)
        print(f"GET {path}")
        print(f"  status:        {r.status_code}")
        print(f"  final url:     {r.url}")
        print(f"  redirects:     {[(h.status_code, h.url) for h in r.history] or 'none'}")
        print(f"  content-type:  {r.headers.get('Content-Type')}")
        print(f"  correlation:   {r.headers.get('X-HubSpot-Correlation-Id') or r.headers.get('X-Trace') or 'none'}")
        print(f"  scopes ok:     {'yes' if 'crm.extensions_calling_transcripts.read' not in r.text else 'MISSING'}")
        print(f"  body:          {r.text[:300].strip()}\n")
    # sanity check: a known-good endpoint with the same token and session
    _hs_limiter.wait()
    r = _hs_session.get(HS_BASE + f"/crm/v3/objects/calls/{call_id}", headers=HS_HEADERS, timeout=60,
                        params={"properties": "hs_call_has_transcript,hs_call_transcription_id"})
    print(f"Control request GET /crm/v3/objects/calls/{call_id}")
    print(f"  status:        {r.status_code}")
    print(f"  final url:     {r.url}")
    print(f"  content-type:  {r.headers.get('Content-Type')}")
    print(f"  body:          {r.text[:300].strip()}\n")


def discover(contact_id):
    types = list(ENGAGEMENTS) + ["tasks", "meetings", "deals", "tickets"]
    try:
        types += [s["objectTypeId"] for s in hs_request("GET", "/crm/v3/schemas").get("results", [])]
    except Exception as e:
        log(f"could not list custom objects: {str(e)[:100]}")
    print(f"\nObject types attached to contact {contact_id}:")
    for t in types:
        try:
            n = len(fetch_associations([contact_id], t)[str(contact_id)])
            print(f"  {t:<24} {n}")
        except Exception as e:
            print(f"  {t:<24} error: {str(e)[:80]}")


def stage_hubspot(args):
    if HUBSPOT_TOKEN.startswith("PASTE_"):
        sys.exit("Set HUBSPOT_TOKEN in the CONFIG block.")
    start = time.time()
    log("========== STAGE 1: HUBSPOT ==========")
    emails = load_emails(args.input, args.column)
    log(f"{len(emails)} unique emails in {args.input}")

    contacts = fetch_contacts(emails)
    missing = [e for e in emails if e not in contacts]
    log(f"Matched {len(contacts)} contacts, {len(missing)} not found")
    for e in missing[:20]:
        log(f"    not found: {e}")
    if len(missing) > 20:
        log(f"    ... and {len(missing) - 20} more")

    if args.discover:
        if not contacts:
            sys.exit("No contacts matched.")
        discover(next(iter(contacts.values()))["id"])
        sys.exit(0)

    preload_owners()
    preload_dispositions()

    output = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "status": "in_progress",
        "stages_completed": [],
        "contact_count": len(emails),
        "contacts": [{"email": e, "found": False, "activity_count": 0, "activities": []} for e in missing],
    }
    save_json(output, args.output)
    log(f"Created {args.output}, saves after every {CHUNK_SIZE} contacts")

    found = [(e, contacts[e]) for e in emails if e in contacts]
    if args.limit:
        found = found[:args.limit]
    batches = list(chunks(found, CHUNK_SIZE))
    done = 0
    for n, batch in enumerate(batches, 1):
        log(f"==== Chunk {n}/{len(batches)}: contacts {done + 1}-{done + len(batch)} of {len(found)} ====")
        entries = process_chunk(batch)
        output["contacts"].extend(entries)
        done += len(batch)
        save_json(output, args.output)
        total = sum(c["activity_count"] for c in output["contacts"])
        log(f"Saved. {done}/{len(found)} contacts, {total} activities total, {fmt(time.time() - start)} elapsed")

    order = {e: i for i, e in enumerate(emails)}
    output["contacts"].sort(key=lambda c: order.get(c["email"], 10 ** 9))
    output["stages_completed"].append("hubspot")
    save_json(output, args.output)

    with_tr = sum(1 for c in output["contacts"] for a in c["activities"]
                  if a.get("type") == "call" and (a.get("transcript") or {}).get("text"))
    log(f"HubSpot stage done in {fmt(time.time() - start)}. Transcripts retrieved: {with_tr}")


# ============================================================================
# STAGE 2: SAKARI
# ============================================================================
SK_BASE = "https://api.sakari.io"
SK_PAGE = 100
_sk_token = None
_sk_session = requests.Session()


def sk_auth():
    global _sk_token
    r = _sk_session.post(f"{SK_BASE}/oauth2/token", timeout=30, json={
        "grant_type": "client_credentials", "client_id": SAKARI_CLIENT_ID, "client_secret": SAKARI_CLIENT_SECRET})
    if r.status_code != 200:
        sys.exit(f"Sakari auth failed: {r.status_code} {r.text[:300]}")
    _sk_token = r.json()["access_token"]


def sk_get(path, params=None):
    for attempt in range(6):
        r = _sk_session.get(f"{SK_BASE}{path}", params=params, timeout=60,
                            headers={"Authorization": f"Bearer {_sk_token}"})
        if r.status_code == 401 and attempt == 0:
            sk_auth()
            continue
        if r.status_code == 429:
            wait = float(r.headers.get("Retry-After", 3))
            log(f"    rate limited by Sakari, waiting {wait:.0f}s")
            time.sleep(wait)
            continue
        if r.status_code >= 500:
            time.sleep(1.5 * (attempt + 1))
            continue
        if r.status_code >= 400:
            raise RuntimeError(f"GET {path} -> {r.status_code}: {r.text[:300]}")
        return r.json()
    raise RuntimeError(f"Gave up on GET {path}")


def cf1_numbers(contacts, min_contacts=3):
    """Numbers appearing on calls for several different contacts are CF1's own lines."""
    seen = {}
    for c in contacts:
        nums = set()
        for a in c.get("activities", []):
            if a.get("type") == "call":
                for k in ("from_number", "to_number"):
                    p = to_e164(a.get(k))
                    if p:
                        nums.add(p)
        for p in nums:
            seen[p] = seen.get(p, 0) + 1
    return {p for p, n in seen.items() if n >= min_contacts}


def contact_phones(c, exclude):
    phones = set()
    for k in ("phone", "mobile_phone"):
        p = to_e164(c.get(k))
        if p:
            phones.add(p)
    for a in c.get("activities", []):
        if a.get("type") != "call":
            continue
        d = (a.get("direction") or "").upper()
        k = "to_number" if d == "OUTBOUND" else "from_number" if d == "INBOUND" else None
        p = to_e164(a.get(k)) if k else None
        if p:
            phones.add(p)
    return phones - exclude


def sk_find_contacts(phone):
    variants = [phone, phone.lstrip("+")] + ([phone[2:]] if phone.startswith("+1") else [])
    for v in variants:
        try:
            data = sk_get(f"/v1/accounts/{SAKARI_ACCOUNT_ID}/contacts", {"mobile": v, "limit": 10})
        except Exception as e:
            log(f"    lookup error for {v}: {str(e)[:120]}")
            continue
        ids = [c["id"] for c in data.get("data", []) if c.get("id")]
        if ids:
            return ids
    return []


def sk_messages(contact_id):
    msgs, offset = [], 0
    while True:
        data = sk_get(f"/v1/accounts/{SAKARI_ACCOUNT_ID}/messages",
                      {"contactId": contact_id, "offset": offset, "limit": SK_PAGE})
        page = data.get("data", [])
        msgs.extend(page)
        if len(page) < SK_PAGE:
            return msgs
        offset += SK_PAGE


def first(d, *keys):
    for k in keys:
        if isinstance(d, dict) and d.get(k) not in (None, ""):
            return d[k]
    return None


def to_iso(value):
    if value is None:
        return None
    if isinstance(value, dict):
        for k in ("at", "date", "timestamp", "time", "value"):
            if value.get(k) is not None:
                return to_iso(value[k])
        return None
    if isinstance(value, (int, float)):
        return ms_to_iso(value if value > 1e11 else value * 1000)
    return str(value)


def normalize_sakari(m):
    raw_dir = str(first(m, "direction", "type", "messageType") or "").upper()
    direction = ("outbound" if raw_dir in ("MT", "OUTBOUND", "OUT", "SENT")
                 else "inbound" if raw_dir in ("MO", "INBOUND", "IN", "RECEIVED")
                 else raw_dir.lower() or None)
    mobile = (m.get("contact") or {}).get("mobile") or {}
    sent_by = m.get("sentBy") or m.get("user") or {}
    sender = (f"{sent_by.get('firstName', '')} {sent_by.get('lastName', '')}".strip()
              or sent_by.get("email") or sent_by.get("name")) if isinstance(sent_by, dict) else None
    out = {
        "id": f"sakari-{m.get('id')}",
        "sakari_id": m.get("id"),
        "source": "sakari",
        "type": "sms",
        "timestamp": to_iso(first(m, "created", "createdAt", "sent", "sentAt", "timestamp", "date")),
        "direction": direction,
        "status": first(m, "status", "state"),
        "contact_number": mobile.get("number") if isinstance(mobile, dict) else mobile,
        "sent_by": sender,
        "body": first(m, "message", "template", "body", "text", "content"),
        "media": m.get("media") or m.get("attachments") or None,
        "owner": None,
    }
    if KEEP_SAKARI_RAW:
        out["raw"] = m
    return out


def stage_sakari(args):
    if SAKARI_CLIENT_ID.startswith("PASTE_"):
        sys.exit("Fill in the Sakari credentials in the CONFIG block.")
    start = time.time()
    log("========== STAGE 2: SAKARI ==========")
    sk_auth()
    log("Authenticated with Sakari")

    if args.sakari_sample:
        print(json.dumps(sk_get(f"/v1/accounts/{SAKARI_ACCOUNT_ID}/messages", {"limit": 1}), indent=2)[:4000])
        sys.exit(0)

    data = load_json(args.output)
    contacts = [c for c in data["contacts"] if c.get("found")]
    if args.limit:
        contacts = contacts[:args.limit]
    exclude = cf1_numbers(data["contacts"])
    if exclude:
        log(f"Ignoring {len(exclude)} numbers that look like CF1 lines")
    log(f"{len(contacts)} contacts to check")

    total = no_phone = no_match = 0
    for i, c in enumerate(contacts, 1):
        phones = contact_phones(c, exclude)
        if not phones:
            no_phone += 1
            log(f"[{i}/{len(contacts)}] {c['email']}  no phone on record")
            continue
        log(f"[{i}/{len(contacts)}] {c['email']}  {', '.join(sorted(phones))}")
        sids = list(dict.fromkeys(sid for p in phones for sid in sk_find_contacts(p)))
        if not sids:
            no_match += 1
            log("    not in Sakari")
            continue

        have = {a.get("sakari_id") for a in c["activities"] if a.get("source") == "sakari"}
        added = 0
        for sid in sids:
            try:
                for m in sk_messages(sid):
                    if m.get("id") not in have:
                        c["activities"].append(normalize_sakari(m))
                        have.add(m.get("id"))
                        added += 1
            except Exception as e:
                log(f"    error fetching messages for Sakari contact {sid}: {str(e)[:150]}")
        sort_activities(c["activities"])
        c.setdefault("activity_counts", {})["sakari_sms"] = len(have)
        c["activity_count"] = len(c["activities"])
        total += added
        log(f"    {len(sids)} Sakari contact(s), {added} new texts")
        if i % 10 == 0:
            save_json(data, args.output)

    if "sakari" not in data.setdefault("stages_completed", []):
        data["stages_completed"].append("sakari")
    save_json(data, args.output)
    log(f"Sakari stage done in {fmt(time.time() - start)}: {total} texts added, "
        f"{no_phone} contacts had no phone, {no_match} not in Sakari")


# ============================================================================
# MAIN
# ============================================================================
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--stage", choices=["all", "hubspot", "sakari"], default="all")
    ap.add_argument("--input", default=INPUT_FILE)
    ap.add_argument("--column", default=EMAIL_COLUMN)
    ap.add_argument("--output", default=OUTPUT_FILE)
    ap.add_argument("--limit", type=int, help="only process this many contacts (testing)")
    ap.add_argument("--discover", action="store_true")
    ap.add_argument("--sakari-sample", action="store_true")
    ap.add_argument("--test-transcript", action="store_true",
                    help="print HubSpot's raw response for one transcript and exit")
    args = ap.parse_args()

    if args.test_transcript:
        test_transcript(args)
        return

    if args.discover:
        stage_hubspot(args)
    if args.sakari_sample:
        stage_sakari(args)

    stages = ["hubspot", "sakari"] if args.stage == "all" else [args.stage]
    if stages == ["sakari"] and not os.path.exists(args.output):
        sys.exit(f"{args.output} doesn't exist yet. Run the hubspot stage first.")

    for s in stages:
        {"hubspot": stage_hubspot, "sakari": stage_sakari}[s](args)

    data = load_json(args.output)
    if args.stage == "all" and not args.limit:
        data["status"] = "complete"
        save_json(data, args.output)
    log(f"Done in {fmt(time.time() - _t0)}. Output: {args.output}")


if __name__ == "__main__":
    main()