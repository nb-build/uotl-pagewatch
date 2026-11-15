#!/usr/bin/env python3
"""Quiet-change detection: nightly text snapshots + sha256 of every evidence URL, with diffs.

Two ways in, same storage and diff logic:

  run     Fetch the URLs directly (urllib). Respects robots.txt (and Crawl-delay), one request
          per host every --delay seconds, generic user agent, 3 MB cap, public pages only.
            python3 snapshot.py run --baseline ../uses/coverage_baseline.csv --root snapshots \
                [--shard 3/7] [--limit 10] [--date 2026-11-11]
  ingest  For agents whose shell cannot reach the web: store text the agent already fetched with
          its own web tool. Accepts Parallel web_fetch JSON (full_content=true) or an inbox dir of
          {url, text, fetched_via, acquired_at} JSON files. robots.txt is then the fetch tool's job
          (recorded as "delegated"). The capture date is the tool's acquired_at, not today.
            python3 snapshot.py ingest --parallel-json fetch.json --baseline ... --root snapshots
  selftest  Local HTTP server test (robots, change, confirm, flip-back, unchanged, sha files).

Output per run date D (D = --date or today):
  snapshots/D/<id>.txt        normalized text (id = first 16 hex of sha256(url))
  snapshots/D/<id>.sha256     "<sha256>  <id>.txt"
  snapshots/D/manifest.json   url, id, sha256, bytes, fetched_via, acquired_at, robots, http status
  snapshots/D/changed_pages.json   what changed vs. the last stored copy, with diffs and flags
  snapshots/state.json        stable sha per URL, pending change, sha history (for flip-back)

A change is "new_change" the first time it is seen and "confirmed" when the new text is still
there on a capture at least 20 hours later (filters A/B tests and CDN churn). Going back to an
older sha is "flip_back". Pages that cannot be fetched are listed, never guessed.
"""
import argparse, csv, difflib, hashlib, html.parser, json, re, sys, threading, time, urllib.error, urllib.request
import urllib.robotparser, random
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urlparse

UA = "UOTL-PageWatch/0.1 (public help-page change monitor; low rate)"
MAX_BYTES = 3_000_000
CONFIRM_HOURS = 20
SIGNAL_PATTERNS = {
    "default": r"\bby default\b|\bdefault(s|ed)? (to|is|on|off)\b|\bturned (on|off) automatically\b|\btoggle(d)? (on|off)\b",
    "maturity": r"\b(beta|alpha|early access|coming soon|limited (release|availability)|rolling out|pilot|preview)\b",
    "plan_gate": r"\b(plan|tier|add-?on|upgrade|subscription|license)\b|\b(available|included) (on|in|with)\b",
    "removal": r"\b(no longer|not available|unavailable|deprecat\w*|remov\w*|sunset\w*|discontinu\w*|retir\w*|does not support|can't|cannot)\b",
    "limit": r"\b\d[\d,]*\s*(per|/|a|each)\s*(month|day|week|hour|minute|user|contact|message|text|call|review|location)s?\b|\b(limit|cap|quota|maximum|up to)\b",
    "price": r"\$\s?\d",
}
VOLATILE = [r"\b\d+\s+(seconds?|minutes?|hours?|days?)\s+ago\b", r"©\s*\d{4}", r"\bnonce=[\w-]+", r"\bcsrf[\w-]*\b[=:]\s*\S+"]


# ---------------------------------------------------------------------------------------------
# text extraction and normalization
# ---------------------------------------------------------------------------------------------
class _Text(html.parser.HTMLParser):
    SKIP = {"script", "style", "noscript", "svg", "template", "iframe"}
    BLOCK = {"p", "div", "li", "tr", "h1", "h2", "h3", "h4", "h5", "h6", "br", "section", "article", "td", "th", "dt", "dd", "title"}

    def __init__(self):
        super().__init__(convert_charrefs=True); self.out, self.skip = [], 0

    def handle_starttag(self, tag, attrs):
        if tag in self.SKIP: self.skip += 1
        if tag in self.BLOCK: self.out.append("\n")

    def handle_endtag(self, tag):
        if tag in self.SKIP and self.skip: self.skip -= 1
        if tag in self.BLOCK: self.out.append("\n")

    def handle_data(self, d):
        if not self.skip: self.out.append(d)


def html_to_text(raw):
    p = _Text(); p.feed(raw); return "".join(p.out)


def normalize(text):
    text = re.sub(r"!\[[^\]]*\]\([^)]*\)", "", text)            # markdown images
    text = re.sub(r"\[([^\]]*)\]\([^)]*\)", r"\1", text)        # markdown links -> link text (tracking params churn)
    for v in VOLATILE: text = re.sub(v, "", text, flags=re.I)
    lines = [re.sub(r"[ \t ]+", " ", ln).strip() for ln in text.splitlines()]
    out, blank = [], False
    for ln in lines:
        if not ln:
            if not blank and out: out.append("")
            blank = True; continue
        blank = False; out.append(ln)
    return "\n".join(out).strip() + "\n"


def url_id(u):
    return hashlib.sha256(u.encode()).hexdigest()[:16]


# ---------------------------------------------------------------------------------------------
# URL list and cell mapping from the coverage baseline
# ---------------------------------------------------------------------------------------------
def load_cells(baseline):
    cells = {}
    if baseline and Path(baseline).exists():
        for r in csv.DictReader(open(baseline, encoding="utf-8")):
            u = (r.get("evidence_url") or "").strip()
            if u:
                cells.setdefault(u, []).append({"tool": r["tool"], "blind_spot_id": r["blind_spot_id"],
                                                "plan": r.get("plan"), "level": r.get("level")})
    return cells


def pick_urls(a, cells):
    urls = sorted(cells)
    if a.urls:
        urls = [ln.strip() for ln in open(a.urls) if ln.strip() and not ln.startswith("#")]
    if a.shard:
        k, n = (int(x) for x in a.shard.split("/"))
        urls = [u for u in urls if int(url_id(u), 16) % n == (k - 1) % n]
    urls = urls[: a.limit] if a.limit else urls
    # Visit order changes every night (seeded by the date, so a night can be replayed), so no
    # site sees the same request at the same minute each day. Pages kept and dates recorded don't change.
    random.Random(date.today().isoformat()).shuffle(urls)
    return urls


# ---------------------------------------------------------------------------------------------
# polite fetcher
# ---------------------------------------------------------------------------------------------
class Fetcher:
    def __init__(self, delay):
        self.delay, self.robots, self.last = delay, {}, {}

    def _get(self, url, timeout=20):
        req = urllib.request.Request(url, headers={"User-Agent": UA, "Accept": "text/html,text/plain;q=0.9,*/*;q=0.5"})
        with urllib.request.urlopen(req, timeout=timeout) as r:
            body = r.read(MAX_BYTES + 1)
            return r.status, r.headers.get("Content-Type", ""), body

    def robots_for(self, url):
        p = urlparse(url); base = f"{p.scheme}://{p.netloc}"
        if base in self.robots: return self.robots[base]
        rp = urllib.robotparser.RobotFileParser(); state = "ok"
        try:
            self._wait(p.netloc)
            st, _, body = self._get(base + "/robots.txt", timeout=10)
            rp.parse(body.decode("utf-8", "replace").splitlines())
        except urllib.error.HTTPError as e:
            if 400 <= e.code < 500: rp.parse([]); state = f"robots {e.code}: treated as allow-all"
            else: rp.disallow_all = True; state = f"robots {e.code}: treated as disallow"
        except Exception as e:
            rp.disallow_all = True; state = f"robots unreachable ({type(e).__name__}): treated as disallow"
        self.robots[base] = (rp, state); return rp, state

    def _wait(self, host, extra=0):
        gap = max(self.delay, extra) + random.uniform(0, self.delay * 0.5)  # small jitter, never faster
        t = self.last.get(host)
        if t is not None and time.time() - t < gap: time.sleep(gap - (time.time() - t))
        self.last[host] = time.time()

    def fetch(self, url):
        rp, rstate = self.robots_for(url)
        if not rp.can_fetch(UA, url):
            return {"status": "skipped_robots", "robots": rstate or "disallowed"}
        cd = rp.crawl_delay(UA) or 0
        self._wait(urlparse(url).netloc, cd)
        try:
            st, ctype, body = self._get(url)
        except urllib.error.HTTPError as e:
            return {"status": f"http_{e.code}", "robots": rstate}
        except Exception as e:
            return {"status": f"error_{type(e).__name__}", "robots": rstate, "detail": str(e)[:200]}
        if len(body) > MAX_BYTES: return {"status": "too_large", "robots": rstate}
        if "html" in ctype or "text" in ctype or not ctype:
            raw = body.decode("utf-8", "replace")
            text = html_to_text(raw) if ("html" in ctype or "<html" in raw[:2000].lower()) else raw
        else:
            text = f"[binary {ctype.split(';')[0]} sha256:{hashlib.sha256(body).hexdigest()} bytes:{len(body)}]"
        return {"status": "ok", "http": st, "robots": rstate, "text": text,
                "acquired_at": datetime.now(timezone.utc).isoformat(timespec="seconds"), "fetched_via": "snapshot.py/urllib"}


# ---------------------------------------------------------------------------------------------
# store + diff
# ---------------------------------------------------------------------------------------------
def signal_hits(lines):
    hits = {}
    for k, pat in SIGNAL_PATTERNS.items():
        m = [ln for ln in lines if re.search(pat, ln, flags=re.I)]
        if m: hits[k] = [ln[:160] for ln in m[:5]]
    return hits


def find_previous(root, uid, before_day):
    days = sorted((p.name for p in root.iterdir() if p.is_dir() and re.fullmatch(r"\d{4}-\d{2}-\d{2}", p.name)), reverse=True)
    for d in days:
        if d < before_day and (root / d / f"{uid}.txt").exists():
            return d, (root / d / f"{uid}.txt").read_text(encoding="utf-8")
    return None, None


def parse_ts(s):
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        return None


def store_and_diff(root, day, results, cells):
    root.mkdir(parents=True, exist_ok=True); out = root / day; out.mkdir(exist_ok=True)
    sp = root / "state.json"
    state = json.loads(sp.read_text()) if sp.exists() else {}
    manifest, changed, unchanged, first_seen, problems = [], [], 0, [], []
    for url, res in results:
        uid = url_id(url)
        if res.get("status") != "ok":
            problems.append({"url": url, "status": res.get("status"), "robots": res.get("robots"), "detail": res.get("detail")})
            continue
        text = normalize(res["text"]); sha = hashlib.sha256(text.encode()).hexdigest()
        (out / f"{uid}.txt").write_text(text, encoding="utf-8")
        (out / f"{uid}.sha256").write_text(f"{sha}  {uid}.txt\n")
        acq = res.get("acquired_at") or f"{day}T12:00:00+00:00"
        manifest.append({"url": url, "id": uid, "sha256": sha, "bytes": len(text.encode()), "fetched_via": res.get("fetched_via"),
                         "acquired_at": acq, "robots": res.get("robots"), "http": res.get("http")})
        st = state.get(url)
        if st is None:
            state[url] = {"stable_sha": sha, "stable_since": acq, "pending": None, "history": [sha], "last_acquired_at": acq}
            first_seen.append(url); continue
        if parse_ts(acq) and parse_ts(st.get("last_acquired_at", "")) and parse_ts(acq) <= parse_ts(st["last_acquired_at"]) \
                and sha != st["stable_sha"] and (st.get("pending") or {}).get("sha") != sha:
            problems.append({"url": url, "status": "stale_capture", "detail": "fetch tool returned a capture not newer than the last one"})
            continue
        st["last_acquired_at"] = max(acq, st.get("last_acquired_at", acq))
        if sha == st["stable_sha"]:
            if st.get("pending"):
                changed.append({"url": url, "id": uid, "status": "reverted_before_confirm", "date": day,
                                "from_sha": st["pending"]["sha"], "to_sha": sha, "cells": cells.get(url, [])})
                st["pending"] = None
            else:
                unchanged += 1
            continue
        prev_day, prev_text = find_previous(root, uid, day)
        old_lines = (prev_text or "").splitlines(); new_lines = text.splitlines()
        diff = list(difflib.unified_diff(old_lines, new_lines, fromfile=f"{prev_day}/{uid}.txt", tofile=f"{day}/{uid}.txt", n=1, lineterm=""))
        added = [l[1:] for l in diff if l.startswith("+") and not l.startswith("+++")]
        removed = [l[1:] for l in diff if l.startswith("-") and not l.startswith("---")]
        ratio = 1 - difflib.SequenceMatcher(None, old_lines, new_lines, autojunk=False).ratio()
        hits = {"added": signal_hits(added), "removed": signal_hits(removed)}
        material = ratio >= 0.02 or bool(hits["added"] or hits["removed"])
        pend = st.get("pending")
        if sha in st["history"] and sha != st["stable_sha"] and not (pend and pend["sha"] == sha):
            status = "flip_back"; st["pending"] = {"sha": sha, "first_seen": acq}
        elif pend and pend["sha"] == sha and parse_ts(acq) and parse_ts(pend["first_seen"]) and \
                parse_ts(acq) - parse_ts(pend["first_seen"]) >= timedelta(hours=CONFIRM_HOURS):
            status = "confirmed"; st["stable_sha"], st["stable_since"], st["pending"] = sha, pend["first_seen"], None
        elif pend and pend["sha"] == sha:
            status = "awaiting_confirmation"
        else:
            status = "new_change"; st["pending"] = {"sha": sha, "first_seen": acq}
        if sha not in st["history"]: st["history"] = (st["history"] + [sha])[-20:]
        changed.append({"url": url, "id": uid, "status": status, "date": day, "previous_copy": prev_day,
                        "from_sha": st["history"][-2] if len(st["history"]) > 1 else None, "to_sha": sha,
                        "change_ratio": round(ratio, 4), "lines_added": len(added), "lines_removed": len(removed),
                        "material": material, "signal_hits": hits, "cells": cells.get(url, []),
                        "diff": diff[:200], "diff_truncated": len(diff) > 200})
    (out / "manifest.json").write_text(json.dumps({"date": day, "pages": manifest}, indent=1))
    report = {"schema": "tools-of-record/changed-pages/v1", "date": day, "fetched": len(manifest), "first_seen": len(first_seen),
              "unchanged": unchanged, "changed": changed, "problems": problems,
              "recheck_cells": sorted({(c["tool"], c["blind_spot_id"]) for ch in changed if ch.get("material")
                                       and ch["status"] in ("new_change", "confirmed", "flip_back") for c in ch["cells"]})}
    report["recheck_cells"] = [{"tool": t, "blind_spot_id": b} for t, b in report["recheck_cells"]]
    (out / "changed_pages.json").write_text(json.dumps(report, indent=1, ensure_ascii=False))
    sp.write_text(json.dumps(state, indent=1, sort_keys=True))
    return report


def summary(rep):
    return json.dumps({k: (len(v) if isinstance(v, list) else v) for k, v in rep.items() if k != "schema"})


# ---------------------------------------------------------------------------------------------
# commands
# ---------------------------------------------------------------------------------------------
def cmd_run(a):
    """Fetch every URL. Different sites are visited at the same time (a.workers threads), but each
    site is still visited one page at a time with the polite per-site gap. A time budget stops new
    fetches after a.budget minutes, so results are always saved even when some sites are slow."""
    from concurrent.futures import ThreadPoolExecutor
    cells = load_cells(a.baseline); urls = pick_urls(a, cells)
    by_host = {}
    for u in urls: by_host.setdefault(urlparse(u).netloc, []).append(u)
    deadline = time.time() + a.budget * 60
    out = {}
    def work(host_urls):
        f = Fetcher(a.delay)
        for u in host_urls:
            if time.time() > deadline:
                out[u] = {"status": "skipped_time_budget", "detail": f"not started within {a.budget} min"}; continue
            try: out[u] = f.fetch(u)
            except Exception as e: out[u] = {"status": f"error_{type(e).__name__}", "detail": str(e)[:200]}
    with ThreadPoolExecutor(max_workers=max(1, a.workers)) as ex:
        list(ex.map(work, by_host.values()))
    res = [(u, out.get(u, {"status": "not_run"})) for u in urls]
    rep = store_and_diff(Path(a.root), a.date or date.today().isoformat(), res, cells); print(summary(rep))


def load_parallel(path):
    d = json.loads(Path(path).read_text())
    if isinstance(d, list) and d and isinstance(d[0], dict) and "text" in d[0]: d = json.loads(d[0]["text"])
    out = []
    for r in d.get("results", []):
        txt = r.get("full_content")
        if not txt:
            out.append((r["url"], {"status": "no_full_content", "detail": "re-fetch with full_content=true (excerpts are not stable)"})); continue
        out.append((r["url"], {"status": "ok", "text": (r.get("title") or "") + "\n" + txt, "robots": "delegated:parallel_web_fetch",
                               "acquired_at": r.get("acquired_at"), "fetched_via": "parallel_web_fetch(full_content)"}))
    for e in d.get("errors") or []:
        out.append((e.get("url", "?"), {"status": "fetch_error", "detail": str(e)[:200]}))
    return out


def cmd_ingest(a):
    cells = load_cells(a.baseline); res = []
    for p in a.parallel_json or []: res += load_parallel(p)
    if a.inbox:
        for p in sorted(Path(a.inbox).glob("*.json")):
            j = json.loads(p.read_text())
            res.append((j["url"], {"status": "ok" if j.get("text") else "empty", "text": j.get("text", ""), "robots": "delegated:" + j.get("fetched_via", "agent"),
                                   "acquired_at": j.get("acquired_at"), "fetched_via": j.get("fetched_via", "agent")}))
    rep = store_and_diff(Path(a.root), a.date or date.today().isoformat(), res, cells); print(summary(rep))


def cmd_selftest(a):
    import http.server, shutil, socketserver, tempfile
    pages = {"/robots.txt": "User-agent: *\nDisallow: /private\n",
             "/help/a": "<html><head><title>Quote follow-ups</title><script>var x=1</script></head><body><p>Quote follow-ups send 2 reminders.</p><p>Updated 3 hours ago</p></body></html>",
             "/help/b": "<html><body><p>Lead source report lists every source.</p></body></html>",
             "/help/c": "<html><body><p>Missed-call text-back is on.</p></body></html>",
             "/private/x": "<html><body>secret</body></html>"}

    class H(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            body = pages.get(self.path)
            if body is None: self.send_response(404); self.end_headers(); return
            self.send_response(200); self.send_header("Content-Type", "text/plain" if self.path.endswith(".txt") else "text/html; charset=utf-8")
            self.end_headers(); self.wfile.write(body.encode())
        def log_message(self, *x): pass

    srv = socketserver.TCPServer(("127.0.0.1", 0), H); port = srv.server_address[1]
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    base = f"http://127.0.0.1:{port}"
    work = Path(tempfile.mkdtemp(prefix="snap_selftest_")); root = work / "snapshots"
    urls = work / "urls.txt"; urls.write_text("\n".join(base + p for p in ("/help/a", "/help/b", "/help/c", "/private/x")))
    bl = work / "baseline.csv"
    with open(bl, "w", newline="") as fh:
        w = csv.writer(fh); w.writerow(["tool", "blind_spot_id", "level", "plan", "default_on", "evidence_url", "confidence", "requires", "notes"])
        w.writerow(["ExampleFSM", "quote-emailed-and-forgotten", "4", "core", "yes", base + "/help/a", "high", "", ""])
    import os
    for k in ("http_proxy", "HTTP_PROXY"): os.environ.pop(k, None)
    os.environ["no_proxy"] = os.environ["NO_PROXY"] = "127.0.0.1,localhost"
    ok = True

    def check(label, cond, detail=""):
        nonlocal ok; ok &= bool(cond); print(("PASS " if cond else "FAIL ") + label + (f" | {detail}" if detail and not cond else ""))

    def day(d, stamp):
        f = Fetcher(0)
        res = []
        for u in urls.read_text().split():
            r = f.fetch(u)
            if r.get("status") == "ok": r["acquired_at"] = stamp
            res.append((u, r))
        return store_and_diff(root, d, res, load_cells(bl))
    r1 = day("2026-11-10", "2026-11-10T03:00:00+00:00")
    check("day 1: three pages stored as first_seen", r1["first_seen"] == 3, summary(r1))
    check("robots.txt disallow respected (/private/x skipped)", any(p["status"] == "skipped_robots" for p in r1["problems"]), r1["problems"])
    t = (root / "2026-11-10" / f"{url_id(base + '/help/a')}.txt").read_text()
    check("script and relative-time text stripped", "var x" not in t and "ago" not in t, t)
    shaf = (root / "2026-11-10" / f"{url_id(base + '/help/a')}.sha256").read_text().split()[0]
    check("sha256 file matches stored text", shaf == hashlib.sha256(t.encode()).hexdigest())
    pages["/help/a"] = pages["/help/a"].replace("Quote follow-ups send 2 reminders.", "Quote follow-ups are available on the Grow plan and up. Turned off by default.")
    pages["/help/c"] = "<html><body><p>Missed-call text-back is in beta for some accounts.</p></body></html>"
    r2 = day("2026-11-11", "2026-11-11T03:00:00+00:00")
    ca = {c["url"].rsplit("/", 1)[-1]: c for c in r2["changed"]}
    check("day 2: /help/a is a new_change", ca.get("a", {}).get("status") == "new_change", summary(r2))
    check("plan-gate and default signals detected", {"plan_gate", "default"} <= set(ca["a"]["signal_hits"]["added"]), ca["a"]["signal_hits"])
    check("changed page maps to its coverage cell", ca["a"]["cells"][0]["blind_spot_id"] == "quote-emailed-and-forgotten")
    check("unchanged page counted as unchanged", r2["unchanged"] == 1)
    check("diff is a unified diff against the last copy", any(l.startswith("+Quote follow-ups are available") for l in ca["a"]["diff"]))
    check("recheck_cells lists the cell", {"tool": "ExampleFSM", "blind_spot_id": "quote-emailed-and-forgotten"} in r2["recheck_cells"])
    pages["/help/c"] = "<html><body><p>Missed-call text-back is on.</p></body></html>"
    r3 = day("2026-11-12", "2026-11-12T03:00:00+00:00")
    cc = {c["url"].rsplit("/", 1)[-1]: c for c in r3["changed"]}
    check("day 3 (+24h): /help/a confirmed", cc.get("a", {}).get("status") == "confirmed", summary(r3))
    check("day 3: /help/c went back before confirmation", cc.get("c", {}).get("status") == "reverted_before_confirm", cc.get("c"))
    pages["/help/a"] = pages["/help/a"].replace("Quote follow-ups are available on the Grow plan and up. Turned off by default.", "Quote follow-ups send 2 reminders.")
    r4 = day("2026-11-13", "2026-11-13T03:00:00+00:00")
    cd = {c["url"].rsplit("/", 1)[-1]: c for c in r4["changed"]}
    check("day 4: return to an old text is flagged flip_back", cd.get("a", {}).get("status") == "flip_back", summary(r4))
    # ingest path with a stale capture
    inbox = work / "inbox"; inbox.mkdir()
    (inbox / "1.json").write_text(json.dumps({"url": base + "/help/b", "text": "Lead source report lists every source, now with AI.",
                                              "fetched_via": "test_tool", "acquired_at": "2026-11-01T00:00:00Z"}))
    class A: pass
    ia = A(); ia.baseline = str(bl); ia.parallel_json = None; ia.inbox = str(inbox); ia.root = str(root); ia.date = "2026-11-14"
    import io, contextlib
    with contextlib.redirect_stdout(io.StringIO()): cmd_ingest(ia)
    r5 = json.loads((root / "2026-11-14" / "changed_pages.json").read_text())
    check("ingest: a capture older than the last one is refused as stale", any(p["status"] == "stale_capture" for p in r5["problems"]), r5)
    srv.shutdown()
    print(("ALL PASS" if ok else "SOME FAILED") + f"; scratch dir: {work}")
    sys.exit(0 if ok else 1)


def main():
    ap = argparse.ArgumentParser(); sp = ap.add_subparsers(dest="cmd", required=True)
    for name in ("run", "ingest"):
        p = sp.add_parser(name)
        p.add_argument("--baseline", default="/home/claude/mapwork/uses/coverage_baseline.csv")
        p.add_argument("--root", default=str(Path(__file__).resolve().parent / "snapshots"))
        p.add_argument("--date")
        if name == "run":
            p.add_argument("--urls"); p.add_argument("--shard"); p.add_argument("--limit", type=int); p.add_argument("--delay", type=float, default=5.0)
            p.add_argument("--workers", type=int, default=24); p.add_argument("--budget", type=float, default=100.0)
        else:
            p.add_argument("--parallel-json", nargs="*"); p.add_argument("--inbox")
    sp.add_parser("selftest")
    a = ap.parse_args()
    {"run": cmd_run, "ingest": cmd_ingest, "selftest": cmd_selftest}[a.cmd](a)


if __name__ == "__main__":
    main()
