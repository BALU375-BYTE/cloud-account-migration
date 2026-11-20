"""
Saved queries from the Neo4j Aura Query tool (Saved Cypher, open query tabs, query history).

Neo4j keeps these with your Aura login (and in the browser), not inside the database, and offers
no public API for them. So the app opens a browser window, you sign in to Aura yourself, and the
app reads the queries the Aura website loads for you - the same data you see on screen.

    python -m connectors.neo4j_queries capture <output folder>     (what the app runs)

Nothing else is saved from the website: no cookies, tokens or passwords, and the browser profile
used for this lives in its own folder, separate from your normal browser.
"""
from __future__ import annotations

import json
import os
import re
import sys
import time
from pathlib import Path

AURA_START = os.getenv("CAM_AURA_START_URL", "https://console.neo4j.io/")
AURA_QUERY = os.getenv("CAM_AURA_QUERY_URL", "https://console.neo4j.io/tools/query")
AURA_HOSTS = tuple(os.getenv("CAM_AURA_HOSTS", "neo4j.io,neo4j.com").split(","))
WAIT_MINUTES = int(os.getenv("CAM_AURA_WAIT_MINUTES", "10"))

KIND_ORDER = {"saved": 0, "tab": 1, "query": 2, "history": 3}
KIND_TITLES = {"saved": "Saved Cypher", "tab": "Open query tabs", "query": "Queries from the Query tool", "history": "Query history"}
CONTENT_KEYS = ("content", "cypher", "query", "statement", "text", "body", "value", "script", "code", "source")
NAME_KEYS = ("name", "title", "label", "displayName", "display_name", "tabName", "description")
FOLDER_KEYS = ("folder", "folderName", "folder_name", "group", "category", "directory")
CYPHER_RE = re.compile(
    r"^\s*(?:(?://[^\n]*\n|/\*.*?\*/)\s*)*"
    r"(?::param|:params|:auto|:use|OPTIONAL\s+MATCH|MATCH|CREATE|MERGE|RETURN|CALL|UNWIND|WITH|SHOW|"
    r"LOAD\s+CSV|DETACH\s+DELETE|DELETE|DROP|USE|EXPLAIN|PROFILE|FOREACH|ALTER|GRANT|REVOKE|DENY|"
    r"START\s+DATABASE|STOP\s+DATABASE|TERMINATE|CYPHER|SET|REMOVE|FINISH)\b",
    re.I | re.S)


# ----------------------------------------------------------------------------- finding queries in data
def looks_like_cypher(s) -> bool:
    return isinstance(s, str) and 6 <= len(s) <= 200_000 and bool(CYPHER_RE.match(s))


def _kind(path: str) -> str:
    p = path.lower()
    if "history" in p:
        return "history"
    if any(w in p for w in ("saved", "favorite", "favourite", "bookmark", "script", "snippet", "library")):
        return "saved"
    if any(w in p for w in ("tab", "editor", "draft")):
        return "tab"
    return "query"


def _first_str(d: dict, keys) -> str:
    for k in keys:
        v = d.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip()
    return ""


def find_queries(obj, source: str = "", path: str = "") -> list[dict]:
    """Walk any JSON-like data and return every Cypher query in it, with its name when there is one."""
    found: list[dict] = []

    def walk(o, p, depth=0):
        if depth > 40:
            return
        if isinstance(o, dict):
            used = None
            for k in CONTENT_KEYS:
                if looks_like_cypher(o.get(k)):
                    used = k
                    found.append({"kind": _kind(source + " " + p), "name": _first_str(o, NAME_KEYS),
                                  "folder": _first_str(o, FOLDER_KEYS), "cypher": o[k].strip(),
                                  "found_in": f"{source}{p}"})
                    break
            for k, v in o.items():
                if k != used:
                    walk(v, f"{p}.{k}", depth + 1)
        elif isinstance(o, list):
            for i, v in enumerate(o):
                walk(v, f"{p}[{i}]", depth + 1)
        elif isinstance(o, str):
            s = o.strip()
            if s[:1] in "{[" and len(s) < 20_000_000:
                try:
                    walk(json.loads(s), p, depth + 1)          # values stored as JSON text (browser storage)
                    return
                except ValueError:
                    pass
            if looks_like_cypher(s):
                found.append({"kind": _kind(source + " " + p), "name": "", "folder": "", "cypher": s,
                              "found_in": f"{source}{p}"})

    walk(obj, path)
    return found


def merge(queries: list[dict]) -> list[dict]:
    """One entry per distinct query; Saved Cypher wins over tabs, tabs over history; keep the best name."""
    best: dict[str, dict] = {}
    for q in queries:
        key = " ".join(q["cypher"].split())
        old = best.get(key)
        if old is None or KIND_ORDER[q["kind"]] < KIND_ORDER[old["kind"]]:
            if old and not q.get("name"):
                q = {**q, "name": old.get("name", "")}
            best[key] = dict(q)
        elif not old.get("name") and q.get("name"):
            old["name"] = q["name"]
    return sorted(best.values(), key=lambda q: (KIND_ORDER[q["kind"]], q.get("folder", ""), q.get("name", "").lower()))


# ----------------------------------------------------------------------------- files
def _fname(i: int, q: dict) -> str:
    base = re.sub(r"[^A-Za-z0-9_\- ]", "", q.get("name") or "").strip().replace(" ", "_")[:60]
    return f"{i:03d}_{base or 'query'}.cypher"


def save(queries: list[dict], folder: Path, notes: list | None = None) -> dict:
    """Write queries.json, one .cypher file per query, and one combined file per kind."""
    folder = Path(folder)
    folder.mkdir(parents=True, exist_ok=True)
    queries = merge(queries)
    (folder / "queries.json").write_text(json.dumps(queries, indent=2, ensure_ascii=False), encoding="utf-8")
    counts = {}
    for kind, title in KIND_TITLES.items():
        items = [q for q in queries if q["kind"] == kind]
        counts[kind] = len(items)
        if not items:
            continue
        sub = folder / kind
        sub.mkdir(exist_ok=True)
        combined = [f"// {title} - {len(items)} quer{'y' if len(items) == 1 else 'ies'}, "
                    f"copied by Cloud Account Migration\n"]
        for i, q in enumerate(items, 1):
            if kind != "history":
                (sub / _fname(i, q)).write_text(q["cypher"] + "\n", encoding="utf-8")
            head = f"// ---- {i}. {q.get('name') or '(no name)'}" + (f"   [folder: {q['folder']}]" if q.get("folder") else "")
            combined.append(f"{head}\n{q['cypher'].rstrip().rstrip(';')};\n")
        (folder / f"all_{kind}.cypher").write_text("\n".join(combined), encoding="utf-8")
    if notes is not None:
        (folder / "capture_notes.json").write_text(json.dumps(notes, indent=2), encoding="utf-8")
    return counts


def load(folder: Path) -> list[dict]:
    f = Path(folder) / "queries.json"
    return json.loads(f.read_text(encoding="utf-8")) if f.exists() else []


def counts_text(queries: list[dict]) -> str:
    parts = []
    for kind, title in KIND_TITLES.items():
        n = sum(q["kind"] == kind for q in queries)
        if n:
            parts.append(f"{n} {title.lower() if kind != 'saved' else 'Saved Cypher'}")
    return ", ".join(parts) or "no queries"


def read_file(path: Path) -> list[dict]:
    """Queries from a file the user already has (Aura export, .cypher, .txt, .json, .csv, .zip)."""
    path = Path(path)
    name = path.name
    if path.suffix.lower() == ".zip":
        import zipfile
        out = []
        with zipfile.ZipFile(path) as z:
            for n in z.namelist():
                if not n.endswith("/"):
                    out += _read_text(z.read(n).decode("utf-8", "replace"), f"file:{name}/{n}")
        return out
    return _read_text(path.read_text(encoding="utf-8", errors="replace"), f"file:{name}")


def _read_text(text: str, source: str) -> list[dict]:
    s = text.strip()
    if s[:1] in "{[":
        try:
            return [dict(q, kind="saved" if q["kind"] == "query" else q["kind"])
                    for q in find_queries(json.loads(s), source)]
        except ValueError:
            pass
    if source.lower().endswith(".csv"):
        import csv
        import io
        rows = list(csv.DictReader(io.StringIO(text)))
        out = []
        for r in rows:
            q = find_queries(r, source)
            out += [dict(x, kind="saved") for x in q]
        if out:
            return out
    # plain Cypher: statements separated by ';'. Our own '// ---- 1. name  [folder: x]' line gives the name;
    # otherwise a '// comment' line just above the statement does.
    marker = re.compile(r"^//\s*-{2,}\s*\d+\.\s*(.*?)(?:\s+\[folder:\s*(.*)\])?\s*$")
    out = []
    for chunk in re.split(r";[ \t]*(?:\r?\n|$)", text):
        lines = chunk.strip().splitlines()
        name = folder = ""
        idx = next((i for i, l in enumerate(lines) if marker.match(l.strip())), None)
        if idx is not None:
            m = marker.match(lines[idx].strip())
            name, folder = ("" if m.group(1) == "(no name)" else m.group(1)), (m.group(2) or "")
            lines = lines[idx + 1:]
        else:
            while lines and (not lines[0].strip() or lines[0].strip().startswith("//")):
                if lines[0].strip():
                    name = lines[0].strip().lstrip("/").strip(" -")
                lines.pop(0)
        body = "\n".join(lines).strip()
        if looks_like_cypher(body):
            out.append({"kind": "saved", "name": name, "folder": folder, "cypher": body, "found_in": source})
    return out


# ----------------------------------------------------------------------------- browser capture
_STORAGE_JS = """
async () => {
  const out = {local: {}, session: {}, idb: {}};
  try { for (let i = 0; i < localStorage.length; i++) { const k = localStorage.key(i); out.local[k] = localStorage.getItem(k); } } catch (e) {}
  try { for (let i = 0; i < sessionStorage.length; i++) { const k = sessionStorage.key(i); out.session[k] = sessionStorage.getItem(k); } } catch (e) {}
  try {
    const dbs = indexedDB.databases ? await indexedDB.databases() : [];
    for (const info of dbs) {
      if (!info.name) continue;
      const db = await new Promise((ok, bad) => { const r = indexedDB.open(info.name); r.onsuccess = () => ok(r.result); r.onerror = () => bad(r.error); });
      out.idb[info.name] = {};
      for (const store of Array.from(db.objectStoreNames)) {
        try {
          const rows = await new Promise((ok, bad) => { const r = db.transaction(store, 'readonly').objectStore(store).getAll(null, 5000); r.onsuccess = () => ok(r.result); r.onerror = () => bad(r.error); });
          out.idb[info.name][store] = JSON.parse(JSON.stringify(rows));
        } catch (e) {}
      }
      db.close();
    }
  } catch (e) {}
  return out;
}
"""


def _say(msg: str) -> None:
    print(msg, flush=True)


def _launch(p, profile: Path, headless: bool):
    """Use the browser already on the computer (Edge on Windows, else Chrome), else Playwright's own."""
    args = dict(headless=headless, no_viewport=True, ignore_default_args=["--enable-automation"],
                args=["--disable-blink-features=AutomationControlled"])
    for channel in (("msedge", "chrome") if sys.platform == "win32" else ("chrome", "msedge")):
        try:
            return p.chromium.launch_persistent_context(str(profile), channel=channel, **args)
        except Exception:
            continue
    try:
        return p.chromium.launch_persistent_context(str(profile), **args)
    except Exception:
        _say("Getting a browser for the first time (one-off download, about 150 MB) ...")
        import subprocess
        subprocess.run([sys.executable, "-m", "playwright", "install", "chromium"], check=False)
        return p.chromium.launch_persistent_context(str(profile), **args)


def capture(out_dir: Path, headless: bool = False) -> dict:
    """Open Aura in a browser window, let the user sign in, collect every query the site loads."""
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        raise SystemExit("ERROR: the browser helper isn't installed. Close the app and run Start.bat again "
                         "(it installs what's missing), then try again.")
    out_dir = Path(out_dir)
    profile = Path(os.getenv("CAM_BROWSER_PROFILE", Path.home() / ".cloud-account-migration" / "aura-browser"))
    profile.mkdir(parents=True, exist_ok=True)
    found, notes, pending = [], [], []
    with sync_playwright() as p:
        ctx = _launch(p, profile, headless)
        ctx.on("response", lambda r: pending.append(r))
        page = ctx.pages[0] if ctx.pages else ctx.new_page()
        closed = {"v": False}
        ctx.on("close", lambda *_: closed.update(v=True))
        page.on("close", lambda *_: closed.update(v=True) if not [x for x in ctx.pages if not x.is_closed()] else None)
        _say("A browser window has opened. Sign in to Neo4j Aura there (the old account).")
        page.goto(AURA_START)
        went_to_query, last_new, deadline, seen = False, time.time(), time.time() + WAIT_MINUTES * 60, set()
        told_connect, stable = False, 0
        while not closed["v"] and time.time() < deadline:
            try:
                page.wait_for_timeout(1500)
            except Exception:
                break
            batch, pending[:] = list(pending), []
            for r in batch:                                       # 1. data the website downloads
                try:
                    host = r.url.split("/")[2].split(":")[0]
                    if not any(host.endswith(h.strip()) for h in AURA_HOSTS):
                        continue
                    if "json" not in (r.headers.get("content-type") or ""):
                        continue
                    data = r.json()
                except Exception:
                    continue
                path = "/" + r.url.split("?")[0].split("/", 3)[-1]
                qs = find_queries(data, f"web:{path}")
                if qs:
                    found += qs
                    notes.append({"where": f"web:{path}", "queries": len(qs)})
            try:                                                  # 2. what the website keeps in the browser
                store = page.evaluate(_STORAGE_JS)
                for area in ("local", "session"):
                    for k, v in store[area].items():
                        qs = find_queries(v, f"{area}Storage:{k}")
                        if qs:
                            found += qs
                            notes.append({"where": f"{area}Storage:{k}", "queries": len(qs)})
                for dbname, stores in store["idb"].items():
                    for sname, rows in stores.items():
                        qs = find_queries(rows, f"indexedDB:{dbname}/{sname}")
                        if qs:
                            found += qs
                            notes.append({"where": f"indexedDB:{dbname}/{sname}", "queries": len(qs)})
            except Exception:
                pass
            keys = {" ".join(q["cypher"].split()) for q in found}
            if keys - seen:
                last_new = time.time()
                seen |= keys
                _say(f"Found {len(seen)} quer{'y' if len(seen) == 1 else 'ies'} so far ...")
            url = page.url
            on_console = url.startswith(AURA_START.rstrip("/")) or url.startswith(AURA_QUERY) or \
                any(h in url.split("/")[2] for h in ("console.neo4j.io", "console-preview.neo4j.io"))
            if not on_console or any(w in url.lower() for w in ("login", "/auth", "signin", "sign-in")):
                stable = 0                                        # still on the sign-in pages
                if went_to_query and not seen:
                    went_to_query = False                         # sent back to sign-in: wait for it again
                continue
            stable += 1
            if stable >= 2 and not went_to_query:
                _say("Signed in. Opening the Query tool ...")
                went_to_query, last_new = True, time.time()
                try:
                    page.goto(AURA_QUERY)
                except Exception:
                    pass
                continue
            if went_to_query and seen and time.time() - last_new > 12:
                break                                             # everything loaded and nothing new for 12 s
            if went_to_query and not seen and not told_connect and time.time() - last_new > 25:
                told_connect = True
                _say("No queries yet. If the window asks you to pick or connect to an instance, do that, "
                     "then open the Saved Cypher panel (bookmark icon). Close the window when you're done.")
        try:
            ctx.close()
        except Exception:
            pass
    counts = save(found, out_dir, notes) if found else {}
    total = sum(counts.values())
    _say(f"DONE {total}" if found else "DONE 0")
    return counts


if __name__ == "__main__":
    if len(sys.argv) >= 3 and sys.argv[1] == "capture":
        capture(Path(sys.argv[2]), headless=os.getenv("CAM_HEADLESS") == "1")
    else:
        print(__doc__)
