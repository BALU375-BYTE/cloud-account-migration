"""Saved queries from the Aura Query tool: finding, saving, backup/restore, and a real browser run on a fake Aura."""
import http.server
import json
import os
import subprocess
import sys
import threading
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from connectors import neo4j_queries as nq  # noqa: E402
from tests.test_neo4j import FakeGraph, conn, sample_source  # noqa: E402

SAVED_API = {"items": [
    {"id": "a1", "name": "Top customers", "folder": "Sales",
     "content": "MATCH (c:Customers)-[:PLACED]->(o:Orders)\nRETURN c.name, count(o) AS orders ORDER BY orders DESC"},
    {"id": "a2", "name": "Products by category", "content": "// all products\nMATCH (p:Products) RETURN p.category, collect(p.name)"},
    {"id": "a3", "name": "Not cypher", "content": "just a note"}]}
TABS_STORAGE = json.dumps({"tabs": [
    {"title": "import", "text": "LOAD CSV WITH HEADERS FROM 'file:///orders.csv' AS row MERGE (o:Orders {order_id: row.id})"},
    {"title": "Untitled Query", "text": "MATCH (n) RETURN n LIMIT 25"},
    {"title": "dup of saved", "text": "// all products\n  MATCH (p:Products)   RETURN p.category, collect(p.name)"}]})
HISTORY_STORAGE = json.dumps(["MATCH (n) RETURN count(n)", "MATCH (n) RETURN n LIMIT 25", "hello"])


def test_finds_queries_anywhere_and_merges():
    found = nq.find_queries(SAVED_API, "web:/v1/saved-cypher") + \
        nq.find_queries(TABS_STORAGE, "localStorage:query-editor") + \
        nq.find_queries(HISTORY_STORAGE, "localStorage:query-history")
    qs = nq.merge(found)
    saved = [q for q in qs if q["kind"] == "saved"]
    assert [q["name"] for q in saved] == ["Products by category", "Top customers"]   # sorted by folder, then name
    assert saved[1]["folder"] == "Sales"
    tabs = {q["name"] for q in qs if q["kind"] == "tab"}
    assert tabs == {"import", "Untitled Query"}                  # duplicate of a saved query kept once, as saved
    hist = [q["cypher"] for q in qs if q["kind"] == "history"]
    assert hist == ["MATCH (n) RETURN count(n)"]                 # the other history entry is already a tab
    assert not any(q["cypher"] in ("just a note", "hello") for q in qs)


def test_save_and_read_back(tmp_path):
    qs = nq.find_queries(SAVED_API, "web:/saved") + nq.find_queries(TABS_STORAGE, "localStorage:tabs")
    counts = nq.save(qs, tmp_path)
    assert counts["saved"] == 2 and counts["tab"] == 3 - 1
    assert (tmp_path / "saved" / "002_Top_customers.cypher").read_text().startswith("MATCH (c:Customers)")
    combined = (tmp_path / "all_saved.cypher").read_text()
    assert "// ---- 2. Top customers   [folder: Sales]" in combined
    back = nq.read_file(tmp_path / "all_saved.cypher")             # a .cypher file reads back with names
    assert [q["name"] for q in back] == ["Products by category", "Top customers"]
    assert len(nq.load(tmp_path)) == 4


def test_reads_files_people_already_have(tmp_path):
    (tmp_path / "export.json").write_text(json.dumps(SAVED_API))
    assert {q["name"] for q in nq.read_file(tmp_path / "export.json")} == {"Top customers", "Products by category"}
    (tmp_path / "q.csv").write_text('name,query\nAll,"MATCH (n) RETURN n"\n')
    assert nq.read_file(tmp_path / "q.csv")[0]["name"] == "All"
    import zipfile
    with zipfile.ZipFile(tmp_path / "q.zip", "w") as z:
        z.writestr("x/one.cypher", "// One\nMATCH (a) RETURN a;\n// Two\nMATCH (b) RETURN b;")
    assert [q["name"] for q in nq.read_file(tmp_path / "q.zip")] == ["One", "Two"]


def test_backup_keeps_queries_and_restore_lists_them(tmp_path, monkeypatch):
    qdir = tmp_path / "captured"
    nq.save(nq.find_queries(SAVED_API, "web:/saved"), qdir)
    src = conn("SRC", sample_source(), monkeypatch, QUERIES_DIR=str(qdir))
    pkg = tmp_path / "pkg"
    res = src.backup(pkg)
    q = [r for r in res if r.kind == "queries"][0]
    assert q.status == "OK" and "2 Saved Cypher" in q.message
    assert (pkg / "neo4j_queries" / "queries.json").exists()

    res = conn("TGT", FakeGraph(["neo4j"]), monkeypatch).restore(pkg)
    assert not [r for r in res if r.status == "FAILED"], res
    q = [r for r in res if r.kind == "queries"][0]
    assert q.status == "MANUAL" and "all_saved.cypher" in q.action
    v = conn("TGT", FakeGraph(["neo4j"]), monkeypatch)
    assert v                                                     # the queries folder is not mistaken for a database


def test_aura_backup_without_queries_says_what_to_do(tmp_path, monkeypatch):
    monkeypatch.delenv("SRC_NEO_QUERIES_DIR", raising=False)
    res = conn("SRC", sample_source(), monkeypatch).backup(tmp_path)
    q = [r for r in res if r.kind == "queries"][0]
    assert q.status == "MANUAL" and "collect my saved queries" in q.message


# ----------------------------------------------------------------------------- real browser, fake Aura website
FAKE_LOGIN = b"""<html><body>Sign in...<script>
setTimeout(() => { document.cookie = 'signedin=1; path=/'; location.href = '/console/'; }, 1500);
</script></body></html>"""
FAKE_CONSOLE = b"""<html><body>Console<script>
if (!document.cookie.includes('signedin')) location.href = '/login';
</script></body></html>"""
FAKE_QUERY = b"""<html><body>Query tool<script>
if (!document.cookie.includes('signedin')) location.href = '/login';
localStorage.setItem('query-editor', %s);
localStorage.setItem('query-history', %s);
fetch('/api/v1/saved-cypher').then(r => r.json());
</script></body></html>""" % (json.dumps(TABS_STORAGE).encode(), json.dumps(HISTORY_STORAGE).encode())


class FakeAura(http.server.BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def do_GET(self):
        routes = {"/login": (FAKE_LOGIN, "text/html"), "/console/": (FAKE_CONSOLE, "text/html"),
                  "/console/tools/query": (FAKE_QUERY, "text/html"),
                  "/api/v1/saved-cypher": (json.dumps(SAVED_API).encode(), "application/json")}
        body, ctype = routes.get(self.path.split("?")[0], (b"not found", "text/plain"))
        self.send_response(200 if body != b"not found" else 404)
        self.send_header("Content-Type", ctype)
        self.end_headers()
        self.wfile.write(body)


def test_browser_capture_end_to_end(tmp_path):
    pytest.importorskip("playwright")
    srv = http.server.ThreadingHTTPServer(("127.0.0.1", 0), FakeAura)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    base = f"http://127.0.0.1:{srv.server_port}"
    env = dict(os.environ, CAM_AURA_START_URL=f"{base}/console/", CAM_AURA_QUERY_URL=f"{base}/console/tools/query",
               CAM_AURA_HOSTS="127.0.0.1", CAM_HEADLESS="1", CAM_AURA_WAIT_MINUTES="2",
               CAM_BROWSER_PROFILE=str(tmp_path / "profile"))
    out = tmp_path / "out"
    p = subprocess.run([sys.executable, "-m", "connectors.neo4j_queries", "capture", str(out)], cwd=ROOT, env=env,
                       capture_output=True, text=True, timeout=180)
    srv.shutdown()
    assert "Signed in" in p.stdout, p.stdout + p.stderr
    qs = nq.load(out)
    assert {q["name"] for q in qs if q["kind"] == "saved"} == {"Top customers", "Products by category"}, p.stdout
    assert {q["name"] for q in qs if q["kind"] == "tab"} == {"import", "Untitled Query"}
    assert any(q["kind"] == "history" for q in qs)
    notes = json.loads((out / "capture_notes.json").read_text())
    assert any(n["where"] == "web:/api/v1/saved-cypher" for n in notes)
    assert "signedin" not in (out / "queries.json").read_text()  # nothing but queries is kept
