"""
Neo4j -> Neo4j migration (AuraDB, Neo4j Desktop or self-managed; Community or Enterprise).

Uses only Cypher over the normal connection (Bolt), so it also works on AuraDB, where the
admin dump/load tools aren't available. Copies, per database:
  * constraints and indexes
  * every node (labels + properties) and every relationship (type + properties)
  * all property types: numbers, text, booleans, lists, dates/times, durations, points, bytes
Check compares counts per label and relationship type, plus a fingerprint of all nodes and
relationships, so identical data is proven, not assumed.
"""
from __future__ import annotations

import base64
import datetime as _dt
import hashlib
import json
from pathlib import Path

from core.base import FAILED, MANUAL, OK, SKIPPED, Connector, Field, Friendly, Resource, Result, safe_name

TMP_LABEL = "__CAM_TMP"          # temporary label so relationships can find their nodes during restore
TMP_KEY = "__cam_id"             # temporary property holding the old node id
TMP_INDEX = "cam_tmp_id"
FP_MOD = 2 ** 64
BUILTIN_ROLES = {"PUBLIC", "reader", "editor", "publisher", "architect", "admin"}


def q_name(name: str) -> str:
    """Safely quote a label / relationship type / database name for Cypher."""
    return "`" + str(name).replace("`", "``") + "`"


# ---------------------------------------------------------------- property values
def enc(v):
    """Neo4j value -> JSON-safe value (special types are tagged so they come back exactly)."""
    from neo4j.spatial import Point
    from neo4j.time import Date, DateTime, Duration, Time
    if v is None or isinstance(v, (bool, int, float, str)):
        return v
    if isinstance(v, (list, tuple)):
        return [enc(x) for x in v]
    if isinstance(v, DateTime):
        return {"$t": "datetime", "v": v.iso_format()}
    if isinstance(v, Date):
        return {"$t": "date", "v": v.iso_format()}
    if isinstance(v, Time):
        return {"$t": "time", "v": v.iso_format()}
    if isinstance(v, Duration):
        return {"$t": "duration", "v": [v.months, v.days, v.seconds, v.nanoseconds]}
    if isinstance(v, Point):
        return {"$t": "point", "srid": v.srid, "v": list(v)}
    if isinstance(v, (bytes, bytearray)):
        return {"$t": "bytes", "v": base64.b64encode(bytes(v)).decode()}
    if isinstance(v, _dt.datetime):
        return {"$t": "datetime", "v": v.isoformat()}
    if isinstance(v, _dt.date):
        return {"$t": "date", "v": v.isoformat()}
    if isinstance(v, _dt.time):
        return {"$t": "time", "v": v.isoformat()}
    return {"$t": "text", "v": str(v)}


def dec(v):
    """JSON value -> Neo4j driver value (the reverse of enc)."""
    if isinstance(v, list):
        return [dec(x) for x in v]
    if not isinstance(v, dict) or "$t" not in v:
        return v
    from neo4j.spatial import CartesianPoint, WGS84Point
    from neo4j.time import Date, DateTime, Duration, Time
    t, val = v["$t"], v.get("v")
    if t == "datetime":
        return DateTime.from_iso_format(val)
    if t == "date":
        return Date.from_iso_format(val)
    if t == "time":
        return Time.from_iso_format(val)
    if t == "duration":
        m, d, s, ns = val
        return Duration(months=m, days=d, seconds=s, nanoseconds=ns)
    if t == "point":
        return (WGS84Point if v.get("srid") in (4326, 4979) else CartesianPoint)(tuple(val))
    if t == "bytes":
        return bytearray(base64.b64decode(val))
    return val


def _h(obj) -> int:
    return int(hashlib.md5(json.dumps(obj, sort_keys=True, separators=(",", ":")).encode()).hexdigest()[:16], 16)


class Neo4jConnector(Connector):
    name = "neo4j"
    env_prefix = "NEO"
    title = "Neo4j (graph database)"
    fields = [
        Field("URI", "Connection URI", required=True,
              help="AuraDB: neo4j+s://xxxxxxxx.databases.neo4j.io (instance card, or the credentials file you "
                   "downloaded). Desktop / own server: bolt://localhost:7687 or neo4j://host:7687."),
        Field("USER", "Username", default="neo4j", required=True,
              help="AuraDB: the username on the credentials screen / file - on newer instances it's the instance ID "
                   "(e.g. 2d6d18bd), on older ones neo4j."),
        Field("PASSWORD", "Password", secret=True, required=True,
              help="AuraDB: from the credentials .txt file downloaded when the instance was created."),
        Field("DATABASES", "Only these databases (comma separated)", sides=("SRC",), advanced=True,
              help="Leave empty to copy every database except 'system'."),
        Field("TARGET_DATABASE", "Database to restore into", sides=("TGT",), advanced=True,
              help="Leave empty: if the new Neo4j has only one database (AuraDB Free, Community), the backup goes "
                   "into it automatically. Otherwise the same names are used."),
        Field("BATCH_SIZE", "Rows per batch", default="5000", advanced=True,
              help="Lower it (e.g. 1000) if you see memory or timeout errors on a small instance."),
    ]

    # ------------------------------------------------------------------ connect
    def connect(self):
        from neo4j import GraphDatabase
        uri = self.env("URI", required=True)
        self.driver = GraphDatabase.driver(uri, auth=(self.env("USER", "neo4j", required=True),
                                                      self.env("PASSWORD", required=True)))
        self.driver.verify_connectivity()
        comp = self._run(None, "CALL dbms.components() YIELD name, versions, edition "
                               "RETURN versions[0] AS version, edition")
        self.version = str(comp[0]["version"]) if comp else "5"
        self.edition = str(comp[0].get("edition", "")) if comp else ""
        try:
            self.major = int(self.version.split(".")[0])
        except ValueError:
            self.major = 5
        self.idf = "elementId" if self.major >= 5 else "id"      # Neo4j 2025.x also counts as >= 5
        self.batch = max(100, int(self.env("BATCH_SIZE", "5000") or 5000))
        only = set(self.env_list("DATABASES"))
        self.dbs = [d for d in self._databases() if not only or d in only]

    def close(self):
        if getattr(self, "driver", None):
            self.driver.close()

    def _run(self, db, query, **params) -> list[dict]:
        with self.driver.session(database=db) as s:
            return [r.data() for r in s.run(query, params)]

    def _write(self, db, query, **params) -> None:
        with self.driver.session(database=db) as s:
            s.run(query, params).consume()

    def _stream(self, db, query):
        with self.driver.session(database=db) as s:
            for r in s.run(query):
                yield r.values()

    def _databases(self) -> list[str]:
        try:
            rows = self._run("system", "SHOW DATABASES")              # works on 4.4, 5.x and 2025.x
            return sorted({r["name"] for r in rows if r["name"] != "system" and r.get("type") != "system"})
        except Exception:
            return ["neo4j"]                                     # Community / restricted users: the default database

    # ---------------------------------------------------------------- plain errors
    def explain(self, err: Exception) -> str:
        if isinstance(err, Friendly):
            return str(err)
        t, low = str(err), str(err).lower()
        which = "old" if self.side == "SRC" else "new"
        if "unauthorized" in low or "authentication failure" in low or "autherror" in low or "credentials" in low:
            return (f"Wrong username or password for the {which} Neo4j. AuraDB: use the password from the credentials "
                    "file downloaded when the instance was created (or reset it on the instance card).")
        if "routing" in low or "serviceunavailable" in low or "couldn't connect" in low or "failed to establish" in low \
                or "connection refused" in low:
            return (f"Can't reach the {which} Neo4j. Check the Connection URI - AuraDB looks like "
                    "neo4j+s://xxxxxxxx.databases.neo4j.io, a local server bolt://localhost:7687. "
                    "AuraDB Free pauses when idle: open console.neo4j.io and Resume the instance.")
        if "certificate" in low or "ssl" in low:
            return ("Secure-connection problem. Use neo4j+s:// for AuraDB. For a server with a self-signed "
                    "certificate use neo4j+ssc:// or bolt+ssc://.")
        if "unsupported administration command" in low or ("create database" in low and "support" in low) \
                or "not available in community" in low:
            names = ", ".join(self._databases()) if getattr(self, "driver", None) else ""
            return ("This Neo4j can't create extra databases (AuraDB Free and Community Edition have only one). "
                    "Set New account > Advanced > 'Database to restore into' to its database name"
                    + (f": {names}." if names else " (AuraDB: usually the instance ID, or neo4j)."))
        if "database does not exist" in low or "databasenotfound" in low or "graph.notfound" in low:
            return f"That database doesn't exist in the {which} Neo4j. Run SHOW DATABASES; to see the names."
        if "constraintvalidationfailed" in low or "already exists with label" in low:
            return ("A uniqueness rule was broken while loading - usually because the target database wasn't empty. "
                    "Restore into an empty database.")
        if "memorypool" in low or "transaction.outofmemory" in low or "timeout" in low:
            return "The instance ran out of memory or time. Lower New account > Advanced > 'Rows per batch' (e.g. 1000)."
        return super().explain(err)

    # ----------------------------------------------------------------- discover
    def _counts(self, db) -> dict:
        labels = {r["label"]: r["c"] for r in self._run(
            db, "MATCH (n) UNWIND labels(n) AS label RETURN label, count(*) AS c")}
        types = {r["t"]: r["c"] for r in self._run(db, "MATCH ()-[r]->() RETURN type(r) AS t, count(*) AS c")}
        nodes = self._run(db, "MATCH (n) RETURN count(n) AS c")[0]["c"]
        rels = self._run(db, "MATCH ()-[r]->() RETURN count(r) AS c")[0]["c"]
        return {"nodes": nodes, "rels": rels, "labels": labels, "types": types}

    def discover(self) -> list[Resource]:
        res = []
        for db in self.dbs:
            c = self._counts(db)
            res.append(Resource("database", db, {"nodes": c["nodes"], "relationships": c["rels"],
                                                 "labels": len(c["labels"]), "relationship types": len(c["types"])}))
            res += [Resource("label", f"{db}:{k}", {"nodes": v}) for k, v in c["labels"].items()]
            res += [Resource("relationship type", f"{db}:{k}", {"count": v}) for k, v in c["types"].items()]
        return res

    def preflight(self) -> str:
        return f"Neo4j {self.version} {self.edition}".strip() + f", databases: {', '.join(self.dbs) or 'none'}"

    # ------------------------------------------------------------------ backup
    def _schema(self, db) -> list[dict]:
        out = []
        try:
            for c in self._run(db, "SHOW CONSTRAINTS YIELD *"):
                out.append({"kind": "constraint", "name": c.get("name"), "stmt": c.get("createStatement")})
        except Exception:
            pass
        try:
            for i in self._run(db, "SHOW INDEXES YIELD *"):
                if str(i.get("type", "")).upper() == "LOOKUP" or i.get("owningConstraint") or i.get("name") == TMP_INDEX:
                    continue                                   # built-in lookup indexes / ones a constraint creates
                out.append({"kind": "index", "name": i.get("name"), "stmt": i.get("createStatement")})
        except Exception:
            pass
        return out

    def _export(self, db, nodes_file=None, rels_file=None) -> dict:
        """Read every node and relationship once. Writes them (backup) and/or just fingerprints them (check)."""
        node_hash, fp_n, fp_r, n, m = {}, 0, 0, 0, 0
        nf = open(nodes_file, "w", encoding="utf-8") if nodes_file else None
        try:
            for rid, labels, props in self._stream(db, f"MATCH (n) RETURN {self.idf}(n), labels(n), properties(n)"):
                p = {k: enc(v) for k, v in props.items() if k != TMP_KEY}
                lab = sorted(l for l in labels if l != TMP_LABEL)
                if nf:
                    nf.write(json.dumps({"id": str(rid), "labels": lab, "props": p}) + "\n")
                h = _h([lab, p])
                node_hash[str(rid)] = h
                fp_n, n = (fp_n + h) % FP_MOD, n + 1
                if n % 50000 == 0:
                    self.log(f"{db}: {n:,} nodes read")
        finally:
            if nf:
                nf.close()
        rf = open(rels_file, "w", encoding="utf-8") if rels_file else None
        try:
            for rid, typ, s, e, props in self._stream(
                    db, f"MATCH (a)-[r]->(b) RETURN {self.idf}(r), type(r), {self.idf}(a), {self.idf}(b), properties(r)"):
                p = {k: enc(v) for k, v in props.items()}
                if rf:
                    rf.write(json.dumps({"type": typ, "s": str(s), "e": str(e), "props": p}) + "\n")
                fp_r, m = (fp_r + _h([typ, node_hash.get(str(s)), node_hash.get(str(e)), p])) % FP_MOD, m + 1
                if m % 50000 == 0:
                    self.log(f"{db}: {m:,} relationships read")
        finally:
            if rf:
                rf.close()
        return {"nodes": n, "rels": m, "fp_nodes": str(fp_n), "fp_rels": str(fp_r)}

    def backup(self, pkg: Path) -> list[Result]:
        base, R = pkg / "neo4j", []
        base.mkdir(parents=True, exist_ok=True)
        for db in self.dbs:
            d = base / safe_name(db)
            d.mkdir(exist_ok=True)
            try:
                self.log(f"Backing up database {db} ...")
                schema = self._schema(db)
                (d / "schema.json").write_text(json.dumps(schema, indent=2))
                stats = self._export(db, d / "nodes.jsonl", d / "rels.jsonl")
                stats.update(self._counts(db), name=db)
                (d / "stats.json").write_text(json.dumps(stats, indent=2))
                R.append(Result("database", db, OK, f"{stats['nodes']:,} nodes, {stats['rels']:,} relationships, "
                                                     f"{len(schema)} constraint(s)/index(es)"))
                no_stmt = [x["name"] for x in schema if not x.get("stmt")]
                if no_stmt:
                    R.append(Result("schema", db, MANUAL, "these couldn't be read as statements and must be recreated "
                                                          "by hand: " + ", ".join(map(str, no_stmt))))
            except Exception as e:
                R.append(Result("database", db, FAILED, self.explain(e)))
        (base / "server.json").write_text(json.dumps({"version": self.version, "edition": self.edition,
                                                      "databases": self.dbs}, indent=2))
        R += self._backup_security(base)
        R += self._backup_queries(pkg)
        return R

    def _is_aura(self) -> bool:
        return "neo4j.io" in (self.env("URI", "") or "")

    def _backup_queries(self, pkg: Path) -> list[Result]:
        """Saved Cypher / query tabs collected from the Aura website just before the backup (see neo4j_queries)."""
        import shutil
        from connectors import neo4j_queries as nq
        src = self.env("QUERIES_DIR")
        qs = nq.load(Path(src)) if src else []
        if qs:
            shutil.copytree(src, pkg / "neo4j_queries", dirs_exist_ok=True)
            return [Result("queries", "Aura Query tool", OK, nq.counts_text(qs) + " saved with the backup")]
        if self._is_aura():
            return [Result("queries", "Aura Query tool", MANUAL,
                           "your saved queries (Saved Cypher and query tabs) weren't collected. On the Back up page, "
                           "tick 'Also collect my saved queries from Aura' and back up again.")]
        return []

    def _backup_security(self, base: Path) -> list[Result]:
        """Enterprise / Aura Business Critical: users and custom roles with their privileges (as commands)."""
        users, roles = [], []
        try:
            me = self._run("system", "SHOW CURRENT USER YIELD user")[0]["user"]
            users = [u["user"] for u in self._run("system", "SHOW USERS YIELD user") if u["user"] not in (me, "neo4j")]
            for r in self._run("system", "SHOW ROLES YIELD role"):
                if r["role"] in BUILTIN_ROLES:
                    continue
                cmds = [x["command"] for x in self._run("system", f"SHOW ROLE {q_name(r['role'])} PRIVILEGES AS COMMANDS")]
                roles.append({"role": r["role"], "commands": cmds})
        except Exception:
            pass                                               # Community / Aura Free: no user management
        (base / "security.json").write_text(json.dumps({"users": users, "roles": roles}, indent=2))
        if users or roles:
            return [Result("security", f"{len(users)} user(s), {len(roles)} role(s)", OK,
                           "saved - restore gives the commands (passwords can't be read)")]
        return []

    # ----------------------------------------------------------------- restore
    def _target_name(self, db: str, n_backup_dbs: int = 1, existing=None) -> str:
        """Where a backed-up database goes. AuraDB names each database after its instance (old 2d6d18bd,
        new ec80891c) and Free / Community can't create another, so with one database on each side, use it."""
        if self.env("TARGET_DATABASE"):
            return self.env("TARGET_DATABASE")
        existing = set(self._databases()) if existing is None else existing
        if n_backup_dbs == 1 and db not in existing and len(existing) == 1:
            return next(iter(existing))
        return db

    def restore(self, pkg: Path) -> list[Result]:
        base, R = pkg / "neo4j", []
        dbs = [p for p in sorted(base.iterdir()) if p.is_dir()] if base.exists() else []
        if self.env("TARGET_DATABASE") and len(dbs) > 1:
            raise Friendly("This backup has several databases, but 'Database to restore into' sends everything to one. "
                           "Leave it empty, or back up one database at a time (old account > Advanced > Only these databases).")
        existing = set(self._databases())
        for d in dbs:
            stats = json.loads((d / "stats.json").read_text())
            src, tgt = stats["name"], self._target_name(stats["name"], len(dbs), existing)
            if tgt != src:
                self.log(f"Restoring '{src}' into the new account's database '{tgt}' ...")
            try:
                if tgt not in existing:
                    self.log(f"Creating database {tgt} ...")
                    self._write("system", f"CREATE DATABASE {q_name(tgt)} IF NOT EXISTS WAIT")
                if self._run(tgt, "MATCH (n) RETURN count(n) AS c")[0]["c"]:
                    fp = self._export(tgt)
                    if fp["fp_nodes"] == stats["fp_nodes"] and fp["fp_rels"] == stats["fp_rels"]:
                        R.append(Result("database", tgt, OK, f"already restored - {fp['nodes']:,} nodes and "
                                        f"{fp['rels']:,} relationships are identical to the backup. Nothing to do."))
                        continue
                    R.append(Result("database", tgt, FAILED,
                                    "the target database isn't empty, so nothing was loaded (it would create duplicates). "
                                    "Empty it, or choose another database, then restore again. To empty it, run in Neo4j "
                                    "Browser (this deletes everything in that database):",
                                    ":auto MATCH (n) CALL { WITH n DETACH DELETE n } IN TRANSACTIONS OF 10000 ROWS;"))
                    continue
                R += self._load_db(d, src, tgt, stats)
            except Exception as e:
                R.append(Result("database", tgt, FAILED, self.explain(e)))
        R += self._restore_security(base)
        R += self._restore_queries(pkg)
        return R

    def _restore_queries(self, pkg: Path) -> list[Result]:
        from connectors import neo4j_queries as nq
        folder = pkg / "neo4j_queries"
        qs = nq.load(folder)
        if not qs:
            return []
        files = [str((folder / f"all_{k}.cypher").resolve()) for k in nq.KIND_TITLES if (folder / f"all_{k}.cypher").exists()]
        return [Result("queries", "Aura Query tool", MANUAL,
                       f"{nq.counts_text(qs)} are ready to use. Neo4j doesn't let outside tools add them to the new "
                       "account's Saved Cypher list, so they're on the Restore page under 'Your saved queries' "
                       "(each with a copy button) and in these files:", "\n".join(files))]

    def _load_db(self, d: Path, src: str, tgt: str, stats: dict) -> list[Result]:
        R = []
        # 1. constraints and indexes first, so lookups are fast and rules are enforced while loading
        bad = []
        for x in json.loads((d / "schema.json").read_text()):
            if not x.get("stmt"):
                continue
            try:
                self._write(tgt, x["stmt"])
            except Exception as e:
                if "already exists" not in str(e).lower() and "equivalent" not in str(e).lower():
                    bad.append((x["stmt"], self.explain(e)))
        for stmt, why in bad:
            R.append(Result("schema", tgt, MANUAL, f"not created ({why[:150]}). Fix the cause, then run:", stmt))
        self._write(tgt, f"CREATE INDEX {TMP_INDEX} IF NOT EXISTS FOR (n:{TMP_LABEL}) ON (n.{TMP_KEY})")
        try:
            self._write(tgt, "CALL db.awaitIndexes(600)")
        except Exception:
            pass

        # 2. nodes, grouped by their exact set of labels (labels can't be parameters in Cypher)
        groups: dict = {}
        loaded = 0

        def flush(key):
            nonlocal loaded
            rows = groups.pop(key, [])
            if not rows:
                return
            lab = "".join(f":{q_name(l)}" for l in key)
            self._write(tgt, f"UNWIND $rows AS row CREATE (n{lab}:{TMP_LABEL} {{{TMP_KEY}: row.id}}) SET n += row.props",
                        rows=rows)
            loaded += len(rows)
            self.log(f"{tgt}: {loaded:,} of {stats['nodes']:,} nodes loaded")

        with open(d / "nodes.jsonl", encoding="utf-8") as f:
            for line in f:
                x = json.loads(line)
                key = tuple(x["labels"])
                groups.setdefault(key, []).append({"id": x["id"], "props": {k: dec(v) for k, v in x["props"].items()}})
                if len(groups[key]) >= self.batch:
                    flush(key)
        for key in list(groups):
            flush(key)

        # 3. relationships, grouped by type, joined to their nodes through the temporary id
        rgroups: dict = {}
        rl = 0

        def rflush(t):
            nonlocal rl
            rows = rgroups.pop(t, [])
            if not rows:
                return
            self._write(tgt, f"UNWIND $rows AS row MATCH (a:{TMP_LABEL} {{{TMP_KEY}: row.s}}) "
                             f"MATCH (b:{TMP_LABEL} {{{TMP_KEY}: row.e}}) "
                             f"CREATE (a)-[r:{q_name(t)}]->(b) SET r += row.props", rows=rows)
            rl += len(rows)
            self.log(f"{tgt}: {rl:,} of {stats['rels']:,} relationships loaded")

        with open(d / "rels.jsonl", encoding="utf-8") as f:
            for line in f:
                x = json.loads(line)
                rgroups.setdefault(x["type"], []).append({"s": x["s"], "e": x["e"],
                                                          "props": {k: dec(v) for k, v in x["props"].items()}})
                if len(rgroups[x["type"]]) >= self.batch:
                    rflush(x["type"])
        for t in list(rgroups):
            rflush(t)

        # 4. remove the temporary label, property and index
        while True:
            c = self._run(tgt, f"MATCH (n:{TMP_LABEL}) WITH n LIMIT $lim REMOVE n:{TMP_LABEL} REMOVE n.{TMP_KEY} "
                               "RETURN count(n) AS c", lim=self.batch)[0]["c"]
            if not c:
                break
        self._write(tgt, f"DROP INDEX {TMP_INDEX} IF EXISTS")
        where = f" into '{tgt}'" if tgt != src else ""
        R.append(Result("database", tgt, OK, f"{loaded:,} nodes and {rl:,} relationships loaded{where}"))
        return R

    def _restore_security(self, base: Path) -> list[Result]:
        f = base / "security.json"
        sec = json.loads(f.read_text()) if f.exists() else {}
        R = []
        if sec.get("roles"):
            lines = []
            for r in sec["roles"]:
                lines.append(f"CREATE ROLE {q_name(r['role'])} IF NOT EXISTS;")
                lines += [c.rstrip(";") + ";" for c in r["commands"]]
            R.append(Result("roles", f"{len(sec['roles'])} role(s)", MANUAL,
                            "custom roles and their privileges. Run these in the new Neo4j (system database):",
                            "\n".join(lines)))
        if sec.get("users"):
            R.append(Result("users", f"{len(sec['users'])} user(s)", MANUAL,
                            "users can't be copied with their passwords. Create each one (system database):",
                            "\n".join(f"CREATE USER {q_name(u)} IF NOT EXISTS SET PASSWORD '<choose a password>' "
                                      f"CHANGE REQUIRED;" for u in sec["users"])))
        return R

    # ---------------------------------------------------------------- validate
    def validate(self, pkg: Path) -> list[Result]:
        base, R = pkg / "neo4j", []
        dirs = sorted(p for p in base.iterdir() if p.is_dir())
        existing = set(self._databases())
        for d in dirs:
            old = json.loads((d / "stats.json").read_text())
            tgt = self._target_name(old["name"], len(dirs), existing)
            try:
                new = self._counts(tgt)
            except Exception as e:
                R.append(Result("database", tgt, FAILED, self.explain(e)))
                continue
            for kind, key in (("label", "labels"), ("relationship type", "types")):
                for name, cnt in old[key].items():
                    got = new[key].get(name, 0)
                    R.append(Result(kind, f"{tgt}:{name}", OK if got == cnt else FAILED, f"old {cnt:,} / new {got:,}"))
            fp = self._export(tgt)
            same_n, same_r = fp["fp_nodes"] == old["fp_nodes"], fp["fp_rels"] == old["fp_rels"]
            R.append(Result("database", tgt, OK if same_n and same_r else FAILED,
                            f"{fp['nodes']:,} nodes, {fp['rels']:,} relationships - "
                            + ("labels, properties and connections are identical (fingerprint match)" if same_n and same_r
                               else "data differs: " + ", ".join(x for x, ok in (("nodes", same_n), ("relationships", same_r))
                                                                if not ok))))
            names = {r.get("name") for r in self._run(tgt, "SHOW CONSTRAINTS YIELD name")} | \
                    {r.get("name") for r in self._run(tgt, "SHOW INDEXES YIELD name")}
            for x in json.loads((d / "schema.json").read_text()):
                R.append(Result(x["kind"], f"{tgt}:{x['name']}", OK if x["name"] in names else FAILED,
                                "present" if x["name"] in names else "missing"))
        return R
