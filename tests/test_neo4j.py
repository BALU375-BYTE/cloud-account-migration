"""Offline tests for the Neo4j connector, using a small in-memory pretend Neo4j."""
import re
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from connectors import neo4j_connector as nc  # noqa: E402

BT = r"`((?:[^`]|``)+)`"


def unq(x):
    return x.replace("``", "`")


class Rec(dict):
    def data(self): return dict(self)
    def values(self): return list(dict.values(self))


class Res(list):
    def consume(self): return None


class FakeGraph:
    def __init__(self, dbs, edition="enterprise", version="5.20.0"):
        self.dbs = {n: {"nodes": {}, "rels": [], "schema": []} for n in dbs}
        self.edition, self.version, self.n = edition, version, 0

    def add_node(self, db, labels, props):
        self.n += 1
        nid = f"4:db:{self.n}"
        self.dbs[db]["nodes"][nid] = {"labels": list(labels), "props": dict(props)}
        return nid

    def add_rel(self, db, typ, s, e, props):
        self.n += 1
        self.dbs[db]["rels"].append({"id": f"5:db:{self.n}", "type": typ, "s": s, "e": e, "props": dict(props)})

    def run(self, db, q, p):
        db = db or "neo4j"
        if q.startswith("CALL dbms.components()"):
            return [Rec(version=self.version, edition=self.edition)]
        if q.startswith("SHOW DATABASES"):
            return [Rec(name=n, type="standard") for n in self.dbs] + [Rec(name="system", type="system")]
        if q.startswith("CREATE DATABASE"):
            if self.edition == "community":
                raise Exception("Neo.ClientError.Statement.UnsupportedAdministrationCommand: Unsupported administration command")
            self.dbs.setdefault(unq(re.search(BT, q).group(1)), {"nodes": {}, "rels": [], "schema": []})
            return []
        if q.startswith("SHOW CURRENT USER") or q.startswith("SHOW USERS") or q.startswith("SHOW ROLE"):
            raise Exception("not available")
        g = self.dbs[db]
        if q == "SHOW CONSTRAINTS YIELD *":
            return [Rec(x) for x in g["schema"] if x["kind"] == "constraint"]
        if q == "SHOW INDEXES YIELD *":
            return [Rec(name="index_lookup", type="LOOKUP", createStatement="CREATE LOOKUP INDEX ...")] + \
                   [Rec(x) for x in g["schema"] if x["kind"] == "index"]
        if q in ("SHOW CONSTRAINTS YIELD name", "SHOW INDEXES YIELD name"):
            k = "constraint" if "CONSTRAINTS" in q else "index"
            return [Rec(name=x["name"]) for x in g["schema"] if x["kind"] == k]
        if q.startswith("CREATE INDEX cam_tmp_id") or q.startswith("CALL db.awaitIndexes") or q.startswith("DROP INDEX"):
            return []
        if q.startswith("CREATE CONSTRAINT") or q.startswith("CREATE FULLTEXT INDEX") or q.startswith("CREATE INDEX"):
            name = q.split()[2] if not q.startswith("CREATE FULLTEXT") else q.split()[3]
            g["schema"].append({"kind": "constraint" if "CONSTRAINT" in q else "index", "name": name,
                                "createStatement": q, "type": "RANGE"})
            return []
        nodes = {k: v for k, v in g["nodes"].items()}
        if q.startswith("MATCH (n) UNWIND labels(n)"):
            c = {}
            for v in nodes.values():
                for l in v["labels"]:
                    c[l] = c.get(l, 0) + 1
            return [Rec(label=k, c=v) for k, v in c.items()]
        if q.startswith("MATCH ()-[r]->() RETURN type(r)"):
            c = {}
            for r in g["rels"]:
                c[r["type"]] = c.get(r["type"], 0) + 1
            return [Rec(t=k, c=v) for k, v in c.items()]
        if q == "MATCH (n) RETURN count(n) AS c":
            return [Rec(c=len(nodes))]
        if q == "MATCH ()-[r]->() RETURN count(r) AS c":
            return [Rec(c=len(g["rels"]))]
        if q.startswith("MATCH (n) RETURN elementId(n)"):
            return [Rec(id=k, labels=list(v["labels"]), props=dict(v["props"])) for k, v in nodes.items()]
        if q.startswith("MATCH (a)-[r]->(b) RETURN elementId(r)"):
            return [Rec(id=r["id"], t=r["type"], s=r["s"], e=r["e"], props=dict(r["props"])) for r in g["rels"]]
        if q.startswith("UNWIND $rows AS row CREATE (n"):
            part = q[q.index("(n") + 2:q.index(f":{nc.TMP_LABEL}")]
            labels = [unq(x) for x in re.findall(BT, part)]
            for row in p["rows"]:
                props = dict(row["props"]); props[nc.TMP_KEY] = row["id"]
                self.add_node(db, labels + [nc.TMP_LABEL], props)
            return []
        if q.startswith("UNWIND $rows AS row MATCH (a:"):
            typ = unq(re.search(r"\[r:" + BT + r"\]", q).group(1))
            by_tmp = {v["props"].get(nc.TMP_KEY): k for k, v in g["nodes"].items()}
            for row in p["rows"]:
                self.add_rel(db, typ, by_tmp[row["s"]], by_tmp[row["e"]], row["props"])
            return []
        if q.startswith(f"MATCH (n:{nc.TMP_LABEL}) WITH n LIMIT $lim"):
            done = 0
            for v in g["nodes"].values():
                if nc.TMP_LABEL in v["labels"] and done < p["lim"]:
                    v["labels"].remove(nc.TMP_LABEL); v["props"].pop(nc.TMP_KEY, None); done += 1
            return [Rec(c=done)]
        raise AssertionError(f"unexpected query: {q}")


class FakeDriver:
    def __init__(self, graph): self.g = graph
    def verify_connectivity(self): pass
    def close(self): pass

    def session(self, database=None):
        g = self.g

        class S:
            def __enter__(self): return self
            def __exit__(self, *a): pass
            def run(self, q, params=None): return Res(g.run(database, q, params or {}))
        return S()


def sample_source():
    from neo4j.spatial import CartesianPoint, WGS84Point
    from neo4j.time import Date, DateTime, Duration
    g = FakeGraph(["neo4j", "movies"])
    a = g.add_node("neo4j", ["Person"], {"name": "Asha", "born": Date(1990, 5, 17),
                                         "seen": DateTime.from_iso_format("2026-11-16T10:20:30.123456789+05:30"),
                                         "home": WGS84Point((77.59, 12.97)), "tags": ["a", "b"],
                                         "avatar": bytearray(b"\x00\x01png"), "score": 9.5, "active": True})
    b = g.add_node("neo4j", ["Company", "Odd `Label"], {"name": "Akira", "hq": CartesianPoint((1.0, 2.0, 3.0))})
    g.add_node("neo4j", [], {"note": "no labels at all"})
    g.add_rel("neo4j", "WORKS_AT", a, b, {"since": Date(2020, 1, 1), "notice": Duration(months=2, days=3)})
    g.add_rel("neo4j", "KNOWS ME", a, a, {})
    m = g.add_node("movies", ["Movie"], {"title": "Matrix"})
    g.add_rel("movies", "SEQUEL_OF", m, m, {})
    g.dbs["neo4j"]["schema"] = [
        {"kind": "constraint", "name": "person_name", "type": "UNIQUENESS",
         "createStatement": "CREATE CONSTRAINT person_name FOR (n:Person) REQUIRE n.name IS UNIQUE"},
        {"kind": "index", "name": "person_name", "type": "RANGE", "owningConstraint": "person_name",
         "createStatement": "CREATE RANGE INDEX person_name ..."},
        {"kind": "index", "name": "company_ft", "type": "FULLTEXT",
         "createStatement": "CREATE FULLTEXT INDEX company_ft FOR (n:Company) ON EACH [n.name]"}]
    return g


def conn(side, graph, monkeypatch, **env):
    import neo4j
    monkeypatch.setattr(neo4j.GraphDatabase, "driver", lambda uri, auth: FakeDriver(graph))
    monkeypatch.setenv(f"{side}_NEO_URI", "neo4j+s://x.databases.neo4j.io")
    monkeypatch.setenv(f"{side}_NEO_PASSWORD", "pw")
    for k, v in env.items():
        monkeypatch.setenv(f"{side}_NEO_{k}", v)
    c = nc.Neo4jConnector(side)
    c.connect()
    return c


def test_values_survive_json_exactly():
    from neo4j.spatial import WGS84Point
    from neo4j.time import Date, DateTime, Duration, Time
    vals = [DateTime.from_iso_format("2026-11-16T10:20:30.5+05:30"), DateTime.from_iso_format("2026-11-16T10:20:30"),
            Date(2026, 1, 2), Time.from_iso_format("09:00:00+01:00"), Duration(months=1, seconds=5),
            WGS84Point((1.5, 2.5)), bytearray(b"\x00\xff"), [1, 2.5, "x"], True, None]
    import json
    for v in vals:
        assert nc.enc(nc.dec(json.loads(json.dumps(nc.enc(v))))) == nc.enc(v), v


def test_full_backup_restore_check(tmp_path, monkeypatch):
    src_g = sample_source()
    src = conn("SRC", src_g, monkeypatch)
    assert src.dbs == ["movies", "neo4j"]
    res = src.backup(tmp_path)
    assert not [r for r in res if r.status == "FAILED"], res
    schema = (tmp_path / "neo4j/neo4j/schema.json").read_text()
    assert "company_ft" in schema and "index_lookup" not in schema          # lookup + constraint-owned index skipped

    tgt_g = FakeGraph(["neo4j"])
    tgt = conn("TGT", tgt_g, monkeypatch)
    res = tgt.restore(tmp_path)
    assert not [r for r in res if r.status == "FAILED"], res
    assert "movies" in tgt_g.dbs                                             # created because Enterprise allows it
    assert all(nc.TMP_LABEL not in v["labels"] and nc.TMP_KEY not in v["props"]
               for d in tgt_g.dbs.values() for v in d["nodes"].values())    # temporary helpers removed
    asha = next(v for v in tgt_g.dbs["neo4j"]["nodes"].values() if v["props"].get("name") == "Asha")
    assert str(asha["props"]["seen"].iso_format()).endswith("+05:30") and asha["props"]["avatar"] == bytearray(b"\x00\x01png")
    odd = next(v for v in tgt_g.dbs["neo4j"]["nodes"].values() if v["props"].get("name") == "Akira")
    assert "Odd `Label" in odd["labels"]

    v = tgt.validate(tmp_path)
    bad = [r for r in v if r.status != "OK"]
    assert not bad, bad
    assert any("fingerprint match" in r.message for r in v)

    # a changed property must be caught by the fingerprint
    asha["props"]["score"] = 1.0
    v2 = tgt.validate(tmp_path)
    assert any(r.status == "FAILED" and "data differs" in r.message for r in v2)


def test_refuses_non_empty_target_and_single_db_targets(tmp_path, monkeypatch):
    src = conn("SRC", sample_source(), monkeypatch, DATABASES="neo4j")
    src.backup(tmp_path)
    busy = FakeGraph(["neo4j"]); busy.add_node("neo4j", ["X"], {})
    res = conn("TGT", busy, monkeypatch).restore(tmp_path)
    r = [x for x in res if x.kind == "database"][0]
    assert r.status == "FAILED" and "isn't empty" in r.message and "DETACH DELETE" in r.action
    community = FakeGraph(["neo4j"], edition="community")
    res = conn("TGT", community, monkeypatch, TARGET_DATABASE="neo4j").restore(tmp_path)
    assert not [x for x in res if x.status == "FAILED"], res


def test_errors_in_plain_language():
    c = nc.Neo4jConnector("TGT")
    assert "Database to restore into" in c.explain(Exception("Unsupported administration command: CREATE DATABASE"))
    assert "Resume" in c.explain(Exception("Unable to retrieve routing information"))
    assert "credentials file" in c.explain(Exception("Neo.ClientError.Security.Unauthorized: authentication failure"))


def test_aura_instance_named_databases_restore_automatically(tmp_path, monkeypatch):
    """New AuraDB: the old database is named after the old instance, the new one after the new instance."""
    old = sample_source()
    old.dbs = {"2d6d18bd": old.dbs["neo4j"]}
    res = conn("SRC", old, monkeypatch).backup(tmp_path)
    assert not [r for r in res if r.status == "FAILED"], res
    new = FakeGraph(["ec80891c"], edition="community")
    monkeypatch.delenv("TGT_NEO_TARGET_DATABASE", raising=False)
    res = conn("TGT", new, monkeypatch).restore(tmp_path)
    assert not [r for r in res if r.status == "FAILED"], res
    assert new.dbs["ec80891c"]["nodes"] and "2d6d18bd" not in new.dbs


def test_restore_twice_and_check_with_instance_named_databases(tmp_path, monkeypatch):
    old = sample_source()
    old.dbs = {"2d6d18bd": old.dbs["neo4j"]}
    conn("SRC", old, monkeypatch).backup(tmp_path)
    new = FakeGraph(["ec80891c"], edition="community")
    monkeypatch.delenv("TGT_NEO_TARGET_DATABASE", raising=False)
    conn("TGT", new, monkeypatch).restore(tmp_path)
    again = conn("TGT", new, monkeypatch).restore(tmp_path)        # pressing Restore a second time
    db = [r for r in again if r.kind == "database"][0]
    assert db.status == "OK" and "already restored" in db.message
    res = conn("TGT", new, monkeypatch).validate(tmp_path)         # Check finds the right database too
    assert res and not [r for r in res if r.status == "FAILED"], [r for r in res if r.status == "FAILED"]
