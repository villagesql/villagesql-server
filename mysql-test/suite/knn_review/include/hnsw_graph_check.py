#!/usr/bin/env python3
"""Checks the links of an HNSW graph in an exported .ibd file.

usage: hnsw_graph_check.py <svector_page_dump> <ibd file>

For every link from node A to node B at the same level:
- B's slot is not free,
- B's owner vid is the vid stored with the link,
- B records the link: B links back to A, or lists A as incoming in its own
  list or its overflow chain,
- the vid is allocated in the vector store.
For every level 0 node, its own vid is allocated in the vector store.
For every node with a lower level node, that node has the same owner vid.

Prints the number of each kind of problem, then up to 10 examples of each.
The counts are 0 for a correct graph.
"""
import json
import subprocess
import sys

TOOL, IBD = sys.argv[1], sys.argv[2]


def page_json(root, page, index=True):
    cmd = [TOOL, IBD, str(root)] + (["-i"] if index else []) + [
        "--page-json", str(page)]
    out = subprocess.run(cmd, capture_output=True, text=True)
    if out.returncode != 0 or not out.stdout.startswith("{"):
        return None
    try:
        return json.loads(out.stdout)
    except json.JSONDecodeError:
        return None


def records(root):
    """nid -> record for every data page of the store at root."""
    recs = {}
    r = page_json(root, root)
    page = r["all_head"] if r else None
    seen = set()
    while page is not None and page not in seen:
        seen.add(page)
        d = page_json(root, page)
        if d is None or d.get("kind") != "data":
            break
        for rec in d["records"]:
            recs[f"{page}:{rec['i']}"] = rec
        page = d.get("next")
    return recs


# The level-0 HNSW root, and the vector store root(s), by type.
l0_root = None
vector_roots = []
for p in range(3, 200):
    r = page_json(p, p)
    if r is None or r.get("kind") != "root" or r.get("version") != 1:
        continue
    idx = r.get("index")
    if idx and idx.get("name") == "HNSW-L0":
        l0_root = r
    elif idx is None and r.get("column_size", 0) > 0:
        vector_roots.append(p)
if l0_root is None:
    print("no HNSW-L0 root page found")
    sys.exit(0)

vectors = {}
for p in vector_roots:
    vectors.update(records(p))

# level -> (primary records, overflow records)
levels = {}
for other in [{"page": l0_root["page"]}] + l0_root["other_roots"]:
    p = other["page"]
    if p is None:
        continue
    r = page_json(p, p)
    if r is None or not r.get("index"):
        continue
    lvl, ov = r["index"]["level"], r["index"]["overflow"]
    levels.setdefault(lvl, [{}, {}])[1 if ov else 0] = records(p)

problems = {"dangling": [], "aliased": [], "unrecorded": [],
            "vector freed": [], "lower mismatch": []}


def incoming_of(node, overflow_recs):
    """nids node records as pointing at it: its own list and overflow chain."""
    nids = {n["nid"] for n in node.get("neighbours", [])}
    cur, seen = node.get("overflow"), set()
    while cur is not None and cur not in seen:
        seen.add(cur)
        ov = overflow_recs.get(cur)
        if ov is None or ov["s"] == "F":
            break
        nids.update(ov.get("incoming", []))
        cur = ov.get("overflow")
    return nids


for lvl, (prim, ovf) in sorted(levels.items()):
    for a_nid, a in prim.items():
        if a["s"] == "F":
            continue
        if lvl == 0 and vectors.get(a.get("vid"), {"s": "F"})["s"] == "F":
            problems["vector freed"].append(f"L0 {a_nid} owns vid {a.get('vid')}")
        if a.get("lower") is not None and lvl > 0:
            low = levels.get(lvl - 1, [{}, {}])[0].get(a["lower"])
            if low is None or low["s"] == "F" or low.get("vid") != a["vid"]:
                problems["lower mismatch"].append(
                    f"L{lvl} {a_nid} vid {a['vid']} lower {a['lower']}")
        for link in a.get("neighbours", []):
            if link["incoming"]:
                continue
            b_nid, vid = link["nid"], link["vid"]
            where = f"L{lvl} {a_nid} -> {b_nid} (vid {vid})"
            b = prim.get(b_nid)
            if b is None or b["s"] == "F":
                problems["dangling"].append(where)
                continue
            if b.get("vid") != vid:
                problems["aliased"].append(where + f", target vid {b.get('vid')}")
            if a_nid not in incoming_of(b, ovf):
                problems["unrecorded"].append(where)
            v = vectors.get(vid)
            if v is None or v["s"] == "F":
                problems["vector freed"].append(where)

for kind, found in problems.items():
    print(f"{kind}: {len(found)}")
for kind, found in problems.items():
    for line in found[:10]:
        print(f"  {kind}: {line}")
