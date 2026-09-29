"""Fast path: resolve WoC commit fields from local git clones.

A commit's fields (author, author-time, message) are immutable properties of its
sha1 -- identical whether read from WoC or from a git clone. So instead of asking
the throttled WoC API for every commit, we clone each repo (blobless, no checkout)
plus its pull-request refs, then resolve the WoC `p2c` sha list locally with one
`git log`. Any sha not present in git (a tiny remainder) is written to a misses
file for retrieve.py to fetch from WoC.

Result schema matches retrieve.py exactly: project_wocid, commit_sha1, author,
time, commit message.
"""
import json, os, subprocess, sys
import pandas as pd

CACHE = "woc_cache"
REPODIR = os.path.join(os.environ.get("TEMP", "."), "mp2_gitrepos")
os.makedirs(REPODIR, exist_ok=True)

US, RS = "\x1f", "\x1e"  # field / record separators (won't appear in git data)

# wocid -> github clone url
REPOS = {
    "panoptes_panoptes-utils": "https://github.com/panoptes/panoptes-utils.git",
    "photrek_nonlinear-statistical-coupling": "https://github.com/Photrek/Nonlinear-Statistical-Coupling.git",
    "npellet_visualizer": "https://github.com/NPellet/visualizer.git",
    "darioizzo_audi": "https://github.com/darioizzo/audi.git",
    "milvus-io_milvus": "https://github.com/milvus-io/milvus.git",
    "antelopeusersgroup_antelope_contrib": "https://github.com/antelopeusersgroup/antelope_contrib.git",
    "oceandatatools_openvdm": "https://github.com/OceanDataTools/openvdm.git",
    "fourierflows_fourierflows.jl": "https://github.com/FourierFlows/FourierFlows.jl.git",
    "gnina_libmolgrid": "https://github.com/gnina/libmolgrid.git",
    "unidata_awips2": "https://github.com/Unidata/awips2.git",
}


def run(args, **kw):
    return subprocess.run(args, capture_output=True, text=True, encoding="utf-8", errors="replace", **kw)


def ensure_clone(wocid, url):
    path = os.path.join(REPODIR, wocid)
    if not os.path.isdir(os.path.join(path, ".git")) and not os.path.isdir(os.path.join(path, "objects")):
        print(f"[{wocid}] cloning (blobless, no checkout)...", flush=True)
        r = run(["git", "clone", "--filter=blob:none", "--no-checkout", "--quiet", url, path])
        if r.returncode:
            print(f"[{wocid}] clone FAILED: {r.stderr[:300]}", flush=True)
            return None
    print(f"[{wocid}] fetching PR head+merge refs...", flush=True)
    run(["git", "-C", path, "fetch", "--filter=blob:none", "--quiet", "origin",
         "+refs/pull/*/head:refs/pull/*", "+refs/pull/*/merge:refs/pullmerge/*"])
    return path


def build_map(path):
    """sha -> (author, author_time, message) for every commit reachable from any ref."""
    fmt = f"%H{US}%an <%ae>{US}%at{US}%B{RS}"
    p = run(["git", "-C", path, "log", "--all", "--no-color", f"--format={fmt}"])
    m = {}
    for rec in p.stdout.split(RS):
        rec = rec.strip("\n")
        if not rec:
            continue
        parts = rec.split(US)
        if len(parts) < 4:
            continue
        sha, author, at, msg = parts[0].strip(), parts[1], parts[2], parts[3]
        m[sha] = (author, at, msg)
    return m


def resolve(wocid):
    shas = json.load(open(f"{CACHE}/{wocid}_p2c.json", encoding="utf-8"))["data"]
    path = ensure_clone(wocid, REPOS[wocid])
    if not path:
        return
    gm = build_map(path)
    rows, misses = [], []
    for s in shas:
        if s in gm:
            a, t, msg = gm[s]
            rows.append({"project_wocid": wocid, "commit_sha1": s,
                         "author": a, "time": int(t), "commit message": msg})
        else:
            misses.append(s)
    pd.DataFrame(rows).to_csv(f"{CACHE}/{wocid}_commits.csv", index=False)
    with open(f"{CACHE}/{wocid}_misses.txt", "w", encoding="utf-8") as f:
        f.write("\n".join(misses))
    print(f"[{wocid}] resolved {len(rows)}/{len(shas)} locally, {len(misses)} misses -> WoC", flush=True)


if __name__ == "__main__":
    names = sys.argv[1:] or list(REPOS)
    for w in names:
        resolve(w)
