"""Assemble the final Part-1 deliverable and print the WoC statistics table.

Combines the per-project commit caches (populated by git_resolve.py + retrieve.py)
into jzr266_project_summary.csv and reports, per project, the WoC statistics:
number of commits, number of authors, earliest and latest commit time.
"""
import json, os
import pandas as pd

CACHE = "woc_cache"
NETID = "jzr266"
PROJECTS = [
    "panoptes_panoptes-utils", "photrek_nonlinear-statistical-coupling",
    "npellet_visualizer", "darioizzo_audi", "milvus-io_milvus",
    "antelopeusersgroup_antelope_contrib", "oceandatatools_openvdm",
    "fourierflows_fourierflows.jl", "gnina_libmolgrid", "unidata_awips2",
]
COLS = ["project_wocid", "commit_sha1", "author", "time", "commit message"]

parts, stat_rows = [], []
for p in PROJECTS:
    df = pd.read_csv(f"{CACHE}/{p}_commits.csv", dtype={"commit_sha1": str})
    df = df.drop_duplicates("commit_sha1")
    df["project_wocid"] = p
    parts.append(df[COLS])
    p2c = len(json.load(open(f"{CACHE}/{p}_p2c.json", encoding="utf-8"))["data"])
    stat_rows.append({
        "project": p,
        "ncommits": p2c,                      # authoritative WoC commit count (p2c)
        "retrieved": len(df),                 # rows we have content for
        "nauthors": df["author"].nunique(),
        "min_time": pd.to_datetime(df["time"].min(), unit="s").strftime("%Y-%m-%d"),
        "max_time": pd.to_datetime(df["time"].max(), unit="s").strftime("%Y-%m-%d"),
    })

summary = pd.concat(parts, ignore_index=True)
out = f"{NETID}_project_summary.csv"
summary.to_csv(out, sep=";", index=False, header=False)
print(f"wrote {out}: {len(summary)} rows across {summary['project_wocid'].nunique()} projects\n")

stats = pd.DataFrame(stat_rows)
print(stats.to_string(index=False))
stats.to_csv(f"{CACHE}/woc_stats.csv", index=False)
print(f"\ncompleteness: {stats['retrieved'].sum()}/{stats['ncommits'].sum()} commits "
      f"({100*stats['retrieved'].sum()/stats['ncommits'].sum():.1f}%)")
