"""Standalone WoC commit retriever (raw HTTP, no python-woc build needed).

Reads cached p2c sha lists from woc_cache/<proj>_p2c.json and fetches commit
content via the WoC batch API, writing woc_cache/<proj>_commits.csv.

Resilient to the WoC public API being overloaded (429 / 5xx / 522 Cloudflare
timeouts): honors Retry-After with a global backoff, re-queues failed batches,
and runs repeated passes until every sha is captured. Fully resumable.
"""
import asyncio, json, os, sys, time
import httpx
import pandas as pd

BASE = "https://worldofcode.org/api"
CACHE = "woc_cache"
BATCH = 10          # keys per request (WoC batch API caps at 10)
CONCURRENCY = 20    # parallel in-flight requests
FLUSH_EVERY = 50    # checkpoint the CSV every N completed batches
MAX_BACKOFF = 60    # cap the server-requested backoff (Retry-After) at this
MAX_PASSES = 8      # how many times to sweep batches that failed (server errors)

PROJECTS = [
    "panoptes_panoptes-utils", "photrek_nonlinear-statistical-coupling",
    "npellet_visualizer", "darioizzo_audi", "milvus-io_milvus",
    "antelopeusersgroup_antelope_contrib", "oceandatatools_openvdm",
    "fourierflows_fourierflows.jl", "gnina_libmolgrid", "unidata_awips2",
]


def load_shas(proj):
    with open(f"{CACHE}/{proj}_p2c.json", encoding="utf-8") as f:
        return json.load(f)["data"]


def load_done(proj):
    path = f"{CACHE}/{proj}_commits.csv"
    if os.path.exists(path):
        df = pd.read_csv(path, dtype={"commit_sha1": str})
        return df.to_dict("records"), set(df["commit_sha1"].astype(str))
    return [], set()


class RateGate:
    """Global pause switch honored by all workers when the server pushes back."""
    def __init__(self):
        self.until = 0.0
    def pause(self, secs):
        self.until = max(self.until, time.time() + min(secs, MAX_BACKOFF))
    async def wait(self):
        d = self.until - time.time()
        if d > 0:
            await asyncio.sleep(d)


async def fetch_batch(client, sem, chunk, gate):
    """Return (data_dict, ok). ok=False means the batch should be retried later.

    WoC's origin intermittently hangs ~19s (Cloudflare 522). Rather than wait it
    out, we use a short per-request timeout and retry fast -- the retry almost
    always lands on a healthy, sub-second response. Only an explicit 429 triggers
    a real (global) backoff.
    """
    url = "/lookup/map/commit.tch?" + "&".join(f"q={k}" for k in chunk)
    for attempt in range(10):
        await gate.wait()
        async with sem:
            try:
                r = await client.get(url, timeout=6.0)
            except Exception:
                continue  # timeout/connect error -> retry immediately
        if r.status_code == 200:
            try:
                return r.json().get("data", {}), True
            except Exception:
                continue
        if r.status_code == 429:                 # true rate limit: back off globally
            gate.pause(int(r.headers.get("Retry-After", 15)))
            continue
        if r.status_code >= 500:                 # 522/503 transient origin hiccup: retry fast
            await asyncio.sleep(0.3)
            continue
        return {}, True                          # other 4xx: keys not in WoC, settle them
    return {}, False


def flatten(proj, data):
    rows = []
    for sha, val in data.items():
        c = val[0] if val else None
        if not c:
            continue
        rows.append({
            "project_wocid": proj, "commit_sha1": sha,
            "author": c[2][0], "time": int(c[2][1]), "commit message": c[4],
        })
    return rows


async def do_project(proj):
    all_shas = load_shas(proj)
    rows, done = load_done(proj)          # done = shas actually retrieved
    processed = set(done)                 # retrieved OR confirmed-unavailable (200 w/o content)
    path = f"{CACHE}/{proj}_commits.csv"
    sem = asyncio.Semaphore(CONCURRENCY)
    gate = RateGate()

    async with httpx.AsyncClient(base_url=BASE, timeout=6.0) as client:
        for p in range(MAX_PASSES):
            todo = [s for s in all_shas if s not in processed]
            print(f"[{proj}] pass {p}: have {len(done)}/{len(all_shas)}, retry {len(todo)}", flush=True)
            if not todo:
                break
            chunks = [todo[i:i + BATCH] for i in range(0, len(todo), BATCH)]
            n = 0
            for w in range(0, len(chunks), FLUSH_EVERY):
                wave = chunks[w:w + FLUSH_EVERY]
                results = await asyncio.gather(*[fetch_batch(client, sem, c, gate) for c in wave])
                for chunk, (data, ok) in zip(wave, results):
                    if not ok:
                        continue              # server error -> leave for next pass
                    new = flatten(proj, data)
                    rows.extend(new)
                    done.update(r["commit_sha1"] for r in new)
                    processed.update(chunk)   # every sha in a 200 response is settled
                n += len(wave)
                pd.DataFrame(rows).drop_duplicates("commit_sha1").to_csv(path, index=False)
                print(f"[{proj}]   {n}/{len(chunks)} batches, have {len(done)}/{len(all_shas)}", flush=True)
            # only sleep between passes if server errors left work behind
            if [s for s in all_shas if s not in processed] and p < MAX_PASSES - 1:
                await asyncio.sleep(3)

    got, want, unavail = len(done), len(all_shas), len(processed) - len(done)
    tail = "" if got == want else f"  ({unavail} sha1s unavailable in WoC, {want-len(processed)} still failing)"
    print(f"[{proj}] DONE retrieved {got}/{want}{tail}", flush=True)


async def main(names):
    for proj in names:
        await do_project(proj)


if __name__ == "__main__":
    args = sys.argv[1:]
    asyncio.run(main(args if args else PROJECTS))
