"""extra: replace partial weekly picks (fewer items than the median of their month) by the best version of the same ISO week.
Writes backfill_versions_extra.json and slices/all_items_extra.json; the existing backfill_versions.json is not touched."""
import json, statistics, collections, datetime as dt, argparse
import make_slices as m
ap = argparse.Namespace(latest=721, cache_dir=None)
dates = m.one_version_per_date(m.version_dates(721))
base = json.load(open("backfill_versions.json"))["versions"]
have = {v["version"] for v in base}
weekly = [v for v in base if v["kinds"] == m.KINDS_PRICE_STORE and v["version"] != 720]
bymonth = collections.defaultdict(list)
for v in weekly: bymonth[v["version_date"][:7]].append(v["n_items"])
med = {k: statistics.median(x) for k, x in bymonth.items()}
def nprice(v):
    its = m.items_for(v, dates[v], m.KINDS_PRICE_STORE)
    return len({i["chain"] for i in its if i["kind"] == "price_full_file"}), its
out, items, report = [], [], []
order = max(v["load_order"] for v in base)
for w in weekly:
    if w["n_items"] >= med[w["version_date"][:7]] and w["version"] not in (81, 83, 89, 93): continue
    d = dt.date.fromisoformat(w["version_date"]); wk = d.isocalendar()[:2]
    cands = [(v, dd) for v, dd in dates.items() if dd.isocalendar()[:2] == wk and v not in have]
    best = None
    for v, dd in cands:
        n, its = nprice(v)
        if best is None or (n, v) > (best[0], best[1]): best = (n, v, its)
    cur = nprice(w["version"])[0]
    if best and best[0] > cur:
        order += 1
        out.append({"version": best[1], "version_date": dates[best[1]].isoformat(), "load_order": order,
                    "kinds": m.KINDS_PRICE_STORE, "n_items": len(best[2]), "raw_bytes": sum(i["size"] for i in best[2]),
                    "replaces_version": w["version"]})
        items += best[2]; report.append((w["version"], best[1], cur, best[0]))
    else: report.append((w["version"], None, cur, best[0] if best else None))
json.dump({"note": "better versions for partial weekly slots; load after the main list", "versions": out}, open("backfill_versions_extra.json", "w"), indent=1)
json.dump(items, open("slices/all_items_extra.json", "w"))
for r in report: print(r)
print(len(out), "replacements", len(items), "items", sum(i["size"] for i in items) / 1e9, "GB raw")
