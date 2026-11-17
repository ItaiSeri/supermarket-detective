"""Generate holidays.csv, the Jewish/Israeli holiday seed for gold.dim_date.

Run on the laptop only (Databricks just reads the CSV, so the cluster needs no library):
    pip install convertdate hdate holidays tzdata
    python databricks/seed/make_holidays.py

Source of dates: `convertdate` (Hebrew calendar arithmetic). Every date is then cross-checked
against two independent sources: `hdate` (day-by-day holiday names for Israel) and `holidays`
(Israel public holidays). Any disagreement is printed and the script exits with an error.

start_date = the EVE: the calendar date whose evening the holiday begins, because shoppers buy
             before. For Independence Day it is Memorial Day (the evening before the observed day).
end_date   = the last day of the holiday (the holiday ends at nightfall of that date).
"""
import csv
import datetime as dt
import pathlib
import sys
from collections import defaultdict

from convertdate import hebrew

FIRST, LAST = dt.date(2024, 9, 1), dt.date(2027, 12, 31)
ONE = dt.timedelta(days=1)

# Hebrew months in convertdate: Nisan 1 ... Elul 6, Tishri 7, Heshvan 8, Kislev 9, Tevet 10,
# Shevat 11, Adar 12 (Adar I in a leap year), Adar II 13 (leap years only).
NISAN, IYAR, SIVAN, TISHRI, KISLEV = 1, 2, 3, 7, 9


def g(hy, hm, hd):
    y, m, d = hebrew.to_gregorian(hy, hm, hd)
    return dt.date(y, m, d)


def purim_month(hy):
    return 13 if hebrew.leap(hy) else 12  # Purim is in Adar II in a leap year


def independence_day(hy):
    """5 Iyar, moved so that Memorial Day / Independence Day avoid Shabbat (Israeli rule since 2004)."""
    d = g(hy, IYAR, 5)
    wd = d.weekday()  # Mon=0 ... Sun=6
    if wd == 4:  # Friday -> Thursday
        return d - ONE
    if wd == 5:  # Saturday -> Thursday
        return d - 2 * ONE
    if wd == 0:  # Monday -> Tuesday
        return d + ONE
    return d


def holidays_for(gy):
    """Holidays of Gregorian year gy: (name, name_he, eve, last_day)."""
    hy_autumn = gy + 3761   # Tishri and Kislev fall in this Hebrew year
    hy_spring = gy + 3760   # Nisan..Sivan and Adar fall in this one
    pm = purim_month(hy_spring)
    ind = independence_day(hy_spring)
    h1 = g(hy_autumn, KISLEV, 25)
    return [
        ("purim", "פורים", g(hy_spring, pm, 14) - ONE, g(hy_spring, pm, 14)),
        ("passover", "פסח", g(hy_spring, NISAN, 15) - ONE, g(hy_spring, NISAN, 21)),
        ("independence_day", "יום העצמאות", ind - ONE, ind),
        ("shavuot", "שבועות", g(hy_spring, SIVAN, 6) - ONE, g(hy_spring, SIVAN, 6)),
        ("rosh_hashana", "ראש השנה", g(hy_autumn, TISHRI, 1) - ONE, g(hy_autumn, TISHRI, 2)),
        ("yom_kippur", "יום כיפור", g(hy_autumn, TISHRI, 10) - ONE, g(hy_autumn, TISHRI, 10)),
        # Sukkot runs through Shemini Atzeret / Simchat Torah, one day (22 Tishri) in Israel
        ("sukkot", "סוכות", g(hy_autumn, TISHRI, 15) - ONE, g(hy_autumn, TISHRI, 22)),
        # Hanukkah: 8 days from 25 Kislev; eve = evening of 24 Kislev, last day = 25 Kislev + 7
        ("hanukkah", "חנוכה", h1 - ONE, h1 + 7 * ONE),
    ]


def build():
    rows = []
    for gy in range(FIRST.year - 1, LAST.year + 1):
        for name, he, start, end in holidays_for(gy):
            if end >= FIRST and start <= LAST:
                rows.append((name, he, start.year, start, end))
    rows.sort(key=lambda r: r[3])
    return rows


def check(rows):
    """Cross-check against hdate (all holidays) and holidays.Israel (public holidays). Returns problems."""
    from hdate import HDateInfo
    import holidays as holidays_lib

    problems = []
    # Source 2, hdate: marks every day of a holiday by name.
    names = {
        "rosh_hashana": {"rosh_hashana_i", "rosh_hashana_ii"}, "yom_kippur": {"yom_kippur"},
        "sukkot": {"sukkot", "hol_hamoed_sukkot", "hoshana_raba", "shmini_atzeret", "simchat_torah"},
        "hanukkah": {"chanukah"}, "purim": {"purim"},
        "passover": {"pesach", "hol_hamoed_pesach", "pesach_vii"},
        "independence_day": {"yom_haatzmaut"}, "shavuot": {"shavuot"},
    }
    seen = defaultdict(set)
    erev = defaultdict(set)  # hdate also marks the eve for these four
    erev_names = {"erev_rosh_hashana": "rosh_hashana", "erev_yom_kippur": "yom_kippur",
                  "erev_sukkot": "sukkot", "erev_pesach": "passover"}
    d = FIRST - 30 * ONE
    while d <= LAST + 30 * ONE:
        for h in HDateInfo(d, diaspora=False).holidays:
            if h.name in erev_names:
                erev[erev_names[h.name]].add(d)
            for ours, theirs in names.items():
                if h.name in theirs:
                    seen[ours].add(d)
        d += ONE
    for name, he, year, start, end in rows:
        mine = {start + i * ONE for i in range(1, (end - start).days + 1)}  # the days, without the eve
        theirs = {x for x in seen[name] if start - 3 * ONE <= x <= end + 3 * ONE}
        if name == "hanukkah":  # hdate also lists the last Kislev days past the 8th; compare the first 8 days
            theirs = {x for x in theirs if x <= end}
        if name in erev_names.values() and start not in erev[name]:
            problems.append(f"hdate: {name} {year}: my eve {start} is not hdate's erev day")
        if mine != theirs:
            problems.append(f"hdate: {name} {year}: mine {min(mine)}..{max(mine)} ({len(mine)} d), "
                            f"hdate {min(theirs) if theirs else None}..{max(theirs) if theirs else None} ({len(theirs)} d)")
    # Source 3, holidays.Israel: public holidays only (no Hanukkah or Purim). Compare the days it lists.
    il = holidays_lib.Israel(years=range(FIRST.year - 1, LAST.year + 2), language="en_US")
    keys = {"independence_day": "Independence", "shavuot": "Shavuot", "passover": "Pesach",
            "rosh_hashana": "Rosh", "yom_kippur": "Kippur", "sukkot": ("Sukkot", "Simchat")}
    for name, he, year, start, end in rows:
        if name not in keys:
            continue
        k = keys[name]
        k = (k,) if isinstance(k, str) else k
        lib_days = {d for d, n in il.items() if any(s in n for s in k) and start < d <= end}
        first, last = start + ONE, end
        # the library lists only the public (full) days: first and last day of Passover and Sukkot, both days of Rosh Hashana
        expected = {first, last}
        if name == "rosh_hashana":
            expected = {first, last}
        elif name in ("independence_day", "shavuot", "yom_kippur"):
            expected = {last}
        if lib_days != expected:
            problems.append(f"holidays lib: {name} {year}: mine {sorted(expected)}, library {sorted(lib_days)}")
    known = {2024: ("2024-12-25", "2025-01-02"), 2025: ("2025-12-14", "2025-12-22"), 2026: ("2026-12-04", "2026-12-12")}
    for name, he, year, start, end in rows:
        if name == "hanukkah" and year in known and (str(start), str(end)) != known[year]:
            problems.append(f"Hanukkah {year} differs from the known span {known[year]}: {start}..{end}")
    return problems


def main():
    rows = build()
    out = pathlib.Path(__file__).with_name("holidays.csv")
    with out.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["holiday_name", "holiday_name_he", "holiday_year", "start_date", "end_date"])
        for name, he, year, start, end in rows:
            w.writerow([name, he, year, start.isoformat(), end.isoformat()])
    print(f"wrote {len(rows)} rows to {out}")
    problems = check(rows)
    for p in problems:
        print("DISAGREEMENT:", p)
    print("cross-check: " + ("all dates agree" if not problems else f"{len(problems)} disagreement(s)"))
    sys.exit(1 if problems else 0)


if __name__ == "__main__":
    main()
