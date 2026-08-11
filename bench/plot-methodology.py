#!/usr/bin/env python3
"""Plot what each load-testing methodology reports about the same proxy.

Both series show p99 latency against **delivered** throughput — the only axis
the two share, and the one that makes the comparison a fair question: at the
same delivered rate, what does each method say the tail was?

The shape of the answer is the point. The closed-loop series stops: it cannot
place a point beyond the throughput the proxy is willing to give it, because it
only issues a request when the last one finishes. The open-loop series keeps
going, because it offers load on a schedule regardless — and that is where the
cliff lives.

No dependencies, for the same reason as plot-curve.py: a chart that needs a
virtualenv is stale within a month.

Usage: plot-methodology.py methodology.csv methodology.svg
"""

import csv
import math
import sys
from pathlib import Path

W, H = 880, 480
PAD_L, PAD_R, PAD_T, PAD_B = 74, 28, 76, 58

INK = "#1f2933"
MUTED = "#7b8794"
GRID = "#d8dee6"
OPEN = "#d9480f"
CLOSED = "#2a6df4"


def read(path):
    out = {"open": [], "closed": []}
    with open(path, newline="") as handle:
        for row in csv.DictReader(handle):
            if row["achieved_rps"] in ("", "NA") or row["p99_ms"] in ("", "NA"):
                continue
            out[row["mode"]].append((float(row["achieved_rps"]), float(row["p99_ms"])))
    for series in out.values():
        series.sort()
    return out


def main():
    if len(sys.argv) < 3:
        print(__doc__)
        return 1
    data = read(sys.argv[1])
    if not data["open"] or not data["closed"]:
        print("need both an open and a closed series", file=sys.stderr)
        return 1

    every = data["open"] + data["closed"]
    x_max = max(x for x, _ in every) * 1.05
    lat = [y for _, y in every if y > 0]
    lo, hi = math.floor(math.log10(min(lat))), math.ceil(math.log10(max(lat)))
    if hi <= lo:
        hi = lo + 1

    def sx(v):
        return PAD_L + (v / x_max) * (W - PAD_L - PAD_R)

    def sy(v):
        frac = (math.log10(max(v, 10**lo)) - lo) / (hi - lo)
        return H - PAD_B - frac * (H - PAD_T - PAD_B)

    svg = [
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {W} {H}" width="{W}" '
        f'height="{H}" font-family="system-ui,-apple-system,sans-serif">',
        f'<text x="{PAD_L}" y="26" font-size="15" font-weight="600" fill="{INK}">'
        "The same proxy, measured two ways</text>",
        f'<text x="{PAD_L}" y="45" font-size="11.5" fill="{MUTED}">'
        "p99 against delivered throughput — 50 connections, identical proxy, backend "
        "and duration; only the measurement discipline differs</text>",
        f'<text x="{PAD_L}" y="61" font-size="11.5" fill="{MUTED}">'
        "The closed loop cannot place a point past the rate the proxy grants it, so "
        "it never reaches the region where the tail collapses.</text>",
    ]

    # Latency grid (log decades).
    d = lo
    while d <= hi:
        v = 10.0**d
        y = sy(v)
        svg.append(
            f'<line x1="{PAD_L}" y1="{y:.1f}" x2="{W - PAD_R}" y2="{y:.1f}" '
            f'stroke="{GRID}" stroke-width="1"/>'
        )
        label = f"{v:g} ms" if v >= 1 else f"{v * 1000:.0f} µs"
        svg.append(
            f'<text x="{PAD_L - 8}" y="{y + 4:.1f}" font-size="11" fill="{MUTED}" '
            f'text-anchor="end">{label}</text>'
        )
        d += 1

    for i in range(6):
        v = x_max * i / 5
        svg.append(
            f'<text x="{sx(v):.1f}" y="{H - PAD_B + 18:.1f}" font-size="11" '
            f'fill="{MUTED}" text-anchor="middle">{v / 1000:.0f}k</text>'
        )
    svg.append(
        f'<text x="{(PAD_L + W - PAD_R) / 2:.0f}" y="{H - 16}" font-size="12" '
        f'fill="{INK}" text-anchor="middle">delivered throughput (req/s)</text>'
    )

    def draw(series, color, label):
        path = " ".join(
            f"{'M' if i == 0 else 'L'}{sx(x):.1f},{sy(y):.1f}"
            for i, (x, y) in enumerate(series)
        )
        svg.append(f'<path d="{path}" fill="none" stroke="{color}" stroke-width="2.5"/>')
        for x, y in series:
            svg.append(f'<circle cx="{sx(x):.1f}" cy="{sy(y):.1f}" r="4" fill="{color}"/>')
        last = series[-1]
        svg.append(
            f'<text x="{sx(last[0]):.1f}" y="{sy(last[1]) - 12:.1f}" font-size="11" '
            f'font-weight="600" fill="{color}" text-anchor="middle">{label}</text>'
        )

    draw(data["closed"], CLOSED, "closed loop (h2load shape)")
    draw(data["open"], OPEN, "open loop (corrected)")

    # Where the closed loop runs out of road.
    ceiling = max(x for x, _ in data["closed"])
    svg.append(
        f'<line x1="{sx(ceiling):.1f}" y1="{PAD_T}" x2="{sx(ceiling):.1f}" '
        f'y2="{H - PAD_B}" stroke="{CLOSED}" stroke-width="1" stroke-dasharray="4 4" '
        f'opacity="0.55"/>'
    )
    svg.append(
        f'<text x="{sx(ceiling) - 8:.1f}" y="{PAD_T + 13}" font-size="11" '
        f'fill="{CLOSED}" text-anchor="end">the closed loop stops here</text>'
    )

    # At matched throughput, how far apart are the two verdicts?
    open_at = [(x, y) for x, y in data["open"] if x <= ceiling * 1.02]
    if open_at:
        x, y = open_at[-1]
        close = min(data["closed"], key=lambda p: abs(p[0] - x))
        if close[1] > 0:
            svg.append(
                f'<text x="{PAD_L}" y="{H - PAD_B + 40:.1f}" font-size="11.5" '
                f'fill="{INK}">At ~{x / 1000:.0f}k req/s delivered: closed loop reports '
                f'{close[1]:.2f} ms, open loop {y:.2f} ms.</text>'
            )

    svg.append("</svg>")
    Path(sys.argv[2]).write_text("\n".join(svg))
    print(f"wrote {sys.argv[2]}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
