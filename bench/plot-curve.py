#!/usr/bin/env python3
"""Draw the latency-vs-offered-load curve as a standalone SVG.

No matplotlib, no dependencies: the whole point of committing a plot is that
anyone who clones this can regenerate it, and a chart that needs a virtualenv is
a chart that will be stale within a month.

Two things are drawn on one pair of axes because they answer the question
together:

  * **achieved vs offered rate** (left axis) — the diagonal is a proxy keeping
    up. Where it flattens is the knee, and the knee is the throughput number.
  * **p99 latency** (right axis, log) — which climbs long before the diagonal
    bends. That gap is the whole argument for reporting a curve instead of a
    single "N req/s" figure.

Usage: plot-curve.py curve.csv curve.svg
"""

import csv
import sys
from pathlib import Path

W, H = 860, 460
PAD_L, PAD_R, PAD_T, PAD_B = 68, 72, 44, 56

# Chosen for legibility in both light and dark viewers: the artifact is a file in
# a repository, and it will be read on both.
INK = "#1f2933"
MUTED = "#7b8794"
GRID = "#d8dee6"
ACHIEVED = "#2a6df4"
LATENCY = "#d9480f"
IDEAL = "#9aa5b1"


def read(path):
    rows = []
    with open(path, newline="") as handle:
        for row in csv.DictReader(handle):
            if row.get("achieved_rps") in (None, "", "NA"):
                continue
            rows.append(row)
    return rows


def nice_ceiling(value):
    """Round up to something a human would put on an axis."""
    if value <= 0:
        return 1.0
    step = 10.0 ** (len(str(int(value))) - 1)
    return step * (int(value / step) + 1)


# The two profiles ask different questions, so their axes mean different things.
# Throughput offers a *rate* and asks what is delivered; concurrency sets a
# number of requests *in flight* and asks whether the proxy holds up. Sharing a
# plotting routine is fine; sharing an x-axis label would be a lie.
PROFILE = {
    "throughput": {
        "x_label": "offered request rate (req/s)",
        "subtitle": "open loop, coordinated-omission corrected; "
        "loopback, generator and proxy sharing 10 cores",
        # Only meaningful when the x axis is a rate.
        "diagonal": True,
    },
    "concurrency": {
        "x_label": "requests in flight (connections × streams)",
        "subtitle": "closed loop — concurrency is set, not offered "
        "(in an open loop, live streams = rate × latency); loopback",
        "diagonal": False,
    },
}

# The project's own target (README, design doc §10): small payloads under a
# 3 ms p99. The knee is marked where that stops holding, rather than where
# throughput collapses — throughput here does not collapse, it queues.
P99_TARGET_MS = 3.0


def plot(rows, profile, out):
    points = [r for r in rows if r["profile"] == profile]
    if not points:
        return None
    style = PROFILE.get(profile, PROFILE["throughput"])

    offered = [float(r["offered_rps"]) for r in points]
    achieved = [float(r["achieved_rps"]) for r in points]
    p99 = [max(float(r["p99_ms"]), 0.01) for r in points]

    x_max = nice_ceiling(max(offered))
    y_max = nice_ceiling(max(max(achieved), max(offered)))
    lat_min, lat_max = min(p99), max(p99)
    # A log latency axis, because the interesting range spans three decades: a
    # linear one renders every sub-millisecond point as the same flat line.
    import math

    lo = math.floor(math.log10(lat_min))
    hi = math.ceil(math.log10(lat_max))
    if hi <= lo:
        hi = lo + 1

    def sx(v):
        return PAD_L + (v / x_max) * (W - PAD_L - PAD_R)

    def sy(v):
        return H - PAD_B - (v / y_max) * (H - PAD_T - PAD_B)

    def sl(v):
        frac = (math.log10(v) - lo) / (hi - lo)
        return H - PAD_B - frac * (H - PAD_T - PAD_B)

    svg = [
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {W} {H}" '
        f'width="{W}" height="{H}" font-family="system-ui,-apple-system,sans-serif">',
        f'<rect width="{W}" height="{H}" fill="none"/>',
        f'<text x="{PAD_L}" y="24" font-size="15" font-weight="600" fill="{INK}">'
        f"h2proxy — {profile}: delivered rate and p99 vs offered load</text>",
        f'<text x="{PAD_L}" y="40" font-size="11" fill="{MUTED}">'
        f"{style['subtitle']}</text>",
    ]

    # Horizontal grid + left axis ticks (rate).
    for i in range(6):
        v = y_max * i / 5
        y = sy(v)
        svg.append(
            f'<line x1="{PAD_L}" y1="{y:.1f}" x2="{W - PAD_R}" y2="{y:.1f}" '
            f'stroke="{GRID}" stroke-width="1"/>'
        )
        svg.append(
            f'<text x="{PAD_L - 8}" y="{y + 4:.1f}" font-size="11" fill="{MUTED}" '
            f'text-anchor="end">{v / 1000:.0f}k</text>'
        )

    # Right axis ticks (latency decades).
    for d in range(lo, hi + 1):
        v = 10.0**d
        y = sl(v)
        label = f"{v:g} ms" if v >= 1 else f"{v * 1000:.0f} µs"
        svg.append(
            f'<text x="{W - PAD_R + 8}" y="{y + 4:.1f}" font-size="11" '
            f'fill="{LATENCY}">{label}</text>'
        )

    # X ticks.
    for i in range(6):
        v = x_max * i / 5
        x = sx(v)
        svg.append(
            f'<text x="{x:.1f}" y="{H - PAD_B + 18:.1f}" font-size="11" '
            f'fill="{MUTED}" text-anchor="middle">{v / 1000:.0f}k</text>'
        )
    svg.append(
        f'<text x="{(PAD_L + W - PAD_R) / 2:.0f}" y="{H - 14}" font-size="12" '
        f'fill="{INK}" text-anchor="middle">{style["x_label"]}</text>'
    )

    # The diagonal a proxy that keeps up would trace — only meaningful when the
    # x axis is a rate that could in principle be matched.
    if style["diagonal"]:
        svg.append(
            f'<line x1="{sx(0):.1f}" y1="{sy(0):.1f}" x2="{sx(x_max):.1f}" '
            f'y2="{sy(x_max):.1f}" stroke="{IDEAL}" stroke-width="1.5" '
            f'stroke-dasharray="5 4"/>'
        )

    def path(xs, ys, mapper):
        return " ".join(
            f"{'M' if i == 0 else 'L'}{sx(x):.1f},{mapper(y):.1f}"
            for i, (x, y) in enumerate(zip(xs, ys))
        )

    svg.append(
        f'<path d="{path(offered, achieved, sy)}" fill="none" '
        f'stroke="{ACHIEVED}" stroke-width="2.5"/>'
    )
    svg.append(
        f'<path d="{path(offered, p99, sl)}" fill="none" stroke="{LATENCY}" '
        f'stroke-width="2.5"/>'
    )
    for x, y in zip(offered, achieved):
        svg.append(f'<circle cx="{sx(x):.1f}" cy="{sy(y):.1f}" r="3.5" fill="{ACHIEVED}"/>')
    for x, y in zip(offered, p99):
        svg.append(f'<circle cx="{sx(x):.1f}" cy="{sl(y):.1f}" r="3.5" fill="{LATENCY}"/>')

    # Mark the knee: the highest point still inside the p99 target.
    #
    # Not "where delivered stops tracking offered" — that never happens here.
    # Past saturation the proxy keeps accepting and the requests queue, so
    # throughput still matches while latency goes up by two orders of magnitude.
    # A knee drawn from throughput alone would be drawn off the right edge of a
    # chart whose whole right half is unusable.
    knee = None
    for x, lat in zip(offered, p99):
        if lat <= P99_TARGET_MS:
            knee = x
    if knee and style["diagonal"]:
        svg.append(
            f'<line x1="{sx(knee):.1f}" y1="{PAD_T}" x2="{sx(knee):.1f}" '
            f'y2="{H - PAD_B}" stroke="{INK}" stroke-width="1" stroke-dasharray="3 3" '
            f'opacity="0.5"/>'
        )
        svg.append(
            f'<text x="{sx(knee) + 6:.1f}" y="{PAD_T + 14}" font-size="11" '
            f'fill="{INK}">knee ≈ {knee / 1000:.0f}k req/s (p99 &lt; {P99_TARGET_MS:g} ms)</text>'
        )

    legend = [("delivered rate", ACHIEVED), ("p99 latency", LATENCY)]
    if style["diagonal"]:
        legend.append(("offered = delivered", IDEAL))
    for i, (text, color) in enumerate(legend):
        y = PAD_T + 10 + i * 17
        x = W - PAD_R - 190
        svg.append(
            f'<line x1="{x}" y1="{y - 4}" x2="{x + 22}" y2="{y - 4}" '
            f'stroke="{color}" stroke-width="2.5"/>'
        )
        svg.append(f'<text x="{x + 28}" y="{y}" font-size="11" fill="{MUTED}">{text}</text>')

    svg.append("</svg>")
    Path(out).write_text("\n".join(svg))
    return out


def main():
    if len(sys.argv) < 3:
        print(__doc__)
        return 1
    rows = read(sys.argv[1])
    if not rows:
        print("no usable rows", file=sys.stderr)
        return 1
    out = Path(sys.argv[2])
    written = []
    present = {r["profile"] for r in rows}
    # Throughput first, deliberately: it takes the unsuffixed filename because it
    # is the headline chart. Sorting alphabetically put *concurrency* there.
    profiles = [p for p in ("throughput", "concurrency") if p in present]
    profiles += sorted(present - set(profiles))
    for index, profile in enumerate(profiles):
        target = out if index == 0 else out.with_name(f"{out.stem}-{profile}{out.suffix}")
        if plot(rows, profile, target):
            written.append(str(target))
    print("wrote " + ", ".join(written))
    return 0


if __name__ == "__main__":
    sys.exit(main())
