#!/usr/bin/env python3
"""Analyse a school comparison Parquet dataset.

Usage:
    uv run python analysis.py results.parquet --type year --year 2024 --variable allecijfers_toetsscore_gem allecijfers_pct_zittenblijvers
    uv run python analysis.py results.parquet --type average --year 2022 2023 2024 --variable duo_pct_fundamenteel_reken

Output:
    - Table: per-variable normalized distance from best (0=best, 1=worst), plus overall
      Euclidean distance from the Pareto-efficient frontier (0=on frontier).
    - Scatterplot (saved as PNG): only when exactly 2 variables are requested.
"""

import argparse
import sys
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

# Variables where a lower raw value is better; these are negated before analysis
# so that "higher is always better" holds throughout.
LOWER_IS_BETTER = {
    "allecijfers_pct_zittenblijvers",
    "allecijfers_uitstroom_spo_pct",
    "allecijfers_uitstroom_overig_pct",
    "allecijfers_schoolweging",
    "duo_pct_zittenblijvers",
    "duo_schooladvies_vmbo_bk_pct",
    "duo_schooladvies_pro_pct",
    "duo_schooladvies_vso_pct",
    "duo_schooladvies_overig_pct",
    "scholenopdekaart_schooladvies_vmbo_bk_pct",
    "scholenopdekaart_schooladvies_vmbo_b_pct",
    "scholenopdekaart_schooladvies_vmbo_k_pct",
    "scholenopdekaart_schooladvies_pro_pct",
    "scholenopdekaart_schooladvies_vso_pct",
}


# ---------------------------------------------------------------------------
# Core helpers
# ---------------------------------------------------------------------------


def load_and_aggregate(path: str, analysis_type: str, years: list[int], variables: list[str]) -> pd.DataFrame:
    """Return a wide DataFrame (index=school_name, cols=variables) after aggregation."""
    df = pd.read_parquet(path)

    # Warn about missing variables
    available = set(df["variable"].unique())
    missing = [v for v in variables if v not in available]
    if missing:
        print(f"WARNING: variable(s) not found in data: {missing}", file=sys.stderr)
        variables = [v for v in variables if v in available]
    if not variables:
        sys.exit("ERROR: No requested variables exist in the dataset.")

    df = df[df["variable"].isin(variables)]

    if analysis_type == "year":
        year = years[0]
        df = df[df["year"] == year]
        if df.empty:
            sys.exit(f"ERROR: No data for year {year}.")
    else:  # average
        df = df[df["year"].isin(years)]
        if df.empty:
            sys.exit(f"ERROR: No data for years {years}.")
        df = df.groupby(["school_name", "variable"], as_index=False)["value"].mean()

    wide = df.pivot_table(index="school_name", columns="variable", values="value", aggfunc="first")
    wide.columns.name = None
    wide = wide.reset_index()
    # Keep only requested variables (some may have been dropped after pivot if all-NaN)
    cols = ["school_name"] + [v for v in variables if v in wide.columns]
    wide = wide[cols].dropna(subset=[c for c in cols if c != "school_name"])
    return wide, [v for v in variables if v in wide.columns]


def invert_lower_is_better(wide: pd.DataFrame, variables: list[str]) -> pd.DataFrame:
    """Negate lower-is-better variables in-place (copy returned)."""
    wide = wide.copy()
    for v in variables:
        if v in LOWER_IS_BETTER:
            print(f"  [inverted] {v}  (lower raw value = better → negated)")
            wide[v] = -wide[v]
    return wide


def normalize_distances(wide: pd.DataFrame, variables: list[str]) -> pd.DataFrame:
    """Return a DataFrame of per-variable normalized distances (0=best, 1=worst)."""
    dist = pd.DataFrame({"school_name": wide["school_name"]})
    for v in variables:
        col = wide[v]
        rng = col.max() - col.min()
        if rng == 0:
            dist[f"{v}_dist"] = 0.0
        else:
            dist[f"{v}_dist"] = (col.max() - col) / rng
    return dist


# ---------------------------------------------------------------------------
# Pareto / efficient frontier
# ---------------------------------------------------------------------------


def pareto_mask(values: np.ndarray) -> np.ndarray:
    """Return boolean mask of Pareto-efficient rows (higher is better on all cols)."""
    n = len(values)
    efficient = np.ones(n, dtype=bool)
    for i in range(n):
        if not efficient[i]:
            continue
        # i is dominated if any other point is >= on all dims and > on at least one
        dominated_by = (
            np.all(values >= values[i], axis=1) &
            np.any(values > values[i], axis=1)
        )
        dominated_by[i] = False
        if dominated_by.any():
            efficient[i] = False
    return efficient


def _point_to_segment_dist(p: np.ndarray, a: np.ndarray, b: np.ndarray) -> float:
    """Euclidean distance from point p to segment a-b."""
    ab = b - a
    ab_sq = np.dot(ab, ab)
    if ab_sq == 0:
        return float(np.linalg.norm(p - a))
    t = np.clip(np.dot(p - a, ab) / ab_sq, 0.0, 1.0)
    proj = a + t * ab
    return float(np.linalg.norm(p - proj))


def frontier_distance_2d(points_norm: np.ndarray, efficient_mask: np.ndarray) -> np.ndarray:
    """
    Compute distance of each point to the piecewise-linear efficient frontier in 2D.

    The frontier is built in normalized [0,1]^2 space (higher is better):
    - Sort efficient points by x ascending.
    - Connect them with line segments.
    - Extend left: vertical line from leftmost efficient point down to x-axis.
    - Extend right: horizontal line from rightmost efficient point to x=1 boundary.
    """
    eff = points_norm[efficient_mask]
    # Sort by x
    order = np.argsort(eff[:, 0])
    eff = eff[order]

    # Build segments
    segments = []

    # Left extension: horizontal from y-axis to leftmost efficient point
    x0, y0 = eff[0]
    segments.append((np.array([0.0, y0]), np.array([x0, y0])))

    # Segments between consecutive efficient points
    for i in range(len(eff) - 1):
        segments.append((eff[i], eff[i + 1]))

    # Right extension: vertical from rightmost efficient point down to x-axis
    xN, yN = eff[-1]
    segments.append((np.array([xN, yN]), np.array([xN, 0.0])))

    dists = np.array([
        min(_point_to_segment_dist(p, a, b) for a, b in segments)
        for p in points_norm
    ])
    return dists


def frontier_distance_nd(points_norm: np.ndarray, efficient_mask: np.ndarray) -> np.ndarray:
    """Distance of each point to its nearest Pareto-efficient point (n > 2 dims)."""
    eff = points_norm[efficient_mask]
    dists = np.array([
        float(np.min(np.linalg.norm(eff - p, axis=1)))
        for p in points_norm
    ])
    return dists


def compute_overall_score(wide: pd.DataFrame, variables: list[str]) -> tuple[np.ndarray, np.ndarray]:
    """Return (raw distances, efficient_mask) in normalized space."""
    vals = wide[variables].values.astype(float)

    # Normalize each column to [0, 1]
    col_min = vals.min(axis=0)
    col_max = vals.max(axis=0)
    rng = col_max - col_min
    rng[rng == 0] = 1  # avoid div-by-zero; constant cols → all zeros after norm
    vals_norm = (vals - col_min) / rng

    efficient = pareto_mask(vals_norm)

    if len(variables) == 2:
        dists = frontier_distance_2d(vals_norm, efficient)
    else:
        dists = frontier_distance_nd(vals_norm, efficient)

    # Normalize overall score to [0, 1]
    max_dist = dists.max()
    if max_dist > 0:
        dists = dists / max_dist

    return dists, efficient, vals_norm


# ---------------------------------------------------------------------------
# Plot (2D only)
# ---------------------------------------------------------------------------


def plot_scatter(wide: pd.DataFrame, variables: list[str], efficient_mask: np.ndarray,
                 vals_norm: np.ndarray, out_path: str) -> None:
    vx, vy = variables
    fig, ax = plt.subplots(figsize=(8, 6))

    x = wide[vx].values
    y = wide[vy].values
    names = wide["school_name"].values

    # Plot all schools
    ax.scatter(x[~efficient_mask], y[~efficient_mask], color="steelblue", zorder=3, label="School")
    ax.scatter(x[efficient_mask], y[efficient_mask], color="crimson", zorder=4,
               marker="*", s=120, label="Pareto-efficient")

    for xi, yi, name in zip(x, y, names):
        ax.annotate(name, (xi, yi), textcoords="offset points", xytext=(4, 4), fontsize=7)

    # Draw efficient frontier in normalized space, then convert back to original scale
    col_min = vals_norm.min(axis=0) * 0  # already 0 in norm, but use original range
    orig_min = np.array([wide[vx].min(), wide[vy].min()])
    orig_max = np.array([wide[vx].max(), wide[vy].max()])
    orig_rng = orig_max - orig_min
    orig_rng[orig_rng == 0] = 1

    eff_norm = vals_norm[efficient_mask]
    order = np.argsort(eff_norm[:, 0])
    eff_norm_sorted = eff_norm[order]

    # Convert frontier vertices back to original scale
    def to_orig(pt_norm):
        return orig_min + pt_norm * orig_rng

    frontier_pts = []
    x0n, y0n = eff_norm_sorted[0]
    frontier_pts.append(to_orig([0.0, y0n]))   # left: horizontal extension to y-axis
    for pt in eff_norm_sorted:
        frontier_pts.append(to_orig(pt))
    xNn, yNn = eff_norm_sorted[-1]
    frontier_pts.append(to_orig([xNn, 0.0]))   # right: vertical extension to x-axis

    fx = [p[0] for p in frontier_pts]
    fy = [p[1] for p in frontier_pts]
    ax.plot(fx, fy, color="crimson", linewidth=1.2, linestyle="--", alpha=0.7, label="Efficient frontier")

    ax.set_xlabel(vx)
    ax.set_ylabel(vy)
    ax.set_title("School comparison")
    ax.legend(fontsize=8)
    plt.tight_layout()
    plt.savefig(out_path, dpi=150)
    plt.close(fig)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Analyse school comparison Parquet data.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("file", help="Path to Parquet file (e.g. results.parquet)")
    parser.add_argument(
        "--type", dest="analysis_type", required=True, choices=["average", "year"],
        help="'year': summarise a single year; 'average': average over specified years",
    )
    parser.add_argument(
        "--year", nargs="+", type=int, required=True, metavar="YEAR",
        help="Year(s): single year for --type year; one or more for --type average",
    )
    parser.add_argument(
        "--variable", nargs="+", required=True, metavar="VAR",
        help="One or more variable names to analyse",
    )

    args = parser.parse_args()

    if args.analysis_type == "year" and len(args.year) != 1:
        parser.error("--type year requires exactly one --year value")

    print(f"\nLoading {args.file} …")
    wide, variables = load_and_aggregate(args.file, args.analysis_type, args.year, args.variable)

    print(f"Schools: {len(wide)}  |  Variables: {variables}\n")

    print("Variable direction adjustments:")
    wide = invert_lower_is_better(wide, variables)

    # Per-variable normalized distances
    dist_df = normalize_distances(wide, variables)

    # Overall score
    overall, efficient_mask, vals_norm = compute_overall_score(wide, variables)
    dist_df["overall_score"] = overall

    # Mark efficient schools
    dist_df["frontier"] = ["*" if e else "" for e in efficient_mask]

    # Print table
    print("\n--- Results (0 = best, 1 = worst / furthest from frontier) ---\n")
    display = dist_df.copy()
    display.insert(0, "school", wide["school_name"].values)
    display = display.drop(columns=["school_name"])
    # Round numeric cols
    num_cols = [c for c in display.columns if c not in ("school", "frontier")]
    display[num_cols] = display[num_cols].round(3)
    try:
        from tabulate import tabulate
        print(tabulate(display, headers="keys", tablefmt="simple", index=False, showindex=False))
    except ImportError:
        pd.set_option("display.max_columns", None)
        pd.set_option("display.width", 120)
        print(display.to_string(index=False))

    # Write output Parquet
    stem = Path(args.file).stem
    out_parquet = f"{stem}_output.parquet"
    output_df = display.rename(columns={"school": "school_name"})
    output_df.to_parquet(out_parquet, index=False)
    print(f"\nResults written to: {out_parquet}")

    # Scatter plot for 2-variable case
    if len(variables) == 2:
        plot_path = f"{stem}_scatter.png"
        plot_scatter(wide, variables, efficient_mask, vals_norm, plot_path)
        print(f"Scatterplot saved to: {plot_path}")


if __name__ == "__main__":
    main()
