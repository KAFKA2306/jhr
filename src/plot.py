from pathlib import Path
from typing import Dict, Any, List, Tuple, Optional
import math
import yaml
try:
    import matplotlib
    import matplotlib.pyplot as plt
    import numpy as np
except Exception as e:
    raise SystemExit(f"Matplotlib/Numpy import failed: {e}")
PNG_DIR = Path("png")
YAML_PATH = Path("jhr_11year_comprehensive_kpi.yaml")
def _to_percent(v: Optional[float]) -> Optional[float]:
    if v is None:
        return None
    try:
        x = float(v)
    except Exception:
        return None
    return x * 100.0 if x <= 1.0 else x
def _month_key_to_int(k: Any) -> Optional[int]:
    try:
        if isinstance(k, int):
            return k
        s = str(k).strip().strip("'").strip('"')
        if not s:
            return None
        return int(s)
    except Exception:
        return None
def load_yaml(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)
def extract(root: Dict[str, Any]) -> Tuple[List[int], Dict[int, Dict[str, Any]]]:
    datasets = root.get("datasets", {}) or {}
    years: List[int] = []
    by_year: Dict[int, Dict[str, Any]] = {}
    for y_key, y_val in datasets.items():
        try:
            y = int(y_key)
        except Exception:
            continue
        if not isinstance(y_val, dict):
            continue
        years.append(y)
        by_year[y] = y_val
    years.sort()
    return years, by_year
def get_annual_summary(ydata: Dict[str, Any]) -> Dict[str, Optional[float]]:
    ann = ydata.get("annual_summary", {}) or {}
    return {
        "occupancy_pct": _to_percent(ann.get("occupancy_avg_pct")),
        "adr_jpy": ann.get("adr_avg_jpy"),
        "revpar_jpy": ann.get("revpar_avg_jpy"),
        "sales_total_mil_jpy": ann.get("sales_total_annual_mil_jpy"),
    }
def get_monthly_series(ydata: Dict[str, Any]) -> Dict[str, List[Optional[float]]]:
    md = ydata.get("monthly_data", {}) or {}
    month_map: Dict[int, Dict[str, Any]] = {}
    for k, v in md.items():
        mi = _month_key_to_int(k)
        if mi is None or mi < 1 or mi > 12:
            continue
        if isinstance(v, dict):
            month_map[mi] = v
    occ: List[Optional[float]] = []
    adr: List[Optional[float]] = []
    rvp: List[Optional[float]] = []
    for m in range(1, 13):
        mv = month_map.get(m, {})
        occ.append(_to_percent(mv.get("occupancy_pct")))
        adr.append(mv.get("adr_jpy"))
        rvp.append(mv.get("revpar_jpy"))
    return {"occ_pct": occ, "adr_jpy": adr, "revpar_jpy": rvp}
def ensure_png_dir():
    PNG_DIR.mkdir(parents=True, exist_ok=True)
def fig_save(fig: matplotlib.figure.Figure, name: str):
    out = PNG_DIR / name
    fig.savefig(out, facecolor="white", bbox_inches="tight")
    plt.close(fig)
def draw_dashboard(root: Dict[str, Any], years: List[int], by_year: Dict[int, Dict[str, Any]]):
    occ = []
    adr = []
    rvp = []
    for y in years:
        ann = get_annual_summary(by_year[y])
        occ.append(ann["occupancy_pct"])
        adr.append(ann["adr_jpy"])
        rvp.append(ann["revpar_jpy"])
    fig = plt.figure(figsize=(10.5, 5.6), constrained_layout=True)
    gs = fig.add_gridspec(2, 3, width_ratios=[3, 2, 2], height_ratios=[1.1, 3])
    ax_title = fig.add_subplot(gs[0, :])
    ax_occ = fig.add_subplot(gs[1, 0])
    ax_adr = fig.add_subplot(gs[1, 1])
    ax_rvp = fig.add_subplot(gs[1, 2])
    cov = root.get("coverage_period", {})
    syear = cov.get("start_year", "?")
    eyear = cov.get("end_year", "?")
    ax_title.axis("off")
    def safe_text(s):
        try:
            return "".join(ch for ch in str(s) if ord(ch) < 128)
        except Exception:
            return str(s)
    title = safe_text(root.get("description", "JHR Dashboard"))
    ax_title.text(0.01, 0.76, f"{title}", fontsize=16, weight="bold", ha="left")
    ax_title.text(0.01, 0.42, f"Coverage: {syear}–{eyear}", fontsize=11)
    if years:
        ylatest = years[-1]
        li = len(years) - 1
        o = occ[li]
        a = adr[li]
        r = rvp[li]
        latest_line = (
            f"Latest {ylatest}: "
            f"Occ {o:.1f}% | ADR {a:,} JPY | RevPAR {r:,} JPY"
            if (o is not None and a is not None and r is not None)
            else f"Latest {ylatest}: data incomplete"
        )
        ax_title.text(0.01, 0.10, latest_line, fontsize=10.5)
    src = root.get("source", {}) or {}
    defs = root.get("data_definitions", {}) or {}
    meta = root.get("metadata", {}) or {}
    metrics = defs.get("metrics", {}) or {}
    regions = defs.get("geographical_regions", []) or []
    meta_lines = []
    if src.get("primary_url"):
        meta_lines.append(f"Source: {safe_text(src['primary_url'])}")
    if src.get("last_updated"):
        meta_lines.append(f"Updated: {safe_text(src['last_updated'])}")
    if metrics:
        meta_lines.append(f"Metrics: {len(metrics)} types")
    if regions:
        meta_lines.append(f"Regions: {len(regions)}")
    if meta:
        te = meta.get("total_expected_records")
        se = meta.get("successful_extractions")
        if te is not None and se is not None:
            meta_lines.append(f"Records: {se}/{te}")
    if meta_lines:
        ax_title.text(0.56, 0.76, "\n".join(meta_lines), fontsize=9.5, ha="left", va="top")
    ax_occ.set_title("Occupancy (%)", fontsize=11)
    ax_occ.plot(years, occ, marker="o", color="
    if years:
        ax_occ.set_xlim(years[0] - 0.25, years[-1] + 0.25)
    ymax = max([x for x in occ if isinstance(x, (int, float))] + [100]) * 1.1
    ax_occ.set_ylim(0, ymax)
    ax_occ.grid(True, alpha=0.3, linestyle="--", linewidth=0.5)
    ax_occ.set_xlabel("Year")
    ax_occ.set_ylabel("%")
    ax_adr.set_title("ADR (JPY)", fontsize=11)
    ax_adr.plot(years, adr, marker="o", color="
    if years:
        ax_adr.set_xlim(years[0] - 0.25, years[-1] + 0.25)
    if any(v is not None for v in adr):
        ax_adr.set_ylim(0, max([v for v in adr if v is not None]) * 1.2)
    ax_adr.grid(True, alpha=0.3, linestyle="--", linewidth=0.5)
    ax_adr.set_xlabel("Year")
    ax_rvp.set_title("RevPAR (JPY)", fontsize=11)
    ax_rvp.plot(years, rvp, marker="o", color="
    if years:
        ax_rvp.set_xlim(years[0] - 0.25, years[-1] + 0.25)
    if any(v is not None for v in rvp):
        ax_rvp.set_ylim(0, max([v for v in rvp if v is not None]) * 1.2)
    ax_rvp.grid(True, alpha=0.3, linestyle="--", linewidth=0.5)
    ax_rvp.set_xlabel("Year")
    for y in years:
        notes = by_year[y].get("special_notes") or []
        if notes:
            ax_occ.plot([y, y], [0, 0.5], color="
            ax_adr.plot([y, y], [0, 0.5], color="
            ax_rvp.plot([y, y], [0, 0.5], color="
    fig_save(fig, "dashboard-summary.png")
def draw_heatmap_occupancy(years: List[int], by_year: Dict[int, Dict[str, Any]]):
    mat = np.full((len(years), 12), np.nan)
    for i, y in enumerate(years):
        ms = get_monthly_series(by_year[y])
        vals = [v if v is not None else np.nan for v in ms["occ_pct"]]
        if len(vals) == 12:
            mat[i, :] = np.array(vals)
    fig, ax = plt.subplots(figsize=(10, 4.2))
    im = ax.imshow(mat, aspect="auto", cmap="YlGnBu", vmin=0, vmax=np.nanmax(mat))
    ax.set_title("Occupancy Heatmap (%)")
    ax.set_xlabel("Month")
    ax.set_ylabel("Year")
    ax.set_xticks(range(12), [str(i) for i in range(1, 13)])
    ax.set_yticks(range(len(years)), [str(y) for y in years])
    cbar = fig.colorbar(im, ax=ax, fraction=0.046, pad=0.04)
    cbar.set_label("%")
    fig_save(fig, "heatmap-occupancy.png")
def draw_trends_combined(years: List[int], by_year: Dict[int, Dict[str, Any]]):
    occ = []
    adr = []
    rvp = []
    for y in years:
        ann = get_annual_summary(by_year[y])
        occ.append(ann["occupancy_pct"])
        adr.append(ann["adr_jpy"])
        rvp.append(ann["revpar_jpy"])
    fig, ax = plt.subplots(figsize=(10, 4.2))
    ax.set_title("Annual Trends: Occupancy, ADR, RevPAR")
    ax.plot(years, occ, marker="o", label="Occ (%)", color="
    ax2 = ax.twinx()
    ax2.plot(years, adr, marker="o", label="ADR (JPY)", color="
    ax2.plot(years, rvp, marker="o", label="RevPAR (JPY)", color="
    if years:
        ax.set_xlim(years[0] - 0.25, years[-1] + 0.25)
    ax.set_ylabel("%")
    ax.set_xlabel("Year")
    ax.grid(True, alpha=0.3, linestyle="--", linewidth=0.5)
    lines1, labels1 = ax.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax2.legend(lines1 + lines2, labels1 + labels2, loc="upper left", frameon=False)
    fig_save(fig, "trends-combined.png")
def draw_per_year_monthlies(years: List[int], by_year: Dict[int, Dict[str, Any]]):
    for y in years:
        ms = get_monthly_series(by_year[y])
        months = list(range(1, 13))
        fig = plt.figure(figsize=(10, 6), constrained_layout=True)
        gs = fig.add_gridspec(2, 2)
        ax_title = fig.add_subplot(gs[0, :])
        ax1 = fig.add_subplot(gs[1, 0])
        ax2 = fig.add_subplot(gs[1, 1])
        ax_title.axis("off")
        notes = by_year[y].get("special_notes") or []
        title = f"{y} Monthly Performance"
        ax_title.text(0.01, 0.75, title, fontsize=14, weight="bold")
        if notes:
            ax_title.text(0.01, 0.40, f"Notes: {', '.join(notes)}", fontsize=10)
        ax1.set_title("Occupancy (%)")
        ax1.plot(months, ms["occ_pct"], marker="o", color="
        ax1.set_xlim(1, 12)
        ax1.set_xticks(months)
        mx = max([v for v in ms["occ_pct"] if v is not None] + [100])
        ax1.set_ylim(0, mx * 1.1)
        ax1.grid(True, alpha=0.3, linestyle="--", linewidth=0.5)
        ax1.set_xlabel("Month")
        ax1.set_ylabel("%")
        ax2.set_title("ADR & RevPAR (JPY)")
        ax2.plot(months, ms["adr_jpy"], marker="o", color="
        ax2.plot(months, ms["revpar_jpy"], marker="o", color="
        ax2.set_xlim(1, 12)
        ax2.set_xticks(months)
        if any(v is not None for v in (ms["adr_jpy"] + ms["revpar_jpy"])):
            mv = max([v for v in (ms["adr_jpy"] + ms["revpar_jpy"]) if v is not None])
            ax2.set_ylim(0, mv * 1.2)
        ax2.grid(True, alpha=0.3, linestyle="--", linewidth=0.5)
        ax2.set_xlabel("Month")
        ax2.legend(frameon=False)
        fig_save(fig, f"monthly-{y}.png")
def draw_metadata_overview(root: Dict[str, Any]):
    fig = plt.figure(figsize=(10, 6))
    ax = fig.add_subplot(111)
    ax.axis("off")
    desc = root.get("description", "")
    schema = root.get("schema_version", "")
    cov = root.get("coverage_period", {})
    src = root.get("source", {})
    defs = root.get("data_definitions", {})
    meta = root.get("metadata", {})
    lines = []
    lines.append(f"Description: {desc}")
    lines.append(f"Schema Version: {schema}")
    if cov:
        lines.append(
            f"Coverage: {cov.get('start_year','?')}–{cov.get('end_year','?')} "
            f"(Total Years: {cov.get('total_years','?')})"
        )
    if src:
        lines.append(f"Primary URL: {src.get('primary_url','-')}")
        lines.append(f"IR Library URL: {src.get('ir_library_url','-')}")
        lines.append(f"Last Updated: {src.get('last_updated','-')}")
    if defs:
        metrics = defs.get("metrics", {}) or {}
        regions = defs.get("geographical_regions", []) or []
        lines.append("Metrics:")
        for k, v in metrics.items():
            lines.append(f"  - {k}: {v}")
        lines.append("Regions:")
        for r in regions:
            lines.append(f"  - {r}")
    if meta:
        lines.append("Metadata:")
        for k in ["created_date", "created_by", "purpose", "data_completeness", "total_expected_records", "successful_extractions"]:
            if k in meta:
                lines.append(f"  - {k}: {meta[k]}")
    ax.text(0.02, 0.98, "JHR KPI YAML Overview", fontsize=16, weight="bold", va="top")
    ax.text(0.02, 0.92, "\n".join(lines), fontsize=10, va="top")
    fig_save(fig, "metadata-overview.png")
def draw_data_completeness(years: List[int], by_year: Dict[int, Dict[str, Any]]):
    comp_counts = []
    for y in years:
        ms = get_monthly_series(by_year[y])
        count = sum(1 for v in ms["occ_pct"] if v is not None)
        comp_counts.append(count)
    fig, ax = plt.subplots(figsize=(10, 4.2))
    ax.bar(years, comp_counts, color="
    ax.set_ylim(0, 12)
    ax.set_ylabel("Months with occupancy data")
    ax.set_xlabel("Year")
    ax.set_title("Data Completeness (Occupancy)")
    for x, c in zip(years, comp_counts):
        ax.text(x, c + 0.2, str(c), ha="center", va="bottom", fontsize=9)
    fig_save(fig, "data-completeness.png")
def main():
    matplotlib.rcParams["figure.dpi"] = 130
    ensure_png_dir()
    doc = load_yaml(YAML_PATH)
    root = doc.get("jhr_comprehensive_kpi", {})
    years, by_year = extract(root)
    draw_dashboard(root, years, by_year)
    print("Generated png/dashboard-summary.png")
if __name__ == "__main__":
    main()
