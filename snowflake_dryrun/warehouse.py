from __future__ import annotations

from snowflake_dryrun.models import ParsedPlan, WarehouseAdvice, WarehouseSize

# Relative cluster count vs XSMALL. Credits/hour equal this number for a standard warehouse.
SIZE_CREDITS: dict[WarehouseSize, int] = {
    "XSMALL": 1,
    "SMALL": 2,
    "MEDIUM": 4,
    "LARGE": 8,
    "XLARGE": 16,
    "XXLARGE": 32,
    "XXXLARGE": 64,
    "X4LARGE": 128,
    "X5LARGE": 256,
    "X6LARGE": 512,
}

SIZES: list[WarehouseSize] = list(SIZE_CREDITS.keys())

# X-Small mixed scan+compute throughput (bytes/sec).
# EXPLAIN bytesAssigned is storage IO Snowflake already pruned. ~1.25 GiB/s is conservative
# vs cached/local SSD (often faster) and still above the old 80 MiB/s floor that overstated
# elapsed time by ~20–30× on real EXPLAIN plans.
EXPLAIN_XSMALL_BYTES_PER_SEC = int(1.25 * 1024**3)
# Synthetic plans have no real bytes; stay conservative.
SYNTHETIC_XSMALL_BYTES_PER_SEC = 160 * 1024 * 1024
MIN_SECONDS = 2.0
PARALLEL_EXPONENT = 0.85


def advise_warehouse(
    plan: ParsedPlan,
    given_size: WarehouseSize,
    finding_codes: list[str] | None = None,
) -> WarehouseAdvice:
    codes = set(finding_codes or [])
    bytes_assigned = plan.global_stats.bytes_assigned or 0
    join_count = sum(1 for n in plan.nodes if "JOIN" in n.operation.upper())
    sort_count = sum(1 for n in plan.nodes if n.operation.upper() in {"SORT", "ORDERBY"})
    window_count = sum(1 for n in plan.nodes if "WINDOW" in n.operation.upper())
    flatten_count = sum(1 for n in plan.nodes if "FLATTEN" in n.operation.upper())
    scan_count = sum(1 for n in plan.nodes if "SCAN" in n.operation.upper())
    explain_backed = plan.source in {"snowflake", "pasted_json"} and bytes_assigned > 0
    xsmall_bps = EXPLAIN_XSMALL_BYTES_PER_SEC if explain_backed else SYNTHETIC_XSMALL_BYTES_PER_SEC

    work = float(max(bytes_assigned, 32 * 1024 * 1024))
    work *= _structural_multiplier(
        codes,
        join_count=join_count,
        sort_count=sort_count,
        window_count=window_count,
        flatten_count=flatten_count,
        explain_backed=explain_backed,
    )

    work_score = work / (1024**3)

    recommended = _pick_size(work, codes, join_count, sort_count, scan_count, xsmall_bps)
    given = given_size if given_size in SIZE_CREDITS else "XSMALL"

    est_given = _runtime_seconds(work, given, xsmall_bps)
    est_rec = _runtime_seconds(work, recommended, xsmall_bps)
    credits_given = SIZE_CREDITS[given]
    credits_rec = SIZE_CREDITS[recommended]

    rationale = _rationale(
        bytes_assigned=bytes_assigned,
        work_score=work_score,
        codes=codes,
        join_count=join_count,
        sort_count=sort_count,
        recommended=recommended,
        given=given,
        est_given=est_given,
        est_rec=est_rec,
        explain_backed=explain_backed,
        xsmall_bps=xsmall_bps,
    )
    if explain_backed:
        scale_note = (
            f"Runtime uses EXPLAIN bytesAssigned at ~{_fmt(xsmall_bps)}/s on X-Small, "
            "scaling sub-linearly with warehouse credits. That tracks remote storage scans; "
            "result cache, local SSD cache, and spilling sorts can be much faster or slower. "
            "This is not a Query Profile."
        )
    else:
        scale_note = (
            "Byte estimates are heuristic (no EXPLAIN bytesAssigned). "
            "Elapsed time is a rough bound. Paste EXPLAIN USING JSON or connect for IO-based timing."
        )
    return WarehouseAdvice(
        given_size=given,
        recommended_size=recommended,
        estimated_seconds_on_given=round(est_given, 1),
        estimated_seconds_on_recommended=round(est_rec, 1),
        credit_hours_on_given=round(credits_given * est_given / 3600, 4),
        credit_hours_on_recommended=round(credits_rec * est_rec / 3600, 4),
        work_score=round(work_score, 3),
        bytes_assigned=bytes_assigned or None,
        rationale=rationale,
        scale_note=scale_note,
    )


def _structural_multiplier(
    codes: set[str],
    *,
    join_count: int,
    sort_count: int,
    window_count: int,
    flatten_count: int,
    explain_backed: bool,
) -> float:
    # EXPLAIN bytesAssigned already is pruned scan IO. Extra factors are CPU/shuffle only.
    if explain_backed:
        m = 1.0
        if "CROSS_JOIN" in codes:
            m *= 2.5
        if "FLATTEN_EXPLODE" in codes:
            m *= 1.5
        if "NESTED_LOOP_JOIN" in codes:
            m *= 1.8
        m *= 1.0 + 0.04 * join_count
        m *= 1.0 + 0.08 * sort_count
        m *= 1.0 + 0.06 * window_count
        m *= 1.0 + 0.1 * flatten_count
        return m
    m = 1.0
    if "CROSS_JOIN" in codes:
        m *= 12.0
    if "FLATTEN_EXPLODE" in codes:
        m *= 4.0
    if "NESTED_LOOP_JOIN" in codes:
        m *= 3.0
    m *= 1.0 + 0.15 * join_count
    m *= 1.0 + 0.25 * sort_count
    m *= 1.0 + 0.2 * window_count
    m *= 1.0 + 0.35 * flatten_count
    return m


def _runtime_seconds(work_bytes: float, size: WarehouseSize, xsmall_bps: int) -> float:
    credits = SIZE_CREDITS[size]
    throughput = xsmall_bps * (credits**PARALLEL_EXPONENT)
    return max(MIN_SECONDS, work_bytes / throughput)


def _pick_size(
    work_bytes: float,
    codes: set[str],
    join_count: int,
    sort_count: int,
    scan_count: int,
    xsmall_bps: int,
) -> WarehouseSize:
    target = 45.0
    best: WarehouseSize = "XSMALL"
    for size in SIZES:
        if size in {"X5LARGE", "X6LARGE"} and work_bytes < 50 * 1024**3:
            continue
        seconds = _runtime_seconds(work_bytes, size, xsmall_bps)
        best = size
        if seconds <= target:
            break

    if "CROSS_JOIN" in codes and SIZE_CREDITS[best] < 4:
        best = "MEDIUM"
    if "CROSS_JOIN" in codes and work_bytes >= 2 * 1024**3 and SIZE_CREDITS[best] < 8:
        best = "LARGE"
    if sort_count and work_bytes >= 8 * 1024**3 and SIZE_CREDITS[best] < 8:
        best = "LARGE"
    if join_count >= 4 and SIZE_CREDITS[best] < 2:
        best = "SMALL"
    return best


def _rationale(
    *,
    bytes_assigned: int,
    work_score: float,
    codes: set[str],
    join_count: int,
    sort_count: int,
    recommended: WarehouseSize,
    given: WarehouseSize,
    est_given: float,
    est_rec: float,
    explain_backed: bool,
    xsmall_bps: int,
) -> list[str]:
    lines: list[str] = []
    if bytes_assigned:
        lines.append(f"EXPLAIN assigns about {_fmt(bytes_assigned)} across scanned micro-partitions.")
    else:
        lines.append("Byte estimates are heuristic because EXPLAIN did not emit bytesAssigned.")
    lines.append(
        f"Assumed X-Small throughput is ~{_fmt(xsmall_bps)}/s"
        + (" from EXPLAIN IO." if explain_backed else " (synthetic plan, conservative).")
    )
    lines.append(f"Work score is {work_score:.2f} GiB-equivalent after join/sort/explosion multipliers.")
    if "CROSS_JOIN" in codes:
        lines.append("A cartesian join can emit far more rows than bytesAssigned suggests; the recommendation is biased larger.")
    if "FLATTEN_EXPLODE" in codes:
        lines.append("FLATTEN multiplies rows; runtime is sensitive to array length, not just table size.")
    if sort_count:
        lines.append("A global sort needs warehouse memory; spilling to local disk stretches elapsed time.")
    if join_count:
        lines.append(f"{join_count} join(s) add build/probe memory and shuffle.")
    if recommended == given:
        lines.append(f"{given} is a reasonable match (~{_fmt_secs(est_given)} estimated).")
    elif SIZE_CREDITS[recommended] > SIZE_CREDITS[given]:
        lines.append(
            f"On {given} this shape is ~{_fmt_secs(est_given)}; {recommended} is ~{_fmt_secs(est_rec)}. "
            "Scale up if this query is latency-sensitive or already spilling."
        )
    else:
        lines.append(
            f"{given} is larger than needed for this plan (~{_fmt_secs(est_given)}). "
            f"{recommended} should finish around {_fmt_secs(est_rec)} and burn fewer credits if the warehouse auto-suspends."
        )
    lines.append("These numbers are dry-run estimates, not a Snowflake Query Profile. Always confirm on a sample.")
    return lines


def _fmt_secs(seconds: float) -> str:
    if seconds < 90:
        return f"{seconds:.0f}s"
    return f"{seconds / 60:.1f} min"


def _fmt(n: int | float) -> str:
    value = float(n)
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if value < 1024 or unit == "TiB":
            return f"{value:.1f} {unit}" if unit != "B" else f"{int(value)} B"
        value /= 1024
    return str(n)
