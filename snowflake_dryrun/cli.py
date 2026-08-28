from __future__ import annotations

import argparse
import json
import sys

from snowflake_dryrun.engine import run_dry_run
from snowflake_dryrun.models import DryRunRequest


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Dry-run a Snowflake query: EXPLAIN analysis, row-explosion warnings, warehouse sizing."
    )
    parser.add_argument("sql", nargs="?", help="SQL text. If omitted, read stdin.")
    parser.add_argument("--sql-file", help="Path to a .sql file")
    parser.add_argument("--explain-json", help="Path to EXPLAIN USING JSON output")
    parser.add_argument(
        "--warehouse-size",
        default="XSMALL",
        help="Warehouse size the query would run on (XSMALL..X6LARGE)",
    )
    parser.add_argument("--json", action="store_true", help="Print full JSON result")
    args = parser.parse_args(argv)

    sql = args.sql or ""
    if args.sql_file:
        sql = open(args.sql_file, encoding="utf-8").read()
    if not sql and not sys.stdin.isatty() and not args.explain_json:
        sql = sys.stdin.read()

    explain = None
    if args.explain_json:
        with open(args.explain_json, encoding="utf-8") as fh:
            explain = json.load(fh)

    if not sql.strip() and not explain:
        parser.error("Provide SQL, --sql-file, stdin, or --explain-json")

    size = args.warehouse_size.upper().replace("-", "").replace("_", "")
    aliases = {"XS": "XSMALL", "XL": "XLARGE", "2XL": "XXLARGE", "3XL": "XXXLARGE", "4XL": "X4LARGE"}
    size = aliases.get(size, size)
    req = DryRunRequest(sql=sql, explain_json=explain, warehouse_size=size)  # type: ignore[arg-type]
    result = run_dry_run(req, allow_snowflake=True)
    if args.json:
        print(result.model_dump_json(indent=2))
        return 0
    _print_human(result)
    return 0


def _print_human(result) -> None:
    wh = result.warehouse
    print(f"Source: {result.source}")
    print(f"Given warehouse: {wh.given_size}")
    print(f"Recommended:     {wh.recommended_size}")
    print(f"Est. runtime on given:        {wh.estimated_seconds_on_given:.1f}s")
    print(f"Est. runtime on recommended:  {wh.estimated_seconds_on_recommended:.1f}s")
    print(f"Est. credits on given:        {wh.credit_hours_on_given:.4f} hour")
    print()
    if result.findings:
        print("Findings:")
        for f in result.findings:
            ops = f"  [ops {', '.join(map(str, f.operator_ids))}]" if f.operator_ids else ""
            print(f"  [{f.severity.upper():8}] {f.code}: {f.title}{ops}")
            print(f"            {f.detail}")
            if f.hint:
                print(f"            Hint: {f.hint}")
    else:
        print("Findings: none (no cartesian join, flatten, or large unfiltered scan detected).")
    if result.static_notes:
        print()
        print("Notes:")
        for n in result.static_notes:
            print(f"  - {n}")
    print()
    print("Rationale:")
    for line in wh.rationale:
        print(f"  - {line}")


if __name__ == "__main__":
    raise SystemExit(main())
