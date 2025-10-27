import sys
from pathlib import Path
from datetime import datetime, timedelta
from revenue_diff_tools import (
    load_and_normalize_excel,
    compare_rows,
    compare_aggregates_fixed,
    generate_report_final,
    generate_email_safe_html_report,
)


def get_daily_file_paths(base_dir: Path, suffix: str = ".xlsx") -> tuple[str, str]:
    today = datetime.today()
    yesterday = today - timedelta(days=1)

    f1 = base_dir / f"Commercial Log {yesterday.strftime('%y%m%d')}{suffix}"
    f2 = base_dir / f"Commercial Log {today.strftime('%y%m%d')}{suffix}"
    return str(f1), str(f2)


def run_daily_report(base_dir: str, output_dir: str) -> None:
    old_file, new_file = get_daily_file_paths(Path(base_dir))
    out_dir = Path(output_dir) / datetime.today().strftime("%Y-%m-%d")
    out_dir.mkdir(parents=True, exist_ok=True)

    try:
        df_old = load_and_normalize_excel(old_file)
        df_new = load_and_normalize_excel(new_file)
    except Exception as e:
        print(f"File loading error: {e}")
        return

    row_diffs = compare_rows(df_old, df_new)
    agg_diffs = compare_aggregates_fixed(df_old, df_new)

    deltas = {
        "total_diff": agg_diffs["total_diff"],
        "by_ae": agg_diffs["by_ae"],
        "by_bill_code": agg_diffs["by_bill_code"],
        "by_month": agg_diffs["by_month"],
        "row_diffs": row_diffs,
    }

    generate_report_final(deltas, str(out_dir / "revenue_change_report.md"))
    generate_email_safe_html_report(deltas, str(out_dir / "revenue_change_report.html"))

    print(f"Reports saved to: {out_dir.resolve()}")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python generate_daily_report.py <input_dir> <output_dir>")
        sys.exit(1)
    run_daily_report(sys.argv[1], sys.argv[2])
