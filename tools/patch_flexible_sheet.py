#!/usr/bin/env python3
import re, sys, shutil, pathlib

def patch_file(path, subs):
    p = pathlib.Path(path)
    src = p.read_text(encoding="utf-8")
    orig = src
    for (pattern, repl, desc) in subs:
        src_new, n = re.subn(pattern, repl, src, flags=re.DOTALL)
        if n == 0:
            print(f"[WARN] No match for: {desc} in {path}")
        else:
            print(f"[OK]   {desc}: {n} change(s) in {path}")
        src = src_new
    if src != orig:
        bak = p.with_suffix(p.suffix + ".bak")
        shutil.copy2(p, bak)
        p.write_text(src, encoding="utf-8")
        print(f"[SAVE] Wrote {path} (backup: {bak})")
    else:
        print(f"[SKIP] No changes written to {path}")

root = pathlib.Path(".").resolve()

# --- File 1: cli/import_closed_data.py --------------------------------------
f1 = root / "cli" / "import_closed_data.py"
subs1 = [
    # display_production_preview: load_workbook + workbook.max_row -> flexible selector + worksheet.max_row
    (r"""
(\s*)# Quick scan for preview\s*\n
(\s*)from openpyxl import load_workbook\s*\n
(\s*)\n
(\s*)workbook\s*=\s*load_workbook\(excel_file,\s*read_only=True,\s*data_only=True\)\s*\n
(\s*)\n
(\s*)total_rows\s*=\s*workbook\.max_row\s*-\s*1\s*\n
(\s*)print\(f"📊 File contains ~\{total_rows:,} rows to analyze"\)
""",
r"""\1# Quick scan for preview
\1from src.services.import_integration_utilities import get_excel_worksheet_flexible

\1worksheet, sheet_name, workbook = get_excel_worksheet_flexible(excel_file)

\1total_rows = max(0, (worksheet.max_row or 1) - 1)
\1print(f"📄 Preview using sheet: {sheet_name}")
\1print(f"📊 File contains ~{total_rows:,} rows to analyze")""",
     "preview: use flexible sheet + worksheet.max_row"),

    # EnhancedMarketSetupManager.scan_excel_for_markets: use flexible selector
    (r"""
(\s*)from openpyxl import load_workbook\s*\n
(\s*)\n
(\s*)workbook\s*=\s*load_workbook\(excel_file,\s*read_only=True,\s*data_only=True\)\s*\n
(\s*)worksheet\s*=\s*workbook\.active
""",
r"""\1from src.services.import_integration_utilities import get_excel_worksheet_flexible

\1worksheet, sheet_name, workbook = get_excel_worksheet_flexible(excel_file)
\1print(f"📄 Market scan using sheet: {sheet_name}")""",
     "market scan: use flexible sheet"),
]

# --- File 2: src/services/broadcast_month_import_service.py -------------------
f2 = root / "src" / "services" / "broadcast_month_import_service.py"
subs2 = [
    # Add flexible import inside method header area (next to openpyxl/datetime/re)
    (r"""
(\s*)from openpyxl import load_workbook\s*\n
(\s*)from datetime import datetime\s*\n
(\s*)import re\s*\n
""",
r"""\1from openpyxl import load_workbook
\2from datetime import datetime
\3import re
\3from src.services.import_integration_utilities import get_excel_worksheet_flexible
""",
     "importer: add flexible import next to locals"),

    # Replace workbook.active block with flexible call + log of sheet name
    (r"""
(\s*)with\s+suppress_verbose_logging\(\),\s*suppress_stdout_stderr\(\):\s*\n
(\s*)workbook\s*=\s*load_workbook\(excel_file,\s*read_only=True,\s*data_only=True\)\s*\n
(\s*)worksheet\s*=\s*workbook\.active
""",
r"""\1with suppress_verbose_logging(), suppress_stdout_stderr():
\2worksheet, sheet_name, workbook = get_excel_worksheet_flexible(excel_file)
\2tqdm.write(f"Using sheet: {sheet_name}")""",
     "importer: use flexible sheet"),
]

def main():
    any_err = False
    try:
        if f1.exists():
            patch_file(f1, subs1)
        else:
            print(f"[MISS] {f1} not found")

        if f2.exists():
            patch_file(f2, subs2)
        else:
            print(f"[MISS] {f2} not found")
    except Exception as e:
        print("[ERROR]", e)
        any_err = True
    sys.exit(1 if any_err else 0)

if __name__ == "__main__":
    main()
