from __future__ import annotations

# Support both module execution and direct file execution by bootstrapping sys.path
try:
    # Preferred: module execution (python -m restructured_code.main.sec.extract.interactive)
    from .runner import extract_for_ticker_all, detect_tickers_with_form_htmls  # type: ignore
    from .run_log import RunFileLogger, TickerStats, new_run_id  # type: ignore
except Exception:
    # Fallback for direct path execution
    import os, sys
    sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../..')))
    from restructured_code.main.sec.extract.runner import extract_for_ticker_all, detect_tickers_with_form_htmls  # type: ignore
    from restructured_code.main.sec.extract.run_log import RunFileLogger, TickerStats, new_run_id  # type: ignore


def _prompt_tickers() -> list[str]:
    s = input("Enter ticker(s), separated by commas if multiple: ").strip()
    return [x.strip().upper() for x in s.split(',') if x.strip()]


def _prompt_form() -> str:
    s = input("Enter form (default 'DEF 14A'): ").strip()
    return s if s else "DEF 14A"


def _prompt_overwrite() -> bool:
    s = input("Overwrite existing outputs? (y/N): ").strip().lower()
    return s in ("y", "yes")


def _prompt_extractor() -> str:
    print("\nChoose HTML extractor:")
    print("  1. XPath (legacy) [default]")
    print("  2. Scoring")
    print("  3. Both (Scoring then XPath fallback)")
    while True:
        choice = input("Choose [1]: ").strip()
        if choice == "" or choice == "1":
            return "xpath"
        if choice == "2":
            return "score"
        if choice == "3":
            return "both"
        print("Enter 1, 2, or 3.")


def _prompt_extract_target() -> str:
    print("What do you want to extract?")
    print("  1. Summary Compensation Table (SCT) [default]")
    print("  2. Beneficial Ownership (BO)")
    choice = input("Choose [1]: ").strip()
    if choice == "2":
        return "BO"
    return "SCT"


def _choose_from_list(title: str, options: list[str], default_index: int = 0, allow_custom: bool = True) -> str:
    print(f"\n{title}")
    for i, opt in enumerate(options, start=1):
        default_marker = " (default)" if (i - 1) == default_index else ""
        print(f"  {i}. {opt}{default_marker}")
    if allow_custom and (not options[-1].lower().startswith("custom")):
        print(f"  {len(options) + 1}. Custom…")
    while True:
        raw = input("Choose an option (Enter for default): ").strip()
        if raw == "":
            return options[default_index]
        try:
            idx = int(raw)
        except ValueError:
            print("Please enter a number from the menu.")
            continue
        max_idx = len(options) + (1 if allow_custom and (not options[-1].lower().startswith("custom")) else 0)
        if not (1 <= idx <= max_idx):
            print(f"Enter a number between 1 and {max_idx}.")
            continue
        if allow_custom and idx == max_idx and (not options[-1].lower().startswith("custom")):
            custom = input("Enter a custom value: ").strip()
            if custom:
                return custom
            print("Custom value cannot be empty.")
            continue
        return options[idx - 1]


def run_interactive() -> int:
    import os
    from pathlib import Path
    # Prompt data root (defaults to SEC_DATA_ROOT or ./data)
    current_root = os.getenv("SEC_DATA_ROOT", "data")
    chosen = input(f"Data root folder (Enter for default '{current_root}'): ").strip()
    if chosen:
        os.environ["SEC_DATA_ROOT"] = chosen
        print(f"Using data root: {chosen}")
    else:
        print(f"Using default data root: {current_root}")

    target = _prompt_extract_target()
    form = "DEF 14A"
    extractor = _prompt_extractor() if target == "SCT" else "xpath"
    include_bo = target == "BO"
    include_sct = target == "SCT"
    bo_max_tables: int | None = None
    if include_bo:
        raw = input("Max BO tables per filing (Enter for no limit): ").strip()
        if raw:
            try:
                val = int(raw)
                bo_max_tables = val if val > 0 else None
            except ValueError:
                print("Invalid number, using no limit.")
                bo_max_tables = None

    detected_tickers = detect_tickers_with_form_htmls(form=form)
    if detected_tickers:
        choice = _choose_from_list(
            "Select tickers source:",
            [f"All detected tickers for '{form}' ({len(detected_tickers)})", "Enter manually"],
            default_index=0,
            allow_custom=False,
        )
        if choice.startswith("All detected"):
            tickers = detected_tickers
        else:
            tickers = _prompt_tickers()
    else:
        print(f"No detected tickers for form '{form}'. Please enter tickers manually.")
        tickers = _prompt_tickers()

    if not tickers:
        print("No tickers provided. Exiting.")
        return 1

    overwrite = _prompt_overwrite()

    # Prepare per-run logger (writes once per ticker)
    data_root = Path(os.environ.get("SEC_DATA_ROOT", "data"))
    run_id = new_run_id()
    run_logger = RunFileLogger(root=data_root, run_id=run_id, form=form)
    print(f"Run ID: {run_id}")
    print(f"Run log: {run_logger.path}")

    total = 0
    for t in tickers:
        stats = TickerStats()
        outs = extract_for_ticker_all(
            t,
            form=form,
            overwrite=overwrite,
            extractor=extractor,
            include_txt=include_sct,  # TXT only relevant for SCT flow
            include_sct=include_sct,
            include_bo=include_bo,
            bo_max_tables=bo_max_tables,
            index=None,
            stats=stats,
        )
        # Persist per-ticker stats to run log
        run_logger.write_ticker(t, stats)
        print(f"{t}: {len(outs)} files extracted")
        total += len(outs)
    print(f"Done. Total outputs: {total}")
    return 0


def main() -> int:
    try:
        return run_interactive()
    except KeyboardInterrupt:
        print("\nInterrupted.")
        return 130


if __name__ == '__main__':
    raise SystemExit(main())
