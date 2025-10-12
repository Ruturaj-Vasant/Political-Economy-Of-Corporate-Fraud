from __future__ import annotations

# Support both module execution and direct file execution by bootstrapping sys.path
try:
    # Preferred: module execution (python -m restructured_code.main.sec.extract.interactive)
    from .runner import extract_for_ticker, detect_tickers_with_form_htmls  # type: ignore
except Exception:
    # Fallback for direct path execution
    import os, sys
    sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../..')))
    from restructured_code.main.sec.extract.runner import extract_for_ticker, detect_tickers_with_form_htmls  # type: ignore


def _prompt_tickers() -> list[str]:
    s = input("Enter ticker(s), separated by commas if multiple: ").strip()
    return [x.strip().upper() for x in s.split(',') if x.strip()]


def _prompt_form() -> str:
    s = input("Enter form (default 'DEF 14A'): ").strip()
    return s if s else "DEF 14A"


def _prompt_overwrite() -> bool:
    s = input("Overwrite existing CSVs? (y/N): ").strip().lower()
    return s in ("y", "yes")


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
    form = _prompt_form()

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

    total = 0
    for t in tickers:
        outs = extract_for_ticker(t, form=form, overwrite=overwrite)
        print(f"{t}: {len(outs)} files extracted")
        total += len(outs)
    print(f"Done. Total CSVs: {total}")
    return 0


def main() -> int:
    try:
        return run_interactive()
    except KeyboardInterrupt:
        print("\nInterrupted.")
        return 130


if __name__ == '__main__':
    raise SystemExit(main())
