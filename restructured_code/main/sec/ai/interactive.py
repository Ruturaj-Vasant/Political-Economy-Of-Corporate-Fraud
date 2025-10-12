from __future__ import annotations

# Support both module execution and direct file execution by bootstrapping sys.path
try:
    # Preferred: module execution (python -m restructured_code.main.sec.ai.interactive)
    from .runner import run_for_ticker, detect_tickers_with_csvs  # type: ignore
    from ..config import load_config  # type: ignore
    from .csv_to_json import list_ollama_models  # type: ignore
except Exception:
    # Fallback for direct path execution
    import os, sys
    sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../..')))
    from restructured_code.main.sec.ai.runner import run_for_ticker, detect_tickers_with_csvs  # type: ignore
    from restructured_code.main.sec.config import load_config  # type: ignore
    from restructured_code.main.sec.ai.csv_to_json import list_ollama_models  # type: ignore


def _prompt_tickers() -> list[str]:
    s = input("Enter ticker(s), separated by commas if multiple: ").strip()
    return [x.strip().upper() for x in s.split(',') if x.strip()]


def _choose_from_list(title: str, options: list[str], default_index: int = 0, allow_custom: bool = True) -> str:
    """Display a small numeric menu to choose from.

    - `default_index` is selected if user presses Enter.
    - If `allow_custom` and user selects the last option labeled 'Custom…', prompt for free-text.
    """
    if not options:
        raise ValueError("options must be non-empty")

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


def _detect_forms_for_tickers(tickers: list[str] | None) -> list[str]:
    """Scan the data root for available forms for the given tickers.

    Looks under `<data_root>/<TICKER>/*/extracted`.
    Returns a de-duplicated, sorted list of form names.
    """
    cfg = load_config()
    root = cfg.data_root
    forms: set[str] = set()
    import os
    # Determine which ticker directories to scan
    if tickers:
        ticker_dirs = [os.path.join(root, t.upper()) for t in tickers]
    else:
        ticker_dirs = [
            os.path.join(root, d)
            for d in os.listdir(root)
            if os.path.isdir(os.path.join(root, d)) and not d.startswith('.')
        ]

    for tdir in ticker_dirs:
        if not os.path.isdir(tdir):
            continue
        try:
            for entry in os.listdir(tdir):
                if entry.startswith('.'):
                    continue
                form_dir = os.path.join(tdir, entry)
                if os.path.isdir(form_dir):
                    extracted_dir = os.path.join(form_dir, 'extracted')
                    if os.path.isdir(extracted_dir):
                        forms.add(entry)
        except Exception:
            continue
    out = sorted(forms)
    return out


def _choose_form(tickers: list[str] | None) -> str:
    detected = _detect_forms_for_tickers(tickers)
    # Ensure DEF 14A is present and at the top; show others after.
    form_options = ["DEF 14A"]
    for f in detected:
        if f != "DEF 14A":
            form_options.append(f)
    return _choose_from_list("Select document/form:", form_options, default_index=0, allow_custom=True)


def _choose_model() -> str:
    # Try to detect locally installed models; fall back to a curated list.
    detected = list_ollama_models() or []
    curated_defaults = [
        "llama3:8b",
        "deepseek-r1:14b",
    ]
    # Merge while preserving order and deduping.
    model_options: list[str] = []
    for name in curated_defaults + detected:
        if name not in model_options:
            model_options.append(name)
    if not model_options:
        model_options = curated_defaults
    return _choose_from_list("Select Ollama model:", model_options, default_index=0, allow_custom=True)


def run_interactive() -> int:
    # Choose form first so we can offer an 'All detected tickers' option next
    form = _choose_form(None)

    # Choose tickers: either use all detected for the chosen form, or manual entry
    detected_tickers = detect_tickers_with_csvs(form=form)
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

    model = _choose_model()

    total = 0
    for t in tickers:
        outs = run_for_ticker(t, form=form, model=model)
        print(f"{t}: {len(outs)} JSON files written")
        total += len(outs)
    print(f"Done. Total JSON files: {total}")
    return 0


def main() -> int:
    try:
        return run_interactive()
    except KeyboardInterrupt:
        print("\nInterrupted.")
        return 130


if __name__ == '__main__':
    raise SystemExit(main())
