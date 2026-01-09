from __future__ import annotations

# Support both module execution and direct file execution by bootstrapping sys.path
try:
    from .cli import iter_extracted_htmls
    from .html_to_json import process_html_file_to_json
    from ..config import load_config
    from ..downloads.file_naming import normalize_form_for_fs
except Exception:
    import os, sys
    sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../..')))
    from restructured_code.main.sec.transform.cli import iter_extracted_htmls  # type: ignore
    from restructured_code.main.sec.transform.html_to_json import process_html_file_to_json  # type: ignore
    from restructured_code.main.sec.config import load_config  # type: ignore
    from restructured_code.main.sec.downloads.file_naming import normalize_form_for_fs  # type: ignore

from pathlib import Path


def _prompt_root() -> Path:
    current_root = Path(load_config().data_root)
    s = input(f"Data root folder (Enter for default '{current_root}'): ").strip()
    if s:
        return Path(s)
    return current_root


def _prompt_form() -> str:
    s = input("Enter form (default 'DEF 14A'): ").strip()
    return s if s else "DEF 14A"


def _prompt_tickers(detected: list[str]) -> list[str]:
    if detected:
        print(f"Detected {len(detected)} tickers with extracted HTMLs.")
        print("  1. All detected")
        print("  2. Enter manually")
        choice = input("Choose [1]: ").strip()
        if choice in ("", "1"):
            return detected
    s = input("Enter ticker(s), separated by commas: ").strip()
    return [x.strip().upper() for x in s.split(",") if x.strip()]


def _prompt_overwrite() -> bool:
    s = input("Overwrite existing JSONs? (y/N): ").strip().lower()
    return s in ("y", "yes")


def run_interactive() -> int:
    data_root = _prompt_root()
    form = _prompt_form()
    overwrite = _prompt_overwrite()

    # Detect tickers with extracted HTMLs
    f_fs = normalize_form_for_fs(form)
    detected: list[str] = []
    for child in data_root.iterdir():
        if not child.is_dir() or child.name.startswith("."):
            continue
        extracted_dir = child / f_fs / "extracted"
        if extracted_dir.exists() and any(extracted_dir.glob("*_SCT.html")):
            detected.append(child.name.upper())
    detected = sorted(set(detected))

    tickers = _prompt_tickers(detected)
    if not tickers:
        print("No tickers selected. Exiting.")
        return 1

    total = 0
    for t in tickers:
        htmls = iter_extracted_htmls(data_root, t, form)
        if not htmls:
            print(f"{t}: no extracted HTMLs found")
            continue
        count = 0
        for hp in htmls:
            out_json = data_root / t / f_fs / "json" / (hp.stem.replace("_SCT", "_SCT") + ".json")
            if out_json.exists() and not overwrite:
                continue
            res = process_html_file_to_json(hp, form=form)
            if res:
                count += 1
                total += 1
        print(f"{t}: processed {count}/{len(htmls)} HTML files")
    print(f"Done. JSON files written: {total}")
    return 0


def main() -> int:
    try:
        return run_interactive()
    except KeyboardInterrupt:
        print("\nInterrupted.")
        return 130


if __name__ == "__main__":
    raise SystemExit(main())

