"""Storage abstractions.

We default to LocalStorage writing under `data/<TICKER>/<FORM>/<DATE>_<FORM>.<ext>`.
You can plug other backends later (e.g., S3) without changing downloader code.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
from dataclasses import dataclass
from typing import Optional, Tuple


@dataclass
class SaveResult:
    path: Path
    size: int
    sha256: str


class LocalStorage:
    def __init__(self, root: str) -> None:
        self.root = Path(root)

    def exists(self, relpath: Path) -> bool:
        return (self.root / relpath).exists()

    def load_bytes(self, relpath: Path) -> Optional[bytes]:
        p = self.root / relpath
        if not p.exists():
            return None
        return p.read_bytes()

    def _write_bytes(self, path: Path, data: bytes) -> SaveResult:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)
        h = hashlib.sha256(data).hexdigest()
        return SaveResult(path=path, size=len(data), sha256=h)

    def save_html(self, relpath: Path, html_bytes: bytes) -> SaveResult:
        return self._write_bytes(self.root / relpath, html_bytes)

    def save_text(self, relpath: Path, text: str) -> SaveResult:
        data = text.encode("utf-8", errors="ignore")
        return self._write_bytes(self.root / relpath, data)

    def write_meta(self, relpath: Path, meta: dict) -> None:
        meta_path = (self.root / relpath).with_suffix(relpath.suffix + ".meta.json")
        meta_path.write_text(json.dumps(meta, indent=2))


# Placeholder for future S3 backend (commented for now)
"""
class S3Storage:
    def __init__(self, bucket: str, prefix: str = "") -> None:
        # TODO: wire boto3 Session/client
        self.bucket = bucket
        self.prefix = prefix.rstrip('/')

    def exists(self, relpath: Path) -> bool:
        # TODO: HEAD object
        ...

    def save_html(self, relpath: Path, html_bytes: bytes) -> SaveResult:
        # TODO: put_object; compute sha256 locally
        ...

    def save_text(self, relpath: Path, text: str) -> SaveResult:
        # TODO: same as save_html, with text encoding
        ...

    def write_meta(self, relpath: Path, meta: dict) -> None:
        # TODO: upload sidecar meta JSON next to the file
        ...
"""

