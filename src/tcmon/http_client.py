"""Polite HTTP client.

* descriptive User-Agent (from TCMON_USER_AGENT)
* throttling between requests and exponential back-off on 429/5xx, and on the
  HTTP 400 "Execution Timeout Expired" answer that the GDACS API returns when its
  database query times out (observed on 18 Nov 2026 00:00 UTC, Workshop 3)
* conditional GET (If-None-Match / If-Modified-Since)
* every response body is saved UNMODIFIED, with a sidecar ``.meta.json``
  (URL, UTC request time, status, headers, size, SHA-256) - this is the
  evidence required by Workshop 1.
"""
from __future__ import annotations

import hashlib
import json
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path

import requests

from .config import REPO_ROOT, user_agent

# Bodies of 4xx answers that are transient server-side failures, not client errors.
TRANSIENT_400_MARKERS = (b"execution timeout", b"timeout expired", b"timed out")

_EVIDENCE_HEADERS = (
    "Content-Type", "Content-Length", "Last-Modified", "ETag", "Date",
    "Cache-Control", "Expires", "X-RateLimit-Limit", "X-RateLimit-Remaining", "Retry-After",
)


@dataclass
class FetchResult:
    url: str
    status: int
    requested_at_utc: str
    elapsed_s: float
    headers: dict = field(default_factory=dict)
    size_bytes: int = 0
    sha256: str | None = None
    not_modified: bool = False
    path: str | None = None
    meta_path: str | None = None

    def to_dict(self) -> dict:
        return asdict(self)


def _rel(path: str | None) -> str | None:
    """Store paths relative to the repository (no local user names in evidence)."""
    if not path:
        return path
    try:
        return Path(path).resolve().relative_to(REPO_ROOT).as_posix()
    except ValueError:
        return Path(path).as_posix()


class PoliteClient:
    def __init__(self, min_interval: float = 1.0, timeout: float = 60, max_retries: int = 4):
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": user_agent(), "Accept-Encoding": "gzip"})
        self.min_interval = min_interval
        self.timeout = timeout
        self.max_retries = max_retries
        self._last = 0.0

    def _throttle(self) -> None:
        wait = self.min_interval - (time.monotonic() - self._last)
        if wait > 0:
            time.sleep(wait)
        self._last = time.monotonic()

    def get(self, url: str, params: dict | None = None, etag: str | None = None,
            last_modified: str | None = None, stream: bool = False) -> requests.Response:
        headers = {}
        if etag:
            headers["If-None-Match"] = etag
        if last_modified:
            headers["If-Modified-Since"] = last_modified
        for attempt in range(self.max_retries + 1):
            self._throttle()
            try:
                resp = self.session.get(url, params=params, headers=headers,
                                        timeout=self.timeout, stream=stream)
            except requests.RequestException:
                if attempt == self.max_retries:
                    raise
                time.sleep(2 ** attempt)
                continue
            if resp.status_code in (429, 500, 502, 503, 504) and attempt < self.max_retries:
                retry_after = resp.headers.get("Retry-After")
                time.sleep(float(retry_after) if retry_after and retry_after.isdigit() else 2 ** attempt)
                continue
            if resp.status_code == 400 and attempt < self.max_retries and self.is_transient_400(resp):
                time.sleep(2 ** (attempt + 2))           # 4 s, 8 s, 16 s: give the database time to recover
                continue
            return resp
        return resp  # pragma: no cover

    @staticmethod
    def is_transient_400(resp: requests.Response) -> bool:
        """True when a 400 answer is really a server-side timeout (GDACS)."""
        try:
            if int(resp.headers.get("Content-Length", "0") or 0) > 4096:
                return False
            body = resp.content.lower()             # small body; requests keeps it for iter_content
        except (ValueError, requests.RequestException):
            return False
        return any(m in body for m in TRANSIENT_400_MARKERS)

    def fetch_to_file(self, url: str, dest: str | Path, params: dict | None = None,
                      etag: str | None = None, last_modified: str | None = None) -> FetchResult:
        """Download ``url`` to ``dest`` byte-for-byte and write ``dest.meta.json``."""
        dest = Path(dest)
        dest.parent.mkdir(parents=True, exist_ok=True)
        requested = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        t0 = time.monotonic()
        resp = self.get(url, params=params, etag=etag, last_modified=last_modified, stream=True)
        result = FetchResult(
            url=resp.url, status=resp.status_code, requested_at_utc=requested,
            elapsed_s=0.0,
            headers={k: resp.headers[k] for k in _EVIDENCE_HEADERS if k in resp.headers},
        )
        if resp.status_code == 304:
            result.not_modified = True
        elif resp.ok:
            h = hashlib.sha256()
            size = 0
            with open(dest, "wb") as fh:
                for chunk in resp.iter_content(chunk_size=1 << 16):
                    fh.write(chunk)
                    h.update(chunk)
                    size += len(chunk)
            result.size_bytes, result.sha256, result.path = size, h.hexdigest(), str(dest)
        else:
            # keep error bodies too: they are evidence of how the source behaves
            body = resp.content
            dest = dest.with_suffix(dest.suffix + f".http{resp.status_code}")
            dest.write_bytes(body)
            result.size_bytes, result.path = len(body), str(dest)
        result.elapsed_s = round(time.monotonic() - t0, 3)
        meta = Path(str(dest) + ".meta.json")
        result.meta_path = str(meta)
        record = result.to_dict()  # the file on disk stores repo-relative paths only
        record["path"], record["meta_path"] = _rel(result.path), _rel(result.meta_path)
        meta.write_text(json.dumps(record, indent=2), encoding="utf-8")
        return result
