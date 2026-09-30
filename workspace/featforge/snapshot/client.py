"""快照搬运 HTTP 客户端（仅依赖标准库 urllib）。

导出端、导入端 API 同构封装；执行器（worker）与演示脚本都用它。
HTTP 层错误抛出 :class:`SnapshotHTTPError`，带 status 与服务端 ``code``。
"""
from __future__ import annotations

import json
import urllib.error
import urllib.request
from typing import Optional


class SnapshotHTTPError(Exception):
    def __init__(self, status: int, code: str, message: str, body: dict):
        self.status = status
        self.code = code
        self.message = message
        self.body = body
        super().__init__(f"HTTP {status} {code}: {message}")


def _request(method: str, url: str, body=None, headers=None,
             raw: bool = False, timeout: float = 30.0):
    data = None
    hdr = {"Accept": "application/json"}
    if headers:
        hdr.update(headers)
    if body is not None and not raw:
        data = json.dumps(body).encode("utf-8")
        hdr["Content-Type"] = "application/json"
    elif raw:
        data = body
        hdr.setdefault("Content-Type", "application/octet-stream")
    req = urllib.request.Request(url, data=data, headers=hdr, method=method)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            payload = resp.read()
            ctype = resp.headers.get("Content-Type", "")
            if "application/json" in ctype:
                return json.loads(payload.decode("utf-8")) if payload else {}
            return payload
    except urllib.error.HTTPError as e:
        payload = e.read()
        try:
            parsed = json.loads(payload.decode("utf-8"))
        except Exception:
            parsed = {"error": {"code": "http_error",
                                "message": payload.decode("utf-8", "replace")}}
        err = parsed.get("error", parsed)
        raise SnapshotHTTPError(
            e.code, err.get("code", "http_error"),
            err.get("message", str(err)), parsed
        ) from None


class SnapshotClient:
    def __init__(self, base_url: str):
        self.base = base_url.rstrip("/")

    # ---------------- 导出端
    def create_export(self, body: dict,
                      fail_after_chunks: Optional[int] = None,
                      x_demo: Optional[str] = None) -> dict:
        headers = {}
        if fail_after_chunks is not None:
            headers["X-Fail-After-Chunks"] = str(fail_after_chunks)
        if x_demo:
            headers["X-Demo-Fault"] = x_demo
        return _request("POST", f"{self.base}/exports", body, headers)

    def get_bundle(self, bundle_id: str) -> dict:
        return _request("GET", f"{self.base}/bundles/{bundle_id}")

    def list_bundles(self) -> dict:
        return _request("GET", f"{self.base}/bundles")

    def get_export_status(self, key: str) -> dict:
        from urllib.parse import quote
        return _request("GET",
                        f"{self.base}/exports/{quote(key, safe='')}")

    def get_chunk(self, bundle_id: str, index: int) -> bytes:
        return _request(
            "GET", f"{self.base}/bundles/{bundle_id}/chunks/{index}")

    # ---------------- 导入端
    def create_import(self, body: dict) -> dict:
        return _request("POST", f"{self.base}/imports", body)

    def get_transfer(self, tid: str) -> dict:
        return _request("GET", f"{self.base}/imports/{tid}")

    def list_transfers(self) -> dict:
        return _request("GET", f"{self.base}/imports")

    def acquire_chunk(self, tid: str, executor_id: str) -> Optional[dict]:
        r = _request("POST", f"{self.base}/imports/{tid}/chunks/acquire",
                     {"executor_id": executor_id})
        return r.get("chunk")

    def put_chunk(self, tid: str, index: int, fence_token: str,
                  data: bytes, corrupt: bool = False) -> dict:
        headers = {"X-Fence-Token": str(fence_token)}
        if corrupt:
            headers["X-Demo-Corrupt"] = "1"
        return _request(
            "PUT", f"{self.base}/imports/{tid}/chunks/{index}",
            data, headers, raw=True,
        )

    def activate(self, tid: str) -> dict:
        return _request("POST", f"{self.base}/imports/{tid}/activate", {})

    def prime_sabotage(self, tid: str, mode: str) -> dict:
        return _request("POST",
                        f"{self.base}/imports/{tid}/debug/sabotage",
                        {"mode": mode})

    # ---------------- 读取
    def active(self, feature: str) -> dict:
        from urllib.parse import quote
        return _request("GET", f"{self.base}/active/{quote(feature, safe='')}")

    def read_rows(self, feature: str, entity_id: Optional[str] = None,
                  bundle_id: Optional[str] = None) -> dict:
        from urllib.parse import urlencode, quote
        q = {}
        if entity_id:
            q["entity_id"] = entity_id
        if bundle_id:
            q["bundle_id"] = bundle_id
        qs = ("?" + urlencode(q)) if q else ""
        return _request(
            "GET", f"{self.base}/active/{quote(feature, safe='')}/rows{qs}")
