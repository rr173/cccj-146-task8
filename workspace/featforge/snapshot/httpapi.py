"""零依赖 HTTP 服务（标准库 ThreadingHTTPServer）：导出端 / 导入端一体。

同一可执行文件以角色启动，两个实例（甚至不同机器/容器）各自监听端口，
通过 JSON + 二进制块 HTTP 互通；SQLite WAL 支持服务线程并发。

导出端
  POST /exports                      创建/幂等重放导出
  GET  /exports/{key}                幂等键状态
  GET  /bundles                      bundle 列表
  GET  /bundles/{id}                 manifest
  GET  /bundles/{id}/chunks/{i}      块字节（Content-Length + x-chunk-sha256）

导入端
  POST /imports                      创建搬运会话（从导出端拉 manifest）
  GET  /imports                      会话列表
  GET  /imports/{tid}                会话状态（持久检查点）
  POST /imports/{tid}/chunks/acquire 领取/接管块租约（fencing token）
  PUT  /imports/{tid}/chunks/{i}     提交块（x-fence-token；迟到提交 409）
  POST /imports/{tid}/activate       显式整体校验 + 原子启用
  POST /imports/{tid}/debug/sabotage 演示注入：delete_chunk/flip_chunk/fake_schema
  GET  /active                       当前服务中的 bundle 总览
  GET  /active/{feature}             active manifest
  GET  /active/{feature}/rows        按实体/历史 bundle 读取
"""
from __future__ import annotations

import json
import re
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Optional
from urllib.parse import parse_qs, unquote, urlsplit

from . import errors as err
from .client import _request  # 导入端作为客户端回源导出端拉 manifest
from .errors import SnapshotError
from .exporter import ExportService
from .importer import ImportService


def _http_fetcher_factory(timeout: float = 30.0):
    def fetch(base_url: str, bundle_id: str) -> dict:
        return _request("GET", f"{base_url}/bundles/{bundle_id}",
                        timeout=timeout)
    return fetch


class _Handler(BaseHTTPRequestHandler):
    server_version = "FeatForgeSnapshot/1.0"
    exporter: Optional[ExportService] = None
    importer: Optional[ImportService] = None

    def log_message(self, fmt, *args):  # 安静一点
        if getattr(self.server, "access_log", False):
            super().log_message(fmt, *args)

    # ---- 基础收发
    def _json_body(self) -> dict:
        n = int(self.headers.get("Content-Length", 0) or 0)
        raw = self.rfile.read(n) if n else b"{}"
        try:
            data = json.loads(raw.decode("utf-8") or "{}")
        except Exception:
            raise err.SnapshotError("请求体不是合法 JSON")
        if not isinstance(data, dict):
            raise err.SnapshotError("请求体必须是 JSON 对象")
        return data

    def _send_json(self, status: int, obj) -> None:
        body = json.dumps(obj, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _send_bytes(self, status: int, data: bytes,
                    digest: str) -> None:
        self.send_response(status)
        self.send_header("Content-Type", "application/octet-stream")
        self.send_header("Content-Length", str(len(data)))
        self.send_header("x-chunk-sha256", digest)
        self.end_headers()
        self.wfile.write(data)

    def _send_error(self, e: Exception) -> None:
        if isinstance(e, SnapshotError):
            status, code = e.http_status, e.code
        else:
            status, code = 500, "internal_error"
        self._send_json(status, {"error": {
            "code": code, "message": str(e),
            "type": type(e).__name__,
        }})

    # ---- 路由
    def _match(self, pattern: str):
        return re.fullmatch(pattern, urlsplit(self.path).path)

    def do_GET(self) -> None:  # noqa: N802
        try:
            parts = urlsplit(self.path)
            path, qs = parts.path, parse_qs(parts.query)

            if path == "/health":
                return self._send_json(200, {"ok": True})

            if self.exporter is not None:
                m = self._match(r"/bundles")
                if m:
                    return self._send_json(200, {"bundles":
                                                 self.exporter.list_bundles()})
                m = self._match(r"/bundles/([^/]+)/chunks/(\d+)")
                if m:
                    data, digest = self.exporter.get_chunk(
                        m.group(1), int(m.group(2)))
                    return self._send_bytes(200, data, digest)
                m = self._match(r"/bundles/([^/]+)")
                if m:
                    return self._send_json(200,
                                           self.exporter.get_bundle(m.group(1)))
                m = self._match(r"/exports/([^/]+)")
                if m:
                    return self._send_json(
                        200, self.exporter.get_export_status(
                            unquote(m.group(1))))

            if self.importer is not None:
                if path == "/imports":
                    return self._send_json(200, {
                        "transfers": self.importer.list_transfers()})
                m = self._match(r"/imports/([^/]+)/chunks/\d+")
                if m:
                    raise err.SnapshotError("块提交必须用 PUT，块经 POST /acquire 领取")
                m = self._match(r"/imports/([^/]+)$")
                if m:
                    return self._send_json(
                        200, self.importer.get_transfer(m.group(1)))
                if path == "/active":
                    return self._send_json(
                        200, {"active": self.importer.active_overview()})
                m = self._match(r"/active/([^/]+)/rows")
                if m:
                    limit = int(qs.get("limit", ["1000"])[0])
                    return self._send_json(200, self.importer.read_rows(
                        unquote(m.group(1)),
                        entity_id=qs.get("entity_id", [None])[0],
                        bundle_id=qs.get("bundle_id", [None])[0],
                        limit=limit))
                m = self._match(r"/active/([^/]+)")
                if m:
                    return self._send_json(
                        200, self.importer.active_manifest(unquote(m.group(1))))
            raise err.NotFound(f"无此路由: GET {path}")
        except Exception as e:  # noqa: BLE001
            self._send_error(e)

    def do_POST(self) -> None:  # noqa: N802
        try:
            path = urlsplit(self.path).path
            body = self._json_body()

            if self.exporter is not None and path == "/exports":
                fail_after = self.headers.get("X-Fail-After-Chunks")
                out = self.exporter.export(
                    idempotency_key=body["idempotency_key"],
                    feature=body["feature"],
                    version=int(body["version"]),
                    cutoff_ts=int(body["cutoff_ts"]),
                    rows_per_chunk=int(body.get("rows_per_chunk", 1000)),
                    params=body.get("params"),
                    fail_after_chunks=int(fail_after) if fail_after else None,
                )
                return self._send_json(201 if not out["replayed"] else 200,
                                       out)

            if self.importer is not None:
                if path == "/imports":
                    out = self.importer.create_transfer(
                        bundle_url=body["bundle_url"],
                        expected_schema_id=body.get("expected_schema_id"),
                        lease_ttl_seconds=body.get("lease_ttl_seconds"),
                        auto_activate=bool(body.get("auto_activate", True)),
                        idempotency_key=body.get("idempotency_key"),
                    )
                    return self._send_json(201 if not out.get("replayed")
                                           else 200, out)

                m = self._match(r"/imports/([^/]+)/chunks/acquire")
                if m:
                    lease = self.importer.acquire_chunk(
                        m.group(1), body.get("executor_id", "anonymous"))
                    return self._send_json(200, {"chunk": lease})

                m = self._match(r"/imports/([^/]+)/activate")
                if m:
                    return self._send_json(200,
                                           self.importer.activate(m.group(1)))

                m = self._match(r"/imports/([^/]+)/debug/sabotage")
                if m:
                    self.importer.prime_sabotage(m.group(1), body["mode"])
                    return self._send_json(200,
                                           {"armed": body["mode"]})
            raise err.NotFound(f"无此路由: POST {path}")
        except Exception as e:  # noqa: BLE001
            self._send_error(e)

    def do_PUT(self) -> None:  # noqa: N802
        try:
            m = self._match(r"/imports/([^/]+)/chunks/(\d+)")
            if not m or self.importer is None:
                raise err.NotFound("块提交路径应为 PUT /imports/{tid}/chunks/{i}")
            n = int(self.headers.get("Content-Length", 0) or 0)
            data = self.rfile.read(n)
            if self.headers.get("X-Demo-Corrupt") == "1":
                # 演示注入：翻转首字节，哈希必然不匹配（服务端在验 fence 后拒绝）
                data = bytes([data[0] ^ 0xFF]) + data[1:]
            token = self.headers.get("X-Fence-Token", "")
            out = self.importer.put_chunk(
                m.group(1), int(m.group(2)), token, data)
            self._send_json(200, out)
        except Exception as e:  # noqa: BLE001
            self._send_error(e)


def build_handler(exporter=None, importer=None):
    attrs = {"exporter": exporter, "importer": importer}
    return type("SnapshotHandler", (_Handler,), attrs)


def run_server(host: str, port: int, exporter=None, importer=None,
               access_log: bool = False) -> ThreadingHTTPServer:
    handler = build_handler(exporter, importer)
    httpd = ThreadingHTTPServer((host, port), handler)
    httpd.access_log = access_log
    return httpd
