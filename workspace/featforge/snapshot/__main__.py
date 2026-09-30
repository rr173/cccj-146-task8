"""命令行入口：独立快照搬运服务与执行器。

    python -m featforge.snapshot exporter --root DIR --port 8101 [--seed]
    python -m featforge.snapshot importer --root DIR --port 8102 \
        [--external-url http://host:8102]
    python -m featforge.snapshot worker --exporter URL --importer URL \
        --transfer ID --executor-id ID [options]
    python -m featforge.snapshot seed-db --root DIR     # 只播种演示数据
"""
from __future__ import annotations

import argparse

from .blobstore import BlobStore
from .db import SnapshotDB
from .demodata import seed_demo
from .executor import ImportExecutor
from .exporter import ExportService
from .httpapi import _http_fetcher_factory, run_server
from .importer import ImportService
from .source import DemoFeatureStore


def _add_common(p: argparse.ArgumentParser) -> None:
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, required=True)
    p.add_argument("--root", required=True, help="数据根目录（db + 对象存储）")


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="featforge.snapshot")
    sub = parser.add_subparsers(dest="role", required=True)

    pe = sub.add_parser("exporter", help="启动导出端服务")
    _add_common(pe)
    pe.add_argument("--seed", action="store_true", help="播种演示特征数据")
    pe.add_argument("--fence-ttl", type=float, default=30.0)
    pe.add_argument("--db-name", default="exporter.db")

    pi = sub.add_parser("importer", help="启动导入端服务")
    _add_common(pi)
    pi.add_argument("--external-url", default="",
                    help="导入端对外可达基址（写进租约的 chunk_url）")
    pi.add_argument("--lease-ttl", type=float, default=15.0)
    pi.add_argument("--db-name", default="importer.db")

    pw = sub.add_parser("worker", help="启动一个导入执行器")
    pw.add_argument("--exporter", required=True)
    pw.add_argument("--importer", required=True)
    pw.add_argument("--transfer", required=True)
    pw.add_argument("--executor-id", default="w1")
    pw.add_argument("--poll", type=float, default=0.2)
    pw.add_argument("--max-chunks", type=int, default=None)
    pw.add_argument("--hold-seconds", type=float, default=0.0,
                    help="领取后持有不提交的秒数（演示租约接管）")
    pw.add_argument("--corrupt", action="store_true",
                    help="提交时翻转首字节（演示坏块拒绝）")
    pw.add_argument("--die-after-acquire", action="store_true",
                    help="领取首块后退出码 7（模拟执行器崩溃）")
    pw.add_argument("--once", action="store_true",
                    help="只做一轮，不持续轮询")

    ps = sub.add_parser("seed-db", help="只播种演示数据后退出")
    ps.add_argument("--root", required=True)
    ps.add_argument("--db-name", default="features.db")

    args = parser.parse_args(argv)

    if args.role == "seed-db":
        store = DemoFeatureStore(f"{args.root}/{args.db_name}")
        print(seed_demo(store))
        return

    if args.role == "exporter":
        store = DemoFeatureStore(f"{args.root}/features.db")
        if args.seed:
            print("播种演示数据:", seed_demo(store))
        svc = ExportService(SnapshotDB(f"{args.root}/{args.db_name}"),
                            BlobStore(f"{args.root}/blobs"), store,
                            fence_ttl_seconds=args.fence_ttl)
        httpd = run_server(args.host, args.port, exporter=svc)
        print(f"exporter listening on http://{args.host}:{args.port}",
              flush=True)
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            pass
        return

    if args.role == "importer":
        svc = ImportService(
            SnapshotDB(f"{args.root}/{args.db_name}"),
            BlobStore(f"{args.root}/blobs"),
            _http_fetcher_factory(),
            lease_ttl_seconds=args.lease_ttl,
            external_base_url=args.external_url,
        )
        httpd = run_server(args.host, args.port, importer=svc)
        print(f"importer listening on http://{args.host}:{args.port}",
              flush=True)
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            pass
        return

    if args.role == "worker":
        ex = ImportExecutor(args.exporter, args.importer,
                            args.executor_id, poll_interval=args.poll)
        if args.once:
            outs = ex.run_once(
                args.transfer, max_chunks=args.max_chunks,
                hold_seconds=args.hold_seconds,
                corrupt_put=args.corrupt,
                die_after_acquire=args.die_after_acquire)
            for o in outs:
                print(f"chunk {o.index}: {o.status} {o.detail}")
        else:
            ex.run_forever(args.transfer)


if __name__ == "__main__":
    main()
