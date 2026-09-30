"""全局配置，均可通过环境变量覆盖。"""
from __future__ import annotations

import os
from dataclasses import dataclass


def _env_int(name: str, default: int) -> int:
    return int(os.environ.get(name, str(default)))


def _env_float(name: str, default: float) -> float:
    return float(os.environ.get(name, str(default)))


@dataclass
class Settings:
    db_path: str = os.environ.get("FEATFORGE_DB", "/tmp/featforge.db")

    # 默认分区长度与样本刻度（秒）。回填按分区推进并在样本刻度上物化。
    partition_seconds: int = _env_int("FEATFORGE_PARTITION_SECONDS", 3600)
    sample_tick_seconds: int = _env_int("FEATFORGE_SAMPLE_TICK_SECONDS", 300)

    # 代际切换门槛
    min_coverage: float = _env_float("FEATFORGE_MIN_COVERAGE", 0.95)
    max_bias: float = _env_float("FEATFORGE_MAX_BIAS", 0.05)
    max_online_offline_bias: float = _env_float("FEATFORGE_MAX_ONLINE_OFFLINE_BIAS", 0.0)
    min_online_offline_coverage: float = _env_float(
        "FEATFORGE_MIN_ONLINE_OFFLINE_COVERAGE", 0.95
    )
    # 在线/离线抽样比例（采样点之间）与最大采样数
    sample_fraction: float = _env_float("FEATFORGE_SAMPLE_FRACTION", 0.2)
    max_sample_pairs: int = _env_int("FEATFORGE_MAX_SAMPLE_PAIRS", 2000)

    worker_poll_seconds: float = _env_float("FEATFORGE_WORKER_POLL", 1.0)

    # 订正落地：每批包含的样本刻度数。每批一个事务提交并保存进度，
    # 进程崩溃后重启从未完成的批次继续。
    correction_batch_ticks: int = _env_int("FEATFORGE_CORRECTION_BATCH_TICKS", 12)

    # ---- 多租户仲裁器 ----
    # 租约 TTL：claim 后该时间内没有心跳/完成，分片回到 ready set 可被接管
    arbiter_lease_ttl: int = _env_int("FEATFORGE_ARBITER_LEASE_TTL", 30)
    # 欠额 + 老化选择器的老化速率：等待时长秒数乘以此值加分
    arbiter_aging_rate: float = _env_float("FEATFORGE_ARBITER_AGING_RATE", 0.1)
    # 仲裁历史保留条数（超出后自动截断最旧记录）
    arbiter_history_limit: int = _env_int("FEATFORGE_ARBITER_HISTORY_LIMIT", 500)
    # 仲裁器快照作业产物目录
    arbiter_snapshot_dir: str = os.environ.get(
        "FEATFORGE_ARBITER_SNAPSHOT_DIR", "/tmp/featforge-snapshots")

    # ---- 后台计算：容量凭证与预算结算 ----
    # 心跳 TTL：running 工单超过该时间无心跳即判失联、释放席位、可被接管
    budget_lease_ttl: int = _env_int("FEATFORGE_BUDGET_LEASE_TTL", 30)
    # 全局执行席位数（跨所有客户；凭证管“钱”，席位管“坑位”，暂停超额
    # 时必须立即让出坑位，不能继续占着）
    budget_seats: int = _env_int("FEATFORGE_BUDGET_SEATS", 4)
    # 台账查询/状态页默认返回上限
    budget_ledger_limit: int = _env_int("FEATFORGE_BUDGET_LEDGER_LIMIT", 1000)


settings = Settings()
