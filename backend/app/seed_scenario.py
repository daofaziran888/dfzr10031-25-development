"""演示场景：一个批次、一组有分叉和汇合的管道节点，以及一次上游更正。

数据准备好之后，可以直接按“汇合节点已领取 v1 时收到 v2”的情形做策略选择：

- 渠道支路（PIPE-1001 → PIPE-1101 → PIPE-1201）已提交 v1，未受影响，可复用；
- 地域支路（PIPE-1102 → PIPE-1202）已提交 v1，随后收到 v2 更正；
- 汇合节点 PIPE-1301 已领取三条支路的 v1，尚未提交；
- 下游 PIPE-1302 依赖汇合结果，当前未就绪。
"""
from __future__ import annotations

from datetime import datetime
from typing import Any

from app.store import store

BATCH_CODE = "BATCH-20261119-01"
PARTITION = "2026-11-19"
PLAN_VERSION = "V1"


def _timestamp(hour: int, minute: int) -> str:
    return datetime(2026, 11, 19, hour, minute, 0).isoformat(timespec="seconds")


COMMITTED_NODES = [
    ("PIPE-1001", "ods.orders", "v1", {}, 8, 5),
    ("PIPE-1002", "dim.user", "v1", {}, 8, 8),
    ("PIPE-1101", "dwd.clean_orders", "v1", {"PIPE-1001": "v1"}, 8, 20),
    ("PIPE-1102", "dwd.clean_orders", "v1", {"PIPE-1001": "v1"}, 8, 24),
    ("PIPE-1103", "dwd.user_profile", "v1", {"PIPE-1002": "v1"}, 8, 26),
    ("PIPE-1201", "dws.channel_agg", "v1", {"PIPE-1101": "v1"}, 8, 40),
    ("PIPE-1202", "dws.region_agg", "v1", {"PIPE-1102": "v1"}, 8, 42),
    ("PIPE-1203", "dws.payment_recon", "v1", {"PIPE-1103": "v1"}, 8, 44),
]


def install() -> None:
    """把演示数据写进内存仓库；只在启动时执行一次。"""
    rows = store.rows("pipeline_batch")
    if rows:
        return
    rows.append({
        "id": 1,
        "status": "执行中",
        "pending": True,
        "abnormal": False,
        "批次编号": BATCH_CODE,
        "数据批次": PARTITION,
        "批次状态": "执行中",
        "外部输入版本": {"ods.orders": "v1", "dim.user": "v1"},
        "说明": "2026-11-19 日批",
    })

    run_rows = store.rows("pipeline_run")
    for index, (node_code, dataset, version, inputs, hour, minute) in enumerate(COMMITTED_NODES, start=1):
        node = _node(node_code)
        timestamp = _timestamp(hour, minute)
        run_rows.append({
            "id": index,
            "status": "已提交",
            "pending": False,
            "abnormal": False,
            "执行单编号": f"RUN-{BATCH_CODE}-{PLAN_VERSION}-{node_code}-{index:04d}",
            "批次编号": BATCH_CODE,
            "数据批次": PARTITION,
            "调度版本": PLAN_VERSION,
            "波次": _wave_of(node_code),
            "节点编号": node_code,
            "节点名称": node.get("节点名称", node_code),
            "输出产物": node.get("输出结果", dataset),
            "领取时消费版本": dict(inputs),
            "实际消费版本": dict(inputs),
            "输出版本": version,
            "产物状态": "当前",
            "领取时间": timestamp,
            "提交时间": timestamp,
            "回执": {
                "回执时间": timestamp,
                "输出版本": version,
                "实际消费版本": dict(inputs),
                "输出校验": f"sha256:seed-{node_code.lower()}-{version}",
                "结果摘要": f"{node_code} 在 {PARTITION} 分区产出 {version}",
                "备注": "",
                "允许历史版本": False,
            },
            "失败原因": "",
            "重试次数": 0,
            "允许原版本完成": False,
            "来源更正": "",
            "备注": "",
        })

    from app.services.scheduler import SchedulerService

    summary, _ = SchedulerService().plan_execution_waves(
        batch_code=BATCH_CODE,
        node_codes=None,
        include_upstream=True,
        plan_version=PLAN_VERSION,
        remark="日批初始排程",
    )
    if summary is None:
        return

    # 汇合节点已领取三条支路的 v1，但尚未提交。
    from app.services.execution import ExecutionService

    execution = ExecutionService()
    run, _ = execution.claim(
        BATCH_CODE,
        node_code="PIPE-1301",
        plan_version=PLAN_VERSION,
        consumed_versions={"PIPE-1201": "v1", "PIPE-1202": "v1", "PIPE-1203": "v1"},
    )
    if run is not None:
        run["领取时间"] = _timestamp(9, 2)
        wave = execution.wave_row(BATCH_CODE, PLAN_VERSION, "PIPE-1301")
        if wave is not None:
            wave["领取时间"] = run["领取时间"]


def _node(node_code: str) -> dict[str, Any]:
    for row in store.rows("pipeline"):
        if str(row.get("节点编号")) == node_code:
            return row
    return {}


def _wave_of(node_code: str) -> int:
    node = _node(node_code)
    raw = str(node.get("执行顺序") or "")
    return int(raw) if raw.isdigit() else 0
