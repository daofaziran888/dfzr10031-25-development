"""任务调度业务规则：状态流转、字段校验、波次排程与筛选口径都收在这里。

调度波次数据集是本模块产出的“可执行清单”：一组分析管道节点 + 一个数据批次
进来，按依赖关系拆成若干波次，循环依赖、缺失依赖和因此被阻塞的节点单独列出。
"""
from __future__ import annotations

from datetime import datetime
from typing import Any

from app.services.wave_planner import parse_dependencies, plan_waves
from app.store import store

MODULE = "scheduler"
WAVE_MODULE = "scheduler_wave"
BATCH_MODULE = "pipeline_batch"
RUN_MODULE = "pipeline_run"
PIPELINE_MODULE = "pipeline"
REQUIRED_FIELDS = ["任务编号", "任务名称", "调度类型"]
STATUS_ORDER = ["待执行", "执行中", "已完成", "执行失败"]
ACTION_RULES = {"立即执行": "执行中", "暂停任务": "待执行", "标记失败": "执行失败"}
NEGATIVE_ACTIONS = []

WAVE_ACTIONABLE = {"待领取", "待补算", "待重试"}


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _next_id(module: str) -> int:
    rows = store.rows(module)
    return max((int(row.get("id", 0)) for row in rows), default=0) + 1


def _split_codes(raw: Any) -> list[str]:
    return parse_dependencies(raw)


def _dataset_codes(raw: Any) -> list[str]:
    return parse_dependencies(raw)


def _format_version_map(versions: dict[str, str]) -> str:
    return "，".join(f"{code}@{version}" for code, version in versions.items()) or "—"


class SchedulerService:
    def list_entries(
        self,
        *,
        keyword: str | None = None,
        status: str | None = None,
        page: int = 1,
        size: int = 20,
        extra: dict[str, str] | None = None,
    ) -> tuple[list[dict[str, Any]], int]:
        rows = store.rows(MODULE)
        if keyword:
            rows = [row for row in rows if keyword in str(row.get("任务编号", ""))]
        if status:
            rows = [row for row in rows if row.get("status") == status]
        if extra:
            for field, val in extra.items():
                rows = [row for row in rows if val in str(row.get(field, ""))]
        total = len(rows)
        start = max(page - 1, 0) * size
        return rows[start:start + size], total

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        return store.find(MODULE, entry_id)

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        missing = [field for field in REQUIRED_FIELDS if not str(values.get(field) or "").strip()]
        if missing:
            return None, missing
        rows = store.rows(MODULE)
        entry = {"id": max((int(row.get("id", 0)) for row in rows), default=0) + 1}
        entry.update({field: values.get(field) for field in REQUIRED_FIELDS})
        entry["status"] = STATUS_ORDER[0]
        entry["pending"] = True
        entry["abnormal"] = False
        rows.append(entry)
        return entry, []

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None, f"调度任务 {entry_id} 不存在或已归档"
        if action not in ACTION_RULES:
            return None, f"动作「{action}」不属于任务调度可执行范围"
        target = ACTION_RULES[action]
        if target not in STATUS_ORDER:
            return None, f"目标状态「{target}」不在允许的状态序列里"
        entry["status"] = target
        entry["pending"] = target != STATUS_ORDER[-1]
        entry["abnormal"] = action in NEGATIVE_ACTIONS
        return entry, f"调度任务已{action}"

    # ------------------------------------------------------------------
    # 调度执行波次数据集
    # ------------------------------------------------------------------
    def get_batch(self, batch_code: str) -> dict[str, Any] | None:
        for row in store.rows(BATCH_MODULE):
            if str(row.get("批次编号")) == batch_code:
                return row
        return None

    def list_batches(self) -> list[dict[str, Any]]:
        return list(store.rows(BATCH_MODULE))

    def committed_output_version(self, batch_code: str, node_code: str) -> tuple[str | None, dict[str, Any] | None]:
        """批次内某个节点当前可用（产物状态为“当前”）的产物版本。"""
        committed = [
            row for row in store.rows(RUN_MODULE)
            if str(row.get("批次编号")) == batch_code
            and str(row.get("节点编号")) == node_code
            and row.get("status") == "已提交"
        ]
        current = [row for row in committed if row.get("产物状态", "当前") == "当前"]
        pool = current or []
        if not pool:
            return None, None
        latest = max(pool, key=lambda row: int(row.get("id", 0)))
        return str(latest.get("输出版本") or ""), latest

    def plan_execution_waves(
        self,
        *,
        batch_code: str,
        node_codes: list[str] | str | None = None,
        include_upstream: bool = True,
        plan_version: str = "V1",
        node_target_versions: dict[str, str] | None = None,
        expected_versions: dict[str, str] | None = None,
        reuse_committed: bool = False,
        deferred_codes: set[str] | None = None,
        remark: str = "",
    ) -> tuple[dict[str, Any] | None, str]:
        """为一组管道节点和指定数据批次生成调度执行波次数据集。

        - 只有依赖全部排进前序波次的节点才会进入波次；
        - 循环依赖、缺失依赖及被其阻塞的下游节点写入 ``blocked``，不进入波次；
        - 节点只有在“计划输入版本”都已提交后才转为 ``待领取``，否则为 ``未就绪``；
        - ``reuse_committed=True`` 时，已有相同目标版本产物的节点标记为“已复用”，
          不重复排程（重试恢复时用于保留未受影响支路）；
        - ``deferred_codes`` 中的节点（仍在执行旧版本的领取）标记为“待补算”，
          在前一笔执行单结束前不可再次领取。
        """
        batch = self.get_batch(batch_code)
        if batch is None:
            return None, f"数据批次 {batch_code} 不存在，无法计算调度波次"
        nodes = [dict(row) for row in store.rows(PIPELINE_MODULE)]
        if not nodes:
            return None, "分析管道还没有登记节点，无法计算调度波次"

        requested = _split_codes(node_codes) if node_codes else [str(row.get("节点编号")) for row in nodes]
        if not requested:
            return None, "请至少指定一个管道节点编号"

        plan = plan_waves(nodes, requested, include_upstream=include_upstream)
        if plan["unknown_selected"]:
            return None, f"节点编号不存在：{'、'.join(plan['unknown_selected'])}"

        node_by_code = {str(row.get("节点编号")): row for row in nodes}
        target_versions = dict(node_target_versions or {})
        expected = dict(expected_versions or {})
        deferred = set(deferred_codes or set())
        existing = [
            row for row in store.rows(WAVE_MODULE)
            if str(row.get("批次编号")) == batch_code and str(row.get("调度版本")) == plan_version
        ]
        for row in existing:
            store.rows(WAVE_MODULE).remove(row)

        now = _now()
        created: list[dict[str, Any]] = []
        wave_of: dict[str, int] = {}
        for wave in plan["waves"]:
            for code in wave["node_codes"]:
                wave_of[code] = int(wave["wave"])

        blocked_map = {item["节点编号"]: item["阻塞原因"] for item in plan["blocked"]}
        cycle_members = {code for group in plan["cycles"] for code in group}

        for code in plan["selected"]:
            node = node_by_code[code]
            dependencies = parse_dependencies(node.get("依赖节点"))
            planned_inputs: dict[str, str] = {}
            if dependencies:
                for dependency in dependencies:
                    if dependency in expected:
                        planned_inputs[dependency] = expected[dependency]
                    elif dependency in target_versions:
                        planned_inputs[dependency] = target_versions[dependency]
                    else:
                        version, _ = self.committed_output_version(batch_code, dependency)
                        planned_inputs[dependency] = (
                            version or target_versions.get(dependency) or plan_version.lower()
                        )
            else:
                external = batch.get("外部输入版本") or {}
                for dataset in _split_codes(node.get("输入数据集")):
                    planned_inputs[dataset] = str(external.get(dataset) or "待产出")

            target_version = target_versions.get(code) or plan_version.lower()
            actual_versions: dict[str, str | None] = {}
            for key in planned_inputs:
                if dependencies and key in node_by_code:
                    actual_versions[key], _ = self.committed_output_version(batch_code, key)
                else:
                    actual_versions[key] = batch.get("外部输入版本", {}).get(key)
            inputs_ready = all(
                actual_versions.get(key) is not None
                and (planned_inputs[key] == "待产出" or actual_versions.get(key) == planned_inputs[key])
                for key in planned_inputs
            )

            current_output, current_run = self.committed_output_version(batch_code, code)
            active_run = self._active_run(batch_code, code)
            active = active_run is not None

            if code in blocked_map:
                state = "阻塞"
                reason = blocked_map[code]
            elif current_output is not None and current_output == target_version:
                state = "已复用" if reuse_committed else "已提交"
                reason = (
                    f"已存在 {target_version} 产物，直接复用，不重复排程"
                    if reuse_committed
                    else ""
                )
            elif code in deferred or active:
                state = "待补算"
                reason = (
                    "旧版本执行单尚未结束，结束后按新版本补算"
                    if code in deferred
                    else "已有执行中的执行单，结束后再判断是否需要补算"
                )
            elif not inputs_ready:
                state = "未就绪"
                pending_inputs = [
                    f"{key}@{planned_inputs[key]}"
                    for key in planned_inputs
                    if not (
                        actual_versions.get(key) is not None
                        and (planned_inputs[key] == "待产出" or actual_versions.get(key) == planned_inputs[key])
                    )
                ]
                reason = "等待上游产物：" + ("、".join(pending_inputs) or "上游未提交")
            else:
                state = "待领取"
                reason = ""

            row: dict[str, Any] = {
                "id": _next_id(WAVE_MODULE),
                "status": state,
                "pending": state in WAVE_ACTIONABLE,
                "abnormal": state == "阻塞",
                "波次编号": f"{batch_code}-{plan_version}-W{wave_of.get(code, 0)}",
                "批次编号": batch_code,
                "数据批次": batch.get("数据批次", ""),
                "调度版本": plan_version,
                "波次": wave_of.get(code, 0),
                "节点编号": code,
                "节点名称": node.get("节点名称", code),
                "依赖节点": "，".join(dependencies) or "—",
                "输出产物": node.get("输出结果", ""),
                "计划输出版本": target_version,
                "计划输入版本": planned_inputs,
                "计划输入说明": _format_version_map(planned_inputs),
                "领取状态": state,
                "执行单编号": (
                    str(current_run.get("执行单编号"))
                    if state in {"已提交", "已复用"} and current_run is not None
                    else str(active_run.get("执行单编号")) if active_run is not None else ""
                ),
                "提交版本": str(current_run.get("输出版本")) if state in {"已提交", "已复用"} and current_run is not None else "",
                "阻塞原因": reason,
                "说明": reason or ("循环依赖" if code in cycle_members else ""),
                "生成时间": now,
                "备注": remark,
            }
            store.rows(WAVE_MODULE).append(row)
            created.append(row)

        summary = {
            "批次编号": batch_code,
            "数据批次": batch.get("数据批次", ""),
            "调度版本": plan_version,
            "波次": [
                {
                    "wave": wave["wave"],
                    "node_codes": wave["node_codes"],
                    "nodes": [
                        {
                            "节点编号": code,
                            "节点名称": node_by_code[code].get("节点名称", code),
                            "计划输出版本": target_versions.get(code) or plan_version.lower(),
                        }
                        for code in wave["node_codes"]
                    ],
                }
                for wave in plan["waves"]
            ],
            "cycles": plan["cycles"],
            "missing": plan["missing"],
            "blocked": plan["blocked"],
            "reused": [row for row in created if row["领取状态"] == "已复用"],
            "released": [row for row in created if row["领取状态"] == "待领取"],
            "pending": [row for row in created if row["领取状态"] in {"未就绪", "待补算"}],
            "wave_rows": created,
            "说明": (
                "波次已生成：依赖齐备的节点进入可领取范围，其余节点等待前序波次。"
                if not plan["cycles"] and not plan["missing"]
                else "波次已生成，但存在循环或缺失依赖，相关节点没有进入执行波次。"
            ),
            "生成时间": now,
        }
        return summary, "调度执行波次数据集已生成"

    def _active_run(self, batch_code: str, node_code: str) -> dict[str, Any] | None:
        for row in store.rows(RUN_MODULE):
            if (
                str(row.get("批次编号")) == batch_code
                and str(row.get("节点编号")) == node_code
                and row.get("status") in {"执行中", "待重试"}
            ):
                return row
        return None

    def get_wave(self, wave_id: int) -> dict[str, Any] | None:
        return store.find(WAVE_MODULE, wave_id)
