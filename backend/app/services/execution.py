"""分析管道执行运行时：领取、执行回执、更正影响与重试恢复。

调度侧负责把节点拆成执行波次（``app.services.scheduler``），本模块负责按波次
领取执行单、校验“版本明确的输入”、记录实际消费的上游产物、提交结果，并在上游
更正时找出受影响的支路和可复用产物。

状态口径：

- 波次行 ``领取状态``：未就绪 → 待领取 → 执行中 → 已提交；
  失败后转 ``待重试``；被更正作废转 ``已作废``；无需重算转 ``已复用``；
  等待补算转 ``待补算``。
- 执行单 ``status``：执行中、已提交、失败、已作废。
- 产物 ``产物状态``：当前、历史版本、已作废。只有“当前”产物可以对外发布。
"""
from __future__ import annotations

from datetime import datetime
from typing import Any

from app.services.wave_planner import parse_dependencies, parse_versions
from app.store import store

BATCH_MODULE = "pipeline_batch"
NODE_MODULE = "pipeline"
WAVE_MODULE = "scheduler_wave"
RUN_MODULE = "pipeline_run"
CORRECTION_MODULE = "pipeline_correction"

CLAIMABLE_STATES = {"待领取", "待补算", "待重试"}
ACTIVE_RUN_STATES = {"执行中", "待重试"}
COMMITTED = "已提交"
CURRENT_ARTIFACT = "当前"
HISTORICAL_ARTIFACT = "历史版本"


def now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def next_id(module: str) -> int:
    rows = store.rows(module)
    return max((int(row.get("id", 0)) for row in rows), default=0) + 1


def version_number(version: str) -> int:
    text = str(version or "").strip().lower()
    digits = "".join(ch for ch in text if ch.isdigit())
    return int(digits) if digits else 0


def next_plan_version(batch_code: str) -> str:
    versions = [
        str(row.get("调度版本"))
        for row in store.rows(WAVE_MODULE)
        if str(row.get("批次编号")) == batch_code and row.get("调度版本")
    ]
    highest = max((version_number(version) for version in versions), default=0)
    return f"V{highest + 1}"


class ExecutionService:
    # ------------------------------------------------------------------
    # 基础查询
    # ------------------------------------------------------------------
    def get_batch(self, batch_code: str) -> dict[str, Any] | None:
        for row in store.rows(BATCH_MODULE):
            if str(row.get("批次编号")) == batch_code:
                return row
        return None

    def list_batches(self) -> list[dict[str, Any]]:
        return list(store.rows(BATCH_MODULE))

    def create_batch(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, str]:
        code = str(values.get("批次编号") or "").strip()
        partition = str(values.get("数据批次") or "").strip()
        if not code or not partition:
            return None, "缺少必填字段：批次编号、数据批次"
        if self.get_batch(code) is not None:
            return None, f"数据批次 {code} 已存在"
        external = values.get("外部输入版本") or {}
        if not isinstance(external, dict):
            return None, "外部输入版本需要是 {数据集: 版本} 的映射"
        batch = {
            "id": next_id(BATCH_MODULE),
            "status": "执行中",
            "pending": True,
            "abnormal": False,
            "批次编号": code,
            "数据批次": partition,
            "批次状态": "执行中",
            "外部输入版本": {str(key): str(value) for key, value in external.items()},
            "说明": str(values.get("说明") or ""),
        }
        store.rows(BATCH_MODULE).append(batch)
        return batch, "数据批次已登记"

    def batch_partition(self, batch_code: str) -> str:
        batch = self.get_batch(batch_code)
        return str(batch.get("数据批次", "")) if batch else ""

    def node(self, node_code: str) -> dict[str, Any] | None:
        for row in store.rows(NODE_MODULE):
            if str(row.get("节点编号")) == node_code:
                return row
        return None

    def list_runs(self, batch_code: str | None = None, plan_version: str | None = None) -> list[dict[str, Any]]:
        rows = list(store.rows(RUN_MODULE))
        if batch_code:
            rows = [row for row in rows if str(row.get("批次编号")) == batch_code]
        if plan_version:
            rows = [row for row in rows if str(row.get("调度版本")) == plan_version]
        rows.sort(key=lambda row: int(row.get("id", 0)))
        return rows

    def get_run(self, run_id: int) -> dict[str, Any] | None:
        return store.find(RUN_MODULE, run_id)

    def committed_runs(self, batch_code: str, node_code: str) -> list[dict[str, Any]]:
        rows = [
            row for row in store.rows(RUN_MODULE)
            if str(row.get("批次编号")) == batch_code
            and str(row.get("节点编号")) == node_code
            and row.get("status") == COMMITTED
        ]
        rows.sort(key=lambda row: int(row.get("id", 0)))
        return rows

    def current_run(self, batch_code: str, node_code: str) -> dict[str, Any] | None:
        """节点在该批次当前可用的产物（产物状态为“当前”的最近一次提交）。"""
        current = [
            row for row in self.committed_runs(batch_code, node_code)
            if row.get("产物状态") == CURRENT_ARTIFACT
        ]
        if not current:
            return None
        return max(current, key=lambda row: int(row.get("id", 0)))

    def current_version(self, batch_code: str, node_code: str) -> str | None:
        run = self.current_run(batch_code, node_code)
        return str(run.get("输出版本")) if run else None

    def active_run(self, batch_code: str, node_code: str) -> dict[str, Any] | None:
        for row in store.rows(RUN_MODULE):
            if (
                str(row.get("批次编号")) == batch_code
                and str(row.get("节点编号")) == node_code
                and row.get("status") in ACTIVE_RUN_STATES
            ):
                return row
        return None

    def wave_row(self, batch_code: str, plan_version: str, node_code: str) -> dict[str, Any] | None:
        for row in store.rows(WAVE_MODULE):
            if (
                str(row.get("批次编号")) == batch_code
                and str(row.get("调度版本")) == plan_version
                and str(row.get("节点编号")) == node_code
            ):
                return row
        return None

    def attach_run_to_wave(self, batch_code: str, plan_version: str, node_code: str, run: dict[str, Any]) -> None:
        """执行中的领取没有对应的波次行（例如波次重排后补登）时，生成一行接住这次领取。"""
        wave = self.wave_row(batch_code, plan_version, node_code)
        consumed = dict(run.get("领取时消费版本") or {})
        if wave is None:
            node = self.node(node_code) or {}
            wave = {
                "id": next_id(WAVE_MODULE),
                "status": "执行中",
                "pending": True,
                "abnormal": False,
                "波次编号": f"{batch_code}-{plan_version}-W?",
                "批次编号": batch_code,
                "数据批次": self.batch_partition(batch_code),
                "调度版本": plan_version,
                "波次": 0,
                "节点编号": node_code,
                "节点名称": node.get("节点名称", node_code),
                "依赖节点": "，".join(parse_dependencies(node.get("依赖节点"))) or "—",
                "输出产物": node.get("输出结果", ""),
                "计划输出版本": run.get("输出版本") or "",
                "计划输入版本": dict(consumed),
                "计划输入说明": "，".join(f"{key}@{value}" for key, value in consumed.items()) or "—",
                "领取状态": "执行中",
                "执行单编号": "",
                "阻塞原因": "",
                "说明": "波次重排前已在执行的领取，保留在该版本下",
                "生成时间": now(),
                "备注": "",
            }
            store.rows(WAVE_MODULE).append(wave)
        wave["领取状态"] = "执行中"
        wave["status"] = "执行中"
        wave["pending"] = True
        wave["执行单编号"] = run.get("执行单编号", "")
        wave["实际消费版本"] = dict(consumed)
        wave["领取时间"] = run.get("领取时间", "")

    def latest_plan_version(self, batch_code: str) -> str | None:
        versions = [
            str(row.get("调度版本"))
            for row in store.rows(WAVE_MODULE)
            if str(row.get("批次编号")) == batch_code
        ]
        if not versions:
            return None
        return max(versions, key=version_number)

    # ------------------------------------------------------------------
    # 波次读取与可领取范围
    # ------------------------------------------------------------------
    def list_waves(self, batch_code: str, plan_version: str | None = None) -> dict[str, Any]:
        batch = self.get_batch(batch_code)
        if batch is None:
            return {}
        version = plan_version or self.latest_plan_version(batch_code) or ""
        rows = [
            row for row in store.rows(WAVE_MODULE)
            if str(row.get("批次编号")) == batch_code and str(row.get("调度版本")) == version
        ]
        rows.sort(key=lambda row: (int(row.get("波次") or 0), str(row.get("节点编号"))))
        return {
            "批次编号": batch_code,
            "数据批次": batch.get("数据批次", ""),
            "调度版本": version,
            "items": rows,
            "可领取": [row for row in rows if row.get("领取状态") in CLAIMABLE_STATES],
            "执行中": [row for row in rows if row.get("领取状态") == "执行中"],
            "已提交": [row for row in rows if row.get("领取状态") == "已提交"],
            "已复用": [row for row in rows if row.get("领取状态") == "已复用"],
            "未就绪": [row for row in rows if row.get("领取状态") in {"未就绪", "待补算"}],
            "已作废": [row for row in rows if row.get("领取状态") == "已作废"],
            "阻塞": [row for row in rows if row.get("领取状态") == "阻塞"],
        }

    def dependency_status(self, batch_code: str, plan_version: str, wave: dict[str, Any]) -> list[dict[str, Any]]:
        planned = wave.get("计划输入版本") or {}
        result = []
        for dependency in parse_dependencies(wave.get("依赖节点")):
            current = self.current_version(batch_code, dependency)
            expected = str(planned.get(dependency) or "")
            result.append({
                "依赖节点": dependency,
                "计划版本": expected,
                "当前已提交版本": current,
                "齐备": bool(current) and (not expected or current == expected),
            })
        return result

    # ------------------------------------------------------------------
    # 领取
    # ------------------------------------------------------------------
    def claim(
        self,
        batch_code: str,
        *,
        node_code: str,
        plan_version: str | None = None,
        consumed_versions: dict[str, str] | str | None = None,
        remark: str = "",
    ) -> tuple[dict[str, Any] | None, str]:
        batch = self.get_batch(batch_code)
        if batch is None:
            return None, f"数据批次 {batch_code} 不存在"
        node_code = str(node_code or "").strip()
        if not node_code:
            return None, "缺少节点编号"
        node = self.node(node_code)
        if node is None:
            return None, f"分析节点 {node_code} 不存在或已归档"

        version = plan_version or self.latest_plan_version(batch_code)
        if not version:
            return None, f"批次 {batch_code} 还没有生成调度执行波次，请先在任务调度里生成波次"
        wave = self.wave_row(batch_code, version, node_code)
        if wave is None:
            return None, f"节点 {node_code} 不在批次 {batch_code} 的调度版本 {version} 里"
        if wave.get("领取状态") not in CLAIMABLE_STATES:
            state = wave.get("领取状态")
            if state == "已复用":
                return None, f"节点 {node_code} 的产物已复用，无需再次领取"
            return None, f"节点 {node_code} 当前状态为「{state}」，不在可领取范围"

        active = self.active_run(batch_code, node_code)
        if active is not None:
            return None, f"节点 {node_code} 已有执行单 {active.get('执行单编号')} 在执行中，不能重复领取"

        dependencies = parse_dependencies(wave.get("依赖节点"))
        planned = dict(wave.get("计划输入版本") or {})
        provided = consumed_versions if isinstance(consumed_versions, dict) else parse_versions(consumed_versions)
        provided = {str(key).strip(): str(value).strip() for key, value in (provided or {}).items()}

        if dependencies:
            if not provided:
                expected_text = "，".join(f"{code}@{planned.get(code, '?')}" for code in dependencies)
                return None, f"领取时必须明确本次消费的上游产物版本，计划输入为：{expected_text}"
            missing = [code for code in dependencies if code not in provided]
            if missing:
                return None, f"消费版本缺少上游节点：{'、'.join(missing)}"
            mismatched = [
                f"{code} 计划 {planned.get(code)} / 实际 {provided.get(code)}"
                for code in dependencies
                if planned.get(code) and planned.get(code) != "待产出" and provided.get(code) != planned.get(code)
            ]
            if mismatched:
                return None, "消费版本与计划输入不一致：" + "；".join(mismatched)
            not_ready = []
            for code in dependencies:
                current = self.current_version(batch_code, code)
                if current is None or current != provided.get(code):
                    not_ready.append(f"{code} 当前已提交 {current or '无'}，请求消费 {provided.get(code)}")
            if not_ready:
                return None, "上游产物尚未齐备，节点不能进入执行：" + "；".join(not_ready)
        else:
            provided = provided or {}

        output_version = str(wave.get("计划输出版本") or "")
        conflict = self._artifact_conflict(batch_code, node_code, output_version, provided)
        if conflict:
            return None, conflict

        run_id = next_id(RUN_MODULE)
        run = {
            "id": run_id,
            "status": "执行中",
            "pending": True,
            "abnormal": False,
            "执行单编号": f"RUN-{batch_code}-{version}-{node_code}-{run_id:04d}",
            "批次编号": batch_code,
            "数据批次": batch.get("数据批次", ""),
            "调度版本": version,
            "波次": wave.get("波次", 0),
            "节点编号": node_code,
            "节点名称": node.get("节点名称", node_code),
            "输出产物": node.get("输出结果", ""),
            "领取时消费版本": dict(provided),
            "实际消费版本": {},
            "输出版本": "",
            "产物状态": "待提交",
            "领取时间": now(),
            "提交时间": "",
            "回执": None,
            "失败原因": "",
            "重试次数": 0,
            "允许原版本完成": False,
            "来源更正": "",
            "备注": remark,
        }
        store.rows(RUN_MODULE).append(run)

        if self.wave_row(batch_code, version, node_code) is None:
            self.attach_run_to_wave(batch_code, version, node_code, run)
        wave["领取状态"] = "执行中"
        wave["status"] = "执行中"
        wave["pending"] = True
        wave["异常"] = False
        wave["执行单编号"] = run["执行单编号"]
        wave["实际消费版本"] = dict(provided)
        wave["领取时间"] = run["领取时间"]

        node["status"] = "运行中"
        node["pending"] = True
        node["abnormal"] = False
        return run, f"节点 {node_code} 已领取，进入执行中"

    def _artifact_conflict(
        self,
        batch_code: str,
        node_code: str,
        output_version: str,
        consumed: dict[str, str],
    ) -> str | None:
        """同一份下游产物不能混合冲突版本：同版本产物必须消费同一组上游版本。"""
        for run in self.committed_runs(batch_code, node_code):
            if str(run.get("输出版本")) != output_version:
                continue
            existing_inputs = dict(run.get("实际消费版本") or run.get("领取时消费版本") or {})
            if existing_inputs != consumed:
                return (
                    f"产物 {node_code}@{output_version} 已存在且消费的上游版本不同，"
                    "同一份下游产物不能混合冲突版本；请提升输出版本"
                )
        return None

    # ------------------------------------------------------------------
    # 执行回执
    # ------------------------------------------------------------------
    def receipt(
        self,
        run_id: int,
        *,
        output_version: str | None = None,
        consumed_versions: dict[str, str] | str | None = None,
        output_checksum: str = "",
        result_summary: str = "",
        remark: str = "",
    ) -> tuple[dict[str, Any] | None, str, list[dict[str, Any]]]:
        run = self.get_run(run_id)
        if run is None:
            return None, f"执行单 {run_id} 不存在", []
        if run.get("status") != "执行中":
            return None, f"执行单 {run.get('执行单编号')} 当前状态为「{run.get('status')}」，不能回执", []

        batch_code = str(run.get("批次编号"))
        node_code = str(run.get("节点编号"))
        claimed_inputs = dict(run.get("领取时消费版本") or {})
        actual = consumed_versions if isinstance(consumed_versions, dict) else parse_versions(consumed_versions)
        actual = {str(key).strip(): str(value).strip() for key, value in (actual or {}).items()}
        if actual and actual != claimed_inputs:
            return None, "回执中的实际消费版本与领取时不一致，执行单不能提交", []
        actual = actual or dict(claimed_inputs)

        dependencies = parse_dependencies(self._node_dependencies(node_code))
        drift: list[str] = []
        for dependency in dependencies:
            current = self.current_version(batch_code, dependency)
            consumed = actual.get(dependency)
            if consumed is None:
                drift.append(f"{dependency} 缺少消费版本")
            elif current != consumed:
                drift.append(f"{dependency} 已从 {consumed} 更正为 {current or '无'}")
        if drift and not run.get("允许原版本完成"):
            return None, (
                "上游产物在领取后发生变化：" + "；".join(drift)
                + "。请先按更正记录处理（作废重规划或允许原版本完成并补算）"
            ), []

        target_version = str(output_version or run.get("输出版本") or "").strip()
        if not target_version:
            wave = self.wave_row(batch_code, str(run.get("调度版本")), node_code)
            target_version = str((wave or {}).get("计划输出版本") or "")
        if not target_version:
            return None, "回执必须写明本次产出的版本", []

        conflict = self._artifact_conflict(batch_code, node_code, target_version, actual)
        if conflict:
            return None, conflict, []

        stale = bool(drift) or bool(run.get("允许原版本完成"))
        if stale:
            artifact_state = HISTORICAL_ARTIFACT
        else:
            artifact_state = CURRENT_ARTIFACT
            for previous in self.committed_runs(batch_code, node_code):
                if previous is run:
                    continue
                if previous.get("产物状态") == CURRENT_ARTIFACT:
                    previous["产物状态"] = HISTORICAL_ARTIFACT
                    previous["状态说明"] = f"已被 {target_version} 取代，转为历史版本"

        run["status"] = COMMITTED
        run["pending"] = False
        run["abnormal"] = False
        run["输出版本"] = target_version
        run["实际消费版本"] = dict(actual)
        run["产物状态"] = artifact_state
        run["提交时间"] = now()
        run["回执"] = {
            "回执时间": run["提交时间"],
            "输出版本": target_version,
            "实际消费版本": dict(actual),
            "输出校验": output_checksum,
            "结果摘要": result_summary,
            "备注": remark,
            "允许历史版本": stale,
        }
        if stale:
            run["状态说明"] = "上游已更正，本执行单按原领取版本完成，仅作历史快照"

        wave = self.wave_row(batch_code, str(run.get("调度版本")), node_code)
        if wave is not None:
            wave["领取状态"] = "已提交"
            wave["status"] = "已提交"
            wave["pending"] = False
            wave["abnormal"] = False
            wave["实际消费版本"] = dict(actual)
            wave["提交版本"] = target_version
            wave["提交时间"] = run["提交时间"]

        node = self.node(node_code)
        if node is not None:
            node["status"] = "已完成"
            node["pending"] = False
            node["abnormal"] = False

        released = self.refresh_waves(batch_code, None)
        return run, f"执行单 {run.get('执行单编号')} 已提交，产出 {node_code}@{target_version}", released

    def _node_dependencies(self, node_code: str) -> str:
        node = self.node(node_code)
        return str(node.get("依赖节点") or "") if node else ""

    def refresh_waves(self, batch_code: str, plan_version: str | None) -> list[dict[str, Any]]:
        """上游提交后更新下游可领取范围：依赖齐备的节点从“未就绪/待补算”转为“待领取”。

        ``plan_version`` 为空时刷新该批次的所有调度版本（补算要在新版本里放行）。
        """
        rows = [
            row for row in store.rows(WAVE_MODULE)
            if str(row.get("批次编号")) == batch_code
            and (plan_version is None or str(row.get("调度版本")) == plan_version)
        ]
        released: list[dict[str, Any]] = []
        changed = True
        while changed:
            changed = False
            for wave in rows:
                if wave.get("领取状态") not in {"未就绪", "待补算"}:
                    continue
                node_code = str(wave.get("节点编号"))
                dependencies = parse_dependencies(wave.get("依赖节点"))
                planned = dict(wave.get("计划输入版本") or {})
                ready = True
                for dependency in dependencies:
                    current = self.current_version(batch_code, dependency)
                    expected = str(planned.get(dependency) or "")
                    if current is None or (expected and expected != "待产出" and current != expected):
                        ready = False
                        break
                if not ready:
                    continue
                if self.active_run(batch_code, node_code) is not None:
                    continue
                wave["领取状态"] = "待领取"
                wave["status"] = "待领取"
                wave["pending"] = True
                wave["abnormal"] = False
                wave["阻塞原因"] = ""
                wave["说明"] = f"上游产物已齐备，{now()} 放行"
                wave["释放时间"] = now()
                released.append(wave)
                changed = True
        return released

    # ------------------------------------------------------------------
    # 失败、重试、作废
    # ------------------------------------------------------------------
    def fail(self, run_id: int, reason: str = "") -> tuple[dict[str, Any] | None, str]:
        run = self.get_run(run_id)
        if run is None:
            return None, f"执行单 {run_id} 不存在"
        if run.get("status") != "执行中":
            return None, f"执行单 {run.get('执行单编号')} 当前状态为「{run.get('status')}」，不能标记失败"
        run["status"] = "失败"
        run["pending"] = True
        run["abnormal"] = True
        run["失败原因"] = reason or "执行失败"
        run["提交时间"] = now()
        wave = self.wave_row(str(run.get("批次编号")), str(run.get("调度版本")), str(run.get("节点编号")))
        if wave is not None:
            wave["领取状态"] = "待重试"
            wave["status"] = "待重试"
            wave["pending"] = True
            wave["abnormal"] = True
        node = self.node(str(run.get("节点编号")))
        if node is not None:
            node["status"] = "失败"
            node["pending"] = True
            node["abnormal"] = True
        return run, "执行单已标记失败，等待重试"

    def retry(self, run_id: int, *, remark: str = "") -> tuple[dict[str, Any] | None, str]:
        run = self.get_run(run_id)
        if run is None:
            return None, f"执行单 {run_id} 不存在"
        if run.get("status") != "失败":
            return None, f"只有失败的执行单可以重试，当前状态为「{run.get('status')}」"
        batch_code = str(run.get("批次编号"))
        node_code = str(run.get("节点编号"))
        wave = self.wave_row(batch_code, str(run.get("调度版本")), node_code)
        if wave is not None and wave.get("领取状态") != "待重试":
            return None, f"波次行当前状态为「{wave.get('领取状态')}」，不允许重试"
        new_run, message = self.claim(
            batch_code,
            node_code=node_code,
            plan_version=str(run.get("调度版本")),
            consumed_versions=dict(run.get("领取时消费版本") or {}),
            remark=remark or f"由 {run.get('执行单编号')} 重试",
        )
        if new_run is None:
            return None, message
        new_run["重试次数"] = int(run.get("重试次数") or 0) + 1
        new_run["重试来源"] = run.get("执行单编号", "")
        if wave is not None:
            wave["重试次数"] = new_run["重试次数"]
        return new_run, f"已重新领取执行单 {new_run.get('执行单编号')}"

    def void(self, run_id: int, reason: str = "") -> tuple[dict[str, Any] | None, str]:
        run = self.get_run(run_id)
        if run is None:
            return None, f"执行单 {run_id} 不存在"
        if run.get("status") != "执行中":
            return None, f"只有执行中的执行单可以作废，当前状态为「{run.get('status')}」"
        run["status"] = "已作废"
        run["pending"] = False
        run["abnormal"] = True
        run["产物状态"] = "已作废"
        run["作废时间"] = now()
        run["作废原因"] = reason or "人工作废"
        wave = self.wave_row(str(run.get("批次编号")), str(run.get("调度版本")), str(run.get("节点编号")))
        if wave is not None:
            wave["领取状态"] = "已作废"
            wave["status"] = "已作废"
            wave["pending"] = False
            wave["abnormal"] = True
            wave["执行单编号"] = ""
            wave["作废原因"] = run["作废原因"]
        node = self.node(str(run.get("节点编号")))
        if node is not None:
            node["status"] = "已配置"
            node["pending"] = True
            node["abnormal"] = False
        return run, f"执行单 {run.get('执行单编号')} 已作废"

    # ------------------------------------------------------------------
    # 上游更正
    # ------------------------------------------------------------------
    def _downstream_closure(self, node_code: str, include_self: bool = False) -> list[str]:
        children: dict[str, list[str]] = {}
        for node in store.rows(NODE_MODULE):
            code = str(node.get("节点编号"))
            for dependency in parse_dependencies(node.get("依赖节点")):
                children.setdefault(dependency, []).append(code)
        seen: set[str] = set()
        queue = list(children.get(node_code, []))
        while queue:
            code = queue.pop(0)
            if code in seen:
                continue
            seen.add(code)
            queue.extend(children.get(code, []))
        if include_self:
            seen.add(node_code)
        return sorted(seen, key=lambda code: self._node_order(code))

    def _node_order(self, node_code: str) -> tuple[int, str]:
        node = self.node(node_code) or {}
        raw = str(node.get("执行顺序") or "")
        return (int(raw) if raw.isdigit() else 10**9, node_code)

    def _branch_path(self, node_code: str) -> list[str]:
        path = [node_code]
        seen = {node_code}
        current = node_code
        while True:
            node = self.node(current) or {}
            dependencies = parse_dependencies(node.get("依赖节点"))
            if not dependencies:
                break
            current = dependencies[0]
            if current in seen:
                break
            seen.add(current)
            path.append(current)
        return list(reversed(path))

    def _branch_label(self, node_code: str) -> str:
        names = []
        for code in self._branch_path(node_code):
            node = self.node(code) or {}
            names.append(str(node.get("节点名称") or code))
        return "→".join(names)

    def _in_flight_affected(self, batch_code: str, affected: list[str]) -> list[dict[str, Any]]:
        """领取了受影响上游（或本身就在受影响支路上）且尚未提交的领取。"""
        affected_set = set(affected)
        result = []
        for run in store.rows(RUN_MODULE):
            if str(run.get("批次编号")) != batch_code:
                continue
            if run.get("status") != "执行中":
                continue
            node_code = str(run.get("节点编号"))
            consumed = dict(run.get("领取时消费版本") or {})
            if node_code in affected_set or any(dep in affected_set for dep in consumed):
                result.append(run)
        return result

    def correction_analysis(self, batch_code: str, node_code: str, old_version: str, new_version: str) -> dict[str, Any]:
        affected = self._downstream_closure(node_code)
        in_flight = self._in_flight_affected(batch_code, affected)
        reusable = []
        for run in store.rows(RUN_MODULE):
            if str(run.get("批次编号")) != batch_code:
                continue
            if run.get("status") != COMMITTED or run.get("产物状态") != CURRENT_ARTIFACT:
                continue
            code = str(run.get("节点编号"))
            if code in affected:
                continue
            reusable.append({
                "节点编号": code,
                "节点名称": run.get("节点名称", code),
                "产物版本": run.get("输出版本"),
                "执行单编号": run.get("执行单编号"),
                "支路": self._branch_label(code),
            })
        join_branches = []
        for code in affected:
            node = self.node(code) or {}
            dependencies = parse_dependencies(node.get("依赖节点"))
            if len(dependencies) < 2:
                continue
            for dependency in dependencies:
                current = self.current_version(batch_code, dependency)
                join_branches.append({
                    "汇合节点": code,
                    "支路": self._branch_label(dependency),
                    "末级上游": dependency,
                    "当前版本": current,
                    "受影响": dependency in affected or current != old_version,
                    "可复用": dependency not in affected and current == old_version,
                })
        return {
            "批次编号": batch_code,
            "更正节点": node_code,
            "原版本": old_version,
            "新版本": new_version,
            "受影响节点": affected,
            "在途执行单": [
                {
                    "执行单编号": run.get("执行单编号"),
                    "节点编号": run.get("节点编号"),
                    "领取时消费版本": run.get("领取时消费版本"),
                }
                for run in in_flight
            ],
            "可复用产物": reusable,
            "汇合支路": join_branches,
            "发布影响": self.publish_scope(batch_code),
        }

    def register_correction(
        self,
        batch_code: str,
        *,
        node_code: str,
        old_version: str,
        new_version: str,
        note: str = "",
    ) -> tuple[dict[str, Any] | None, str]:
        batch = self.get_batch(batch_code)
        if batch is None:
            return None, f"数据批次 {batch_code} 不存在"
        node_code = str(node_code or "").strip()
        old_version = str(old_version or "").strip()
        new_version = str(new_version or "").strip()
        if not node_code or not old_version or not new_version:
            return None, "缺少必填字段：节点编号、原版本、新版本"
        if old_version == new_version:
            return None, "原版本与新版本不能相同"
        node = self.node(node_code)
        if node is None:
            return None, f"分析节点 {node_code} 不存在或已归档"
        old_run = None
        for run in self.committed_runs(batch_code, node_code):
            if str(run.get("输出版本")) == old_version:
                old_run = run
        if old_run is None:
            return None, f"节点 {node_code} 在批次 {batch_code} 里没有 {old_version} 的已提交产物"

        partition = self.batch_partition(batch_code)
        correction_id = next_id(CORRECTION_MODULE)
        correction = {
            "id": correction_id,
            "status": "待处理",
            "pending": True,
            "abnormal": True,
            "更正编号": f"CORR-{partition.replace('-', '')}-{correction_id:03d}",
            "批次编号": batch_code,
            "数据分区": partition,
            "节点编号": node_code,
            "节点名称": node.get("节点名称", node_code),
            "原版本": old_version,
            "新版本": new_version,
            "更正时间": now(),
            "说明": note or f"{node_code} 在同一分区产出更正：{old_version} → {new_version}",
            "处理方式": "",
            "新调度版本": "",
            "解析时间": "",
        }
        store.rows(CORRECTION_MODULE).append(correction)

        # 更正产物由上游直接产出：登记为已提交执行单，旧版本转为历史版本。
        existing_new = [
            run for run in self.committed_runs(batch_code, node_code)
            if str(run.get("输出版本")) == new_version
        ]
        if not existing_new:
            run_id = next_id(RUN_MODULE)
            input_versions = dict(old_run.get("实际消费版本") or old_run.get("领取时消费版本") or {})
            new_run = {
                "id": run_id,
                "status": COMMITTED,
                "pending": False,
                "abnormal": False,
                "执行单编号": f"RUN-{batch_code}-CORR-{node_code}-{run_id:04d}",
                "批次编号": batch_code,
                "数据批次": partition,
                "调度版本": str(old_run.get("调度版本") or ""),
                "波次": old_run.get("波次", 0),
                "节点编号": node_code,
                "节点名称": node.get("节点名称", node_code),
                "输出产物": node.get("输出结果", ""),
                "领取时消费版本": input_versions,
                "实际消费版本": dict(input_versions),
                "输出版本": new_version,
                "产物状态": CURRENT_ARTIFACT,
                "领取时间": correction["更正时间"],
                "提交时间": correction["更正时间"],
                "回执": {
                    "回执时间": correction["更正时间"],
                    "输出版本": new_version,
                    "实际消费版本": dict(input_versions),
                    "输出校验": f"correction:{correction['更正编号']}",
                    "结果摘要": "上游对同一分区重新产出",
                    "备注": note,
                    "允许历史版本": False,
                },
                "失败原因": "",
                "重试次数": 0,
                "允许原版本完成": False,
                "来源更正": correction["更正编号"],
                "备注": note,
            }
            store.rows(RUN_MODULE).append(new_run)
        for run in self.committed_runs(batch_code, node_code):
            if str(run.get("输出版本")) != new_version and run.get("产物状态") == CURRENT_ARTIFACT:
                run["产物状态"] = HISTORICAL_ARTIFACT
                run["状态说明"] = f"已被更正版本 {new_version} 取代，转为历史版本"

        analysis = self.correction_analysis(batch_code, node_code, old_version, new_version)
        correction["影响分析"] = analysis
        return correction, f"已登记上游更正 {correction['更正编号']}，并识别受影响的支路"

    def list_corrections(self, batch_code: str | None = None) -> list[dict[str, Any]]:
        rows = list(store.rows(CORRECTION_MODULE))
        if batch_code:
            rows = [row for row in rows if str(row.get("批次编号")) == batch_code]
        return rows

    def _expected_versions(self, batch_code: str, producer: str, new_version: str, plan_version: str, affected: list[str]) -> dict[str, str]:
        expected: dict[str, str] = {}
        for node in store.rows(NODE_MODULE):
            code = str(node.get("节点编号"))
            current = self.current_version(batch_code, code)
            if current:
                expected[code] = current
        expected[producer] = new_version
        for code in affected:
            expected[code] = plan_version.lower()
        return expected

    def resolve_correction(
        self,
        correction_id: int,
        strategy: str,
        *,
        remark: str = "",
    ) -> tuple[dict[str, Any] | None, str]:
        """按选定策略处理上游更正。

        - ``void_and_replan``：作废尚未提交的领取，以新版本重新放行受影响支路；
        - ``finish_and_backfill``：允许原领取按旧版本跑完并标记历史版本，另排补算。
        """
        correction = store.find(CORRECTION_MODULE, correction_id)
        if correction is None:
            return None, f"更正记录 {correction_id} 不存在"
        if correction.get("status") != "待处理":
            return None, f"更正记录 {correction.get('更正编号')} 已处理，不能重复解析"
        if strategy not in {"void_and_replan", "finish_and_backfill"}:
            return None, "处理方式只能是 void_and_replan 或 finish_and_backfill"

        batch_code = str(correction.get("批次编号"))
        producer = str(correction.get("节点编号"))
        old_version = str(correction.get("原版本"))
        new_version = str(correction.get("新版本"))
        affected = self._downstream_closure(producer)
        in_flight = self._in_flight_affected(batch_code, affected)
        plan_version = next_plan_version(batch_code)
        deferred: set[str] = set()

        voided: list[dict[str, Any]] = []
        if strategy == "void_and_replan":
            for run in in_flight:
                result, message = self.void(
                    int(run.get("id")),
                    reason=f"上游 {producer} 更正为 {new_version}，原 {old_version} 领取作废",
                )
                if result is not None:
                    voided.append({"执行单编号": result.get("执行单编号"), "说明": message})
        else:
            for run in in_flight:
                run["允许原版本完成"] = True
                run["补算"] = True
                run["状态说明"] = (
                    f"上游 {producer} 已更正为 {new_version}；本执行单按原领取版本完成，"
                    "结果仅作历史快照，另排补算"
                )
                deferred.add(str(run.get("节点编号")))

        expected = self._expected_versions(batch_code, producer, new_version, plan_version, affected)
        target_versions: dict[str, str] = {}
        for node in store.rows(NODE_MODULE):
            code = str(node.get("节点编号"))
            if code not in expected:
                continue
            if code == producer:
                target_versions[code] = new_version
            elif code in affected:
                target_versions[code] = plan_version.lower()
            else:
                target_versions[code] = expected[code]

        from app.services.scheduler import SchedulerService  # 局部导入避免循环依赖

        summary, message = SchedulerService().plan_execution_waves(
            batch_code=batch_code,
            node_codes=affected,
            include_upstream=True,
            plan_version=plan_version,
            node_target_versions=target_versions,
            expected_versions=expected,
            reuse_committed=True,
            deferred_codes=deferred,
            remark=remark or f"更正 {correction.get('更正编号')} 处理：{strategy}",
        )
        if summary is None:
            return None, message

        correction["status"] = "已解析"
        correction["pending"] = False
        correction["abnormal"] = False
        correction["处理方式"] = strategy
        correction["新调度版本"] = plan_version
        correction["解析时间"] = now()
        correction["处理说明"] = (
            "作废未提交的领取，以新版本重新放行受影响支路；未受影响支路复用已提交产物。"
            if strategy == "void_and_replan"
            else "允许原领取按旧版本完成并标记历史版本，同时排入补算任务。"
        )
        correction["影响分析"] = self.correction_analysis(batch_code, producer, old_version, new_version)

        return {
            "更正记录": correction,
            "处理方式": strategy,
            "作废执行单": voided,
            "新调度版本": plan_version,
            "调度波次": summary,
            "发布范围": self.publish_scope(batch_code),
        }, f"更正 {correction.get('更正编号')} 已按「{strategy}」处理，生成调度版本 {plan_version}"

    # ------------------------------------------------------------------
    # 发布范围与追溯
    # ------------------------------------------------------------------
    def publish_scope(self, batch_code: str) -> dict[str, Any]:
        batch = self.get_batch(batch_code)
        if batch is None:
            return {}
        planned_codes = {
            str(row.get("节点编号"))
            for row in store.rows(WAVE_MODULE)
            if str(row.get("批次编号")) == batch_code
        } | {
            str(row.get("节点编号"))
            for row in store.rows(RUN_MODULE)
            if str(row.get("批次编号")) == batch_code
        }
        nodes = [
            node for node in store.rows(NODE_MODULE)
            if str(node.get("节点编号")) in planned_codes
        ]
        open_corrections = [
            row for row in store.rows(CORRECTION_MODULE)
            if str(row.get("批次编号")) == batch_code and row.get("status") == "待处理"
        ]
        stale_nodes = {str(row.get("节点编号")) for row in open_corrections}
        for row in open_corrections:
            stale_nodes.update(self._downstream_closure(str(row.get("节点编号"))))

        publishable: list[dict[str, Any]] = []
        blocked: list[dict[str, Any]] = []
        for node in nodes:
            code = str(node.get("节点编号"))
            current = self.current_run(batch_code, code)
            if current is None:
                latest = self.committed_runs(batch_code, code)
                reason = "没有“当前”产物"
                if latest:
                    reason = f"最新提交 {latest[-1].get('输出版本')} 不是当前版本（{latest[-1].get('产物状态')}）"
                else:
                    active = self.active_run(batch_code, code)
                    if active is not None:
                        reason = f"执行单 {active.get('执行单编号')} 尚未提交"
                blocked.append({"节点编号": code, "节点名称": node.get("节点名称", code), "原因": reason})
                continue
            consumed = dict(current.get("实际消费版本") or current.get("领取时消费版本") or {})
            mismatched = []
            for dependency, version in consumed.items():
                actual = self.current_version(batch_code, dependency)
                if actual is not None and actual != version:
                    mismatched.append(f"{dependency} 消费 {version}，当前 {actual}")
            if code in stale_nodes:
                blocked.append({
                    "节点编号": code,
                    "节点名称": node.get("节点名称", code),
                    "产物版本": current.get("输出版本"),
                    "原因": "受未解析的上游更正影响",
                })
            elif mismatched:
                blocked.append({
                    "节点编号": code,
                    "节点名称": node.get("节点名称", code),
                    "产物版本": current.get("输出版本"),
                    "原因": "消费的上游版本已过期：" + "；".join(mismatched),
                })
            else:
                publishable.append({
                    "节点编号": code,
                    "节点名称": node.get("节点名称", code),
                    "产物": node.get("输出结果", ""),
                    "产物版本": current.get("输出版本"),
                    "执行单编号": current.get("执行单编号"),
                    "消费版本": consumed,
                })

        join_nodes = [
            node for node in store.rows(NODE_MODULE)
            if len(parse_dependencies(node.get("依赖节点"))) >= 2
        ]
        joins = []
        for node in join_nodes:
            code = str(node.get("节点编号"))
            current = self.current_run(batch_code, code)
            joins.append({
                "节点编号": code,
                "节点名称": node.get("节点名称", code),
                "当前产物版本": current.get("输出版本") if current else None,
                "可发布": current is not None and any(item["节点编号"] == code for item in publishable),
                "原因": (
                    "已提交当前版本"
                    if current is not None and any(item["节点编号"] == code for item in publishable)
                    else "没有可发布的当前汇合结果"
                ),
            })

        return {
            "批次编号": batch_code,
            "数据批次": batch.get("数据批次", ""),
            "可发布产物": publishable,
            "不可发布产物": blocked,
            "汇合节点": joins,
            "未解析更正": [
                {"更正编号": row.get("更正编号"), "节点编号": row.get("节点编号")}
                for row in open_corrections
            ],
            "说明": (
                "当前批次可以按列出的产物版本对外发布"
                if publishable and not blocked
                else (
                    "部分产物可发布，其余节点暂不可发布，原因见不可发布产物"
                    if publishable
                    else "当前批次没有可对外发布的产物"
                )
            ),
        }

    def trace(self, batch_code: str) -> dict[str, Any]:
        batch = self.get_batch(batch_code)
        if batch is None:
            return {}
        latest = self.latest_plan_version(batch_code)
        waves = self.list_waves(batch_code, latest) if latest else {"items": []}
        runs = self.list_runs(batch_code)
        corrections = self.list_corrections(batch_code)
        affected: set[str] = set()
        for correction in corrections:
            if correction.get("status") == "待处理":
                affected.update(self._downstream_closure(str(correction.get("节点编号"))))

        run_views = []
        for run in runs:
            run_views.append({
                "执行单编号": run.get("执行单编号"),
                "节点编号": run.get("节点编号"),
                "节点名称": run.get("节点名称"),
                "调度版本": run.get("调度版本"),
                "状态": run.get("status"),
                "领取时消费版本": run.get("领取时消费版本"),
                "实际消费版本": run.get("实际消费版本"),
                "输出版本": run.get("输出版本"),
                "产物状态": run.get("产物状态"),
                "支路": self._branch_label(str(run.get("节点编号"))),
                "受影响": str(run.get("节点编号")) in affected,
                "回执": run.get("回执"),
                "作废原因": run.get("作废原因", ""),
                "允许原版本完成": run.get("允许原版本完成", False),
            })

        join_branches = []
        for wave in waves.get("items", []):
            node = self.node(str(wave.get("节点编号"))) or {}
            dependencies = parse_dependencies(node.get("依赖节点"))
            if len(dependencies) < 2:
                continue
            for dependency in dependencies:
                current = self.current_version(batch_code, dependency)
                join_branches.append({
                    "汇合节点": wave.get("节点编号"),
                    "支路": self._branch_label(dependency),
                    "末级上游": dependency,
                    "当前版本": current,
                    "需重新计算": dependency in affected,
                    "可复用": dependency not in affected and current is not None,
                })

        return {
            "批次编号": batch_code,
            "数据批次": batch.get("数据批次", ""),
            "调度版本": latest,
            "波次": waves,
            "执行单": run_views,
            "更正记录": corrections,
            "汇合支路": join_branches,
            "发布范围": self.publish_scope(batch_code),
        }
