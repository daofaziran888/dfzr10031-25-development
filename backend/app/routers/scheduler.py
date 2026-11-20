"""任务调度接口：维护调度任务，并为一组管道节点和数据批次生成调度执行波次。

波次数据集由本模块产出（``/waves``），分析管道侧只读取它来领取和执行。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query, Request

from app.schemas import ActionResult, EntryPayload, PageResult
from app.services.scheduler import SchedulerService
from app.services.wave_planner import parse_versions

router = APIRouter(prefix="/api/scheduler", tags=["任务调度"])

service = SchedulerService()

LIST_FIELDS = ["任务编号", "任务名称", "调度类型", "执行周期", "上次执行", "下次执行", "依赖关系", "任务状态"]
STATUSES = ["待执行", "执行中", "已完成", "执行失败"]


@router.get("/export")
def export_entries() -> dict[str, Any]:
    """导出任务调度清单：返回当前过滤条件下的全量数据。"""
    items, total = service.list_entries(page=1, size=10000)
    return {"module": "scheduler", "total": total, "items": items}


@router.get("/batches", response_model=list[dict])
def list_batches() -> list[dict]:
    """调度批次清单：波次排程以数据批次为单位。"""
    return service.list_batches()


@router.post("/batches", response_model=ActionResult)
def create_batch(payload: EntryPayload) -> ActionResult:
    """登记一个待调度的数据批次及其外部输入版本。"""
    values = payload.values
    version_map = parse_versions(values.get("外部输入版本"))
    existing = service.get_batch(str(values.get("批次编号") or "").strip())
    if existing is not None:
        return ActionResult(ok=False, message=f"数据批次 {existing.get('批次编号')} 已存在")
    from app.services.execution import ExecutionService

    batch, message = ExecutionService().create_batch({**values, "外部输入版本": version_map})
    if batch is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=batch)


@router.post("/waves", response_model=ActionResult)
def create_waves(payload: EntryPayload) -> ActionResult:
    """为一组管道节点和数据批次计算调度执行波次数据集。"""
    values = payload.values
    batch_code = str(values.get("批次编号") or "").strip()
    if not batch_code:
        return ActionResult(ok=False, message="缺少必填字段：批次编号")
    node_codes = values.get("节点编号") or values.get("nodes") or []
    if isinstance(node_codes, str):
        node_codes = [code for code in node_codes.replace("；", ",").replace(";", ",").split(",") if code.strip()]
    if not isinstance(node_codes, list):
        return ActionResult(ok=False, message="节点编号需要是数组或逗号分隔的字符串")
    plan_version = str(values.get("调度版本") or "V1").strip() or "V1"
    summary, message = service.plan_execution_waves(
        batch_code=batch_code,
        node_codes=[str(code).strip() for code in node_codes] or None,
        include_upstream=bool(values.get("包含上游", True)),
        plan_version=plan_version,
        node_target_versions=parse_versions(values.get("节点目标版本")),
        expected_versions=parse_versions(values.get("预期输入版本")),
        remark=str(values.get("备注") or ""),
    )
    if summary is None:
        return ActionResult(ok=False, message=message, entry=_wave_payload(batch_code))
    return ActionResult(ok=True, message=message, entry=summary)


@router.get("/waves", response_model=dict)
def list_waves(
    batch: str = Query(description="批次编号"),
    version: str | None = Query(default=None, description="调度版本，默认取最新"),
) -> dict:
    """读取某个批次的调度执行波次数据集。"""
    payload = _wave_payload(batch, version)
    if not payload:
        raise HTTPException(status_code=404, detail=f"批次 {batch} 还没有调度执行波次")
    return payload


@router.get("/waves/{wave_id}", response_model=dict)
def get_wave(wave_id: int) -> dict:
    """读取单条波次行。"""
    row = service.get_wave(wave_id)
    if row is None:
        raise HTTPException(status_code=404, detail=f"波次行 {wave_id} 不存在")
    return row


@router.get("", response_model=PageResult[dict])
def list_entries(
    request: Request,
    keyword: str | None = Query(default=None, description="按任务编号检索"),
    status: str | None = Query(default=None, description="待执行、执行中、已完成、执行失败"),
    page: int = 1,
    size: int = 20,
) -> PageResult[dict]:
    """按任务编号与状态过滤任务调度列表；没有数据时返回空页，不报错。"""
    if size > 200:
        raise HTTPException(status_code=400, detail="每页最多 200 条，请缩小分页范围")
    extra = {k: v for k, v in request.query_params.items() if k not in ("keyword", "status", "page", "size") and v.strip()}
    items, total = service.list_entries(keyword=keyword, status=status, page=page, size=size, extra=extra)
    return PageResult(items=items, total=total, page=page, size=size)


@router.get("/{entry_id}", response_model=dict)
def get_entry(entry_id: int) -> dict:
    """读取单条调度任务明细；不存在时给出可读的错误说明。"""
    entry = service.get_entry(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"调度任务 {entry_id} 不存在或已归档")
    return entry


@router.post("", response_model=ActionResult)
def create_entry(payload: EntryPayload) -> ActionResult:
    """登记一条调度任务，缺字段时说明原因而不是静默丢弃。"""
    entry, missing = service.create_entry(payload.values)
    if missing:
        return ActionResult(ok=False, message=f"缺少必填字段：{'、'.join(missing)}")
    return ActionResult(ok=True, message="调度任务已登记", entry=entry)


@router.post("/{entry_id}/actions", response_model=ActionResult)
def run_action(entry_id: int, payload: EntryPayload) -> ActionResult:
    """对单条调度任务执行立即执行、暂停任务、标记失败；不允许的动作会被拦下并说明原因。"""
    action = str(payload.values.get("action") or "").strip()
    entry, message = service.run_action(entry_id, action)
    if entry is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=entry)


def _wave_payload(batch_code: str, plan_version: str | None = None) -> dict[str, Any]:
    if not batch_code:
        return {}
    from app.services.execution import ExecutionService

    return ExecutionService().list_waves(batch_code, plan_version)
