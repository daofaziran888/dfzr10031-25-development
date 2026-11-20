"""分析管道接口：读取调度执行波次、领取执行单、回执、更正与重试恢复。

分析管道只消费任务调度产出的波次数据集：先按波次领取（领取时明确本次上游消费
版本），执行完成后回执实际消费的上游产物；上游更正后按更正记录决定受影响支路。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query, Request

from app.schemas import ActionResult, EntryPayload, PageResult
from app.services.execution import ExecutionService
from app.services.pipeline import PipelineService
from app.services.wave_planner import parse_versions

router = APIRouter(prefix="/api/pipeline", tags=["分析管道"])

service = PipelineService()
execution = ExecutionService()

LIST_FIELDS = ["节点编号", "输入数据集", "分析算子", "输出结果", "执行顺序", "依赖节点", "失败策略", "节点状态"]
STATUSES = ["草稿", "已配置", "运行中", "已完成", "失败"]


@router.get("/export")
def export_entries() -> dict[str, Any]:
    """导出分析管道清单：返回当前过滤条件下的全量数据。"""
    items, total = service.list_entries(page=1, size=10000)
    return {"module": "pipeline", "total": total, "items": items}


@router.get("/waves", response_model=dict)
def read_waves(
    batch: str = Query(description="批次编号"),
    version: str | None = Query(default=None, description="调度版本，默认取最新"),
) -> dict:
    """读取任务调度产出的执行波次数据集，作为本次领取与执行的依据。"""
    payload = execution.list_waves(batch, version)
    if not payload:
        raise HTTPException(status_code=404, detail=f"批次 {batch} 还没有调度执行波次")
    return payload


@router.post("/claims", response_model=ActionResult)
def claim_node(payload: EntryPayload) -> ActionResult:
    """领取一个已就绪的波次节点，必须带上本次消费的上游产物版本。"""
    values = payload.values
    batch_code = str(values.get("批次编号") or "").strip()
    node_code = str(values.get("节点编号") or "").strip()
    if not batch_code or not node_code:
        return ActionResult(ok=False, message="缺少必填字段：批次编号、节点编号")
    run, message = execution.claim(
        batch_code,
        node_code=node_code,
        plan_version=str(values.get("调度版本") or "").strip() or None,
        consumed_versions=parse_versions(values.get("消费版本")),
        remark=str(values.get("备注") or ""),
    )
    if run is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=run)


@router.get("/runs", response_model=list[dict])
def list_runs(
    batch: str | None = Query(default=None),
    version: str | None = Query(default=None),
    node: str | None = Query(default=None),
) -> list[dict]:
    """执行单列表：用于核对领取、回执与重试记录。"""
    rows = execution.list_runs(batch, version)
    if node:
        rows = [row for row in rows if str(row.get("节点编号")) == node]
    return rows


@router.post("/runs/{run_id}/receipt", response_model=ActionResult)
def submit_receipt(run_id: int, payload: EntryPayload) -> ActionResult:
    """执行回执：写清实际消费的上游产物版本与本次产出的版本。"""
    values = payload.values
    run, message, released = execution.receipt(
        run_id,
        output_version=str(values.get("输出版本") or "").strip() or None,
        consumed_versions=parse_versions(values.get("实际消费版本")),
        output_checksum=str(values.get("输出校验") or ""),
        result_summary=str(values.get("结果摘要") or ""),
        remark=str(values.get("备注") or ""),
    )
    if run is None:
        return ActionResult(ok=False, message=message)
    entry = dict(run)
    entry["下游放行"] = [
        {"节点编号": row.get("节点编号"), "波次": row.get("波次"), "领取状态": row.get("领取状态")}
        for row in released
    ]
    return ActionResult(ok=True, message=message, entry=entry)


@router.post("/runs/{run_id}/fail", response_model=ActionResult)
def fail_run(run_id: int, payload: EntryPayload) -> ActionResult:
    """执行失败：释放波次行进入待重试，不影响同一批次其他支路。"""
    run, message = execution.fail(run_id, str(payload.values.get("原因") or ""))
    if run is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=run)


@router.post("/runs/{run_id}/retry", response_model=ActionResult)
def retry_run(run_id: int, payload: EntryPayload) -> ActionResult:
    """重试失败执行单：按原领取的上游版本重新领取。"""
    run, message = execution.retry(run_id, remark=str(payload.values.get("备注") or ""))
    if run is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=run)


@router.post("/runs/{run_id}/void", response_model=ActionResult)
def void_run(run_id: int, payload: EntryPayload) -> ActionResult:
    """作废尚未提交的领取：产物不落库，节点可被重新放行。"""
    run, message = execution.void(run_id, str(payload.values.get("原因") or ""))
    if run is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=run)


@router.get("/corrections", response_model=list[dict])
def list_corrections(batch: str | None = Query(default=None)) -> list[dict]:
    """上游更正记录：同一分区产出新版本时登记。"""
    return execution.list_corrections(batch)


@router.post("/corrections", response_model=ActionResult)
def register_correction(payload: EntryPayload) -> ActionResult:
    """登记上游更正，并返回受影响的支路与在途领取。"""
    values = payload.values
    batch_code = str(values.get("批次编号") or "").strip()
    if not batch_code:
        return ActionResult(ok=False, message="缺少必填字段：批次编号")
    correction, message = execution.register_correction(
        batch_code,
        node_code=str(values.get("节点编号") or ""),
        old_version=str(values.get("原版本") or ""),
        new_version=str(values.get("新版本") or ""),
        note=str(values.get("说明") or ""),
    )
    if correction is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=correction)


@router.get("/corrections/{correction_id}", response_model=dict)
def get_correction(correction_id: int) -> dict:
    for row in execution.list_corrections():
        if int(row.get("id", 0)) == correction_id:
            return row
    raise HTTPException(status_code=404, detail=f"更正记录 {correction_id} 不存在")


@router.post("/corrections/{correction_id}/resolve", response_model=ActionResult)
def resolve_correction(correction_id: int, payload: EntryPayload) -> ActionResult:
    """选择处理策略：作废重规划，或允许原版本完成并补算。"""
    strategy = str(payload.values.get("处理方式") or "").strip()
    result, message = execution.resolve_correction(
        correction_id,
        strategy,
        remark=str(payload.values.get("备注") or ""),
    )
    if result is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=result)


@router.get("/publish-scope", response_model=dict)
def publish_scope(batch: str = Query(description="批次编号")) -> dict:
    """当前批次能对外发布的产物范围，以及暂不可发布的节点与原因。"""
    payload = execution.publish_scope(batch)
    if not payload:
        raise HTTPException(status_code=404, detail=f"批次 {batch} 不存在")
    return payload


@router.get("/trace", response_model=dict)
def trace(batch: str = Query(description="批次编号")) -> dict:
    """追溯执行链路：波次、执行单、回执、更正与支路影响。"""
    payload = execution.trace(batch)
    if not payload:
        raise HTTPException(status_code=404, detail=f"批次 {batch} 不存在")
    return payload


@router.get("", response_model=PageResult[dict])
def list_entries(
    request: Request,
    keyword: str | None = Query(default=None, description="按节点编号检索"),
    status: str | None = Query(default=None, description="草稿、已配置、运行中、已完成、失败"),
    page: int = 1,
    size: int = 20,
) -> PageResult[dict]:
    """按节点编号与状态过滤分析管道列表；没有数据时返回空页，不报错。"""
    if size > 200:
        raise HTTPException(status_code=400, detail="每页最多 200 条，请缩小分页范围")
    extra = {k: v for k, v in request.query_params.items() if k not in ("keyword", "status", "page", "size") and v.strip()}
    items, total = service.list_entries(keyword=keyword, status=status, page=page, size=size, extra=extra)
    return PageResult(items=items, total=total, page=page, size=size)


@router.get("/{entry_id}", response_model=dict)
def get_entry(entry_id: int) -> dict:
    """读取单条分析节点明细；不存在时给出可读的错误说明。"""
    entry = service.get_entry(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"分析节点 {entry_id} 不存在或已归档")
    return entry


@router.post("", response_model=ActionResult)
def create_entry(payload: EntryPayload) -> ActionResult:
    """登记一条分析节点，缺字段时说明原因而不是静默丢弃。"""
    entry, missing = service.create_entry(payload.values)
    if missing:
        return ActionResult(ok=False, message=f"缺少必填字段：{'、'.join(missing)}")
    return ActionResult(ok=True, message="分析节点已登记", entry=entry)


@router.post("/{entry_id}/actions", response_model=ActionResult)
def run_action(entry_id: int, payload: EntryPayload) -> ActionResult:
    """对单条分析节点执行提交配置、启动节点、标记失败；不允许的动作会被拦下并说明原因。"""
    action = str(payload.values.get("action") or "").strip()
    entry, message = service.run_action(entry_id, action)
    if entry is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=entry)
