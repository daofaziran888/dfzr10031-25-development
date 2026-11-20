"""分析管道依赖图：只做纯计算，不读写数据仓库。

调度波次、循环依赖、缺失依赖与阻塞节点的判定都放在这里，
方便 ``app.services.scheduler`` 与示例数据复用同一套口径。
"""
from __future__ import annotations

from collections import defaultdict, deque
from typing import Any, Iterable

DEPENDENCY_SEPARATORS = (",", "，", ";", "；", "、", " ", "\n", "\t")
IGNORED_DEPENDENCIES = {"", "-", "—", "无", "none", "None", "null"}


def parse_dependencies(raw: Any) -> list[str]:
    """把“依赖节点”字段拆成节点编号列表，保持顺序并去重。"""
    if raw is None:
        return []
    if isinstance(raw, (list, tuple, set)):
        parts: Iterable[Any] = raw
    else:
        text = str(raw)
        for separator in DEPENDENCY_SEPARATORS[1:]:
            text = text.replace(separator, DEPENDENCY_SEPARATORS[0])
        parts = text.split(DEPENDENCY_SEPARATORS[0])
    result: list[str] = []
    for part in parts:
        code = str(part).strip()
        if code in IGNORED_DEPENDENCIES or code in result:
            continue
        result.append(code)
    return result


def parse_versions(raw: Any) -> dict[str, str]:
    """解析“节点/数据集 → 版本”的映射。

    接受 ``{"PIPE-1": "v1"}``、``"PIPE-1@v1,PIPE-2@v2"`` 或
    ``["PIPE-1@v1", "PIPE-2@v2"]`` 三种写法。
    """
    if raw is None:
        return {}
    if isinstance(raw, dict):
        return {str(key).strip(): str(value).strip() for key, value in raw.items() if str(key).strip()}
    parts: Iterable[Any] = raw if isinstance(raw, (list, tuple, set)) else str(raw).replace("；", ",").replace(";", ",").split(",")
    versions: dict[str, str] = {}
    for part in parts:
        text = str(part).strip()
        if not text:
            continue
        if "@" in text:
            code, version = text.rsplit("@", 1)
        elif "=" in text:
            code, version = text.split("=", 1)
        else:
            continue
        code = code.strip()
        if code:
            versions[code] = version.strip()
    return versions


def _sort_key(code: str, node_by_code: dict[str, dict[str, Any]]) -> tuple[int, str]:
    raw_order = str(node_by_code.get(code, {}).get("执行顺序") or "")
    order = int(raw_order) if raw_order.isdigit() else 10**9
    return order, code


def _tarjan_scc(codes: list[str], dep_map: dict[str, list[str]]) -> list[list[str]]:
    """Tarjan 强连通分量：用于识别循环依赖。"""
    index = 0
    indexes: dict[str, int] = {}
    lowlinks: dict[str, int] = {}
    stack: list[str] = []
    on_stack: set[str] = set()
    result: list[list[str]] = []

    def strongconnect(code: str) -> None:
        nonlocal index
        indexes[code] = index
        lowlinks[code] = index
        index += 1
        stack.append(code)
        on_stack.add(code)

        for dependency in dep_map.get(code, []):
            if dependency not in dep_map:
                continue
            if dependency not in indexes:
                strongconnect(dependency)
                lowlinks[code] = min(lowlinks[code], lowlinks[dependency])
            elif dependency in on_stack:
                lowlinks[code] = min(lowlinks[code], indexes[dependency])

        if lowlinks[code] == indexes[code]:
            component: list[str] = []
            while True:
                current = stack.pop()
                on_stack.discard(current)
                component.append(current)
                if current == code:
                    break
            result.append(component)

    for code in codes:
        if code not in indexes:
            strongconnect(code)
    return result


def plan_waves(
    nodes: list[dict[str, Any]],
    selected_codes: list[str] | None = None,
    include_upstream: bool = True,
) -> dict[str, Any]:
    """按依赖关系计算执行波次。

    返回结构：

    - ``waves``：``[{"wave": 1, "node_codes": [...]}, ...]``，只有依赖全部
      能排进前序波次的节点才会进入波次；
    - ``cycles``：循环依赖分组；
    - ``missing``：缺失依赖（节点定义不存在，或未纳入本次调度范围）；
    - ``blocked``：因循环、缺失依赖或上游阻塞而无法排程的节点及原因；
    - ``unknown_selected``：调用方指定了但不存在的节点编号。
    """
    node_by_code: dict[str, dict[str, Any]] = {}
    for node in nodes:
        code = str(node.get("节点编号") or "").strip()
        if code:
            node_by_code[code] = node

    if selected_codes is None:
        selected = set(node_by_code)
        unknown_selected: list[str] = []
    else:
        requested = [str(code).strip() for code in selected_codes if str(code).strip()]
        unknown_selected = [code for code in requested if code not in node_by_code]
        selected = {code for code in requested if code in node_by_code}
        if include_upstream:
            stack = list(selected)
            while stack:
                code = stack.pop()
                for dependency in parse_dependencies(node_by_code[code].get("依赖节点")):
                    if dependency in node_by_code and dependency not in selected:
                        selected.add(dependency)
                        stack.append(dependency)

    dep_map: dict[str, list[str]] = {
        code: parse_dependencies(node_by_code[code].get("依赖节点")) for code in selected
    }

    missing: list[dict[str, Any]] = []
    blocked_roots: set[str] = set()
    for code in sorted(selected, key=lambda item: _sort_key(item, node_by_code)):
        missing_dependencies: list[dict[str, str]] = []
        for dependency in dep_map[code]:
            if dependency not in node_by_code:
                missing_dependencies.append({"节点编号": dependency, "原因": "节点定义不存在"})
            elif dependency not in selected:
                missing_dependencies.append({"节点编号": dependency, "原因": "未纳入本次调度范围"})
        if missing_dependencies:
            missing.append({"节点编号": code, "缺失依赖": missing_dependencies})
            blocked_roots.add(code)

    children: dict[str, list[str]] = defaultdict(list)
    for code in selected:
        for dependency in dep_map[code]:
            if dependency in selected and dependency in node_by_code:
                children[dependency].append(code)

    components = _tarjan_scc(sorted(selected), dep_map)
    cycle_groups: list[list[str]] = []
    cycle_nodes: set[str] = set()
    for component in components:
        is_self_loop = len(component) == 1 and component[0] in dep_map[component[0]]
        if len(component) > 1 or is_self_loop:
            group = sorted(component, key=lambda item: _sort_key(item, node_by_code))
            cycle_groups.append(group)
            cycle_nodes.update(group)
    cycle_groups.sort(key=lambda group: _sort_key(group[0], node_by_code))
    blocked_roots.update(cycle_nodes)

    blocked: set[str] = set()
    queue: deque[str] = deque(sorted(blocked_roots))
    while queue:
        code = queue.popleft()
        if code in blocked:
            continue
        blocked.add(code)
        for child in children.get(code, []):
            if child not in blocked:
                queue.append(child)

    schedulable = selected - blocked
    indegree = {
        code: sum(
            1
            for dependency in dep_map[code]
            if dependency in schedulable and dependency in node_by_code
        )
        for code in schedulable
    }

    waves: list[dict[str, Any]] = []
    current = sorted(
        (code for code in schedulable if indegree[code] == 0),
        key=lambda item: _sort_key(item, node_by_code),
    )
    wave_number = 1
    while current:
        waves.append({"wave": wave_number, "node_codes": list(current)})
        next_codes: list[str] = []
        for code in current:
            for child in children.get(code, []):
                if child not in schedulable:
                    continue
                indegree[child] -= 1
                if indegree[child] == 0:
                    next_codes.append(child)
        current = sorted(set(next_codes), key=lambda item: _sort_key(item, node_by_code))
        wave_number += 1

    blocked_details: list[dict[str, Any]] = []
    for code in sorted(blocked, key=lambda item: _sort_key(item, node_by_code)):
        reasons: list[str] = []
        if code in cycle_nodes:
            group = next((group for group in cycle_groups if code in group), [code])
            reasons.append("处于循环依赖：" + "、".join(group))
        else:
            for dependency in dep_map.get(code, []):
                if dependency not in node_by_code:
                    reasons.append(f"依赖节点 {dependency} 不存在")
                elif dependency not in selected:
                    reasons.append(f"依赖节点 {dependency} 未纳入本次调度范围")
                elif dependency in blocked:
                    reasons.append(f"上游节点 {dependency} 未完成排程")
        blocked_details.append({
            "节点编号": code,
            "阻塞原因": "；".join(dict.fromkeys(reasons)) or "上游依赖未完成排程",
        })

    return {
        "waves": waves,
        "cycles": cycle_groups,
        "missing": missing,
        "blocked": blocked_details,
        "unknown_selected": unknown_selected,
        "selected": sorted(selected, key=lambda item: _sort_key(item, node_by_code)),
        "schedulable": sorted(schedulable, key=lambda item: _sort_key(item, node_by_code)),
        "blocked_codes": sorted(blocked, key=lambda item: _sort_key(item, node_by_code)),
    }
