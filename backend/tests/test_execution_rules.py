"""调度波次与管道执行规则的自测：python3 -m unittest discover -s tests

覆盖：波次拆分、循环/缺失依赖、领取的输入版本校验、回执与下游放行、
失败重试、更正影响分析，以及两种更正处理策略下的发布范围。
"""
from __future__ import annotations

import unittest

from app.services.execution import ExecutionService
from app.services.scheduler import SchedulerService
from app.services.wave_planner import parse_dependencies, plan_waves
from app.store import store

BATCH = "BATCH-20261119-01"
JOIN = "PIPE-1301"


def wave_nodes(waves):
    return [list(wave["node_codes"]) for wave in waves]


class WavePlannerTest(unittest.TestCase):
    def test_parse_dependencies(self):
        self.assertEqual(parse_dependencies("PIPE-1, PIPE-2；PIPE-3"), ["PIPE-1", "PIPE-2", "PIPE-3"])
        self.assertEqual(parse_dependencies("无"), [])
        self.assertEqual(parse_dependencies(["PIPE-1", "PIPE-1"]), ["PIPE-1"])

    def test_fork_join_waves(self):
        nodes = [
            {"节点编号": "A", "依赖节点": "", "执行顺序": "1"},
            {"节点编号": "B", "依赖节点": "A", "执行顺序": "2"},
            {"节点编号": "C", "依赖节点": "A", "执行顺序": "2"},
            {"节点编号": "D", "依赖节点": "B,C", "执行顺序": "3"},
        ]
        plan = plan_waves(nodes)
        self.assertEqual(wave_nodes(plan["waves"]), [["A"], ["B", "C"], ["D"]])
        self.assertEqual(plan["cycles"], [])
        self.assertEqual(plan["blocked"], [])

    def test_cycle_and_missing_block_downstream(self):
        nodes = [
            {"节点编号": "A", "依赖节点": "B", "执行顺序": "1"},
            {"节点编号": "B", "依赖节点": "A", "执行顺序": "2"},
            {"节点编号": "C", "依赖节点": "A", "执行顺序": "3"},
            {"节点编号": "D", "依赖节点": "X", "执行顺序": "4"},
            {"节点编号": "E", "依赖节点": "D", "执行顺序": "5"},
        ]
        plan = plan_waves(nodes)
        self.assertEqual(plan["waves"], [])
        self.assertEqual(plan["cycles"], [["A", "B"]])
        self.assertEqual(plan["missing"][0]["节点编号"], "D")
        self.assertEqual(set(plan["blocked_codes"]), {"A", "B", "C", "D", "E"})
        reasons = {item["节点编号"]: item["阻塞原因"] for item in plan["blocked"]}
        self.assertIn("循环依赖", reasons["A"])
        self.assertIn("不存在", reasons["D"])
        self.assertIn("D", reasons["E"])

    def test_shared_node_belongs_to_single_wave(self):
        nodes = [
            {"节点编号": "A", "依赖节点": "", "执行顺序": "1"},
            {"节点编号": "B", "依赖节点": "A", "执行顺序": "2"},
            {"节点编号": "C", "依赖节点": "A", "执行顺序": "5"},
            {"节点编号": "D", "依赖节点": "C", "执行顺序": "6"},
        ]
        plan = plan_waves(nodes)
        self.assertEqual(wave_nodes(plan["waves"]), [["A"], ["B", "C"], ["D"]])


class ExecutionRulesTest(unittest.TestCase):
    def setUp(self):
        store._tables = {name: [dict(row) for row in rows] for name, rows in _snapshot().items()}

    def test_join_cannot_be_claimed_twice(self):
        execution = ExecutionService()
        run, message = execution.claim(BATCH, node_code=JOIN, consumed_versions={})
        self.assertIsNone(run)
        self.assertIn("执行中", message)

    def test_claim_requires_explicit_versions(self):
        execution = ExecutionService()
        run, message = execution.claim(BATCH, node_code="PIPE-1302")
        self.assertIsNone(run)
        # 汇合节点尚未提交，下游既没有可领取状态，也没有输入版本
        self.assertTrue("不在可领取范围" in message or "上游产物尚未齐备" in message)

    def test_commit_releases_downstream_and_keeps_history(self):
        execution = ExecutionService()
        join_run = execution.active_run(BATCH, JOIN)
        run, message, released = execution.receipt(
            join_run["id"],
            output_version="v1",
            consumed_versions={"PIPE-1201": "v1", "PIPE-1202": "v1", "PIPE-1203": "v1"},
        )
        self.assertIsNotNone(run, message)
        self.assertEqual([row["节点编号"] for row in released], ["PIPE-1302"])
        self.assertEqual(execution.wave_row(BATCH, "V1", "PIPE-1302")["领取状态"], "待领取")
        publish = execution.publish_scope(BATCH)
        self.assertIn("PIPE-1301", [item["节点编号"] for item in publish["可发布产物"]])

    def test_wrong_receipt_version_rejected(self):
        execution = ExecutionService()
        join_run = execution.active_run(BATCH, JOIN)
        run, message, _ = execution.receipt(
            join_run["id"],
            output_version="v1",
            consumed_versions={"PIPE-1201": "v2", "PIPE-1202": "v1", "PIPE-1203": "v1"},
        )
        self.assertIsNone(run)
        self.assertIn("不一致", message)

    def test_retry_reuses_claimed_versions(self):
        execution = ExecutionService()
        join_run = execution.active_run(BATCH, JOIN)
        execution.fail(join_run["id"], "算子异常")
        retried, message = execution.retry(join_run["id"])
        self.assertIsNotNone(retried, message)
        self.assertEqual(retried["领取时消费版本"], {"PIPE-1201": "v1", "PIPE-1202": "v1", "PIPE-1203": "v1"})
        self.assertEqual(retried["重试次数"], 1)

    def test_correction_flags_join_and_keeps_channel_branch_reusable(self):
        execution = ExecutionService()
        correction, message = execution.register_correction(BATCH, node_code="PIPE-1102", old_version="v1", new_version="v2")
        self.assertIsNotNone(correction, message)
        analysis = correction["影响分析"]
        self.assertEqual(analysis["受影响节点"], ["PIPE-1202", JOIN, "PIPE-1302"])
        reusable = {item["节点编号"] for item in analysis["可复用产物"]}
        self.assertIn("PIPE-1201", reusable)
        self.assertNotIn("PIPE-1202", reusable)
        branches = {item["末级上游"]: item for item in analysis["汇合支路"]}
        self.assertTrue(branches["PIPE-1201"]["可复用"])
        self.assertTrue(branches["PIPE-1202"]["受影响"])

    def test_void_and_replan_then_backfill(self):
        execution = ExecutionService()
        correction, _ = execution.register_correction(BATCH, node_code="PIPE-1102", old_version="v1", new_version="v2")
        result, message = execution.resolve_correction(correction["id"], "void_and_replan")
        self.assertIsNotNone(result, message)
        version = result["新调度版本"]
        self.assertEqual(version, "V2")
        self.assertIsNone(execution.active_run(BATCH, JOIN))
        waves = {(row["节点编号"]): row for row in execution.list_waves(BATCH, version)["items"]}
        self.assertEqual(waves["PIPE-1101"]["领取状态"], "已复用")
        self.assertEqual(waves["PIPE-1202"]["领取状态"], "待领取")
        self.assertEqual(waves[JOIN]["领取状态"], "未就绪")

        run, message = execution.claim(BATCH, node_code="PIPE-1202", plan_version=version, consumed_versions={"PIPE-1102": "v2"})
        self.assertIsNotNone(run, message)
        _, message, released = execution.receipt(run["id"], output_version="v2", consumed_versions={"PIPE-1102": "v2"})
        self.assertEqual([row["节点编号"] for row in released], [JOIN])

        join_run, message = execution.claim(
            BATCH, node_code=JOIN, plan_version=version,
            consumed_versions={"PIPE-1201": "v1", "PIPE-1202": "v2", "PIPE-1203": "v1"},
        )
        self.assertIsNotNone(join_run, message)
        join_run, message, _ = execution.receipt(
            join_run["id"], output_version="v2",
            consumed_versions={"PIPE-1201": "v1", "PIPE-1202": "v2", "PIPE-1203": "v1"},
        )
        self.assertIsNotNone(join_run, message)
        publish = execution.publish_scope(BATCH)
        targets = {item["节点编号"]: item["产物版本"] for item in publish["可发布产物"]}
        self.assertEqual(targets[JOIN], "v2")
        self.assertEqual(targets["PIPE-1202"], "v2")
        blocked = {item["节点编号"] for item in publish["不可发布产物"]}
        self.assertEqual(blocked, {"PIPE-1302"})

    def test_finish_and_backfill_marks_old_join_historical(self):
        execution = ExecutionService()
        correction, _ = execution.register_correction(BATCH, node_code="PIPE-1102", old_version="v1", new_version="v2")
        result, message = execution.resolve_correction(correction["id"], "finish_and_backfill")
        self.assertIsNotNone(result, message)
        version = result["新调度版本"]
        old_join = execution.active_run(BATCH, JOIN)
        self.assertTrue(old_join["允许原版本完成"])
        self.assertEqual(execution.wave_row(BATCH, version, JOIN)["领取状态"], "待补算")

        # 旧版本按原领取版本跑完，但只作历史版本
        old_join, message, _ = execution.receipt(
            old_join["id"], output_version="v1",
            consumed_versions={"PIPE-1201": "v1", "PIPE-1202": "v1", "PIPE-1203": "v1"},
        )
        self.assertIsNotNone(old_join, message)
        self.assertEqual(old_join["产物状态"], "历史版本")
        publish = execution.publish_scope(BATCH)
        self.assertNotIn(JOIN, [item["节点编号"] for item in publish["可发布产物"]])

    def test_conflicting_artifact_version_rejected(self):
        execution = ExecutionService()
        correction, _ = execution.register_correction(BATCH, node_code="PIPE-1102", old_version="v1", new_version="v2")
        execution.resolve_correction(correction["id"], "finish_and_backfill")
        join_run = execution.active_run(BATCH, JOIN)
        execution.receipt(
            join_run["id"], output_version="v1",
            consumed_versions={"PIPE-1201": "v1", "PIPE-1202": "v1", "PIPE-1203": "v1"},
        )
        # 同一输出别名 v1 已消费过另一组上游版本，不能再次提交
        replayed, message = execution.claim(
            BATCH, node_code=JOIN, plan_version="V1",
            consumed_versions={"PIPE-1201": "v1", "PIPE-1202": "v2", "PIPE-1203": "v1"},
        )
        self.assertIsNone(replayed)
        self.assertIn("不在可领取范围", message)


def _snapshot():
    """重置到演示初始状态：重新导入 seed 数据与场景。"""
    from app import seed, seed_scenario

    store._tables = {name: [dict(row) for row in rows] for name, rows in seed.SEED_ROWS.items()}
    store._tables.setdefault("pipeline_batch", [])
    store._tables.setdefault("pipeline_run", [])
    store._tables.setdefault("scheduler_wave", [])
    store._tables.setdefault("pipeline_correction", [])
    seed_scenario.install()
    return store._tables


if __name__ == "__main__":
    unittest.main()
