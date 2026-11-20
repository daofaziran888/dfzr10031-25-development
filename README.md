# 数据分析实验与数据应用服务平台

面向指标口径管理、分析实验设计、OLAP查询、数据看板构建与数据API发布的数据应用平台。

这是一个前后端分离的管理平台：前端 Vue 3 + Vite + TypeScript，后端 FastAPI（Python）。
两边各自独立启动，前端 dev server 已关掉自动打开页面，启动后按终端打印的地址手工打开。

## 目录结构

```text
.
├── frontend/                 Vue 3 + Vite + TypeScript 前端
│   ├── src/views/            每个业务模块一个页面
│   ├── src/api/              统一请求封装
│   ├── src/stores/           会话与筛选状态
│   └── vite.config.ts        dev server 配置（open: false）
├── backend/                  FastAPI（Python） 后端
│   ├── app/routers/          每个业务模块一组接口
│   ├── app/services/         业务规则与状态流转
│   └── app/store.py          内存数据仓库与示例数据
├── .gitignore
└── docker-compose.yml
```

## 启动

### 后端

```bash
cd backend
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
./run.sh
```

健康检查：`curl http://127.0.0.1:8000/api/health`

### 前端

```bash
cd frontend
npm install
npm run dev
```

前端默认监听 `http://127.0.0.1:5173/`，dev server 不会自动打开浏览器，
需要自己访问。`/api` 由 vite 代理到后端 `http://127.0.0.1:8000`。

## 业务模块

| 模块 | 目录 | 业务对象 | 主要字段 |
| --- | --- | --- | --- |
| 指标定义 | `metric_def` | 指标 | 指标编号、指标名称、计算公式 |
| 维度管理 | `dimension` | 分析维度 | 维度编号、维度名称、维度类型 |
| 数据集管理 | `dataset` | 分析数据集 | 数据集编号、数据集名称、来源类型 |
| 分析实验 | `experiment` | 实验方案 | 方案编号、实验名称、分析数据集 |
| OLAP查询 | `query_builder` | 查询模板 | 模板编号、模板名称、查询语句 |
| 数据看板 | `dashboard` | 看板配置 | 看板编号、看板名称、包含图表 |
| 图表配置 | `chart` | 图表 | 图表编号、图表名称、图表类型 |
| 数据API发布 | `data_service` | 数据服务 | 服务编号、服务名称、输出数据集 |
| 智能告警 | `alert` | 告警规则 | 规则编号、告警名称、监控指标 |
| 分析用户组 | `user_group` | 用户组 | 组编号、组名称、组描述 |
| 数据快照 | `snapshot` | 快照记录 | 快照编号、数据集名称、快照时间 |
| 任务调度 | `scheduler` | 调度任务 / 调度执行波次 | 任务编号、任务名称、调度类型 |
| 缓存管理 | `cache` | 缓存配置 | 缓存编号、缓存目标、缓存策略 |
| 分析管道 | `pipeline` | 分析节点 / 执行单 | 节点编号、输入数据集、分析算子 |
| 分析模型 | `model` | 模型配置 | 模型编号、模型名称、模型类型 |
| 数据集市 | `data_mart` | 集市表 | 表编号、表名称、主题域 |
| 数据导出 | `export` | 导出任务 | 任务编号、导出对象、导出格式 |
| 数据审计 | `audit` | 审计记录 | 记录编号、操作类型、操作人 |

## 调度执行波次与分析管道

任务调度除了任务台账，还负责把「一组管道节点 + 一个数据批次」算成可执行的波次：

1. `POST /api/scheduler/batches` 登记数据批次与外部输入版本；
2. `POST /api/scheduler/waves` 生成调度执行波次数据集（`GET /api/scheduler/waves?batch=...` 读取）；
3. 循环依赖、缺失依赖和被它们阻塞的下游节点写入 `blocked`，不进入波次；
   节点只有在计划输入版本都已提交后才转为「待领取」。

分析管道读取波次后执行：

```text
POST /api/pipeline/claims                    领取，必须写清消费的上游产物版本
POST /api/pipeline/runs/{id}/receipt         回执，写实际消费版本与输出版本，并放行下游
POST /api/pipeline/runs/{id}/fail|retry|void 失败、重试、作废未提交的领取
POST /api/pipeline/corrections               登记上游更正，返回受影响支路
POST /api/pipeline/corrections/{id}/resolve  选择 void_and_replan 或 finish_and_backfill
GET  /api/pipeline/publish-scope?batch=...   当前批次可对外发布的产物范围
GET  /api/pipeline/trace?batch=...           波次、执行单、回执与支路追溯
```

更正处理策略的差别：

- `void_and_replan`：作废尚未提交的领取，以新版本重新放行受影响支路；
  未受影响支路直接复用已提交产物，不重复计算。
- `finish_and_backfill`：允许原领取按旧版本跑完，但产物只记「历史版本」，
  同时把同一批次的受影响节点排成补算波次。

启动后后端会自动装载一个演示批次 `BATCH-20261119-01`：渠道 / 地域 / 对账三条支路
汇合到 `PIPE-1301`，汇合节点已领取三条支路的 v1 但尚未提交，可直接在「分析管道 →
更正与重试」里走一遍上游更正。

### 自测

```bash
cd backend
python3 -m unittest discover -s tests -t .
```

## 约定

- 每个模块的前端页面在 `frontend/src/views/<模块>/index.vue`，后端接口在
  `backend/app/routers/<模块>.py`，业务规则在 `backend/app/services/<模块>.py`。
- 列表接口统一返回 `{ items, total, page, size }`，动作接口统一返回 `{ ok, message }`。
- 状态流转只允许在 `app/services` 里改，路由层不做业务判断。
