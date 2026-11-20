<template>
  <section class="page" data-module="pipeline">
    <header class="page-head">
      <div>
        <h2>分析管道管理</h2>
        <p class="page-desc">读取调度执行波次领取节点、回执实际消费的上游产物；上游更正后按受影响支路补算或作废重放。</p>
      </div>
      <div class="page-actions">
        <button class="btn primary" type="button" @click="openCreate">登记分析节点</button>
        <button class="btn" type="button" @click="exportRows">导出分析管道清单</button>
      </div>
    </header>

    <div class="tabs">
      <button
        v-for="tab in tabs"
        :key="tab.key"
        class="tab"
        :class="{ active: activeTab === tab.key }"
        type="button"
        @click="switchTab(tab.key)"
      >
        {{ tab.label }}
      </button>
    </div>

    <!-- ============ 节点台账 ============ -->
    <template v-if="activeTab === 'nodes'">
      <div class="stat-row">
        <article v-for="item in stats" :key="item.label" class="stat-card">
          <span class="stat-label">{{ item.label }}</span>
          <strong class="stat-value">{{ item.value }}</strong>
        </article>
      </div>

      <form class="filter-bar" @submit.prevent="reload">
        <label v-for="field in filterFields" :key="field" class="filter-item">
          <span>{{ field }}</span>
          <input v-model="filters[field]" :placeholder="`按${field}检索`" />
        </label>
        <button class="btn" type="submit">查询</button>
        <button class="btn ghost" type="button" @click="resetFilters">重置条件</button>
      </form>

      <table class="data-table">
        <thead>
          <tr>
            <th v-for="column in columns" :key="column">{{ column }}</th>
            <th>可执行动作</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="row in rows" :key="String(row.id)">
            <td v-for="column in columns" :key="column">{{ row[column] ?? '—' }}</td>
            <td class="row-actions">
              <button
                v-for="action in actions"
                :key="action"
                class="link"
                type="button"
                @click="runAction(action, row)"
              >
                {{ action }}
              </button>
            </td>
          </tr>
          <tr v-if="!rows.length">
            <td :colspan="columns.length + 1" class="empty-state">暂无分析管道数据，可先登记分析节点</td>
          </tr>
        </tbody>
      </table>

      <footer class="page-foot">
        <span>共 {{ total }} 条分析管道记录</span>
        <span v-if="errorMessage" class="error-text">{{ errorMessage }}</span>
      </footer>
    </template>

    <!-- ============ 波次执行 ============ -->
    <template v-else-if="activeTab === 'execute'">
      <div class="section-card">
        <h3>读取调度执行波次</h3>
        <p class="section-desc">分析管道只消费任务调度产出的波次数据集；同一节点在上游产物齐备前不可领取。</p>
        <div class="form-row">
          <label>
            <span>批次编号</span>
            <input v-model="batchCode" placeholder="BATCH-20261119-01" />
          </label>
          <label>
            <span>调度版本</span>
            <input v-model="planVersion" placeholder="留空取最新" />
          </label>
          <button class="btn primary" type="button" :disabled="busy" @click="loadWaves">读取波次</button>
          <button class="btn" type="button" :disabled="busy" @click="loadTrace">读取追溯</button>
        </div>
        <div v-if="message" class="alert-box" :class="messageOk ? 'ok' : 'bad'">{{ message }}</div>
      </div>

      <div v-if="waveView" class="section-card">
        <h3>
          波次视图
          <span class="chip">{{ waveView.数据批次 }}</span>
          <span class="chip">版本 {{ waveView.调度版本 }}</span>
        </h3>
        <p class="section-desc">
          可领取 {{ waveView.可领取.length }} · 执行中 {{ waveView.执行中.length }} ·
          已提交 {{ waveView.已提交.length }} · 已复用 {{ waveView.已复用.length }} ·
          未就绪 {{ waveView.未就绪.length }} · 阻塞 {{ waveView.阻塞.length }}
        </p>
        <div v-if="waveView.阻塞.length" class="alert-box bad">
          循环或缺失依赖：
          <span v-for="item in waveView.阻塞" :key="item.节点编号" class="chip bad">
            {{ item.节点编号 }}：{{ item.阻塞原因 }}
          </span>
        </div>

        <table class="sub-table">
          <thead>
            <tr>
              <th>波次</th><th>节点</th><th>领取状态</th><th>输出版本</th>
              <th>计划输入</th><th>执行单</th><th>操作</th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="row in waveView.items" :key="row.id">
              <td>{{ row.波次 }}</td>
              <td>{{ row.节点编号 }}<br /><span class="meta">{{ row.节点名称 }}</span></td>
              <td><span class="chip" :class="stateClass(row.领取状态)">{{ row.领取状态 }}</span></td>
              <td>{{ row.计划输出版本 }}</td>
              <td>{{ row.计划输入说明 }}</td>
              <td>{{ row.执行单编号 || '—' }}</td>
              <td>
                <div class="inline-actions">
                  <button
                    v-if="['待领取', '待补算', '待重试'].includes(row.领取状态)"
                    class="link"
                    type="button"
                    @click="prepareClaim(row)"
                  >领取</button>
                  <button
                    v-if="row.领取状态 === '执行中' && row.执行单编号"
                    class="link"
                    type="button"
                    @click="prepareReceipt(row)"
                  >回执</button>
                  <button
                    v-if="row.领取状态 === '执行中' && row.执行单编号"
                    class="link"
                    type="button"
                    @click="voidRun(row)"
                  >作废</button>
                </div>
              </td>
            </tr>
          </tbody>
        </table>
      </div>

      <div class="section-card">
        <h3>领取执行单</h3>
        <p class="section-desc">
          领取时必须写清本次消费的上游产物版本，例如 <code>PIPE-1201@v1,PIPE-1202@v2</code>；
          版本与计划输入不一致会被拦下。
        </p>
        <div class="form-row">
          <label>
            <span>节点编号</span>
            <select v-model="claimForm.节点编号">
              <option value="">选择可领取节点</option>
              <option v-for="row in claimable" :key="row.节点编号" :value="row.节点编号">
                {{ row.节点编号 }} {{ row.节点名称 }}
              </option>
            </select>
          </label>
          <label>
            <span>消费版本</span>
            <input v-model="claimForm.消费版本" placeholder="PIPE-1201@v1,PIPE-1202@v2" />
          </label>
          <label>
            <span>备注</span>
            <input v-model="claimForm.备注" placeholder="可选" />
          </label>
          <button class="btn primary" type="button" :disabled="busy" @click="claimNode">领取</button>
        </div>
        <div class="toolbar-note">点击上方表格的“领取”会自动带出计划输入版本，可按实际消费调整。</div>
      </div>

      <div class="section-card">
        <h3>执行回执</h3>
        <p class="section-desc">
          回执写明实际消费的上游产物与本次产出的版本；回执提交后自动放行下游可领取范围。
          若上游已在领取后更正，会提示先按更正记录处理。
        </p>
        <div class="form-row">
          <label>
            <span>执行单编号</span>
            <select v-model="receiptForm.执行单编号">
              <option value="">选择执行中</option>
              <option v-for="run in openRuns" :key="run.执行单编号" :value="run.执行单编号">
                {{ run.执行单编号 }} {{ run.节点名称 }}
              </option>
            </select>
          </label>
          <label>
            <span>输出版本</span>
            <input v-model="receiptForm.输出版本" placeholder="v1" />
          </label>
          <label>
            <span>实际消费版本</span>
            <input v-model="receiptForm.消费版本" placeholder="PIPE-1201@v1,PIPE-1202@v2" />
          </label>
          <label>
            <span>输出校验</span>
            <input v-model="receiptForm.输出校验" placeholder="sha256:..." />
          </label>
          <button class="btn primary" type="button" :disabled="busy" @click="submitReceipt">提交回执</button>
        </div>
      </div>

      <div class="section-card">
        <h3>执行单</h3>
        <table class="sub-table">
          <thead>
            <tr>
              <th>执行单</th><th>节点</th><th>状态</th><th>领取消费</th>
              <th>实际消费</th><th>输出</th><th>产物状态</th><th>操作</th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="run in runs" :key="run.id">
              <td>{{ run.执行单编号 }}</td>
              <td>{{ run.节点编号 }}{{ run.重试次数 ? `（重试 ${run.重试次数}）` : '' }}</td>
              <td><span class="chip" :class="runClass(run.status)">{{ run.status }}</span></td>
              <td>{{ formatVersions(run.领取时消费版本) }}</td>
              <td>{{ formatVersions(run.实际消费版本) }}</td>
              <td>{{ run.输出版本 || '—' }}</td>
              <td>{{ run.产物状态 || '—' }}</td>
              <td>
                <div class="inline-actions">
                  <button v-if="run.status === '执行中'" class="link" type="button" @click="failRun(run)">标记失败</button>
                  <button v-if="run.status === '失败'" class="link" type="button" @click="retryRun(run)">重试</button>
                  <button v-if="run.status === '执行中'" class="link" type="button" @click="prepareReceiptByRun(run)">回执</button>
                </div>
              </td>
            </tr>
            <tr v-if="!runs.length">
              <td colspan="8" class="empty-state">暂无执行单</td>
            </tr>
          </tbody>
        </table>
        <div v-if="trace && trace.执行单.length" class="toolbar-note">
          最近一次追溯：{{ trace.执行单.length }} 张执行单，受影响节点
          {{ trace.执行单.filter((run: any) => run.受影响).length }} 个。
        </div>
      </div>
    </template>

    <!-- ============ 更正与重试 ============ -->
    <template v-else>
      <div class="section-card">
        <h3>登记上游更正</h3>
        <p class="section-desc">
          同一个分区的上游节点重新产出时登记更正；系统会列出受影响的支路、仍然在途的领取，以及可复用的产物。
        </p>
        <div class="form-row">
          <label>
            <span>批次编号</span>
            <input v-model="correctionForm.批次编号" placeholder="BATCH-20261119-01" />
          </label>
          <label>
            <span>更正节点</span>
            <input v-model="correctionForm.节点编号" placeholder="PIPE-1102" />
          </label>
          <label>
            <span>原版本</span>
            <input v-model="correctionForm.原版本" placeholder="v1" />
          </label>
          <label>
            <span>新版本</span>
            <input v-model="correctionForm.新版本" placeholder="v2" />
          </label>
          <button class="btn primary" type="button" :disabled="busy" @click="registerCorrection">登记更正</button>
        </div>
        <div v-if="message" class="alert-box" :class="messageOk ? 'ok' : 'bad'">{{ message }}</div>
      </div>

      <div v-if="activeCorrection" class="section-card">
        <h3>
          更正影响分析
          <span class="chip warn">{{ activeCorrection.更正编号 }}</span>
          <span class="chip">{{ activeCorrection.状态 }}</span>
        </h3>
        <p class="section-desc">
          {{ activeCorrection.节点编号 }} 在同一分区从 {{ activeCorrection.原版本 }} 更正为
          {{ activeCorrection.新版本 }}；受影响节点参与重算，其余支路可直接复用。
        </p>
        <div class="alert-box warn">
          受影响节点：{{ (activeCorrection.影响分析?.受影响节点 ?? []).join('、') || '无' }}
        </div>
        <div class="alert-box" :class="(activeCorrection.影响分析?.在途执行单 ?? []).length ? 'bad' : 'ok'">
          在途领取：{{ (activeCorrection.影响分析?.在途执行单 ?? []).map((run: any) => `${run.执行单编号}（${run.节点编号}）`).join('、') || '无' }}
        </div>

        <table class="sub-table">
          <thead><tr><th>支路</th><th>当前版本</th><th>受影响</th><th>可复用</th></tr></thead>
          <tbody>
            <tr v-for="branch in activeCorrection.影响分析?.汇合支路 ?? []" :key="branch.支路">
              <td>{{ branch.支路 }}<br /><span class="meta">汇合节点 {{ branch.汇合节点 }}</span></td>
              <td>{{ branch.当前版本 ?? '—' }}</td>
              <td><span class="chip" :class="branch.受影响 ? 'bad' : 'ok'">{{ branch.受影响 ? '需重算' : '未受影响' }}</span></td>
              <td>{{ branch.可复用 ? '可复用' : '不可复用' }}</td>
            </tr>
            <tr v-if="!(activeCorrection.影响分析?.汇合支路 ?? []).length">
              <td colspan="4" class="empty-state">没有分叉汇合的支路</td>
            </tr>
          </tbody>
        </table>

        <h3 style="margin-top:14px">选择处理策略</h3>
        <p class="section-desc">
          两种策略对“当前批次对外可用范围”的影响不同：作废重放不会产出旧版本，但延迟可见；
          跑完再补算可立即拿到旧版本快照，但它在批次里只作历史版本。
        </p>
        <div class="form-row">
          <button class="btn primary" type="button" :disabled="busy || activeCorrection.状态 !== '待处理'" @click="resolveCorrection('void_and_replan')">
            作废未提交领取，以新版本重放
          </button>
          <button class="btn" type="button" :disabled="busy || activeCorrection.状态 !== '待处理'" @click="resolveCorrection('finish_and_backfill')">
            原领取跑完 v1，另排补算
          </button>
          <button class="btn ghost" type="button" :disabled="busy" @click="loadWaves">查看最新波次</button>
        </div>
      </div>

      <div class="section-card">
        <h3>更正记录</h3>
        <table class="sub-table">
          <thead>
            <tr><th>更正编号</th><th>批次</th><th>节点</th><th>版本</th><th>状态</th><th>处理方式</th><th>新调度版本</th></tr>
          </thead>
          <tbody>
            <tr v-for="item in corrections" :key="item.id">
              <td>{{ item.更正编号 }}</td>
              <td>{{ item.批次编号 }}</td>
              <td>{{ item.节点编号 }}</td>
              <td>{{ item.原版本 }} → {{ item.新版本 }}</td>
              <td><span class="chip" :class="item.状态 === '待处理' ? 'warn' : 'ok'">{{ item.状态 }}</span></td>
              <td>{{ item.处理方式 || '—' }}</td>
              <td>{{ item.新调度版本 || '—' }}</td>
            </tr>
            <tr v-if="!corrections.length">
              <td colspan="7" class="empty-state">暂无更正记录</td>
            </tr>
          </tbody>
        </table>
      </div>

      <div v-if="trace" class="section-card">
        <h3>批次可发布范围</h3>
        <p class="section-desc">{{ trace.发布范围?.说明 }}</p>
        <div class="alert-box ok">
          可发布：
          <span v-for="item in trace.发布范围?.可发布产物 ?? []" :key="item.节点编号" class="chip ok">
            {{ item.节点编号 }}@{{ item.产物版本 }}
          </span>
          <span v-if="!(trace.发布范围?.可发布产物 ?? []).length">无</span>
        </div>
        <div class="alert-box bad" v-if="(trace.发布范围?.不可发布产物 ?? []).length">
          暂不可发布：
          <span v-for="item in trace.发布范围?.不可发布产物 ?? []" :key="item.节点编号" class="chip bad">
            {{ item.节点编号 }}：{{ item.原因 }}
          </span>
        </div>
        <table class="sub-table">
          <thead><tr><th>汇合节点</th><th>当前产物版本</th><th>是否可发布</th><th>原因</th></tr></thead>
          <tbody>
            <tr v-for="join in trace.发布范围?.汇合节点 ?? []" :key="join.节点编号">
              <td>{{ join.节点编号 }} {{ join.节点名称 }}</td>
              <td>{{ join.当前产物版本 ?? '—' }}</td>
              <td><span class="chip" :class="join.可发布 ? 'ok' : 'bad'">{{ join.可发布 ? '可发布' : '不可发布' }}</span></td>
              <td>{{ join.原因 }}</td>
            </tr>
          </tbody>
        </table>
      </div>

      <div v-if="trace" class="section-card">
        <h3>支路追溯</h3>
        <table class="sub-table">
          <thead><tr><th>汇合节点</th><th>支路</th><th>末级上游版本</th><th>是否需重算</th><th>可复用</th></tr></thead>
          <tbody>
            <tr v-for="branch in trace.汇合支路 ?? []" :key="`${branch.汇合节点}-${branch.末级上游}`">
              <td>{{ branch.汇合节点 }}</td>
              <td>{{ branch.支路 }}</td>
              <td>{{ branch.当前版本 ?? '—' }}</td>
              <td><span class="chip" :class="branch.需重新计算 ? 'bad' : 'ok'">{{ branch.需重新计算 ? '需重算' : '可复用' }}</span></td>
              <td>{{ branch.可复用 ? '是' : '否' }}</td>
            </tr>
          </tbody>
        </table>
      </div>
    </template>

    <Teleport to="body">
      <div v-if="showCreateModal" class="modal-overlay" @click.self="showCreateModal = false">
        <div class="modal-card">
          <header class="modal-head">
            <h3>登记分析节点</h3>
            <button class="btn ghost" type="button" @click="showCreateModal = false">&times;</button>
          </header>
          <form class="modal-body" @submit.prevent="submitCreate">
            <label v-for="field in columns.slice(0, 3)" :key="field" class="modal-field">
              <span>{{ field }}</span>
              <input v-model="createFields[field]" :placeholder="`请输入${field}`" required />
            </label>
            <footer class="modal-foot">
              <button class="btn" type="button" @click="showCreateModal = false">取消</button>
              <button class="btn primary" type="submit">提交登记</button>
            </footer>
          </form>
        </div>
      </div>
    </Teleport>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'

import { request } from '@/api/client'

type Row = Record<string, string | number | null>
type AnyRow = Record<string, any>

const ENDPOINT = '/api/pipeline'
const columns = ["节点编号", "输入数据集", "分析算子", "输出结果", "执行顺序", "依赖节点", "失败策略", "节点状态"]
const actions = ["提交配置", "启动节点", "标记失败"]
const statuses = ["草稿", "已配置", "运行中", "已完成", "失败"]
const stats = [{"label": "运行中节点", "value": 0}, {"label": "已完成节点", "value": 0}, {"label": "失败节点", "value": 0}]
const tabs = [
  { key: 'nodes', label: '节点台账' },
  { key: 'execute', label: '波次执行' },
  { key: 'recover', label: '更正与重试' },
]

const rows = ref<Row[]>([])
const total = ref(0)
const errorMessage = ref('')
const filters = ref<Record<string, string>>({})
const filterFields = columns.slice(0, 3)
const showCreateModal = ref(false)
const createFields = ref<Record<string, string>>({})
const activeTab = ref('nodes')
const busy = ref(false)
const message = ref('')
const messageOk = ref(true)

const batchCode = ref('BATCH-20261119-01')
const planVersion = ref('')
const waveView = ref<AnyRow | null>(null)
const runs = ref<AnyRow[]>([])
const trace = ref<AnyRow | null>(null)
const corrections = ref<AnyRow[]>([])
const activeCorrection = ref<AnyRow | null>(null)

const claimForm = ref({ 节点编号: '', 消费版本: '', 备注: '' })
const receiptForm = ref({ 执行单编号: '', 输出版本: '', 消费版本: '', 输出校验: '' })
const correctionForm = ref({ 批次编号: 'BATCH-20261119-01', 节点编号: 'PIPE-1102', 原版本: 'v1', 新版本: 'v2' })

const claimable = computed<AnyRow[]>(() => waveView.value?.可领取 ?? [])
const openRuns = computed<AnyRow[]>(() => runs.value.filter((run) => run.status === '执行中'))

function formatVersions(versions: Record<string, string> | undefined | null): string {
  if (!versions || !Object.keys(versions).length) return '—'
  return Object.entries(versions).map(([key, value]) => `${key}@${value}`).join('，')
}

function toVersionString(versions: Record<string, string> | undefined | null): string {
  if (!versions) return ''
  return Object.entries(versions).map(([key, value]) => `${key}@${value}`).join(',')
}

function stateClass(state: string | undefined) {
  if (state === '已提交' || state === '已复用') return 'ok'
  if (state === '执行中' || state === '待领取' || state === '待补算' || state === '待重试') return 'run'
  if (state === '阻塞' || state === '已作废') return 'bad'
  return 'warn'
}

function runClass(state: string | undefined) {
  if (state === '已提交') return 'ok'
  if (state === '执行中') return 'run'
  if (state === '失败' || state === '已作废') return 'bad'
  return 'warn'
}

function updateStats() {
  const data = rows.value
  stats.length > 0 && (stats[0].value = data.filter((r: Row) => r.status === statuses[1] || r.pending).length)
  stats.length > 1 && (stats[1].value = data.filter((r: Row) => r.status && !r.pending && !r.abnormal).length)
  stats.length > 2 && (stats[2].value = data.filter((r: Row) => r.abnormal).length)
}

function resetFilters() {
  filters.value = {}
  void reload()
}

function exportRows() {
  window.open(`${ENDPOINT}/export`, '_blank')
}

function openCreate() {
  errorMessage.value = ''
  createFields.value = {}
  showCreateModal.value = true
}

function switchTab(key: string) {
  activeTab.value = key
  if (key === 'execute') void loadAllExecution()
  if (key === 'recover') void loadRecover()
}

async function submitCreate() {
  errorMessage.value = ''
  try {
    const response = await request(ENDPOINT, {
      method: 'POST',
      body: JSON.stringify({ values: createFields.value }),
    })
    if (!response.ok) {
      const data = await response.json()
      throw new Error(data.message || '登记失败')
    }
    showCreateModal.value = false
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '登记失败'
  }
}

async function runAction(action: string, row: Row) {
  errorMessage.value = ''
  try {
    const response = await request(`${ENDPOINT}/${row.id}/actions`, {
      method: 'POST',
      body: JSON.stringify({ values: { action } }),
    })
    if (!response.ok) {
      throw new Error('分析管道动作未生效，请稍后重试')
    }
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '分析管道操作失败'
  }
}

async function reload() {
  errorMessage.value = ''
  const query = new URLSearchParams(filters.value as Record<string, string>).toString()
  try {
    const response = await request(`${ENDPOINT}?${query}`)
    if (!response.ok) {
      throw new Error('分析节点列表读取失败')
    }
    const payload = await response.json()
    rows.value = payload.items ?? []
    total.value = payload.total ?? rows.value.length
    updateStats()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '分析节点列表读取失败'
  }
}

function note(text: string, ok = true) {
  message.value = text
  messageOk.value = ok
}

async function callAction(path: string, values: Record<string, unknown>) {
  const response = await request(path, {
    method: 'POST',
    body: JSON.stringify({ values }),
  })
  return (await response.json()) as AnyRow
}

async function loadWaves() {
  if (!batchCode.value.trim()) {
    note('请先填写批次编号', false)
    return
  }
  busy.value = true
  try {
    const query = new URLSearchParams({ batch: batchCode.value.trim() })
    if (planVersion.value.trim()) query.set('version', planVersion.value.trim())
    const response = await request(`${ENDPOINT}/waves?${query.toString()}`)
    if (!response.ok) {
      waveView.value = null
      note('该批次还没有调度执行波次，请先在任务调度生成波次', false)
      return
    }
    waveView.value = await response.json()
    planVersion.value = String(waveView.value?.调度版本 ?? '')
    await loadRuns()
  } catch (error) {
    note(error instanceof Error ? error.message : '波次读取失败', false)
  } finally {
    busy.value = false
  }
}

async function loadRuns() {
  const query = new URLSearchParams({ batch: batchCode.value.trim() })
  if (planVersion.value.trim()) query.set('version', planVersion.value.trim())
  const response = await request(`${ENDPOINT}/runs?${query.toString()}`)
  if (response.ok) runs.value = await response.json()
}

async function loadTrace() {
  try {
    const response = await request(`${ENDPOINT}/trace?batch=${encodeURIComponent(batchCode.value.trim())}`)
    if (response.ok) trace.value = await response.json()
  } catch {
    trace.value = null
  }
}

async function loadCorrections() {
  const response = await request(`${ENDPOINT}/corrections?batch=${encodeURIComponent(batchCode.value.trim())}`)
  if (response.ok) {
    corrections.value = await response.json()
    activeCorrection.value = corrections.value.find((item) => item.状态 === '待处理') ?? corrections.value[0] ?? null
  }
}

async function loadRecover() {
  await Promise.all([loadTrace(), loadCorrections()])
  if (trace.value?.波次?.items) {
    waveView.value = trace.value.波次
    planVersion.value = String(trace.value.调度版本 ?? planVersion.value)
  }
}

async function loadAllExecution() {
  await loadWaves()
  await loadTrace()
}

function prepareClaim(row: AnyRow) {
  claimForm.value.节点编号 = String(row.节点编号)
  claimForm.value.消费版本 = toVersionString(row.计划输入版本)
  claimForm.value.备注 = ''
  note(`已带出 ${row.节点编号} 的计划输入版本，请核对后领取`)
}

async function claimNode() {
  if (!claimForm.value.节点编号) {
    note('请选择要领取的节点', false)
    return
  }
  busy.value = true
  try {
    const payload = await callAction(`${ENDPOINT}/claims`, {
      批次编号: batchCode.value.trim(),
      节点编号: claimForm.value.节点编号,
      调度版本: planVersion.value.trim(),
      消费版本: claimForm.value.消费版本,
      备注: claimForm.value.备注,
    })
    note(payload.message, Boolean(payload.ok))
    if (payload.ok) {
      claimForm.value = { 节点编号: '', 消费版本: '', 备注: '' }
      await loadWaves()
      await loadTrace()
    }
  } catch (error) {
    note(error instanceof Error ? error.message : '领取失败', false)
  } finally {
    busy.value = false
  }
}

function prepareReceipt(row: AnyRow) {
  const run = runs.value.find((item) => item.执行单编号 === row.执行单编号)
  if (run) prepareReceiptByRun(run)
}

function prepareReceiptByRun(run: AnyRow) {
  const consumed = run.实际消费版本 && Object.keys(run.实际消费版本).length
    ? run.实际消费版本
    : run.领取时消费版本
  receiptForm.value = {
    执行单编号: String(run.执行单编号),
    输出版本: String(waveView.value?.items?.find((item: AnyRow) => item.节点编号 === run.节点编号)?.计划输出版本 ?? ''),
    消费版本: toVersionString(consumed),
    输出校验: '',
  }
  note('请确认本次实际消费的上游产物版本后提交回执')
}

async function submitReceipt() {
  const run = runs.value.find((item) => item.执行单编号 === receiptForm.value.执行单编号)
  if (!run) {
    note('请选择执行中的执行单', false)
    return
  }
  busy.value = true
  try {
    const payload = await callAction(`${ENDPOINT}/runs/${run.id}/receipt`, {
      输出版本: receiptForm.value.输出版本,
      实际消费版本: receiptForm.value.消费版本,
      输出校验: receiptForm.value.输出校验,
    })
    note(payload.message, Boolean(payload.ok))
    if (payload.ok) {
      receiptForm.value = { 执行单编号: '', 输出版本: '', 消费版本: '', 输出校验: '' }
      await loadWaves()
      await loadTrace()
    }
  } catch (error) {
    note(error instanceof Error ? error.message : '回执失败', false)
  } finally {
    busy.value = false
  }
}

async function failRun(run: AnyRow) {
  busy.value = true
  try {
    const payload = await callAction(`${ENDPOINT}/runs/${run.id}/fail`, { 原因: '执行异常，人工标记' })
    note(payload.message, Boolean(payload.ok))
    await loadWaves()
  } catch (error) {
    note(error instanceof Error ? error.message : '标记失败失败', false)
  } finally {
    busy.value = false
  }
}

async function retryRun(run: AnyRow) {
  busy.value = true
  try {
    const payload = await callAction(`${ENDPOINT}/runs/${run.id}/retry`, { 备注: '人工重试' })
    note(payload.message, Boolean(payload.ok))
    await loadWaves()
    await loadTrace()
  } catch (error) {
    note(error instanceof Error ? error.message : '重试失败', false)
  } finally {
    busy.value = false
  }
}

async function voidRun(row: AnyRow) {
  const run = runs.value.find((item) => item.执行单编号 === row.执行单编号)
  if (!run) return
  busy.value = true
  try {
    const payload = await callAction(`${ENDPOINT}/runs/${run.id}/void`, { 原因: '人工作废尚未提交的领取' })
    note(payload.message, Boolean(payload.ok))
    await loadWaves()
  } catch (error) {
    note(error instanceof Error ? error.message : '作废失败', false)
  } finally {
    busy.value = false
  }
}

async function registerCorrection() {
  busy.value = true
  try {
    const payload = await callAction(`${ENDPOINT}/corrections`, {
      批次编号: correctionForm.value.批次编号.trim(),
      节点编号: correctionForm.value.节点编号.trim(),
      原版本: correctionForm.value.原版本.trim(),
      新版本: correctionForm.value.新版本.trim(),
    })
    note(payload.message, Boolean(payload.ok))
    if (payload.ok) {
      activeCorrection.value = payload.entry
      batchCode.value = correctionForm.value.批次编号.trim()
      await loadRecover()
    }
  } catch (error) {
    note(error instanceof Error ? error.message : '更正登记失败', false)
  } finally {
    busy.value = false
  }
}

async function resolveCorrection(strategy: string) {
  if (!activeCorrection.value) return
  busy.value = true
  try {
    const payload = await callAction(
      `${ENDPOINT}/corrections/${activeCorrection.value.id}/resolve`,
      { 处理方式: strategy },
    )
    note(payload.message, Boolean(payload.ok))
    if (payload.ok) {
      planVersion.value = String(payload.entry?.新调度版本 ?? '')
      await loadRecover()
      await loadWaves()
    }
  } catch (error) {
    note(error instanceof Error ? error.message : '更正解析失败', false)
  } finally {
    busy.value = false
  }
}

onMounted(() => {
  void reload()
})
</script>
