<template>
  <section class="page" data-module="scheduler">
    <header class="page-head">
      <div>
        <h2>任务调度管理</h2>
        <p class="page-desc">维护调度任务；按一组管道节点和数据批次计算调度执行波次，循环或缺失依赖不进入波次。</p>
      </div>
      <div class="page-actions">
        <button class="btn primary" type="button" @click="openCreate">登记调度任务</button>
        <button class="btn" type="button" @click="exportRows">导出任务调度清单</button>
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

    <!-- ============ 调度任务台账 ============ -->
    <template v-if="activeTab === 'jobs'">
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
            <td :colspan="columns.length + 1" class="empty-state">暂无任务调度数据，可先登记调度任务</td>
          </tr>
        </tbody>
      </table>

      <footer class="page-foot">
        <span>共 {{ total }} 条任务调度记录</span>
        <span v-if="errorMessage" class="error-text">{{ errorMessage }}</span>
      </footer>
    </template>

    <!-- ============ 调度执行波次 ============ -->
    <template v-else>
      <div class="section-card">
        <h3>数据批次</h3>
        <p class="section-desc">波次以“节点组 + 数据批次”为单位计算；外部输入版本决定首批节点能否领取。</p>
        <div class="form-row">
          <label>
            <span>批次编号</span>
            <input v-model="newBatch.批次编号" placeholder="BATCH-20261120-01" />
          </label>
          <label>
            <span>数据批次（分区）</span>
            <input v-model="newBatch.数据批次" placeholder="2026-11-20" />
          </label>
          <label>
            <span>外部输入版本</span>
            <input v-model="newBatch.外部输入版本" placeholder="ods.orders@v1,dim.user@v1" />
          </label>
          <button class="btn primary" type="button" :disabled="busy" @click="createBatch">登记批次</button>
        </div>
        <div class="toolbar-note">
          已有批次：
          <button
            v-for="batch in batches"
            :key="String(batch.批次编号)"
            class="link"
            type="button"
            @click="selectBatch(batch.批次编号)"
          >
            {{ batch.批次编号 }}（{{ batch.数据批次 }}）
          </button>
          <span v-if="!batches.length">暂无</span>
        </div>
      </div>

      <div class="section-card">
        <h3>计算调度执行波次</h3>
        <p class="section-desc">
          指定节点组与数据批次；相同节点不会重复进入同一波次，只有上游计划版本都已提交，下游节点才会被放行。
        </p>
        <div class="form-row">
          <label>
            <span>批次编号</span>
            <input v-model="planForm.批次编号" placeholder="选择或输入批次编号" />
          </label>
          <label>
            <span>节点编号（逗号分隔，留空=全部）</span>
            <input v-model="planForm.节点编号" placeholder="PIPE-1301,PIPE-1302" />
          </label>
          <label>
            <span>调度版本</span>
            <input v-model="planForm.调度版本" placeholder="V2" />
          </label>
          <button class="btn primary" type="button" :disabled="busy" @click="generateWaves">生成波次</button>
          <button class="btn" type="button" :disabled="busy" @click="loadWaves">刷新</button>
        </div>
        <div v-if="planMessage" class="alert-box" :class="planOk ? 'ok' : 'bad'">{{ planMessage }}</div>
      </div>

      <div v-if="waveView" class="section-card">
        <h3>
          调度版本 {{ waveView.调度版本 }}
          <span class="chip">{{ waveView.数据批次 }}</span>
        </h3>
        <p class="section-desc">
          可领取 {{ waveView.可领取.length }} · 执行中 {{ waveView.执行中.length }} ·
          已提交 {{ waveView.已提交.length }} · 已复用 {{ waveView.已复用.length }} ·
          未就绪 {{ waveView.未就绪.length }} · 阻塞 {{ waveView.阻塞.length }}
        </p>
        <div v-if="blockedRows.length" class="alert-box bad">
          存在循环或缺失依赖，相关节点没有进入波次：
          <span v-for="item in blockedRows" :key="item.节点编号" class="chip bad">
            {{ item.节点编号 }}：{{ item.阻塞原因 }}
          </span>
        </div>
        <div class="wave-grid">
          <div v-for="lane in lanes" :key="lane.wave" class="wave-lane">
            <h4>第 {{ lane.wave }} 波</h4>
            <div v-for="node in lane.nodes" :key="node.节点编号" class="wave-node">
              <strong>
                {{ node.节点编号 }} {{ node.节点名称 }}
                <span class="chip" :class="stateClass(node.领取状态)">{{ node.领取状态 }}</span>
              </strong>
              <div class="meta">
                输出 {{ node.输出产物 }}@{{ node.计划输出版本 }}<br />
                输入 {{ node.计划输入说明 }}<br />
                <template v-if="node.执行单编号">执行单 {{ node.执行单编号 }}<br /></template>
                <template v-if="node.说明">{{ node.说明 }}</template>
              </div>
            </div>
            <div v-if="!lane.nodes.length" class="meta">该波暂无节点</div>
          </div>
        </div>
      </div>

      <footer class="page-foot">
        <span>{{ waveView ? `批次 ${waveView.批次编号} · 版本 ${waveView.调度版本}` : '尚未读取波次' }}</span>
        <span v-if="errorMessage" class="error-text">{{ errorMessage }}</span>
      </footer>
    </template>

    <Teleport to="body">
      <div v-if="showCreateModal" class="modal-overlay" @click.self="showCreateModal = false">
        <div class="modal-card">
          <header class="modal-head">
            <h3>登记调度任务</h3>
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
type WaveRow = Record<string, any>

const ENDPOINT = '/api/scheduler'
const columns = ["任务编号", "任务名称", "调度类型", "执行周期", "上次执行", "下次执行", "依赖关系", "任务状态"]
const actions = ["立即执行", "暂停任务", "标记失败"]
const statuses = ["待执行", "执行中", "已完成", "执行失败"]
const stats = [{"label": "运行中任务", "value": 0}, {"label": "待执行任务", "value": 0}, {"label": "失败任务", "value": 0}]
const tabs = [
  { key: 'jobs', label: '调度任务台账' },
  { key: 'waves', label: '调度执行波次' },
]

const rows = ref<Row[]>([])
const total = ref(0)
const errorMessage = ref('')
const filters = ref<Record<string, string>>({})
const filterFields = columns.slice(0, 3)
const showCreateModal = ref(false)
const createFields = ref<Record<string, string>>({})
const activeTab = ref('jobs')
const busy = ref(false)

const batches = ref<Row[]>([])
const waveView = ref<WaveRow | null>(null)
const planMessage = ref('')
const planOk = ref(true)
const newBatch = ref({ 批次编号: '', 数据批次: '', 外部输入版本: '' })
const planForm = ref({ 批次编号: 'BATCH-20261119-01', 节点编号: '', 调度版本: '' })

const blockedRows = computed<WaveRow[]>(() => waveView.value?.阻塞 ?? [])
const lanes = computed(() => {
  const items: WaveRow[] = waveView.value?.items ?? []
  const grouped = new Map<number, WaveRow[]>()
  items.forEach((item) => {
    const wave = Number(item.波次 || 0)
    if (!grouped.has(wave)) grouped.set(wave, [])
    grouped.get(wave)!.push(item)
  })
  return [...grouped.entries()]
    .sort((a, b) => a[0] - b[0])
    .map(([wave, nodes]) => ({ wave, nodes }))
})

function stateClass(state: string | undefined) {
  if (state === '已提交' || state === '已复用') return 'ok'
  if (state === '执行中' || state === '待领取' || state === '待补算' || state === '待重试') return 'run'
  if (state === '阻塞' || state === '已作废') return 'bad'
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
  if (key === 'waves') void loadBatches()
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
      throw new Error('任务调度动作未生效，请稍后重试')
    }
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '任务调度操作失败'
  }
}

async function reload() {
  errorMessage.value = ''
  const query = new URLSearchParams(filters.value as Record<string, string>).toString()
  try {
    const response = await request(`${ENDPOINT}?${query}`)
    if (!response.ok) {
      throw new Error('调度任务列表读取失败')
    }
    const payload = await response.json()
    rows.value = payload.items ?? []
    total.value = payload.total ?? rows.value.length
    updateStats()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '任务调度列表读取失败'
  }
}

async function loadBatches() {
  try {
    const response = await request(`${ENDPOINT}/batches`)
    if (!response.ok) throw new Error('批次列表读取失败')
    batches.value = await response.json()
    if (!batches.value.length) return
    const exists = batches.value.some((batch) => batch.批次编号 === planForm.value.批次编号)
    if (!exists) planForm.value.批次编号 = String(batches.value[0].批次编号)
    await loadWaves()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '批次列表读取失败'
  }
}

async function createBatch() {
  busy.value = true
  errorMessage.value = ''
  try {
    const response = await request(`${ENDPOINT}/batches`, {
      method: 'POST',
      body: JSON.stringify({ values: newBatch.value }),
    })
    const payload = await response.json()
    if (!payload.ok) throw new Error(payload.message || '批次登记失败')
    planMessage.value = payload.message
    planOk.value = true
    planForm.value.批次编号 = newBatch.value.批次编号
    newBatch.value = { 批次编号: '', 数据批次: '', 外部输入版本: '' }
    await loadBatches()
  } catch (error) {
    planOk.value = false
    planMessage.value = error instanceof Error ? error.message : '批次登记失败'
  } finally {
    busy.value = false
  }
}

function selectBatch(code: string | number | null) {
  planForm.value.批次编号 = String(code ?? '')
  void loadWaves()
}

async function loadWaves() {
  if (!planForm.value.批次编号) {
    waveView.value = null
    return
  }
  try {
    const query = new URLSearchParams({ batch: planForm.value.批次编号 })
    if (planForm.value.调度版本) query.set('version', planForm.value.调度版本)
    const response = await request(`${ENDPOINT}/waves?${query.toString()}`)
    if (!response.ok) {
      waveView.value = null
      return
    }
    waveView.value = await response.json()
    if (waveView.value && !planForm.value.调度版本) {
      planForm.value.调度版本 = String(waveView.value.调度版本 || '')
    }
  } catch {
    waveView.value = null
  }
}

async function generateWaves() {
  busy.value = true
  planMessage.value = ''
  errorMessage.value = ''
  try {
    const values: Record<string, unknown> = {
      批次编号: planForm.value.批次编号.trim(),
      节点编号: planForm.value.节点编号
        .split(/[,，;；\s]+/)
        .map((code) => code.trim())
        .filter(Boolean),
    }
    if (planForm.value.调度版本.trim()) values.调度版本 = planForm.value.调度版本.trim()
    const response = await request(`${ENDPOINT}/waves`, {
      method: 'POST',
      body: JSON.stringify({ values }),
    })
    const payload = await response.json()
    planOk.value = Boolean(payload.ok)
    planMessage.value = payload.message
    if (payload.ok) {
      const entry = payload.entry ?? {}
      planForm.value.调度版本 = String(entry.调度版本 || planForm.value.调度版本)
      await loadWaves()
    }
  } catch (error) {
    planOk.value = false
    planMessage.value = error instanceof Error ? error.message : '波次生成失败'
  } finally {
    busy.value = false
  }
}

onMounted(async () => {
  await reload()
  await loadBatches()
})
</script>
