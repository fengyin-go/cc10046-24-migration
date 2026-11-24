<template>
  <section class="page dedup-migration-page" data-module="product_dedup_migration">
    <header class="page-head">
      <div>
        <h2>商品去重键 V2 迁移</h2>
        <p class="page-desc">
          旧拼接键保留原口径与留档计数；V2 只从真实原始行重算，并读取实际发布的商品去重数据。
        </p>
      </div>
      <div class="page-actions">
        <button class="btn primary" type="button" :disabled="loading" @click="claim">
          {{ loading ? '核对中…' : '运营领取新版' }}
        </button>
      </div>
    </header>

    <div v-if="error" class="error-banner">{{ error }}</div>

    <div class="migration-grid">
      <article v-for="item in readiness" :key="item.batch_id" class="migration-card">
        <div class="migration-card-head">
          <div>
            <h3>{{ item.party || item.batch_id }}</h3>
            <span class="muted">{{ item.batch_id }}</span>
          </div>
          <span class="status-pill" :data-ready="item.new_version_ready">
            {{ item.status }}
          </span>
        </div>

        <dl class="metric-list">
          <div><dt>V2 可用</dt><dd>{{ item.new_version_ready ? '是' : '否' }}</dd></div>
          <div><dt>真实原始行</dt><dd>{{ item.raw_rows_available ? '已具备' : '缺失' }}</dd></div>
          <div><dt>旧计数留档</dt><dd>{{ item.checks?.old_counts_archived ? '是' : '否' }}</dd></div>
        </dl>

        <p v-if="item.pending_reason" class="pending-reason">
          <strong>待发布/不可用原因：</strong>{{ item.pending_reason }}
        </p>
        <p v-if="item.missing_evidence?.length" class="missing-evidence">
          <strong>缺少证据：</strong>{{ item.missing_evidence.join('、') }}
        </p>

        <div class="card-actions">
          <button type="button" class="link" @click="selectBatch(item.batch_id)">查看批次</button>
          <button type="button" class="link" @click="queryNew(item.batch_id)">读新版计数</button>
          <button type="button" class="link" @click="queryOld(item.batch_id)">比对旧接口</button>
        </div>
      </article>
    </div>

    <section v-if="batchId" class="panel batch-panel">
      <header class="panel-head">
        <h3>批次详情：{{ detail?.name || batchId }}</h3>
        <span class="muted">{{ batchId }}</span>
      </header>

      <div class="result-columns">
        <div class="result-box">
          <h4>新版数据处理</h4>
          <pre v-if="newResult">{{ JSON.stringify(newResult.steps?.dedup || newResult, null, 2) }}</pre>
          <p v-else class="muted">尚未读取。</p>
        </div>
        <div class="result-box">
          <h4>旧接口 / 最早留档</h4>
          <pre v-if="oldResult">{{ JSON.stringify(oldResult.steps?.dedup || oldResult, null, 2) }}</pre>
          <p v-else class="muted">尚未比对。</p>
        </div>
      </div>

      <div v-if="detail" class="detail-counts">
        <span>状态：{{ detail.status }}</span>
        <span>V2 计数：{{ formatCounts(detail.new_counts) }}</span>
        <span>实际保留：{{ detail.actual_retained_products?.length || 0 }} 条</span>
        <span>计数与明细一致：{{ detail.retained_count_matches_detail ? '是' : '否' }}</span>
      </div>

      <div v-if="detail && !detail.raw_rows_available" class="backfill-box">
        <h4>补交乙/该批次真实原始行</h4>
        <p class="muted">只能提交真实行；系统不会用旧汇总行数反推商品。</p>
        <textarea v-model="rawRowsText" rows="8" placeholder='[{"商品ID":"P1","商品组":"A|B","地区":"C"}]'></textarea>
        <div class="page-actions">
          <button class="btn primary" type="button" :disabled="submitting" @click="continueWithRows">
            {{ submitting ? '续做中…' : '补交并续做、发布 V2、重开回执' }}
          </button>
        </div>
      </div>

      <div v-if="detail?.actual_retained_products?.length" class="table-wrap">
        <h4>实际保留商品</h4>
        <table class="data-table">
          <thead>
            <tr>
              <th v-for="col in productColumns" :key="col">{{ col }}</th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="(row, index) in detail.actual_retained_products" :key="String(row.商品ID || row.id || index)">
              <td v-for="col in productColumns" :key="col">{{ row[col] ?? '—' }}</td>
            </tr>
          </tbody>
        </table>
      </div>

      <div v-if="detail?.old_receipt" class="receipt-box">
        <h4>旧回执 {{ detail.old_receipt.receipt_id }}（{{ detail.old_receipt.status }}）</h4>
        <p>留档三计数：{{ formatCounts(detail.old_receipt.archived_counts || detail.old_receipt.counts) }}</p>
      </div>
    </section>

    <section class="panel lookup-panel">
      <h3>两个业务键样本查询</h3>
      <form class="lookup-form" @submit.prevent="lookupKey">
        <label>
          批次
          <input v-model="lookup.batch_id" placeholder="甲 / A / BATCH-JIA" />
        </label>
        <label>
          商品组
          <input v-model="lookup.product_group" placeholder="A|B" />
        </label>
        <label>
          地区
          <input v-model="lookup.region" placeholder="C" />
        </label>
        <button class="btn primary" type="submit">查询对应商品</button>
      </form>
      <pre v-if="lookupResult">{{ JSON.stringify(lookupResult.products || lookupResult, null, 2) }}</pre>
    </section>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'
import { request } from '@/api/client'

type AnyRecord = Record<string, any>

const loading = ref(false)
const submitting = ref(false)
const error = ref('')
const readiness = ref<AnyRecord[]>([])
const batchId = ref('')
const detail = ref<AnyRecord | null>(null)
const newResult = ref<AnyRecord | null>(null)
const oldResult = ref<AnyRecord | null>(null)
const rawRowsText = ref('')
const lookupResult = ref<AnyRecord | null>(null)
const lookup = ref({ batch_id: '甲', product_group: 'A|B', region: 'C' })

const productColumns = computed(() => {
  const rows = detail.value?.actual_retained_products || []
  if (!rows.length) return ['商品ID', '商品名称', '商品组', '地区']
  const keys = new Set<string>()
  rows.forEach((row: AnyRecord) => Object.keys(row).forEach((key) => {
    if (!key.startsWith('_')) keys.add(key)
  }))
  return Array.from(keys).slice(0, 8)
})

async function postJson<T = AnyRecord>(path: string, body: AnyRecord): Promise<T> {
  const response = await request(path, { method: 'POST', body: JSON.stringify(body) })
  const data = await response.json()
  if (!response.ok) throw new Error(data?.detail?.reason || data?.detail || `请求失败：${response.status}`)
  return data as T
}

async function getJson<T = AnyRecord>(path: string): Promise<T> {
  const response = await request(path)
  const data = await response.json()
  if (!response.ok) throw new Error(data?.detail || `请求失败：${response.status}`)
  return data as T
}

function formatCounts(counts: AnyRecord | null | undefined): string {
  if (!counts) return '不可用'
  return `去重前 ${counts.original_count} / 保留 ${counts.deduped_count} / 去除 ${counts.removed_count}`
}

async function claim(): Promise<void> {
  loading.value = true
  error.value = ''
  try {
    const result = await postJson<{ items: AnyRecord[] }>('/api/product-dedup/claim', {})
    readiness.value = result.items || []
    if (!batchId.value && readiness.value.length) {
      const jia = readiness.value.find((item) => item.party === '甲') || readiness.value[0]
      await selectBatch(jia.batch_id)
    }
  } catch (err) {
    error.value = err instanceof Error ? err.message : '领取新版失败'
  } finally {
    loading.value = false
  }
}

async function selectBatch(id: string): Promise<void> {
  batchId.value = id
  error.value = ''
  newResult.value = null
  oldResult.value = null
  try {
    detail.value = await getJson(`/api/product-dedup/batches/${encodeURIComponent(id)}`)
  } catch (err) {
    error.value = err instanceof Error ? err.message : '读取批次详情失败'
  }
}

async function queryNew(id = batchId.value): Promise<void> {
  if (!id) return
  error.value = ''
  try {
    newResult.value = await postJson('/api/chains/data-process-v2', { batch_id: id })
    await selectBatch(id)
  } catch (err) {
    error.value = err instanceof Error ? err.message : '读取新版计数失败'
  }
}

async function queryOld(id = batchId.value): Promise<void> {
  if (!id) return
  error.value = ''
  try {
    oldResult.value = await postJson('/api/chains/data-process', { batch_id: id })
  } catch (err) {
    error.value = err instanceof Error ? err.message : '读取旧接口失败'
  }
}

async function continueWithRows(): Promise<void> {
  if (!batchId.value) return
  submitting.value = true
  error.value = ''
  try {
    const rows = JSON.parse(rawRowsText.value)
    detail.value = await postJson(`/api/product-dedup/batches/${encodeURIComponent(batchId.value)}/continue`, {
      rows,
      reopen: true,
      publish: true,
    })
    await queryNew(batchId.value)
    await queryOld(batchId.value)
    await claim()
  } catch (err) {
    error.value = err instanceof Error ? err.message : '补交原始行失败，请确认 JSON 为真实行数组'
  } finally {
    submitting.value = false
  }
}

async function lookupKey(): Promise<void> {
  error.value = ''
  const params = new URLSearchParams({
    batch_id: lookup.value.batch_id,
    product_group: lookup.value.product_group,
    region: lookup.value.region,
  })
  try {
    lookupResult.value = await getJson(`/api/product-dedup/keys/lookup?${params.toString()}`)
  } catch (err) {
    error.value = err instanceof Error ? err.message : '业务键查询失败'
  }
}

onMounted(claim)
</script>

<style scoped>
.dedup-migration-page { display: flex; flex-direction: column; gap: 16px; }
.error-banner { padding: 10px 12px; border: 1px solid #f0a; color: #b42318; background: #fff1f0; border-radius: 8px; }
.migration-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(280px, 1fr)); gap: 16px; }
.migration-card, .panel { background: #fff; border: 1px solid #e5e7eb; border-radius: 12px; padding: 16px; }
.migration-card-head, .panel-head, .detail-counts, .card-actions { display: flex; align-items: center; justify-content: space-between; gap: 12px; flex-wrap: wrap; }
.migration-card-head h3, .panel h3, .panel h4 { margin: 0; }
.muted { color: #667085; font-size: 13px; }
.status-pill { padding: 3px 10px; border-radius: 999px; background: #fff7ed; color: #9a3412; font-size: 12px; }
.status-pill[data-ready="true"] { background: #ecfdf3; color: #027a48; }
.metric-list { display: grid; grid-template-columns: repeat(3, 1fr); gap: 8px; margin: 14px 0; }
.metric-list div { background: #f9fafb; border-radius: 8px; padding: 8px; }
.metric-list dt { font-size: 12px; color: #667085; }
.metric-list dd { margin: 4px 0 0; font-weight: 700; }
.pending-reason, .missing-evidence { font-size: 13px; line-height: 1.6; }
.pending-reason { color: #9a3412; }
.missing-evidence { color: #b42318; }
.result-columns { display: grid; grid-template-columns: repeat(auto-fit, minmax(280px, 1fr)); gap: 12px; margin: 12px 0; }
.result-box, .backfill-box { background: #f9fafb; border-radius: 8px; padding: 12px; }
.result-box pre, .lookup-panel pre { white-space: pre-wrap; word-break: break-all; margin: 8px 0 0; font-size: 12px; }
.detail-counts { justify-content: flex-start; margin: 12px 0; font-size: 14px; }
.backfill-box textarea { width: 100%; box-sizing: border-box; border: 1px solid #d0d5dd; border-radius: 8px; padding: 10px; font-family: ui-monospace, SFMono-Regular, Menlo, monospace; }
.table-wrap { overflow-x: auto; margin-top: 12px; }
.receipt-box { margin-top: 12px; padding: 12px; border-left: 3px solid #175cd3; background: #eff8ff; border-radius: 8px; }
.lookup-form { display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 12px; align-items: end; }
.lookup-form label { display: flex; flex-direction: column; gap: 6px; font-size: 13px; color: #344054; }
.lookup-form input { padding: 9px 10px; border: 1px solid #d0d5dd; border-radius: 8px; }
</style>
