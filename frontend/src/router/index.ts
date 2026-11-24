import { createRouter, createWebHistory } from 'vue-router'

import Overview from '@/views/Overview.vue'
import ProductDedupView from '@/views/ProductDedupView.vue'
import WorkbenchView from '@/views/WorkbenchView.vue'

const router = createRouter({
  history: createWebHistory(),
  routes: [
    { path: '/', name: 'overview', component: Overview },
    { path: '/product-dedup', name: 'product-dedup', component: ProductDedupView },
    { path: '/:moduleKey', name: 'module', component: WorkbenchView, props: true },
  ],
})

export default router
