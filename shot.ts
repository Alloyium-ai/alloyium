import { chromium } from 'playwright'

const url = process.env.SHOT_URL ?? 'http://localhost:8900'
const out = process.env.SHOT_OUT ?? '/tmp/a2a_portal.png'
const clickCh = process.env.SHOT_CHANNEL ?? 'portal-demo'

let browser
try {
  browser = await chromium.launch()
} catch {
  browser = await chromium.launch({ executablePath: '/usr/bin/chromium-browser' })
}
const ctx = await browser.newContext({ viewport: { width: 1360, height: 840 }, deviceScaleFactor: 2 })
const page = await ctx.newPage()
await page.goto(url, { waitUntil: 'domcontentloaded' })
await page.waitForTimeout(2200) // channels + presence load, auto-select fires
try { await page.getByText(clickCh, { exact: true }).click({ timeout: 3000 }) } catch {}
await page.waitForTimeout(1000)
await page.screenshot({ path: out })
await browser.close()
console.log('screenshot saved →', out)
