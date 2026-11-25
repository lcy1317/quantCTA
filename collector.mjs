import { readFile } from 'node:fs/promises';
import { existsSync } from 'node:fs';
import { createInterface } from 'node:readline';
import { chromium } from 'playwright';

const url = 'https://0xverybigorange.github.io/cta-factors/';
const symbols = JSON.parse(await readFile(new URL('./symbols.json', import.meta.url), 'utf8'));
const periods = ['5m','15m','30m','1h','4h','1d','1w'];
const probe = process.argv.includes('--probe');
const watch = process.argv.includes('--watch');
const now = () => Math.floor(Date.now() / 1000);

function sourceTimestamp(text) {
  const match = /最新\s*K\s*线\s+(\d{4})[/-](\d{1,2})[/-](\d{1,2})[\s,，]+(\d{1,2}):(\d{2})(?::(\d{2}))?/.exec(text);
  if (!match) return null;
  const [year, month, day, hour, minute, second] = match.slice(1).map(n => Number(n || 0));
  return Date.UTC(year, month - 1, day, hour - 8, minute, second) / 1000;
}

async function openTab(page, label) {
  await page.locator('.tabs').getByRole('button', {name:label, exact:true}).click({timeout:6000});
}

async function capture(page, symbol, overview, source) {
  await page.locator('button.watch-main').filter({
    has:page.getByText(symbol, {exact:true}),
  }).click({timeout:6000});
  const matrix = await page.locator('table.matrix').evaluate(table => {
    const read = element => ({
      text:element.innerText.trim(),
      title:(element.querySelector('[title]') || element).getAttribute('title'),
    });
    return [...table.rows].map(row => [...row.cells].map(read));
  });
  const header = await page.locator('.symbol-head').evaluate(node => ({
    symbol:node.querySelector('h2').firstChild.textContent.trim(),
    name:node.querySelector('h2 span').innerText.trim(),
    price:node.querySelector('.px strong').innerText.trim(),
    change:node.querySelector('.px em').innerText.trim(),
    meta:[...node.querySelector('.meta').children].map(el => el.innerText.trim()),
  }));
  if (header.symbol !== symbol || matrix[0].length !== periods.length + 1) {
    throw new Error('币种或周期与原站不一致');
  }
  await openTab(page, '综合概率');
  const cards = page.locator('.prob-grid .prob-card');
  if (await cards.count() !== periods.length) throw new Error('综合概率列发生变化');
  const summaries = await cards.evaluateAll(nodes => nodes.map(card => {
    const displayed = card.querySelector('strong').innerText.trim();
    const percent = parseFloat(card.querySelector('i').style.width);
    return {
      label:card.querySelector('span').innerText.trim(),
      displayed,
      bias:card.querySelector('small').innerText.trim(),
      probability:displayed.endsWith('%') && Number.isFinite(percent) ? percent / 100 : null,
    };
  }));
  const observedAt = now();
  const composite = {};
  const probabilities = {};
  for (const [i, period] of periods.entries()) {
    const summary = summaries[i];
    composite[period] = summary;
    if (summary.probability === null) continue;
    probabilities[period] = summary.probability;
    // Read the per-period breakdown and factor quality rows as presented on the original site.
    await cards.nth(i).evaluate(button => button.click());
    Object.assign(summary, await page.locator('.combined').evaluate(node => ({
      score:node.querySelector('.breakdown h3')?.innerText.trim() ?? null,
      categories:[...node.querySelectorAll('.breakdown li')].map(row =>
        [...row.children].map(cell => cell.innerText.trim())),
      factor_quality:[...node.querySelectorAll('table.quality tbody tr')].map(row =>
        [...row.cells].map(cell => cell.innerText.trim())),
    })));
  }
  return {
    id:symbol, base:symbol, label:header.name === symbol ? symbol : symbol + ' ' + header.name,
    observed_at:observedAt, source_at:sourceTimestamp(header.meta.join(' ')), probabilities,
    data:{header, matrix, probabilities, composite, watchlist_probabilities:overview,
          historical_ic_weight:true, source},
  };
}

async function setup(browser) {
  if (!Array.isArray(symbols) || !symbols.length ||
      new Set(symbols).size !== symbols.length || !symbols.every(symbol => /^[A-Z0-9]{2,12}$/.test(symbol))) {
    throw new Error('symbols.json 包含无效币种');
  }
  const page = await browser.newPage({locale:'zh-CN', timezoneId:'Asia/Shanghai',
    viewport:{width:1500,height:960}});
  try {
    const origin = new URL(url).origin;
    await page.addInitScript(({symbols, origin}) => {
      if (location.origin !== origin) return;
      const watch = symbols.map(symbol => ({market:'crypto', symbol}));
      localStorage.setItem('cta-factors-watchlist-v1', JSON.stringify(watch));
      localStorage.setItem('cta-factors-active-v1', JSON.stringify(watch[0]));
      localStorage.setItem('cta-factors-ic-weight-v1','1');
    }, {symbols, origin});
    await page.goto(url, {waitUntil:'domcontentloaded', timeout:20000});
    await page.locator('table.matrix').waitFor({timeout:15000});
    const catalog = await page.locator('.watch-main .watch-symbol b').allTextContents();
    if (symbols.length !== catalog.length || symbols.some(symbol => !catalog.includes(symbol))) {
      throw new Error('自选列表未覆盖配置的全部币种');
    }
    return page;
  } catch (error) {
    await page.close();
    throw error;
  }
}

async function snapshot(page, cold, deadline) {
    const errors = [];
    const assets = [];
    try {
      await page.waitForFunction(() => {
        const m = /(\d+)\s*\/\s*(\d+)/.exec(document.querySelector('.tabs .progress')?.innerText || '');
        return m && m[1] === m[2];
      }, null, {timeout: probe ? 17000 : cold ? 180000 : 25000});
    } catch {
      errors.push('原站部分周期仍在加载，保留本轮已显示的结果');
    }
    await openTab(page, '自选总览');
    const source = {
      url,
      bundle:await page.locator('script[type="module"][src]').first().getAttribute('src', {timeout:3000}),
      watchlist_observed_at:now(),
    };
    const overviewRows = await page.locator('table.overview-table tbody tr').evaluateAll(rows =>
      rows.map(row => [row.querySelector('th b').innerText.trim(),
        [...row.querySelectorAll('td')].map(node => node.innerText.trim())]));
    const overview = Object.fromEntries(overviewRows.map(([symbol, values]) =>
      [symbol, Object.fromEntries(periods.map((period,i) => [period, values[i] ?? null]))]));
    for (const symbol of probe ? symbols.slice(0,1) : symbols) {
      if (Date.now() > deadline) {
        errors.push('已达到本轮采集时限');
        break;
      }
      try {
        const result = await capture(page, symbol, overview[symbol] ?? null, source);
        assets.push(result);
      } catch (error) {
        errors.push(symbol + ': ' + String(error.message).split('\n')[0]);
      }
    }
    const missing = assets.filter(result =>
      !result.source_at || periods.some(period => result.probabilities[period] === undefined));
    if (missing.length) errors.push('原站部分时间戳或周期概率缺失：' + missing.map(result => result.id).join(','));
    return {expected:symbols.length, assets, errors};
}

async function main() {
  const chrome = process.platform === 'darwin' &&
    existsSync('/Applications/Google Chrome.app/Contents/MacOS/Google Chrome');
  const browser = await chromium.launch({headless:true, ...(chrome ? {channel:'chrome'} : {})});
  let page;
  const request = async () => {
    const deadline = Date.now() + (probe ? 45000 : 225000);
    const cold = !page;
    page ??= await setup(browser);
    const result = await snapshot(page, cold, deadline);
    process.stdout.write(JSON.stringify(result) + '\n');
  };
  try {
    if (!watch) {
      await request();
      return;
    }
    for await (const _ of createInterface({input:process.stdin, crlfDelay:Infinity})) {
      try {
        await request();
      } catch (error) {
        process.stderr.write(String(error.stack || error) + '\n');
        await page?.close().catch(() => {});
        page = undefined;
        process.stdout.write(JSON.stringify({expected:symbols.length, assets:[],
          errors:['原站采集失败：' + String(error.message).split('\n')[0]]}) + '\n');
      }
    }
  } finally {
    await browser.close();
  }
}

main().catch(error => {
  process.stderr.write(String(error.stack || error) + '\n');
  process.exitCode = 1;
});
