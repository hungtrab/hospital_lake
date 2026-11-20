// Uses local static assets and optional read-only requests to HOSPITAL_WEB_URL.
const {chromium} = require('playwright');
const assert = require('node:assert/strict');
const fs = require('node:fs/promises');
const path = require('node:path');
const site = path.resolve(__dirname,'../../dashboard/hospital/static');
(async () => {
  const live = process.env.HOSPITAL_WEB_URL;
  const base = live || 'https://dashboard.test';
  const browser = await chromium.launch();
  try {
    const page = await browser.newPage({viewport:{width:1440,height:950}, httpCredentials:live ?
      {username:'admin',password:process.env.HOSPITAL_WEB_PASSWORD,origin:new URL(base).origin} : undefined});
    const errors = [], writes = [];
    page.on('pageerror', e => errors.push(e.message));
    page.on('request', r => { if (r.url().includes('/api/') && r.method() !== 'GET') writes.push(r.method()); });
    let unavailable = false, reads = 0;
    if (!live) {
      await page.clock.install();
      await page.route(base+'/**',async route => {
        const url = new URL(route.request().url());
        if (!url.pathname.startsWith('/api/')) {
          const file = url.pathname === '/' ? 'index.html' : url.pathname.slice(1);
          return route.fulfill({path:path.join(site,file)});
        }
        const now = new Date().toISOString(), day = now.slice(0,10);
        const charts = {as_of:now,hours:[{arrivals:1,admissions:0},{arrivals:1,admissions:1}],
          ages:{adult:1,unknown:1}, waits:{short:1}, conditions:{'Tiêu hóa':1,'Chưa ghi nhận':1}};
        const data = {
          status:{simulation:{enabled:true,sent_events:11,last_sent_at:now,days:[],visits:5315}},
          overview:{date:day,queried_at:now,charts,today:{arrivals:2,admissions:1,discharges:0,outpatients:0,labs:0,abnormal_labs:0},state:{current_patients:3,icu_occupancy:1},departments:[]},
          weekly:[],manual:{items:[],departments:[],condition_groups:[]},rooms:{rooms:[],as_of:now},
          history:[{date:day,admissions:1,discharges:0}],
          patients:{items:[{encounter_id:'E-TRACK',patient_id:'P-TRACK',event_time:now,last_event:'ADMISSION'}],as_of:now},
          timeline:[{event_id:'1',event_type:'ADMISSION',department:'ICU',event_time:now}]
        };
        if (url.pathname === '/api/status') {
          reads++;
          if (unavailable) return route.fulfill({status:503,json:{error:'unavailable'}});
        }
        if (url.pathname === '/api/timeline' && unavailable) {
          data.timeline.push({event_id:'2',event_type:'TRANSFER',department:'CARDIOLOGY',event_time:now});
        }
        if (url.pathname === '/api/timeline' && url.searchParams.get('encounter_id') !== 'E-TRACK') data.timeline = [];
        return route.fulfill({json:data[url.pathname.slice(5)]});
      });
    }
    const firstOverview = page.waitForResponse(r => new URL(r.url()).pathname === '/api/overview');
    await page.goto(base+'/dashboard.html');
    const payload = await (await firstOverview).json();
    await page.waitForFunction(expected => ['arrivals','admissions','discharges'].every(k =>
      document.getElementById(k).textContent === Number(expected[k]).toLocaleString('vi-VN')), payload.today);
    assert.ok(payload.charts, 'daily chart query succeeded');
    const series = payload.charts;
    const nreg = series.hours.reduce((sum,h)=>sum+h.arrivals,0);
    for (const counts of [series.ages,series.conditions]) assert.equal(Object.values(counts).reduce((a,b)=>a+b,0),nreg);
    await page.locator('#hour-chart button').first().click();
    assert.equal(await page.locator('#hour-chart button').count(),series.hours.length);
    assert.equal(await page.locator('#hour-chart .future-hour').count(),24-series.hours.length);
    assert.match(await page.locator('#hour-detail').textContent(),/^0h–1h/);
    if (!live) {
      assert.match(await page.locator('#age-chart').textContent(),/Chưa ghi nhận1 lượt · 50%/);
      assert.match(await page.locator('#wait-chart').textContent(),/Dưới 15 phút1 lượt · 100%/);
    }
    await page.locator('#enabled').getByText('Đang bật',{exact:true}).waitFor();
    await page.locator('#flow .flow-row').first().waitFor();
    assert.equal(await page.locator('#flow .flow-row').count(),7);
    await page.locator('#range').selectOption('14');
    assert.equal(await page.locator('#flow .flow-row').count(),14);
    await page.locator('#recent button').first().click();
    await page.locator('#tracked-events strong').first().waitFor();
    const id = await page.locator('#tracked-id').textContent();
    assert.equal(new URL(page.url()).searchParams.get('visit'),id);
    await page.goto(base+'/dashboard.html');
    await page.locator('#tracked-events strong').first().waitFor();
    assert.equal(await page.locator('#tracked-id').textContent(),id);
    await page.locator('#untrack').click();
    assert.equal(new URL(page.url()).searchParams.has('visit'),false);
    await page.locator('#visit').fill(id);
    await page.locator('#track-form button').click();
    await page.locator('#tracked-events strong').first().waitFor();
    if (!live) {
      await page.locator('#refresh:enabled').waitFor();
      const before = reads;
      unavailable = true;
      await page.clock.fastForward(30001);
      await page.locator('#status-note.stale').waitFor();
      await page.locator('#tracked-events strong').getByText('Chuyển khoa',{exact:true}).waitFor();
      assert.ok(reads>before);
      assert.equal(await page.locator('#enabled').textContent(),'Đang bật');
      unavailable = false;
      await page.locator('#refresh').click();
      await page.locator('#status-note:not(.stale)').waitFor();
    }
    for (const width of [1440,390]) {
      await page.setViewportSize({width,height:900});
      assert.ok(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth),'layout at '+width);
      if (process.env.HOSPITAL_CHECK_DIR) {
        await fs.mkdir(process.env.HOSPITAL_CHECK_DIR,{recursive:true});
        await page.evaluate(()=>window.scrollTo({top:0,behavior:'instant'}));
        await page.screenshot({path:path.join(process.env.HOSPITAL_CHECK_DIR,'dashboard-'+width+'.png'),fullPage:true});
      }
    }
    await page.goto(base+'/#patients');
    await page.locator('#patient-rows button').first().click();
    await page.locator('#events strong').first().waitFor();
    const dialogId = await page.locator('#journey-id').textContent();
    await page.getByRole('link',{name:'Theo dõi trên Dashboard'}).click();
    await page.locator('#tracked-events strong').first().waitFor();
    assert.equal(await page.locator('#tracked-id').textContent(),dialogId);
    for (const url of ['/','/manual.html','/map.html']) {
      await page.goto(base+url);
      assert.equal(await page.getByRole('link',{name:'Dashboard',exact:true}).getAttribute('href'),'/dashboard.html');
    }
    assert.deepEqual(errors,[]); assert.deepEqual(writes,[]);
    console.log('PASS',live?'deployed':'fixture','navigation, chart, tracking, persistence, responsive layout; all API reads',id);
  } finally {await browser.close();}
})().catch(e=>{console.error(e);process.exitCode=1;});
