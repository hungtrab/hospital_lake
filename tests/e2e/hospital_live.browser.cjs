/* Real deployed data: HOSPITAL_WEB_URL=https://... node tests/e2e/hospital_live.browser.cjs */
const { chromium } = require('playwright');
const assert = require('node:assert/strict');
const fs = require('node:fs/promises');

(async () => {
  const base = process.env.HOSPITAL_WEB_URL;
  assert.ok(base, 'HOSPITAL_WEB_URL is required');
  const output = process.env.HOSPITAL_CHECK_DIR || 'results/live-deploy';
  await fs.mkdir(output, {recursive:true});
  const browser = await chromium.launch({headless:true});
  try {
    const page = await browser.newPage({httpCredentials:{username:'admin',password:process.env.HOSPITAL_WEB_PASSWORD,origin:new URL(base).origin},viewport:{width:1440,height:1000}});
    const errors=[];
    page.on('pageerror', e => errors.push(e.message));
    await page.goto(base);
    const results = {};
    for (const route of ['status','overview','patients','history','weekly']) {
      const response = await page.request.get(base+'/api/'+route);
      assert.equal(response.status(),200,route+': '+await response.text());
      results[route] = await response.json();
    }
    assert.equal(results.status.synthetic,true);
    assert.equal(typeof results.status.simulation.enabled,'boolean');
    if (process.env.HOSPITAL_EXPECT_ENABLED !== undefined) {
      assert.ok(['true','false'].includes(process.env.HOSPITAL_EXPECT_ENABLED));
      assert.equal(results.status.simulation.enabled,process.env.HOSPITAL_EXPECT_ENABLED==='true');
    }
    if (process.env.HOSPITAL_EXPECT_PILOT === 'true') {
      await page.getByText('Đang chạy thử',{exact:true}).waitFor();
      assert.match(await page.locator('#sim-detail').textContent(),/lượt thử hôm nay/);
    }
    assert.ok(results.overview.state,'expected a real published snapshot after the gate');
    assert.ok(results.patients.length>0,'run live gate before browser test');
    const id=results.patients[0].encounter_id;
    const timeline=await page.request.get(base+'/api/timeline?encounter_id='+encodeURIComponent(id));
    assert.equal(timeline.status(),200);
    results.timeline=await timeline.json();
    assert.ok(results.timeline.some(r=>['PATIENT_REGISTERED','ADMISSION'].includes(r.event_type)));
    const denied=await page.request.post(base+'/api/simulation',{data:{enabled:true}});
    assert.equal(denied.status(),403);
    await page.waitForFunction(()=>document.getElementById('updated').textContent.includes('Dữ liệu lúc'));
    await page.waitForFunction(()=>document.querySelector('#patient-rows button'));
    await page.locator('#patient-rows button').first().click();
    await page.waitForFunction(()=>document.querySelector('#events strong'));
    assert.equal(await page.locator('#journey').isVisible(),true);
    await page.screenshot({path:output+'/timeline-desktop.png'});
    await page.locator('#close-journey').click();
    for(const width of [1440,768,390]){
      await page.setViewportSize({width,height:1000});
      await page.evaluate(()=>window.scrollTo({top:0,behavior:'instant'}));
      assert.ok(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth),'overflow at '+width);
      await page.screenshot({path:`${output}/website-${width}.png`,fullPage:true});
    }
    // Pair an actual ID with its most recent status, then try an empty search.
    const stable = results.patients.find(r=>['discharged','completed'].includes(r.status)) || results.patients[0];
    await page.locator('#search').fill(stable.encounter_id);
    await page.locator('#patient-status').selectOption(stable.status);
    await page.waitForFunction(id=>document.querySelectorAll('#patient-rows .sub-id').length===1 &&
      document.querySelector('#patient-rows .sub-id').textContent===id,stable.encounter_id);
    await page.locator('#search').fill('NO-MATCH-SEARCH-TEST');
    await page.getByText('Không có lượt khám phù hợp.').waitFor();
    await page.locator('#patient-filters button[type=reset]').click();
    await page.waitForFunction(()=>document.querySelector('#patient-rows button'));
    await page.locator('#patient-lab').selectOption('abnormal');
    const labResponse = await page.request.get(base+'/api/patients?page=1&lab=abnormal');
    assert.equal(labResponse.status(),200);
    const labPage = await labResponse.json();
    if (labPage.items.length) {
      await page.waitForFunction(id=>document.querySelector('#patient-rows .sub-id')?.textContent===id,labPage.items[0].encounter_id);
      await page.locator('#patient-rows button').first().click();
      await page.locator('#events').getByText(/Ngoài khoảng tham chiếu/).first().waitFor();
      await page.locator('#close-journey').click();
    } else {
      await page.getByText('Không có lượt khám phù hợp.').waitFor();
    }
    await page.getByRole('link',{name:'Bản đồ',exact:true}).click();
    await page.waitForFunction(()=>document.querySelector('.room-tile'));
    await page.locator('#map-query').fill('co xuong khop');
    await page.locator('#filter-wing').selectOption('Khám ngoại trú');
    await page.locator('#filter-occupancy').selectOption('empty');
    await page.locator('.room-result').first().waitFor();
    const targetRoom = await page.locator('.room-result').first().getAttribute('data-result');
    await page.locator('.room-result').first().click();
    assert.equal(await page.locator('.room-tile[aria-pressed=true]').getAttribute('data-room'),targetRoom);
    await page.locator('#map-query').fill('NO-MATCH-SEARCH-TEST');
    await page.getByText('Không tìm thấy phòng phù hợp.').waitFor();
    await page.locator('#map-filters button[type=reset]').click();
    await page.waitForFunction(()=>document.querySelector('#map-matches').hidden);
    const rooms = (await (await page.request.get(base+'/api/rooms')).json()).rooms;
    const emptyBed = rooms.find(r=>r.beds?.length && !r.patients.length);
    assert.ok(emptyBed);
    await page.locator('#map-query').fill(emptyBed.beds[0]);
    await page.locator('.room-result').first().click();
    assert.equal(await page.locator('.room-tile[aria-pressed=true]').getAttribute('data-room'),emptyBed.id);
    assert.match(await page.locator('#occupants .bed-list').innerText(),/Trống/);
    const stay = rooms.find(r=>r.patients.some(p=>p.status==='Đã nhập viện'));
    assert.ok(stay,'live inpatient needed for map search');
    const person = stay.patients.find(p=>p.status==='Đã nhập viện');
    await page.goto(base+'/map.html?q='+encodeURIComponent(person.encounter));
    await page.waitForFunction(id=>document.querySelector('.room-tile[aria-pressed=true]')?.dataset.room===id,stay.id);
    const occupied = page.locator('.room-tile.occupied').first();
    await occupied.click();
    await page.locator('#occupants button').first().click();
    await page.waitForFunction(()=>document.querySelector('#events strong'));
    assert.equal(await page.locator('#journey').isVisible(),true);
    await page.locator('#close-journey').click();
    await page.getByRole('button',{name:/^Khám ngoại trú,/}).click();
    if (await page.locator('#floor option').count() > 1) {
      await page.locator('#floor').selectOption('1');
      assert.match(await page.locator('.hall').innerText(), /2/);
    }
    for (const width of [390,1440]) {
      await page.setViewportSize({width,height:1000});
      assert.ok(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth),'map overflow');
      await page.screenshot({path:output+'/map-'+width+'.png',fullPage:true});
    }
    assert.deepEqual(errors,[]);
    await fs.writeFile(output+'/website-check.json',JSON.stringify({status:'PASS',results},null,2));
    console.log('PASS real API routes, timeline, mutation auth, desktop/mobile rendering');
  } finally {await browser.close();}
})().catch(e=>{console.error(e);process.exitCode=1;});
