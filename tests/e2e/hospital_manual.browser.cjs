// Opt-in: creates one synthetic visit, moves it, then discharges it on the target.
const {chromium} = require('playwright');
const assert = require('node:assert/strict');
const {randomUUID} = require('node:crypto');
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));
(async () => {
  const base = process.env.HOSPITAL_WEB_URL;
  assert.ok(base && process.env.HOSPITAL_WEB_PASSWORD);
  const code = 'QA-' + randomUUID().slice(0,8).toUpperCase();
  const browser = await chromium.launch();
  try {
    const page = await browser.newPage({httpCredentials:{username:'admin',password:process.env.HOSPITAL_WEB_PASSWORD,origin:new URL(base).origin}});
    page.on('dialog', dialog => dialog.accept());
    await page.goto(base+'/manual.html');
    await page.locator('#code').fill(code);
    await page.locator('#ward').selectOption('EMERGENCY');
    await page.locator('#age').fill('68');
    await page.locator('#condition').selectOption('Tim mạch');
    const action = async click => {
      const [response] = await Promise.all([
        page.waitForResponse(r => r.url().endsWith('/api/manual') && r.request().method() === 'POST'),
        click()
      ]);
      assert.equal(response.status(),200,await response.text());
      return response.json();
    };
    const first = await action(() => page.getByRole('button',{name:'Nhập viện',exact:true}).click());
    console.log('admitted', first.patient_id, first.encounter_id);
    async function verify(caseRow, expectedTypes) {
      const deadline = Date.now()+240000;
      let types, locations;
      do {
        const timeline = await page.request.get(base+'/api/timeline?encounter_id='+caseRow.encounter_id);
        const map = await page.request.get(base+'/api/rooms');
        if (timeline.ok() && map.ok()) {
          types = (await timeline.json()).map(row => row.event_type);
          locations = (await map.json()).rooms.flatMap(r => r.patients.filter(p => p.encounter===caseRow.encounter_id).map(p => p.bed));
          if (JSON.stringify(types)===JSON.stringify(expectedTypes) &&
              JSON.stringify(locations)===JSON.stringify(caseRow.bed_id ? [caseRow.bed_id] : [])) {
            console.log('verified',caseRow.revision, locations, types); return;
          }
        }
        await sleep(5000);
      } while (Date.now()<deadline);
      throw Error('Timed out verifying '+caseRow.encounter_id+' '+JSON.stringify({locations, types}));
    }
    const events = ['PATIENT_REGISTERED','ADMISSION','BED_ASSIGNED'];
    await verify(first,events);
    await page.locator('#destination').selectOption('CARDIOLOGY');
    const moved = await action(() => page.getByRole('button',{name:'Chuyển khoa',exact:true}).click());
    events.push('TRANSFER');
    await verify(moved,events);
    const done = await action(() => page.locator('#discharge').click());
    events.push('BED_RELEASED','DISCHARGE');
    await verify(done,events);
    await page.locator('#show-events').click();
    await page.locator('#events').getByText('Xuất viện',{exact:true}).waitFor();
    await page.locator('#events').getByText('68 tuổi · Tim mạch',{exact:true}).waitFor();
    console.log('PASS manual web / Kafka / Silver / Trino room map and timeline');
  } finally { await browser.close(); }
})().catch(error => { console.error(error); process.exitCode = 1; });
