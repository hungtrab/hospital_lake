/* Optional browser QA: requires Playwright; server must already be running.
 * QUALITY_WEB_URL=http://127.0.0.1:8765 node tests/e2e/quality_manager.browser.cjs
 * RUN_QUALITY_JOB=1 also exercises the real Spark subprocess from the UI.
 */
const {chromium} = require('playwright');
const assert = require('node:assert/strict');
const fs = require('node:fs/promises');
const path = require('node:path');

(async () => {
  const output = path.resolve('results/quality-web-qa');
  await fs.mkdir(output, {recursive:true});
  const browser = await chromium.launch({headless:true, ...(process.env.BROWSER_EXECUTABLE ? {executablePath:process.env.BROWSER_EXECUTABLE} : {})});
  const context = await browser.newContext({viewport:{width:1440,height:1100}});
  const page = await context.newPage();
  const errors = [];
  page.on('pageerror',error=>errors.push(error.message));
  page.on('console',message=>{if(message.type()==='error') errors.push(message.text());});
  const base = process.env.QUALITY_WEB_URL || 'http://127.0.0.1:8765';
  try {
    await page.goto(base);
    await page.waitForFunction(()=>document.getElementById('total-input').textContent==='39');
    assert.equal(await page.locator('#total-valid').textContent(),'11');
    assert.equal(await page.locator('#total-rejected').textContent(),'27');
    assert.equal(await page.locator('#checks-passed').textContent(),'36');
    await page.waitForTimeout(350);
    await page.screenshot({path:path.join(output,'overview-desktop.png'),fullPage:true});
    await page.getByRole('button',{name:'Xem kịch bản →'}).click();
    await page.locator('#outcome-filter').selectOption('quarantine');
    assert.equal(await page.locator('#scenario-rows tr').count(),25);
    await page.locator('#search').fill('L_ALIAS');
    assert.equal(await page.locator('#scenario-rows tr').count(),0);
    assert.equal(await page.locator('#no-matches').isVisible(),true);
    await page.locator('#outcome-filter').selectOption('');
    await page.getByRole('button',{name:'Xem chi tiết L_ALIAS',exact:true}).click();
    assert.equal(await page.locator('#diff-rows .changed').count(),3);
    await page.screenshot({path:path.join(output,'detail-desktop.png'),fullPage:false});
    await page.getByRole('button',{name:'Đóng chi tiết'}).click();
    await page.locator('#search').fill('ER123');
    await page.getByRole('button',{name:'Xem chi tiết E_TYPO',exact:true}).click();
    assert.match(await page.locator('#detail-reasons').textContent(),/UNKNOWN_DEPARTMENT/);
    const oldStatus = await page.locator('#review-status').inputValue();
    const oldNote = await page.locator('#review-note').inputValue();
    await page.locator('#review-status').selectOption('investigate');
    const note = 'Kiểm thử giao diện: <img src=x onerror=alert(1)> cần đối chiếu nguồn.';
    await page.locator('#review-note').fill(note);
    await page.getByRole('button',{name:'Lưu rà soát',exact:true}).click();
    await page.waitForFunction(()=>document.getElementById('review-feedback').textContent==='Đã lưu rà soát.');
    await page.reload();
    await page.waitForFunction(()=>document.getElementById('total-input').textContent==='39');
    await page.locator('#search').fill('ER123');
    await page.getByRole('button',{name:'Xem chi tiết E_TYPO',exact:true}).click();
    assert.equal(await page.locator('#review-note').inputValue(),note);
    assert.equal(await page.locator('#review-status').inputValue(),'investigate');
    // Restore the prior note; QA must not leave its sample review in user data.
    await page.locator('#review-status').selectOption(oldStatus);
    await page.locator('#review-note').fill(oldNote);
    await page.getByRole('button',{name:'Lưu rà soát',exact:true}).click();
    await page.waitForFunction(()=>document.getElementById('review-feedback').textContent==='Đã lưu rà soát.');
    await page.keyboard.press('Escape');
    assert.equal(await page.locator('#detail-dialog').isVisible(),false);
    await page.locator('#search').fill('');
    await page.locator('#entity-filter').selectOption('lab_results');
    assert.equal(await page.locator('#scenario-rows tr').count(),14);
    await page.screenshot({path:path.join(output,'scenarios-desktop.png'),fullPage:true});
    const downloadEvent = page.waitForEvent('download');
    await page.locator('#download').click();
    const download = await downloadEvent;
    assert.match(download.suggestedFilename(),/^run-.*\.json$/);
    await page.getByRole('button',{name:'Lịch sử chạy',exact:true}).click();
    assert.ok(await page.locator('#history-rows tr').count()>=1);
    for (const width of [1024,768,390,360]) {
      await page.setViewportSize({width,height:900});
      await page.getByRole('button',{name:'Tổng quan',exact:true}).click();
      assert.ok(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth),`overflow at ${width}`);
      await page.screenshot({path:path.join(output,`overview-${width}.png`),fullPage:true});
    }
    await page.getByRole('button',{name:/Kịch bản & rà soát/}).click();
    await page.locator('#entity-filter').selectOption('');
    await page.locator('#search').fill('L_ALIAS');
    assert.ok(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth),'scenario view overflows mobile');
    await page.getByRole('button',{name:'Xem chi tiết L_ALIAS',exact:true}).click();
    assert.ok(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth),'dialog overflows mobile');
    await page.screenshot({path:path.join(output,'detail-mobile.png'),fullPage:false});
    await page.keyboard.press('Escape');
    await page.setViewportSize({width:1440,height:1100});
    if (process.env.RUN_QUALITY_JOB === '1') {
      await page.getByRole('button',{name:'Chạy bộ kiểm tra',exact:true}).click();
      await page.waitForFunction(()=>document.getElementById('run-button').disabled);
      await page.getByRole('button',{name:'Xem nhật ký',exact:true}).click();
      await page.waitForFunction(()=>document.getElementById('job-title').textContent==='Lượt kiểm tra đã hoàn tất',null,{timeout:240000});
      await page.waitForFunction(()=>document.getElementById('total-input').textContent==='39');
      assert.match(await page.locator('#job-log').textContent(),/PASS 36 scenarios/);
      assert.equal(await page.locator('#run-button').isDisabled(),false);
    }
    const emptyPage = await context.newPage();
    await emptyPage.route('**/api/state',route=>route.fulfill({json:{runs:[],job:null,csrf_token:'test-only'}}));
    await emptyPage.goto(base);
    await emptyPage.waitForSelector('#empty:not([hidden])');
    assert.equal(await emptyPage.locator('#report-content').isVisible(),false);
    await emptyPage.getByRole('button',{name:'Lịch sử chạy',exact:true}).click();
    await emptyPage.locator('#history-empty').waitFor({state:'visible'});
    assert.equal(await emptyPage.locator('#history-empty').isVisible(),true);
    await emptyPage.unroute('**/api/state');
    await emptyPage.route('**/api/state',route=>route.fulfill({status:503,json:{error:'Không đọc được kết quả kiểm thử.'}}));
    await emptyPage.reload();
    await emptyPage.waitForSelector('#error:not([hidden])');
    assert.match(await emptyPage.locator('#error').textContent(),/Không đọc được/);
    await emptyPage.close();
    assert.deepEqual(errors,[]);
    console.log('PASS: real report totals, filters, field diff, persisted review, inert note text, download, history, responsive layouts, keyboard dialog, empty and error states' + (process.env.RUN_QUALITY_JOB==='1' ? ', real Spark execution' : ''));
  } finally { await browser.close(); }
})().catch(error=>{console.error(error);process.exitCode=1;});
