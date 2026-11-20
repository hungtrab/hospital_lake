import {test} from 'node:test';
import assert from 'node:assert/strict';
import worker from './worker.mjs';
import {createHash} from 'node:crypto';
const authorization = 'Basic '+btoa('admin:fixture-only');
function authenticated(url, init={}) {
  return new Request(url,{...init,headers:{...init.headers,Authorization:authorization}});
}
const serviceToken='service-token-for-tests-only-0123456789';
const env={HOSPITAL_SERVICE_TOKEN:serviceToken,HOSPITAL_ADMIN_TOKEN:"internal-test",HOSPITAL_LOGIN_DIGEST:createHash("sha256").update(authorization).digest("hex"),ASSETS:{fetch:async()=>new Response('<html>HospitalLake</html>')},HOSPITAL_API:{fetch:async()=>Response.json({ok:true})}};
test('serves website with CSP',async()=>{const r=await worker.fetch(authenticated('https://demo.test/'),env);assert.equal(r.status,200);assert.match(r.headers.get('Content-Security-Policy'),/frame-ancestors 'none'/);});
test('fixed private backend, trusted service token replaces incoming value',async()=>{let called;const bound={...env,HOSPITAL_API:{fetch:async(url,opts)=>{called={url,opts};return Response.json({ok:true});}}};const r=await worker.fetch(authenticated('https://demo.test/api/status?target=https://evil.test',{headers:{'X-Backend-Token':'attacker'}}),bound);assert.equal(r.status,200);assert.equal(called.url,'http://hospital-api:8088/api/status');assert.equal(called.opts.headers.get('X-Backend-Token'),serviceToken);assert.equal(r.headers.get('Cache-Control'),'no-store');});
test('rejects arbitrary proxy paths and unauthenticated mutations',async()=>{assert.equal((await worker.fetch(authenticated('https://demo.test/api/query'),env)).status,404);assert.equal((await worker.fetch(authenticated('https://demo.test/api/simulation',{method:'POST',body:'{}'}),env)).status,403);});
test('cross origin and oversized bodies rejected',async()=>{const headers={'X-Admin-Token':'x'.repeat(40),Origin:'https://evil.test'};assert.equal((await worker.fetch(authenticated('https://demo.test/api/simulation',{method:'POST',headers,body:'{}'}),env)).status,403);headers.Origin='https://demo.test';assert.equal((await worker.fetch(authenticated('https://demo.test/api/simulation',{method:'POST',headers,body:'x'.repeat(1025)}),env)).status,413);});
test('backend failures do not expose errors or secrets',async()=>{const r=await worker.fetch(authenticated('https://demo.test/api/status'),{...env,HOSPITAL_API:{fetch:async()=>{throw new Error(serviceToken);}}});assert.equal(r.status,503);assert.ok(!(await r.text()).includes(serviceToken));});

test('anonymous and incorrect credentials receive a login challenge for assets and API',async()=>{
  for(const path of ['/','/app.js','/api/status','/api/simulation']){
    for(const credential of ['', 'Basic '+btoa('admin:wrong')]){
      const result=await worker.fetch(new Request('https://demo.test'+path,{headers:{Authorization:credential}}),env);
      assert.equal(result.status,401);
      assert.match(result.headers.get('WWW-Authenticate'),/Basic/);
    }
  }
});
test('authorized simulation uses private credential',async()=>{
  let headers;
  const origin='https://demo.test';
  const bound={...env,HOSPITAL_API:{fetch:async(url,options)=>{headers=options.headers;return Response.json({});}}};
  const response=await worker.fetch(authenticated(origin+'/api/simulation',{method:'POST',headers:{Origin:origin},body:'{"enabled":false}'}),bound);
  assert.equal(response.status,200);
  assert.equal(headers.get('X-Admin-Token'),env.HOSPITAL_ADMIN_TOKEN);
  assert.equal(headers.get('Authorization'),null);
});
test('forwards only recognized patient search filters', async () => {
  let target;
  const bound = {...env, HOSPITAL_API:{fetch:async url => {target = new URL(url); return Response.json([]);}}};
  const response = await worker.fetch(authenticated('https://demo.test/api/patients?q=P-BM&status=inpatient&department=ICU&lab=done&from=2026-11-19&to=2026-11-20&page=2&as_of=example&unused=1'), bound);
  assert.equal(response.status,200);
  assert.deepEqual(Object.fromEntries(target.searchParams), {q:'P-BM',status:'inpatient',department:'ICU',lab:'done',from:'2026-11-19',to:'2026-11-20',page:'2',as_of:'example'});
});
test('manual commands require login, same origin and private admin credentials', async () => {
  const origin = 'https://demo.test', url = origin + '/api/manual';
  assert.equal((await worker.fetch(new Request(url),env)).status,401);
  assert.equal((await worker.fetch(authenticated(url,{method:'POST',body:'{}'}),env)).status,403);
  let received;
  const bound = {...env,HOSPITAL_API:{fetch:async(target,options) => {
    received = options; assert.equal(target,'http://hospital-api:8088/api/manual'); return Response.json({});
  }}};
  assert.equal((await worker.fetch(authenticated(url),bound)).status,200);
  assert.equal(received.headers.get('X-Admin-Token'),null);
  assert.equal((await worker.fetch(authenticated(url,{method:'POST',headers:{Origin:origin},body:'{}'}),bound)).status,200);
  assert.equal(received.headers.get('X-Admin-Token'),env.HOSPITAL_ADMIN_TOKEN);
});
test('overview forwards only the fixed chart opt-in', async () => {
  const targets=[];
  const bound={...env,HOSPITAL_API:{fetch:async url=>{targets.push(url);return Response.json({});}}};
  for(const query of ['?charts=1&sql=ignored','?charts=0','']) {
    await worker.fetch(authenticated('https://demo.test/api/overview'+query),bound);
  }
  assert.deepEqual(targets.map(url=>new URL(url).search),['?charts=1','','']);
});
