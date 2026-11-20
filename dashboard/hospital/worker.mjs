const READ_PATHS = new Set(['/api/status','/api/manual','/api/overview','/api/patients','/api/rooms','/api/history','/api/weekly','/api/timeline']);
const SECURITY = {
  'X-Content-Type-Options':'nosniff',
  'Referrer-Policy':'same-origin',
  'Content-Security-Policy':"default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; img-src 'self' data:; connect-src 'self'; frame-ancestors 'none'; base-uri 'none'; form-action 'self'",
};
function json(body,status){return Response.json(body,{status,headers:{...SECURITY,'Cache-Control':'no-store'}});}

export default {
  async fetch(request,env){
    if (!env.HOSPITAL_LOGIN_DIGEST) return json({error:'Login configuration unavailable'},503);
    const credential = request.headers.get('Authorization') || '';
    const digest = await crypto.subtle.digest('SHA-256',new TextEncoder().encode(credential));
    const actual = Array.from(new Uint8Array(digest),byte=>byte.toString(16).padStart(2,'0')).join('');
    if (actual !== env.HOSPITAL_LOGIN_DIGEST) {
      return new Response('Đăng nhập HospitalLake', {status:401,headers:{
        ...SECURITY, 'Cache-Control':'no-store', 'WWW-Authenticate':'Basic realm="HospitalLake", charset="UTF-8"'
      }});
    }
    const url=new URL(request.url);
    if(!url.pathname.startsWith('/api/')){
      if(!['GET','HEAD'].includes(request.method))return json({error:'Method not allowed'},405);
      const assetRequest = new Request(request);
      assetRequest.headers.delete('Authorization');
      const result=await env.ASSETS.fetch(assetRequest);
      const response=new Response(result.body,result);
      for(const [key,value] of Object.entries(SECURITY))response.headers.set(key,value);
      response.headers.set('Cache-Control','private, no-store');
      return response;
    }
    const control=['/api/simulation','/api/manual'].includes(url.pathname)&&request.method==='POST';
    if(!control&&!(request.method==='GET'&&READ_PATHS.has(url.pathname)))return json({error:'Not found'},404);
    if(!env.HOSPITAL_SERVICE_TOKEN||!env.HOSPITAL_API)return json({error:'Kết nối máy chủ chưa được cấu hình.'},503);
    const headers=new Headers({'X-Backend-Token':env.HOSPITAL_SERVICE_TOKEN,'Content-Type':'application/json'});
    let body;
    if(control){
      const origin=request.headers.get('Origin');
      if(origin!==url.origin)return json({error:'Origin not allowed'},403);
      if (!env.HOSPITAL_ADMIN_TOKEN) return json({error:'Simulation control unavailable'},503);
      headers.set('X-Admin-Token',env.HOSPITAL_ADMIN_TOKEN);
      if(!request.body)return json({error:'Missing request body'},400);
      const reader=request.body.getReader(), chunks=[];
      let length=0;
      for(;;){const {value,done}=await reader.read();if(done)break;length+=value.byteLength;
        if(length>1024){await reader.cancel();return json({error:'Request too large'},413);}chunks.push(value);}
      body=new Uint8Array(length);let offset=0;for(const chunk of chunks){body.set(chunk,offset);offset+=chunk.byteLength;}
    }
    try{
      const target=new URL('http://hospital-api:8088'+url.pathname);
      if(url.pathname==='/api/overview' && url.searchParams.get('charts')==='1')target.searchParams.set('charts','1');
      if(url.pathname==='/api/patients'){
        for(const key of ['q','status','department','lab','from','to','page','as_of']){
          if(url.searchParams.has(key))target.searchParams.set(key,url.searchParams.get(key));
        }
      }
      if(url.pathname==='/api/timeline')target.searchParams.set('encounter_id',url.searchParams.get('encounter_id')||'');
      const result=await env.HOSPITAL_API.fetch(target.href,{method:request.method,headers,body,redirect:'manual',signal:AbortSignal.timeout(30000)});
      if(result.status>=300&&result.status<400)return json({error:'Unexpected backend redirect'},502);
      return new Response(result.body,{status:result.status,headers:{...SECURITY,'Content-Type':'application/json; charset=utf-8','Cache-Control':'no-store'}});
    }catch(error){
      console.error(JSON.stringify({event:'hospital_backend_unavailable',name:error.name}));
      return json({error:'Máy chủ dữ liệu chưa phản hồi. Vui lòng thử lại.'},503);
    }
  }
};
