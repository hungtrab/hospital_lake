import React from 'react';
import {AbsoluteFill, Sequence, staticFile, useCurrentFrame, useVideoConfig, interpolate, cancelRender} from 'remotion';
import {Audio} from '@remotion/media';
import {loadFont} from '@remotion/fonts';
import {useGsapTimeline} from '@remotion/gsap';
import rawTimeline from './timeline.json';

export type Scene = {
  index:number; title:string; visual:string; eyebrow:string; from:number; duration:number;
  audio:{src:string;from:number;duration:number}[];
  captions:{text:string;startMs:number;endMs:number}[]; marks:number[];
};
type Timeline = {id:string;fps:number;durationInFrames:number;scenes:Scene[];draft?:boolean};
export const timeline:Timeline=rawTimeline;
const INK='#14213D',TEAL='#0F766E',MUTED='#526171',LINE='#D7E1E5',SOFT='#EAF5F3',WASH='#F5F7F8';
Promise.all([
  loadFont({family:'Inter',url:staticFile('fonts/Inter-Regular.otf'),weight:'400'}),
  loadFont({family:'Inter',url:staticFile('fonts/Inter-SemiBold.otf'),weight:'600'}),
]).catch(cancelRender);

const fade=(t:number,at:number)=>interpolate(t,[at,at+.6],[0,1],{extrapolateLeft:'clamp',extrapolateRight:'clamp'});
const Reveal:React.FC<{t:number;at:number;children:React.ReactNode;style?:React.CSSProperties}>=({t,at,children,style})=><div data-reveal style={{opacity:fade(t,at),transform:`translateY(${(1-fade(t,at))*16}px)`,...style}}>{children}</div>;
const Label:React.FC<{children:React.ReactNode;style?:React.CSSProperties}>=({children,style})=><div style={{fontSize:23,letterSpacing:1.3,fontWeight:600,color:TEAL,...style}}>{children}</div>;
const Box:React.FC<{children:React.ReactNode;style?:React.CSSProperties}>=({children,style})=><div style={{border:`2px solid ${LINE}`,background:WASH,borderRadius:12,padding:'28px 32px',...style}}>{children}</div>;
const Arrow=()=> <div style={{fontSize:50,color:MUTED,padding:'0 14px',alignSelf:'center'}}>→</div>;
const Callout:React.FC<{children:React.ReactNode;style?:React.CSSProperties}>=({children,style})=><div style={{background:SOFT,borderLeft:`5px solid ${TEAL}`,padding:'23px 30px',fontSize:34,lineHeight:1.5,...style}}>{children}</div>;
const Source:React.FC<{children:React.ReactNode}>=({children})=><div style={{position:'absolute',left:100,right:100,bottom:125,fontSize:21,color:MUTED,lineHeight:1.4}}>{children}</div>;

function Problem({t,marks:m}:{t:number;marks:number[]}){
 return <>
  <div style={{position:'absolute',left:100,top:265,width:440}}>
   {['Tiếp nhận','Xét nghiệm','Giường bệnh'].map((label,i)=><Reveal key={label} t={t} at={m[Math.min(i,1)]} style={{marginBottom:22}}><Box style={{height:135,background:'white'}}><Label>{label.toUpperCase()}</Label><div style={{height:7,width:270,background:LINE,marginTop:18}}/><div style={{height:7,width:195,background:LINE,marginTop:13}}/></Box></Reveal>)}
  </div>
  <Reveal t={t} at={m[2]} style={{position:'absolute',left:590,top:360,width:490}}><Box style={{background:SOFT,borderColor:TEAL,padding:40}}><div style={{fontSize:58,fontWeight:600,letterSpacing:-2}}>HospitalLake</div><div style={{fontSize:34,marginTop:20,lineHeight:1.7}}>Gom ghi chép<br/>Kiểm tra dữ liệu<br/>Tính số liệu</div></Box></Reveal>
  <Reveal t={t} at={m[3]} style={{position:'absolute',left:1150,top:350,width:635}}><Label>CÂU HỎI CẦN TRẢ LỜI</Label><div style={{fontSize:57,fontWeight:600,lineHeight:1.35,marginTop:25}}>Mỗi khoa có<br/>bao nhiêu lượt<br/>nhập viện?</div></Reveal>
  <Source>Tình huống minh họa · Project kỹ thuật dữ liệu · Dữ liệu giả lập.</Source>
 </>;
}

function Entities({t,marks:m}:{t:number;marks:number[]}){
 const entities=[['patients','Bệnh nhân'],['encounters','Lượt khám / nhập viện'],['lab_results','Kết quả xét nghiệm'],['medications','Y lệnh thuốc'],['vitals','Lần đo sinh hiệu'],['bed_events','Gán / chuyển / trả giường']];
 return <>
  <Reveal t={t} at={m[0]} style={{position:'absolute',left:100,top:270,width:1040}}>
   <Label>MỖI DÒNG MỘT BẢN GHI · MỖI CỘT MỘT THÔNG TIN</Label>
   <div style={{marginTop:24,border:`2px solid ${LINE}`,borderRadius:10,overflow:'hidden'}}>
    {entities.map(([name,label],i)=><div key={name} style={{display:'grid',gridTemplateColumns:'360px 1fr',padding:'19px 25px',background:i%2?'white':WASH,borderBottom:i===5?0:`1px solid ${LINE}`,opacity:fade(t,m[1]+i*.38),fontSize:32}}><span style={{color:TEAL,fontWeight:600}}>{name}</span><span>{label}</span></div>)}
   </div>
  </Reveal>
  <Reveal t={t} at={m[2]} style={{position:'absolute',left:1220,top:340,width:570}}><div style={{fontSize:100,fontWeight:600,color:TEAL}}>6</div><div style={{fontSize:43,lineHeight:1.4}}>loại ghi chép<br/>do chương trình tự sinh</div></Reveal>
  <Reveal t={t} at={m[3]} style={{position:'absolute',left:1220,top:655,width:560}}><Callout style={{fontSize:30}}>Dữ liệu giả lập.<br/>Không có hồ sơ người thật.</Callout></Reveal>
  <Source>Nguồn mô hình: docs/data-model.md · Cùng seed và cấu hình → tạo lại cùng dữ liệu.</Source>
 </>;
}

function Relationships({t,marks:m}:{t:number;marks:number[]}){
 return <>
  <div style={{position:'absolute',left:100,right:100,top:330,display:'flex',alignItems:'center'}}>
   <Reveal t={t} at={m[0]} style={{flex:1}}><Box style={{height:210,background:SOFT,borderColor:TEAL}}><Label>BỆNH NHÂN</Label><div style={{fontSize:68,fontWeight:600,marginTop:18}}>P001</div></Box></Reveal><Arrow/>
   <Reveal t={t} at={m[1]} style={{flex:1}}><Box style={{height:210}}><Label>LƯỢT NHẬP VIỆN</Label><div style={{fontSize:68,fontWeight:600,marginTop:18}}>E001</div></Box></Reveal><Arrow/>
   <Reveal t={t} at={m[3]} style={{flex:1}}><Box style={{height:210}}><Label>KẾT QUẢ XÉT NGHIỆM</Label><div style={{fontSize:68,fontWeight:600,marginTop:18}}>L001</div></Box></Reveal>
  </div>
  <Reveal t={t} at={m[2]} style={{position:'absolute',left:100,top:625,width:800}}><Callout><b>patient_id</b> → của người nào?</Callout></Reveal>
  <Reveal t={t} at={m[3]} style={{position:'absolute',left:980,top:625,width:840}}><Callout><b>encounter_id</b> → trong lượt nào?</Callout></Reveal>
  <Source>Mã được rút gọn để minh họa · Một người có nhiều lượt; một lượt có nhiều xét nghiệm.</Source>
 </>;
}

function TwoSpeeds({t,marks:m}:{t:number;marks:number[]}){
 return <>
  <Reveal t={t} at={m[0]} style={{position:'absolute',left:100,top:280,width:810}}><Box style={{height:455,padding:42}}><Label>NHÌN LẠI QUÁ KHỨ</Label><div style={{fontSize:48,fontWeight:600,lineHeight:1.4,marginTop:25}}>Tháng trước,<br/>mỗi khoa có<br/>bao nhiêu lượt?</div><Reveal t={t} at={m[1]}><div style={{fontSize:35,color:TEAL,marginTop:30}}>Batch = xử lý theo lô</div></Reveal></Box></Reveal>
  <Reveal t={t} at={m[2]} style={{position:'absolute',left:980,top:280,width:840}}><Box style={{height:455,padding:42,background:SOFT,borderColor:TEAL}}><Label>THEO DÕI VIỆC MỚI</Label><div style={{fontSize:48,fontWeight:600,lineHeight:1.4,marginTop:25}}>Trong 5 phút vừa rồi<br/>có thêm<br/>bao nhiêu ca?</div><div style={{fontSize:35,color:TEAL,marginTop:30}}>Stream = nhận liên tục</div></Box></Reveal>
  <Reveal t={t} at={m[3]} style={{position:'absolute',left:100,right:100,top:780}}><Callout style={{fontSize:32}}><b>Lambda Architecture</b> = hai nhánh xử lý + lớp phục vụ kết quả.</Callout></Reveal>
  <Source>Thiết kế của project · Chỉ số trực tiếp là bước tiếp theo; nhánh stream hiện ghi được vào kho.</Source>
 </>;
}

function Bronze({t,marks:m}:{t:number;marks:number[]}){
 const rows=[['E001','P001','ER','08:00','10:00'],['E001','P001','ER','08:00','10:00'],['E002','P002','ICU','09:00','12:00'],['E003','P003','ICU','11:00','10:00'],['E004','P999','ER','10:00','12:00']];
 return <>
  <Reveal t={t} at={m[0]} style={{position:'absolute',left:100,top:270,width:1130}}><Label>BỘ VÍ DỤ NHỎ · CÓ P001, P002, P003</Label>
   <div style={{marginTop:25,border:`2px solid ${LINE}`,borderRadius:10,overflow:'hidden'}}>
    <div style={{display:'grid',gridTemplateColumns:'repeat(5,1fr)',fontSize:25,padding:'20px 28px',background:SOFT,fontWeight:600}}>{['Mã lượt','Mã người','Khoa','Giờ vào','Giờ ra'].map(s=><div key={s}>{s}</div>)}</div>
    {rows.map((row,i)=><div key={i} data-toy-row style={{display:'grid',gridTemplateColumns:'repeat(5,1fr)',fontSize:32,padding:'21px 28px',borderTop:`1px solid ${LINE}`,background:i===1&&t>=m[1]?'#EFF2F5':i>=3&&t>=m[2]?'#FFF1E7':'white'}}>{row.map((text,j)=><div key={j}>{text}</div>)}</div>)}
   </div>
  </Reveal>
  <div style={{position:'absolute',left:1300,top:300,width:510}}>
   <Reveal t={t} at={m[1]}><div style={{fontSize:36,marginBottom:35}}>E001 lặp hai lần.</div></Reveal>
   <Reveal t={t} at={m[2]}><div style={{fontSize:36,marginBottom:35}}>E003 sai giờ.<br/>P999 không tồn tại.</div></Reveal>
   <Reveal t={t} at={m[3]}><Callout style={{fontSize:34}}><b>Bronze</b><br/>Giữ cả 5 dòng gốc.</Callout></Reveal>
  </div>
  <Reveal t={t} at={m[4]} style={{position:'absolute',left:100,right:100,top:810}}><div style={{fontSize:43,fontWeight:600}}>5 dòng nguồn có phải là 5 lượt hợp lệ?</div></Reveal>
  <Source>Ví dụ minh họa, không phải kết quả live · Các giờ trong cùng ngày, múi giờ UTC.</Source>
 </>;
}

function SilverGold({t,marks:m}:{t:number;marks:number[]}){
 return <>
  <Reveal t={t} at={m[0]} style={{position:'absolute',left:100,top:280,width:800}}><Box style={{height:360,padding:38}}><Label>HAI LƯỢT HỢP LỆ</Label><div style={{fontSize:38,lineHeight:2,marginTop:30}}>E001 · P001 · EMERGENCY<br/>E002 · P002 · ICU</div><Reveal t={t} at={m[2]}><div style={{fontSize:43,fontWeight:600,color:TEAL,marginTop:30}}>Silver = bản sạch</div></Reveal></Box></Reveal>
  <Reveal t={t} at={m[1]} style={{position:'absolute',left:100,top:680,width:800}}><Callout style={{fontSize:30}}>2 dòng lỗi → khu riêng có lý do.<br/>1 bản sao E001 → gộp lại.</Callout></Reveal>
  <Reveal t={t} at={m[3]} style={{position:'absolute',left:980,top:280,width:840}}><Box style={{height:360,padding:38,background:SOFT,borderColor:TEAL}}><Label>ĐẾM THEO KHOA</Label><div style={{display:'flex',justifyContent:'space-between',marginTop:30,fontSize:38,lineHeight:2}}><div>EMERGENCY<br/>ICU</div><div style={{fontWeight:600}}>1 lượt<br/>1 lượt</div></div><Reveal t={t} at={m[4]}><div style={{fontSize:43,fontWeight:600,color:TEAL,marginTop:30}}>Gold = tổng hợp</div></Reveal></Box></Reveal>
  <Reveal t={t} at={m[4]} style={{position:'absolute',left:980,top:680,width:840}}><div style={{fontSize:40,lineHeight:1.7}}>Gốc → sạch → tổng hợp.<br/>Ba mức dữ liệu trong cùng kho.</div></Reveal>
  <Source>Ví dụ minh họa · Quarantine = giữ riêng dòng lỗi; bản gốc vẫn được bảo toàn ở Bronze.</Source>
 </>;
}

function Roles({t,marks:m}:{t:number;marks:number[]}){
 const rows=[['Spark','Xử lý và tính toán dữ liệu.'],['MinIO','Giữ các file trong kho.'],['Iceberg','Tổ chức file thành bảng có quản lý.'],['Trino','Đọc bảng, trả lời câu hỏi SQL.']];
 return <>
  <div style={{position:'absolute',left:100,top:280,right:100}}>{rows.map(([name,description],i)=><Reveal key={name} t={t} at={m[i]} style={{display:'flex',alignItems:'center',borderBottom:`1px solid ${LINE}`,padding:'26px 30px',background:i===1||i===2?SOFT:'white'}}><div style={{width:350,fontSize:46,fontWeight:600,color:TEAL}}>{name}</div><div style={{fontSize:39}}>{description}</div></Reveal>)}</div>
  <Reveal t={t} at={m[4]} style={{position:'absolute',left:100,right:100,top:805}}><Callout style={{fontSize:34}}><b>Lakehouse ở đây = MinIO + Iceberg.</b> Các lớp dữ liệu cùng nằm trong kho này.</Callout></Reveal>
  <Source>Nguồn: docs/architecture.md · SQL là ngôn ngữ viết câu hỏi cho bảng dữ liệu.</Source>
 </>;
}

const FlowRow:React.FC<{labels:string[][];t:number;at:number;activeAt?:number;top:number}>=({labels,t,at,top})=><Reveal t={t} at={at} style={{position:'absolute',left:100,right:100,top,display:'flex',alignItems:'stretch'}}>{labels.map(([name,description],i)=><React.Fragment key={name}>{i>0?<Arrow/>:null}<Box style={{flex:1,padding:'20px 15px',background:'white',fontSize:31,textAlign:'center',height:135}}><div style={{fontWeight:600,fontSize:33}}>{name}</div><div style={{fontSize:27,color:MUTED,marginTop:12}}>{description}</div></Box></React.Fragment>)}</Reveal>;
function Architecture({t,marks:m}:{t:number;marks:number[]}){
 return <>
  <div style={{position:'absolute',left:100,top:263}}><Label>NHÁNH LỊCH SỬ · ĐÃ CÓ KẾT QUẢ TRUY VẤN</Label></div>
  <FlowRow t={t} at={m[0]} top={310} labels={[["File nguồn","bảng lịch sử"],["Spark","đọc và tính"],["Bronze / Silver / Gold","gốc → sạch → tổng hợp"],["Trino","câu hỏi → kết quả"]]}/>
  <Reveal t={t} at={m[1]} style={{position:'absolute',left:100,top:490}}><Label>NHÁNH SỰ KIỆN MỚI · HIỆN TỚI BƯỚC LƯU VÀO KHO</Label></Reveal>
  <FlowRow t={t} at={m[1]} top={535} labels={[["Simulator","giả lập thông báo"],["Kafka","giữ thông báo"],["Spark Streaming","đọc và kiểm tra"],["Bronze stream","bản gốc / khu lỗi"]]}/>
  <Reveal t={t} at={m[4]} style={{position:'absolute',left:100,right:100,top:714}}><Callout style={{fontSize:31}}><b>Lưu chung: MinIO + Iceberg.</b> Các lớp là bảng, không phải máy riêng.</Callout></Reveal>
  <Reveal t={t} at={m[5]} style={{position:'absolute',left:100,right:100,top:820}}><div style={{fontSize:29,color:MUTED,border:`2px dashed ${MUTED}`,borderRadius:8,padding:'18px 26px'}}>Kế hoạch: chỉ số trực tiếp → ClickHouse → Grafana · Kubernetes về sau.</div></Reveal>
  <Source>Nguồn: docs/architecture.md · Sơ đồ hiện trạng; phần viền đứt là kế hoạch.</Source>
 </>;
}

function Progress({t,marks:m}:{t:number;marks:number[]}){
 return <>
  <div style={{position:'absolute',left:100,right:100,top:290,display:'flex',gap:70}}>
   {[['1.000','bệnh nhân giả lập',m[0]],['3.094','dòng lượt khám / nhập viện',m[1]],['11 / 11','truy vấn có kết quả',m[2]]].map(([number,label,at])=><Reveal key={String(number)} t={t} at={Number(at)} style={{flex:1}}><div style={{fontSize:100,color:TEAL,fontWeight:600,letterSpacing:-4}}>{number}</div><div style={{fontSize:31,marginTop:15}}>{label}</div></Reveal>)}
  </div>
  <Reveal t={t} at={m[2]} style={{position:'absolute',left:100,right:100,top:530}}><Callout style={{fontSize:35}}>Đã chạy: nguồn → bản gốc → 6 nhóm sạch → 4 bảng tổng hợp → truy vấn.</Callout></Reveal>
  <Reveal t={t} at={m[3]} style={{position:'absolute',left:100,right:100,top:660}}><div style={{fontSize:35}}>Đã chạy thêm: thông báo → Kafka → Spark → kho dữ liệu.</div></Reveal>
  <Reveal t={t} at={m[4]} style={{position:'absolute',left:100,right:100,top:755}}><Box style={{fontSize:32,background:'#FFF4EB',borderColor:'#E8CEB7',padding:'24px 30px'}}>Stream còn tăng hàng chờ và từng hết bộ nhớ.<br/><b>Chưa kết luận chạy ổn định lâu dài.</b></Box></Reveal>
  <Source>Số đo 25/10/2026 · PATIENTS=1000, SEED=42 · docs/demo-script.md; docs/architecture.md.</Source>
 </>;
}

function Demo({t,marks:m}:{t:number;marks:number[]}){
 const commands=[['1. Khởi động','make up',m[1]],['2. Xử lý dữ liệu','make batch PYTHON=.venv/bin/python',m[2]],['3. Hỏi và xem kết quả','make query PYTHON=.venv/bin/python',m[3]]];
 return <>
  <Reveal t={t} at={m[0]} style={{position:'absolute',left:100,top:260,width:1020}}><Label>CHẠY TRÊN MÔI TRƯỜNG ĐÃ CHUẨN BỊ</Label><div style={{marginTop:22,background:INK,color:'white',borderRadius:12,padding:'20px 34px',height:440}}>{commands.map(([label,command,at])=><Reveal key={String(label)} t={t} at={Number(at)} style={{margin:'22px 0 34px'}}><div style={{fontSize:23,color:'#B8D8D3',marginBottom:12}}>{label}</div><div style={{fontFamily:'monospace',fontSize:30,whiteSpace:'nowrap'}}>{command}</div></Reveal>)}</div></Reveal>
  <Reveal t={t} at={m[3]} style={{position:'absolute',left:1200,top:340,width:610}}><div style={{fontSize:48,fontWeight:600,lineHeight:1.45}}>Một câu hỏi.<br/>Một bảng kết quả.<br/>Có thể chạy lại.</div></Reveal>
  <Reveal t={t} at={m[4]} style={{position:'absolute',left:100,right:100,top:765}}><Callout style={{fontSize:33}}><b>Tiếp theo:</b> ổn định stream → chỉ số trực tiếp và biểu đồ → triển khai cụm máy.</Callout></Reveal>
  <Source>Repo: github.com/hungtrab/hospital_lake · Chuẩn bị theo docs/demo-script.md trước buổi demo.</Source>
 </>;
}

const Visual:React.FC<{scene:Scene;t:number}>=({scene,t})=>{
 const props={t,marks:scene.marks};
 switch(scene.visual){
  case 'problem':return <Problem {...props}/>;case 'entities':return <Entities {...props}/>;
  case 'relationships':return <Relationships {...props}/>;case 'two-speeds':return <TwoSpeeds {...props}/>;
  case 'bronze':return <Bronze {...props}/>;case 'silver-gold':return <SilverGold {...props}/>;
  case 'roles':return <Roles {...props}/>;case 'architecture':return <Architecture {...props}/>;
  case 'progress':return <Progress {...props}/>;case 'demo':return <Demo {...props}/>;
  default:throw new Error('Missing visual: '+scene.visual);
 }
};

const TeachingScene:React.FC<{scene:Scene}>=({scene})=>{
 const frame=useCurrentFrame();const {fps}=useVideoConfig();const t=frame/fps;
 const scope=useGsapTimeline<HTMLDivElement>(({timeline:tl,selector})=>{
  tl.from(selector('[data-heading]'),{y:14,autoAlpha:0,duration:.7,stagger:.08,ease:'power2.out'},.05);
  tl.from(selector('[data-title-rule]'),{scaleX:0,transformOrigin:'left center',duration:.8,ease:'power2.inOut'},.15);
 });
 const caption=scene.captions.find(c=>t*1000>=c.startMs&&t*1000<c.endMs);
 const opacity=interpolate(frame,[scene.duration-9,scene.duration],[1,0],{extrapolateLeft:'clamp',extrapolateRight:'clamp'});
 return <AbsoluteFill ref={scope} style={{background:'white',fontFamily:'Inter',color:INK,opacity}}>
  <div data-heading style={{position:'absolute',left:100,top:48,fontSize:23,letterSpacing:1.3,color:TEAL,fontWeight:600}}>{scene.eyebrow}</div>
  <div data-heading style={{position:'absolute',left:100,top:105,right:210,fontSize:61,fontWeight:600,lineHeight:1.3,letterSpacing:-1.4}}>{scene.title}</div>
  <div style={{position:'absolute',right:100,top:117,fontSize:28,color:MUTED}}>{scene.index+1} / {timeline.scenes.length}</div>
  <div data-title-rule style={{position:'absolute',left:100,right:100,top:215,height:2,background:LINE}}/>
  <Visual scene={scene} t={t}/>
  {scene.audio.map((part,i)=><Sequence key={i} name={'Lời đọc '+(i+1)} from={part.from} durationInFrames={part.duration} layout="none"><Audio src={staticFile(part.src)}/></Sequence>)}
  <div style={{position:'absolute',left:100,right:100,bottom:95,height:1,background:LINE}}/>
  <div data-caption style={{position:'absolute',left:100,right:100,bottom:32,height:40,fontSize:30,fontWeight:400,lineHeight:'40px',whiteSpace:'nowrap',textAlign:'center',color:MUTED}}>{caption?.text??''}</div>
  <div style={{position:'absolute',left:0,right:0,bottom:0,height:5,background:WASH}}><div style={{height:5,background:TEAL,width:`${(scene.from+frame)/timeline.durationInFrames*100}%`}}/></div>
 </AbsoluteFill>;
};

export const Video:React.FC=()=> <AbsoluteFill>{timeline.scenes.map(scene=><Sequence key={scene.index} name={scene.title} from={scene.from} durationInFrames={scene.duration}><TeachingScene scene={scene}/></Sequence>)}</AbsoluteFill>;
