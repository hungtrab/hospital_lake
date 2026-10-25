import fs from 'node:fs/promises';
import path from 'node:path';
import {createHash} from 'node:crypto';
import {fileURLToPath} from 'node:url';
import {bundle} from '@remotion/bundler';
import {openBrowser,selectComposition,renderStill,renderMedia} from '@remotion/renderer';

const root=path.dirname(fileURLToPath(import.meta.url));
const output=path.resolve(root,'../../results/video');
const stills=process.argv.includes('--stills');
const content=await fs.readFile(path.join(root,'content.json'));
const data=JSON.parse(await fs.readFile(path.join(root,'src/timeline.json'),'utf8'));
if(data.draft)throw new Error('Narration is still a draft; run voice and alignment before rendering.');
if(data.content_digest!==createHash('sha256').update(content).digest('hex'))throw new Error('Stale timeline; run prepare.py align.');
const concurrency=Number(process.env.VIDEO_CONCURRENCY??2);
if(!Number.isInteger(concurrency)||concurrency<1||concurrency>4)throw new Error('VIDEO_CONCURRENCY must be 1..4.');
await fs.mkdir(output,{recursive:true});
const serveUrl=await bundle({entryPoint:path.join(root,'src/index.ts'),publicDir:path.join(root,'public'),outDir:path.join(output,'bundle')});
const browser=await openBrowser('chrome',{browserExecutable:process.env.VIDEO_BROWSER,logLevel:'error'});
try{
 const composition=await selectComposition({serveUrl,id:data.id,puppeteerInstance:browser,logLevel:'error'});
 if(stills){
  const folder=path.join(output,'previews');await fs.mkdir(folder,{recursive:true});
  const manifest=[];
  for(const scene of data.scenes){
   const times=[['start',1.2],...scene.marks.map((mark,i)=>['beat-'+i,mark+.8]),['end',scene.duration/data.fps-.8]];
   for(const [label,seconds] of times){
    const frame=scene.from+Math.min(scene.duration-1,Math.round(Number(seconds)*data.fps));
    const file=`${String(scene.index+1).padStart(2,'0')}-${label}.png`;
    await renderStill({serveUrl,composition,puppeteerInstance:browser,frame,output:path.join(folder,file),imageFormat:'png',logLevel:'error'});
    manifest.push({scene:scene.index+1,title:scene.title,frame,file});
   }
   console.log(`PREVIEW ${scene.index+1}/${data.scenes.length}: ${scene.title}`);
  }
  await fs.writeFile(path.join(output,'preview-manifest.json'),JSON.stringify(manifest,null,2)+'\n');
 }else{
  const files=['content.json','src/timeline.json','src/index.ts','src/Video.tsx',
   'public/fonts/Inter-Regular.otf','public/fonts/Inter-SemiBold.otf',
   ...data.scenes.flatMap(scene=>scene.audio.map(part=>'public/'+part.src))];
  const inputs={};for(const file of files)inputs[file]=createHash('sha256').update(await fs.readFile(path.join(root,file))).digest('hex');
  let previous=-1;console.log(`RENDER ${(data.durationInFrames/data.fps).toFixed(1)}s, concurrency ${concurrency}`);
  const raw=path.join(output,'hospitallake-raw.mp4');
  await renderMedia({serveUrl,composition,puppeteerInstance:browser,outputLocation:raw,codec:'h264',crf:19,
   imageFormat:'jpeg',jpegQuality:90,pixelFormat:'yuv420p',concurrency,x264Preset:'fast',
   audioCodec:'aac',audioBitrate:'160k',logLevel:'error',
   onProgress:({progress})=>{const step=Math.floor(progress*20);if(step>previous){previous=step;console.log(`RENDER ${step*5}%`);}}});
  await fs.writeFile(path.join(output,'render-manifest.json'),JSON.stringify({inputs,durationInFrames:data.durationInFrames,
   raw_sha256:createHash('sha256').update(await fs.readFile(raw)).digest('hex')},null,2)+'\n');
  console.log('RENDERED:',raw);
 }
}finally{await browser.close({silent:true});}
