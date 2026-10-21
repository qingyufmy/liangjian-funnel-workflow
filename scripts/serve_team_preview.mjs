/** Loopback-only static development preview, no application imports or production reads. */
import http from 'node:http';
import {readFile,stat} from 'node:fs/promises';
import {resolve,extname,sep} from 'node:path';
import {fileURLToPath} from 'node:url';
import {loadTeamFixture} from './load_team_fixture.mjs';
import {parseMockRoute,projectMockResponse} from './team_mock_api.mjs';
const root=resolve(fileURLToPath(new URL('../web/team',import.meta.url)));
const types={'.html':'text/html; charset=utf-8','.js':'text/javascript; charset=utf-8','.css':'text/css; charset=utf-8','.json':'application/json; charset=utf-8'};
let fixturePromise;
const server=http.createServer(async(req,res)=>{
  if(req.method!=='GET'&&req.method!=='HEAD'){res.writeHead(405,{'Allow':'GET, HEAD'}).end();return;}
  const url=new URL(req.url,'http://127.0.0.1'),path=url.pathname;
  if(path==='/reference-v3'){
    const file=resolve(root,'../../.impeccable/review/team-20261021-V3/reference.html');
    try{const body=await readFile(file);res.writeHead(200,{'Content-Type':'text/html; charset=utf-8','Cache-Control':'no-store'}).end(body);}catch{res.writeHead(404).end();}return;
  }
  if(path==='/api/ui/local-today'){
    const file=resolve(root,'../../.impeccable/review/team-20261021-V3/local-today.json');
    try{const info=await stat(file);if(info.size>120000)throw Error('BOUND');res.writeHead(200,{'Content-Type':'application/json; charset=utf-8','Cache-Control':'no-store','X-Team-Data-Mode':'LOCAL_PARTIAL_20261021'}).end(await readFile(file));}catch{res.writeHead(404).end();}return;
  }
  if(path.startsWith('/api/ui/')){
    const route=parseMockRoute(path,url.searchParams);
    if(route.error){res.writeHead(route.error,{'Content-Type':'application/json'}).end(JSON.stringify({error:'LOCAL_FIXTURE_ROUTE_OR_QUERY_UNAVAILABLE'}));return;}
    try{
      fixturePromise??=loadTeamFixture(process.env.TEAM_UI_LOCAL_EVIDENCE_ROOT);
      const fixture=await fixturePromise;
      if(path==='/api/ui/events'){
        res.writeHead(200,{'Content-Type':'text/event-stream','Cache-Control':'no-store','X-Team-Data-Mode':'LOCAL_HISTORICAL_FIXTURE'});
        if(req.method==='HEAD'){res.end();return;}
        fixture.events.forEach((event,i)=>res.write(`id: local-20261020-${i}\ndata: ${JSON.stringify(event)}\n\n`));
        res.end('event: fixture-end\ndata: {}\n\n');return;
      }
      res.writeHead(200,{'Content-Type':'application/json; charset=utf-8','Cache-Control':'no-store','X-Team-Data-Mode':'LOCAL_HISTORICAL_FIXTURE'});
      res.end(req.method==='HEAD'?undefined:JSON.stringify(projectMockResponse(route,fixture)));
    }catch{fixturePromise=null;res.writeHead(503,{'Content-Type':'application/json'}).end(JSON.stringify({error:'LOCAL_PINNED_EVIDENCE_UNAVAILABLE'}));}
    return;
  }
  if(path!=='/team'&&!path.startsWith('/team/')){res.writeHead(404).end();return;}
  let suffix;try{suffix=decodeURIComponent(path.slice(5));}catch{res.writeHead(400).end();return;}if(!extname(suffix))suffix='/index.html';
  const file=resolve(root,'.'+suffix);
  if(!file.startsWith(root+sep)){res.writeHead(403).end();return;}
  try{const info=await stat(file);if(!info.isFile()||info.size>2_000_000)throw Error('NOT_STATIC_FILE');
    let body=await readFile(file);
    // Local QA only: exercise the real dependency-unavailable fallback, not a production feature.
    if(extname(file)==='.html'&&url.searchParams.get('webgl')==='off'){
      body=Buffer.from(body.toString('utf8').replace(/<script defer src="https:\/\/cdnjs\.cloudflare\.com\/ajax\/libs\/three\.js\/0\.149\.0\/three\.min\.js"><\/script>/,''));
    }
    res.writeHead(200,{'Content-Type':types[extname(file)]??'application/octet-stream','Cache-Control':'no-store','X-Content-Type-Options':'nosniff'});res.end(req.method==='HEAD'?undefined:body);
  }catch{res.writeHead(404).end();}
});
const port=Number(process.env.TEAM_PREVIEW_PORT??8787);
server.listen(port,'127.0.0.1',()=>process.stdout.write(`Local-only team preview: http://127.0.0.1:${port}/team\n`));
