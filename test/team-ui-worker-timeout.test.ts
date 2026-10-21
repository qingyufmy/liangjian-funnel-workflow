import {mkdtemp,mkdir,writeFile,rm} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {basename,dirname,join,resolve} from 'node:path';
import {performance} from 'node:perf_hooks';
import {expect,test} from 'vitest';
import {TeamUiReadOnly} from '../server/team-ui-readonly.js';

test('real slow child is killed at its deadline and releases its slot',async()=>{
  const root=await mkdtemp(join(tmpdir(),'team-ui-timeout-test-'));
  try{
    await mkdir(join(root,'scripts'));
    // This throwaway worker has no database, network, model or production access.
    await writeFile(join(root,'scripts/team_ui_readonly.py'),'import time\ntime.sleep(3)\nprint("{}")\n');
    const reader=new TeamUiReadOnly(root,process.platform==='win32'?'python':'python3');
    const start=performance.now();let error:unknown;
    try{await reader.read({kind:'plans',trade_date:'2026-10-21',limit:50,offset:0,days:20});}catch(e){error=e;}
    expect(error).toMatchObject({killed:true});
    // OS/CI scheduling tolerance is not an assertion of the VM HTTP latency gate.
    expect(performance.now()-start).toBeLessThan(900);
    expect(reader.diagnostics()).toMatchObject({active:0,cacheEntries:0});
  }finally{
    if(dirname(resolve(root))!==resolve(tmpdir())||!basename(root).startsWith('team-ui-timeout-test-'))throw Error('UNSAFE_TEST_CLEANUP');
    await rm(root,{recursive:true,force:true});
  }
});
