/** Browser-only read transport. No overlapping request and no discarded last data. */
export function createPoller({fetchPage,onRows,onStatus,onUnauthorized,onPage=()=>{},initialDelay=10000,now=Date.now,schedule=setTimeout,cancel=clearTimeout}){
  let cursor=0,timer=null,stopped=true,busy=0,pending=false,generation=0,refreshRequested=false;
  const next=delay=>{if(!stopped){if(timer!==null)cancel(timer);timer=schedule(tick,delay);}};
  async function tick(){
    timer=null;
    if(stopped)return;
    if(pending){next(1000);return;}
    const own=generation;pending=true;
    try{
      const response=await fetchPage(cursor);
      if(stopped||own!==generation)return;
      if(response.status===401){stopped=true;onUnauthorized();return;}
      if(response.status===503){busy+=1;const delay=Math.min(60000,10000*2**Math.min(busy,3));onStatus('稍后刷新',{state:'BUSY',retry_ms:delay});next(delay);return;}
      if(!response.ok)throw Error('READ_UNAVAILABLE');
      const page=await response.json();
      if(stopped||own!==generation)return;
      if(!Array.isArray(page.rows)||!Number.isSafeInteger(page.next_since_id)||page.next_since_id<cursor||page.rows.some(r=>!Number.isSafeInteger(r.cursor_id)||r.cursor_id<=cursor||r.cursor_id>page.next_since_id))throw Error('INVALID_INCREMENT');
      onPage(page);onRows(page.rows);cursor=page.next_since_id;busy=0;onStatus('只读数据已更新',{state:'CURRENT',received_at:now()});next(10000);
    }catch(error){
      // OFFLINE has a separate recovery budget: ≤5s until retry, then the two
      // HTTP reads each have a2s deadline. Normal10s and BUSY20/40/60 stay put.
      if(!stopped&&own===generation){const offline=error?.name==='TypeError';busy=offline?0:busy+1;const delay=offline?5000:Math.min(60000,10000*2**Math.min(busy,3));onStatus('稍后刷新',{state:offline?'OFFLINE':'BUSY',retry_ms:delay});next(delay);}
    }finally{pending=false;if(refreshRequested&&!stopped){refreshRequested=false;next(0);}}
  }
  return {start(){if(!stopped)return;stopped=false;generation+=1;next(initialDelay);},refresh(){if(stopped)return;if(pending){refreshRequested=true;return;}next(0);},stop(){stopped=true;generation+=1;refreshRequested=false;if(timer!==null)cancel(timer);timer=null;},cursor:()=>cursor};
}

/** Independent tiny service availability probe; never a data freshness clock.
 * 2s wait + caller's1.5s HTTP deadline detects real service loss inside5s.
 * A recovery triggers one data refresh, not a faster normal dashboard cadence.
 */
export function createHeartbeat({fetchPing,onOffline,onReachable,onUnauthorized,schedule=setTimeout,cancel=clearTimeout}){
  let stopped=true,timer=null,pending=false,generation=0,offline=false;
  const next=()=>{if(!stopped)timer=schedule(tick,2000);};
  async function tick(){
    timer=null;if(stopped)return;if(pending){next();return;}const own=generation;pending=true;
    try{
      const response=await fetchPing();if(stopped||own!==generation)return;
      if(response.status===401){stopped=true;onUnauthorized();return;}
      if(!response.ok)throw Error('PING_UNAVAILABLE');
      const body=await response.json();if(stopped||own!==generation)return;
      if(body?.schema!=='team-ui-ping/1'||body.reachable!==true)throw Error('PING_SOURCE_INVALID');
      const recovered=offline;offline=false;onReachable({recovered});
    }catch{
      if(!stopped&&own===generation&&!offline){offline=true;onOffline();}
    }finally{pending=false;if(!stopped&&own===generation)next();}
  }
  return {start(){if(!stopped)return;stopped=false;generation+=1;timer=schedule(tick,0);},probe(){if(stopped||pending)return;if(timer!==null)cancel(timer);timer=schedule(tick,0);},stop(){stopped=true;generation+=1;if(timer!==null)cancel(timer);timer=null;}};
}

/** Transport freshness, independent of whether any new business event occurred. */
export function freshness(lastSuccess,now=Date.now()){
  return lastSuccess==null?'UNKNOWN':now-lastSuccess>=30000?'STALE':'CURRENT';
}
