/** Browser-only read transport. No overlapping request and no discarded last data. */
export function createPoller({fetchPage,onRows,onStatus,onUnauthorized,schedule=setTimeout,cancel=clearTimeout}){
  let cursor=0,timer=null,stopped=true,busy=0,pending=false,generation=0;
  const next=delay=>{if(!stopped)timer=schedule(tick,delay);};
  async function tick(){
    if(stopped||pending)return;
    const own=generation;pending=true;
    try{
      const response=await fetchPage(cursor);
      if(stopped||own!==generation)return;
      if(response.status===401){stopped=true;onUnauthorized();return;}
      if(response.status===503){busy+=1;const delay=Math.min(60000,10000*2**Math.min(busy,3));onStatus('稍后刷新');next(delay);return;}
      if(!response.ok)throw Error('READ_UNAVAILABLE');
      const page=await response.json();
      if(stopped||own!==generation)return;
      if(!Array.isArray(page.rows)||!Number.isSafeInteger(page.next_since_id)||page.next_since_id<cursor||page.rows.some(r=>!Number.isSafeInteger(r.cursor_id)||r.cursor_id<=cursor||r.cursor_id>page.next_since_id))throw Error('INVALID_INCREMENT');
      onRows(page.rows);cursor=page.next_since_id;busy=0;onStatus('只读数据已更新');next(10000);
    }catch{
      if(!stopped&&own===generation){busy+=1;onStatus('稍后刷新');next(Math.min(60000,10000*2**Math.min(busy,3)));}
    }finally{pending=false;}
  }
  return {start(){if(!stopped)return;stopped=false;generation+=1;next(10000);},stop(){stopped=true;generation+=1;if(timer!==null)cancel(timer);timer=null;},cursor:()=>cursor};
}
