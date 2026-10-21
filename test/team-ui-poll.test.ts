import {afterEach,test,expect,vi} from 'vitest';
// @ts-expect-error The static browser module deliberately stays framework-free JS.
import {createPoller} from '../web/team/poll.js';
afterEach(()=>vi.useRealTimers());
test('10s polling sends only advancing cursor; busy preserves rows and backs off20/40/60',async()=>{
  vi.useFakeTimers();const cursors:number[]=[],rows:unknown[]=[],status:string[]=[];
  const responses=[{ok:true,status:200,json:async()=>({rows:[{cursor_id:7}],next_since_id:7})},{ok:false,status:503},{ok:false,status:503},{ok:false,status:503},{ok:true,status:200,json:async()=>({rows:[],next_since_id:7})}];
  const poll=createPoller({fetchPage:async(c:number)=>{cursors.push(c);return responses.shift();},onRows:(r:unknown[])=>rows.push(...r),onStatus:(x:string)=>status.push(x),onUnauthorized:vi.fn()});
  poll.start();await vi.advanceTimersByTimeAsync(9999);expect(cursors).toEqual([]);
  await vi.advanceTimersByTimeAsync(1);expect(cursors).toEqual([0]);expect(rows).toEqual([{cursor_id:7}]);
  await vi.advanceTimersByTimeAsync(10000);expect(cursors).toEqual([0,7]);
  await vi.advanceTimersByTimeAsync(19999);expect(cursors).toHaveLength(2);await vi.advanceTimersByTimeAsync(1);
  await vi.advanceTimersByTimeAsync(40000);expect(cursors).toEqual([0,7,7,7]);
  await vi.advanceTimersByTimeAsync(60000);expect(cursors).toEqual([0,7,7,7,7]);expect(rows).toHaveLength(1);
  expect(status).toContain('稍后刷新');expect(poll.cursor()).toBe(7);poll.stop();
});
test('401 stops polling and invokes login exactly once without discarding data',async()=>{
  vi.useFakeTimers();const login=vi.fn(),onRows=vi.fn(),fetchPage=vi.fn(async()=>({ok:false,status:401}));
  const poll=createPoller({fetchPage,onRows,onStatus:vi.fn(),onUnauthorized:login});poll.start();
  await vi.advanceTimersByTimeAsync(120000);expect(login).toHaveBeenCalledTimes(1);expect(fetchPage).toHaveBeenCalledTimes(1);expect(onRows).not.toHaveBeenCalled();
});
test('late response after stop is ignored and requests never overlap',async()=>{
  vi.useFakeTimers();let finish!:(r:unknown)=>void;const onRows=vi.fn(),fetchPage=vi.fn(()=>new Promise(done=>{finish=done;}));
  const poll=createPoller({fetchPage,onRows,onStatus:vi.fn(),onUnauthorized:vi.fn()});poll.start();
  await vi.advanceTimersByTimeAsync(30000);expect(fetchPage).toHaveBeenCalledTimes(1);poll.stop();
  finish({ok:true,status:200,json:async()=>({rows:[{cursor_id:1}],next_since_id:1})});await vi.advanceTimersByTimeAsync(100000);
  expect(onRows).not.toHaveBeenCalled();expect(poll.cursor()).toBe(0);
});
