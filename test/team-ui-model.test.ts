import {test} from 'vitest';
import assert from 'node:assert/strict';
import {emptyState,reduceEvent,validateEvent,EVENT_TYPES,addedPositions} from '../web/team/model.js';
test('unknown data stays unknown, including empty ledger and account',()=>{
  const s=emptyState();assert.equal(s.account,null);assert.equal(s.positions,null);assert.equal(s.plans,null);assert.equal(s.events,0);
});
test('work state cannot synthesize financial data or a fill',()=>{
  const s=reduceEvent(emptyState(),{type:'role',id:'tr',state:'work'});assert.equal(s.positions,null);assert.equal(s.account,null);assert.equal(s.feed.length,0);
});
test('position empty list is distinguishable from missing source',()=>{
  const s=reduceEvent(emptyState(),{type:'positions',rows:[]});assert.deepEqual(s.positions,[]);
});
test('initial ledger sync and duplicate snapshots are not new fills',()=>{
  const rows=[{code:'000001'}];assert.deepEqual(addedPositions(null,rows),[]);
  assert.deepEqual(addedPositions(rows,rows),[]);assert.deepEqual(addedPositions([],rows),rows);
});
test('all fourteen explicitly listed event types retained, no silent risk omission',()=>{
  assert.equal(EVENT_TYPES.length,14);assert.ok(EVENT_TYPES.includes('risk'));assert.ok(EVENT_TYPES.includes('theme'));
});
test('reject invalid role, state, number, conditions, clock and unknown event',()=>{
  for(const ev of[{type:'role',id:'fake',state:'work'},{type:'role',id:'tr',state:'BUY'},{type:'account',nav:NaN},{type:'conds',met:[1]},{type:'clock',hhmm:'25:10'},{type:'unknown'}])assert.throws(()=>validateEvent(ev));
});
test('conditions preserve actual count and unknown values rather than inventing seven gates',()=>{
  const s=reduceEvent(emptyState(),{type:'conds',met:[true,false,null]});assert.deepEqual(s.conds.met,[true,false,null]);
});
test('source events not mutated, account NAV is not recomputed from unproven prices',()=>{
  const ev={type:'account',nav:1000000,cash:980000};const s=reduceEvent(emptyState(),ev);assert.deepEqual(ev,{type:'account',nav:1000000,cash:980000});assert.equal(s.account.nav,1000000);assert.equal(s.account.float,undefined);
});
test('bounded feed and task identity updates',()=>{
  let s=emptyState();for(let i=0;i<510;i++)s=reduceEvent(s,{type:'say',id:'a1',text:String(i)});
  assert.equal(s.feed.length,500);s=reduceEvent(s,{type:'task',id:'close',status:'now'});s=reduceEvent(s,{type:'task',id:'close',status:'done'});assert.equal(s.tasks.length,1);assert.equal(s.tasks[0].status,'done');
});
test('explicit sourced relay is retained, without generating any financial or trading event',()=>{
  const ev={type:'relay',from:'a1',to:'a2',at:'2026-10-20T08:00:00+08:00',provenance:{id:'test-source',sha256:'a'.repeat(64)}};
  const s=reduceEvent(emptyState(),ev);assert.deepEqual(s.relay,ev);assert.equal(s.positions,null);assert.equal(s.account,null);assert.equal(s.feed.length,0);
});
