import {readFileSync} from 'node:fs';
import {test,expect} from 'vitest';
// @ts-expect-error Framework-free browser copy module.
import {REASONS,reasonText,conditionName,translateReasonMessage} from '../web/team/copy/reasons.js';
const fixture=JSON.parse(readFileSync(new URL('./fixtures/team-ui-reasons-20261021.json',import.meta.url),'utf8'));
test('every captured reason/condition through14:30 has Chinese explanation, never echoes a raw unknown',()=>{
  for(const code of fixture.codes){const translated=reasonText(code);expect(translated,code).not.toContain('尚无核实过');expect(translated,code).not.toContain(code);expect(translated,code).toMatch(/[\u4e00-\u9fff]/);}
  expect(Object.isFrozen(REASONS)).toBe(true);expect(reasonText('UNSEEN_NEW_REASON')).not.toContain('UNSEEN_NEW_REASON');
});
test('translations preserve safety meaning rather than inventing cause, permission or outcome',()=>{
  expect(reasonText('ROUND_LOT_UNAVAILABLE')).toContain('价位和限额');
  expect(reasonText('START_CONFIRMATION')).toContain('暂缓');
  expect(reasonText('A2_UNAVAILABLE')).toContain('降级');
  expect(reasonText('INTERNAL_DELIVERY_UNCONFIRMED')).toContain('未确认送达');
  expect(reasonText('TEAM_MONITOR_DEGRADED')).toContain('以账户锁原件为准');
  expect(reasonText('FILLED')).toContain('不代表真实券商');
});
test('indexed candle conditions use stable Chinese labels; source objects and raw codes remain unchanged',()=>{
  const source={reason_codes:['C_PRICE_REPAIR_44','A_STABILIZED']};const before=JSON.stringify(source);
  expect(conditionName('C_PRICE_REPAIR_44')).toBe('价格修复');expect(conditionName('A_HIGHER_LOWS')).toBe('低点抬高');
  expect(translateReasonMessage(source.reason_codes.join(' / '))).not.toMatch(/C_PRICE_REPAIR|A_STABILIZED/);
  expect(JSON.stringify(source)).toBe(before);
});
