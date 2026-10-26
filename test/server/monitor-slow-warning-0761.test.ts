import { mkdtempSync, readFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { describe, expect, it } from 'vitest';
import { monitorSnapshotDelay, writeMonitorSlowWarning } from '../../server/runner.js';

describe('first real monitor slow warning', () => {
  it('is not a termination, is immutable per day, and keeps the 50s snapshot', () => {
    const root = mkdtempSync(path.join(tmpdir(), 'lj-slow-'));
    try {
      const start = new Date('2026-10-26T02:25:03Z');
      expect(() => writeMonitorSlowWarning(root, 'monitor-first', start, new Date(start.getTime() + 19_999))).toThrow();
      writeMonitorSlowWarning(root, 'monitor-first', start, new Date(start.getTime() + 20_001));
      const file = path.join(root, 'outputs/node/slow-monitor/2026-10-26.json');
      const before = readFileSync(file);
      expect(JSON.parse(before.toString())).toMatchObject({ elapsed_ms: 20_001, task_terminated: false });
      writeMonitorSlowWarning(root, 'monitor-second', start, new Date(start.getTime() + 22_000));
      expect(readFileSync(file)).toEqual(before);
      expect(monitorSnapshotDelay('monitor', 58_000)).toBe(50_000);
    } finally { rmSync(root, { recursive: true, force: true }); }
  });
});
