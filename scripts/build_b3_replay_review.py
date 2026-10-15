"""Seal an early partial-day B3 replay review; never a release approval."""
from pathlib import Path
from datetime import datetime
from zoneinfo import ZoneInfo
import hashlib
import json
import subprocess
import zipfile


def main():
    root = Path(__file__).resolve().parents[1]
    out = root / 'artifacts/replay-20261015'
    source = out / 'variants-stream-final-v3'
    result = json.loads((source / 'summary.json').read_text(encoding='utf-8'))
    now = datetime.now(ZoneInfo('Asia/Shanghai'))
    assert result['V0_diff_count'] == 0 and result['V0_evaluated_window_count'] == 21165
    lookup = {(row['trade_date'], row['variant']): row for row in result['days']}
    lines = ['# B3六日回放：午间源的第一版结果', '',
        '实际封包时刻（Asia/Shanghai）：' + now.isoformat(), '',
        '这是午间截面的只读回放评审，不是完整10-15数据，也不是生产候选发布批准。收盘后须新建完整源与最终结果，原件不覆盖。', '',
        '## 技术候选计划数（不是成交数）', '',
        '| 日期 | V0 | V1 | V2 | V3 | V4 | V5 |', '| --- | --- | --- | --- | --- | --- | --- |']
    for day in sorted({key[0] for key in lookup}):
        values = []
        for variant in ('V0','V1','V2','V3','V4','V5'):
            row = lookup[(day, variant)]
            values.append('NO_PLAN' if row.get('evidence_status') == 'NO_PLAN_NOT_TESTED' else
                ('DATA_LIMITED' if variant in ('V3','V4','V5') else str(row['first_technical_candidate_plan_count'])))
        lines.append('| ' + day + ('（午间）' if day == '2026-10-15' else '') + ' | ' + ' | '.join(values) + ' |')
    lines += ['', '## 已证与未证', '',
        '- V0共21165窗口：kernel与有原证据的Monitor最终层均0差异；257条原输入阻断保留；245条非策略记录另计。',
        '- 原适用分钟缺口242：10-08为95、10-12为144、10-14为3。不能用现有窗口0差异宣称六日执行无缺口。',
        '- V1仅新增603087.SH在10-08 11:08的技术候选，参考75.05；V2没有相对V0新增候选。原失效之后是右删失，不能据此推断延后失效的全天没有机会。',
        '- 10-13的301152.SZ为基线已有技术候选，13:26原模型明确VETO；后四分钟为真实episode去重，不是成交或新变体通过。',
        '- V3/V4/V5严格缺09:25原竞价价，DATA_LIMITED不是0机会。80份原报价均09:26/09:27 POST_AUCTION，不能冒充09:25。',
        '- ATR从有版本的缓存可研究派生，但有限历史seed与完整A3历史不等价；Wilder与末14TR均值分开标注。',
        '- 严格T+1/T+3均为NULL：原PIT身份未完整封存、部分目标日未成熟。技术候选没有新模型审核、组合风险许可、订单、成交或费用证明。',
        '- 独立UNVERIFIED_SYMBOL_REFERENCE_SENSITIVITY：603087原symbol收盘T+1约+0.626%、T+3约-4.064%，观察MAE约-2.452%；301152原symbol T+1约-5.685%。它们不进入策略胜率门，不能据此批准V1放宽。',
        '- 601877在10-13 10:16可有7叶门满足，但盈亏比/geometry仍不合格；max=7不等于漏买点，诊断保留该失败。',
        '- 没有每变体各一盈利与一亏损的可证成交样本，不制造第二样本；此项验收待证据。', '',
        '## 本轮建议', '',
        '保持15m压力模式与固定入场带；V1胜率门尚不具备分母。V2默认两根永久失效语义须先明确5m独立分支是否仍有效。竞价重锚先解决真实09:25价及ATR基准合同，不能以竞价后价冒充。A3簇分散只能限制相关暴露，不会凭空产生三个行业或五条计划。', '',
        '声明式反例及逐计划max/失效/首时刻见plan_metrics.jsonl、tests/test_b3_research_variants.py与tests/test_b3_variant_replay.py。', '',
        'CODE：切片及初轮全量通过，最终HEAD回执未出。REPLAY：现有窗口一致，10-15部分、历史运营缺口、竞价与收益缺口明确。OPERATIONS：未部署。STRATEGY：未证有效，不改生产参数。']
    summary = out / 'REPLAY_SUMMARY.md'
    if summary.exists(): raise FileExistsError(summary)
    summary.write_text('\n'.join(lines) + '\n', encoding='utf-8')
    selected = [summary, source/'summary.json', source/'plan_metrics.jsonl', source/'RUN_RECEIPT.json',
        out/'v0-coverage-1230/summary.json', out/'auction-evidence/AUCTION_INPUT_COVERAGE.md',
        root/'docs/B3_V0_REPLAY_2026-10-15.md', root/'docs/B3_VARIANT_REPLAY_RUNNER_2026-10-15.md',
        root/'docs/B3_I6_NETWORK_FLOOR_2026-10-15.md']
    for name in ('b3_baseline','b3_variants','b3_variant_replay'):
        selected.append(root/f'src/liangjian_funnel/evaluation/{name}.py')
    for name in ('b3_baseline','b3_research_variants','b3_variant_replay'):
        selected.append(root/f'tests/test_{name}.py')
    archive = out / ('B3_REPLAY_PARTIAL_' + now.strftime('%Y%m%dT%H%M%S') + '.zip')
    files = []
    with zipfile.ZipFile(archive, 'x', zipfile.ZIP_DEFLATED) as zipped:
        for path in selected:
            relative = path.relative_to(root).as_posix()
            raw = path.read_bytes(); files.append({'path':relative,'sha256':hashlib.sha256(raw).hexdigest()})
            zipped.writestr(relative,raw)
    proof = {'actual_at':now.isoformat(),'head':subprocess.check_output(['git','rev-parse','HEAD'],cwd=root,text=True).strip(),
        'status':'PARTIAL_REPLAY_REVIEW_NOT_RELEASE_CANDIDATE', 'files':files,
        'archive':str(archive), 'archive_sha256':hashlib.sha256(archive.read_bytes()).hexdigest(),
        'source_comparisons_path':str(source/'comparisons.jsonl'),
        'source_comparisons_sha256':hashlib.sha256((source/'comparisons.jsonl').read_bytes()).hexdigest(),
        'production_writes':0,'push':False,'deploy':False}
    (out/'PARTIAL_REVIEW_MANIFEST.json').write_text(json.dumps(proof,ensure_ascii=False,indent=2),encoding='utf-8')
    delivery = out/'PARTIAL_REVIEW_DELIVERY.md'
    delivery.write_text('实际封包时刻（Asia/Shanghai）：'+now.isoformat()+'\n\n午间源早期回放评审，不是发布候选或发布批准。\n\n'
        + f"评审包：`{archive}`\n\nSHA：`{proof['archive_sha256']}`\n\n说明：`{summary}`\n\n"
        + '请核实0152的V2/V3/V4精确定义以及严格竞价/收益缺口；本轮不放宽压力门，不将0基线收益设置为0胜率。工程、收盘观察与下午完整回放继续。\n',encoding='utf-8')
    print(json.dumps({'archive':str(archive),'sha256':proof['archive_sha256'],'delivery':str(delivery)},ensure_ascii=True))


if __name__ == '__main__':
    main()
