"""Offline, resumable Chinese OCR for hash-verified recovered PDFs; never rewrites historical facts."""
import argparse
import csv
from datetime import datetime, timedelta
import hashlib
import io
import json
import os
import re
from pathlib import Path
import subprocess
import tempfile
from zoneinfo import ZoneInfo

import pypdfium2 as pdfium

from liangjian_funnel.data.cninfo_pdf import CninfoPdfEvidence, _build_snippets, BUSINESS_EXTRACTION_VERSION
from liangjian_funnel.pipeline.local_fact_cache import LocalFactCache
from liangjian_funnel.reporting import atomic_write_json
from liangjian_funnel.settings import Settings


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--recovery-dir', type=Path, required=True)
    parser.add_argument('--publish-current-cache', action='store_true')
    parser.add_argument('--retry-low-quality', action='store_true')
    parser.add_argument('--retry-layout', action='store_true', help='Try dense-table segmentation at 288 DPI on remaining low-quality pages')
    args = parser.parse_args()
    source = json.loads((args.recovery_dir / 'pdf-recovery.json').read_text())
    results = []
    # Limit CPU and raster memory; OCR is an offline maintenance operation.
    environment = {**os.environ, 'OMP_THREAD_LIMIT': '1'}
    for record in source['documents']:
        evidence = record['recovery']
        path = args.recovery_dir / 'pdfs' / evidence['cache_relative_path']
        if not record['same_original_bytes'] or hashlib.sha256(path.read_bytes()).hexdigest() != evidence['pdf_sha256']:
            raise ValueError('Recovered bytes differ from original evidence')
        output = args.recovery_dir / 'ocr' / (evidence['pdf_sha256'] + '.json')
        saved = json.loads(output.read_text()) if output.is_file() else {'pdf_sha256':evidence['pdf_sha256'], 'pages':[]}
        with pdfium.PdfDocument(str(path)) as doc:
            pending = list(range(len(saved['pages']), len(doc)))
            if args.retry_low_quality or args.retry_layout:
                pending = [i for i,p in enumerate(saved['pages']) if p['mean_word_confidence'] < 70] + pending
            for index in pending:
                with tempfile.TemporaryDirectory(prefix='liangjian-ocr-') as scratch:
                    image = Path(scratch) / 'page.png'
                    subprocess.run(['pdftoppm','-f',str(index+1),'-l',str(index+1),'-singlefile',
                                    '-r','288' if args.retry_layout else ('216' if args.retry_low_quality else '144'),'-png',str(path),str(image.with_suffix(''))],
                                   capture_output=True,timeout=60,check=True)
                    run = subprocess.run(['tesseract',str(image),'stdout','-l','chi_sim+eng','--psm','6' if args.retry_layout else ('1' if args.retry_low_quality else '3'),'tsv'],
                        capture_output=True,timeout=60,env=environment,check=True)
                    words = list(csv.DictReader(io.StringIO(run.stdout.decode('utf-8')),delimiter='\t'))
                    lines = {}
                    confidence = []
                    for word in words:
                        text = str(word.get('text') or '').strip()
                        if not text:
                            continue
                        key = (word['block_num'],word['par_num'],word['line_num'])
                        lines.setdefault(key,[]).append(text)
                        score = float(word.get('conf') or -1)
                        if score >= 0:
                            confidence.append(score)
                    text = '\n'.join(' '.join(v) for v in lines.values())
                    page_result = {'page_number':index+1,'text':text,
                        'mean_word_confidence':sum(confidence)/len(confidence) if confidence else 0,
                        'word_count':len(confidence), 'method':'PSM6_288DPI' if args.retry_layout else ('OSD_216DPI' if args.retry_low_quality else 'PSM3_144DPI')}
                    if index < len(saved['pages']):
                        old = saved['pages'][index]
                        if page_result['mean_word_confidence'] > old['mean_word_confidence']:
                            page_result['previous_attempt_confidence'] = old['mean_word_confidence']
                            saved['pages'][index] = page_result
                    else:
                        saved['pages'].append(page_result)
                    saved.update({'page_count':len(doc),'engine':'tesseract/5 chi_sim+eng; per-page method recorded',
                                  'completed':len(saved['pages'])==len(doc),
                                  'recovered_at':datetime.now(ZoneInfo('Asia/Shanghai')).isoformat()})
                    atomic_write_json(output,saved)
                    if (index+1) % 10 == 0 or index+1 == len(doc):
                        print(json.dumps({'symbol':record['symbol'],'id':record['announcement_id'],'pages':index+1,'total':len(doc)},ensure_ascii=False),flush=True)
        # Tesseract separates Chinese glyphs with spaces; restore words without
        # joining table numbers or changing the preserved page/quality evidence.
        for page in saved['pages']:
            page['text'] = re.sub(r'(?<=[\u3400-\u9fff])[ \t]+(?=[\u3400-\u9fff])', '', page['text'])
        atomic_write_json(output, saved)
        complete = saved['completed'] and all(p['text'] and p['mean_word_confidence'] >= 70 for p in saved['pages'])
        quality = {'page_count':len(saved['pages']), 'min_page_mean_confidence':min(p['mean_word_confidence'] for p in saved['pages']),
                   'quality_gate_passed':complete, 'manual_verified':False, 'numeric_statement_source':False,
                   'text_artifact_sha256':hashlib.sha256(output.read_bytes()).hexdigest()}
        result = {'symbol':record['symbol'],'id':record['announcement_id'],'title':record['title'],
                  'ocr_path':str(output),'quality':quality,'published_current_revision':False}
        if args.publish_current_cache and complete:
            now = datetime.now(ZoneInfo('Asia/Shanghai'))
            snippets = _build_snippets([(p['page_number'],p['text']) for p in saved['pages']])
            updated = CninfoPdfEvidence.model_validate({**evidence,'available':True,'reason_code':'OK',
                'fetched_at':now,'extraction_version':BUSINESS_EXTRACTION_VERSION,'parser':saved['engine'],
                'text_method':'OCR','ocr_quality':quality,'pages_scanned':len(saved['pages']),
                'extracted_chars':sum(len(p['text']) for p in saved['pages']),'snippets':snippets,'truncated':False,
                'prompt_injection_suspected':any(s.prompt_injection_suspected for s in snippets)})
            settings = Settings.from_env(root=Path.cwd())
            cache = LocalFactCache(settings.fact_cache_db_path)
            cache.put_cached_result('CNINFO_PDF_EVIDENCE',record['announcement_id'],updated.model_dump(mode='json'),
                                    fetched_at=now,expires_at=now+timedelta(days=365))
            result['published_current_revision'] = True
        results.append(result)
        atomic_write_json(args.recovery_dir/'ocr-acceptance.json',{'historical_mutation':False,'results':results})


if __name__ == '__main__':
    main()
