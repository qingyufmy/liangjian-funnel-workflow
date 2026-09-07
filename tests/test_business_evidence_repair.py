from datetime import datetime
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest

from liangjian_funnel.data.business_disclosure import business_disclosure_kind
from liangjian_funnel.data.cninfo_pdf import CninfoPdfEvidence, PdfEvidenceSnippet, MAX_PDF_BYTES
from liangjian_funnel.facts.cninfo import compact_cninfo_pdf_evidence, _pdf_payload, is_full_periodic_report
from liangjian_funnel.workflow import WorkflowApplication, _main_business_evidence

NOW = datetime(2026, 9, 7, 8, 0, tzinfo=ZoneInfo('Asia/Shanghai'))


@pytest.mark.parametrize('text,expected', [
    ('分产品 营业收入 营业成本 毛利率 工业设备 100万元 70万元 30%', 'SEGMENT_REVENUE_DISCLOSURE'),
    ('占比10%以上的产品或服务情况 云计算业务 营业收入40,808,411.64 网络游戏业务12,983,732.66', 'SEGMENT_REVENUE_DISCLOSURE'),
    ('公司主要从事精密光学仪器的研发、生产和销售。', 'COMPANY_BUSINESS_DESCRIPTION'),
    ('主要财务数据变动 营业收入100万元 营业成本70万元 毛利率30%', None),
    ('主营业务分产品 营业收入 毛利率', None),
    ('公司主营业务未发生变化，详见2025年年度报告。', None),
    ('目录 主营业务分行业 12 营业收入 13', None),
    ('公司主要从事的主要业务，请参见年度报告。', None),
    ('若供应商无法向公司提供服务，公司经营活动可能受到干扰。', None),
    ('本公司为中国海上原油及天然气生产商，主要业务为勘探、开发、生产及销售原油和天然气。', 'COMPANY_BUSINESS_DESCRIPTION'),
    ('本公司向客户提供批发及零售银行产品和服务。', 'COMPANY_BUSINESS_DESCRIPTION'),
    ('数字支付业务收入为28.43亿元，同比增长20.32%。', 'BUSINESS_REVENUE_NARRATIVE'),
    ('主营业务收入为28.43亿元。', None),
    ('利息净收入除以生息资产平均余额的比率；不良贷款余额除以客户贷款总额。', None),
    ('业务分部收入29.17%，详细数据请参阅财务报告附注46。', None),
])
def test_business_evidence_requires_business_not_just_total_income(text, expected):
    assert business_disclosure_kind(text) == expected


def test_full_cache_business_survives_four_snippet_projection_without_redownload(tmp_path):
    text = '分产品 营业收入 营业成本 毛利率 工业设备 100万元 70万元 30%'
    snippets = tuple(PdfEvidenceSnippet(page_number=i+1, text='合同担保金额 诉讼风险 100万元',
                     matched_keywords=('合同','担保','金额','诉讼','风险')) for i in range(11))
    evidence = CninfoPdfEvidence(announcement_id='ann', pdf_url='https://static.cninfo.com.cn/finalpage/test.pdf',
        available=True, reason_code='OK', fetched_at=NOW, pdf_sha256='a'*64, cache_relative_path='raw/test.pdf',
        page_count=20, extracted_chars=1000, snippets=snippets+(PdfEvidenceSnippet(page_number=20,text=text),))
    compact = compact_cninfo_pdf_evidence(evidence)
    assert len(compact.snippets) == 4 and any(x.text == text for x in compact.snippets)
    assert len(evidence.snippets) == 12
    ann = SimpleNamespace(announcement_id='ann', pdf_url=evidence.pdf_url, sec_code='600001')
    payload = evidence.model_dump(mode='json'); payload.pop('extraction_version')
    app = object.__new__(WorkflowApplication)
    app.settings = SimpleNamespace(cninfo_pdf_cache_dir=tmp_path, cninfo_pdf_retain_raw=False)
    cached = app._cached_cninfo_pdf_evidence_from_record(ann, {'payload':payload})
    assert cached is not None and cached.cache_hit and cached.extraction_version == 'legacy'
    row = {**_pdf_payload(compact), 'announcement_id':'ann', 'content_hash':'a'*64}
    assert _main_business_evidence({'by_symbol':{'600001.SH':[row]}},['600001.SH'])['600001.SH']['available']


@pytest.mark.parametrize('title,expected', [
    ('工商银行2026半年度报告', True),
    ('603660_2026年_半年度报告（全文)', True),
    ('2026年半年度报告（更正后）', True),
    ('2026年半年度报告摘要', False),
    ('关于2026年半年度报告的更正公告', False),
    ('2026年半年度募集资金存放与使用情况专项报告', False),
    ('吉林泉阳泉股份有限公司《2026年半年度报告》', True),
])
def test_periodic_titles_do_not_require_one_vendor_spelling(title, expected):
    assert is_full_periodic_report(title) is expected


def test_old_size_failure_retries_only_when_download_limit_increases():
    app = object.__new__(WorkflowApplication)
    evidence = CninfoPdfEvidence(announcement_id='ann', pdf_url='https://static.cninfo.com.cn/finalpage/test.pdf',
        available=False, reason_code='CNINFO_PDF_TOO_LARGE', fetched_at=NOW)
    ann = SimpleNamespace(announcement_id='ann', pdf_url=evidence.pdf_url)
    payload = evidence.model_dump(mode='json'); payload.pop('download_limit_bytes')
    assert app._cached_cninfo_pdf_evidence_from_record(ann, {'payload':payload}) is None
    payload['download_limit_bytes'] = MAX_PDF_BYTES
    assert app._cached_cninfo_pdf_evidence_from_record(ann, {'payload':payload}).reason_code == 'CNINFO_PDF_TOO_LARGE'


@pytest.mark.parametrize('title,expected', [
    ('公司首次公开发行股票并上市招股说明书', True),
    ('招股说明书（注册稿）', False), ('招股说明书摘要', False),
    ('关于招股说明书的审核问询回复', False),
])
def test_only_final_prospectus_is_business_candidate(title, expected):
    from liangjian_funnel.facts.cninfo import is_final_prospectus
    assert is_final_prospectus(title) is expected


def test_bse_official_upload_path_is_narrowly_supported():
    from liangjian_funnel.data.bse import _announcement_url, BseContractError
    path = '/uploads/6/file/public/202609/20260903180311_db3p3vkjf7.pdf'
    assert _announcement_url(path) == 'https://www.bse.cn' + path
    for invalid in ('https://evil.example'+path, '/uploads/private/test.pdf',
                    path+'?token=1', '/uploads/6/file/public/202609/../test.pdf'):
        with pytest.raises(BseContractError):
            _announcement_url(invalid)


def test_empty_user_password_pdf_is_readable_but_password_protection_is_not(tmp_path):
    from pypdf import PdfWriter
    from liangjian_funnel.data.cninfo_pdf import _pdfium_business_text
    # Exercise actual native PDFium API/resource closure, not only a stub.
    path = tmp_path/'public.pdf'
    writer = PdfWriter(); writer.add_blank_page(width=100, height=100)
    writer.encrypt('', owner_password='owner', algorithm='AES-256')
    writer.write(path)
    result = _pdfium_business_text(path)
    assert result and result[1] == 1 and result[2] == 1
    assert result[-1].startswith('pypdfium2/')


def test_periodic_business_proof_precedes_newer_project_event_and_rejects_injection():
    text = '公司主要从事精密光学仪器的研发、生产和销售。'
    def row(aid, title, date, suspected=False):
        return {'announcement_id': aid, 'announcement_title': title, 'publish_time': date,
            'pdf_evidence_available': True, 'pdf_sha256': 'b'*64, 'content_hash': 'a'*64,
            'pdf_evidence_snippets': [{'page_number': 1, 'text': text, 'prompt_injection_suspected': suspected}]}
    result = _main_business_evidence({'by_symbol': {'600001.SH': [
        row('annual', '2026年半年度报告', '2026-08-28'),
        row('event', '关于投资项目的公告', '2026-09-02'),
        row('injected', '2026年半年度报告', '2026-09-03', True),
    ]}}, ['600001.SH'])['600001.SH']
    assert result['evidence'][0]['announcement_id'] == 'annual'
    assert result['evidence'][0]['content_hash'] == 'b'*64
    assert all(p['announcement_id'] != 'injected' for p in result['evidence'])


def test_ipo_query_fallback_is_cached_and_periodic_reports_skip_extra_queries(tmp_path):
    from liangjian_funnel.data.cninfo import CninfoAnnouncement, CninfoFetchResult
    from liangjian_funnel.pipeline.local_fact_cache import LocalFactCache
    class Client:
        def __init__(self, title, keyword):
            self.title, self.keyword, self.calls = title, keyword, []
        def fetch_announcements(self, symbol, start_date, end_date, *, search_keyword=''):
            self.calls.append(search_keyword)
            anns = () if search_keyword != self.keyword else (CninfoAnnouncement(
                announcement_id='ann', sec_code='600001', sec_name='测试公司',
                announcement_title=self.title, adjunct_url='https://static.cninfo.com.cn/finalpage/test.pdf', publish_time=NOW),)
            return CninfoFetchResult(symbol=symbol, start_date=start_date, end_date=end_date,
                ok=True, complete=True, reason_code='OK', announcements=anns,
                fetched_at=datetime.now(ZoneInfo('Asia/Shanghai')))
    for index, (title, keyword, expected) in enumerate([
        ('2026年半年度报告', '年度报告', ['', '年度报告']),
        ('首次公开发行股票招股说明书', '招股说明书', ['', '年度报告', '报告', '招股说明书']),
    ]):
        app = object.__new__(WorkflowApplication)
        app.fact_cache = LocalFactCache(tmp_path/f'{index}.sqlite3')
        client = Client(title, keyword)
        args = (client, '600001.SH', '2026-08-28', '2026-09-07', '2025-06-01')
        result = app._fetch_cninfo_candidate_queries(*args)
        assert result[3].announcements[0].announcement_title == title
        assert client.calls == expected
        app._fetch_cninfo_candidate_queries(*args)
        assert client.calls == expected


def test_production_fallback_keeps_risk_document_and_stops_after_first_proof(tmp_path, monkeypatch):
    from liangjian_funnel.data.cninfo import CninfoAnnouncement, CninfoFetchResult
    from liangjian_funnel.runtime.progress import WorkflowProgress
    from datetime import timedelta
    def ann(aid, title, days):
        return CninfoAnnouncement(announcement_id=aid, sec_code='600001', sec_name='测试公司',
            announcement_title=title, adjunct_url=f'https://static.cninfo.com.cn/finalpage/{aid}.pdf', publish_time=NOW-timedelta(days=days))
    newest, older, oldest, risk = (ann('new','2026年半年度报告',0), ann('old','2025年年度报告',120),
        ann('oldest','2025年半年度报告',365), ann('risk','重大诉讼公告',0))
    def proof(a, text):
        return CninfoPdfEvidence(announcement_id=a.announcement_id, pdf_url=a.pdf_url, available=True,
            reason_code='OK', fetched_at=NOW, pdf_sha256='a'*64, cache_relative_path='raw/a.pdf',
            page_count=1, extracted_chars=len(text), snippets=(PdfEvidenceSnippet(page_number=1,text=text),))
    evidence = {'new': proof(newest, '公司治理及一般财务数据'), 'risk': proof(risk, '重大诉讼事项')}
    ids = {'600001.SH':['new','risk']}
    result = CninfoFetchResult(symbol='600001.SH', start_date='2025-01-01', end_date='2026-09-07',
        ok=True, complete=True, reason_code='OK', announcements=(newest,older,oldest,risk), fetched_at=NOW)
    app = object.__new__(WorkflowApplication)
    app.settings = SimpleNamespace(cninfo_pdf_cache_dir=tmp_path, timeout_seconds=1, cninfo_pdf_workers=2)
    calls = []
    def cached(_client, a):
        calls.append(a.announcement_id)
        return proof(a, '公司主要从事精密光学仪器的研发、生产和销售。')
    monkeypatch.setattr(app, '_cached_cninfo_pdf_evidence', cached)
    progress = WorkflowProgress(tmp_path/'progress.json',run_id='test',job='prepare')
    app._supplement_cninfo_business_evidence({'600001.SH':result}, evidence, ids, {}, progress=progress)
    assert calls == ['old'] and ids['600001.SH'] == ['new','risk','old']
    assert progress.snapshot()['data']['processed'] == progress.snapshot()['data']['total'] == 3
    app._supplement_cninfo_business_evidence({'600001.SH':result}, evidence, ids, {})
    assert calls == ['old']
