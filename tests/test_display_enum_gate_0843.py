from pathlib import Path
import importlib.util,json
from liangjian_funnel.runtime import lark_notifications as display
from liangjian_funnel.pipeline.a3_display import A3_REASON_LABELS

ROOT=Path(__file__).resolve().parents[1]
spec=importlib.util.spec_from_file_location('display_enum_ci',ROOT/'scripts/check_display_enums.py')
census=importlib.util.module_from_spec(spec);spec.loader.exec_module(census)

def test_literal_a4_producer_enums_have_chinese_display_contract():
    codes=census.producer_codes((ROOT/'src/liangjian_funnel/runtime/strategies.py').read_text('utf8'))
    known=set(display._DISPLAY_LABELS)|set(display._THEME_LABELS)|set(A3_REASON_LABELS)
    assert not codes-known,sorted(codes-known)

def test_all_47_original_missing_codes_now_map_and_future_unknown_is_visible():
    frozen=json.loads((ROOT/'tests/fixtures/postclose_20261027/missing_labels_original.json').read_bytes())
    assert len(frozen)==47
    for item in frozen:
        rendered=display._display_text(item['code'])
        assert '原码：' not in rendered and '未识别' not in rendered,(item['code'],rendered)
    assert '原码：UNREGISTERED_FUTURE_ENUM' in display._display_text('UNREGISTERED_FUTURE_ENUM')
