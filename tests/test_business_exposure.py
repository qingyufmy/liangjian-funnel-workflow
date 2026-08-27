from liangjian_funnel.pipeline.business_exposure import extract_business_exposure_facts


def test_business_exposure_extracts_only_explicit_percentages_with_evidence():
    facts = extract_business_exposure_facts({
        "600000.SH": {
            "available": True,
            "evidence": [{
                "source_ref": "cninfo:ann-1:page:8",
                "page_number": 8,
                "publish_time": "2026-04-30T09:00:00+08:00",
                "text": "报告期内，算力设备业务占公司营业收入65.5%。其他业务保持稳定。",
            }],
        },
        "000001.SZ": {
            "available": True,
            "evidence": [{
                "source_ref": "cninfo:ann-2:page:2",
                "text": "公司持续发展数字化业务，但未披露收入占比。",
            }],
        },
    })

    assert len(facts) == 1
    assert facts[0]["symbol"] == "600000.SH"
    assert facts[0]["revenue_exposure_pct"] == 65.5
    assert facts[0]["page_number"] == 8
    assert facts[0]["evidence_ref"] == "cninfo:ann-1:page:8"


def test_business_exposure_rejects_out_of_range_and_missing_source():
    facts = extract_business_exposure_facts({
        "600000.SH": {
            "evidence": [
                {"source_ref": "cninfo:bad", "text": "算力业务占公司营业收入165%"},
                {"source_ref": "cninfo:threshold", "text": "算力业务占公司营业收入30%以上"},
                {"text": "算力业务占公司营业收入65%"},
            ],
        },
    })
    assert facts == []


def test_business_exposure_extracts_flattened_revenue_composition_table():
    facts = extract_business_exposure_facts({
        "000757.SZ": {
            "available": True,
            "evidence": [{
                "source_ref": "cninfo:1225486185:page:23",
                "page_number": 23,
                "publish_time": "2026-08-21T00:00:00+08:00",
                "text": (
                    "营业收入构成 单位：元 本报告期 上年同期 同比增减 "
                    "分行业 制造业 357,026,405.88 35.41% 411,348,709.76 28.23% -13.21% "
                    "汽车服务 645,948,836.69 64.07% 1,036,911,485.11 71.15% -37.70% "
                    "分产品 机械配件 357,026,405.88 35.41% 411,348,709.76 28.23% -13.21% "
                    "分地区 西南地区 43,975,485.05 4.36%"
                ),
            }],
        },
    })

    by_name = {item["business_name"]: item for item in facts}
    assert by_name["制造业"]["revenue_exposure_pct"] == 35.41
    assert by_name["汽车服务"]["revenue_exposure_pct"] == 64.07
    assert by_name["机械配件"]["extraction_method"] == "REVENUE_COMPOSITION_TABLE_分产品"
    assert "西南地区" not in by_name
