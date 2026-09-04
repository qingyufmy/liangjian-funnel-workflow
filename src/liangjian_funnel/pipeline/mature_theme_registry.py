"""Stable A1 theme families and point-in-time taxonomy resolution.

The monthly research model may activate a theme, but it must not be the
authority for changing the meaning of that theme.  This module provides the
small, versioned registry that owns that meaning.  Industry and concept codes
are deliberately resolved from the frozen THS catalogs at run time; the
registry stores names and never stores a stock list.

The three public functions are pure transformations:

``resolve_mature_theme_registry``
    Resolve exact Chinese names against a current catalog and keep unknown
    names visible without turning them into taxonomy links.

``activate_mature_themes``
    Find deterministic keyword evidence in model/broker/monthly text.  It
    returns the evidence used for every activation and has no stock-selection
    side effect.

``augment_discovery_with_mature_registry``
    Append canonical themes, a traceable core node and validated taxonomy
    links to an A1 discovery response.  Existing model rows are never replaced
    or rewritten, and no company or financial fact is manufactured.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from copy import deepcopy
import hashlib
import json
import re
import unicodedata
from typing import Any


MODULE_VERSION = "mature-theme-registry/1.0.0"
REGISTRY_VERSION = "mature-theme-registry/2026.09.v1"
_MAX_EVIDENCE_EXCERPT = 320
_MAX_EVIDENCE_ROWS_PER_THEME = 24
_MAX_SOURCE_REFS = 24
_REF_KEYS = frozenset(
    {
        "source_ref",
        "source_refs",
        "source_url",
        "source_urls",
        "fact_id",
        "document_id",
        "evidence_id",
        "ref",
        "refs",
    }
)
_NON_TEXT_KEYS = frozenset(
    {
        "canonical_id",
        "theme_id",
        "node_id",
        "thscode",
        "taxonomy_code",
        "industry_thscode",
        "concept_thscode",
        "code",
        "rank",
        "score",
        "confidence",
        "value",
        "count",
    }
)


# Names below are the exact names present in the current THS catalog used by
# the A1 snapshots (industry catalog 320 rows, concept catalog 390 rows).  A
# future catalog revision can make a name unresolved; that is intentional and
# is reported by ``resolve_mature_theme_registry`` rather than fuzzy-matched.
_DEFAULT_THEMES: tuple[dict[str, Any], ...] = (
    {
        "canonical_id": "AI_COMPUTE_INFRASTRUCTURE",
        "display_name": "AI算力基础设施",
        "activation_keywords": (
            "AI算力", "算力基础设施", "数据中心", "液冷", "CPO", "光模块", "光纤", "铜缆", "PCB", "通信设备",
        ),
        "industry_names": (
            "计算机设备", "通信设备", "通信服务", "通信网络设备及器件", "通信线缆及配套", "通信终端及配件",
            "元件", "印制电路板", "光学光电子",
        ),
        "concept_names": (
            "数据中心(AIDC)", "东数西算(算力)", "液冷服务器", "算力租赁", "共封装光学(CPO)", "光纤概念",
            "铜缆高速连接", "PCB概念", "云计算", "人工智能", "英伟达概念",
        ),
    },
    {
        "canonical_id": "SEMICONDUCTOR_LOCALIZATION",
        "display_name": "半导体国产化",
        "activation_keywords": (
            "半导体", "芯片国产化", "国产替代", "集成电路", "先进封装", "存储芯片", "光刻胶", "光刻机",
        ),
        "industry_names": (
            "半导体", "半导体材料", "半导体设备", "集成电路制造", "集成电路封测", "数字芯片设计",
            "模拟芯片设计", "电子化学品", "光学光电子",
        ),
        "concept_names": (
            "芯片概念", "存储芯片", "MCU芯片", "第三代半导体", "先进封装", "光刻胶", "光刻机",
            "中芯国际概念", "汽车芯片", "华为海思概念股", "国产操作系统",
        ),
    },
    {
        "canonical_id": "NATIONAL_DEFENSE",
        "display_name": "国防军工",
        "activation_keywords": (
            "国防军工", "军工", "航空装备", "航天装备", "军工电子", "商业航天", "大飞机", "低空经济",
        ),
        "industry_names": (
            "军工装备", "航天装备", "航空装备", "地面兵装", "航海装备", "军工电子", "其他运输设备",
        ),
        "concept_names": (
            "军工", "大飞机", "海工装备", "国产航母", "无人机", "卫星导航", "航空发动机", "军民融合",
            "成飞概念", "商业航天", "低空经济", "军工信息化",
        ),
    },
    {
        "canonical_id": "CONSUMER_ELECTRONICS",
        "display_name": "消费电子",
        "activation_keywords": (
            "消费电子", "手机产业链", "AI手机", "AI眼镜", "折叠屏", "智能穿戴", "苹果产业链", "智能硬件",
        ),
        "industry_names": (
            "消费电子", "消费电子零部件及组装", "品牌消费电子", "光学光电子", "面板", "LED",
            "光学元件", "家电零部件",
        ),
        "concept_names": (
            "消费电子概念", "苹果概念", "无线充电", "智能穿戴", "无线耳机", "柔性屏(折叠屏)",
            "电子纸", "OLED", "AI手机", "AI眼镜", "华为手机", "小米概念", "富士康概念",
        ),
    },
    {
        "canonical_id": "ROBOTICS_ADVANCED_MANUFACTURING",
        "display_name": "机器人与先进制造",
        "activation_keywords": (
            "机器人", "人形机器人", "先进制造", "工业母机", "高端装备", "自动化", "减速器", "机器视觉",
        ),
        "industry_names": (
            "自动化设备", "机器人", "工控设备", "专用设备", "通用设备", "机床工具", "仪器仪表", "工程机械",
            "工程机械", "激光设备",
        ),
        "concept_names": (
            "机器人概念", "人形机器人", "工业母机", "高端装备", "减速器", "机器视觉", "3D打印",
            "工业互联网", "新型工业化", "传感器",
        ),
    },
    {
        "canonical_id": "POWER_EQUIPMENT_NEW_ENERGY",
        "display_name": "电力设备与新能源",
        "activation_keywords": (
            "电力设备", "新能源", "光伏", "风电", "储能", "电网", "特高压", "固态电池", "锂电池",
        ),
        "industry_names": (
            "电力", "电网设备", "电气自控设备", "输变电设备", "其他电源设备", "光伏设备", "风电设备",
            "电池", "新能源发电", "电能综合服务",
        ),
        "concept_names": (
            "新能源汽车", "风电", "核电", "燃料电池", "特高压", "智能电网", "锂电池概念", "充电桩",
            "储能", "固态电池", "钠离子电池", "光伏概念", "光伏建筑一体化", "虚拟电厂", "绿色电力",
            "电力物联网", "超超临界发电",
        ),
    },
    {
        "canonical_id": "RESOURCES_ENERGY",
        "display_name": "资源能源",
        "activation_keywords": (
            "资源能源", "煤炭", "石油", "天然气", "有色金属", "黄金", "白银", "铜", "铝", "锂资源",
        ),
        "industry_names": (
            "煤炭开采加工", "油气开采及服务", "石油加工贸易", "工业金属", "贵金属", "小金属", "能源金属",
            "钢铁", "港口航运",
        ),
        "concept_names": (
            "煤炭概念", "煤化工概念", "黄金概念", "金属铜", "金属镍", "金属钴", "金属锌", "小金属概念",
            "稀土永磁", "盐湖提锂", "可燃冰", "页岩气", "天然气", "航运概念",
        ),
    },
    {
        "canonical_id": "CHEMICAL_NEW_MATERIALS",
        "display_name": "化工新材料",
        "activation_keywords": (
            "化工新材料", "新材料", "基础化工", "电子化学品", "氟化工", "磷化工", "有机硅", "高分子材料",
        ),
        "industry_names": (
            "化学原料", "化学制品", "电子化学品", "非金属材料", "金属新材料", "塑料制品", "橡胶制品",
            "化学纤维", "农化制品", "合成树脂", "膜材料",
        ),
        "concept_names": (
            "煤化工概念", "氟化工概念", "磷化工", "钛白粉概念", "碳纤维", "有机硅概念", "可降解塑料",
            "石墨烯", "光刻胶", "PVDF概念", "PEEK材料", "合成生物", "维生素", "丙烯酸", "化肥",
        ),
    },
    {
        "canonical_id": "INNOVATIVE_MEDICINE_HEALTHCARE",
        "display_name": "创新药与医疗",
        "activation_keywords": (
            "创新药", "医药", "医疗", "生物制品", "CXO", "CRO", "脑机接口", "合成生物", "中报增长",
        ),
        "industry_names": (
            "化学制药", "中药", "生物制品", "医药商业", "医疗器械", "医疗服务", "医疗研发外包",
            "医疗设备", "体外诊断",
        ),
        "concept_names": (
            "创新药", "生物疫苗", "医疗器械概念", "CRO概念", "基因测序", "细胞免疫治疗", "眼科医疗",
            "脑机接口", "智能医疗", "医美概念", "重组蛋白", "减肥药", "合成生物", "辅助生殖",
        ),
    },
    {
        "canonical_id": "CONSUMER_SERVICES",
        "display_name": "消费服务",
        "activation_keywords": (
            "消费服务", "服务消费", "内需", "旅游", "酒店", "餐饮", "零售", "食品饮料", "文旅",
        ),
        "industry_names": (
            "旅游及酒店", "餐饮", "零售", "互联网电商", "食品加工制造", "饮料制造", "休闲食品", "文化传媒",
            "教育", "美容护理", "家居用品",
        ),
        "concept_names": (
            "旅游概念", "免税店", "消费电子概念", "白酒概念", "啤酒概念", "乳业", "预制菜", "宠物经济",
            "跨境电商", "网红经济", "短剧游戏", "体育产业", "IP经济(谷子经济)",
        ),
    },
    {
        "canonical_id": "FINANCIAL_HIGH_DIVIDEND",
        "display_name": "金融高股息",
        "activation_keywords": (
            "金融高股息", "高股息", "红利资产", "银行", "保险", "证券", "中特估", "资本市场扩容",
        ),
        "industry_names": (
            "银行", "证券", "保险", "多元金融", "国有大型银行", "股份制银行", "城商行", "农商行",
        ),
        "concept_names": (
            "高股息精选", "同花顺中特估100", "中字头股票", "央企国企改革", "国企改革", "参股银行", "参股保险",
            "参股券商", "融资融券", "互联网金融",
        ),
    },
    {
        "canonical_id": "AGRICULTURE_FOOD_SECURITY",
        "display_name": "农业与粮食安全",
        "activation_keywords": (
            "农业", "粮食安全", "种植业", "种业", "养殖业", "农产品", "生猪", "玉米", "农业政策",
        ),
        "industry_names": (
            "种植业与林业", "种子生产", "粮食种植", "农业综合", "养殖业", "生猪养殖", "农产品加工",
            "动物保健", "农产品加工", "畜禽饲料", "水产养殖",
        ),
        "concept_names": (
            "农业种植", "粮食概念", "玉米", "大豆", "转基因", "猪肉", "养鸡", "生态农业", "乡村振兴",
            "农机", "农村电商", "供销社", "数字乡村",
        ),
    },
    {
        "canonical_id": "AI_APPLICATIONS_DIGITAL_ECONOMY",
        "display_name": "AI应用与数字经济",
        "activation_keywords": (
            "AI应用", "人工智能应用", "数字经济", "数据要素", "软件", "信创", "数据安全", "智能体",
        ),
        "industry_names": (
            "软件开发", "IT服务", "计算机设备", "通信服务", "数字媒体", "文化传媒", "广告营销",
        ),
        "concept_names": (
            "人工智能", "AI应用", "AIGC概念", "ChatGPT概念", "多模态AI", "AI智能体", "DeepSeek概念",
            "数据要素", "数据安全", "数字经济", "数字货币", "智慧政务", "信创", "华为鲲鹏", "MLOps概念",
        ),
    },
    {
        "canonical_id": "INFRASTRUCTURE_DOMESTIC_DEMAND",
        "display_name": "基础设施与内需",
        "activation_keywords": (
            "基础设施", "稳增长", "内需", "基建", "建筑", "城市更新", "房地产", "水利", "重大工程",
        ),
        "industry_names": (
            "建筑装饰", "基础建设", "房屋建设", "专业工程", "建筑材料", "水泥", "房地产", "房地产服务",
            "公路铁路运输", "工程咨询服务",
        ),
        "concept_names": (
            "一带一路", "水利", "新型城镇化", "装配式建筑", "地下管网", "建筑节能", "PPP概念", "西部大开发",
            "统一大市场", "房屋检测", "新型工业化",
        ),
    },
)


DEFAULT_MATURE_THEME_REGISTRY: dict[str, Any] = {
    "schema_version": MODULE_VERSION,
    "version": REGISTRY_VERSION,
    "enabled": True,
    "activation": {"minimum_keyword_hits": 1},
    "themes": [deepcopy(dict(theme)) for theme in _DEFAULT_THEMES],
}


def resolve_mature_theme_registry(
    registry: Mapping[str, Any] | Sequence[Mapping[str, Any]],
    industry_catalog: Any,
    concept_catalog: Any,
) -> dict[str, Any]:
    """Resolve configured taxonomy names against the current THS catalogs.

    Matching is intentionally exact after trimming surrounding whitespace.
    Names with no current catalog match are returned under both the theme's
    ``unresolved`` list and the top-level ``unresolved`` list.  They never
    appear in ``taxonomy_links`` or in the resolved code arrays.
    """

    source = _unwrap_registry(registry)
    themes = _normalise_themes(source)
    industry_index, industry_count = _catalog_index(industry_catalog)
    concept_index, concept_count = _catalog_index(concept_catalog)
    unresolved: list[dict[str, str]] = []
    resolved_themes: list[dict[str, Any]] = []

    for theme in themes:
        item = deepcopy(theme)
        theme_unresolved: list[dict[str, str]] = []
        industry_mappings = _resolve_names(
            item["industry_names"],
            industry_index,
            taxonomy="INDUSTRY",
            canonical_id=item["canonical_id"],
            unresolved=theme_unresolved,
        )
        concept_mappings = _resolve_names(
            item["concept_names"],
            concept_index,
            taxonomy="CONCEPT",
            canonical_id=item["canonical_id"],
            unresolved=theme_unresolved,
        )
        theme_unresolved.sort(key=lambda row: (row["taxonomy"], row["name"]))
        unresolved.extend(theme_unresolved)

        item["industry_mappings"] = industry_mappings
        item["concept_mappings"] = concept_mappings
        item["industry_codes"] = [row["thscode"] for row in industry_mappings]
        item["concept_codes"] = [row["thscode"] for row in concept_mappings]
        item["taxonomy_links"] = _taxonomy_links_for_theme(
            item["canonical_id"],
            industry_mappings,
            concept_mappings,
        )
        item["unresolved"] = theme_unresolved
        item["unresolved_names"] = {
            "industry": [row["name"] for row in theme_unresolved if row["taxonomy"] == "INDUSTRY"],
            "concept": [row["name"] for row in theme_unresolved if row["taxonomy"] == "CONCEPT"],
        }
        item["resolution_status"] = "RESOLVED" if not theme_unresolved else (
            "PARTIAL" if item["taxonomy_links"] else "UNRESOLVED"
        )
        resolved_themes.append(item)

    version = _text(source.get("version")) or REGISTRY_VERSION
    result = {
        "schema_version": MODULE_VERSION,
        "registry_version": version,
        "version": version,
        "enabled": source.get("enabled", True) is not False,
        "activation": deepcopy(source.get("activation")) if isinstance(source.get("activation"), Mapping) else {"minimum_keyword_hits": 1},
        "themes": resolved_themes,
        "unresolved": unresolved,
        "unresolved_names": {
            "industry": [row["name"] for row in unresolved if row["taxonomy"] == "INDUSTRY"],
            "concept": [row["name"] for row in unresolved if row["taxonomy"] == "CONCEPT"],
        },
        "catalogs": {
            "industry": {"record_count": industry_count, "exact_name_count": len(industry_index)},
            "concept": {"record_count": concept_count, "exact_name_count": len(concept_index)},
        },
    }
    result["registry_hash"] = _digest(
        {
            "version": version,
            "themes": [
                {
                    "canonical_id": item["canonical_id"],
                    "industry_codes": item["industry_codes"],
                    "concept_codes": item["concept_codes"],
                    "unresolved": item["unresolved"],
                }
                for item in resolved_themes
            ],
        }
    )
    return result


def activate_mature_themes(
    resolved_registry: Mapping[str, Any],
    evidence_values: Any,
) -> dict[str, Any]:
    """Activate registry themes from keyword hits in bounded evidence text.

    Evidence can be a string, a list of strings/records, or an arbitrarily
    nested mapping.  Source references are inherited from enclosing records;
    when a record has no external reference, a deterministic local evidence
    reference is emitted so the activation remains auditable.
    """

    registry = _unwrap_registry(resolved_registry)
    themes = _normalise_resolved_themes(registry)
    rows = _flatten_evidence(evidence_values)
    minimum_hits = _activation_minimum_hits(registry)
    activated: list[dict[str, Any]] = []

    for theme in themes:
        keywords = _unique_texts((*theme.get("activation_keywords", ()), theme.get("display_name")))
        hits: set[str] = set()
        matched_rows: list[dict[str, Any]] = []
        for row in rows:
            matched = sorted({keyword for keyword in keywords if _contains(row["normalized_text"], keyword)})
            if not matched:
                continue
            hits.update(matched)
            matched_rows.append(
                {
                    "path": row["path"],
                    "excerpt": row["text"][:_MAX_EVIDENCE_EXCERPT],
                    "matched_keywords": matched,
                    "source_refs": row["source_refs"],
                }
            )
        matched_rows.sort(key=lambda row: (row["path"], row["excerpt"], tuple(row["matched_keywords"])))
        matched_rows = matched_rows[:_MAX_EVIDENCE_ROWS_PER_THEME]
        keyword_hits = sorted(hits)
        if len(keyword_hits) < minimum_hits:
            continue
        source_refs = sorted(
            {
                ref
                for row in matched_rows
                for ref in row["source_refs"]
                if ref
            }
        )[:_MAX_SOURCE_REFS]
        activated.append(
            {
                "canonical_id": theme["canonical_id"],
                "theme_id": theme["canonical_id"],
                "display_name": theme["display_name"],
                "keyword_hits": keyword_hits,
                "keyword_hit_count": len(keyword_hits),
                "evidence": matched_rows,
                "activation_evidence": matched_rows,
                "source_refs": source_refs,
                "activation_status": "ACTIVATED",
            }
        )

    # More independent keyword hits are a stronger activation signal.  The
    # canonical id is the tie breaker, making output independent of input
    # mapping insertion order.
    activated.sort(key=lambda row: (-int(row["keyword_hit_count"]), row["canonical_id"]))
    evidence_by_id = {
        row["canonical_id"]: {
            "display_name": row["display_name"],
            "keyword_hits": list(row["keyword_hits"]),
            "keyword_hit_count": row["keyword_hit_count"],
            "source_refs": list(row["source_refs"]),
            "evidence": deepcopy(row["evidence"]),
        }
        for row in activated
    }
    return {
        "schema_version": MODULE_VERSION,
        "registry_version": _text(registry.get("registry_version") or registry.get("version")) or REGISTRY_VERSION,
        "minimum_keyword_hits": minimum_hits,
        "evidence_row_count": len(rows),
        "activated_themes": activated,
        "activation_evidence": evidence_by_id,
    }


def augment_discovery_with_mature_registry(
    discovery: Mapping[str, Any],
    resolved_registry: Mapping[str, Any],
    activation_evidence: Any,
) -> dict[str, Any]:
    """Append canonical registry rows to an A1 discovery response.

    Existing model output remains byte-for-byte equivalent at each existing
    list element.  Only missing generated rows are appended.  The generated
    links use resolved THS codes only; unresolved names are retained in the
    registry metadata and cannot become links.
    """

    output = deepcopy(dict(discovery) if isinstance(discovery, Mapping) else {})
    registry = _unwrap_registry(resolved_registry)
    themes = {
        str(theme.get("canonical_id")): theme
        for theme in _normalise_resolved_themes(registry)
        if str(theme.get("canonical_id") or "").strip()
    }
    activation_rows, unknown_activation = _activation_rows(activation_evidence)
    unknown_activation = [
        {"canonical_id": row["canonical_id"], "reason": "MATURE_THEME_NOT_IN_REGISTRY"}
        for row in unknown_activation
    ]
    active_ids = sorted(
        {
            str(row.get("canonical_id") or row.get("theme_id") or "").strip()
            for row in activation_rows
            if str(row.get("canonical_id") or row.get("theme_id") or "").strip() in themes
        }
    )
    known_ids = set(themes)
    unknown_activation.extend(
        {
            "canonical_id": row["canonical_id"],
            "reason": "MATURE_THEME_NOT_IN_REGISTRY",
        }
        for row in activation_rows
        if row.get("canonical_id") not in known_ids
        and row.get("canonical_id")
        not in {item.get("canonical_id") for item in unknown_activation}
    )
    unknown_activation.sort(key=lambda row: (row["canonical_id"], row["reason"]))

    structural = _list_or_empty(output.get("structural_themes"))
    nodes = _list_or_empty(output.get("industry_chain_graph"))
    links = _list_or_empty(output.get("taxonomy_links"))
    mappings = _list_or_empty(output.get("industry_theme_mappings"))
    existing_theme_ids = {
        str(item.get("canonical_id") or item.get("theme_id") or "").strip()
        for item in structural
        if isinstance(item, Mapping)
    }
    existing_node_ids = {
        str(item.get("node_id") or "").strip()
        for item in nodes
        if isinstance(item, Mapping)
    }
    existing_link_keys = {
        (
            str(item.get("node_id") or "").strip(),
            str(item.get("taxonomy") or "").strip().upper(),
            str(item.get("taxonomy_code") or "").strip().upper(),
        )
        for item in links
        if isinstance(item, Mapping)
    }
    existing_mapping_keys = {
        (
            str(item.get("industry_thscode") or "").strip().upper(),
            str(theme_id).strip(),
        )
        for item in mappings
        if isinstance(item, Mapping)
        for theme_id in (item.get("mapped_theme_ids") or ())
    }

    appended_theme_ids: list[str] = []
    appended_node_ids: list[str] = []
    appended_link_keys: list[tuple[str, str, str]] = []
    appended_mapping_keys: list[tuple[str, str]] = []
    activation_by_id = {
        str(row.get("canonical_id") or row.get("theme_id") or "").strip(): row
        for row in activation_rows
    }

    for canonical_id in active_ids:
        theme = themes[canonical_id]
        activation = activation_by_id.get(canonical_id, {})
        source_refs = _activation_source_refs(activation, registry, canonical_id)
        resolved_links = [
            dict(row)
            for row in theme.get("taxonomy_links", ())
            if isinstance(row, Mapping) and str(row.get("taxonomy_code") or "").strip()
        ]
        resolved_links.sort(
            key=lambda row: (
                str(row.get("taxonomy") or "").upper(),
                str(row.get("taxonomy_code") or "").upper(),
                str(row.get("taxonomy_name") or ""),
            )
        )
        industry_codes = [
            str(row.get("taxonomy_code") or "").strip().upper()
            for row in resolved_links
            if str(row.get("taxonomy") or "").upper() == "INDUSTRY"
        ]
        concept_codes = [
            str(row.get("taxonomy_code") or "").strip().upper()
            for row in resolved_links
            if str(row.get("taxonomy") or "").upper() == "CONCEPT"
        ]
        industry_names = [
            str(row.get("taxonomy_name") or "").strip()
            for row in resolved_links
            if str(row.get("taxonomy") or "").upper() == "INDUSTRY"
        ]
        concept_names = [
            str(row.get("taxonomy_name") or "").strip()
            for row in resolved_links
            if str(row.get("taxonomy") or "").upper() == "CONCEPT"
        ]
        node_id = f"MTR:{canonical_id}:CORE"
        if canonical_id not in existing_theme_ids:
            structural.append(
                {
                    "theme_id": canonical_id,
                    "canonical_id": canonical_id,
                    "display_name": theme["display_name"],
                    "theme_type": "MATURE_THEME_REGISTRY",
                    "mapping_status": "REGISTRY_RESOLVED" if resolved_links else "REGISTRY_UNRESOLVED",
                    "registry_version": _registry_version(registry),
                    "keyword_hits": list(activation.get("keyword_hits") or ()),
                    "activation_source_refs": list(source_refs),
                    "source_refs": list(source_refs),
                    "unresolved": deepcopy(theme.get("unresolved") or []),
                }
            )
            existing_theme_ids.add(canonical_id)
            appended_theme_ids.append(canonical_id)

        if node_id not in existing_node_ids:
            nodes.append(
                {
                    "node_id": node_id,
                    "theme_id": canonical_id,
                    "theme_ids": [canonical_id],
                    "canonical_id": canonical_id,
                    "display_name": f"{theme['display_name']}规范映射",
                    "node_type": "MATURE_THEME_TAXONOMY_CORE",
                    "industry_thscodes": list(dict.fromkeys(industry_codes)),
                    "concept_thscodes": list(dict.fromkeys(concept_codes)),
                    "industry_names": list(dict.fromkeys(industry_names)),
                    "concept_names": list(dict.fromkeys(concept_names)),
                    "registry_version": _registry_version(registry),
                    "source_refs": list(source_refs),
                    "unresolved": deepcopy(theme.get("unresolved") or []),
                }
            )
            existing_node_ids.add(node_id)
            appended_node_ids.append(node_id)

        for link in resolved_links:
            taxonomy = str(link.get("taxonomy") or "").strip().upper()
            code = str(link.get("taxonomy_code") or "").strip().upper()
            name = str(link.get("taxonomy_name") or "").strip()
            if taxonomy not in {"INDUSTRY", "CONCEPT"} or not code or not name:
                continue
            key = (node_id, taxonomy, code)
            if key in existing_link_keys:
                continue
            links.append(
                {
                    "node_id": node_id,
                    "theme_id": canonical_id,
                    "taxonomy": taxonomy,
                    "taxonomy_code": code,
                    "taxonomy_name": name,
                    "match_method": "MATURE_THEME_REGISTRY_EXACT_NAME",
                    "confidence": 1.0,
                    "registry_version": _registry_version(registry),
                    "source_refs": list(source_refs),
                }
            )
            existing_link_keys.add(key)
            appended_link_keys.append(key)

        for code in industry_codes:
            key = (code, canonical_id)
            if key in existing_mapping_keys:
                continue
            mappings.append(
                {
                    "industry_thscode": code,
                    "mapped_theme_ids": [canonical_id],
                    "mapping_status": "MAPPED",
                    "mapping_source": "MATURE_THEME_REGISTRY",
                    "supporting_source_refs": list(source_refs),
                    "confidence": 1.0,
                }
            )
            existing_mapping_keys.add(key)
            appended_mapping_keys.append(key)

    # Store generated lists only when the source was list-shaped or absent.
    # Malformed model fields are left untouched so this helper cannot silently
    # rewrite model output.
    if isinstance(output.get("structural_themes"), list) or "structural_themes" not in output:
        output["structural_themes"] = structural
    if isinstance(output.get("industry_chain_graph"), list) or "industry_chain_graph" not in output:
        output["industry_chain_graph"] = nodes
    if isinstance(output.get("taxonomy_links"), list) or "taxonomy_links" not in output:
        output["taxonomy_links"] = links
    if isinstance(output.get("industry_theme_mappings"), list) or "industry_theme_mappings" not in output:
        output["industry_theme_mappings"] = mappings
    stable_node_ids = [f"MTR:{canonical_id}:CORE" for canonical_id in active_ids]
    stable_link_keys = sorted(
        {
            (
                f"MTR:{canonical_id}:CORE",
                str(link.get("taxonomy") or "").upper(),
                str(link.get("taxonomy_code") or "").upper(),
            )
            for canonical_id in active_ids
            for link in themes[canonical_id].get("taxonomy_links", ())
            if isinstance(link, Mapping)
            and str(link.get("taxonomy") or "").upper() in {"INDUSTRY", "CONCEPT"}
            and str(link.get("taxonomy_code") or "").strip()
        }
        | set(appended_link_keys)
    )
    stable_mapping_keys = sorted(
        {
            (
                str(link.get("taxonomy_code") or "").upper(),
                canonical_id,
            )
            for canonical_id in active_ids
            for link in themes[canonical_id].get("taxonomy_links", ())
            if isinstance(link, Mapping)
            and str(link.get("taxonomy") or "").upper() == "INDUSTRY"
            and str(link.get("taxonomy_code") or "").strip()
        }
        | set(appended_mapping_keys)
    )
    output["mature_theme_registry"] = {
        "schema_version": MODULE_VERSION,
        "registry_version": _registry_version(registry),
        "activated_theme_ids": active_ids,
        "ensured_theme_ids": active_ids,
        "ensured_node_ids": stable_node_ids,
        "ensured_taxonomy_link_keys": [list(key) for key in stable_link_keys],
        "ensured_industry_mapping_keys": [list(key) for key in stable_mapping_keys],
        "unresolved_registry": deepcopy(registry.get("unresolved") or []),
        "unresolved_activation": unknown_activation,
    }
    return output


def _unwrap_registry(value: Any) -> Mapping[str, Any]:
    if isinstance(value, Mapping):
        nested = value.get("mature_theme_registry")
        if isinstance(nested, Mapping) and not value.get("themes"):
            return nested
        return value
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return {"version": REGISTRY_VERSION, "themes": list(value)}
    return {"version": REGISTRY_VERSION, "themes": []}


def _normalise_themes(registry: Mapping[str, Any]) -> list[dict[str, Any]]:
    raw = registry.get("themes")
    rows: list[Mapping[str, Any]] = []
    if isinstance(raw, Mapping):
        for key, value in raw.items():
            if isinstance(value, Mapping):
                row = dict(value)
                row.setdefault("canonical_id", key)
                rows.append(row)
    elif isinstance(raw, Sequence) and not isinstance(raw, (str, bytes, bytearray)):
        rows = [item for item in raw if isinstance(item, Mapping)]
    grouped: dict[str, dict[str, Any]] = {}
    for raw_theme in rows:
        canonical_id = _text(
            raw_theme.get("canonical_id")
            or raw_theme.get("theme_id")
            or raw_theme.get("id")
        )
        display_name = _text(
            raw_theme.get("display_name")
            or raw_theme.get("theme_name")
            or raw_theme.get("name")
        )
        if not canonical_id or not display_name:
            continue
        normalized = dict(raw_theme)
        normalized["canonical_id"] = canonical_id
        normalized["display_name"] = display_name
        normalized["activation_keywords"] = list(
            _unique_texts(raw_theme.get("activation_keywords") or ())
        )
        normalized["industry_names"] = list(_unique_texts(raw_theme.get("industry_names") or ()))
        normalized["concept_names"] = list(_unique_texts(raw_theme.get("concept_names") or ()))
        current = grouped.get(canonical_id)
        if current is None:
            grouped[canonical_id] = normalized
            continue
        # Duplicate rows with one canonical id are merged, independently of
        # input order.  Scalar metadata retains the lexicographically smallest
        # value so a changed config order cannot cause data drift.
        for key in ("activation_keywords", "industry_names", "concept_names"):
            current[key] = list(
                _unique_texts((*current.get(key, ()), *normalized.get(key, ())))
            )
        current["display_name"] = min(current["display_name"], normalized["display_name"])
    return [grouped[key] for key in sorted(grouped)]


def _normalise_resolved_themes(registry: Mapping[str, Any]) -> list[dict[str, Any]]:
    raw = registry.get("themes")
    if not isinstance(raw, Sequence) or isinstance(raw, (str, bytes, bytearray)):
        return _normalise_themes(registry)
    result: list[dict[str, Any]] = []
    for row in raw:
        if not isinstance(row, Mapping):
            continue
        item = dict(row)
        canonical_id = _text(item.get("canonical_id") or item.get("theme_id"))
        display_name = _text(item.get("display_name") or item.get("theme_name"))
        if not canonical_id or not display_name:
            continue
        item["canonical_id"] = canonical_id
        item["display_name"] = display_name
        item["activation_keywords"] = list(_unique_texts(item.get("activation_keywords") or ()))
        item["taxonomy_links"] = [
            dict(link)
            for link in item.get("taxonomy_links", ())
            if isinstance(link, Mapping)
            and str(link.get("taxonomy_code") or "").strip()
            and str(link.get("taxonomy") or "").strip().upper() in {"INDUSTRY", "CONCEPT"}
        ]
        result.append(item)
    result.sort(key=lambda item: item["canonical_id"])
    return result


def _catalog_index(catalog: Any) -> tuple[dict[str, list[dict[str, str]]], int]:
    records = _catalog_records(catalog)
    index: dict[str, list[dict[str, str]]] = {}
    seen: set[tuple[str, str]] = set()
    for row in records:
        if not isinstance(row, Mapping):
            continue
        name = _text(
            row.get("name")
            or row.get("taxonomy_name")
            or row.get("industry_name")
            or row.get("concept_name")
            or row.get("display_name")
        )
        code = _text(
            row.get("thscode")
            or row.get("taxonomy_code")
            or row.get("industry_thscode")
            or row.get("concept_thscode")
            or row.get("code")
        )
        if not name or not code:
            continue
        code = code.upper()
        key = (name, code)
        if key in seen:
            continue
        seen.add(key)
        index.setdefault(name, []).append({"name": name, "thscode": code})
    for values in index.values():
        values.sort(key=lambda item: (item["thscode"], item["name"]))
    return index, len(records)


def _catalog_records(catalog: Any) -> list[Any]:
    if isinstance(catalog, Mapping):
        records = catalog.get("records")
        if isinstance(records, Sequence) and not isinstance(records, (str, bytes, bytearray)):
            return list(records)
        for key in ("payload", "data", "result", "items"):
            nested = catalog.get(key)
            if isinstance(nested, Mapping) or isinstance(nested, Sequence):
                found = _catalog_records(nested)
                if found:
                    return found
        return []
    if isinstance(catalog, Sequence) and not isinstance(catalog, (str, bytes, bytearray)):
        return list(catalog)
    return []


def _resolve_names(
    names: Sequence[str],
    index: Mapping[str, Sequence[Mapping[str, str]]],
    *,
    taxonomy: str,
    canonical_id: str,
    unresolved: list[dict[str, str]],
) -> list[dict[str, str]]:
    resolved: list[dict[str, str]] = []
    seen_codes: set[str] = set()
    for name in _unique_texts(names):
        matches = index.get(name, ())
        if not matches:
            unresolved.append({"canonical_id": canonical_id, "taxonomy": taxonomy, "name": name})
            continue
        for match in matches:
            code = _text(match.get("thscode"))
            if not code or code in seen_codes:
                continue
            seen_codes.add(code)
            resolved.append({"name": name, "thscode": code})
    resolved.sort(key=lambda row: (row["thscode"], row["name"]))
    return resolved


def _taxonomy_links_for_theme(
    canonical_id: str,
    industry: Sequence[Mapping[str, str]],
    concept: Sequence[Mapping[str, str]],
) -> list[dict[str, Any]]:
    links: list[dict[str, Any]] = []
    for taxonomy, rows in (("INDUSTRY", industry), ("CONCEPT", concept)):
        for row in rows:
            code = _text(row.get("thscode"))
            name = _text(row.get("name"))
            if not code or not name:
                continue
            links.append(
                {
                    "theme_id": canonical_id,
                    "canonical_id": canonical_id,
                    "taxonomy": taxonomy,
                    "taxonomy_code": code.upper(),
                    "taxonomy_name": name,
                    "match_method": "MATURE_THEME_REGISTRY_EXACT_NAME",
                    "confidence": 1.0,
                }
            )
    links.sort(key=lambda row: (row["taxonomy"], row["taxonomy_code"], row["taxonomy_name"]))
    return links


def _flatten_evidence(value: Any) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    counter = [0]

    def visit(item: Any, path: str, inherited_refs: Sequence[str]) -> None:
        local_refs = list(inherited_refs)
        if isinstance(item, Mapping):
            local_refs.extend(_extract_refs(item))
            local_refs = sorted(set(local_refs))[:_MAX_SOURCE_REFS]
            for key in sorted(item, key=lambda value: str(value)):
                key_text = str(key)
                if key_text.lower() in _REF_KEYS or key_text.lower() in _NON_TEXT_KEYS:
                    continue
                visit(item[key], f"{path}.{key_text}" if path else key_text, local_refs)
            return
        if isinstance(item, Sequence) and not isinstance(item, (str, bytes, bytearray)):
            for index, child in enumerate(item):
                visit(child, f"{path}[{index}]", local_refs)
            return
        if item is None or isinstance(item, bool) or isinstance(item, (int, float)):
            return
        text = str(item).strip()
        if not text:
            return
        if not local_refs:
            counter[0] += 1
            local_refs = [f"derived:mature-theme-registry:evidence:{counter[0]}"]
        result.append(
            {
                "path": path or f"evidence[{len(result)}]",
                "text": text,
                "normalized_text": _normalize_for_match(text),
                "source_refs": sorted(set(local_refs))[:_MAX_SOURCE_REFS],
            }
        )

    visit(value, "", ())
    result.sort(key=lambda row: (row["path"], row["text"], tuple(row["source_refs"])))
    return result


def _extract_refs(value: Mapping[str, Any]) -> list[str]:
    refs: list[str] = []
    for key in _REF_KEYS:
        raw = value.get(key)
        if isinstance(raw, str):
            if raw.strip():
                refs.append(raw.strip())
        elif isinstance(raw, Sequence) and not isinstance(raw, (str, bytes, bytearray)):
            refs.extend(str(item).strip() for item in raw if str(item).strip())
    return sorted(set(refs))[:_MAX_SOURCE_REFS]


def _activation_rows(value: Any) -> tuple[list[dict[str, Any]], list[dict[str, str]]]:
    raw_rows: Any = value
    if isinstance(value, Mapping):
        if isinstance(value.get("activated_themes"), Sequence) and not isinstance(value.get("activated_themes"), (str, bytes, bytearray)):
            raw_rows = value.get("activated_themes")
        elif isinstance(value.get("activation_evidence"), Mapping):
            raw_rows = [
                {"canonical_id": key, **(dict(row) if isinstance(row, Mapping) else {})}
                for key, row in value["activation_evidence"].items()
            ]
        elif value.get("canonical_id") or value.get("theme_id"):
            raw_rows = [value]
        else:
            raw_rows = [
                {"canonical_id": key, **(dict(row) if isinstance(row, Mapping) else {})}
                for key, row in value.items()
                if isinstance(row, Mapping)
            ]
    if not isinstance(raw_rows, Sequence) or isinstance(raw_rows, (str, bytes, bytearray)):
        raw_rows = []
    rows: list[dict[str, Any]] = []
    unknown: list[dict[str, str]] = []
    for raw in raw_rows:
        if not isinstance(raw, Mapping):
            continue
        row = dict(raw)
        canonical_id = _text(row.get("canonical_id") or row.get("theme_id"))
        if not canonical_id:
            continue
        row["canonical_id"] = canonical_id
        rows.append(row)
    # Keep one activation row per canonical id.  If duplicate rows are
    # supplied, merge evidence and references in a stable manner.
    grouped: dict[str, dict[str, Any]] = {}
    for row in rows:
        current = grouped.get(row["canonical_id"])
        if current is None:
            grouped[row["canonical_id"]] = row
            continue
        current["keyword_hits"] = list(
            _unique_texts((*current.get("keyword_hits", ()), *row.get("keyword_hits", ())))
        )
        current["source_refs"] = list(
            _unique_texts((*current.get("source_refs", ()), *row.get("source_refs", ())))
        )
        existing_evidence = current.get("evidence") or current.get("activation_evidence") or []
        next_evidence = row.get("evidence") or row.get("activation_evidence") or []
        if isinstance(existing_evidence, Sequence) and not isinstance(existing_evidence, (str, bytes, bytearray)):
            current["evidence"] = [*existing_evidence, *next_evidence] if isinstance(next_evidence, Sequence) else list(existing_evidence)
    # Unknown ids are only knowable when the resolved registry is available to
    # the caller; retain them here for the augmentation metadata and filter at
    # the public function boundary below.
    return [grouped[key] for key in sorted(grouped)], unknown


def _activation_source_refs(
    activation: Mapping[str, Any],
    registry: Mapping[str, Any],
    canonical_id: str,
) -> list[str]:
    refs = list(_unique_texts(activation.get("source_refs") or ()))
    for key in ("evidence", "activation_evidence"):
        rows = activation.get(key)
        if not isinstance(rows, Sequence) or isinstance(rows, (str, bytes, bytearray)):
            continue
        for row in rows:
            if isinstance(row, Mapping):
                refs.extend(_extract_refs(row))
                raw = row.get("source_refs")
                if isinstance(raw, Sequence) and not isinstance(raw, (str, bytes, bytearray)):
                    refs.extend(str(item).strip() for item in raw if str(item).strip())
    refs = list(_unique_texts(refs))
    if not refs:
        refs = [f"derived:mature-theme-registry:{_registry_version(registry)}:{canonical_id}"]
    return refs[:_MAX_SOURCE_REFS]


def _activation_minimum_hits(registry: Mapping[str, Any]) -> int:
    activation = registry.get("activation")
    raw = activation.get("minimum_keyword_hits") if isinstance(activation, Mapping) else registry.get("minimum_keyword_hits")
    try:
        value = int(raw)
    except (TypeError, ValueError):
        value = 1
    return max(1, min(value, 20))


def _list_or_empty(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return list(value)
    return []


def _unique_texts(values: Any) -> tuple[str, ...]:
    if isinstance(values, str):
        values = (values,)
    if not isinstance(values, Sequence) or isinstance(values, (bytes, bytearray)):
        return ()
    return tuple(
        sorted(
            {
                str(value).strip()
                for value in values
                if value is not None
                and not isinstance(value, bool)
                and str(value).strip()
            }
        )
    )


def _text(value: Any) -> str | None:
    if value is None or isinstance(value, bool):
        return None
    text = str(value).strip()
    return text or None


def _normalize_for_match(value: Any) -> str:
    text = unicodedata.normalize("NFKC", str(value or "")).casefold()
    return re.sub(r"\s+", "", text)


def _contains(normalized_text: str, keyword: str) -> bool:
    normalized_keyword = _normalize_for_match(keyword)
    return bool(normalized_keyword) and normalized_keyword in normalized_text


def _registry_version(registry: Mapping[str, Any]) -> str:
    return _text(registry.get("registry_version") or registry.get("version")) or REGISTRY_VERSION


def _digest(value: Any) -> str:
    payload = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


__all__ = [
    "DEFAULT_MATURE_THEME_REGISTRY",
    "MODULE_VERSION",
    "REGISTRY_VERSION",
    "activate_mature_themes",
    "augment_discovery_with_mature_registry",
    "resolve_mature_theme_registry",
]
