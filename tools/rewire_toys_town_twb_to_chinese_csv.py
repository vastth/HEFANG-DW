from __future__ import annotations

import argparse
from pathlib import Path
import xml.etree.ElementTree as ET


WORKBOOK_PATH = Path(
    r"d:\tianhao\Documents\我的 Tableau 存储库\工作簿\#VOTD Sales Dashboard (Retail Toy Store)_v2025.3\Retail Toy Store 学习版.twb"
)
ZH_DATA_DIR = Path(
    r"d:\tianhao\Documents\我的 Tableau 存储库\工作簿\#VOTD Sales Dashboard (Retail Toy Store)_v2025.3\mock_source_data_zh-CN"
)

MAIN_DS_NAME = "federated.147bdyc0b2z2zf14jhlxx104odb4"
AUX_DS_NAME = "federated.088ckyf1m6vd6f1e8ywoi0ixoewq"

PRODUCT_NAME_VALUE_MAP = {
    "Action Figure": "动作人偶",
    "Animal Figures": "动物手办",
    "Barrel O' Slime": "桶装史莱姆",
    "Chutes & Ladders": "滑道与梯子",
    "Classic Dominoes": "经典多米诺",
    "Colorbuds": "彩色耳塞",
    "Dart Gun": "飞镖枪",
    "Deck Of Cards": "扑克牌",
    "Dino Egg": "恐龙蛋",
    "Dinosaur Figures": "恐龙手办",
    "Etch A Sketch": "神奇画板",
    "Foam Disk Launcher": "泡沫飞盘发射器",
    "Gamer Headphones": "游戏耳机",
    "Glass Marbles": "玻璃弹珠",
    "Hot Wheels 5-Pack": "风火轮五件套",
    "Jenga": "叠叠乐",
    "Kids Makeup Kit": "儿童化妆套装",
    "Lego Bricks": "乐高积木",
    "Magic Sand": "魔力沙",
    "Mini Basketball Hoop": "迷你篮球",
    "Mini Ping Pong Set": "迷你乒乓套装",
    "Monopoly": "大富翁",
    "Mr. Potatohead": "土豆先生",
    "Nerf Gun": "软弹枪",
    "PlayDoh Can": "培乐多彩泥罐",
    "PlayDoh Playset": "培乐多套装",
    "PlayDoh Toolkit": "培乐多工具组",
    "Playfoam": "创意泡沫泥",
    "Plush Pony": "毛绒小马",
    "Rubik's Cube": "魔方",
    "Splash Balls": "戏水球",
    "Supersoaker Water Gun": "超级水枪",
    "Teddy Bear": "泰迪熊",
    "Toy Robot": "玩具机器人",
    "Uno Card Game": "UNO 卡牌",
}

CATEGORY_VALUE_MAP = {
    "Art & Crafts": "美术手作",
    "Electronics": "电子玩具",
    "Games": "游戏",
    "Sports & Outdoors": "运动户外",
    "Toys": "玩具",
}

MAIN_CONNECTIONS = {
    "sales": ("textscan.sales.zh", ZH_DATA_DIR / "sales_zh.csv"),
    "Custom SQL Query": ("textscan.stores.zh", ZH_DATA_DIR / "stores_zh.csv"),
    "calendar": ("textscan.calendar.zh", ZH_DATA_DIR / "calendar_zh.csv"),
    "products": ("textscan.products.zh", ZH_DATA_DIR / "products_zh.csv"),
}

MAIN_TABLES = {
    "sales": "[sales_zh.csv]",
    "Custom SQL Query": "[stores_zh.csv]",
    "calendar": "[calendar_zh.csv]",
    "products": "[products_zh.csv]",
}

MAIN_MAP_VALUES = {
    "[date (calendar)]": "[calendar].[date]",
    "[date]": "[sales].[date]",
    "[product_category]": "[products].[product_category]",
    "[product_cost]": "[products].[product_cost]",
    "[product_id (products)]": "[products].[product_id]",
    "[product_id]": "[sales].[product_id]",
    "[product_name]": "[products].[product_name]",
    "[product_price]": "[products].[product_price]",
    "[sale_id]": "[sales].[sale_id]",
    "[store_city]": "[Custom SQL Query].[store_city]",
    "[store_id (Custom SQL Query)]": "[Custom SQL Query].[store_id]",
    "[store_id]": "[sales].[store_id]",
    "[store_location]": "[Custom SQL Query].[store_location]",
    "[store_name]": "[Custom SQL Query].[store_name]",
    "[units]": "[sales].[units]",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Rewire Toys Town workbook to live Chinese CSV sources.")
    parser.add_argument("--workbook", type=Path, default=WORKBOOK_PATH)
    parser.add_argument("--zh-data-dir", type=Path, default=ZH_DATA_DIR)
    return parser.parse_args()


def set_connection_attributes(conn: ET.Element, filename: Path) -> None:
    conn.attrib.clear()
    conn.set("class", "textscan")
    conn.set("cleaning", "no")
    conn.set("compat", "no")
    conn.set("dataRefreshTime", "")
    conn.set("filename", filename.as_posix())
    conn.set("interpretationMode", "0")
    conn.set("password", "")
    conn.set("server", "")
    conn.set("validate", "no")


def decode_tableau_string(value: str | None) -> str | None:
    if value is None:
        return None
    text = value.strip()
    if text.startswith('&quot;') and text.endswith('&quot;'):
        text = text[len('&quot;') : -len('&quot;')]
    elif text.startswith('"') and text.endswith('"'):
        text = text[1:-1]
    return text.replace('&apos;', "'").replace('&amp;', '&')


def encode_tableau_string(value: str) -> str:
    return f'"{value}"'


def localize_parameter_members(column: ET.Element, value_map: dict[str, str]) -> None:
    calculation = column.find("./calculation")
    current_value = decode_tableau_string(column.get("value"))
    if current_value in value_map:
        localized = value_map[current_value]
        encoded = encode_tableau_string(localized)
        column.set("value", encoded)
        if calculation is not None:
            calculation.set("formula", encoded)

    members = column.find("./members")
    if members is not None:
        for member in members.findall("./member"):
            member_value = decode_tableau_string(member.get("value"))
            if member_value in value_map:
                member.set("value", encode_tableau_string(value_map[member_value]))


def update_parameter_defaults(root: ET.Element) -> None:
    for column in root.findall(".//column[@name='[Parameter 5]']"):
        localize_parameter_members(column, PRODUCT_NAME_VALUE_MAP)

    for column in root.findall(".//column[@name='[Parameter 6]']"):
        localize_parameter_members(column, CATEGORY_VALUE_MAP)


def update_category_palette_buckets(root: ET.Element) -> None:
    for bucket in root.findall(".//encoding[@field='[none:product_category:nk]']/map/bucket"):
        bucket_text = decode_tableau_string(bucket.text)
        if bucket_text in CATEGORY_VALUE_MAP:
            bucket.text = encode_tableau_string(CATEGORY_VALUE_MAP[bucket_text])


def update_aux_datasource(ds: ET.Element, zh_data_dir: Path) -> None:
    connection = ds.find("./connection")
    named_connection = connection.find("./named-connections/named-connection")
    named_connection.set("caption", "按钮辅助表 CSV")
    aux_conn = named_connection.find("./connection")
    set_connection_attributes(aux_conn, zh_data_dir / "auxiliar_buttons_zh.csv")

    relation = connection.find("./relation")
    relation.set("table", "[auxiliar_buttons_zh.csv]")

    columns = relation.find("./columns")
    column_names = ["Button Text", "Value", "Switch"]
    datatypes = ["string", "integer", "integer"]
    for index, column in enumerate(columns.findall("./column")):
        column.set("name", column_names[index])
        column.set("datatype", datatypes[index])

    object_node = ds.find("./object-graph/objects/object")
    context_relation = object_node.find("./properties[@context='']/relation")
    context_relation.set("table", "[auxiliar_buttons_zh.csv]")
    extract_props = object_node.find("./properties[@context='extract']")
    if extract_props is not None:
        object_node.remove(extract_props)

    extract_node = ds.find("./extract")
    if extract_node is not None:
        ds.remove(extract_node)


def rebuild_named_connections(connection: ET.Element, zh_data_dir: Path) -> None:
    named_connections = connection.find("./named-connections")
    for child in list(named_connections):
        named_connections.remove(child)

    for relation_name, (conn_name, filename) in MAIN_CONNECTIONS.items():
        named = ET.SubElement(named_connections, "named-connection")
        named.set("caption", relation_name)
        named.set("name", conn_name)
        conn = ET.SubElement(named, "connection")
        set_connection_attributes(conn, zh_data_dir / filename.name)


def update_main_relations(ds: ET.Element) -> None:
    connection = ds.find("./connection")
    relation_collection = connection.find("./relation")
    for relation in relation_collection.findall("./relation"):
        relation_name = relation.get("name")
        relation.set("connection", MAIN_CONNECTIONS[relation_name][0])
        relation.set("type", "table")
        relation.set("table", MAIN_TABLES[relation_name])
        relation.text = None
        relation[:] = []


def update_main_maps(ds: ET.Element) -> None:
    for mapping in ds.findall("./connection/cols/map"):
        key = mapping.get("key")
        if key in MAIN_MAP_VALUES:
            mapping.set("value", MAIN_MAP_VALUES[key])


def update_main_object_graph(ds: ET.Element) -> None:
    for obj in ds.findall("./object-graph/objects/object"):
        context_relation = obj.find("./properties[@context='']/relation")
        relation_name = context_relation.get("name")
        if relation_name in MAIN_CONNECTIONS:
            context_relation.set("connection", MAIN_CONNECTIONS[relation_name][0])
            context_relation.set("type", "table")
            context_relation.set("table", MAIN_TABLES[relation_name])
            context_relation.text = None
            context_relation[:] = []

        extract_props = obj.find("./properties[@context='extract']")
        if extract_props is not None:
            obj.remove(extract_props)


def update_main_datasource(ds: ET.Element, zh_data_dir: Path) -> None:
    connection = ds.find("./connection")
    rebuild_named_connections(connection, zh_data_dir)
    update_main_relations(ds)
    update_main_maps(ds)
    update_main_object_graph(ds)

    extract_node = ds.find("./extract")
    if extract_node is not None:
        ds.remove(extract_node)


def main() -> None:
    args = parse_args()
    tree = ET.parse(args.workbook)
    root = tree.getroot()

    aux_ds = root.find(f"./datasources/datasource[@name='{AUX_DS_NAME}']")
    main_ds = root.find(f"./datasources/datasource[@name='{MAIN_DS_NAME}']")
    if aux_ds is None or main_ds is None:
        raise ValueError("Target datasources not found in workbook")

    update_parameter_defaults(root)
    update_category_palette_buckets(root)
    update_aux_datasource(aux_ds, args.zh_data_dir)
    update_main_datasource(main_ds, args.zh_data_dir)

    tree.write(args.workbook, encoding="utf-8", xml_declaration=True)
    print(f"UPDATED={args.workbook}")


if __name__ == "__main__":
    main()