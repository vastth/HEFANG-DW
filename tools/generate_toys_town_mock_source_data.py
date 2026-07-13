from __future__ import annotations

import argparse
import csv
import random
from bisect import bisect
from collections import OrderedDict
from datetime import datetime
from pathlib import Path
from typing import Iterable


WORKBOOK_DIR = Path(
    r"d:\tianhao\Documents\我的 Tableau 存储库\工作簿\#VOTD Sales Dashboard (Retail Toy Store)_v2025.3"
)
DATA_DIR = WORKBOOK_DIR / "Data"
DEFAULT_OUTPUT_DIR = WORKBOOK_DIR / "mock_source_data"
DEFAULT_ZH_OUTPUT_DIR = WORKBOOK_DIR / "mock_source_data_zh-CN"


PRODUCT_NAME_MAP = {
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
    "Mini Basketball": "迷你篮球",
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
    "Supersoaker Gun": "超级水枪",
    "Teddy Bear": "泰迪熊",
    "Toy Robot": "玩具机器人",
    "Uno Card Game": "UNO 卡牌",
}

CITY_NAME_MAP = {
    "Aguascalientes": "阿瓜斯卡连特斯",
    "Campeche": "坎佩切",
    "Chetumal": "切图马尔",
    "Chihuahua": "奇瓦瓦",
    "Chilpancingo": "奇尔潘辛戈",
    "Ciudad Victoria": "维多利亚城",
    "Cuernavaca": "库埃纳瓦卡",
    "Ciudad de Mexico": "墨西哥城",
    "Cuidad de Mexico": "墨西哥城",
    "Culiacan": "库利阿坎",
    "Durango": "杜兰戈",
    "Guadalajara": "瓜达拉哈拉",
    "Guanajuato": "瓜纳华托",
    "Hermosillo": "埃莫西约",
    "La Paz": "拉巴斯",
    "Merida": "梅里达",
    "Mexicali": "墨西卡利",
    "Monterrey": "蒙特雷",
    "Morelia": "莫雷利亚",
    "Oaxaca": "瓦哈卡",
    "Pachuca": "帕丘卡",
    "Puebla": "普埃布拉",
    "Saltillo": "萨尔蒂约",
    "San Luis Potosi": "圣路易斯波托西",
    "Santiago": "圣地亚哥",
    "Toluca": "托卢卡",
    "Tuxtla Gutierrez": "图斯特拉-古铁雷斯",
    "Villahermosa": "比亚埃尔莫萨",
    "Xalapa": "哈拉帕",
    "Zacatecas": "萨卡特卡斯",
}

CATEGORY_MAP = {
    "Art & Crafts": "美术手作",
    "Electronics": "电子玩具",
    "Games": "游戏",
    "Sports & Outdoors": "运动户外",
    "Toys": "玩具",
}

LOCATION_MAP = {
    "Airport": "机场店",
    "Commercial": "商业区店",
    "Downtown": "市中心店",
    "Residential": "居民区店",
}

BUTTON_TEXT_MAP = {
    "Bars": "柱状",
    "Map": "地图",
}

ZH_FIELD_MAP = {
    "active_button": "激活按钮",
    "button_text": "按钮文案",
    "date": "日期",
    "product_category": "产品品类",
    "product_cost": "产品成本",
    "product_id": "产品ID",
    "product_name": "产品名称",
    "product_price": "产品单价",
    "sale_id": "销售单ID",
    "store_city": "门店城市",
    "store_id": "门店ID",
    "store_location": "门店位置类型",
    "store_name": "门店名称",
    "switch": "切换值",
    "units": "销量",
    "value": "值",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate source-like mock data for the Toys Town Tableau learning workbook."
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=DATA_DIR,
        help="Directory that contains the extracted CSV templates.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help="Directory where the simulated source tables will be written.",
    )
    parser.add_argument(
        "--zh-output-dir",
        type=Path,
        default=DEFAULT_ZH_OUTPUT_DIR,
        help="Directory where the Chinese mock tables will be written.",
    )
    parser.add_argument(
        "--locale",
        choices=("raw", "zh-cn", "both"),
        default="raw",
        help="Which output set to generate.",
    )
    parser.add_argument(
        "--sales-rows",
        type=int,
        default=0,
        help="How many rows to write into sales.csv. 0 means reuse all visible order seeds.",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=20260521,
        help="Random seed for deterministic output.",
    )
    return parser.parse_args()


def read_csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def parse_date(value: str) -> datetime.date:
    value = value.strip()
    for pattern in ("%Y/%m/%d", "%Y-%m-%d", "%Y年%m月%d日"):
        try:
            return datetime.strptime(value, pattern).date()
        except ValueError:
            continue
    raise ValueError(f"Unsupported date format: {value}")


def load_products(path: Path) -> list[dict[str, object]]:
    rows = read_csv_rows(path)
    products: list[dict[str, object]] = []
    for index, row in enumerate(rows, start=1):
        products.append(
            {
                "product_id": index,
                "product_name": row["Product Name"].strip(),
                "product_category": row["Category"].strip(),
                "product_cost": f"{float(row['Cost']):.2f}",
                "product_price": f"{float(row['Price']):.2f}",
            }
        )
    return products


def load_stores(path: Path) -> list[dict[str, object]]:
    rows = read_csv_rows(path)
    deduped: OrderedDict[str, dict[str, object]] = OrderedDict()
    for row in rows:
        store_name = row["Store Name"].strip()
        if store_name in deduped:
            continue
        deduped[store_name] = {
            "store_id": len(deduped) + 1,
            "store_name": (
                store_name if store_name.startswith("Maven Toys ") else f"Maven Toys {store_name}"
            ),
            "store_city": row["City"].strip(),
            "store_location": row["Location"].strip(),
        }
    return list(deduped.values())


def load_calendar(path: Path) -> list[dict[str, str]]:
    rows = read_csv_rows(path)
    dates = sorted({parse_date(row["Order Date"]) for row in rows})
    return [{"date": day.isoformat()} for day in dates]


def load_aux_buttons(path: Path) -> list[dict[str, str]]:
    rows = read_csv_rows(path)
    output: list[dict[str, str]] = []
    for row in rows:
        output.append(
            {
                "active_button": row["Active Button"].strip(),
                "button_text": row["Button Text"].strip(),
                "switch": row["Switch"].strip(),
                "value": row["Value"].strip(),
            }
        )
    return output


def load_sales_seeds(path: Path) -> list[dict[str, int]]:
    rows = read_csv_rows(path)
    seeds: list[dict[str, int]] = []
    for row in rows:
        seeds.append(
            {
                "sale_id": int(row["Order ID"]),
                "units": int(row["Quantity"]),
            }
        )
    return seeds


def weighted_picker(items: Iterable[dict[str, object]], weights: Iterable[float]):
    items_list = list(items)
    cumulative: list[float] = []
    total = 0.0
    for weight in weights:
        total += weight
        cumulative.append(total)

    def pick(rng: random.Random) -> dict[str, object]:
        target = rng.random() * total
        index = bisect(cumulative, target)
        return items_list[index]

    return pick


def build_sales_rows(
    seeds: list[dict[str, int]],
    calendar_rows: list[dict[str, str]],
    product_rows: list[dict[str, object]],
    store_rows: list[dict[str, object]],
    sales_rows: int,
    seed: int,
) -> list[dict[str, object]]:
    rng = random.Random(seed)
    target_rows = sales_rows or len(seeds)

    month_weights = {
        1: 1.08,
        2: 1.02,
        3: 0.96,
        4: 0.94,
        5: 0.92,
        6: 0.90,
        7: 0.91,
        8: 0.95,
        9: 1.00,
        10: 1.08,
        11: 1.22,
        12: 1.38,
    }
    date_items = []
    date_weights = []
    for row in calendar_rows:
        day = parse_date(row["date"])
        weekday_bonus = 1.08 if day.weekday() >= 5 else 1.0
        date_items.append(row)
        date_weights.append(month_weights[day.month] * weekday_bonus)
    pick_date = weighted_picker(date_items, date_weights)

    category_weights = {
        "Games": 1.15,
        "Toys": 1.10,
        "Sports & Outdoors": 1.05,
        "Art & Crafts": 0.95,
        "Electronics": 0.90,
    }
    product_weights = [category_weights.get(str(row["product_category"]), 1.0) for row in product_rows]
    pick_product = weighted_picker(product_rows, product_weights)

    location_weights = {
        "Commercial": 1.18,
        "Downtown": 1.10,
        "Airport": 0.92,
        "Residential": 0.88,
    }
    store_weights = [location_weights.get(str(row["store_location"]), 1.0) for row in store_rows]
    pick_store = weighted_picker(store_rows, store_weights)

    sales_output: list[dict[str, object]] = []
    for index in range(target_rows):
        if index < len(seeds):
            sale_id = seeds[index]["sale_id"]
            units = seeds[index]["units"]
        else:
            sale_id = seeds[-1]["sale_id"] + (index - len(seeds)) + 1
            units = rng.choices([1, 2, 3, 4, 5], weights=[68, 20, 7, 3, 2], k=1)[0]

        day = pick_date(rng)
        product = pick_product(rng)
        store = pick_store(rng)
        sales_output.append(
            {
                "sale_id": sale_id,
                "date": day["date"],
                "store_id": store["store_id"],
                "product_id": product["product_id"],
                "units": units,
            }
        )
    return sales_output


def write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        raise ValueError(f"No rows to write: {path}")
    fieldnames = list(rows[0].keys())
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def write_readme(path: Path, sales_rows: int, calendar_rows: int, store_rows: int, product_rows: int) -> None:
    content = f"""# Toys Town 模拟源数据

这套数据是根据 Tableau 工作簿关系和解包后的 CSV 模板反推生成的学习用 source-like 数据，不依赖 Hyper。

## 表结构

- calendar.csv: date
- products.csv: product_id, product_name, product_category, product_cost, product_price
- stores.csv: store_id, store_name, store_city, store_location
- sales.csv: sale_id, date, store_id, product_id, units
- auxiliar_buttons.csv: active_button, button_text, switch, value

## 当前规模

- calendar: {calendar_rows} 行
- products: {product_rows} 行
- stores: {store_rows} 行
- sales: {sales_rows} 行

## 对应关系

- sales.date -> calendar.date
- sales.store_id -> stores.store_id
- sales.product_id -> products.product_id
- Tableau 中的 store_name 实际来自 stores 表再去掉前缀 Maven Toys 

## 用途

适合拿来单独建 MySQL / CSV 数据源，重新练习关系模型、字段计算、参数筛选和卡片/趋势/散点图搭建。
"""
    path.write_text(content, encoding="utf-8")


def translate_store_name(store_name: str) -> str:
    stripped = store_name.replace("Maven Toys ", "", 1)
    for city_en, city_zh in sorted(CITY_NAME_MAP.items(), key=lambda item: len(item[0]), reverse=True):
        if stripped.startswith(city_en):
            suffix = stripped[len(city_en) :].strip()
            suffix_value = suffix if suffix else ""
            if suffix_value:
                return f"玩具城 {city_zh} {suffix_value}店"
            return f"玩具城 {city_zh}店"
    return store_name


def localize_products(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    output: list[dict[str, object]] = []
    for row in rows:
        output.append(
            {
                "product_id": row["product_id"],
                "product_name": PRODUCT_NAME_MAP.get(str(row["product_name"]), row["product_name"]),
                "product_category": CATEGORY_MAP.get(str(row["product_category"]), row["product_category"]),
                "product_cost": row["product_cost"],
                "product_price": row["product_price"],
            }
        )
    return output


def localize_stores(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    output: list[dict[str, object]] = []
    for row in rows:
        city_cn = CITY_NAME_MAP.get(str(row["store_city"]), row["store_city"])
        output.append(
            {
                "store_id": row["store_id"],
                "store_name": translate_store_name(str(row["store_name"])),
                "store_city": city_cn,
                "store_location": LOCATION_MAP.get(str(row["store_location"]), row["store_location"]),
            }
        )
    return output


def localize_calendar(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    return [{"date": row["date"]} for row in rows]


def localize_sales(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    output: list[dict[str, object]] = []
    for row in rows:
        output.append(
            {
                "sale_id": row["sale_id"],
                "date": row["date"],
                "store_id": row["store_id"],
                "product_id": row["product_id"],
                "units": row["units"],
            }
        )
    return output


def localize_aux_buttons(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    output: list[dict[str, str]] = []
    for row in rows:
        output.append(
            {
                "Button Text": BUTTON_TEXT_MAP.get(row["button_text"], row["button_text"]),
                "Value": row["value"],
                "Switch": row["switch"],
            }
        )
    return output


def write_zh_readme(path: Path, sales_rows: int, calendar_rows: int, store_rows: int, product_rows: int) -> None:
    content = f"""# Toys Town 中文模拟源数据

这套数据是在英文 mock_source_data 基础上生成的中文版学习数据，适合直接查看表结构和字段语义。

## 中文数据说明

- calendar_zh.csv: 英文键列 `date`，日期值保持原格式
- products_zh.csv: 英文键列，产品名称/品类值已汉化
- stores_zh.csv: 英文键列，门店名称/城市/位置类型值已汉化
- sales_zh.csv: 英文键列 `sale_id/date/store_id/product_id/units`
- auxiliar_buttons_zh.csv: 英文键列 `Button Text/Value/Switch`，按钮文本值已汉化

## 当前规模

- calendar_zh: {calendar_rows} 行
- products_zh: {product_rows} 行
- stores_zh: {store_rows} 行
- sales_zh: {sales_rows} 行

## 说明

- 中文版和英文版使用同一套英文键列与 ID 体系，可以直接接入原 workbook，同时保留中文显示值。
- 门店名称采用中文城市名 + 店号的形式，便于直接理解。
- 这仍然是学习用模拟数据，不等同于原作者真实源库。
"""
    path.write_text(content, encoding="utf-8")


def main() -> None:
    args = parse_args()
    data_dir: Path = args.data_dir
    output_dir: Path = args.output_dir
    zh_output_dir: Path = args.zh_output_dir

    products = load_products(data_dir / "sales+ (toy_sales_data)_Products.csv")
    stores = load_stores(data_dir / "sales+ (toy_sales_data)_Stores.csv")
    calendar = load_calendar(data_dir / "sales+ (toy_sales_data)_Calendar.csv")
    aux_buttons = load_aux_buttons(data_dir / "Sheet1 (Auxiliar Buttons Toys)_Auxiliar Table.csv")
    sales_seeds = load_sales_seeds(data_dir / "sales+ (toy_sales_data)_Sales.csv")
    sales = build_sales_rows(
        seeds=sales_seeds,
        calendar_rows=calendar,
        product_rows=products,
        store_rows=stores,
        sales_rows=args.sales_rows,
        seed=args.seed,
    )

    if args.locale in {"raw", "both"}:
        write_csv(output_dir / "calendar.csv", calendar)
        write_csv(output_dir / "products.csv", products)
        write_csv(output_dir / "stores.csv", stores)
        write_csv(output_dir / "sales.csv", sales)
        write_csv(output_dir / "auxiliar_buttons.csv", aux_buttons)
        write_readme(
            output_dir / "README.md",
            sales_rows=len(sales),
            calendar_rows=len(calendar),
            store_rows=len(stores),
            product_rows=len(products),
        )
        print(f"OUTPUT_DIR={output_dir}")

    if args.locale in {"zh-cn", "both"}:
        zh_calendar = localize_calendar(calendar)
        zh_products = localize_products(products)
        zh_stores = localize_stores(stores)
        zh_sales = localize_sales(sales)
        zh_aux_buttons = localize_aux_buttons(aux_buttons)

        write_csv(zh_output_dir / "calendar_zh.csv", zh_calendar)
        write_csv(zh_output_dir / "products_zh.csv", zh_products)
        write_csv(zh_output_dir / "stores_zh.csv", zh_stores)
        write_csv(zh_output_dir / "sales_zh.csv", zh_sales)
        write_csv(zh_output_dir / "auxiliar_buttons_zh.csv", zh_aux_buttons)
        write_zh_readme(
            zh_output_dir / "README.md",
            sales_rows=len(zh_sales),
            calendar_rows=len(zh_calendar),
            store_rows=len(zh_stores),
            product_rows=len(zh_products),
        )
        print(f"ZH_OUTPUT_DIR={zh_output_dir}")

    print(f"calendar={len(calendar)}")
    print(f"products={len(products)}")
    print(f"stores={len(stores)}")
    print(f"sales={len(sales)}")


if __name__ == "__main__":
    main()