from pathlib import Path

from PIL import Image, ImageDraw, ImageFont


SOURCE_DIR = Path(
    r"d:\tianhao\Documents\我的 Tableau 存储库\工作簿\#VOTD Sales Dashboard (Retail Toy Store)_v2025.3\Image"
)
OUTPUT_DIR = SOURCE_DIR / "zh-CN"
FONT_PATH = Path(r"C:\Windows\Fonts\msyhbd.ttc")


BUTTON_SPECS = {
    "Open Filters.png": {
        "text": "筛选",
        "text_box": (34, 6, 86, 34),
        "fill": "#0b6c39",
        "text_fill": "#ffffff",
        "font_size": 17,
    },
    "Close Filters.png": {
        "text": "筛选",
        "text_box": (34, 6, 86, 34),
        "fill": "#0b6c39",
        "text_fill": "#ffffff",
        "font_size": 17,
    },
    "Open Months.png": {
        "text": "月份",
        "text_box": (34, 6, 86, 34),
        "fill": "#fcfcfc",
        "text_fill": "#b8b8b8",
        "font_size": 15,
    },
    "Close Months.png": {
        "text": "月份",
        "text_box": (34, 6, 86, 34),
        "fill": "#fcfcfc",
        "text_fill": "#b8b8b8",
        "font_size": 15,
    },
}


def draw_cn_text(image_path: Path, output_path: Path, spec: dict) -> None:
    image = Image.open(image_path).convert("RGBA")
    draw = ImageDraw.Draw(image)
    x0, y0, x1, y1 = spec["text_box"]

    draw.rectangle((x0, y0, x1, y1), fill=spec["fill"])

    font = ImageFont.truetype(str(FONT_PATH), spec["font_size"])
    text = spec["text"]
    left, top, right, bottom = draw.textbbox((0, 0), text, font=font)
    text_width = right - left
    text_height = bottom - top
    text_x = x0 + ((x1 - x0) - text_width) / 2
    text_y = y0 + ((y1 - y0) - text_height) / 2 - 1

    draw.text((text_x, text_y), text, font=font, fill=spec["text_fill"])
    image.save(output_path)


def main() -> None:
    if not SOURCE_DIR.exists():
        raise FileNotFoundError(f"Image directory not found: {SOURCE_DIR}")
    if not FONT_PATH.exists():
        raise FileNotFoundError(f"Chinese font not found: {FONT_PATH}")

    OUTPUT_DIR.mkdir(exist_ok=True)

    for file_name, spec in BUTTON_SPECS.items():
        source_path = SOURCE_DIR / file_name
        if not source_path.exists():
            raise FileNotFoundError(f"Source image not found: {source_path}")
        output_path = OUTPUT_DIR / file_name
        draw_cn_text(source_path, output_path, spec)
        print(f"CREATED {output_path}")


if __name__ == "__main__":
    main()