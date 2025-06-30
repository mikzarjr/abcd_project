import argparse
import glob
import matplotlib.pyplot as plt
import pandas as pd
from pathlib import Path

# 1. аргументы
parser = argparse.ArgumentParser(description="Frequency of messages from the author")
parser.add_argument("--input", dest="in_file", help="CSV: date,author,text")
parser.add_argument("--output", dest="out_file", help="Куда сохранить stats‑CSV")
parser.add_argument("--no-plot", action="store_true", help="Не сохранять PNG‑график")
args = parser.parse_args()

# 2. интерактив + дефолты
if not args.in_file:
    cands = sorted(glob.glob("data/*messages*.csv"))
    if cands:
        args.in_file = cands[0]
    else:
        args.in_file = input("Введите путь к CSV с сообщениями: ").strip()

if not args.out_file:
    base = Path(args.in_file).with_suffix("").name
    args.out_file = f"../../Documents/GitHub/abcd_project/data/{base}_author_stats.csv"

# 3. читаем данные и считаем статистику
df = pd.read_csv(args.in_file)
stats = (
    df.groupby("author")["text"]
    .size()
    .sort_values(ascending=False)
    .reset_index(name="messages")
)

out_path = Path(args.out_file)
out_path.parent.mkdir(parents=True, exist_ok=True)
stats.to_csv(out_path, index=False, encoding="utf-8")
print(f"✅ stats → {out_path}")
import re, warnings

warnings.filterwarnings("ignore", message="Glyph.*missing")

emoji = re.compile('[\U00010000-\U0010FFFF]', flags=re.UNICODE)
stats['author'] = stats['author'].str.replace(emoji, '', regex=True)
# 4. сохраняем график TOP‑10, если не отключён флагом
if not args.no_plot:
    top10 = stats.head(10)
    plt.figure(figsize=(8, 4))
    plt.barh(top10["author"][::-1], top10["messages"][::-1])
    plt.title("TOP‑10 authors by number of posts")
    plt.xlabel("number of messages")
    plt.tight_layout()
    img_path = out_path.with_suffix(".png")
    plt.savefig(img_path, dpi=150)
    print(f"📊 график → {img_path}")
