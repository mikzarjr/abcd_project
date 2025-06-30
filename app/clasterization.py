import argparse
import glob
import json
import re
from pathlib import Path

import pandas as pd
from sentence_transformers import SentenceTransformer
from sklearn.metrics.pairwise import cosine_similarity
from tqdm.auto import tqdm

# аргументы
ap = argparse.ArgumentParser()
ap.add_argument("--in", dest="in_file", help="CSV date,author,text")
ap.add_argument("--cat", dest="cat_file", default="categories.json",
                help="JSON с категориями и описаниями")
ap.add_argument("--th", dest="th", type=float, default=0.25,
                help="порог кос.-сходства; ниже → 'other'")
args = ap.parse_args()

if not args.in_file:
    args.in_file = sorted(glob.glob("data/*messages*.csv"))[0]
print("[INFO] Файл:", args.in_file)

# читаем категории
if not Path(args.cat_file).exists():
    cats = {
        "global_news": "telegram world news international ban regulation",
        "local_news": "россия украина снг закон госдума роскомнадзор",
        "updates": "release update version changelog beta новая функция",
        "bugs_incidents": "не работает сбой outage down dc error баг",
        "security_privacy": "vpn proxy encryption privacy безопасность блокировка",
        "tutorials_tips": "how to guide инструкция лайфхак подсказка tips",
        "promotions_ads": "скидка акция premium promo реклама sale discount",
        "facts_insights": "статистика отчёт research аналитика миллионов пользователей",
        "memes_humor": "мем joke humour смешно шутка картинка",
        "other": "прочее misc"
    }
    json.dump(cats, open(args.cat_file, "w", encoding="utf-8"), ensure_ascii=False, indent=2)
else:
    cats = json.load(open(args.cat_file, encoding="utf-8"))

labels, cat_texts = zip(*cats.items())

# очистка текста
DOMAIN_STOP = {"telegram", "телеграм", "tg", "на", "для", "по", "что", "не"}


def clean(txt: str) -> str:
    txt = re.sub(r"https?://\S+|@\w+|#\w+", " ", txt or "").lower()
    txt = re.sub(r"[^a-zа-яё ]+", " ", txt)
    return " ".join(w for w in txt.split() if w not in DOMAIN_STOP and len(w) > 2)


df = pd.read_csv(args.in_file)
df["text"] = df["text"].fillna("").astype(str)
tqdm.pandas()
df["clean"] = df["text"].progress_apply(clean)

# эмбеддинги
model = SentenceTransformer("sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2")
emb_msg = model.encode(df["clean"].tolist(), batch_size=64, show_progress_bar=True)
emb_cats = model.encode(list(cat_texts), show_progress_bar=False)

# кос.сходство и присвоение категории
sim = cosine_similarity(emb_msg, emb_cats)  # shape (N_msg, N_cat)
best_idx = sim.argmax(axis=1)
best_val = sim.max(axis=1)

assigned = [
    labels[i] if score >= args.th else "other"
    for i, score in zip(best_idx, best_val)
]
df["category"] = assigned

# сохранение
base = Path(args.in_file).with_suffix("").name
out_csv = f"data/{base}_labeled.csv"
df[["date", "author", "text", "category"]].to_csv(out_csv, index=False, encoding="utf-8")

stats = df["category"].value_counts().rename_axis("category").reset_index(name="count")
stats.to_csv(f"data/{base}_stats.csv", index=False, encoding="utf-8")

print("✅ labeled CSV  →", out_csv)
print("   stats        →", f"data/{base}_stats.csv")
