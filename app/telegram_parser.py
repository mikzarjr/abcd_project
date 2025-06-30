import argparse
import asyncio
import os
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from telethon import TelegramClient, errors
from telethon.tl import types


# 1. CLI
def parse_args():
    p = argparse.ArgumentParser(
        description="Выгружает сообщения из публичного канала Telegram → CSV"
    )
    p.add_argument("--chat", help="@username или slug канала (без https://t.me/)")
    p.add_argument("--limit", type=int, default=1000,
                   help="Сколько последних сообщений (по-умолчанию 1000; 0 = всё)")
    p.add_argument("--out", help="Итоговый CSV (формируется автоматически)")
    p.add_argument("--session", default="parser_session",
                   help="Имя файла сессии Telethon (.session)")

    args = p.parse_args()
    if not args.chat:
        args.chat = input("Введите slug/@username канала: ").strip().lstrip("@")
    if not args.out:
        args.out = f"data/{args.chat}_messages.csv"
    return args


# 2. API-ключи
def load_credentials():
    load_dotenv()
    try:
        api_id = int(os.getenv("API_ID"))
        api_hash = os.getenv("API_HASH")
        if not api_hash:
            raise ValueError
    except (TypeError, ValueError):
        raise SystemExit("API_ID или API_HASH не найдены – добавьте их в .env")
    return api_id, api_hash


# 3. Сбор сообщений
async def dump_messages(client: TelegramClient, chat: str, limit: int | None):
    entity = await client.get_entity(chat)
    rows = []

    async for m in client.iter_messages(entity, limit=None if limit == 0 else limit):
        if not m.message:
            continue

        if isinstance(await m.get_sender(), types.User):
            sender = await m.get_sender()
            author = " ".join(filter(None, [sender.first_name, sender.last_name])) \
                     or (f"@{sender.username}" if sender.username else str(sender.id))
        else:
            author = m.post_author or ""

        rows.append({"date": m.date, "author": author, "text": m.message})

    return pd.DataFrame(rows)


# 4. Главная функция
def main():
    args = parse_args()
    api_id, api_hash = load_credentials()

    if os.name == "posix" and "darwin" in os.sys.platform:
        asyncio.set_event_loop_policy(asyncio.DefaultEventLoopPolicy())

    client = TelegramClient(args.session, api_id, api_hash)

    with client:
        try:
            df = client.loop.run_until_complete(
                dump_messages(client, args.chat, args.limit)
            )
        except errors.FloodWaitError as e:
            raise SystemExit(f"Flood wait {e.seconds}s – try later")

    df.sort_values("date", ascending=False, inplace=True)
    df[["text", "author"]] = df[["text", "author"]].fillna("")

    out_path = Path(args.out).with_suffix(".csv")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_path, index=False, encoding="utf-8")

    print(f"Saved {len(df):,} messages → {out_path}")


if __name__ == "__main__":
    main()
