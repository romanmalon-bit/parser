import asyncio
import json
import logging
from collections import defaultdict
from pathlib import Path
from datetime import datetime
from typing import Optional

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    ContextTypes,
)

from parser_core import run_project, load_history

# =========================
# НАЛАШТУВАННЯ
# =========================
TELEGRAM_BOT_TOKEN = "8146349890:AAGvkkJnglQfQak0yRxX3JMGZ3zzbKSU-Eo"
ADMIN_CHAT_ID = 8146349890  # ← твій ID (заміни, якщо інший)

PROJECTS_FILE = "projects.json"
MIN_KEYWORDS_FOR_ALERT = 2
DROP_THRESHOLD = 0.5

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# =========================
# ПРОЄКТИ
# =========================
def load_projects():
    with open(PROJECTS_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)
    return data["projects"]

PROJECTS = load_projects()
PROJECTS_BY_NAME = {p["name"]: p for p in PROJECTS}

# =========================
# HELPERS + ANALYTICS (без змін)
# =========================
def resolve_output_path(raw) -> Optional[Path]:
    if isinstance(raw, (str, Path)):
        return Path(raw)
    if isinstance(raw, (list, tuple)) and raw:
        return Path(raw[0])
    if isinstance(raw, dict):
        for k in ("output_file", "excel_file", "path"):
            if k in raw:
                return Path(raw[k])
    return None

def rename_excel(path: Path, pages: int) -> Path:
    n = pages * 10
    ts = datetime.now().strftime("%Y%m%d_%H%M")
    new_path = path.with_name(f"serp_top_{n}_{ts}.xlsx")
    path.rename(new_path)
    return new_path

def analyze_changes():
    history = load_history()
    if len(history) < 2:
        return [], []
    prev, curr = history[-2], history[-1]
    def build(entry):
        mp = defaultdict(set)
        for r in entry.get("results", []):
            if r.get("Is_Target"):
                mp[r["Domain"]].add(r["Keyword"])
        return mp
    p, c = build(prev), build(curr)
    drops, new_domains = [], []
    for d, prev_kw in p.items():
        if len(prev_kw) < MIN_KEYWORDS_FOR_ALERT:
            continue
        curr_kw = c.get(d, set())
        lost = prev_kw - curr_kw
        if len(lost) >= len(prev_kw) * DROP_THRESHOLD:
            drops.append((d, len(prev_kw), len(curr_kw)))
    for d, curr_kw in c.items():
        if d not in p and len(curr_kw) >= MIN_KEYWORDS_FOR_ALERT:
            new_domains.append((d, len(curr_kw)))
    return drops, new_domains

# =========================
# АВТОПАРСИНГ — ТІЛЬКИ ПАРСИНГ, БЕЗ МЕНЮ
# =========================
async def auto_parsing_task():
    while True:
        try:
            print(f"[{datetime.now()}] Автоматичний парсинг усіх проєктів (топ-30)...")
            
            pages = 3  # саме 3 сторінки = топ-30
            projects_to_parse = list(PROJECTS_BY_NAME.keys())  # усі проєкти з projects.json
            
            if not projects_to_parse:
                await application.bot.send_message(ADMIN_CHAT_ID, "Немає проєктів у projects.json")
                await asyncio.sleep(3 * 60 * 60)
                continue

            for name in projects_to_parse:
                cfg = dict(PROJECTS_BY_NAME[name])
                cfg["max_positions"] = pages * 10

                print(f"Парсинг: {name}")
                raw = await run_project(cfg)
                path = resolve_output_path(raw)

                if not path or not path.exists():
                    await application.bot.send_message(ADMIN_CHAT_ID, f"Не вдалося створити файл для {name}")
                    continue

                path = rename_excel(path, pages)
                drops, new_domains = analyze_changes()

                # Коротке повідомлення
                msg = f"Готовий файл для {name} (топ-30)\n"
                if drops:
                    msg += "DROP: " + ", ".join([f"{d} ({b}→{c})" for d, b, c in drops]) + "\n"
                if new_domains:
                    msg += "NEW: " + ", ".join([f"{d} ({c} ключів)" for d, c in new_domains])
                await application.bot.send_message(ADMIN_CHAT_ID, msg)

                # Відправляємо файл
                with path.open("rb") as f:
                    await application.bot.send_document(
                        ADMIN_CHAT_ID,
                        document=f,
                        filename=path.name
                    )

            await application.bot.send_message(ADMIN_CHAT_ID, "Автопарсинг усіх проєктів завершено. Наступний через 3 години.")

        except Exception as e:
            print("Помилка автопарсингу:", e)
            await application.bot.send_message(ADMIN_CHAT_ID, f"Помилка автопарсингу: {e}")

        await asyncio.sleep(3 * 60 * 60)  # 3 години

# =========================
# MAIN
# =========================
application = Application.builder().token(TELEGRAM_BOT_TOKEN).build()

# Залишаємо меню тільки для ручного запуску
application.add_handler(CommandHandler("start", start))
application.add_handler(CallbackQueryHandler(callback))

async def main():
    # Запускаємо автопарсинг у фоні
    asyncio.create_task(auto_parsing_task())
    
    print("Бот запущений. Автоматичний парсинг усіх проєктів (топ-30) кожні 3 години активний.")
    await application.run_polling()

if __name__ == "__main__":
    asyncio.run(main())
