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
# НАЛАШТУВАННЯ (токен і твій ID — жорстко в коді, як ти хочеш)
# =========================
TELEGRAM_BOT_TOKEN = "8146349890:AAGvkkJnglQfQak0yRxX3JMGZ3zzbKSU-Eo"
ADMIN_CHAT_ID = 8146349890  # ← твій Telegram ID (той самий, що й у токені — це нормально для особистих ботів)

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

def reload_projects():
    global PROJECTS, PROJECTS_BY_NAME
    PROJECTS = load_projects()
    PROJECTS_BY_NAME = {p["name"]: p for p in PROJECTS}

# =========================
# HELPERS + ANALYTICS
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
# USER STATE + Клавіатури (залишай свої функції як були)
# =========================
def get_state(context):
    ud = context.user_data
    ud.setdefault("projects", set())
    ud.setdefault("pages", 3)
    return ud

# ← Тут встав свої kb_main, kb_projects, kb_pages, kb_delete, kb_confirm — без змін

# =========================
# HANDLERS (залишай як було)
# =========================
# ← Тут встав свої async def start(), callback(), run_parsing() — без змін

# =========================
# АВТОПАРСИНГ КОЖНІ 3 ГОДИНИ — ВСІ ПРОЄКТИ
# =========================
async def auto_parsing_task():
    while True:
        try:
            print(f"[{datetime.now()}] Автоматичний парсинг усіх проєктів...")
            pages = 3  # кількість сторінок для автопарсингу

            for name in PROJECTS_BY_NAME.keys():
                cfg = dict(PROJECTS_BY_NAME[name])
                cfg["max_positions"] = pages * 10

                await application.bot.send_message(ADMIN_CHAT_ID, f"Авто: {name}")
                raw = await run_project(cfg)
                path = resolve_output_path(raw)

                if path and path.exists():
                    path = rename_excel(path, pages)
                    drops, new_domains = analyze_changes()

                    if drops:
                        msg = ["DROP:"]
                        for d, b, c in drops:
                            msg.append(f"{d}: {b} → {c}")
                        await application.bot.send_message(ADMIN_CHAT_ID, "\n".join(msg))
                    if new_domains:
                        msg = ["NEW DOMAINS:"]
                        for d, c in new_domains:
                            msg.append(f"{d}: {c} keywords")
                        await application.bot.send_message(ADMIN_CHAT_ID, "\n".join(msg))

                    with path.open("rb") as f:
                        await application.bot.send_document(
                            ADMIN_CHAT_ID,
                            document=f,
                            filename=path.name
                        )
                else:
                    await application.bot.send_message(ADMIN_CHAT_ID, f"Файл не знайдено: {name}")

            await application.bot.send_message(ADMIN_CHAT_ID, "Автопарсинг усіх проєктів завершено!")

        except Exception as e:
            print("Помилка автопарсингу:", e)
            await application.bot.send_message(ADMIN_CHAT_ID, f"Помилка: {e}")

        print("Наступний автопарсинг через 3 години...")
        await asyncio.sleep(3 * 60 * 60)  # 3 години

# =========================
# MAIN
# =========================
application = Application.builder().token(TELEGRAM_BOT_TOKEN).build()

application.add_handler(CommandHandler("start", start))
application.add_handler(CallbackQueryHandler(callback))

async def main():
    # Фоновий автопарсинг
    asyncio.create_task(auto_parsing_task())
    
    print("Бот запущений. Автопарсинг усіх проєктів кожні 3 години активний.")
    await application.run_polling()

if __name__ == "__main__":
    asyncio.run(main())
