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
ADMIN_CHAT_ID = 8146349890  # твій ID

PROJECTS_FILE = "projects.json"
MIN_KEYWORDS_FOR_ALERT = 2
DROP_THRESHOLD = 0.5

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# =========================
# ПРОЄКТИ + HELPERS + ANALYTICS (без змін)
# =========================
def load_projects():
    with open(PROJECTS_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)
    return data["projects"]

PROJECTS = load_projects()
PROJECTS_BY_NAME = {p["name"]: p for p in PROJECTS}

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
# КЛАВІАТУРИ + ХЕНДЛЕРИ (твої, без змін)
# =========================
def get_state(context):
    ud = context.user_data
    ud.setdefault("projects", set())
    ud.setdefault("pages", 3)
    return ud

# ← встав тут свої kb_main, kb_projects, kb_pages, kb_delete, kb_confirm (як у тебе було)

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    st = get_state(context)
    await update.effective_chat.send_message("Головне меню:", reply_markup=kb_main(st))

async def callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # ← встав сюди свій повний callback (той що був раніше)

async def run_parsing(chat_id, context, st):
    # ← встав сюди свій повний run_parsing (той що був раніше)

# =========================
# АВТОПАРСИНГ КОЖНІ 3 ГОДИНИ
# =========================
async def auto_parsing_task(app: Application):
    while True:
        try:
            print(f"[{datetime.now()}] Автопарсинг усіх проєктів (топ-30)...")
            pages = 3
            for name in PROJECTS_BY_NAME.keys():
                cfg = dict(PROJECTS_BY_NAME[name])
                cfg["max_positions"] = pages * 10
                raw = await run_project(cfg)
                path = resolve_output_path(raw)
                if path and path.exists():
                    path = rename_excel(path, pages)
                    drops, new_domains = analyze_changes()
                    msg = f"{name} (топ-30)"
                    if drops:
                        msg += "\nDROP: " + ", ".join([f"{d}({b}→{c})" for d, b, c in drops])
                    if new_domains:
                        msg += "\nNEW: " + ", ".join([f"{d}({c})" for d, c in new_domains])
                    await app.bot.send_message(ADMIN_CHAT_ID, msg)
                    with path.open("rb") as f:
                        await app.bot.send_document(ADMIN_CHAT_ID, document=f, filename=path.name)
            await app.bot.send_message(ADMIN_CHAT_ID, "Автопарсинг завершено. Наступний через 3 години.")
        except Exception as e:
            await app.bot.send_message(ADMIN_CHAT_ID, f"Помилка автопарсингу: {e}")
            print("Помилка:", e)
        await asyncio.sleep(3 * 60 * 60)

# =========================
# MAIN — ТЕПЕР БЕЗ asyncio.run!
# =========================
def main():
    app = Application.builder().token(TELEGRAM_BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CallbackQueryHandler(callback))

    # Запускаємо автопарсинг у фоні через job_queue (найнадійніший спосіб)
    app.job_queue.run_repeating(
        callback=lambda context: asyncio.create_task(auto_parsing_task(app)),
        interval=10800,  # 3 години
        first=10         # перший запуск через 10 секунд
    )

    print("Бот запущений + автопарсинг кожні 3 години (усі проєкти, топ-30)")
    app.run_polling()

if __name__ == "__main__":
    main()
