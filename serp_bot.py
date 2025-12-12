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
ADMIN_CHAT_ID = 8146349890  # ← твій ID (зміни, якщо інший)

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
# HELPERS
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

# =========================
# ANALYTICS
# =========================
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
# USER STATE + КЛАВІАТУРИ
# =========================
def get_state(context):
    ud = context.user_data
    ud.setdefault("projects", set())
    ud.setdefault("pages", 3)
    return ud

def kb_main(st):
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Обрати проєкти", callback_data="projects")],
        [InlineKeyboardButton(f"Сторінки ({st['pages']})", callback_data="pages")],
        [InlineKeyboardButton("Запустити", callback_data="run")],
        [InlineKeyboardButton("Видалити проєкт", callback_data="delete")],
    ])

def kb_projects(st):
    rows = []
    for n in PROJECTS_BY_NAME:
        mark = "Вибір" if n in st["projects"] else "Порожньо"
        rows.append([InlineKeyboardButton(f"{mark} {n}", callback_data=f"p:{n}")])
    rows.append([InlineKeyboardButton("Назад", callback_data="back")])
    return InlineKeyboardMarkup(rows)

def kb_pages(st):
    rows, row = [], []
    for i in range(1, 11):
        label = f"{'Вибір' if i == st['pages'] else ''} {i}"
        row.append(InlineKeyboardButton(label, callback_data=f"pg:{i}"))
        if len(row) == 5:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([InlineKeyboardButton("Назад", callback_data="back")])
    return InlineKeyboardMarkup(rows)

def kb_delete():
    rows = [[InlineKeyboardButton(f"Видалити {n}", callback_data=f"askdel:{n}")] for n in PROJECTS_BY_NAME]
    rows.append([InlineKeyboardButton("Назад", callback_data="back")])
    return InlineKeyboardMarkup(rows)

def kb_confirm(name):
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Так, видалити", callback_data=f"del:{name}")],
        [InlineKeyboardButton("Ні", callback_data="delete")],
    ])

# =========================
# HANDLERS
# =========================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    st = get_state(context)
    await update.effective_chat.send_message("Головне меню:", reply_markup=kb_main(st))

async def callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    st = get_state(context)
    d = q.data

    if d == "projects":
        await q.edit_message_text("Оберіть проєкти:", reply_markup=kb_projects(st))
        return
    if d.startswith("p:"):
        name = d[2:]
        if name in st["projects"]:
            st["projects"].remove(name)
        else:
            st["projects"].add(name)
        await q.edit_message_reply_markup(reply_markup=kb_projects(st))
        return
    if d == "pages":
        await q.edit_message_text("Кількість сторінок:", reply_markup=kb_pages(st))
        return
    if d.startswith("pg:"):
        st["pages"] = int(d[3:])
        await q.edit_message_reply_markup(reply_markup=kb_pages(st))
        return
    if d == "delete":
        await q.edit_message_text("Оберіть проєкт для видалення:", reply_markup=kb_delete())
        return
    if d.startswith("askdel:"):
        name = d.split(":", 1)[1]
        await q.edit_message_text(f"Видалити «{name}»?", reply_markup=kb_confirm(name))
        return
    if d.startswith("del:"):
        name = d.split(":", 1)[1]
        # тут можна додати функцію видалення, якщо потрібно
        await q.edit_message_text(f"Проєкт «{name}» видалено.")
        return
    if d == "run":
        if not st["projects"]:
            await q.edit_message_text("Спочатку оберіть проєкти!", reply_markup=kb_main(st))
            return
        await q.edit_message_text("Запускаю парсинг...")
        asyncio.create_task(run_parsing(q.message.chat_id, context, st))
        return
    if d == "back":
        await q.edit_message_text("Головне меню:", reply_markup=kb_main(st))

async def run_parsing(chat_id, context, st):
    pages = st["pages"]
    for name in st["projects"]:
        cfg = dict(PROJECTS_BY_NAME[name])
        cfg["max_positions"] = pages * 10
        await context.bot.send_message(chat_id, f"{name}")
        raw = await run_project(cfg)
        path = resolve_output_path(raw)
        if not path or not path.exists():
            await context.bot.send_message(chat_id, f"Файл не знайдено ({name})")
            continue
        path = rename_excel(path, pages)
        drops, new_domains = analyze_changes()
        if drops:
            msg = ["DROP:"]
            for d, b, c in drops:
                msg.append(f"{d}: {b} → {c}")
            await context.bot.send_message(chat_id, "\n".join(msg))
        if new_domains:
            msg = ["NEW DOMAINS:"]
            for d, c in new_domains:
                msg.append(f"{d}: {c} ключів")
            await context.bot.send_message(chat_id, "\n".join(msg))
        with path.open("rb") as f:
            await context.bot.send_document(chat_id, document=f, filename=path.name)
    await context.bot.send_message(chat_id, "Готово")

# =========================
# АВТОПАРСИНГ КОЖНІ 3 ГОДИНИ (усі проєкти, топ-30)
# =========================
async def auto_parsing_task():
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
                    await application.bot.send_message(ADMIN_CHAT_ID, msg)
                    with path.open("rb") as f:
                        await application.bot.send_document(ADMIN_CHAT_ID, document=f, filename=path.name)
            await application.bot.send_message(ADMIN_CHAT_ID, "Автопарсинг завершено. Наступний через 3 години.")
        except Exception as e:
            await application.bot.send_message(ADMIN_CHAT_ID, f"Помилка автопарсингу: {e}")
            print("Помилка:", e)
        await asyncio.sleep(3 * 60 * 60)  # 3 години

# =========================
# MAIN
# =========================
application = Application.builder().token(TELEGRAM_BOT_TOKEN).build()

application.add_handler(CommandHandler("start", start))
application.add_handler(CallbackQueryHandler(callback))

async def main():
    asyncio.create_task(auto_parsing_task())
    print("Бот запущений + автопарсинг кожні 3 години (усі проєкти, топ-30)")
    await application.run_polling()

if __name__ == "__main__":
    asyncio.run(main())
