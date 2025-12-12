import asyncio
import json
import logging
from collections import defaultdict
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, List, Optional

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

def delete_project(name: str) -> bool:
    with open(PROJECTS_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)

    before = len(data["projects"])
    data["projects"] = [p for p in data["projects"] if p["name"] != name]

    if len(data["projects"]) == before:
        return False

    with open(PROJECTS_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    reload_projects()
    return True

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
# ANALYTICS (DROP + NEW)
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
# USER STATE
# =========================

def get_state(context):
    ud = context.user_data
    ud.setdefault("projects", set())
    ud.setdefault("pages", 3)
    return ud

# =========================
# KEYBOARDS
# =========================

def kb_main(st):
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🧩 Обрати проєкти", callback_data="projects")],
        [InlineKeyboardButton(f"📄 Сторінки ({st['pages']})", callback_data="pages")],
        [InlineKeyboardButton("▶️ Запустити", callback_data="run")],
        [InlineKeyboardButton("🗑 Видалити проєкт", callback_data="delete")],
    ])

def kb_projects(st):
    rows = []
    for n in PROJECTS_BY_NAME:
        mark = "✅" if n in st["projects"] else "⚪"
        rows.append([InlineKeyboardButton(f"{mark} {n}", callback_data=f"p:{n}")])
    rows.append([InlineKeyboardButton("⬅ Назад", callback_data="back")])
    return InlineKeyboardMarkup(rows)

def kb_pages(st):
    rows, row = [], []
    for i in range(1, 11):
        label = f"{'✅' if i == st['pages'] else ''} {i}"
        row.append(InlineKeyboardButton(label, callback_data=f"pg:{i}"))
        if len(row) == 5:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([InlineKeyboardButton("⬅ Назад", callback_data="back")])
    return InlineKeyboardMarkup(rows)

def kb_delete():
    rows = [[InlineKeyboardButton(f"🗑 {n}", callback_data=f"askdel:{n}")] for n in PROJECTS_BY_NAME]
    rows.append([InlineKeyboardButton("⬅ Назад", callback_data="back")])
    return InlineKeyboardMarkup(rows)

def kb_confirm(name):
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ Так, видалити", callback_data=f"del:{name}")],
        [InlineKeyboardButton("❌ Назад", callback_data="delete")],
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
        st["projects"].toggle(name) if hasattr(st["projects"], "toggle") else (
            st["projects"].remove(name) if name in st["projects"] else st["projects"].add(name)
        )
        await q.edit_message_reply_markup(kb_projects(st))
        return

    if d == "pages":
        await q.edit_message_text("Оберіть кількість сторінок:", reply_markup=kb_pages(st))
        return

    if d.startswith("pg:"):
        st["pages"] = int(d[3:])
        await q.edit_message_reply_markup(kb_pages(st))
        return

    if d == "delete":
        await q.edit_message_text("Оберіть проєкт:", reply_markup=kb_delete())
        return

    if d.startswith("askdel:"):
        name = d.split(":", 1)[1]
        await q.edit_message_text(f"Точно видалити «{name}»?", reply_markup=kb_confirm(name))
        return

    if d.startswith("del:"):
        name = d.split(":", 1)[1]
        delete_project(name)
        st["projects"].discard(name)
        await q.edit_message_text("Проєкт видалено.", reply_markup=kb_delete())
        return

    if d == "run":
        if not st["projects"]:
            await q.edit_message_text("⚠️ Спочатку оберіть проєкти.", reply_markup=kb_main(st))
            return
        await q.edit_message_text("🚀 Запускаю парсинг...")
        asyncio.create_task(run_parsing(q.message.chat_id, context, st))
        return

    if d == "back":
        await q.edit_message_text("Головне меню:", reply_markup=kb_main(st))

# =========================
# PARSING
# =========================

async def run_parsing(chat_id, context, st):
    pages = st["pages"]
    for name in st["projects"]:
        cfg = dict(PROJECTS_BY_NAME[name])
        cfg["max_positions"] = pages * 10

        await context.bot.send_message(chat_id, f"⏳ {name}")
        raw = await run_project(cfg)

        path = resolve_output_path(raw)
        if not path or not path.exists():
            await context.bot.send_message(chat_id, f"❌ Файл не знайдено ({name})")
            continue

        path = rename_excel(path, pages)

        drops, new_domains = analyze_changes()

        if drops:
            msg = ["⚠️ DROP:"]
            for d, b, c in drops:
                msg.append(f"{d}: {b} → {c}")
            await context.bot.send_message(chat_id, "\n".join(msg))

        if new_domains:
            msg = ["🆕 NEW DOMAINS:"]
            for d, c in new_domains:
                msg.append(f"{d}: {c} keywords")
            await context.bot.send_message(chat_id, "\n".join(msg))

        with path.open("rb") as f:
            await context.bot.send_document(chat_id, document=f, filename=path.name)

    await context.bot.send_message(chat_id, "🏁 Готово")

# =========================
# MAIN
# =========================

def main():
    app = Application.builder().token(TELEGRAM_BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CallbackQueryHandler(callback))
    app.run_polling()

if __name__ == "__main__":
    main()
