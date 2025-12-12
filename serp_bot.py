import asyncio
import json
import logging
import os
from collections import defaultdict
from pathlib import Path
from datetime import datetime
from typing import Optional, List

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    ContextTypes,
    ConversationHandler,
    MessageHandler,
    filters,
)

from parser_core import run_project, load_history, save_history

# =========================
# НАЛАШТУВАННЯ (ТВОЇ)
# =========================
TELEGRAM_BOT_TOKEN = "8146349890:AAGvkkJnglQfQak0yRxX3JMGZ3zzbKSU-Eo"
ADMIN_CHAT_ID = 512739407

PROJECTS_FILE = "projects.json"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# =========================
# СТАН ДОДАВАННЯ ПРОЄКТУ
# =========================
(
    NAME, LOCATION, LANGUAGE, API_KEYS,
    TARGET_DOMAINS, KEYWORDS,
    OUTPUT_PREFIX, HISTORY_FILE
) = range(8)

# =========================
# ПРОЄКТИ
# =========================
def load_projects() -> List[dict]:
    if not os.path.exists(PROJECTS_FILE):
        with open(PROJECTS_FILE, "w", encoding="utf-8") as f:
            json.dump({"projects": []}, f, ensure_ascii=False, indent=2)
        return []
    with open(PROJECTS_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)
    return data.get("projects", [])

def save_projects(projects: List[dict]):
    with open(PROJECTS_FILE, "w", encoding="utf-8") as f:
        json.dump({"projects": projects}, f, ensure_ascii=False, indent=2)

PROJECTS = load_projects()
PROJECTS_BY_NAME = {p["name"]: p for p in PROJECTS}

def reload_projects():
    global PROJECTS, PROJECTS_BY_NAME
    PROJECTS = load_projects()
    PROJECTS_BY_NAME = {p["name"]: p for p in PROJECTS}

# =========================
# STATE (ТВОЯ ЛОГІКА)
# =========================
def get_state(context: ContextTypes.DEFAULT_TYPE):
    if "state" not in context.user_data:
        context.user_data["state"] = {
            "pages": 3,
            "projects": [],
        }
    return context.user_data["state"]

# =========================
# КЛАВІАТУРА
# =========================
def kb_main(st):
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🧩 Виберіть проєкти", callback_data="projects")],
        [InlineKeyboardButton(f"📄 Сторінки: {st['pages']} (топ {st['pages']*10})", callback_data="pages")],
        [InlineKeyboardButton("▶️ Запустити парсинг", callback_data="run")],
        [InlineKeyboardButton("➕ Додати новий проєкт", callback_data="add_project")],
        [InlineKeyboardButton("🗑 Видалити проєкт", callback_data="delete")],
        [InlineKeyboardButton("ℹ️ Довідка", callback_data="info")],
    ])

def kb_projects(st):
    buttons = []
    for p in PROJECTS:
        name = p["name"]
        mark = "✅" if name in st["projects"] else "☑️"
        buttons.append([InlineKeyboardButton(f"{mark} {name}", callback_data=f"toggle:{name}")])
    buttons.append([InlineKeyboardButton("⬅️ Назад", callback_data="back")])
    return InlineKeyboardMarkup(buttons)

def kb_pages():
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("1", callback_data="setpages:1"),
            InlineKeyboardButton("2", callback_data="setpages:2"),
            InlineKeyboardButton("3", callback_data="setpages:3"),
        ],
        [
            InlineKeyboardButton("4", callback_data="setpages:4"),
            InlineKeyboardButton("5", callback_data="setpages:5"),
        ],
        [InlineKeyboardButton("⬅️ Назад", callback_data="back")],
    ])

def kb_delete():
    buttons = []
    for p in PROJECTS:
        buttons.append([InlineKeyboardButton(f"🗑 {p['name']}", callback_data=f"del:{p['name']}")])
    buttons.append([InlineKeyboardButton("⬅️ Назад", callback_data="back")])
    return InlineKeyboardMarkup(buttons)

# =========================
# /start
# =========================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    st = get_state(context)
    await update.effective_chat.send_message(
        "Привіт! Це бот для парсингу SERP.",
        reply_markup=kb_main(st)
    )

# =========================
# ✅ CALLBACK — ГОЛОВНЕ ВИПРАВЛЕННЯ
# =========================
async def callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    st = get_state(context)
    data = query.data

    if data == "projects":
        reload_projects()
        await query.edit_message_text("Виберіть проєкти:", reply_markup=kb_projects(st))

    elif data.startswith("toggle:"):
        name = data.split(":", 1)[1]
        if name in st["projects"]:
            st["projects"].remove(name)
        else:
            st["projects"].append(name)
        await query.edit_message_reply_markup(reply_markup=kb_projects(st))

    elif data == "pages":
        await query.edit_message_text("Оберіть кількість сторінок:", reply_markup=kb_pages())

    elif data.startswith("setpages:"):
        st["pages"] = int(data.split(":")[1])
        await query.edit_message_text("Оновлено.", reply_markup=kb_main(st))

    elif data == "run":
        await query.edit_message_text("⏳ Запуск парсингу…")
        for name in st["projects"]:
            project = PROJECTS_BY_NAME.get(name)
            if project:
                await run_project(project, pages=st["pages"])
        await query.edit_message_text("✅ Готово.", reply_markup=kb_main(st))

    elif data == "add_project":
        await query.edit_message_text("Запусти команду /addproject")

    elif data == "delete":
        await query.edit_message_text("Оберіть проєкт для видалення:", reply_markup=kb_delete())

    elif data.startswith("del:"):
        name = data.split(":", 1)[1]
        projects = load_projects()
        projects = [p for p in projects if p["name"] != name]
        save_projects(projects)
        reload_projects()
        await query.edit_message_text(f"Проєкт «{name}» видалено.", reply_markup=kb_main(st))

    elif data == "info":
        await query.edit_message_text("ℹ️ /start /addproject /cancel", reply_markup=kb_main(st))

    elif data == "back":
        await query.edit_message_text("Меню:", reply_markup=kb_main(st))

# =========================
# Conversation: ДОДАВАННЯ ПРОЄКТУ
# =========================
async def start_add_project(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["new_project"] = {}
    await update.message.reply_text("Крок 1: Назва проєкту")
    return NAME

async def get_name(update, context):
    context.user_data["new_project"]["name"] = update.message.text
    await update.message.reply_text("Крок 2: Країна")
    return LOCATION

async def get_location(update, context):
    context.user_data["new_project"]["location"] = update.message.text
    await update.message.reply_text("Крок 3: Мова (hl/gl)")
    return LANGUAGE

async def get_language(update, context):
    lang = update.message.text
    context.user_data["new_project"]["hl"] = lang
    context.user_data["new_project"]["gl"] = lang
    await update.message.reply_text("Крок 4: API ключі")
    return API_KEYS

async def get_api_keys(update, context):
    context.user_data["new_project"]["api_keys"] = update.message.text.split(",")
    await update.message.reply_text("Крок 5: Домени")
    return TARGET_DOMAINS

async def get_target_domains(update, context):
    context.user_data["new_project"]["target_domains"] = update.message.text.split(",")
    await update.message.reply_text("Крок 6: Ключові слова")
    return KEYWORDS

async def get_keywords(update, context):
    context.user_data["new_project"]["keywords"] = update.message.text.split(",")
    await update.message.reply_text("Крок 7: output_prefix")
    return OUTPUT_PREFIX

async def get_output_prefix(update, context):
    context.user_data["new_project"]["output_prefix"] = update.message.text
    await update.message.reply_text("Крок 8: history_file")
    return HISTORY_FILE

async def get_history_file(update, context):
    project = context.user_data["new_project"]
    project["history_file"] = update.message.text

    projects = load_projects()
    projects.append(project)
    save_projects(projects)
    reload_projects()

    Path(project["history_file"]).touch(exist_ok=True)
    await update.message.reply_text("✅ Проєкт додано.", reply_markup=kb_main(get_state(context)))
    return ConversationHandler.END

async def cancel_add_project(update, context):
    await update.message.reply_text("❌ Скасовано.", reply_markup=kb_main(get_state(context)))
    return ConversationHandler.END

# =========================
# AUTO PARSING
# =========================
async def auto_parsing_task(context: ContextTypes.DEFAULT_TYPE):
    reload_projects()
    for project in PROJECTS:
        await run_project(project, pages=3)

# =========================
# MAIN
# =========================
def main():
    app = Application.builder().token(TELEGRAM_BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CallbackQueryHandler(callback))

    add_conv = ConversationHandler(
        entry_points=[CommandHandler("addproject", start_add_project)],
        states={
            NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, get_name)],
            LOCATION: [MessageHandler(filters.TEXT & ~filters.COMMAND, get_location)],
            LANGUAGE: [MessageHandler(filters.TEXT & ~filters.COMMAND, get_language)],
            API_KEYS: [MessageHandler(filters.TEXT & ~filters.COMMAND, get_api_keys)],
            TARGET_DOMAINS: [MessageHandler(filters.TEXT & ~filters.COMMAND, get_target_domains)],
            KEYWORDS: [MessageHandler(filters.TEXT & ~filters.COMMAND, get_keywords)],
            OUTPUT_PREFIX: [MessageHandler(filters.TEXT & ~filters.COMMAND, get_output_prefix)],
            HISTORY_FILE: [MessageHandler(filters.TEXT & ~filters.COMMAND, get_history_file)],
        },
        fallbacks=[CommandHandler("cancel", cancel_add_project)],
    )
    app.add_handler(add_conv)

    app.job_queue.run_repeating(auto_parsing_task, interval=10800, first=15)
    app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
