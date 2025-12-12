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
# НАЛАШТУВАННЯ
# =========================
TELEGRAM_BOT_TOKEN = "8146349890:AAGvkkJnglQfQak0yRxX3JMGZ3zzbKSU-Eo"
ADMIN_CHAT_ID = 512739407  # Твій ID — сюди приходять алерти

PROJECTS_FILE = "projects.json"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# =========================
# СТАН ДОДАВАННЯ ПРОЄКТУ
# =========================
(
    NAME, LOCATION, LANGUAGE, API_KEYS, TARGET_DOMAINS, KEYWORDS, OUTPUT_PREFIX, HISTORY_FILE
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
# ЛОГУВАННЯ ПОМИЛОК
# =========================
async def send_error_to_admin(context: ContextTypes.DEFAULT_TYPE, error_text: str):
    try:
        await context.bot.send_message(
            ADMIN_CHAT_ID,
            f"🚨 ПОМИЛКА В БОТІ:\n{error_text}\nЧас: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        )
    except Exception as e:
        print("Не вдалося надіслати помилку адміну:", e)

# =========================
# ДОДАВАННЯ ПРОЄКТУ КРОК ЗА КРОКОМ
# =========================
async def start_add_project(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Почнемо додавання нового проєкту!\n\nКрок 1: Введіть назву проєкту (наприклад: FR Drops)")
    context.user_data["new_project"] = {}
    return NAME

async def get_name(update: Update, context: ContextTypes.DEFAULT_TYPE):
    name = update.message.text.strip()
    if name in PROJECTS_BY_NAME:
        await update.message.reply_text(f"Проєкт з назвою «{name}» вже існує. Спробуйте іншу назву.")
        return NAME
    context.user_data["new_project"]["name"] = name
    await update.message.reply_text(f"Назва: {name}\n\nКрок 2: Введіть країну (location, наприклад: France)")
    return LOCATION

async def get_location(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["new_project"]["location"] = update.message.text.strip()
    await update.message.reply_text(f"Країна: {update.message.text}\n\nКрок 3: Введіть код мови (hl та gl, наприклад: fr)")
    return LANGUAGE

async def get_language(update: Update, context: ContextTypes.DEFAULT_TYPE):
    lang = update.message.text.strip()
    context.user_data["new_project"]["hl"] = lang
    context.user_data["new_project"]["gl"] = lang
    await update.message.reply_text(f"Мова: {lang}\n\nКрок 4: Введіть API ключі (через кому, якщо кілька)")
    return API_KEYS

async def get_api_keys(update: Update, context: ContextTypes.DEFAULT_TYPE):
    keys = [k.strip() for k in update.message.text.split(",") if k.strip()]
    context.user_data["new_project"]["api_keys"] = keys
    await update.message.reply_text(f"Ключів: {len(keys)}\n\nКрок 5: Введіть таргет-домени (по одному на рядок або через кому)")
    return TARGET_DOMAINS

async def get_target_domains(update: Update, context: ContextTypes.DEFAULT_TYPE):
    domains = [d.strip() for d in update.message.text.replace(",", "\n").split("\n") if d.strip()]
    context.user_data["new_project"]["target_domains"] = domains
    await update.message.reply_text(f"Доменів: {len(domains)}\n\nКрок 6: Введіть ключові слова (по одному на рядок або через кому)")
    return KEYWORDS

async def get_keywords(update: Update, context: ContextTypes.DEFAULT_TYPE):
    keywords = [k.strip() for k in update.message.text.replace(",", "\n").split("\n") if k.strip()]
    context.user_data["new_project"]["keywords"] = keywords
    await update.message.reply_text(f"Ключів: {len(keywords)}\n\nКрок 7: Введіть префікс вихідного файлу (наприклад: serp_top30_FR)")
    return OUTPUT_PREFIX

async def get_output_prefix(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["new_project"]["output_prefix"] = update.message.text.strip()
    await update.message.reply_text(f"Префікс: {update.message.text}\n\nКрок 8: Введіть ім'я файлу історії (наприклад: serp_history_FR2.json)")
    return HISTORY_FILE

async def get_history_file(update: Update, context: ContextTypes.DEFAULT_TYPE):
    history_file = update.message.text.strip()
    context.user_data["new_project"]["history_file"] = history_file

    # Створюємо новий проєкт
    new_project = context.user_data["new_project"]

    # Додаємо в projects.json
    PROJECTS.append(new_project)
    save_projects(PROJECTS)
    reload_projects()

    # Створюємо порожній файл історії, якщо не існує
    history_path = Path(history_file)
    if not history_path.exists():
        history_path.write_text(json.dumps([], ensure_ascii=False, indent=2), encoding="utf-8")

    await update.message.reply_text(
        f"Проєкт «{new_project['name']}» успішно додано!\n"
        f"Тепер доступний для парсингу (ручного та автоматичного).\n"
        "Повертаюсь у головне меню.",
        reply_markup=kb_main(get_state(context))
    )

    context.user_data.clear()
    return ConversationHandler.END

async def cancel_add_project(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Додавання проєкту скасовано.", reply_markup=kb_main(get_state(context)))
    context.user_data.clear()
    return ConversationHandler.END

# =========================
# КЛАВІАТУРИ (з кнопкою додавання)
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

# ... (інші клавіатури kb_projects, kb_pages, kb_delete, kb_confirm — як у твоєму оригінальному коді)

# =========================
# HANDLERS (start визначено перед додаванням)
# =========================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    st = get_state(context)
    await update.effective_chat.send_message(
        "Привіт! Це бот для парсингу SERP.\n"
        "- Авто-парсинг усіх проєктів (топ-30) кожні 3 години.\n"
        "- Ручний парсинг: виберіть проєкти/сторінки і запустіть.\n"
        "Оберіть опцію в меню:",
        reply_markup=kb_main(st)
    )

# ... (інші хендлери callback, run_parsing, analyze_changes тощо — як у твоєму оригінальному коді)

# =========================
# MAIN
# =========================
def main():
    app = Application.builder().token(TELEGRAM_BOT_TOKEN).build()

    # Додаємо хендлер для меню
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CallbackQueryHandler(callback))

    # Покрокове додавання проєкту
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

    # Автопарсинг (топ-30, кожні 3 години)
    app.job_queue.run_repeating(auto_parsing_task, interval=10800, first=15)

    print("Бот запущений з покроковим додаванням проєктів та логуванням помилок.")
    app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
