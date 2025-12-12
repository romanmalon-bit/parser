import asyncio
import json
import logging
import os
from pathlib import Path
from datetime import datetime
from time import perf_counter
from typing import List

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

from parser_core import run_project

# =========================
# НАЛАШТУВАННЯ
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
# STATE (меню)
# =========================
def get_state(context: ContextTypes.DEFAULT_TYPE):
    if "state" not in context.user_data:
        context.user_data["state"] = {"pages": 3, "projects": []}
    return context.user_data["state"]

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
        logger.error("Не вдалося надіслати помилку адміну: %s", e)

# =========================
# XLSX SCAN/SEND
# =========================
def _scan_xlsx_files() -> dict:
    files = {}
    base = Path(".")
    for p in base.rglob("*.xlsx"):
        try:
            files[str(p.resolve())] = p.stat().st_mtime
        except Exception:
            pass
    return files

def _diff_new_xlsx(before: dict, after: dict, min_mtime: float) -> List[str]:
    out = []
    for path, mtime in after.items():
        if mtime >= min_mtime and (path not in before or before[path] < mtime):
            out.append(path)
    out.sort(key=lambda x: after.get(x, 0), reverse=True)
    return out

async def send_xlsx_files(context: ContextTypes.DEFAULT_TYPE, chat_id: int, paths: List[str], caption_prefix: str = ""):
    if not paths:
        return
    for p in paths[:5]:
        try:
            file_path = Path(p)
            if not file_path.exists():
                continue
            caption = f"{caption_prefix}{file_path.name}".strip()
            with file_path.open("rb") as f:
                await context.bot.send_document(chat_id=chat_id, document=f, caption=caption[:1024])
        except Exception as e:
            logger.error("Не вдалося надіслати файл %s: %s", p, e)

# =========================
# ✅ RUN PROJECT SAFE (передає pages у project_config)
# =========================
async def run_project_safe(project: dict, pages: int):
    before = _scan_xlsx_files()
    start_wall = datetime.now().timestamp()
    t0 = perf_counter()

    # ✅ Гарантовано передаємо pages у parser_core
    project_cfg = dict(project)
    project_cfg["pages"] = pages

    res = run_project(project_cfg)
    if asyncio.iscoroutine(res):
        await res

    after = _scan_xlsx_files()
    dt = perf_counter() - t0
    new_files = _diff_new_xlsx(before, after, min_mtime=start_wall)
    return dt, new_files

# =========================
# КЛАВІАТУРИ
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
            InlineKeyboardButton("4", callback_data="setpages:4"),
            InlineKeyboardButton("5", callback_data="setpages:5"),
        ],
        [
            InlineKeyboardButton("6", callback_data="setpages:6"),
            InlineKeyboardButton("7", callback_data="setpages:7"),
            InlineKeyboardButton("8", callback_data="setpages:8"),
            InlineKeyboardButton("9", callback_data="setpages:9"),
            InlineKeyboardButton("10", callback_data="setpages:10"),
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
# HANDLERS
# =========================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    st = get_state(context)
    await update.effective_chat.send_message(
        "Привіт! Це бот для парсингу SERP.\nОберіть опцію в меню:",
        reply_markup=kb_main(st)
    )

# =========================
# CALLBACK
# =========================
async def callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    st = get_state(context)
    data = query.data
    chat_id = query.message.chat_id

    if data == "projects":
        reload_projects()
        await query.edit_message_text("Виберіть проєкти:", reply_markup=kb_projects(st))
        return

    if data.startswith("toggle:"):
        name = data.split(":", 1)[1]
        if name in st["projects"]:
            st["projects"].remove(name)
        else:
            st["projects"].append(name)
        await query.edit_message_reply_markup(reply_markup=kb_projects(st))
        return

    if data == "pages":
        await query.edit_message_text("Оберіть кількість сторінок:", reply_markup=kb_pages())
        return

    if data.startswith("setpages:"):
        st["pages"] = int(data.split(":")[1])
        await query.edit_message_text("Оновлено.", reply_markup=kb_main(st))
        return

    if data == "run":
        if not st["projects"]:
            await query.edit_message_text("Спочатку оберіть хоча б один проєкт.", reply_markup=kb_main(st))
            return

        pages = st["pages"]
        await query.edit_message_text(
            f"⏳ Старт ручного парсингу\n"
            f"Проєктів: {len(st['projects'])}\n"
            f"Сторінок: {pages} (топ {pages*10})"
        )

        total_sent = 0

        for i, name in enumerate(st["projects"], start=1):
            project = PROJECTS_BY_NAME.get(name)
            if not project:
                await context.bot.send_message(chat_id=chat_id, text=f"⚠️ [{i}/{len(st['projects'])}] Проєкт «{name}» не знайдено у projects.json")
                continue

            await context.bot.send_message(chat_id=chat_id, text=f"▶️ [{i}/{len(st['projects'])}] Парсю «{name}»… (сторінок: {pages})")

            try:
                dt, new_xlsx = await run_project_safe(project, pages=pages)

                if new_xlsx:
                    await context.bot.send_message(chat_id=chat_id, text=f"✅ «{name}» готово за {dt:.1f} сек. Excel: {len(new_xlsx)}")
                    await send_xlsx_files(context, chat_id, new_xlsx, caption_prefix=f"{name} — ")
                    total_sent += min(len(new_xlsx), 5)
                else:
                    await context.bot.send_message(
                        chat_id=chat_id,
                        text=(
                            f"✅ «{name}» готово за {dt:.1f} сек.\n"
                            f"⚠️ Excel (*.xlsx) не знайдено після парсингу.\n"
                            f"Я шукав файли рекурсивно в корені проекту."
                        )
                    )

            except Exception as e:
                err = f"Run project failed ({name}): {e}"
                logger.exception(err)
                await send_error_to_admin(context, err)
                await context.bot.send_message(chat_id=chat_id, text=f"🚨 Помилка в «{name}»: {e}")

        await context.bot.send_message(chat_id=chat_id, text=f"🏁 Ручний парсинг завершено. Надіслано файлів: {total_sent}")
        await context.bot.send_message(chat_id=chat_id, text="Меню:", reply_markup=kb_main(st))
        return

    if data == "add_project":
        await query.edit_message_text("Запусти команду /addproject")
        return

    if data == "delete":
        reload_projects()
        await query.edit_message_text("Оберіть проєкт для видалення:", reply_markup=kb_delete())
        return

    if data.startswith("del:"):
        name = data.split(":", 1)[1]
        projects = load_projects()
        projects = [p for p in projects if p["name"] != name]
        save_projects(projects)
        reload_projects()

        if name in st["projects"]:
            st["projects"].remove(name)

        await query.edit_message_text(f"Проєкт «{name}» видалено.", reply_markup=kb_main(st))
        return

    if data == "info":
        await query.edit_message_text("ℹ️ /start /addproject /cancel", reply_markup=kb_main(st))
        return

    if data == "back":
        await query.edit_message_text("Меню:", reply_markup=kb_main(st))
        return

    await query.edit_message_text(f"Невідома дія: {data}", reply_markup=kb_main(st))

# =========================
# Conversation: ДОДАВАННЯ ПРОЄКТУ
# =========================
async def start_add_project(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Почнемо додавання нового проєкту!\n\nКрок 1: Введіть назву проєкту")
    context.user_data["new_project"] = {}
    return NAME

async def get_name(update: Update, context: ContextTypes.DEFAULT_TYPE):
    reload_projects()
    name = update.message.text.strip()
    if name in PROJECTS_BY_NAME:
        await update.message.reply_text(f"Проєкт з назвою «{name}» вже існує. Спробуйте іншу назву.")
        return NAME
    context.user_data["new_project"]["name"] = name
    await update.message.reply_text("Крок 2: Введіть країну (location, наприклад: France)")
    return LOCATION

async def get_location(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["new_project"]["location"] = update.message.text.strip()
    await update.message.reply_text("Крок 3: Введіть код мови (hl та gl, наприклад: fr)")
    return LANGUAGE

async def get_language(update: Update, context: ContextTypes.DEFAULT_TYPE):
    lang = update.message.text.strip()
    context.user_data["new_project"]["hl"] = lang
    context.user_data["new_project"]["gl"] = lang
    await update.message.reply_text("Крок 4: Введіть API ключі (через кому, якщо кілька)")
    return API_KEYS

async def get_api_keys(update: Update, context: ContextTypes.DEFAULT_TYPE):
    keys = [k.strip() for k in update.message.text.split(",") if k.strip()]
    context.user_data["new_project"]["api_keys"] = keys
    await update.message.reply_text("Крок 5: Введіть таргет-домени (по одному на рядок або через кому)")
    return TARGET_DOMAINS

async def get_target_domains(update: Update, context: ContextTypes.DEFAULT_TYPE):
    domains = [d.strip() for d in update.message.text.replace(",", "\n").split("\n") if d.strip()]
    context.user_data["new_project"]["target_domains"] = domains
    await update.message.reply_text("Крок 6: Введіть ключові слова (по одному на рядок або через кому)")
    return KEYWORDS

async def get_keywords(update: Update, context: ContextTypes.DEFAULT_TYPE):
    keywords = [k.strip() for k in update.message.text.replace(",", "\n").split("\n") if k.strip()]
    context.user_data["new_project"]["keywords"] = keywords
    await update.message.reply_text("Крок 7: Введіть префікс вихідного файлу (наприклад: serp_top30_FR)")
    return OUTPUT_PREFIX

async def get_output_prefix(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["new_project"]["output_prefix"] = update.message.text.strip()
    await update.message.reply_text("Крок 8: Введіть ім'я файлу історії (наприклад: serp_history_FR.json)")
    return HISTORY_FILE

async def get_history_file(update: Update, context: ContextTypes.DEFAULT_TYPE):
    history_file = update.message.text.strip()
    context.user_data["new_project"]["history_file"] = history_file

    new_project = context.user_data["new_project"]

    projects = load_projects()
    projects.append(new_project)
    save_projects(projects)
    reload_projects()

    history_path = Path(history_file)
    if not history_path.exists():
        history_path.write_text(json.dumps([], ensure_ascii=False, indent=2), encoding="utf-8")

    await update.message.reply_text(
        f"✅ Проєкт «{new_project['name']}» успішно додано!\nПовертаюсь у меню.",
        reply_markup=kb_main(get_state(context))
    )

    context.user_data.pop("new_project", None)
    return ConversationHandler.END

async def cancel_add_project(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("❌ Додавання проєкту скасовано.", reply_markup=kb_main(get_state(context)))
    context.user_data.pop("new_project", None)
    return ConversationHandler.END

# =========================
# AUTO PARSING (топ-30 кожні 3 години)
# =========================
async def auto_parsing_task(context: ContextTypes.DEFAULT_TYPE):
    try:
        reload_projects()
        if not PROJECTS:
            return

        await context.bot.send_message(chat_id=ADMIN_CHAT_ID, text=f"🤖 Автопарсинг стартував. Проєктів: {len(PROJECTS)} (топ 30 / pages=3)")

        for i, project in enumerate(PROJECTS, start=1):
            name = project.get("name", "Unnamed")
            try:
                await context.bot.send_message(chat_id=ADMIN_CHAT_ID, text=f"▶️ [{i}/{len(PROJECTS)}] Парсю «{name}»…")
                dt, new_xlsx = await run_project_safe(project, pages=3)

                if new_xlsx:
                    await context.bot.send_message(chat_id=ADMIN_CHAT_ID, text=f"✅ «{name}» готово за {dt:.1f} сек. Excel: {len(new_xlsx)}")
                    await send_xlsx_files(context, ADMIN_CHAT_ID, new_xlsx, caption_prefix=f"AUTO {name} — ")
                else:
                    await context.bot.send_message(chat_id=ADMIN_CHAT_ID, text=f"✅ «{name}» готово за {dt:.1f} сек, але Excel (*.xlsx) не знайдено.")

            except Exception as e:
                err = f"Auto parsing failed ({name}): {e}"
                logger.exception(err)
                await send_error_to_admin(context, err)

        await context.bot.send_message(chat_id=ADMIN_CHAT_ID, text="🏁 Автопарсинг завершено.")

    except Exception as e:
        err = f"auto_parsing_task crashed: {e}"
        logger.exception(err)
        await send_error_to_admin(context, err)

# =========================
# ERROR HANDLER
# =========================
async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    logger.exception("Unhandled exception:", exc_info=context.error)
    try:
        await send_error_to_admin(context, str(context.error))
    except Exception:
        pass

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

    app.add_error_handler(error_handler)

    # Автопарсинг кожні 3 години (pages=3)
    app.job_queue.run_repeating(auto_parsing_task, interval=10800, first=15)

    logger.info("Бот запущений.")
    app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
