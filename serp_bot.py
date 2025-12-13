import asyncio
import json
import logging
import os
from pathlib import Path
from datetime import datetime
from typing import List, Optional, Dict, Tuple

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
from openpyxl import load_workbook, Workbook
from openpyxl.utils import get_column_letter
from parser_core import run_project  # НЕ чіпаємо parser_core.py

# =========================
# НАЛАШТУВАННЯ
# =========================
TELEGRAM_BOT_TOKEN = "8146349890:AAGvkkJnglQfQak0yRxX3JMGZ3zzbKSU-Eo"
PROJECTS_FILE = "projects.json"
USERS_FILE = "users.txt"
ADMIN_FILE = "admin_chat_id.txt"
LAST_HISTORY_DIR = "last_history"  # Папка для зберігання останньої історії кожного проєкту

DEFAULT_ADMIN_CHAT_ID = 909587225
ADMIN_CHAT_ID = int(os.getenv("ADMIN_CHAT_ID", str(DEFAULT_ADMIN_CHAT_ID)))

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# =========================
# ADMIN & USERS
# =========================
def load_admin_chat_id() -> int:
    if ADMIN_CHAT_ID:
        return ADMIN_CHAT_ID
    try:
        if os.path.exists(ADMIN_FILE):
            return int(Path(ADMIN_FILE).read_text(encoding="utf-8").strip())
    except Exception:
        pass
    return 0

def save_admin_chat_id(chat_id: int):
    Path(ADMIN_FILE).write_text(str(chat_id), encoding="utf-8")

def load_users() -> set[int]:
    if not os.path.exists(USERS_FILE):
        return set()
    try:
        return {int(line.strip()) for line in Path(USERS_FILE).read_text(encoding="utf-8").splitlines() if line.strip()}
    except Exception:
        return set()

def save_users(users: set[int]):
    Path(USERS_FILE).write_text("\n".join(map(str, sorted(users))), encoding="utf-8")

def add_user(chat_id: int):
    users = load_users()
    if chat_id not in users:
        users.add(chat_id)
        save_users(users)

async def cmd_admin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    save_admin_chat_id(chat_id)
    await update.message.reply_text(f"✅ ADMIN_CHAT_ID встановлено: {chat_id}")

async def cmd_users(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.id != load_admin_chat_id():
        await update.message.reply_text("❌ Доступ заборонено.")
        return
    users = load_users()
    if not users:
        await update.message.reply_text("Активних користувачів немає.")
        return
    text = f"👥 Активні користувачі ({len(users)}):\n" + "\n".join(map(str, sorted(users)))
    await update.message.reply_text(text)

async def send_error_to_admin(context: ContextTypes.DEFAULT_TYPE, error_text: str):
    admin_id = load_admin_chat_id()
    if not admin_id:
        return
    try:
        await context.bot.send_message(
            chat_id=admin_id,
            text=f"🚨 ПОМИЛКА:\n{error_text}\nЧас: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        )
    except Exception as e:
        logger.error("Не вдалося надіслати помилку адміну: %s", e)

# =========================
# ПРОЄКТИ
# =========================
def load_projects() -> List[dict]:
    if not os.path.exists(PROJECTS_FILE):
        with open(PROJECTS_FILE, "w", encoding="utf-8") as f:
            json.dump({"projects": []}, f, ensure_ascii=False, indent=2)
        return []
    try:
        with open(PROJECTS_FILE, "r", encoding="utf-8") as f:
            return json.load(f).get("projects", [])
    except Exception:
        return []

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
# STATE
# =========================
def get_state(context: ContextTypes.DEFAULT_TYPE):
    if "state" not in context.user_data:
        context.user_data["state"] = {"pages": 3, "projects": []}
    return context.user_data["state"]

# =========================
# SAFE SEND
# =========================
async def _safe_send_message(bot, chat_id: int, text: str) -> bool:
    if not chat_id:
        return False
    try:
        await bot.send_message(chat_id=chat_id, text=text, parse_mode="Markdown")
        return True
    except Exception as e:
        logger.warning("send_message failed for %s: %s", chat_id, e)
        return False

async def _safe_send_document(bot, chat_id: int, path: Path, caption: str) -> bool:
    if not chat_id:
        return False
    try:
        with path.open("rb") as f:
            await bot.send_document(chat_id=chat_id, document=f, caption=caption)
        return True
    except Exception as e:
        logger.warning("send_document failed for %s: %s", chat_id, e)
        return False

# =========================
# XLSX HELPERS
# =========================
def find_latest_xlsx(since_ts: float) -> Optional[Path]:
    latest = None
    latest_mtime = 0.0
    for p in Path(".").rglob("*.xlsx"):
        try:
            m = p.stat().st_mtime
            if m >= since_ts and m >= latest_mtime:
                latest = p
                latest_mtime = m
        except Exception:
            continue
    return latest

def find_previous_report(output_prefix: str, current_path: Path) -> Optional[Path]:
    candidates = []
    for p in Path(".").rglob(f"{output_prefix}_*.xlsx"):
        try:
            if p.resolve() == current_path.resolve():
                continue
            candidates.append(p)
        except Exception:
            continue
    if not candidates:
        return None
    candidates.sort(key=lambda x: x.stat().st_mtime, reverse=True)
    return candidates[0]

def read_target_domain_stats(xlsx_path: Path) -> Dict[str, Dict[str, float]]:
    wb = load_workbook(xlsx_path, read_only=True, data_only=True)
    if "Target Domains Stats" not in wb.sheetnames:
        return {}
    ws = wb["Target Domains Stats"]
    header_cells = next(ws.iter_rows(min_row=1, max_row=1))
    header = [str(c.value).strip() if c.value is not None else "" for c in header_cells]
    idx = {name: i for i, name in enumerate(header)}
    domain_i = idx.get("Domain")
    kw_i = idx.get("Keywords")
    score_i = idx.get("Score")
    total_i = idx.get("Total")
    if domain_i is None:
        return {}
    out: Dict[str, Dict[str, float]] = {}
    for row in ws.iter_rows(min_row=2):
        domain = row[domain_i].value
        if not domain:
            continue
        domain = str(domain).strip().lower()
        kw_count = 0
        if kw_i is not None:
            cell = row[kw_i].value
            if cell:
                kws = [k.strip() for k in str(cell).split(";") if k.strip()]
                kw_count = len(set(kws))
        if kw_count == 0 and total_i is not None:
            try:
                kw_count = int(row[total_i].value or 0)
            except Exception:
                kw_count = 0
        score = 0.0
        if score_i is not None:
            try:
                score = float(row[score_i].value or 0)
            except Exception:
                score = 0.0
        out[domain] = {"kw": float(kw_count), "score": score}
    return out

def _badge(prev: float, now: float) -> str:
    if prev == 0 and now > 0:
        return "🟢"
    if prev > 0 and now == 0:
        return "🟥"
    if now > prev:
        return "🟢"
    if now < prev:
        if now * 2 < prev:
            return "🟥"
        return "🔻"
    return "⚪"

def format_delta_report(prev_map: Dict[str, Dict[str, float]],
                        cur_map: Dict[str, Dict[str, float]],
                        top_n: int = 30) -> str:
    domains = sorted(set(prev_map.keys()) | set(cur_map.keys()))
    rows: List[Tuple[float, str, float, float, float, float]] = []
    summary = {"kw_up": 0, "kw_down": 0, "kw_severe": 0, "kw_new": 0, "kw_lost": 0, "kw_same": 0}
    for d in domains:
        pkw = float(prev_map.get(d, {}).get("kw", 0))
        nkw = float(cur_map.get(d, {}).get("kw", 0))
        ps = float(prev_map.get(d, {}).get("score", 0))
        ns = float(cur_map.get(d, {}).get("score", 0))
        if pkw == 0 and nkw > 0:
            summary["kw_new"] += 1
        elif pkw > 0 and nkw == 0:
            summary["kw_lost"] += 1
            summary["kw_down"] += 1
            summary["kw_severe"] += 1
        elif nkw > pkw:
            summary["kw_up"] += 1
        elif nkw < pkw:
            summary["kw_down"] += 1
            if nkw * 2 < pkw:
                summary["kw_severe"] += 1
        else:
            summary["kw_same"] += 1
        if nkw != pkw or ns != ps:
            rows.append((abs(nkw - pkw), d, pkw, nkw, ps, ns))
    rows.sort(key=lambda x: x[0], reverse=True)
    rows = rows[:top_n]
    lines = []
    lines.append(
        f"📊 *Динаміка vs попередній парсинг*\n"
        f"Keywords: 🟢 {summary['kw_up']} | 🔻 {summary['kw_down']} (🟥 {summary['kw_severe']}) | NEW {summary['kw_new']} | LOST {summary['kw_lost']}"
    )
    lines.append("")
    lines.append("```")
    lines.append("KW | Prev→Now | ΔKW | SCORE | Prev→Now | ΔS | Domain")
    lines.append("---+----------+-----+-------+----------+-----+------------------------------")
    for _, d, pkw, nkw, ps, ns in rows:
        kw_badge = _badge(pkw, nkw)
        score_badge = _badge(ps, ns)
        dkw = int(nkw - pkw)
        ds = ns - ps
        dom = d[:30]
        lines.append(
            f"{kw_badge} {int(pkw):>3}→{int(nkw):<3} {dkw:+4} "
            f"{score_badge} {ps:>5.0f}→{ns:<5.0f} {ds:+5.0f} {dom}"
        )
    if not rows:
        lines.append("Нема змін у порівнянні з попереднім парсингом.")
    lines.append("```")
    return "\n".join(lines)

# Нова функція: додати історію тільки якщо це не перший парсинг
def add_history_sheet_if_needed(xlsx_path: Path, project_name: str):
    history_path = Path(LAST_HISTORY_DIR) / f"{project_name}.json"
    Path(LAST_HISTORY_DIR).mkdir(exist_ok=True)

    # Зберігаємо поточні дані як останню історію
    try:
        wb = load_workbook(xlsx_path, read_only=False)
        data_to_save = {}
        for sheet_name in wb.sheetnames:
            ws = wb[sheet_name]
            data = []
            for row in ws.iter_rows(values_only=True):
                data.append(row)
            data_to_save[sheet_name] = data
        history_path.write_text(json.dumps(data_to_save, ensure_ascii=False, indent=2), encoding="utf-8")

        # Якщо це НЕ перший парсинг — додаємо лист "History"
        prev_reports = list(Path(".").rglob(f"*_{project_name}_*.xlsx"))  # грубий пошук за назвою проєкту
        if len(prev_reports) > 1 or history_path.stat().st_size > 10:  # вже є історія
            if "History" in wb.sheetnames:
                wb.remove(wb["History"])
            history_ws = wb.create_sheet("History")
            if history_path.exists():
                old_data = json.loads(history_path.read_text(encoding="utf-8"))
                for sheet_name, rows in old_data.items():
                    history_ws.append([f"=== Попередній парсинг: {sheet_name} ==="])
                    for row in rows:
                        history_ws.append(row)
                    history_ws.append([])  # порожній рядок між листами
        wb.save(xlsx_path)
    except Exception as e:
        logger.error(f"Помилка при додаванні історії для {project_name}: {e}")

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
# START & CALLBACK
# =========================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    add_user(chat_id)
    st = get_state(context)
    await update.effective_message.reply_text(
        "Привіт! Це бот для парсингу SERP.\n"
        "— Ручний парсинг: виберіть проєкти + сторінки і натисніть ▶️\n"
        "— Автопарсинг: кожні 3 години (TOP-30), звіти всім користувачам\n\n"
        "Оберіть опцію:",
        reply_markup=kb_main(st)
    )

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
            await query.edit_message_text("Оберіть хоча б один проєкт.", reply_markup=kb_main(st))
            return
        pages = int(st["pages"])
        top_n = pages * 10
        await query.edit_message_text(
            f"⏳ Ручний парсинг запущено\nПроєктів: {len(st['projects'])}\nTOP: {top_n}",
            reply_markup=kb_main(st)
        )

        async def runner():
            try:
                for i, name in enumerate(st["projects"], start=1):
                    reload_projects()
                    project = PROJECTS_BY_NAME.get(name)
                    if not project:
                        await _safe_send_message(context.bot, chat_id, f"⚠️ [{i}] Проєкт «{name}» не знайдено.")
                        continue
                    cfg = dict(project)
                    cfg["max_positions"] = top_n
                    output_prefix = cfg.get("output_prefix", "report")

                    await _safe_send_message(
                        context.bot, chat_id,
                        f"▶️ [{i}/{len(st['projects'])}] Парсю «{name}» (TOP {top_n})"
                    )
                    start_ts = datetime.now().timestamp()
                    started_msg = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                    try:
                        out_path = await run_project(cfg)
                    except Exception as e:
                        await _safe_send_message(context.bot, chat_id, f"🚨 Помилка в «{name}»: {e}")
                        await send_error_to_admin(context, f"Помилка в «{name}»: {e}")
                        continue

                    xlsx_path = None
                    if isinstance(out_path, str) and out_path.strip():
                        p = Path(out_path)
                        if p.exists():
                            xlsx_path = p
                    if xlsx_path is None:
                        xlsx_path = find_latest_xlsx(start_ts)

                    if xlsx_path and xlsx_path.exists():
                        # Додаємо історію (тільки якщо це не перший парсинг)
                        add_history_sheet_if_needed(xlsx_path, name)

                        await _safe_send_message(context.bot, chat_id, f"✅ «{name}» готово ({started_msg})")
                        await _safe_send_document(context.bot, chat_id, xlsx_path, caption=xlsx_path.name)

                        prev_xlsx = find_previous_report(output_prefix, xlsx_path)
                        if prev_xlsx and prev_xlsx.exists():
                            prev_stats = read_target_domain_stats(prev_xlsx)
                            cur_stats = read_target_domain_stats(xlsx_path)
                            msg = format_delta_report(prev_stats, cur_stats, top_n=30)
                            await _safe_send_message(context.bot, chat_id, msg)
                        else:
                            await _safe_send_message(context.bot, chat_id, "ℹ️ Це перший звіт — порівняння немає.")
                    else:
                        await _safe_send_message(context.bot, chat_id, f"✅ «{name}» виконано, але файл не знайдено.")
                await _safe_send_message(context.bot, chat_id, "🏁 Ручний парсинг завершено.")
            except Exception as e:
                logger.exception("runner error: %s", e)
                await send_error_to_admin(context, f"runner error: {e}")

        context.application.create_task(runner())
        return

    if data == "add_project":
        await query.edit_message_text("Запустіть команду /addproject")
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
        await query.edit_message_text(
            "ℹ️ Команди:\n/start — меню\n/addproject — додати проєкт\n/cancel — скасувати\n/admin — алерти помилок",
            reply_markup=kb_main(st)
        )
        return
    if data == "back":
        await query.edit_message_text("Меню:", reply_markup=kb_main(st))
        return

    await query.edit_message_text(f"Невідома дія: {data}", reply_markup=kb_main(st))

# =========================
# ДОДАВАННЯ ПРОЄКТУ
# =========================
(NAME, LOCATION, LANGUAGE, API_KEYS, TARGET_DOMAINS, KEYWORDS, OUTPUT_PREFIX, HISTORY_FILE) = range(8)

async def start_add_project(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Крок 1: Введіть назву проєкту")
    context.user_data["new_project"] = {}
    return NAME

async def get_name(update: Update, context: ContextTypes.DEFAULT_TYPE):
    reload_projects()
    name = update.message.text.strip()
    if name in PROJECTS_BY_NAME:
        await update.message.reply_text(f"Проєкт «{name}» вже існує. Інша назва?")
        return NAME
    context.user_data["new_project"]["name"] = name
    await update.message.reply_text("Крок 2: Введіть країну (location, наприклад: France)")
    return LOCATION

async def get_location(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["new_project"]["location"] = update.message.text.strip()
    await update.message.reply_text("Крок 3: Введіть код мови (hl/gl, наприклад: fr)")
    return LANGUAGE

async def get_language(update: Update, context: ContextTypes.DEFAULT_TYPE):
    lang = update.message.text.strip()
    context.user_data["new_project"]["hl"] = lang
    context.user_data["new_project"]["gl"] = lang
    await update.message.reply_text("Крок 4: API ключі (через кому)")
    return API_KEYS

async def get_api_keys(update: Update, context: ContextTypes.DEFAULT_TYPE):
    keys = [k.strip() for k in update.message.text.split(",") if k.strip()]
    context.user_data["new_project"]["api_keys"] = keys
    await update.message.reply_text("Крок 5: Таргет-домени (по одному на рядок або через кому)")
    return TARGET_DOMAINS

async def get_target_domains(update: Update, context: ContextTypes.DEFAULT_TYPE):
    domains = [d.strip() for d in update.message.text.replace(",", "\n").split("\n") if d.strip()]
    context.user_data["new_project"]["target_domains"] = domains
    await update.message.reply_text("Крок 6: Ключові слова (по одному на рядок або через кому)")
    return KEYWORDS

async def get_keywords(update: Update, context: ContextTypes.DEFAULT_TYPE):
    keywords = [k.strip() for k in update.message.text.replace(",", "\n").split("\n") if k.strip()]
    context.user_data["new_project"]["keywords"] = keywords
    await update.message.reply_text("Крок 7: Префікс файлу (наприклад: serp_fr)")
    return OUTPUT_PREFIX

async def get_output_prefix(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["new_project"]["output_prefix"] = update.message.text.strip()
    await update.message.reply_text("Крок 8: Ім'я файлу історії (наприклад: history_fr.json) — можна будь-яке")
    return HISTORY_FILE

async def get_history_file(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["new_project"]["history_file"] = update.message.text.strip()
    new_project = context.user_data["new_project"]
    projects = load_projects()
    projects.append(new_project)
    save_projects(projects)
    reload_projects()

    await update.message.reply_text(
        f"✅ Проєкт «{new_project['name']}» додано!\nПовертаюсь у меню.",
        reply_markup=kb_main(get_state(context))
    )
    context.user_data.pop("new_project", None)
    return ConversationHandler.END

async def cancel_add_project(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("❌ Скасовано.", reply_markup=kb_main(get_state(context)))
    context.user_data.pop("new_project", None)
    return ConversationHandler.END

# =========================
# AUTO PARSING
# =========================
AUTO_LOCK = asyncio.Lock()

async def auto_parsing_task(context: ContextTypes.DEFAULT_TYPE):
    logger.info("Автопарсинг запущено")
    if AUTO_LOCK.locked():
        logger.warning("Попередній автопарсинг ще триває")
        return

    async with AUTO_LOCK:
        users = load_users()
        if not users:
            return

        reload_projects()
        if not PROJECTS:
            for uid in users:
                await _safe_send_message(context.bot, uid, "⚠️ Немає проєктів для автопарсингу")
            return

        for uid in users:
            await _safe_send_message(context.bot, uid, f"🤖 Автопарсинг стартував ({len(PROJECTS)} проєктів, TOP-30)")

        for i, project in enumerate(PROJECTS, start=1):
            name = project.get("name", "Unnamed")
            cfg = dict(project)
            cfg["max_positions"] = 30
            output_prefix = cfg.get("output_prefix", "report")

            for uid in users:
                await _safe_send_message(context.bot, uid, f"▶️ [{i}/{len(PROJECTS)}] Парсю «{name}»")

            start_ts = datetime.now().timestamp()
            try:
                out_path = await run_project(cfg)
            except Exception as e:
                msg = f"🚨 Помилка в «{name}»: {e}"
                for uid in users:
                    await _safe_send_message(context.bot, uid, msg)
                await send_error_to_admin(context, msg)
                continue

            xlsx_path = None
            if isinstance(out_path, str) and out_path.strip():
                p = Path(out_path)
                if p.exists():
                    xlsx_path = p
            if xlsx_path is None:
                xlsx_path = find_latest_xlsx(start_ts)

            if xlsx_path and xlsx_path.exists():
                add_history_sheet_if_needed(xlsx_path, name)

                for uid in users:
                    await _safe_send_message(context.bot, uid, f"✅ «{name}» готово")
                    await _safe_send_document(context.bot, uid, xlsx_path, caption=f"AUTO {xlsx_path.name}")

                    prev_xlsx = find_previous_report(output_prefix, xlsx_path)
                    if prev_xlsx and prev_xlsx.exists():
                        prev_stats = read_target_domain_stats(prev_xlsx)
                        cur_stats = read_target_domain_stats(xlsx_path)
                        msg = format_delta_report(prev_stats, cur_stats)
                        await _safe_send_message(context.bot, uid, msg)
                    else:
                        await _safe_send_message(context.bot, uid, "ℹ️ Перший автозвіт — порівняння немає.")
            else:
                for uid in users:
                    await _safe_send_message(context.bot, uid, f"✅ «{name}» виконано, але файл не знайдено.")

        for uid in users:
            await _safe_send_message(context.bot, uid, "🏁 Автопарсинг завершено.")

# =========================
# ERROR & MAIN
# =========================
async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    logger.exception("Помилка: %s", context.error)
    await send_error_to_admin(context, str(context.error))

import asyncio
import signal
import os

def main():
    if not TELEGRAM_BOT_TOKEN:
        raise RuntimeError("Встановіть TELEGRAM_BOT_TOKEN у змінних середовища Render!")

    app = Application.builder().token(TELEGRAM_BOT_TOKEN).build()

    # === Всі твої хендлери (залишаються без змін) ===
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("admin", cmd_admin))
    app.add_handler(CommandHandler("users", cmd_users))
    app.add_handler(CallbackQueryHandler(callback))

    conv = ConversationHandler(
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
    app.add_handler(conv)
    app.add_error_handler(error_handler)

    # Автопарсинг кожні 3 години
    app.job_queue.run_repeating(auto_parsing_task, interval=10800, first=60)

    logger.info("Бот запущено на Background Worker (polling)")

    # === Ключова частина: graceful shutdown для Background Worker ===
    async def stop_bot():
        logger.info("Отримано сигнал завершення — зупиняємо бота граціозно...")
        await app.stop()
        await app.shutdown()
        logger.info("Бот успішно зупинено")

    # Ловимо сигнали, які Render надсилає при деплої/рестарті
    loop = asyncio.get_event_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, lambda: asyncio.create_task(stop_bot()))

    # Запускаємо polling
    try:
        app.run_polling(
            drop_pending_updates=True,
            allowed_updates=Update.ALL_TYPES,  # на всяк випадок
        )
    finally:
        # Якщо щось пішло не так — все одно намагаємося зупинити
        asyncio.run(stop_bot())


if __name__ == "__main__":
    main()
