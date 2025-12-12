import json
import asyncio
from pathlib import Path

import streamlit as st

from parser_core import run_project

PROJECTS_FILE = "projects.json"


def load_projects():
    path = Path(PROJECTS_FILE)
    if not path.exists():
        return {"projects": []}
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def save_projects(data):
    with open(PROJECTS_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def get_project_names(data):
    return [p["name"] for p in data.get("projects", [])]


def get_project_by_name(data, name):
    for p in data.get("projects", []):
        if p["name"] == name:
            return p
    return None


def app():
    st.set_page_config(page_title="SERP Parser (Serper.dev)", layout="wide")
    st.title("SERP Parser (Serper.dev) з проєктами")

    data = load_projects()

    col1, col2 = st.columns([2, 1])
    with col1:
        st.subheader("Вибір або створення проєкту")
    with col2:
        if st.button("Перезавантажити projects.json"):
            st.rerun()

    project_names = get_project_names(data)
    project_names_with_new = ["<Новий проєкт>"] + project_names

    selected_name = st.selectbox(
        "Обери проєкт", project_names_with_new, index=0
    )

    if selected_name == "<Новий проєкт>":
        project = {
            "name": "New Project",
            "location": "France",
            "gl": "fr",
            "hl": "fr",
            "api_keys": [""],
            "target_domains": [],
            "keywords": [],
            "max_positions": 30,
            "history_file": "serp_history_new.json",
            "output_prefix": "serp_top_serper_NEW",
        }
        is_new = True
    else:
        project = get_project_by_name(data, selected_name)
        is_new = False
        if project is None:
            st.error("Не вдалося знайти проєкт.")
            return
        if "max_positions" not in project:
            project["max_positions"] = 30

    st.markdown("### Налаштування проєкту")

    # Основні поля проєкту
    project["name"] = st.text_input("Назва проєкту", value=project["name"])
    project["location"] = st.text_input(
        "Location (Geo)", value=project.get("location", "France")
    )
    cols_geo = st.columns(2)
    project["gl"] = cols_geo[0].text_input(
        "gl (country code)", value=project.get("gl", "fr")
    )
    project["hl"] = cols_geo[1].text_input(
        "hl (language code)", value=project.get("hl", "fr")
    )

    st.markdown("#### Serper.dev API Keys")
    api_keys_text = st.text_area(
        "API-ключі (по одному в рядок)",
        value="\n".join(project.get("api_keys", [])),
        height=100,
    )
    project["api_keys"] = [
        k.strip() for k in api_keys_text.splitlines() if k.strip()
    ]

    st.markdown("#### Таргет-домени")
    target_domains_text = st.text_area(
        "Домени для трекінгу (по одному в рядок)",
        value="\n".join(project.get("target_domains", [])),
        height=150,
    )
    project["target_domains"] = [
        d.strip() for d in target_domains_text.splitlines() if d.strip()
    ]

    st.markdown("#### Ключові слова")
    keywords_text = st.text_area(
        "Ключові слова (по одному в рядок)",
        value="\n".join(project.get("keywords", [])),
        height=200,
    )
    project["keywords"] = [
        k.strip() for k in keywords_text.splitlines() if k.strip()
    ]

    st.markdown("#### Глибина парсингу для цього проєкту")
    cols_misc = st.columns(3)
    project["max_positions"] = cols_misc[0].number_input(
        "Максимальна позиція (Top N)",
        min_value=10,
        max_value=200,
        value=int(project.get("max_positions", 30)),
        step=10,
        help="Наприклад: 10, 20, 30, 50. Парсер сам порахує, скільки сторінок запитувати.",
    )
    project["history_file"] = cols_misc[1].text_input(
        "Ім'я файлу історії (JSON)",
        value=project.get("history_file", "serp_history.json"),
    )
    project["output_prefix"] = cols_misc[2].text_input(
        "Префікс для Excel-файлу",
        value=project.get("output_prefix", "serp_top_serper"),
    )

    st.markdown("---")
    cols_buttons = st.columns(3)

    # 💾 Зберегти проєкт
    with cols_buttons[0]:
        if st.button("💾 Зберегти проєкт"):
            if not project["name"].strip():
                st.error("Назва проєкту не може бути порожньою.")
            elif not project["api_keys"]:
                st.error("Потрібен хоча б один API-ключ.")
            elif not project["keywords"]:
                st.error("Потрібно вказати принаймні одне ключове слово.")
            else:
                if is_new:
                    data.setdefault("projects", []).append(project)
                else:
                    for idx, p in enumerate(data["projects"]):
                        if p["name"] == selected_name:
                            data["projects"][idx] = project
                            break
                save_projects(data)
                st.success("Проєкт збережено.")
                st.rerun()

    # 🚀 Запустити парсинг тільки для поточного проєкту
    with cols_buttons[1]:
        run_clicked = st.button("🚀 Запустити цей проєкт")

    # 🗑️ Видалити проєкт
    with cols_buttons[2]:
        if not is_new and st.button("🗑️ Видалити проєкт"):
            data["projects"] = [
                p for p in data.get("projects", []) if p["name"] != selected_name
            ]
            save_projects(data)
            st.success(f"Проєкт '{selected_name}' видалено.")
            st.rerun()

    # Одиночний запуск
    if run_clicked:
        if not project["api_keys"]:
            st.error("Додай хоча б один API-ключ перед запуском.")
            return
        if not project["keywords"]:
            st.error("Додай ключові слова перед запуском.")
            return
        if not project["target_domains"]:
            st.warning(
                "Таргет-домени порожні — парсер працюватиме, "
                "але збігів з таргетами не буде."
            )

        progress_bar = st.progress(0)
        status_text = st.empty()

        def progress_callback(done, total, found):
            frac = done / total if total else 0
            progress_bar.progress(frac)
            status_text.text(
                f"[{project['name']}] Оброблено {done}/{total} ключових слів | "
                f"знайдено позицій: {found}"
            )

        st.info("Парсинг запущено. Не закривай вкладку до завершення.")

        project_config = {
            "name": project["name"],
            "location": project["location"],
            "gl": project["gl"],
            "hl": project["hl"],
            "api_keys": project["api_keys"],
            "target_domains": project["target_domains"],
            "keywords": project["keywords"],
            "max_positions": int(project["max_positions"]),
            "history_file": project["history_file"],
            "output_prefix": project["output_prefix"],
        }

        output_file = asyncio.run(run_project(project_config, progress_callback))

        if Path(output_file).exists():
            st.success(f"Готово! Звіт збережено: {output_file}")
            with open(output_file, "rb") as f:
                st.download_button(
                    "⬇️ Завантажити Excel для цього проєкту",
                    data=f,
                    file_name=Path(output_file).name,
                    mime=(
                        "application/"
                        "vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                    ),
                )
        else:
            st.error("Щось пішло не так — файл не знайдено.")

    # =========================
    # МАСОВИЙ ПАРСИНГ ПРОЄКТІВ
    # =========================
    st.markdown("---")
    st.markdown("### Масовий парсинг проєктів")

    if not project_names:
        st.info("Ще немає збережених проєктів для масового парсингу.")
        return

    selected_projects_multi = st.multiselect(
        "Обери проєкти для масового парсингу",
        options=project_names,
        default=project_names,
        help="Можеш вибрати один або декілька проєктів.",
    )

    cols_mass = st.columns(2)
    with cols_mass[0]:
        pages_override = st.number_input(
            "Кількість сторінок (опціонально, для масового парсингу)",
            min_value=0,
            max_value=10,
            value=0,
            step=1,
            help=(
                "0 — використовувати налаштування Top N кожного проєкту.\n"
                "Якщо >0 — для всіх вибраних проєктів парсити саме стільки сторінок "
                "(по 10 результатів на сторінку)."
            ),
        )
    with cols_mass[1]:
        mass_run_clicked = st.button("🚀 Спарсити вибрані проєкти")

    if mass_run_clicked:
        if not selected_projects_multi:
            st.error("Вибери хоча б один проєкт для масового парсингу.")
            return

        st.info(
            "Масовий парсинг запущено. Не закривай вкладку, поки всі проєкти не завершаться."
        )

        # Якщо задано pages_override, конвертуємо в Top N
        max_positions_override = None
        if pages_override > 0:
            max_positions_override = pages_override * 10

        total_projects = len(selected_projects_multi)

        for idx, proj_name in enumerate(selected_projects_multi, start=1):
            proj = get_project_by_name(data, proj_name)
            if proj is None:
                st.warning(f"Пропускаю '{proj_name}' — не знайдено у projects.json.")
                continue

            st.markdown(
                f"#### Проєкт {idx}/{total_projects}: **{proj['name']}**"
            )
            progress_bar = st.progress(0)
            status_text = st.empty()

            def progress_callback(done, total, found, _proj_name=proj["name"]):
                frac = done / total if total else 0
                progress_bar.progress(frac)
                status_text.text(
                    f"[{_proj_name}] Оброблено {done}/{total} ключових слів | "
                    f"знайдено позицій: {found}"
                )

            # Формуємо конфіг для запуску
            cfg = {
                "name": proj["name"],
                "location": proj.get("location", "France"),
                "gl": proj.get("gl", "fr"),
                "hl": proj.get("hl", "fr"),
                "api_keys": proj.get("api_keys", []),
                "target_domains": proj.get("target_domains", []),
                "keywords": proj.get("keywords", []),
                "max_positions": int(
                    max_positions_override
                    if max_positions_override is not None
                    else proj.get("max_positions", 30)
                ),
                "history_file": proj.get(
                    "history_file",
                    f"serp_history_{proj['name'].replace(' ', '_')}.json",
                ),
                "output_prefix": proj.get(
                    "output_prefix",
                    f"serp_top_serper_{proj['name'].replace(' ', '_')}",
                ),
            }

            # Валідація мінімальних даних
            if not cfg["api_keys"]:
                st.warning(
                    f"[{proj['name']}] Пропуск — немає API-ключів."
                )
                continue
            if not cfg["keywords"]:
                st.warning(
                    f"[{proj['name']}] Пропуск — немає ключових слів."
                )
                continue

            output_file = asyncio.run(run_project(cfg, progress_callback))

            if Path(output_file).exists():
                st.success(f"[{proj['name']}] Готово! Звіт: {output_file}")
                with open(output_file, "rb") as f:
                    st.download_button(
                        f"⬇️ Завантажити Excel ({proj['name']})",
                        data=f,
                        file_name=Path(output_file).name,
                        mime=(
                            "application/"
                            "vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                        ),
                        key=f"download_{proj['name']}_{idx}",
                    )
            else:
                st.error(
                    f"[{proj['name']}] Щось пішло не так — файл не знайдено."
                )


if __name__ == "__main__":
    app()
