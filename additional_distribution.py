from datetime import timedelta


def find_previous_existing_sheet(sheet, start_date):
    """
    Ищет ближайший существующий лист,
    начиная с start_date и двигаясь назад.
    """
    RU_WEEKDAYS = {0: "пн", 1: "вт", 2: "ср", 3: "чт", 4: "пт", 5: "сб", 6: "вс"}

    for i in range(1, 14):  # максимум 2 недели назад
        check_date = start_date - timedelta(days=i)
        name = f"{check_date:%d.%m}({RU_WEEKDAYS[check_date.weekday()]})"

        try:
            return sheet.worksheet(name)
        except:
            continue

    return None

from datetime import timedelta


def run_additional_distribution(sheet_url: str, creds_path: str):
    import gspread
    from google.oauth2.service_account import Credentials
    import re
    import pandas as pd
    from zoneinfo import ZoneInfo
    from datetime import datetime, timedelta

    RU_WEEKDAYS = {0: "пн", 1: "вт", 2: "ср", 3: "чт", 4: "пт", 5: "сб", 6: "вс"}

    def normalize_component(text: str) -> str:
        return str(text).strip().upper().replace("Ё", "Е")

    def extract_component(note: str, known_components: list[str]) -> str | None:
        note_norm = normalize_component(note)

        for component in sorted(known_components, key=len, reverse=True):
            pattern = rf"(?<!\w){re.escape(component)}(?!\w)"
            if re.search(pattern, note_norm):
                return component

        return None

    # ==== ДАТА ТЕКУЩЕГО ЛИСТА ====
    now = datetime.now(ZoneInfo("Europe/Moscow"))
    today = now.date()

    if today.weekday() == 5:
        today += timedelta(days=2)
    elif today.weekday() == 6:
        today += timedelta(days=1)

    today_name = f"{today:%d.%m}({RU_WEEKDAYS[today.weekday()]})"

    # ==== ПОДКЛЮЧЕНИЕ ====
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]

    creds = Credentials.from_service_account_file(creds_path, scopes=scopes)
    client = gspread.authorize(creds)
    sheet = client.open_by_url(sheet_url)

    # ==== ТЕКУЩИЙ ЛИСТ ====
    try:
        ws = sheet.worksheet(today_name)
    except:
        raise Exception(f"Лист {today_name} не найден")

    raw = ws.get("A1:J1000")

    if not raw:
        print("Нет данных")
        return

    headers = raw[0]
    rows = raw[1:]

    rows = [
        list(row) + [""] * (len(headers) - len(row)) if len(row) < len(headers)
        else list(row)[:len(headers)]
        for row in rows
    ]

    df = pd.DataFrame(rows, columns=headers).fillna("")

    if df.empty:
        print("Нет данных для распределения")
        return

    # ==== ЧИТАЕМ МАТРИЦУ РАСПРЕДЕЛЕНИЯ С ЭТОГО ЖЕ ЛИСТА ====
    subsystem_row = ws.get("O2:U2")
    ruip_row = ws.get("O3:U3")
    responsible_rows = ws.get("O4:U10")

    subsystems = subsystem_row[0] if subsystem_row else []
    ruip_numbers = ruip_row[0] if ruip_row else []

    component_map = {}
    ruip_map = {}

    max_cols = max(len(subsystems), len(ruip_numbers))

    for col_index in range(max_cols):
        people = []

        for row in responsible_rows:
            if col_index < len(row):
                val = str(row[col_index]).strip()
                if val:
                    people.append(val)

        if not people:
            continue

        if col_index < len(subsystems):
            component_cell = normalize_component(subsystems[col_index])
            if component_cell:
                component_map[component_cell] = people

        if col_index < len(ruip_numbers):
            ruip_cell = str(ruip_numbers[col_index]).strip()
            if ruip_cell:
                ruips = re.findall(r"RUIP-\d+", ruip_cell.upper())
                for ruip in ruips:
                    ruip_map[ruip] = people

    if "Не назначен" not in df.columns:
        raise Exception("Колонка 'Не назначен' не найдена")

    if "Заметки для ребят/вопрос другой команде" not in df.columns:
        raise Exception("Колонка 'Заметки для ребят/вопрос другой команде' не найдена")

    assigned = df["Не назначен"].tolist()
    notes = df["Заметки для ребят/вопрос другой команде"].tolist()

    component_counters = {k: 0 for k in component_map}
    ruip_counters = {k: 0 for k in ruip_map}

    known_components = list(component_map.keys())

    for i in range(len(df)):
        current_assign = str(assigned[i]).strip().lower()

        if current_assign and current_assign != "не назначен":
            continue

        note = str(notes[i]).strip()
        if not note:
            continue

        # сначала ищем компонент
        component = extract_component(note, known_components)
        if component and component in component_map:
            people = component_map[component]
            idx = component_counters[component] % len(people)
            assigned[i] = people[idx]
            component_counters[component] += 1
            continue

        # если компонент не найден — ищем тикет
        match = re.search(r"RUIP-\d+", note.upper())
        if match:
            ruip = match.group()
            if ruip in ruip_map:
                people = ruip_map[ruip]
                idx = ruip_counters[ruip] % len(people)
                assigned[i] = people[idx]
                ruip_counters[ruip] += 1

    ws.update(
        f"D2:D{len(df)+1}",
        [[v] for v in assigned],
        value_input_option="USER_ENTERED"
    )

    print("Дополнительное распределение завершено")