from zoneinfo import ZoneInfo

import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from datetime import date, timedelta, datetime
from gspread.exceptions import WorksheetNotFound

from additional_distribution import find_previous_existing_sheet
from find_sheet_smart import find_best_sheet
from style_sheet import copy_data_validations, apply_row_coloring, apply_table_style, set_duration_format, \
    set_number_format, set_duration_format2


# ===================== НАСТРОЙКИ =====================
def run_export(xls_path: str, sheet_url: str, creds_path: str):

    from zoneinfo import ZoneInfo
    import pandas as pd
    import gspread
    from google.oauth2.service_account import Credentials
    from datetime import date, timedelta, datetime
    from gspread.exceptions import WorksheetNotFound
    import re

    # ===================== ВСПОМОГАТЕЛЬНЫЕ =====================

    def clean(v, default=""):
        if pd.isna(v):
            return default
        return str(v).strip()

    # ===================== ДАТЫ =====================

    RU_WEEKDAYS = {0:"пн",1:"вт",2:"ср",3:"чт",4:"пт",5:"сб",6:"вс"}

    def prev_workday(d: date) -> date:
        d -= timedelta(days=1)
        while d.weekday() >= 5:
            d -= timedelta(days=1)
        return d

    now = datetime.now(ZoneInfo("Europe/Moscow"))
    today = now.date()


    if today.weekday() == 5:
        today += timedelta(days=2)
    elif today.weekday() == 6:
        today += timedelta(days=1)

    prev_day = prev_workday(today)

    today_name = f"{today:%d.%m}({RU_WEEKDAYS[today.weekday()]})"
    prev_name = f"{prev_day:%d.%m}({RU_WEEKDAYS[prev_day.weekday()]})"
    chelo_sheet = f"Чел: Сгорят {today:%d.%m}"
    jira_sheet = f"Jira от {prev_day:%d.%m}"


    # ===================== ЧТЕНИЕ XLS =====================

    #df = pd.read_html(XLS_PATH)[0]
    #df = read_any_excel(XLS_PATH)
    # ===================== GOOGLE SHEETS ССЛЫЛОЧКИ =====================

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]

    creds = Credentials.from_service_account_file(creds_path, scopes=scopes)
    client = gspread.authorize(creds)
    sheet = client.open_by_url(sheet_url)

    try:
        worksheet = sheet.worksheet(today_name)

    except WorksheetNotFound:
        prev_ws = find_best_sheet(sheet, prev_name)
        source_ws = find_best_sheet(sheet, prev_name)

        if not source_ws:
            raise Exception("Не найден ни один предыдущий лист для шаблона")

        worksheet = source_ws.duplicate(new_sheet_name=today_name)

        copy_data_validations(sheet, prev_ws, worksheet)

        # ==== предыдущий лист (ищем назад) ====
        prev_ws = None

    try:
        prev_ws = sheet.worksheet(prev_name)
    except:
        prev_ws = find_previous_existing_sheet(sheet, prev_day)

    if not prev_ws:
        raise Exception("Не найден ни один предыдущий лист для распределения")
    prev_name = prev_ws.title

    print(prev_name)

    worksheet.batch_clear(["A2:J"])


    # ===================== ЧТЕНИЕ ФАЙЛА =====================

    df = pd.read_html(xls_path)[0]

    ticket_col = next((c for c in df.columns if "номер" in c.lower()), None)
    if not ticket_col:
        raise ValueError("Колонка с номером заявки не найдена")

    df["Номер заявки"] = (
        df[ticket_col]
        .astype(str)
        .str.replace('="', '', regex=False)
        .str.replace('"', '', regex=False)
    )

    df_out = df[[
        "Номер заявки",
        "Время исполнения целевое",
        "(доп.) Заявка Jira",
        "Клиент. Компания",
    ]].rename(columns={
        "(доп.) Заявка Jira": "Заметки для ребят/вопрос другой команде",
        "Клиент. Компания": "СТД заявителя",
    })

    df_out = df_out.fillna("").astype(str)
    rows = len(df_out)


    # ===================== ЗАГОЛОВКИ =====================

    headers = [
        "Номер заявки",
        "Время исполнения целевое",
        "Осталось SLA",
        "Не назначен",
        "Статус по работе с заявкой",
        "От кого ждём действия",
        "Заметки для ребят/вопрос другой команде",
        "Буфер",
        "Наименование тикета",
        "СТД заявителя",
    ]

    worksheet.update("A1", [headers], value_input_option="USER_ENTERED")

    # ===================== ЗАПИСЬ ОСНОВНЫХ ДАННЫХ =====================

    worksheet.update(f"A2:A{rows+1}", [[v] for v in df_out["Номер заявки"]])
    worksheet.update(f"B2:B{rows+1}", [[v] for v in df_out["Время исполнения целевое"]])
    worksheet.update(f"G2:G{rows+1}", [[v] for v in df_out["Заметки для ребят/вопрос другой команде"]])
    worksheet.update(f"J2:J{rows+1}", [[v] for v in df_out["СТД заявителя"]])

    # ===================== ПЕРЕНОС С ПРОШЛОГО ЛИСТА =====================

    prev_ws = sheet.worksheet(prev_name)
    prev_data = prev_ws.get("A1:F1000")

    prev_status_map = {}
    prev_wait_map = {}
    prev_assign_map = {}

    if prev_data:
        headers_prev = prev_data[0]
        rows_prev = prev_data[1:]
        prev_df = pd.DataFrame(rows_prev, columns=headers_prev).fillna("")

        for _, row in prev_df.iterrows():
            ticket = clean(row.get("Номер заявки"))
            if not ticket:
                continue

            status = clean(row.get("Статус по работе с заявкой"))
            wait = clean(row.get("От кого ждём действия"))
            assign = clean(row.get("Не назначен"))

            prev_status_map[ticket] = status
            prev_wait_map[ticket] = wait
            prev_assign_map[ticket] = assign

    status_values = []
    wait_values = []
    assign_values = []

    for _, row in df_out.iterrows():

        ticket = clean(row["Номер заявки"])
        deadline = clean(row["Время исполнения целевое"])

        old_status = prev_status_map.get(ticket, "")
        old_wait = prev_wait_map.get(ticket, "")
        old_assign = prev_assign_map.get(ticket, "")

        # ================= СТАТУС =================

        if not old_status or old_status.lower() == "не обработано":
            new_status = "Не обработано"
            new_wait = "-"
            new_assign = "Не назначен"

        else:
            new_status = old_status
            new_wait = old_wait if old_wait else "-"
            new_assign = old_assign

            # меняем статус ТОЛЬКО если есть дедлайн
            if deadline:
                if new_status.lower() == "ожидание клиента":
                    new_status = "Вышло из ОК"
            if new_status.lower() == "закрыта":
                new_status = "Возврат"
            if new_status.lower() == "обработано":
                new_status = "Возврат от коллег"

        status_values.append([new_status])
        wait_values.append([new_wait])
        assign_values.append([new_assign])

    worksheet.update(f"D2:D{rows + 1}", assign_values)
    worksheet.update(f"E2:E{rows + 1}", status_values)
    worksheet.update(f"F2:F{rows + 1}", wait_values)

    # ===================== ФОРМУЛЫ =====================

    worksheet.update(
        f"C2:C{rows+1}",
        [[f'=ЕСЛИОШИБКА(ВПР(A{r};\'{chelo_sheet}\'!A:E;5;0)*1;"-")']
         for r in range(2, rows+2)],
        value_input_option="USER_ENTERED"
    )

    worksheet.update(
        f"H2:H{rows+1}",
        [[f'=REGEXEXTRACT(СЖПРОБЕЛЫ(G{r});"^RUIP-[0-9]+")']
         for r in range(2, rows+2)],
        value_input_option="USER_ENTERED"
    )

    worksheet.update(
        f"I2:I{rows+1}",
        [[f'=ВПР(H{r};\'{jira_sheet}\'!B:E;4;0)']
         for r in range(2, rows+2)],
        value_input_option="USER_ENTERED"
    )


    all_data = worksheet.get(f"A2:J{rows + 1}")
    not_empty = [r for r in all_data if r[1].strip()]
    empty = [r for r in all_data if not r[1].strip()]

    worksheet.update(
        f"A2:J{rows + 1}",
        not_empty + empty,
        value_input_option="USER_ENTERED"
    )



    apply_table_style(worksheet, rows)
    apply_row_coloring(worksheet)

    worksheet.sort(
        (2, "asc"),
        range="A2:J300"
    )

    set_duration_format(worksheet, rows)
    set_number_format(worksheet, rows)
    set_duration_format2(worksheet, rows)


    worksheet.freeze(rows=1)
    worksheet.set_basic_filter()


    print(f"Готово. Лист «{today_name}» обновлён")

import os

def read_any_excel(path):
    with open(path, "rb") as f:
        header = f.read(4)

    # XLSX начинается с PK (zip)
    if header.startswith(b"PK"):
        return pd.read_excel(path, engine="openpyxl")

    # Старый XLS
    try:
        return pd.read_excel(path, engine="xlrd")
    except:
        pass

    # Если это HTML (часто так бывает)
    try:
        return pd.read_html(path)[0]
    except:
        pass

    raise ValueError("Не удалось определить формат файла")

