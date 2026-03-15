import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from datetime import date
from zoneinfo import ZoneInfo
from gspread.exceptions import WorksheetNotFound


def run_ticket_sla_export(xls_path: str, sheet_url: str, creds_path: str):
    SPREADSHEET_URL = sheet_url
    CREDS_PATH = creds_path
    XLS_PATH = xls_path

    # ===== дата (Москва) =====
    today = date.today()
    sheet_name = f"Чел: Сгорят {today:%d.%m}"

    # ===== читаем Excel =====
    df = pd.read_excel(xls_path)
    print("СТАТУС (гр):")
    print(df["Текущий статус"].value_counts())

    print("\nSLA (%):")
    print(df["Осталось SLA(г)"].value_counts())

    # ===== ЖЁСТКО ЗАДАННЫЕ ЗНАЧЕНИЯ =====
    allowed_status = {"ожидание пользователя", "в работе"}
    allowed_status_gr = {"В работе"}
    allowed_sla = {"< 10%", "10% - 25%", "Просрочен"}

    # ===== фильтрация =====
    df = df[
        df["Текущий статус"].isin(allowed_status)
        &
        df["Осталось SLA(г)"].isin(allowed_sla)
        &
        df["Текущий статус (гр)"].isin(allowed_status_gr)
    ].copy()

    if df.empty:
        print("⚠ После фильтрации нет строк — проверь входной файл")
        return

    # ===== нужные столбцы =====
    df_out = df[
        [
            "Номер обращения",
            "Номер КСА",
            "Текущий статус",
            "Осталось SLA(ч)",
        ]
    ].copy()

    rows = len(df_out)

    # ===== Google Sheets =====
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_file(CREDS_PATH, scopes=scopes)
    client = gspread.authorize(creds)
    sheet = client.open_by_url(SPREADSHEET_URL)

    # ===== лист =====
    try:
        ws = sheet.worksheet(sheet_name)
    except WorksheetNotFound:
        ws = sheet.add_worksheet(title=sheet_name, rows="1000", cols="10")

    ws.clear()

    # ===== заголовки =====
    headers = [
        "Номер обращения",
        "У нас в табличке",
        "Номер КСА",
        "Текущий статус",
        "Осталось SLA (ч)",
    ]
    ws.update("A1", [headers], value_input_option="USER_ENTERED")

    # ===== данные =====
    ws.update(
        f"A2:A{rows+1}",
        [[v] for v in df_out["Номер обращения"]],
        value_input_option="USER_ENTERED",
    )


    ws.update(
        f"C2:C{rows+1}",
        [[v] for v in df_out["Номер КСА"]],
        value_input_option="USER_ENTERED",
    )

    ws.update(
        f"D2:D{rows+1}",
        [[v] for v in df_out["Текущий статус"]],
        value_input_option="USER_ENTERED",
    )

    ws.update(
        f"E2:E{rows+1}",
        [[v] for v in df_out["Осталось SLA(ч)"]],
        value_input_option="USER_ENTERED",
    )

    # ===== формула "У нас в табличке" =====
    today_name = today.strftime("%d.%m(%a)").lower() \
        .replace("mon", "пн") \
        .replace("tue", "вт") \
        .replace("wed", "ср") \
        .replace("thu", "чт") \
        .replace("fri", "пт")

    ws.update(
        f"B2:B{rows+1}",
        [[
            f'=ЕСЛИ(ЕОШИБКА(ВПР(A{r};\'{today_name}\'!A:A;1;0));"Нет";"Да")'
        ] for r in range(2, rows + 2)],
        value_input_option="USER_ENTERED",
    )

    apply_table_style(ws, rows)
    set_number_format(ws, rows)

    # ===== сортировка по SLA (ч) =====
    ws.sort((5, "asc"), range=f"A2:E{rows+1}")
    set_duration_format(ws, rows)


    ws.freeze(rows=1)
    ws.set_basic_filter()


    print(f"SLA обращения выгружены: {rows} строк → лист «{sheet_name}»")



def apply_table_style(ws, rows):
    from gspread_formatting import (
        CellFormat,
        TextFormat,
        set_row_height,
        set_column_width,
        format_cell_ranges
    )
    from gspread_formatting import CellFormat, format_cell_ranges

    fmt = CellFormat(
        textFormat=TextFormat(fontSize=10),
        horizontalAlignment="LEFT",
        verticalAlignment="MIDDLE"
    )

    format_cell_ranges(
        ws,
        [
            (f"A1:J{rows+1}", fmt),
        ]
    )

    # (опционально) аккуратная высота строк
    set_row_height(ws, f"1:{rows+1}", 28)

from gspread_formatting import CellFormat, NumberFormat, format_cell_ranges

def set_duration_format(ws, rows):
    duration_fmt = CellFormat(
        numberFormat=NumberFormat(
            type="DATE_TIME",   # ← ВАЖНО
            pattern="[h]:mm:ss"
        )
    )
    format_cell_ranges(ws, [(f"E2:E{rows+1}", duration_fmt)])

from gspread_formatting import CellFormat, NumberFormat, format_cell_ranges

def set_number_format(ws, rows):
    number_fmt = CellFormat(
        numberFormat=NumberFormat(
            type="NUMBER",
            pattern="0"
        )
    )

    format_cell_ranges(
        ws,
        [(f"A2:A{rows+1}", number_fmt)]
    )
