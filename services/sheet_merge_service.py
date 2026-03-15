import pandas as pd

from style_sheet import set_duration_format2


def merge_with_sheet(df_sheet: pd.DataFrame, df_db: pd.DataFrame) -> pd.DataFrame:
    """
    - не удаляем старые строки
    - обновляем SM-поля по существующим заявкам
    - добавляем новые в конец
    - 'Дата создания' пишем как google serial number (чтобы DATE_TIME формат работал)
    """
    df_sheet = df_sheet.copy()

    df_sheet["Номер заявки"] = df_sheet["Номер заявки"].astype(str).str.strip()

    db = df_db.copy()
    db["ticket_id"] = db["ticket_id"].astype(str).str.strip()
    db_idx = db.set_index("ticket_id")

    # ===== обновление существующих =====
    for idx, row in df_sheet.iterrows():
        ticket = row["Номер заявки"]
        if not ticket or ticket not in db_idx.index:
            continue

        sm_row = db_idx.loc[ticket]

        # ВАЖНО: названия колонок в SHEET
        if "Статус SM" in df_sheet.columns:
            df_sheet.at[idx, "Статус SM"] = sm_row.get("status_sm", "")

        if "Время исполнения целевое" in df_sheet.columns:
            df_sheet.at[idx, "Время исполнения целевое"] = sm_row.get("target_time", "")

        if "Осталось SLA" in df_sheet.columns:
            df_sheet.at[idx, "Осталось SLA"] = sm_row.get("sla_left", "")

        if "Дата создания" in df_sheet.columns:
            set_duration_format2(sm_row, row)

    # ===== добавление новых =====
    sheet_ids = set(df_sheet["Номер заявки"])
    new_ids = set(db["ticket_id"]) - sheet_ids

    if new_ids:
        new_rows_db = db[db["ticket_id"].isin(new_ids)]

        rows_to_add = []
        for _, sm_row in new_rows_db.iterrows():
            new_entry = {col: "" for col in df_sheet.columns}

            new_entry["Номер заявки"] = sm_row.get("ticket_id", "")
            if "Статус SM" in new_entry:
                new_entry["Статус SM"] = sm_row.get("status_sm", "")
            if "Время исполнения целевое" in new_entry:
                new_entry["Время исполнения целевое"] = sm_row.get("target_time", "")
            if "Осталось SLA" in new_entry:
                new_entry["Осталось SLA"] = sm_row.get("sla_left", "")
            if "Дата создания" in new_entry:
                new_entry["Дата создания"] = to_google_serial(sm_row.get("created_at", ""))

            # новые по умолчанию
            if "Статус по работе с заявкой" in new_entry and not new_entry["Статус по работе с заявкой"]:
                new_entry["Статус по работе с заявкой"] = "Не обработано"

            rows_to_add.append(new_entry)

        df_sheet = pd.concat([df_sheet, pd.DataFrame(rows_to_add)], ignore_index=True)

    return df_sheet