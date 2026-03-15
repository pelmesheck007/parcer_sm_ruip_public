import pandas as pd

def normalize_sm(df_raw):
    return pd.DataFrame({
        "Номер заявки": df_raw[0].apply(lambda x: x["label"]),
        "Статус SM": df_raw[2],
        "Дата создания": pd.to_datetime(df_raw[6], errors="coerce"),
        "Время исполнения целевое": df_raw[7],
        "Наименование тикета": df_raw[5],
        "СТД заявителя": df_raw[9],
        "Осталось SLA": df_raw[22],
    })

from datetime import datetime

def merge_updates(df_sheet, df_sm):

    today = datetime.today().date()

    sheet_ids = set(df_sheet["Номер заявки"])
    sm_ids = set(df_sm["Номер заявки"])

    logs = []

    # ===== Новые заявки =====
    new_ids = sm_ids - sheet_ids
    new_rows = df_sm[df_sm["Номер заявки"].isin(new_ids)].copy()

    for i, row in new_rows.iterrows():
        if row["Дата создания"].date() != today:
            new_rows.loc[i, "Статус по работе с заявкой"] = "Возврат"
            logs.append(f"Новая (возврат): {row['Номер заявки']}")
        else:
            new_rows.loc[i, "Статус по работе с заявкой"] = "В работе"
            logs.append(f"Новая: {row['Номер заявки']}")

    df_sheet = pd.concat([df_sheet, new_rows], ignore_index=True)

    # ===== Обновление существующих =====
    for idx, sheet_row in df_sheet.iterrows():

        ticket = sheet_row["Номер заявки"]

        if ticket not in sm_ids:
            continue

        sm_row = df_sm[df_sm["Номер заявки"] == ticket].iloc[0]

        # 1. Вышла из ОК
        if (
            sheet_row["Статус по работе с заявкой"] == "Ожидание клиента"
            and pd.notna(sm_row["Время исполнения целевое"])
        ):
            df_sheet.loc[idx, "Время исполнения целевое"] = sm_row["Время исполнения целевое"]
            df_sheet.loc[idx, "Статус по работе с заявкой"] = "Вышла из ОК"
            logs.append(f"{ticket}: вышла из ОК")

        # 2. Возврат от коллег
        if (
            sheet_row["Статус по работе с заявкой"] in ["Обработано", "Закрыта"]
            and sm_row["Статус SM"] != "Закрыта"
        ):
            df_sheet.loc[idx, "Статус по работе с заявкой"] = "Возврат от коллег"
            logs.append(f"{ticket}: возврат от коллег")

        # 3. Обновление заметок
        if (
            not sheet_row["Заметки для ребят/вопрос другой команде"]
            and sm_row.get("Заметки для ребят/вопрос другой команде")
        ):
            df_sheet.loc[idx, "Заметки для ребят/вопрос другой команде"] = \
                sm_row["Заметки для ребят/вопрос другой команде"]
            logs.append(f"{ticket}: обновлены заметки")

    return df_sheet, logs