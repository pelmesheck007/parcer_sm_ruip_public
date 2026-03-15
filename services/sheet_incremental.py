import pandas as pd


def incremental_update(df_sheet, df_sm):

    df_sheet = df_sheet.copy()

    sheet_ids = set(df_sheet["Номер заявки"].astype(str))
    db_ids = set(df_sm["ticket_id"].astype(str))

    new_ids = db_ids - sheet_ids

    if not new_ids:
        return df_sheet

    new_rows = df_sm[df_sm["ticket_id"].isin(new_ids)]

    rows_to_add = []

    for _, sm_row in new_rows.iterrows():

        new_entry = {col: "" for col in df_sheet.columns}

        new_entry["Номер заявки"] = sm_row["ticket_id"]
        new_entry["Статус SM"] = sm_row["status_sm"]
        new_entry["Время исполнения целевое"] = sm_row["target_time"]
        new_entry["Осталось SLA"] = sm_row["sla_left"]
        new_entry["Статус по работе с заявкой"] = "Не обработано"

        rows_to_add.append(new_entry)

    df_sheet = pd.concat([df_sheet, pd.DataFrame(rows_to_add)], ignore_index=True)

    return df_sheet