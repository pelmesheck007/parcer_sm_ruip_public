def apply_status_logic(df_sheet, df_sm):

    df_sheet = df_sheet.copy()

    sm_index = df_sm.set_index("ticket_id")

    for idx, row in df_sheet.iterrows():

        ticket = str(row["Номер заявки"])

        if ticket not in sm_index.index:
            continue

        sm_row = sm_index.loc[ticket]

        current_status = row.get("Статус по работе с заявкой", "")
        sm_status = sm_row["status_sm"]

        # если статус не "Не обработано"
        if current_status and current_status != "Не обработано":

            # было ожидание клиента → вышло из ОК
            if current_status == "Ожидание клиента":
                df_sheet.loc[idx, "Статус по работе с заявкой"] = "Вышло из ОК"

            # закрыта → возврат
            elif sm_status == "Закрыта":
                df_sheet.loc[idx, "Статус по работе с заявкой"] = "Возврат"

        # обновляем служебные SM поля
        df_sheet.loc[idx, "Статус SM"] = sm_status
        df_sheet.loc[idx, "Осталось SLA"] = sm_row["sla_left"]
        df_sheet.loc[idx, "Время исполнения целевое"] = sm_row["target_time"]

    return df_sheet