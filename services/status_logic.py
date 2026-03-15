import pandas as pd


def update_status_columns(df_sheet: pd.DataFrame, df_db: pd.DataFrame) -> pd.DataFrame:
    """
    Правила:
    - старые заявки не сносим
    - если текущий 'Статус по работе с заявкой' != 'Не обработано' -> сохраняем (подтягиваем с прошлого состояния)
    - если текущий статус был 'Ожидание клиента' -> ставим 'Вышло из ОК'
    - если SM статус 'Закрыта' -> ставим 'Возврат'
    """
    df_sheet = df_sheet.copy()

    # индекс по ticket_id из БД
    db = df_db.copy()
    db["ticket_id"] = db["ticket_id"].astype(str)
    db_idx = db.set_index("ticket_id")

    # нормализатор сравнения
    def norm(s):
        return str(s).strip().lower()

    for i, row in df_sheet.iterrows():
        ticket = str(row.get("Номер заявки", "")).strip()
        if not ticket or ticket not in db_idx.index:
            continue

        sm_status = str(db_idx.loc[ticket, "status_sm"] or "").strip()

        cur_status = row.get("Статус по работе с заявкой", "")
        cur_status_norm = norm(cur_status)

        # 1) если было ожидание клиента — переводим
        if cur_status_norm == norm("Ожидание клиента"):
            df_sheet.at[i, "Статус по работе с заявкой"] = "Вышло из ОК"
            continue

        # 2) если SM закрыта — возврат (это важнее)
        if norm(sm_status) == norm("Закрыта"):
            df_sheet.at[i, "Статус по работе с заявкой"] = "Возврат"
            continue

        # 3) подтягиваем старое: если НЕ "не обработано" — оставляем как есть
        if cur_status_norm and cur_status_norm != norm("Не обработано"):
            # ничего не меняем
            continue

        # 4) иначе базовое значение (если хочешь)
        # df_sheet.at[i, "Статус по работе с заявкой"] = "В работе"

    return df_sheet