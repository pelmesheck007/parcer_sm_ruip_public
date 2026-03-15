import asyncio
from datetime import datetime

from services.auto_update_service import detect_changes
from services.db_service import (
    init_db,
    load_all,
    upsert_ticket,
)
from services.sheet_service import load_google_sheet
from services.sm_service import SMService
from config import BASE_URL, VIEW_ID, SPREADSHEET_URL, credits_path
from update_sheet_sm import normalize_sm


sm_service = SMService(BASE_URL, VIEW_ID)


async def auto_update_loop(bot, user_id):

    init_db()

    while True:

        try:
            from sm_export_page import export_sm_view_all_pages

            df_raw = sm_service.export(export_sm_view_all_pages)
            df_sm = normalize_sm(df_raw)

            df_old = load_all()

            new_rows, changed_rows = detect_changes(df_sm, df_old)

            if new_rows.empty and changed_rows.empty:
                await bot.send_message(user_id, "Изменений нет")
            else:
                df_sheet, ws = load_google_sheet(SPREADSHEET_URL, credits_path)

                # Добавляем новые строки
                if not new_rows.empty:
                    ws.append_rows(new_rows.values.tolist())

                # Обновляем изменённые
                if not changed_rows.empty:
                    for _, row in changed_rows.iterrows():
                        ticket_id = row["Номер заявки"]

                        # ищем строку в sheet
                        cell = ws.find(ticket_id)
                        if cell:
                            row_index = cell.row

                            ws.update(
                                f"A{row_index}",
                                [row.values.tolist()]
                            )

                # Обновляем БД
                for _, row in df_sm.iterrows():
                    upsert_ticket(
                        row["Номер заявки"],
                        row["Статус по работе с заявкой"],
                        row["От кого ждём действия"]
                    )

                await bot.send_message(
                    user_id,
                    f"Обновлено: +{len(new_rows)} новых, "
                    f"{len(changed_rows)} изменённых "
                    f"{datetime.now().strftime('%H:%M')}"
                )

        except Exception as e:
            await bot.send_message(user_id, f"Ошибка: {e}")

        await asyncio.sleep(900)  # 15 минут