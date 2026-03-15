import asyncio
from datetime import datetime
import pandas as pd

from sm_export_page import export_sm_view_all_pages
from services.sm_service import SMService
from services.db_service import init_db, upsert_sm, load_sm
from services.sheet_service import load_google_sheet, save_google_sheet
from services.sheet_merge_service import merge_with_sheet
from services.status_logic import update_status_columns
from config import BASE_URL, VIEW_ID, SPREADSHEET_URL, credits_path
from style_sheet import set_duration_format2
from update_sheet_sm import normalize_sm


sm_service = SMService(BASE_URL, VIEW_ID)


async def auto_update_loop(bot, user_id):
    init_db()

    while True:
        try:
            # 1) SM -> df_sm
            df_raw = sm_service.export(export_sm_view_all_pages)
            df_sm = normalize_sm(df_raw)

            # чистим только NaN/inf, НЕ astype(str) (ломает даты)
            df_sm = df_sm.replace([float("inf"), float("-inf")], None).fillna("")

            # 2) SM -> DB
            upsert_sm(df_sm)

            # 3) DB -> df_db
            df_db = load_sm()

            # 4) Sheet load
            df_sheet, ws = load_google_sheet(SPREADSHEET_URL, credits_path)

            # 5) merge SM-полей + добавление новых (без удаления старых)
            df_sheet = merge_with_sheet(df_sheet, df_db)

            # 6) бизнес-логика статусов (без формул)
            df_sheet = update_status_columns(df_sheet, df_db)

            # 7) save + формат дат
            save_google_sheet(ws, df_sheet)
            set_duration_format2(ws, rows=len(df_sheet))

            await bot.send_message(
                user_id,
                f"Автообновление: {len(df_sheet)} строк ({datetime.now().strftime('%H:%M')})"
            )

        except Exception as e:
            await bot.send_message(user_id, f"Ошибка авто: {e}")

        await asyncio.sleep(900)