import asyncio
import os
import threading

from aiogram import Dispatcher, F
from aiogram.enums import ContentType
from aiogram.types import Message

from config import *
from daily_export import run_export
from handlers.state import user_mode
from services.meme_service import get_random_meme_and_delete
from services.sheet_service import get_or_create_today_sheet, build_sheet_link


def register_files(dp: Dispatcher):

    @dp.message(F.content_type == ContentType.DOCUMENT)
    async def handle_file(message: Message):
        user_id = message.from_user.id

        if user_id not in user_mode:
            await message.answer("Сначала выберите режим.")
            return

        file = message.document

        if file is None:
            await message.answer("Файл не найден.")
            return

        if not file.file_name.endswith((".xls", ".xlsx")):
            await message.answer("Нужен Excel файл.")
            return

        file_path = f"tmp_{file.file_name}"

        telegram_file = await message.bot.get_file(file.file_id)
        await message.bot.download_file(telegram_file.file_path, destination=file_path)

        await message.answer("Обработка...")

        loop = asyncio.get_running_loop()

        def task():
            try:
                mode = user_mode[user_id]

                if mode == "today":

                    ws, created = get_or_create_today_sheet()

                    run_export(
                        file_path,
                        sheet_url=SPREADSHEET_URL,
                        creds_path=credits_path
                    )

                    sheet_link = build_sheet_link(ws)

                    meme = get_random_meme_and_delete()

                    if meme:
                        asyncio.run_coroutine_threadsafe(
                            message.bot.send_photo(user_id, meme),
                            loop
                        )

                    text = "Табличка готова"
                    if not created:
                        text += "\n(лист обновлён)"

                    asyncio.run_coroutine_threadsafe(
                        message.bot.send_message(user_id, f"{text}\n\n{sheet_link}"),
                        loop
                    )

                elif mode == "cheli":
                    from ticket_chell_export import run_ticket_sla_export

                    run_ticket_sla_export(
                        file_path,
                        sheet_url=SPREADSHEET_URL,
                        creds_path=credits_path
                    )

                asyncio.run_coroutine_threadsafe(
                    message.bot.send_message(user_id, "Готово"),
                    loop
                )

                user_mode.pop(user_id, None)

            except Exception as e:
                asyncio.run_coroutine_threadsafe(
                    message.bot.send_message(user_id, f"Ошибка: {e}"),
                    loop
                )

            finally:
                if os.path.exists(file_path):
                    os.remove(file_path)

        threading.Thread(target=task, daemon=True).start()