import asyncio
import threading

from aiogram import Dispatcher, F
from aiogram.types import Message, BufferedInputFile

from additional_distribution import run_additional_distribution
from config import *
from export_to_sheet import run_export_sm
from handlers.state import user_mode, user_waiting_cookie
from services.auto_update_service import sm_service
from services.meme_service import get_random_meme_and_delete, sm_guard_caption
from services.sheet_service import find_today_sheet
from sm_exporter import run_export_from_sm


def register_menu(dp: Dispatcher):
    @dp.message(F.text.in_(["Сегодняшние заявки", "Челиковские заявки", "Табличка по SM", "Доп. распределение"]))
    async def choose_mode(message: Message):
        user_id = message.from_user.id

        if message.text == "Сегодняшние заявки":
            user_mode[user_id] = "today"
            await message.answer("Отправьте Excel файл.")

        elif message.text == "Челиковские заявки":
            user_mode[user_id] = "cheli"
            await message.answer("Отправьте Excel файл.")
        elif message.text == "Доп. распределение":
            await message.answer("Запускаю доп. распределение...")

            loop = asyncio.get_running_loop()

            def task():
                try:
                    run_additional_distribution(SPREADSHEET_URL, credits_path)

                    asyncio.run_coroutine_threadsafe(
                        message.bot.send_message(user_id, "Доп. распределение завершено"),
                        loop
                    )
                    user_mode.pop(user_id, None)

                except Exception as e:
                    asyncio.run_coroutine_threadsafe(
                        message.bot.send_message(user_id, f"Ошибка: {e}"),
                        loop
                    )

            threading.Thread(target=task, daemon=True).start()

        else:
            user_mode[user_id] = "sm"
            await message.answer("Запускаю выгрузку из SM...")

            loop = asyncio.get_running_loop()

            def task():
                try:
                    df = sm_service.export(run_export_from_sm)
                    run_export_sm(df,SPREADSHEET_URL, credits_path)
                    ws = find_today_sheet()

                    if ws:
                        sheet_link = f"{SPREADSHEET_URL}#gid={ws.id}"
                    else:
                        sheet_link = SPREADSHEET_URL

                    chat_id = message.chat.id
                    meme = get_random_meme_and_delete()
                    if meme:
                        meme_bytes = meme.read()

                        caption = sm_guard_caption(sheet_link)

                        photo_user = BufferedInputFile(meme_bytes, filename="meme.jpg")
                        photo_group = BufferedInputFile(meme_bytes, filename="meme.jpg")

                        future1 = asyncio.run_coroutine_threadsafe(
                            message.bot.send_photo(
                                chat_id=user_id,
                                photo=photo_user,
                                caption=caption,
                                parse_mode="HTML"
                            ),
                            loop
                        )

                        future2 = asyncio.run_coroutine_threadsafe(
                            message.bot.send_photo(
                                chat_id=GROUP_CHAT_ID,
                                photo=photo_group,
                                caption=caption,
                                parse_mode="HTML"
                            ),
                            loop
                        )

                        # вот это ВАЖНО
                        future1.result()
                        future2.result()
                        print("MEME OBJECT:", meme)

                    else:
                        asyncio.run_coroutine_threadsafe(
                            message.bot.send_message(user_id, f"Меме кончились"),
                            loop
                        )
                        user_mode.pop(user_id, None)


                    asyncio.run_coroutine_threadsafe(
                        message.bot.send_message(user_id, f"SM готово Строк: {len(df)}"),
                        loop
                    )
                    user_mode.pop(user_id, None)

                except Exception as e:
                    if str(e) == "COOKIE_MISSING" or str(e) == "COOKIE_EXPIRED":
                        user_waiting_cookie[user_id] = True
                        asyncio.run_coroutine_threadsafe(
                            message.bot.send_message(
                                user_id,
                                "⚠ Cookie отсутствуют или устарели.\n"
                                "Введите /update_cookie и отправьте новые."
                            ),
                            loop
                        )
                    else:
                        asyncio.run_coroutine_threadsafe(
                            message.bot.send_message(user_id, f"Ошибка: {e}"),
                            loop
                        )


            threading.Thread(target=task, daemon=True).start()
