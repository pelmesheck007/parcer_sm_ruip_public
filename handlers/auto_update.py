import asyncio

from aiogram.types import Message
from aiogram import Dispatcher, F
from aiogram.filters import Command
from aiogram.types import Message

from handlers.state import auto_update_state, main_keyboard
from services.auto_update_service import auto_update_loop

def register_auto_update(dp: Dispatcher):

    @dp.message(F.text == "Автообновление")
    async def start_auto_button(message: Message):

        user_id = message.from_user.id

        if user_id in auto_update_state and auto_update_state[user_id]["active"]:
            await message.answer("Автообновление уже работает.")
            return

        auto_update_state[user_id] = {
            "active": True,
            "interval": 900,
            "last_full_sync": 0
        }

        loop = asyncio.get_running_loop()
        task = loop.create_task(auto_update_loop(message.bot, user_id))
        auto_update_state[user_id]["task"] = task

        await message.answer("Автообновление запущено.")

    @dp.message(F.text == "Остановить авто")
    async def stop_auto_button(message: Message):

        user_id = message.from_user.id

        if user_id not in auto_update_state or not auto_update_state[user_id]["active"]:
            await message.answer("Автообновление не запущено.")
            return

        auto_update_state[user_id]["active"] = False

        task = auto_update_state[user_id].get("task")
        if task:
            task.cancel()

        await message.answer("Автообновление остановлено.")

    @dp.message(Command("interval"))
    async def change_interval(message: Message):

        parts = message.text.split()

        if len(parts) != 2 or not parts[1].isdigit():
            await message.answer("Используйте: /interval 300")
            return

        seconds = int(parts[1])
        user_id = message.from_user.id

        if user_id not in auto_update_state:
            await message.answer("Сначала запустите автообновление.")
            return

        auto_update_state[user_id]["interval"] = seconds

        await message.answer(f"Интервал обновления: {seconds} секунд.")
    @dp.message(Command("menu"))
    async def show_menu(message: Message):
        await message.answer("Меню:", reply_markup=main_keyboard)
