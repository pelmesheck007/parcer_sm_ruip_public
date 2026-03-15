import asyncio

from aiogram import Dispatcher, F
from aiogram.filters import Command
from aiogram.types import Message

from config import GROUP_CHAT_ID
from handlers.state import main_keyboard, auto_update_state
from services.auto_update_service import auto_update_loop


#@dp.message()
#async def debug_chat(message: Message):
#    print("CHAT ID:", message.chat.id)

def register_start(dp: Dispatcher):
    @dp.message(Command("start"))
    async def start_handler(message: Message):
        await message.answer("Выберите режим:", reply_markup=main_keyboard)
        print("CHAT ID:", message.chat.id)
        await message.bot.send_message(GROUP_CHAT_ID, "Тест из старта")

