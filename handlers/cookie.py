from aiogram import F, Dispatcher
from aiogram.filters import Command
from aiogram.types import Message

from handlers.state import user_waiting_cookie
from services.sheet_service import sm_service

def register_cookie(dp: Dispatcher):

    @dp.message(Command("update_cookie"))
    async def update_cookie(message: Message):
        user_waiting_cookie[message.from_user.id] = True
        await message.answer(
            "Отправьте ТОЛЬКО значение OTRSAgentInterface\n\n"
            "Без 'OTRSAgentInterface=', без других cookie."
        )


    @dp.message(F.text & F.chat.type == "private")
    async def receive_cookie(message: Message):
        user_id = message.from_user.id

        if not user_waiting_cookie.get(user_id):
            return

        raw_value = message.text.strip()

        if not raw_value:
            await message.answer("Пустое значение.")
            return

        cookies_dict = {
            "OTRSAgentInterface": raw_value
        }

        sm_service.save_cookies(cookies_dict)

        user_waiting_cookie[user_id] = False
        await message.answer("Cookie сохранены")

    @dp.message(Command("set_cookie"))
    async def set_cookie(message: Message):
        parts = message.text.split(maxsplit=1)

        if len(parts) < 2:
            await message.answer("Используйте:\n/set_cookie ЗНАЧЕНИЕ")
            return

        raw_value = parts[1].strip()

        cookies_dict = {
            "OTRSAgentInterface": raw_value
        }

        sm_service.save_cookies(cookies_dict)

        await message.answer("Cookie сохранены")

