import asyncio

from aiogram import Bot, Dispatcher
from config import BOT_TOKEN
from handlers.register import register_handlers

async def main():
    bot = Bot(token=BOT_TOKEN)
    dp = Dispatcher()
    print("BOT STARTED")
    register_handlers(dp)
    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())