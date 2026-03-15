# state.py
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton

user_mode = {}
user_waiting_cookie = {}
auto_update_state = {}

main_keyboard = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="Сегодняшние заявки")],
        [KeyboardButton(text="Челиковские заявки")],
        [KeyboardButton(text="Табличка по SM")],
        [KeyboardButton(text="Доп. распределение")],
        [KeyboardButton(text="Автообновление")],
        [KeyboardButton(text="Остановить авто")],
    ],
    resize_keyboard=True,
)