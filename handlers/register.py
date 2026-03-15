# handlers/register.py
from aiogram import Dispatcher

from .cookie import register_cookie
from .excel import register_files
from .sm import register_menu
from .start import register_start
from .auto_update import register_auto_update


def register_handlers(dp: Dispatcher):
    register_auto_update(dp)
    register_start(dp)
    register_menu(dp)
    register_files(dp)
    register_cookie(dp)
