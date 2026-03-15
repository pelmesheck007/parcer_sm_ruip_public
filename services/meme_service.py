# services/meme_service.py
from aiogram.types import BufferedInputFile
import random

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import io

from proto.marshal.compat import message

from config import *

def get_random_meme_and_delete():


    scopes = ["https://www.googleapis.com/auth/drive"]

    creds = Credentials.from_service_account_file(credits_path, scopes=scopes)
    service = build("drive", "v3", credentials=creds)

    results = service.files().list(
        q=f"'{MEME_FOLDER_ID}' in parents and mimeType contains 'image/' and trashed=false",
        fields="files(id,name)",
        supportsAllDrives=True,
        includeItemsFromAllDrives=True
    ).execute()

    files = results.get("files", [])

    if not files:
        return None

    file = random.choice(files)
    file_id = file["id"]

    # скачиваем
    request = service.files().get_media(
        fileId=file_id,
        supportsAllDrives=True
    )

    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)

    done = False
    while not done:
        _, done = downloader.next_chunk()

    fh.seek(0)

    # перемещаем в архив (без удаления)
    service.files().update(
        fileId=file_id,
        addParents=ARCHIVE_FOLDER_ID,
        removeParents=MEME_FOLDER_ID,
        supportsAllDrives=True
    ).execute()

    fh.seek(0)
    fh.name = file["name"]  # ВАЖНО
    return fh



def sm_guard_caption(sheet_link):
    return f'<a href="{sheet_link}">Табличка</a>  готова.\n\n'


async def send_report(chat_id, group_id, photo_bytes, caption):

    photo1 = BufferedInputFile(photo_bytes, filename="meme.jpg")

    await message.bot.send_photo(
        chat_id=chat_id,
        photo=photo1,
        caption=caption,
        parse_mode="HTML"
    )

    if chat_id != group_id:
        photo2 = BufferedInputFile(photo_bytes, filename="meme.jpg")

        await message.bot.send_photo(
            chat_id=group_id,
            photo=photo2,
            caption=caption,
            parse_mode="HTML"
        )