# services/sheet_service.py
import re
from datetime import datetime

import gspread
import pandas as pd
from google.oauth2.service_account import Credentials

from config import *
from services import SMService

sm_service = SMService(BASE_URL, VIEW_ID)

def get_gspread_client(creds_path: str):
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_file(creds_path, scopes=scopes)
    return gspread.authorize(creds)


def load_google_sheet(sheet_url, creds_path):

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]

    creds = Credentials.from_service_account_file(creds_path, scopes=scopes)
    client = gspread.authorize(creds)

    sh = client.open_by_url(sheet_url)
    ws = sh.sheet1

    data = ws.get_all_values()

    if not data:
        return pd.DataFrame(), ws

    headers = data[0]
    rows = data[1:]

    df = pd.DataFrame(rows, columns=headers)

    return df, ws



def save_google_sheet(ws, df):
    ws.clear()
    ws.update([df.columns.values.tolist()] + df.values.tolist())



def get_or_create_today_sheet():

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]

    creds = Credentials.from_service_account_file(credits_path, scopes=scopes)
    client = gspread.authorize(creds)

    sh = client.open_by_url(SPREADSHEET_URL)

    today_title = datetime.now().strftime("%d.%m")

    try:
        ws = sh.worksheet(today_title)
        return ws, False  # уже существовал
    except gspread.exceptions.WorksheetNotFound:
        ws = sh.add_worksheet(title=today_title, rows=1000, cols=20)
        return ws, True



def find_today_sheet():

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]

    creds = Credentials.from_service_account_file(credits_path, scopes=scopes)
    client = gspread.authorize(creds)

    sh = client.open_by_url(SPREADSHEET_URL)

    today = datetime.now().strftime("%d.%m")

    pattern = re.compile(rf"^{today}\(")

    for ws in sh.worksheets():
        if pattern.match(ws.title):
            return ws

    return None


def build_sheet_link(ws):
    return f"{SPREADSHEET_URL}#gid={ws.id}"
