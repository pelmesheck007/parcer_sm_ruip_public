# services/snapshot_service.py
import pickle
from pathlib import Path
from typing import Optional, Set

import pandas as pd
from aiofiles import os

from config import *


def load_snapshot():
    if os.path.exists(SNAPSHOT_FILE):
        with open(SNAPSHOT_FILE, "rb") as f:
            return pickle.load(f)
    return None


def save_snapshot(df):
    with open(SNAPSHOT_FILE, "wb") as f:
        pickle.dump(df, f)


def load_active_index():
    if os.path.exists(ACTIVE_INDEX_FILE):
        with open(ACTIVE_INDEX_FILE, "rb") as f:
            return pickle.load(f)
    return set()


def save_active_index(active_ids):
    with open(ACTIVE_INDEX_FILE, "wb") as f:
        pickle.dump(active_ids, f)


def get_active_ids(df):
    active_df = df[
        ~df["Статус по работе с заявкой"].isin(["Закрыта", "Обработано"])
    ]
    return set(active_df["Номер заявки"])


def get_changed_tickets(df_old, df_new):

    if df_old is None:
        return df_new  # первый запуск — всё новое

    df_old = df_old.set_index("Номер заявки")
    df_new = df_new.set_index("Номер заявки")

    changed_ids = []

    for ticket_id in df_new.index:

        if ticket_id not in df_old.index:
            changed_ids.append(ticket_id)
            continue

        if not df_new.loc[ticket_id].equals(df_old.loc[ticket_id]):
            changed_ids.append(ticket_id)

    return df_new.loc[changed_ids].reset_index()
