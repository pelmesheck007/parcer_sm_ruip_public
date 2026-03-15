import sqlite3
from pathlib import Path
from datetime import datetime
import pandas as pd

DB_PATH = Path("database/sm.db")
DB_PATH.parent.mkdir(exist_ok=True)


def get_conn():
    return sqlite3.connect(DB_PATH)


def init_db():
    with get_conn() as conn:
        conn.execute("""
        CREATE TABLE IF NOT EXISTS sm_raw (
            ticket_id TEXT PRIMARY KEY,
            status_sm TEXT,
            created_at TEXT,
            target_time TEXT,
            title TEXT,
            sla_left TEXT,
            last_sync TEXT
        )
        """)
        conn.commit()


def upsert_sm(df: pd.DataFrame):
    now = datetime.now().isoformat()

    with get_conn() as conn:
        for _, row in df.iterrows():
            conn.execute("""
            INSERT INTO sm_raw (
                ticket_id, status_sm, created_at,
                target_time, title, sla_left, last_sync
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(ticket_id) DO UPDATE SET
                status_sm=excluded.status_sm,
                target_time=excluded.target_time,
                sla_left=excluded.sla_left,
                last_sync=excluded.last_sync
            """, (
                row["Номер заявки"],
                row["Статус SM"],
                str(row["Дата создания"]),
                str(row["Время исполнения целевое"]),
                row["Наименование тикета"],
                row["Осталось SLA"],
                now
            ))
        conn.commit()


def load_sm():
    with get_conn() as conn:
        return pd.read_sql("SELECT * FROM sm_raw", conn)