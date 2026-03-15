import math
import requests
import pandas as pd
import time

from export_to_sheet import run_export_sm
from sm_export_page import export_sm_view_all_pages

def export_sm_view_json(base_url, view_id, cookies):
    import requests
    import pandas as pd

    url = f"{base_url}/sm/json.pl"

    params = {
        "Action": "ESMPTicketSelectJSON",
        "Subaction": "GetTable",
        "ViewID": view_id,
        "Page": 1,
        "Autorefresh": 1,
    }

    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json",
        "X-Requested-With": "XMLHttpRequest",
        "Referer": f"{base_url}/sm/ESMPTicketSelect?ViewID={view_id}",
    }

    session = requests.Session()
    session.headers.update(headers)
    session.cookies.set(
        "OTRSAgentInterface",
        cookies["OTRSAgentInterface"],
        domain="sm.gasvybory2.ru",
        path="/sm",
    )

    r = session.get(url, params=params, timeout=30)
    r.raise_for_status()

    data = r.json()
    if data.get("Success") not in ("1", 1, True):
        raise RuntimeError("SM вернул Success=0")

    content = data["Content"]

    table = content.get("TableData", [])
    hidden = content.get("HiddenTableData", [])

    if not table:
        raise RuntimeError("SM: TableData пустая")

    print("\n===== RAW SM TableData =====")
    for i, row in enumerate(table[:5]):  # первые 5 строк
        print(f"\n--- ROW {i} ---")
        for j, cell in enumerate(row):
            print(f"[{j}] {cell}")
    print("===== END TableData =====\n")

    # превращаем в DataFrame
    df = pd.DataFrame(table)

    # ───── SLA ИЗ HiddenTableData ─────
    sla_values = []

    for row in hidden:
        sla = ""
        if isinstance(row, dict):
            title = row.get("title", "")
            if isinstance(title, str) and "Осталось" in title:
                sla = title.replace("Осталось:", "").strip()
        sla_values.append(sla)

    # защита от рассинхрона
    while len(sla_values) < len(df):
        sla_values.append("")

    df["Осталось SLA"] = sla_values

    return df

def parse_sm_df(raw_df: pd.DataFrame) -> pd.DataFrame:
    out = pd.DataFrame()

    # Номер заявки
    out["Номер заявки"] = raw_df[0].apply(
        lambda x: x.get("label") if isinstance(x, dict) else ""
    )

    # Время исполнения целевое (дата)
    out["Время исполнения целевое"] = raw_df[1].apply(
        lambda x: x.get("label") if isinstance(x, dict) else ""
    )

    # Осталось SLA (HH:MM:SS)
    def extract_sla(x):
        if isinstance(x, dict):
            title = x.get("title", "")
            if "Осталось:" in title:
                return title.replace("Осталось:", "").strip()
        return ""

    out["Осталось SLA"] = raw_df[1].apply(extract_sla)

    return out

def run_export_from_sm(base_url, view_id, cookies):
    df_raw = export_sm_view_all_pages(base_url, view_id, cookies)
    for i, cell in enumerate(df_raw.iloc[0]):
        print(i, cell)

    df_norm = normalize_sm_df(df_raw)


    return df_norm


import pandas as pd

def _first_row(df_raw: pd.DataFrame):
    if df_raw is None or df_raw.empty:
        return None
    return df_raw.iloc[0].tolist()

def _find_col_idx_by_predicate(df_raw: pd.DataFrame, pred):
    row0 = _first_row(df_raw)
    if row0 is None:
        return None
    for i, v in enumerate(row0):
        try:
            if pred(v):
                return i
        except Exception:
            pass
    return None

def _as_label(v):
    if isinstance(v, dict):
        return str(v.get("label", "")).strip()
    return str(v).strip() if v is not None else ""

def _as_title(v):
    if isinstance(v, dict):
        return str(v.get("title", "")).strip()
    return ""

def normalize_sm_df(df_raw: pd.DataFrame) -> pd.DataFrame:
    rows = []

    for _, row in df_raw.iterrows():
        ticket_number = ""
        target_time = ""
        sla_left = ""
        jira_note = ""
        client_company = ""

        # 0 — номер заявки
        if isinstance(row[0], dict):
            ticket_number = row[0].get("label", "")

        # 3 — дедлайн + SLA
        if len(row) > 3 and isinstance(row[3], dict):
            target_time = row[3].get("label", "")

            title = row[3].get("title", "")
            if isinstance(title, str) and "Осталось" in title:
                sla_left = title.replace("Осталось:", "").strip()

        # 5 — "(доп.) Заявка Jira"
        if len(row) > 5 and isinstance(row[5], str):
            jira_note = row[5]

        # 19 — "Клиент. Компания"
        if len(row) > 19 and isinstance(row[19], str):
            client_company = row[19]

        rows.append({
            "Номер заявки": ticket_number,
            "Время исполнения целевое": target_time,
            "Осталось SLA": sla_left,
            "(доп.) Заявка Jira": jira_note,
            "Клиент. Компания": client_company,
        })

    return pd.DataFrame(rows)

