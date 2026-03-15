import json
from datetime import datetime
import requests
import pandas as pd
import math


def export_sm_view_all_pages(base_url, view_id, cookies):
    url = f"{base_url}/sm/json.pl"

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

    all_rows = []
    page = 1
    pages = None

    while True:
        params = {
            "Action": "ESMPTicketSelectJSON",
            "Subaction": "GetTable",
            "ViewID": view_id,
            "Page": page,
            "Autorefresh": 1,
        }

        r = session.get(url, params=params, timeout=30)
        r.raise_for_status()
        data = r.json()
        import json

        content = data.get("Content", {})

        dump = {
            "TableData": content.get("TableData"),
            "HiddenTableData": content.get("HiddenTableData"),
            "MetaData": content.get("MetaData"),
        }

        ts = datetime.now().strftime("%Y%m%d_%H%M%S")

        #with open(f"sm_table_{ts}.json", "w", encoding="utf-8") as f:
        #    json.dump(dump, f, ensure_ascii=False, indent=2)

        #print(f"SM TABLE JSON сохранён: sm_table_{ts}.json")

        content = data.get("Content")
        if not content:
            raise RuntimeError("SM: нет Content")

        table = content.get("TableData")
        if not table:
            break


        all_rows.extend(table)

        meta = content.get("MetaData", {})
        total = int(meta.get("Total", len(table)))
        pagesize = int(meta.get("Pagesize", len(table)))
        pages = math.ceil(total / pagesize)

        print(f"SM: страница {page} → +{len(table)} строк")

        if page >= pages:
            break

        page += 1

    return pd.DataFrame(all_rows)

def export_sm_view_first_page(base_url, view_id, cookies):

    url = f"{base_url}/sm/json.pl"

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

    params = {
        "Action": "ESMPTicketSelectJSON",
        "Subaction": "GetTable",
        "ViewID": view_id,
        "Page": 1,
        "Autorefresh": 1,
    }

    r = session.get(url, params=params, timeout=30)
    r.raise_for_status()

    data = r.json()
    content = data.get("Content")

    if not content:
        raise RuntimeError("SM: нет Content")

    table = content.get("TableData")
    if not table:
        return pd.DataFrame()

    return pd.DataFrame(table)