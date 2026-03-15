import os
import json
import requests

COOKIES_FILE = "cookies.json"


class SMService:

    def __init__(self, base_url: str, view_id: int):
        self.base_url = base_url
        self.view_id = view_id

    # ================= Cookies =================

    def load_cookies(self):
        if not os.path.exists(COOKIES_FILE):
            return {}
        with open(COOKIES_FILE, "r", encoding="utf-8") as f:
            return json.load(f)

    def save_cookies(self, cookies: dict):
        with open(COOKIES_FILE, "w", encoding="utf-8") as f:
            json.dump(cookies, f, ensure_ascii=False, indent=4)

    def parse_cookie_string(raw_cookie: str):
        cookies_dict = {}

        raw_cookie = raw_cookie.strip()

        # удаляем возможные переносы строки
        raw_cookie = raw_cookie.replace("\n", "").replace("\r", "")

        parts = raw_cookie.split(";")

        for part in parts:
            part = part.strip()

            if "=" not in part:
                continue

            key, value = part.split("=", 1)

            key = key.strip()
            value = value.strip()

            # пропускаем служебные атрибуты
            if key.lower() in ["path", "secure", "httponly", "samesite", "domain"]:
                continue

            cookies_dict[key] = value

        print("PARSED COOKIES:", cookies_dict)  # ВАЖНО: временный debug

        return cookies_dict

    def is_cookie_valid(self, cookies: dict):
        try:
            response = requests.get(
                f"{self.base_url}/otrs/index.pl",
                cookies=cookies,
                timeout=10
            )

            if "login" in response.url.lower():
                return False

            return response.status_code == 200

        except Exception:
            return False

    # ================= Export =================

    def export(self, run_export_from_sm):
        cookies = self.load_cookies()

        if not cookies:
            raise Exception("COOKIE_MISSING")

        if not self.is_cookie_valid(cookies):
            raise Exception("COOKIE_EXPIRED")

        return run_export_from_sm(
            self.base_url,
            self.view_id,
            cookies
        )

    def export_by_ids(self, ticket_ids):
        """
        Получает тикеты напрямую по ID.
        """
        import requests

        cookies = self.load_cookies()

        rows = []

        for ticket_id in ticket_ids:

            url = f"{self.base_url}/sm/ESMPTicketZoom?TicketID={ticket_id}"

            response = requests.get(
                url,
                cookies=cookies,
                timeout=30
            )

            if response.status_code != 200:
                raise Exception("COOKIE_EXPIRED")

            #row = parse_single_ticket(response.text)
            #rows.append(row)

        return rows