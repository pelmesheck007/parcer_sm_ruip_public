# SM Parser Bot

Telegram-бот для обработки выгрузок из Service Manager и синхронизации данных с Google Sheets.

## Что делает проект
- Принимает Excel-файлы с заявками.
- Обновляет/создаёт листы в Google Sheets.
- Делает автообновление статусов и SLA.
- Публикует отчёты в Telegram.

## Технологии
- Python 3.11+
- aiogram
- pandas
- gspread + Google API
- sqlite3

## Быстрый старт
1. Клонируйте репозиторий.
2. Создайте виртуальное окружение и установите зависимости:
   ```bash
   python -m venv .venv
   source .venv/bin/activate  # Linux/macOS
   pip install -r requirements.txt
   ```
3. Создайте файл `.env` на основе `.env.example`.
4. Положите Google credentials JSON по пути из `GOOGLE_CREDENTIALS_PATH`.
5. Запустите бота:
   ```bash
   python app.py
   ```

## Важные замечания для публичного репозитория
- В репозиторий **не добавляются** токены, cookies, базы и рабочие данные.
- Для локального запуска используйте `.env` и локальные файлы в папках `data/` и `database/`.

## Структура
- `handlers/` — Telegram-хендлеры.
- `services/` — бизнес-логика и интеграции.
- `data/` — локальные входные/служебные данные (игнорируются git).
- `database/` — SQLite база (игнорируется git).
