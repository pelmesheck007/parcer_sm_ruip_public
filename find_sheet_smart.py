import re
from datetime import datetime

def find_best_sheet(sheet, target_name, date_pattern=r"\d{2}\.\d{2}"):
    """
    Ищет лист:
    1) по точному совпадению
    2) по совпадению даты
    3) самый свежий по дате
    4) первый лист книги
    """

    all_sheets = sheet.worksheets()
    all_names = [ws.title for ws in all_sheets]

    # 1Точное совпадение
    if target_name in all_names:
        return sheet.worksheet(target_name)

    # Ищем по совпадению даты
    match_date = re.search(date_pattern, target_name)
    if match_date:
        date_str = match_date.group()

        for name in all_names:
            if date_str and "(" in name:
                return sheet.worksheet(name)

    # 3️Берём самый новый лист по дате
    dated_sheets = []
    for name in all_names:
        m = re.search(date_pattern, name)
        if m:
            try:
                dt = datetime.strptime(m.group(), "%d.%m")
                dated_sheets.append((dt, name))
            except:
                pass

    if dated_sheets:
        dated_sheets.sort(reverse=True)
        return sheet.worksheet(dated_sheets[0][1])

    # Самый первый лист (как последний fallback)
    return all_sheets[0]
