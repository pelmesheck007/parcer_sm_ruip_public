from colorsys import rgb_to_hls


def apply_table_style(ws, rows):
    from gspread_formatting import (
        CellFormat,
        TextFormat,
        set_row_height,
        set_column_width,
        format_cell_ranges
    )
    from gspread_formatting import CellFormat, format_cell_ranges

    fmt = CellFormat(
        textFormat=TextFormat(fontSize=10, fontFamily="Calibre"),
        horizontalAlignment="LEFT",
        verticalAlignment="MIDDLE"

    )

    format_cell_ranges(
        ws,
        [
            (f"A1:J{rows+1}", fmt),
        ]
    )

    # (опционально) аккуратная высота строк
    set_row_height(ws, f"1:{rows+1}", 28)



def set_duration_format(ws, rows):
    duration_fmt = CellFormat(
        numberFormat=NumberFormat(
            type="DATE_TIME",   # ← ВАЖНО
            pattern="[h]:mm:ss"
        )
    )
    format_cell_ranges(ws, [(f"C2:C{rows+1}", duration_fmt)])


def set_duration_format2(ws, rows):
    duration_fmt = CellFormat(
        numberFormat=NumberFormat(
            type="DATE_TIME",   # ← ВАЖНО
            pattern="dd.MM.yyyy hh:mm:ss"
        )
    )
    format_cell_ranges(ws, [(f"B2:B{rows+1}", duration_fmt)])

from gspread_formatting import CellFormat, NumberFormat, format_cell_ranges

def set_number_format(ws, rows):
    number_fmt = CellFormat(
        numberFormat=NumberFormat(
            type="NUMBER",
            pattern="0"
        )
    )

    format_cell_ranges(
        ws,
        [(f"A2:A{rows+1}", number_fmt)]
    )

def apply_row_coloring(ws):
    from gspread_formatting import (
        ConditionalFormatRule,
        BooleanRule,
        BooleanCondition,
        CellFormat,
        Color,
        GridRange,
        get_conditional_format_rules,
    )

    rules = get_conditional_format_rules(ws)
    rules.clear()

    rng = GridRange.from_a1_range("A2:J1000", ws)

    # ВОЗВРАТ / ВЫШЛО ИЗ ОК
    rules.append(
        ConditionalFormatRule(
            ranges=[rng],
            booleanRule=BooleanRule(
                condition=BooleanCondition(
                    "CUSTOM_FORMULA",
                    ['=OR($E2="Возврат"; $E2="Вышло из ОК"; $E2="Возврат от коллег")']
                ),
                format=CellFormat(
                    backgroundColor=Color(1.0, 0.8, 0.85)  # розовый
                )
            )
        )
    )

    rules.append(
        ConditionalFormatRule(
            ranges=[rng],
            booleanRule=BooleanRule(
                condition=BooleanCondition(
                    "CUSTOM_FORMULA",
                    ['=OR($E2="Обработано"; $E2="Закрыта"; $E2="ОРЗ"; AND($E2="Ожидание клиента"; $F2="Пользователь"))']
                ),
                format=CellFormat(
                    backgroundColor=Color(216/255, 215/255, 215/255)  # розовый
                )
            )
        )
    )

    # RUIP-17154 ярко-зеленый
    rules.append(
        ConditionalFormatRule(
            ranges=[rng],
            booleanRule=BooleanRule(
                condition=BooleanCondition(
                    "CUSTOM_FORMULA",
                    ['=ISNUMBER(SEARCH("RUIP-17154"; $H2))']
                ),
                format=CellFormat(
                    backgroundColor=Color(0.0, 1.0, 0.0)  # ярко-зелёный
                )
            )
        )
    )


    # ПУСТО (ТОЛЬКО если B реально пустая)
    rules.append(
        ConditionalFormatRule(
            ranges=[rng],
            booleanRule=BooleanRule(
                condition=BooleanCondition(
                    "CUSTOM_FORMULA",
                    ['=AND(NOT(ISBLANK($A2)); TRIM($A2)<>""; OR(ISBLANK($B2); TRIM($B2)=""))']

                ),
                format=CellFormat(
                    backgroundColor=Color(211/255, 227/255, 253/255)  # голубой
                )
            )
        )
    )

    # ПРОСРОЧЕНО
    rules.append(
        ConditionalFormatRule(
            ranges=[rng],
            booleanRule=BooleanRule(
                condition=BooleanCondition(
                    "CUSTOM_FORMULA",
                    ['=AND(TRIM($B2)<>""; INT($B2) < TODAY())']
                ),
                format=CellFormat(
                    backgroundColor=Color(0.96, 0.80, 0.80)  # красный
                )
            )
        )
    )

    # СЕГОДНЯ
    rules.append(
        ConditionalFormatRule(
            ranges=[rng],
            booleanRule=BooleanRule(
                condition=BooleanCondition(
                    "CUSTOM_FORMULA",
                    ['=AND(TRIM($B2)<>""; INT($B2) = TODAY())']
                ),
                format=CellFormat(
                    backgroundColor=Color(252/255, 229/255, 205/255)  # оранжевый
                )
            )
        )
    )

    # ЗАВТРА
    rules.append(
        ConditionalFormatRule(
            ranges=[rng],
            booleanRule=BooleanRule(
                condition=BooleanCondition(
                    "CUSTOM_FORMULA",
                    ['=AND(TRIM($B2)<>""; INT($B2) = TODAY()+1)']
                ),
                format=CellFormat(
                    backgroundColor=Color(1.00, 0.95, 0.80)  # жёлтый
                )
            )
        )
    )

    # ВСЁ ОСТАЛЬНОЕ (будущее > завтра)
    rules.append(
        ConditionalFormatRule(
            ranges=[rng],
            booleanRule=BooleanRule(
                condition=BooleanCondition(
                    "CUSTOM_FORMULA",
                    ['=AND(TRIM($B2)<>""; INT($B2) > TODAY()+1)']
                ),
                format=CellFormat(
                    backgroundColor=Color(0.85, 0.92, 0.83)  # зелёный
                )
            )
        )
    )

    rules.save()


def copy_data_validations(sheet, source_ws, target_ws):
    import requests

    spreadsheet_id = sheet.id

    body = {
        "requests": [
            {
                "copyPaste": {
                    "source": {
                        "sheetId": source_ws._properties["sheetId"]
                    },
                    "destination": {
                        "sheetId": target_ws._properties["sheetId"]
                    },
                    "pasteType": "PASTE_DATA_VALIDATION",
                    "pasteOrientation": "NORMAL"
                }
            }
        ]
    }

    sheet.client.request(
        "post",
        f"https://sheets.googleapis.com/v4/spreadsheets/{spreadsheet_id}:batchUpdate",
        json=body,
    )

