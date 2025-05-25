# nmls_scraper/utils.py
import datetime
import re
import logging
import locale

try:
    locale.setlocale(locale.LC_TIME, 'ru_RU.UTF-8')
except locale.Error:
    try:
        locale.setlocale(locale.LC_TIME, 'ru_RU')
    except locale.Error:
        try:
             locale.setlocale(locale.LC_TIME, 'Russian')
        except locale.Error:
             logging.warning("Не удалось установить русскую локаль.")

month_map = {
    'января': 1, 'февраля': 2, 'марта': 3, 'апреля': 4, 'мая': 5, 'июня': 6,
    'июля': 7, 'августа': 8, 'сентября': 9, 'октября': 10, 'ноября': 11, 'декабря': 12,
    'январь': 1, 'февраль': 2, 'март': 3, 'апрель': 4, 'май': 5, 'июнь': 6,
    'июль': 7, 'август': 8, 'сентябрь': 9, 'октябрь': 10, 'ноябрь': 11, 'декабрь': 12
}

def parse_date_string(date_text, logger=None):
    """
    Парсит строку даты объявления и возвращает объект datetime.
    Обрабатывает различные форматы даты.
    :param date_text: Строка с датой (например, "Сегодня, 14:30", "15 мая", "01.01.2023 10:00").
    :param logger: Объект логгера (например, self.logger из Scrapy spider) для вывода предупреждений.
    :return: Объект datetime или None, если дата не распознана.
    """
    if not logger:
        logger = logging 

    if not date_text:
        logger.debug("Пустая строка даты для парсинга.")
        return None

    date_text = date_text.strip()
    now = datetime.datetime.now()
    parsed_date = None

    # Попытка 1: "Сегодня, ЧЧ:ММ" или "Вчера, ЧЧ:ММ"
    match_ty = re.search(r'(Сегодня|Вчера)\s*,\s*(\d{2}:\d{2})', date_text, re.IGNORECASE)
    if match_ty:
        day_word = match_ty.group(1).lower()
        time_str = match_ty.group(2)
        try:
            hour, minute = map(int, time_str.split(':'))
            target_date = now
            if day_word == 'вчера':
                target_date = now - datetime.timedelta(days=1)
            parsed_date = target_date.replace(year=now.year, month=now.month, day=target_date.day, hour=hour, minute=minute, second=0, microsecond=0)
        except ValueError:
            logger.warning(f"Время '{time_str}' неформат в '{date_text}' при парсинге даты.")

    # Попытка 2: "ДД месяц [ГГГГ]"
    if parsed_date is None:
        match_date_only = re.search(r'(\d{1,2})\s+([А-Яа-я]+)\s*(\d{4})?', date_text, re.IGNORECASE)
        if match_date_only:
            day = int(match_date_only.group(1))
            month_name = match_date_only.group(2).lower()
            year_str = match_date_only.group(3)

            month = month_map.get(month_name)

            if month is not None:
                year = int(year_str) if year_str else now.year
                try:
                    parsed_date = datetime.datetime(year, month, day, 0, 0, 0)
                except ValueError:
                    logger.warning(f"Дата из частей ({year}, {month}, {day}) неверна для '{date_text}' при парсинге даты.")
            else:
                logger.warning(f"Месяц '{month_name}' неизвестен в '{date_text}' при парсинге даты.")

    # Попытка 3: "дд.мм.гггг ЧЧ:ММ"
    if parsed_date is None:
        try:
           parsed_date = datetime.datetime.strptime(date_text, '%d.%m.%Y %H:%M')
        except (ValueError, TypeError):
           pass

    # Попытка 4: "дд.мм.гггг"
    if parsed_date is None:
        try:
           parsed_date = datetime.datetime.strptime(date_text, '%d.%m.%Y')
        except (ValueError, TypeError):
           pass

    if parsed_date is None:
        logger.warning(f"Дата '{date_text}' не распознана.")

    return parsed_date