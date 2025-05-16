# -*- coding: utf-8 -*-

import scrapy
import hashlib
import datetime
import json
import re
from urllib.parse import urlparse
import logging
import locale
# Импорт для чтения настроек
from scrapy.utils.project import get_project_settings

# Попытка установить русскую локаль
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


# Импорт Item
from nmls_scraper.items import obyavlenie, image, phone

class NmlsSpider(scrapy.Spider):
    name = 'nmls_spider'
    allowed_domains = ['nmls.ru']

    def start_requests(self):
        # Получаем настройки проекта
        settings = get_project_settings()
        crawl_specific = settings.getbool('SPECIFIC_REGION', False)
        specific_subdomain = settings.get('SPECIFIC_REGION_SUBDOMAIN')

        if crawl_specific and specific_subdomain:
            # Если включен парсинг конкретного региона
            region_url = f'https://{specific_subdomain}.nmls.ru/'
            logging.info(f"Парсинг только региона: {specific_subdomain}. Начальный URL: {region_url}")
            # Пропускаем парсинг всех регионов и сразу идем на домашнюю страницу выбранного региона
            yield scrapy.Request(url=region_url, callback=self.parse_region_home)
        else:
            # По умолчанию парсим все регионы, начиная с главной страницы
            logging.info("Парсинг всех регионов. Начальный URL: https://nmls.ru/")
            yield scrapy.Request(url='https://nmls.ru/', callback=self.parse_regions)

    # Сопоставление URL-сегментов и ID категорий/типов
    CAT_MAP = {
        'kvartir': 1, 'komnat': 2, 'domov': 3, 'zemelnyh-uchastkov': 4,
        'garazhey': 5, 'kommercheskoy-nedvizhimosti': 6,
    }
    ADVT_MAP = {
        'prodazha': 2, 'arenda': 3,
    }

    # Регулярки для URL
    PATH_SEG_RE = re.compile(r'/([^/]+)-([^/]+)')
    DOMAIN_RE = re.compile(r'https?://([^.]+)\.nmls\.ru')

    def parse_regions(self, response):
        # Этот метод вызывается только если SPECIFIC_REGION = False
        logging.info(f"Парсим регионы: {response.url}")
        region_urls = response.xpath('//div[@id="regions-modal"]//a[contains(@href, "nmls.ru")]/@href').getall()

        seen_domains = set()
        for link in region_urls:
            try:
                parsed_u = urlparse(link)
                domain = parsed_u.netloc
                if domain.startswith('www.'):
                    domain = domain[4:]

                if domain and domain not in seen_domains:
                    seen_domains.add(domain)
                    region_url = f'{parsed_u.scheme}://{domain}/'
                    logging.info(f"Найден домен: {domain}. Переход на {region_url}")
                    yield response.follow(region_url, self.parse_region_home)
            except Exception as e:
                 logging.error(f"Ошибка ссылки региона '{link}': {e}")

    def parse_region_home(self, response):
        # Этот метод вызывается только если SPECIFIC_REGION = True
        logging.info(f"Домашняя региона: {response.url}")
        section_urls = response.css('.navbar-nav a.dropdown-item::attr(href)').getall()

        if not section_urls:
             logging.warning(f"Нет ссылок разделов на {response.url}")

        for link in section_urls:
            full_url = response.urljoin(link)
            parsed_u = urlparse(full_url)
            path = parsed_u.path

            match = self.PATH_SEG_RE.search(path)
            if match:
                advt_type_seg = match.group(1)
                cat_seg = match.group(2)

                advt_type_id = self.ADVT_MAP.get(advt_type_seg)
                cat_id = self.CAT_MAP.get(cat_seg)

                if advt_type_id is not None and cat_id is not None:
                    logging.info(f"Раздел: {full_url}, тип={advt_type_id}, кат={cat_id}")
                    yield response.follow(
                        full_url,
                        self.parse_listing_page,
                        meta={'cat_id': cat_id, 'advt_type_id': advt_type_id}
                    )
                else:
                    logging.debug(f"Пропуск ссылки (неизвестный тип/кат): {full_url}")
            else:
                 logging.debug(f"Пропуск ссылки (формат пути): {full_url}")

    def parse_listing_page(self, response):
        cat_id = response.meta.get('cat_id')
        advt_type_id = response.meta.get('advt_type_id')

        if cat_id is None or advt_type_id is None:
             logging.error(f"Нет cat_id/advt_type_id для {response.url}")
             return

        logging.info(f"Список: {response.url}")

        # Ссылки на объявления
        ad_urls = response.css('div.listing-item a[href*="/id"]::attr(href)').getall()
        if not ad_urls:
             ad_urls = response.css('a[href*="/id"]::attr(href)').getall()

        if not ad_urls:
            logging.warning(f"Нет ссылок объявлений на {response.url}")

        for link in ad_urls:
            full_url = response.urljoin(link)
            if re.search(r'/id\d+$', full_url):
                 yield response.follow(
                     full_url,
                     self.parse_detail_page,
                     meta={'cat_id': cat_id, 'advt_type_id': advt_type_id}
                 )
            else:
                 logging.debug(f"Пропуск ссылки (не объявление): {link}")

        # Пагинация
        next_url = response.css('a[rel="next"]::attr(href)').get()
        if not next_url:
             next_page_indicator = response.css('a.page-link span[aria-hidden="true"]::text').re_first(r'›')
             if next_page_indicator:
                 next_url = response.css('a.page-link:contains("›")::attr(href)').get()

        if next_url:
            logging.info(f"След. страница: {next_url}")
            yield response.follow(
                next_url,
                self.parse_listing_page,
                meta={'cat_id': cat_id, 'advt_type_id': advt_type_id}
            )
        else:
             logging.info(f"Конец пагинации на {response.url}")

    def parse_detail_page(self, response):
        cat_id = response.meta.get('cat_id')
        advt_type_id = response.meta.get('advt_type_id')

        item = obyavlenie()
        item['url'] = response.url
        item['id'] = hashlib.sha1(response.url.encode('utf-8')).hexdigest()
        item['date_update'] = datetime.datetime.now()
        item['source'] = 8
        item['is_active'] = True

        logging.info(f"Объявление: {item['url']}")

        item['title'] = ''.join(response.css('h1 ::text').getall()).strip() if response.css('h1 ::text').getall() else None

        price_text = response.css('.card-price::text').get()
        if price_text:
            cleaned_price = re.sub(r'\D', '', price_text)
            try:
                item['price'] = int(cleaned_price) if cleaned_price else 0
            except ValueError:
                logging.warning(f"Цена '{price_text}' не число для {response.url}")
                item['price'] = 0
        else:
            item['price'] = 0

        # Контакты
        contacts_block = response.css('.object-infoblock.object-contacts')

        item['is_company'] = False
        item['contactname'] = None
        item['company'] = None
        phones_list = []

        if contacts_block and contacts_block.css('noindex').get() is None:
             logging.debug(f"Контакты видимы для {response.url}")

             contact_person = contacts_block.css('.dit div.mb10::text').get()
             if contact_person:
                  item['contactname'] = contact_person.strip()

             company_text_lines = contacts_block.css('.dit div.mb10::text').getall()
             company_org_name = None
             for line in company_text_lines:
                 if 'Агентство недвижимости:' in line:
                     company_org_name = line.replace('Агентство недвижимости:', '').strip()
                     break

             if company_org_name:
                  item['is_company'] = True
                  item['company'] = company_org_name
                  if item['contactname'] is None:
                       item['contactname'] = company_org_name
             elif item['contactname']:
                 item['is_company'] = False

             # Сбор телефонов
             phone_hrefs = contacts_block.css('a[href^="tel:"]::attr(href)').getall()
             for phone_href_raw in phone_hrefs:
                 phone_digits = re.sub(r'\D', '', phone_href_raw.replace('tel:', '').replace('tel:+', ''))
                 if len(phone_digits) == 11 and phone_digits.startswith('7'):
                     if phone_digits not in phones_list:
                          phones_list.append(phone_digits)
                     else:
                          logging.debug(f"Дубликат телефона: {phone_digits} для {response.url}")
                 else:
                      logging.debug(f"Телефон '{phone_href_raw}' неформат для {response.url}")
        else:
            logging.debug(f"Контакты скрыты/нет для {response.url}")

        # Регион, Город
        reg_city_text = response.css('div.header .region a::text').get()
        if reg_city_text:
            reg_city_text = reg_city_text.strip()
            parts = [p.strip() for p in re.split(r' и |, ', reg_city_text) if p.strip()]
            if len(parts) > 1:
                 item['city'] = parts[0]
                 item['region'] = parts[-1]
            elif parts:
                 item['city'] = parts[0]
                 item['region'] = parts[0]
            else:
                 item['city'] = 'Не указан'
                 item['region'] = 'Не указан'
        else:
             item['city'] = 'Не указан'
             item['region'] = 'Не указан'

        # Адрес
        address_td = response.xpath('//table[@class="object_info"]//td[text()="Адрес"]/following-sibling::td').get()
        if address_td:
            selector = scrapy.Selector(text=f'<td>{address_td}</td>')
            full_address = ' '.join(selector.xpath('//td//text()').getall()).strip()
            full_address = re.sub(r'\s+', ' ', full_address).strip()
            full_address = re.sub(r'\s*,', ',', full_address)
            full_address = re.sub(r',+', ',', full_address)
            full_address = full_address.strip(', ')
            item['address'] = full_address if full_address else 'Не указан'
        else:
             item['address'] = 'Не указан'

        # Описание
        desc_block = response.css('.object-infoblock .descr')
        if desc_block:
            desc_paragraphs = desc_block.css('p::text').getall()
            if desc_paragraphs:
                 desc_text = ' '.join([p.strip() for p in desc_paragraphs if p.strip()]).strip()
                 item['description'] = re.sub(r'\s+', ' ', desc_text).strip()
            else:
                 desc_text = ' '.join(desc_block.xpath('.//text()').getall()).strip()
                 item['description'] = re.sub(r'\s+', ' ', desc_text).strip() if desc_text else 'Нет описания'
        else:
             item['description'] = 'Нет описания'

        item['advt_type'] = advt_type_id
        item['cat'] = cat_id

        # Координаты
        lat_text = response.css('#objectMap::attr(data-lat)').get()
        lon_text = response.css('#objectMap::attr(data-lng)').get()
        try:
            item['lat'] = float(lat_text) if lat_text else None
            item['lon'] = float(lon_text) if lon_text else None
        except (ValueError, TypeError):
             logging.warning(f"Координаты ({lat_text}, {lon_text}) не число для {response.url}")
             item['lat'] = None
             item['lon'] = None

        # Параметры (jsonb)
        params_data = {}
        for row in response.xpath('//table[@class="object_info"]/tbody/tr'):
             tds = row.xpath('./td')
             if len(tds) >= 2:
                 k_td = tds[0]
                 v_td = tds[1]

                 k_text = k_td.xpath('.//text()').get()
                 if k_text:
                     key = k_text.strip()

                     if key == 'Адрес': continue
                     elif key == 'Площадь (кв.м.)':
                         area_values = v_td.css('span.d-none::text').get()
                         if area_values:
                             params_data[key] = area_values.strip()
                         else:
                             area_parts_raw = v_td.css('span.d-block.d-md-inline').xpath('string()').getall()
                             area_parts = [p.strip() for p in area_parts_raw if p.strip()]
                             params_data[key] = ' '.join(area_parts) if area_parts else ' '.join(v_td.xpath('.//text()').getall()).strip()
                     else:
                         v_parts = v_td.xpath('.//text()').getall()
                         value = ''.join(v_parts).strip()
                         value = re.sub(r'\s*–.*', '', value).strip()
                         value = re.sub(r'\s+', ' ', value).strip()
                         if value:
                            params_data[key] = value

        item['params'] = json.dumps(params_data, ensure_ascii=False)

        # Дата публикации
        date_text = response.css('.object-header span[style*="font-size"]::text, .object-header .text-muted::text').get()
        item['date_posted'] = None

        if date_text:
            date_text = date_text.strip()
            now = datetime.datetime.now()

            month_map = {
                'января': 1, 'февраля': 2, 'марта': 3, 'апреля': 4, 'мая': 5, 'июня': 6,
                'июля': 7, 'августа': 8, 'сентября': 9, 'октября': 10, 'ноября': 11, 'декабря': 12,
                'январь': 1, 'февраль': 2, 'март': 3, 'апрель': 4, 'май': 5, 'июнь': 6,
                'июль': 7, 'август': 8, 'сентябрь': 9, 'октябрь': 10, 'ноябрь': 11, 'декабрь': 12
            }

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
                    # Установка даты с учетом года, месяца, дня от "Сегодня" или "Вчера"
                    item['date_posted'] = target_date.replace(year=now.year, month=now.month, day=target_date.day, hour=hour, minute=minute, second=0, microsecond=0)

                except ValueError:
                    logging.warning(f"Время '{time_str}' неформат в '{date_text}' для {response.url}")

            # Попытка 2: "ДД месяц [ГГГГ]"
            if item['date_posted'] is None:
                match_date_only = re.search(r'(\d{1,2})\s+([А-Яа-я]+)\s*(\d{4})?', date_text, re.IGNORECASE)
                if match_date_only:
                    day = int(match_date_only.group(1))
                    month_name = match_date_only.group(2).lower()
                    year_str = match_date_only.group(3)

                    month = month_map.get(month_name)

                    if month is not None:
                        year = int(year_str) if year_str else now.year

                        try:
                            item['date_posted'] = datetime.datetime(year, month, day, 0, 0, 0)
                        except ValueError:
                             logging.warning(f"Дата из частей ({year}, {month}, {day}) неверна для '{date_text}' на {response.url}")
                    else:
                         logging.warning(f"Месяц '{month_name}' неизвестен в '{date_text}' на {response.url}")

            # Попытка 3: "дд.мм.гггг ЧЧ:ММ"
            if item['date_posted'] is None:
                 try:
                    item['date_posted'] = datetime.datetime.strptime(date_text, '%d.%m.%Y %H:%M')
                 except (ValueError, TypeError):
                    pass

            # Попытка 4: "дд.мм.гггг"
            if item['date_posted'] is None:
                 try:
                    item['date_posted'] = datetime.datetime.strptime(date_text, '%d.%m.%Y')
                 except (ValueError, TypeError):
                    pass

            if item['date_posted'] is None:
                 logging.warning(f"Дата '{date_text}' не распознана для {response.url}")
        else:
            logging.debug(f"Строка даты не найдена для {response.url}")

        yield item

        # Сбор изображений
        image_urls_list = response.css('.fotorama a::attr(href)').getall()
        if not image_urls_list:
             logging.debug(f"Нет картинок для {item['id']}")

        for img_url_raw in image_urls_list:
            img_item = image()
            img_item['advt_id'] = item['id']
            img_item['url'] = response.urljoin(img_url_raw)
            img_item['date_update'] = datetime.datetime.now()
            yield img_item

        # Сбор телефонов
        if phones_list:
             for phone_digits in phones_list:
                 phone_item = phone()
                 phone_item['advt_id'] = item['id']
                 try:
                      phone_item['phone'] = int(phone_digits)
                 except ValueError:
                      logging.warning(f"Телефон '{phone_digits}' не int для {response.url}")
                      continue
                 phone_item['is_fake'] = False
                 phone_item['date_update'] = datetime.datetime.now()
                 yield phone_item
        else:
             logging.debug(f"Телефонов не найдено для {item['id']}")