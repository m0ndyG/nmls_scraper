# -*- coding: utf-8 -*-
import scrapy
import hashlib
import datetime
import json
import re
from urllib.parse import urlparse
from scrapy.utils.project import get_project_settings
from nmls_scraper.items import AdvertItem, ImageItem, PhoneItem
from nmls_scraper.utils import parse_date_string 

class NmlsSpider(scrapy.Spider):
    name = 'nmls_spider'
    allowed_domains = ['nmls.ru']

    def start_requests(self):
        settings = get_project_settings()
        crawl_specific = settings.getbool('SPECIFIC_REGION', False)
        specific_subdomain = settings.get('SPECIFIC_REGION_SUBDOMAIN')

        if crawl_specific and specific_subdomain:
            # Если включен парсинг конкретного региона
            region_url = f'https://{specific_subdomain}.nmls.ru/'
            self.logger.info(f"Парсинг только региона: {specific_subdomain}. Начальный URL: {region_url}")
            yield scrapy.Request(url=region_url, callback=self.parse_region_home)
        else:
            self.logger.info("Парсинг всех регионов. Начальный URL: https://nmls.ru/")
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

    def parse_regions(self, response):
        self.logger.info(f"Парсим регионы: {response.url}")
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
                    self.logger.info(f"Найден домен: {domain}. Переход на {region_url}")
                    yield response.follow(region_url, self.parse_region_home)
            except Exception as e:
                 self.logger.error(f"Ошибка ссылки региона '{link}': {e}")

    def parse_region_home(self, response):
        self.logger.info(f"Домашняя региона: {response.url}")
        section_urls = response.xpath(
            '//div[contains(@class, "realty-filter")]//a[contains(@class, "btn-category")]/@href'
        ).getall()
        if not section_urls:
            self.logger.warning(f"Нет ссылок в блоке realty-filter на {response.url}. Попытка поиска в навбаре.")
            section_urls = response.xpath('//nav[@class="navbar"]//a[contains(@class, "dropdown-item")]/@href').getall()


        if not section_urls:
             self.logger.warning(f"Нет ссылок разделов на {response.url} (ни в realty-filter, ни в навбаре).")
             return 

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
                    self.logger.info(f"Раздел: {full_url}, тип={advt_type_id}, кат={cat_id}")
                    yield response.follow(
                        full_url,
                        self.parse_listing_page,
                        meta={'cat_id': cat_id, 'advt_type_id': advt_type_id}
                    )
                else:
                    self.logger.debug(f"Пропуск ссылки (неизвестный тип/кат): {full_url}")
            else:
                 self.logger.debug(f"Пропуск ссылки (формат пути): {full_url}")

    def parse_listing_page(self, response):
        cat_id = response.meta.get('cat_id')
        advt_type_id = response.meta.get('advt_type_id')

        if cat_id is None or advt_type_id is None:
             self.logger.error(f"Нет cat_id/advt_type_id для {response.url}")
             return

        self.logger.info(f"Список: {response.url}")

        # Ссылки на объявления
        # заменил .css на .xpath
        ad_urls = response.xpath('//div[contains(@class, "listing-item")]/a[contains(@href, "/id")]/@href').getall()
        if not ad_urls:
             ad_urls = response.xpath('//a[contains(@href, "/id")]/@href').getall()

        if not ad_urls:
            self.logger.warning(f"Нет ссылок объявлений на {response.url}")

        for link in ad_urls:
            full_url = response.urljoin(link)
            if re.search(r'/id\d+$', full_url):
                 yield response.follow(
                     full_url,
                     self.parse_detail_page,
                     meta={'cat_id': cat_id, 'advt_type_id': advt_type_id}
                 )
            else:
                 self.logger.debug(f"Пропуск ссылки (не объявление): {link}")

        # Пагинация
        # заменил .css на .xpath
        next_url = response.xpath('//a[@rel="next"]/@href').get()
        if not next_url:
             next_page_indicator = response.xpath('//a[contains(@class, "page-link")]/span[@aria-hidden="true"]/text()').re_first(r'›')
             if next_page_indicator:
                 next_url = response.xpath('//a[contains(@class, "page-link") and contains(span[@aria-hidden="true"], "›")]/@href').get()

        if next_url:
            self.logger.info(f"След. страница: {next_url}")
            yield response.follow(
                next_url,
                self.parse_listing_page,
                meta={'cat_id': cat_id, 'advt_type_id': advt_type_id}
            )
        else:
             self.logger.info(f"Конец пагинации на {response.url}")

    def parse_detail_page(self, response):
        cat_id = response.meta.get('cat_id')
        advt_type_id = response.meta.get('advt_type_id')

        item = AdvertItem()
        item['url'] = response.url
        item['id'] = hashlib.sha1(response.url.encode('utf-8')).hexdigest()
        item['date_update'] = datetime.datetime.now()
        item['source'] = 8
        item['is_active'] = True

        self.logger.info(f"Объявление: {item['url']}")

        # заменил .css на .xpath
        item['title'] = ''.join(response.xpath('//h1//text()').getall()).strip() if response.xpath('//h1//text()').getall() else None

        # заменил .css на .xpath
        price_text = response.xpath('//div[contains(@class, "card-price")]/text()').get()
        if price_text:
            cleaned_price = re.sub(r'\D', '', price_text)
            try:
                item['price'] = int(cleaned_price) if cleaned_price else 0
            except ValueError:
                self.logger.warning(f"Цена '{price_text}' не число для {response.url}")
                item['price'] = 0
        else:
            item['price'] = 0

        # Контакты
        # заменил .css на .xpath
        contacts_block = response.xpath('//div[contains(@class, "object-infoblock") and contains(@class, "object-contacts")]')

        item['is_company'] = False
        item['contactname'] = None
        item['company'] = None
        phones_set = set() # Изменили список на set для автоматической уникальности

        # заменил .css на .xpath (относительно contacts_block)
        if contacts_block and contacts_block.xpath('./noindex').get() is None:
             self.logger.debug(f"Контакты видимы для {response.url}")

             # заменил .css на .xpath (относительно contacts_block)
             contact_person = contacts_block.xpath('.//div[contains(@class, "dit")]/div[contains(@class, "mb10")]/text()').get()
             if contact_person:
                  item['contactname'] = contact_person.strip()

             # заменил .css на .xpath (относительно contacts_block)
             company_text_lines = contacts_block.xpath('.//div[contains(@class, "dit")]/div[contains(@class, "mb10")]/text()').getall()
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
             # заменил .css на .xpath (относительно contacts_block)
             phone_hrefs = contacts_block.xpath('.//a[starts-with(@href, "tel:")]/@href').getall()
             for phone_href_raw in phone_hrefs:
                 # Упростил регулярное выражение
                 phone_digits = re.sub(r'\D', '', phone_href_raw)
                 if len(phone_digits) == 11 and phone_digits.startswith('7'):
                     phones_set.add(phone_digits) 
                 else:
                      self.logger.debug(f"Телефон '{phone_href_raw}' неформат для {response.url}")
        else:
            self.logger.debug(f"Контакты скрыты/нет для {response.url}")

        # Регион, Город
        # заменил .css на .xpath
        reg_city_text = response.xpath('//div[contains(@class, "header")]//div[contains(@class, "region")]/a/text()').get()
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


        address_parts = response.xpath('//table[@class="object_info"]//td[text()="Адрес"]/following-sibling::td//text()').getall()
        if address_parts:
            full_address = ' '.join([p.strip() for p in address_parts if p.strip()]).strip()
            full_address = re.sub(r'\s+', ' ', full_address).strip()
            full_address = re.sub(r'\s*,', ',', full_address)
            full_address = re.sub(r',+', ',', full_address)
            full_address = full_address.strip(', ')
            item['address'] = full_address if full_address else 'Не указан'
        else:
             item['address'] = 'Не указан'

        # Описание
        # заменил .css на .xpath
        desc_block = response.xpath('//div[contains(@class, "object-infoblock")]/div[contains(@class, "descr")]')
        if desc_block:
            # заменил .css на .xpath (относительно desc_block)
            desc_paragraphs = desc_block.xpath('./p/text()').getall()
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
        # заменил .css на .xpath
        lat_text = response.xpath('//div[@id="objectMap"]/@data-lat').get()
        lon_text = response.xpath('//div[@id="objectMap"]/@data-lng').get()
        try:
            item['lat'] = float(lat_text) if lat_text else None
            item['lon'] = float(lon_text) if lon_text else None
        except (ValueError, TypeError):
             self.logger.warning(f"Координаты ({lat_text}, {lon_text}) не число для {response.url}")
             item['lat'] = None
             item['lon'] = None

        # Параметры (jsonb)
        params_data = {}
        for row in response.xpath('//table[@class="object_info"]/tbody/tr'):
             tds = row.xpath('./td')
             if len(tds) < 2:
                 continue
             k_td = tds[0]
             v_td = tds[1]

             k_text = k_td.xpath('.//text()').get()
             if not k_text:
                 continue

             key = k_text.strip()

             if key == 'Адрес': continue
             elif key == 'Площадь (кв.м.)':
                 # заменил .css на .xpath 
                 area_values = v_td.xpath('./span[contains(@class, "d-none")]/text()').get()
                 if area_values:
                     params_data[key] = area_values.strip()
                 else:
                     # заменил .css на .xpath 
                     area_parts_raw = v_td.xpath('./span[contains(@class, "d-block") and contains(@class, "d-md-inline")]/string()').getall()
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
        # заменил .css на .xpath
        date_text = response.xpath(
            '//div[contains(@class, "object-header")]/span[contains(@style, "font-size")]/text() | '
            '//div[contains(@class, "object-header")]/span[contains(@class, "text-muted")]/text()'
        ).get()

        # Перенесли логику парсинга даты в отдельную функцию в utils.py
        item['date_posted'] = parse_date_string(date_text, logger=self.logger)


        yield item

        # Сбор изображений
        # заменил .css на .xpath
        image_urls_list = response.xpath('//div[contains(@class, "fotorama")]/a/@href').getall()
        if not image_urls_list:
             self.logger.debug(f"Нет картинок для {item['id']}")

        for img_url_raw in image_urls_list:
            img_item = ImageItem() 
            img_item['advt_id'] = item['id']
            img_item['url'] = response.urljoin(img_url_raw)
            img_item['date_update'] = datetime.datetime.now()
            yield img_item

        # Сбор телефонов
        if phones_set: # Теперь используем set
             for phone_digits in phones_set:
                 phone_item = PhoneItem() 
                 phone_item['advt_id'] = item['id']
                 try:
                      phone_item['phone'] = int(phone_digits)
                 except ValueError:
                      self.logger.warning(f"Телефон '{phone_digits}' не int для {response.url}")
                      continue
                 phone_item['is_fake'] = False
                 phone_item['date_update'] = datetime.datetime.now()
                 yield phone_item
        else:
             self.logger.debug(f"Телефонов не найдено для {item['id']}")