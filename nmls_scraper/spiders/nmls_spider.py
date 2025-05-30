# -*- coding: utf-8 -*-
import scrapy
import hashlib
import datetime
import json
import re
from urllib.parse import urlparse, urlunparse, urlencode, parse_qs
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
            region_url = f'https://{specific_subdomain}.nmls.ru/'
            self.logger.info(f"парсинг только региона: {specific_subdomain}. начальный url: {region_url}")
            yield scrapy.Request(url=region_url, callback=self.parse_region_home)
        else:
            self.logger.info("парсинг всех регионов. начальный url: https://nmls.ru/")
            yield scrapy.Request(url='https://nmls.ru/', callback=self.parse_regions)


    CAT_MAP = {
        'kvartir': 1, 'komnat': 2, 'domov': 3, 'zemelnyh-uchastkov': 4,
        'garazhey': 5, 'kommercheskoy-nedvizhimosti': 6,
    }
    ADVT_MAP = {
        'prodazha': 2, 'arenda': 3,
    }

    PATH_SEG_RE = re.compile(r'/([^/]+)-([^/]+)')

    def parse_regions(self, response):
        self.logger.info(f"парсим регионы: {response.url}")
        region_urls = response.xpath('//div[@id="regions-modal"]//a[contains(@href, "nmls.ru")]/@href').getall()

        seen_domains = set()
        for link in region_urls:
            try:
                parsed_u = urlparse(link)
                domain = parsed_u.netloc
                if domain.startswith('www.'):
                    domain = domain[4:]

                if not domain or domain in seen_domains:
                    self.logger.debug(f"пропуск домена (пустой или уже виден): {domain} для ссылки {link}")
                    continue

                seen_domains.add(domain)
                region_url = f'{parsed_u.scheme}://{domain}/'
                self.logger.info(f"найден домен: {domain}. переход на {region_url}")
                #   передаем домен как идентификатор региона для статистики
                yield scrapy.Request(region_url, self.parse_region_home, meta={'region_domain': domain.split('.')[0]})
            except Exception as e:
                 self.logger.error(f"ошибка ссылки региона '{link}': {e}")

    def parse_region_home(self, response):
        #   получаем домен региона из meta, если есть. иначе из url.
        region_domain = response.meta.get('region_domain', urlparse(response.url).netloc.split('.')[0])
        self.logger.info(f"домашняя региона: {response.url} (регион: {region_domain})")

        section_urls = response.xpath(
            '//div[contains(@class, "realty-filter")]//a[contains(@class, "btn-category")]/@href'
        ).getall()

        if not section_urls:
            self.logger.warning(f"нет ссылок в блоке realty-filter на {response.url}. попытка поиска в навбаре.")
            section_urls = response.xpath('//nav[@class="navbar"]//a[contains(@class, "dropdown-item")]/@href').getall()

        if not section_urls:
             self.logger.warning(f"нет ссылок разделов на {response.url} (ни в realty-filter, ни в навбаре).")
             return

        for link in section_urls:
            full_url = response.urljoin(link)
            parsed_u = urlparse(full_url)
            path = parsed_u.path

            match = self.PATH_SEG_RE.search(path)
            if not match:
                self.logger.debug(f"пропуск ссылки (неверный формат пути): {full_url}")
                continue

            advt_type_seg = match.group(1)
            cat_seg = match.group(2)

            advt_type_id = self.ADVT_MAP.get(advt_type_seg)
            cat_id = self.CAT_MAP.get(cat_seg)

            if advt_type_id is None or cat_id is None:
                self.logger.debug(f"пропуск ссылки (неизвестный тип/кат): {full_url}")
                continue

            self.logger.info(f"раздел: {full_url}, тип={advt_type_id}, кат={cat_id}")
            #   передаем region_domain, чтобы использовать его в parse_listing_page для статистики
            yield scrapy.Request(
                full_url,
                self.parse_listing_page,
                meta={
                    'cat_id': cat_id,
                    'advt_type_id': advt_type_id,
                    'initial_category_page': True,
                    'region_domain': region_domain 
                }
            )

    def parse_listing_page(self, response):
        cat_id = response.meta.get('cat_id')
        advt_type_id = response.meta.get('advt_type_id')
        current_page_num = response.meta.get('current_page', 1)
        all_pages_generated = response.meta.get('all_pages_generated', False)
        total_pages = response.meta.get('total_pages', 'N/A') 
        region_domain = response.meta.get('region_domain', 'unknown_region') 

        if cat_id is None or advt_type_id is None:
             self.logger.error(f"нет cat_id/advt_type_id для {response.url}")
             return

        #   инкрементируем счетчик страниц для региона
        self.crawler.stats.inc_value(f'pages_crawled_by_region/{region_domain}')
        self.crawler.stats.inc_value('total_pages_crawled')

        self.logger.info(f"парсим страницу списка объявлений: {response.url} (страница: {current_page_num} из {total_pages})")

        #   логика пагинации по последней странице
        if not all_pages_generated:
            # ищем ссылку на последнюю страницу, используя класс nav-last
            last_page_link = response.xpath("//a[@class='nav-last']/@href").get()
            last_page_num = 1
            
            # если ссылки на последнюю страницу нет, но есть rel="next", ищем max page number в других ссылках
            if not last_page_link:
                page_links = response.xpath("//a[contains(@href, 'page=')]/@href").getall()
                for plink in page_links:
                    page_match = re.search(r'page=(\d+)', plink)
                    if page_match:
                        last_page_num = max(last_page_num, int(page_match.group(1)))
            
            elif last_page_link:
                last_page_match = re.search(r'page=(\d+)', last_page_link)
                if last_page_match:
                    last_page_num = int(last_page_match.group(1))

            # если нашли больше одной страницы, генерируем все
            if last_page_num > 1:
                parsed_url = urlparse(response.url)
                base_url = urlunparse(parsed_url._replace(query=""))

                self.logger.info(f"найдена последняя страница ({last_page_num}). генерируем запросы для всех страниц в этой категории.")
                #   инкрементируем счетчик категорий, для которых сгенерировали все страницы
                self.crawler.stats.inc_value(f'categories_full_pagination_generated/{region_domain}')
                
                for page_num in range(1, last_page_num + 1):
                    params = {'page': page_num}
                    new_query = urlencode(params)
                    full_page_url = urlunparse(parsed_url._replace(query=new_query))
                    
                    yield scrapy.Request(
                        full_page_url,
                        self.parse_listing_page,
                        meta={
                            'cat_id': cat_id,
                            'advt_type_id': advt_type_id,
                            'all_pages_generated': True, 
                            'current_page': page_num,
                            'total_pages': last_page_num, 
                            'region_domain': region_domain 
                        }
                    )
                return # после генерации всех страниц, текущую обработку прекращаем

        # Если пагинация уже сгенерирована или это первая страница без явной "последней страницы"
        # парсим текущую страницу
        ad_urls = response.xpath('//div[contains(@class, "object-title")]/a/@href').getall()

        if not ad_urls:
            self.logger.warning(f"нет ссылок объявлений на {response.url}")
        else:
            self.logger.info(f"на странице {current_page_num} найдено {len(ad_urls)} объявлений.")

        for link in ad_urls:
            full_url = response.urljoin(link)
            if not re.search(r'/id\d+$', full_url):
                 self.logger.debug(f"пропуск ссылки (не объявление): {link}")
                 continue

            yield scrapy.Request(
                full_url,
                self.parse_detail_page,
                meta={'cat_id': cat_id, 'advt_type_id': advt_type_id, 'region_domain': region_domain} #   передаем region_domain
            )

        # если пагинация по "последней странице" не сработала, то используем старую логику "следующая"
        if not all_pages_generated:
            next_url = response.xpath('//a[@rel="next"]/@href').get()
            if not next_url:
                next_page_indicator = response.xpath('//a[contains(@class, "page-link")]/span[@aria-hidden="true"]/text()').re_first(r'›')
                if next_page_indicator:
                    next_url = response.xpath('//a[contains(@class, "page-link") and contains(span[@aria-hidden="true"], "›")]/@href').get()

            if next_url:
                self.logger.info(f"след. страница: {next_url}")
                yield scrapy.Request(
                    next_url,
                    self.parse_listing_page,
                    meta={
                        'cat_id': cat_id,
                        'advt_type_id': advt_type_id,
                        'current_page': current_page_num + 1,
                        'total_pages': total_pages, # передаем текущее значение total_pages
                        'region_domain': region_domain # передаем дальше
                    }
                )
            else:
                 self.logger.info(f"конец пагинации на {response.url}")


    def parse_detail_page(self, response):
        cat_id = response.meta.get('cat_id')
        advt_type_id = response.meta.get('advt_type_id')
        region_domain = response.meta.get('region_domain', 'unknown_region') #   получаем регион для статистики

        item = AdvertItem()
        item['url'] = response.url
        item['id'] = hashlib.sha1(response.url.encode('utf-8')).hexdigest()
        item['date_update'] = datetime.datetime.now()
        item['source'] = 8
        item['is_active'] = True

        #   логируем URL и ID объявления
        self.logger.info(f"парсим объявление: {item['url']} (id: {item['id']})")

        item['title'] = ''.join(response.xpath('//h1//text()').getall()).strip() or None

        price_text = response.xpath('//div[contains(@class, "card-price")]/text()').get()
        if price_text:
            cleaned_price = re.sub(r'\D', '', price_text)
            try:
                item['price'] = int(cleaned_price) if cleaned_price else 0
            except ValueError:
                self.logger.warning(f"цена '{price_text}' не число для {response.url}")
                item['price'] = 0
        else:
            item['price'] = 0 

        # Контакты
        contacts_block = response.xpath('//div[contains(@class, "object-infoblock") and contains(@class, "object-contacts")]')
        item['is_company'] = False
        item['contactname'] = None
        item['company'] = None
        phones_set = set()

        if not contacts_block or contacts_block.xpath('./noindex').get() is not None:
             self.logger.debug(f"контакты скрыты/нет для {response.url}")
        else:
             contact_person = contacts_block.xpath('.//div[contains(@class, "dit")]/div[contains(@class, "mb10")]/text()').get()
             if contact_person:
                  item['contactname'] = contact_person.strip() or None
             company_text_lines = contacts_block.xpath('.//div[contains(@class, "dit")]/div[contains(@class, "mb10")]/text()').getall()
             company_org_name = None
             for line in company_text_lines:
                 if 'Агентство недвижимости:' in line:
                     company_org_name = line.replace('Агентство недвижимости:', '').strip()
                     break

             if company_org_name:
                  item['is_company'] = True
                  item['company'] = company_org_name or None
                  if item['contactname'] is None:
                       item['contactname'] = company_org_name or None
             elif item['contactname']: 
                 item['is_company'] = False

             # Сбор телефонов
             phone_hrefs = contacts_block.xpath('.//a[starts-with(@href, "tel:")]/@href').getall()
             for phone_href_raw in phone_hrefs:
                 phone_digits = re.sub(r'\D', '', phone_href_raw)
                 if not (len(phone_digits) == 11 and phone_digits.startswith('7')):
                      self.logger.debug(f"телефон '{phone_href_raw}' неформат для {response.url}")
                      continue
                 phones_set.add(phone_digits)

        # Регион, Город
        reg_city_text = response.xpath('//div[contains(@class, "header")]//div[contains(@class, "region")]/a/text()').get()
        item['city'] = None 
        item['region'] = None 

        if reg_city_text:
            reg_city_text = reg_city_text.strip()
            parts = [p.strip() for p in re.split(r' и |, ', reg_city_text) if p.strip()]
            if len(parts) > 1:
                 item['city'] = parts[0]
                 item['region'] = parts[-1]
            elif parts:
                 item['city'] = parts[0]
                 item['region'] = parts[0]

        # Адрес
        address_parts = response.xpath('//table[@class="object_info"]//td[text()="Адрес"]/following-sibling::td//text()').getall()
        item['address'] = None 

        if address_parts:
            full_address = ' '.join([p.strip() for p in address_parts if p.strip()]).strip()
            full_address = re.sub(r'\s+', ' ', full_address).strip()
            full_address = re.sub(r'\s*,', ',', full_address)
            full_address = re.sub(r',+', ',', full_address)
            full_address = full_address.strip(', ')
            item['address'] = full_address or None 

        # Описание
        desc_block = response.xpath('//div[contains(@class, "object-infoblock")]/div[contains(@class, "descr")]')
        item['description'] = None 

        if desc_block:
            desc_paragraphs = desc_block.xpath('./p/text()').getall()
            if desc_paragraphs:
                 desc_text = ' '.join([p.strip() for p in desc_paragraphs if p.strip()]).strip()
                 item['description'] = re.sub(r'\s+', ' ', desc_text).strip() or None
            else:
                 desc_text = ' '.join(desc_block.xpath('.//text()').getall()).strip()
                 item['description'] = re.sub(r'\s+', ' ', desc_text).strip() or None

        item['advt_type'] = advt_type_id
        item['cat'] = cat_id

        # Координаты 
        lat_text = response.xpath('//div[@id="objectMap"]/@data-lat').get()
        lon_text = response.xpath('//div[@id="objectMap"]/@data-lng').get()
        try:
            item['lat'] = float(lat_text) if lat_text else None
            item['lon'] = float(lon_text) if lon_text else None
        except (ValueError, TypeError):
             self.logger.warning(f"координаты ({lat_text}, {lon_text}) не число для {response.url}")
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

            if key == 'Адрес':
                continue 

            v_parts = v_td.xpath(".//text()").getall()
            value = " ".join([_.strip() for _ in v_parts if _.strip()]).strip()
            value = re.sub(r'\s*–.*', '', value).strip() 
            value = re.sub(r'\s+', ' ', value).strip()   

            params_data[key] = value or None 


        item['params'] = json.dumps(params_data, ensure_ascii=False)

        # Дата публикации (parse_date_string возвращает None, если не распознает)
        date_text = response.xpath(
            '//div[contains(@class, "object-header")]//span[contains(@style, "font-size")]/text() | '
            '//div[contains(@class, "object-header")]//span[contains(@class, "text-muted")]/text()'
        ).get()

        item['date_posted'] = parse_date_string(date_text, logger=self.logger)

        #   инкрементируем общие счетчики и счетчики по регионам/городам
        self.crawler.stats.inc_value('total_items_scraped')
        if item['region']:
            # используем item['region'] для более точной статистики по регионам, чем domain
            self.crawler.stats.inc_value(f'items_scraped_by_region_name/{item["region"]}')
        if item['city']:
            self.crawler.stats.inc_value(f'items_scraped_by_city/{item["city"]}')
        
        yield item

        # Сбор изображений
        image_urls_list = response.xpath('//div[contains(@class, "fotorama")]/a/@href').getall()
        if not image_urls_list:
             self.logger.debug(f"нет картинок для {item['id']}")

        for img_url_raw in image_urls_list:
            img_item = ImageItem()
            img_item['advt_id'] = item['id']
            img_item['url'] = response.urljoin(img_url_raw)
            img_item['date_update'] = datetime.datetime.now()
            yield img_item

        # Сбор телефонов
        if phones_set:
             for phone_digits in phones_set:
                 phone_item = PhoneItem()
                 phone_item['advt_id'] = item['id']
                 try:
                      phone_item['phone'] = int(phone_digits)
                 except ValueError:
                      self.logger.warning(f"телефон '{phone_digits}' не int для {response.url}")
                      continue
                 phone_item['is_fake'] = False
                 phone_item['date_update'] = datetime.datetime.now()
                 yield phone_item
        else:
             self.logger.debug(f"телефонов не найдено для {item['id']}")