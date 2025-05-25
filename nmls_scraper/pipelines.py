import psycopg2
import logging
from scrapy.exceptions import DropItem
from nmls_scraper.items import AdvertItem, ImageItem, PhoneItem

class NmlsScraperPipeline:

    schema_name = 'data'
    advt_table = 'advt'
    images_table = 'images'
    phones_table = 'phones'

    def __init__(self, db_settings):
        self.db_settings = db_settings
        self.connection = None
        self.cursor = None

    @classmethod
    def from_crawler(cls, crawler):
        db_settings = crawler.settings.getdict('DB_SETTINGS')
        if not db_settings:
             raise ValueError("DB_SETTINGS is not configured in settings.py")
        return cls(db_settings)

    def open_spider(self, spider):
        try:
            self.connection = psycopg2.connect(**self.db_settings)
            self.cursor = self.connection.cursor()

            self.cursor.execute(f'SET search_path TO {self.schema_name}, public;')
            self.connection.commit()
            logging.info(f"Успешно подключено к базе данных. Установлен search_path на '{self.schema_name}'.")
        except psycopg2.Error as e:
            logging.error(f"Ошибка подключения к базе данных: {e}")

            raise DropItem(f"Ошибка подключения к БД: {e}")


    def close_spider(self, spider):
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
            logging.info("Соединение с базой данных закрыто.")

    def process_item(self, item, spider):
        if not self.connection or not self.cursor:
             logging.error("Отсутствует подключение к базе данных, Item не будет сохранен.")
             return item

        try:
            if isinstance(item, AdvertItem):
                self.insert_or_update_advt(item)
            elif isinstance(item, ImageItem):
                self.insert_image(item)
            elif isinstance(item, PhoneItem):
                self.insert_phone_number(item)
            else:
                logging.warning(f"Неизвестный тип Item: {type(item).__name__}")

        except psycopg2.Error as e:

             if self.connection:
                 self.connection.rollback()
             logging.error(f"Ошибка БД при сохранении Item типа {type(item).__name__} ({item.get('id') or item.get('advt_id')}): {e}", exc_info=True)


        except Exception as e:

             if self.connection:
                 self.connection.rollback()
             logging.error(f"Неожиданная ошибка при обработке Item типа {type(item).__name__}: {e}", exc_info=True)


        return item

    def insert_or_update_advt(self, item):
        sql = f"""
        INSERT INTO {self.advt_table} (id, url, title, price, date_update, is_company, contactname, company, region, city, address, description, advt_type, source, cat, lat, lon, params, date_posted, is_active)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (id)
        DO UPDATE SET
            title = EXCLUDED.title,
            price = EXCLUDED.price,
            date_update = EXCLUDED.date_update,
            is_company = EXCLUDED.is_company,
            contactname = EXCLUDED.contactname,
            company = EXCLUDED.company,
            region = EXCLUDED.region,
            city = EXCLUDED.city,
            address = EXCLUDED.address,
            description = EXCLUDED.description,
            advt_type = EXCLUDED.advt_type,
            source = EXCLUDED.source,
            cat = EXCLUDED.cat,
            lat = EXCLUDED.lat,
            lon = EXCLUDED.lon,
            params = EXCLUDED.params,
            date_posted = EXCLUDED.date_posted,
            is_active = EXCLUDED.is_active;
        """
        self.cursor.execute(sql, (
            item.get('id'), item.get('url'), item.get('title'), item.get('price'), item.get('date_update'),
            item.get('is_company'), item.get('contactname'), item.get('company'), item.get('region'),
            item.get('city'), item.get('address'), item.get('description'), item.get('advt_type'),
            item.get('source'), item.get('cat'), item.get('lat'), item.get('lon'),
            item.get('params'),
            item.get('date_posted'), item.get('is_active'),
        ))
        self.connection.commit()


    def insert_image(self, item):

        sql = f"""
        INSERT INTO {self.images_table} (advt_id, url, date_update)
        VALUES (%s, %s, %s)
        ON CONFLICT (advt_id, url) -- Предполагаем уникальность пары объявление+URL изображения
        DO NOTHING; -- Если уже есть, ничего не делаем
        """
        self.cursor.execute(sql, (
            item.get('advt_id'), item.get('url'), item.get('date_update'),
        ))
        self.connection.commit()


    def insert_phone_number(self, item):
        sql = f"""
        INSERT INTO {self.phones_table} (advt_id, phone, is_fake, date_update)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (advt_id, phone) -- Предполагаем уникальность пары объявление+телефон
        DO NOTHING; -- Если уже есть, ничего не делаем
        """
        phone_value = item.get('phone')

        self.cursor.execute(sql, (
            item.get('advt_id'), phone_value, item.get('is_fake'), item.get('date_update'),
        ))
        self.connection.commit()