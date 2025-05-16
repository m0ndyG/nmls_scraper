BOT_NAME = 'nmls_scraper'
SPIDER_MODULES = ['nmls_scraper.spiders']
NEWSPIDER_MODULE = 'nmls_scraper.spiders'
USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36' 
ROBOTSTXT_OBEY = True # проверять согласно robots.txt
DOWNLOAD_DELAY = 1 
CLOSESPIDER_ITEMCOUNT = 1000  # количество объявлений 
ITEM_PIPELINES = {
   'nmls_scraper.pipelines.NmlsScraperPipeline': 300,
}
AUTOTHROTTLE_ENABLED = True
AUTOTHROTTLE_START_DELAY = 1
AUTOTHROTTLE_MAX_DELAY = 60

# настройки для выбора конкретного региона
SPECIFIC_REGION = True # <-- Установите True, чтобы парсить только один регион
SPECIFIC_REGION_SUBDOMAIN = 'nn' # <-- Укажите поддомен региона (например, 'nn' для Нижнего Новгорода). Используется только если SPECIFIC_REGION = True

DB_SETTINGS = {
    'database': 'tdata',
    'user': 'postgres',
    'password': 'postgres',
    'host': 'localhost', 
    'port': '5432'      
}