import scrapy

# Переименовано obyavlenie в AdvertItem
class AdvertItem(scrapy.Item):
    id = scrapy.Field()
    url = scrapy.Field()
    title = scrapy.Field()
    price = scrapy.Field()
    date_update = scrapy.Field()
    is_company = scrapy.Field()
    contactname = scrapy.Field()
    company = scrapy.Field()
    region = scrapy.Field()
    city = scrapy.Field()
    address = scrapy.Field()
    description = scrapy.Field()
    advt_type = scrapy.Field()
    source = scrapy.Field()
    cat = scrapy.Field()
    lat = scrapy.Field()
    lon = scrapy.Field()
    params = scrapy.Field()
    date_posted = scrapy.Field()
    is_active = scrapy.Field()

# Переименовано image в ImageItem
class ImageItem(scrapy.Item):
    advt_id = scrapy.Field()
    url = scrapy.Field()
    date_update = scrapy.Field()

# Переименовано phone в PhoneItem
class PhoneItem(scrapy.Item):
    advt_id = scrapy.Field()
    phone = scrapy.Field()
    is_fake = scrapy.Field()
    date_update = scrapy.Field()