# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy

class TrabajandoItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    pass


def select_data(value):
    if isinstance(value, tuple) and len(value) > 0:
        return value[0]
    return value

class JobItem(scrapy.Item):
   
    data_id = scrapy.Field(serializer=select_data) 
    url = scrapy.Field(serializer=select_data)
    title = scrapy.Field(serializer=select_data)
    company = scrapy.Field(serializer=select_data)
    location = scrapy.Field(serializer=select_data) 
    type_job = scrapy.Field(serializer=select_data)
    date_published = scrapy.Field(serializer=select_data) 
    job_description = scrapy.Field(serializer=select_data)
    date_expiration = scrapy.Field(serializer=select_data)
    date_saved = scrapy.Field(serializer=select_data)

    def __getitem__(self, key):
        """
        Override __getitem__ to apply serializers when accessing fields
        This ensures serializers are applied during item access
        """

        value = super(JobItem, self).__getitem__(key)
        field = self.fields[key]

        if 'serializer' in field:
            return field['serializer'](value)

        return value

