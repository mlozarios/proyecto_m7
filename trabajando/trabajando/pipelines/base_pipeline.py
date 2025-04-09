# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
from scrapy.utils.project import get_project_settings
from datetime import datetime 
import os 
import json

class TrabajandoPipeline:
    def __init__(self) -> None:
        self.settings = get_project_settings()

    def get_filename(self, spider, item, zone):
        timestamp = datetime.now().strftime('%Y%m%d%H%M%S')

        return f"{spider.name}{timestamp}.json"
    
    def save_to_zone(self, item, zone_path, filename):
        
        os.makedirs(zone_path, exist_ok=True)

        file_path = os.path.join(zone_path, filename)
        with open(file_path, 'w') as f:
            json.dump(dict(item), f)
        
        print(f"{filename} saved in {zone_path}")
        return file_path

    def process_item(self, item, spider):

        zone_path = self.settings.get("LANDING_ZONE", 'datalake/landing')

        filename = self.get_filename(spider, item, 'landing')

        self.save_to_zone(item, zone_path, filename)

        return item

