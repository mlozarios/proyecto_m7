from .base_pipeline import TrabajandoPipeline
import os
import json

class LandingPipeline(TrabajandoPipeline):

    def open_spider(self, spider):
        self.items = []
        self.landing_zone = self.settings.get('LANDING_ZONE')
        self.filename = self.get_filename(spider, None, 'landing') 

    def process_item(self, item, spider):
        self.items.append(dict(item)) 
        return item 

    def close_spider(self, spider):
        os.makedirs(self.landing_zone, exist_ok=True)
        path = os.path.join(self.landing_zone, self.filename)

        # save all items to one JSON file
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(self.items, f, ensure_ascii=False, indent=2)
