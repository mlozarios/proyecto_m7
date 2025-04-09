from .base_pipeline import TrabajandoPipeline
from datetime import datetime
import re 
from urllib.parse import urlparse
from dateutil import parser
import os 
import psycopg2
from dotenv import load_dotenv
import json
from scrapy.utils.project import get_project_settings


class ConsumptionPipeline(TrabajandoPipeline):

    def __init__(self) -> None:
            # information Connection with DB 
            hostname = os.getenv('DB_HOST') 
            username = os.getenv('DB_USER') 
            password = os.getenv('DB_PASSWORD') 
            database = os.getenv('DB_DATABASE') 
            
            self.connection = psycopg2.connect(host=hostname, user=username, password=password, dbname=database)

            self.cur = self.connection.cursor()

            self.cur.execute("""
                             CREATE TABLE IF NOT EXISTS job_data_consumption (
                             id serial PRIMARY KEY,
                             url text,
                             title text,
                             company text,
                             location text,
                             type_job text,
                             job_description text,
                             date_published text,
                             date_expiration text, 
                             date_saved_iso text
                             )
                             """
                             )

            self.connection.commit()

    def open_spider(self, spider):
        self.items = []
        settings = get_project_settings()
        self.consumption_zone = settings.get('CONSUMPTION_ZONE')
        self.filename = self.get_filename(spider, None, 'consumption') 



    def process_item(self, item, spider):
        
        settings = get_project_settings()
        landing_zone = settings.get('CONSUMPTION_ZONE')

        filename = self.get_filename(spider, item, 'consumption')

        transformed_item = self.transform_item(item)

        self.save_to_zone(item, landing_zone, filename)
        
        return  transformed_item


    def transform_item(self, item):

        """ Here we can perform all the transformation or even separate it into another file 
         for readability puroposes."""

        # Convert the item to a dictionary first
        transformed = dict(item)
        


        self.cur.execute("""
                         SELECT * FROM job_data_consumption
                         WHERE url = %s""", (transformed['url'],))
        

        res = self.cur.fetchone()

        if res:
            print(f"THis item: {transformed['url']} is already in the DB.")
            raise Exception(f"The item is already in the DB.")

        else:

            self.cur.execute("""
                             INSERT INTO job_data_consumption (url, title, company, location, type_job, job_description, date_published, date_expiration, date_saved_iso)
                             VALUES (%s,%s,%s,%s,%s,%s,%s,%s, %s)""",(
                                transformed['url'], 
                                transformed['title'], 
                                transformed['company'], 
                                transformed['location'], 
                                transformed['type_job'], 
                                transformed['job_description'], 
                                transformed['date_published'], 
                                transformed['date_expiration'],
                                transformed['date_saved_iso']
                                 )
                             )

            self.connection.commit()


        return transformed


    def close_connection(self, spider):
        self.cur.close()
        self.connection()

    def close_spider(self, spider):
        os.makedirs(self.landing_zone, exist_ok=True)
        path = os.path.join(self.landing_zone, self.filename)

        # save all items to one JSON file
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(self.items, f, ensure_ascii=False, indent=2)

