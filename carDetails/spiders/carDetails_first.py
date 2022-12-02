import datetime
import json
import pymongo
import scrapy
from scrapy.cmdline import execute



class CardetailsFirstSpider(scrapy.Spider):
    name = 'carDetails_first'

    def start_requests(self):
        url = "https://soa.audi.co.uk/pdb-webservices/services/rest/pdb/carlinegroups?financeable=true&showCarlines=true"
        # url = "https://soa.audi.co.uk/pdb-webservices/services/rest/pdb/carlinegroups/A3/carlines/A3%20Sportback/trimlines?financeable=true&showDerivatives=true&sortBy=otrPriceMinWithMetallicPaint"

        yield scrapy.Request(url, callback=self.parse)

    def parse(self, response, **k):
        port = '27017'
        host = 'localhost'
        con = pymongo.MongoClient(f'mongodb://{host}:{port}/')
        db_name = 'car_details'
        table = f'models_{datetime.date.today().strftime("%d_%m_%Y")}'

        mydb = con[db_name]

        conn = mydb[table]
        a = response.text
        print()

        json_data = json.loads(response.text)

        for info in json_data['result']:
            item = dict()
            model = info['displayName']
            feedName_main = info['feedName']
            rangeOrder = info['rangeOrder']
            otrPriceMinWithMetallicPaint_main = info['otrPriceMinWithMetallicPaint']

            for more_Data in info['carlines']:
                displayName = more_Data['displayName']
                feedName = more_Data['feedName']
                otrPriceMinWithMetallicPaint = more_Data['otrPriceMinWithMetallicPaint']

                item['model'] =model
                item['feedName_main'] =feedName_main
                item['rangeOrder'] =rangeOrder
                item['otrPriceMinWithMetallicPaint_main'] =otrPriceMinWithMetallicPaint_main
                item['displayName'] =displayName
                item['feedName'] =feedName
                item['otrPriceMinWithMetallicPaint'] =otrPriceMinWithMetallicPaint

                conn.insert_one(dict(item))
                print("insert", conn)


if __name__ == '__main__':
    execute("scrapy crawl carDetails_first".split())