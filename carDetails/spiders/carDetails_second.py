import datetime
import json

import pymongo
import scrapy
from scrapy.cmdline import execute


class CardetailsSecondSpider(scrapy.Spider):
    name = 'carDetails_second'

    def __init__(self, **kwargs):

        port = '27017'
        host = 'localhost'
        con = pymongo.MongoClient(f'mongodb://{host}:{port}/')

        self.conn = con['car_details']
        self.table = self.conn[f'models_{datetime.date.today().strftime("%d_%m_%Y")}']
        self.table_two = self.conn[f'engine_data_{datetime.date.today().strftime("%d_%m_%Y")}']

    def start_requests(self):

        results = self.table.find({'status': None})

        for data in results:
            model = data['model']
            feedName_main = data['feedName_main']
            rangeOrder = data['rangeOrder']
            otrPriceMinWithMetallicPaint_main = data['otrPriceMinWithMetallicPaint_main']
            displayName = data['displayName']
            feedName = data['feedName']
            otrPriceMinWithMetallicPaint = data['otrPriceMinWithMetallicPaint']
            print()

            url = f'https://soa.audi.co.uk/pdb-webservices/services/rest/pdb/carlinegroups/{feedName_main}/carlines/{displayName}/trimlines?financeable=true&showDerivatives=true&sortBy=otrPriceMinWithMetallicPaint'

            yield scrapy.Request(url, callback=self.parse, meta={
                'model': model,
                'feedName_main': feedName_main,
                'rangeOrder': rangeOrder,
                'otrPriceMinWithMetallicPaint_main': otrPriceMinWithMetallicPaint_main,
                'displayName': displayName,
                'feedName': feedName,
                'otrPriceMinWithMetallicPaint': otrPriceMinWithMetallicPaint

            })

    def parse(self, response):
        # a = response.text
        # print()

        # port = '27017'
        # host = 'localhost'
        # con = pymongo.MongoClient(f'mongodb://{host}:{port}/')
        # db_name = 'car_details'
        # table = 'engine_data'
        # table2 = 'models_'
        # mydb = con[db_name]
        # conn = mydb[table]
        # conn2 = mydb[table2]

        model = response.meta['model']
        feedName_main = response.meta['feedName_main']
        rangeOrder = response.meta['rangeOrder']
        otrPriceMinWithMetallicPaint_main = response.meta['otrPriceMinWithMetallicPaint_main']
        displayName = response.meta['displayName']
        feedName = response.meta['feedName']
        otrPriceMinWithMetallicPaint_second = response.meta['otrPriceMinWithMetallicPaint']

        json_data = json.loads(response.text)

        for data in json_data['result']:
            item = dict()
            try:
                trim_one = data['displayName']
            except:
                trim_one = ''
            try:
                trim_two = data['feedName']
            except:
                trim_two = ''
            try:
                otrPriceMinWithMetallicPaint_two = data['otrPriceMinWithMetallicPaint']
            except:
                otrPriceMinWithMetallicPaint_two = ''

            for more_Data in data['derivatives']:
                try:
                    engineTransmissionCombined = more_Data['engineTransmissionCombined']
                except:
                    engineTransmissionCombined = ''
                try:
                    financeable = more_Data['financeable']
                except:
                    financeable = ''
                try:
                    modelCode = more_Data['modelCode']
                except:
                    modelCode = ''
                try:
                    displayEngineName = more_Data['displayEngineName']
                except:
                    displayEngineName = ''
                try:
                    otrPriceMinWithMetallicPaint = more_Data['otrPriceMinWithMetallicPaint']
                except:
                    otrPriceMinWithMetallicPaint = ''
                try:
                    transmission = more_Data['transmission']
                except:
                    transmission = ''

                item['engineTransmissionCombined'] = engineTransmissionCombined
                item['financeable'] = financeable
                item['modelCode'] = modelCode
                item['displayEngineName'] = displayEngineName
                item['otrPriceMinWithMetallicPaint'] = otrPriceMinWithMetallicPaint
                item['transmission'] = transmission
                item['trim_one'] = trim_one
                item['trim_two'] = trim_two
                item['otrPriceMinWithMetallicPaint_two'] = otrPriceMinWithMetallicPaint_two
                item['model'] = model
                item['otrPriceMinWithMetallicPaint_second'] = otrPriceMinWithMetallicPaint_second
                item['feedName_main'] = feedName_main
                item['rangeOrder'] = rangeOrder
                item['otrPriceMinWithMetallicPaint_main'] = otrPriceMinWithMetallicPaint_main
                item['displayName'] = displayName
                item['feedName'] = feedName

                try:
                    self.table_two.insert_one(dict(item))
                    print("insert", self.conn)

                except Exception as E:
                    print(E)

            self.table.update_one({'feedName_main': feedName_main, 'displayName': displayName}, {'$set': {
                'status': 'done'
            }})
            print("update", self.table)


if __name__ == '__main__':
    execute("scrapy crawl carDetails_second".split())
