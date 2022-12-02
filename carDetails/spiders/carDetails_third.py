import json
import datetime
import pymongo
import scrapy
from scrapy.cmdline import execute
from urllib.parse import unquote, quote


class CardetailsThirdSpider(scrapy.Spider):
    name = 'carDetails_third'

    def __init__(self):
        port = '27017'
        host = 'localhost'
        con = pymongo.MongoClient(f'mongodb://{host}:{port}/')

        conn = con['car_details']
        self.table = conn[f'engine_data_{datetime.date.today().strftime("%d_%m_%Y")}']
        self.table2 = conn[f'month_miles_{datetime.date.today().strftime("%d_%m_%Y")}']
        self.insert_table = conn[f'calculation_data_{datetime.date.today().strftime("%d_%m_%Y")}']

    def start_requests(self):

        res1 = self.table2.find({'status': 'pending'})

        for info in res1:
            months = info['months']
            miles = info['miles']
            results = self.table.find({'status': 'pending'})

            for data in results:
                engineTransmissionCombined = data['engineTransmissionCombined']
                financeable = data['financeable']
                modelCode = data['modelCode']
                model_encode = quote(modelCode)
                displayEngineName = data['displayEngineName']
                otrPriceMinWithMetallicPaint = data['otrPriceMinWithMetallicPaint']
                transmission = data['transmission']
                trim_one = data['trim_one']
                trim_two = data['trim_two']
                otrPriceMinWithMetallicPaint_two = data['otrPriceMinWithMetallicPaint_two']
                model = data['model']
                otrPriceMinWithMetallicPaint_second = data['otrPriceMinWithMetallicPaint_second']
                feedName_main = data['feedName_main']
                rangeOrder = data['rangeOrder']
                otrPriceMinWithMetallicPaint_main = data['otrPriceMinWithMetallicPaint_main']
                displayName = data['displayName']
                feedName = data['feedName']

                url = f'https://soa.audi.co.uk/pdb-webservices/services/rest/finance/v2/quote/json?modelCode={model_encode}&deposit=4500.00&mileage{miles}&period={months}&noCache=0.027523934683353124&productType=SOLUTIONS&withMetallicPaint=true'

                yield scrapy.Request(url=url, callback=self.parse, meta={
                    'engineTransmissionCombined': engineTransmissionCombined,
                    'financeable': financeable,
                    'modelCode': modelCode,
                    'displayEngineName': displayEngineName,
                    'otrPriceMinWithMetallicPaint': otrPriceMinWithMetallicPaint,
                    'transmission': transmission,
                    'trim_one': trim_one,
                    'trim_two': trim_two,
                    'otrPriceMinWithMetallicPaint_two': otrPriceMinWithMetallicPaint_two,
                    'model': model,
                    'otrPriceMinWithMetallicPaint_second': otrPriceMinWithMetallicPaint_second,
                    'feedName_main': feedName_main,
                    'rangeOrder': rangeOrder,
                    'otrPriceMinWithMetallicPaint_main': otrPriceMinWithMetallicPaint_main,
                    'displayName': displayName,
                    'feedName': feedName,
                    'months': months,
                    'miles': miles
                })

    def parse(self, response, **k):
        a = response.text
        print()
        item = dict()
        json_data = json.loads(response.text)

        # port = '27017'
        # host = 'localhost'
        # con = pymongo.MongoClient(f'mongodb://{host}:{port}/')
        # db_name = 'car_details'
        # table = 'calculation_data'
        # table2 = 'engine_data'
        # table3 = 'month_miles'
        # mydb = con[db_name]
        # conn = mydb[table]
        # conn2 = mydb[table2]
        # conn3 = mydb[table3]

        months = response.meta['months']
        miles = response.meta['miles']
        engineTransmissionCombined = response.meta['engineTransmissionCombined']
        financeable = response.meta['financeable']
        modelCode = response.meta['modelCode']
        displayEngineName = response.meta['displayEngineName']
        otrPriceMinWithMetallicPaint = response.meta['otrPriceMinWithMetallicPaint']
        transmission = response.meta['transmission']
        trim_one = response.meta['trim_one']
        trim_two = response.meta['trim_two']
        otrPriceMinWithMetallicPaint_two = response.meta['otrPriceMinWithMetallicPaint_two']
        model = response.meta['model']
        otrPriceMinWithMetallicPaint_second = response.meta['otrPriceMinWithMetallicPaint_second']
        feedName_main = response.meta['feedName_main']
        rangeOrder = response.meta['rangeOrder']
        otrPriceMinWithMetallicPaint_main = response.meta['otrPriceMinWithMetallicPaint_main']
        displayName = response.meta['displayName']
        feedName = response.meta['feedName']
        try:
            depositContribution = json_data['data']['quote']['depositContribution']
        except:
            depositContribution = ''
        try:
            retailerCashPrice = json_data['data']['quote']['retailerCashPrice']
        except:
            retailerCashPrice = ''
        try:
            initialPayment = json_data['data']['quote']['initialPayment']
        except:
            initialPayment = ''
        try:
            initialPaymentText = json_data['data']['quote']['initialPaymentText']
        except:
            initialPaymentText = ''
        try:
            apr = json_data['data']['quote']['apr']
        except:
            apr = ''
        try:
            customerRate = json_data['data']['quote']['customerRate']
        except:
            customerRate = ''
        try:
            acceptanceFee = json_data['data']['quote']['acceptanceFee']
        except:
            acceptanceFee = ''
        try:
            isBalancedFee = json_data['data']['quote']['isBalancedFee']
        except:
            isBalancedFee = ''
        try:
            purchaseOptionFee = json_data['data']['quote']['purchaseOptionFee']
        except:
            purchaseOptionFee = ''
        try:
            documentationFee = json_data['data']['quote']['documentationFee']
        except:
            documentationFee = ''
        try:
            monthlyRental = json_data['data']['quote']['monthlyRental']
        except:
            monthlyRental = ''
        try:
            finalPayment = json_data['data']['quote']['finalPayment']
        except:
            finalPayment = ''
        try:
            financeCommission = json_data['data']['quote']['financeCommission']
        except:
            financeCommission = ''
        try:
            manufacturerSubsidy = json_data['data']['quote']['manufacturerSubsidy']
        except:
            manufacturerSubsidy = ''
        try:
            brandSubsidy = json_data['data']['quote']['brandSubsidy']
        except:
            brandSubsidy = ''
        try:
            amountFinanced = json_data['data']['quote']['amountFinanced']
        except:
            amountFinanced = ''
        try:
            excessMileage = json_data['data']['quote']['excessMileage']
        except:
            excessMileage = ''
        try:
            totalAmountPayable = json_data['data']['quote']['totalAmountPayable']
        except:
            totalAmountPayable = ''
        try:
            vehicleFinancePayment = json_data['data']['quote']['vehicleFinancePayment']
        except:
            vehicleFinancePayment = ''
        try:
            fixedCostMaintenanceCost = json_data['data']['quote']['fixedCostMaintenanceCost']
        except:
            fixedCostMaintenanceCost = ''
        try:
            fixedCostMaintenanceInitialPayment = json_data['data']['quote']['fixedCostMaintenanceInitialPayment']
        except:
            fixedCostMaintenanceInitialPayment = ''
        try:
            aerPayment = json_data['data']['quote']['aerPayment']
        except:
            aerPayment = ''
        try:
            monthlyPayment = json_data['data']['quote']['monthlyPayment']
        except:
            monthlyPayment = ''
        try:
            monthlyPaymentText = json_data['data']['quote']['monthlyPaymentText']
        except:
            monthlyPaymentText = ''
        try:
            lastPayment = json_data['data']['quote']['lastPayment']
        except:
            lastPayment = ''
        try:
            lastPaymentText = json_data['data']['quote']['lastPaymentText']
        except:
            lastPaymentText = ''
        try:
            isOffer = json_data['data']['quote']['isOffer']
        except:
            isOffer = ''
        try:
            legalText = json_data['data']['quote']['legalText']
        except:
            legalText = ''
        try:
            metallicPaintPrice = json_data['data']['quote']['metallicPaintPrice']
        except:
            metallicPaintPrice = ''
        try:
            productLabel = json_data['data']['quote']['productLabel']
        except:
            productLabel = ''
        try:
            balancedFee = json_data['data']['quote']['balancedFee']
        except:
            balancedFee = ''
        try:
            offer = json_data['data']['quote']['offer']
        except:
            offer = ''

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
        item['depositContribution'] = depositContribution
        item['retailerCashPrice'] = retailerCashPrice
        item['initialPayment'] = initialPayment
        item['initialPaymentText'] = initialPaymentText
        item['apr'] = apr
        item['customerRate'] = customerRate
        item['acceptanceFee'] = acceptanceFee
        item['isBalancedFee'] = isBalancedFee
        item['purchaseOptionFee'] = purchaseOptionFee
        item['documentationFee'] = documentationFee
        item['monthlyRental'] = monthlyRental
        item['finalPayment'] = finalPayment
        item['financeCommission'] = financeCommission
        item['manufacturerSubsidy'] = manufacturerSubsidy
        item['brandSubsidy'] = brandSubsidy
        item['amountFinanced'] = amountFinanced
        item['excessMileage'] = excessMileage
        item['totalAmountPayable'] = totalAmountPayable
        item['vehicleFinancePayment'] = vehicleFinancePayment
        item['fixedCostMaintenanceCost'] = fixedCostMaintenanceCost
        item['fixedCostMaintenanceInitialPayment'] = fixedCostMaintenanceInitialPayment
        item['aerPayment'] = aerPayment
        item['monthlyPayment'] = monthlyPayment
        item['monthlyPaymentText'] = monthlyPaymentText
        item['lastPayment'] = lastPayment
        item['lastPaymentText'] = lastPaymentText
        item['isOffer'] = isOffer
        item['legalText'] = legalText
        item['metallicPaintPrice'] = metallicPaintPrice
        item['productLabel'] = productLabel
        item['balancedFee'] = balancedFee
        item['offer'] = offer
        item['months'] = months
        item['miles'] = miles

        try:
            self.conn.insert_one(dict(item))
            print("insert", self.conn)

            # # conn.update_one({{'feedName_main': feedName_main,'displayName': displayName}, {'$set': {'status': 'done2'}}})
            # conn2.update_one({'modelCode': modelCode}, {'$set': {
            #     'status': 'done'
            # }})
            #
            # conn3.update_one({'months':months, 'miles':miles}, {'$set': {
            #     'status': 'done'
            # }})
            # print("update", table)

        except Exception as E:
            print(E)


if __name__ == '__main__':
    execute("scrapy crawl carDetails_third".split())
''' "depositContribution": "2500.00",
    "retailerCashPrice": "30150.00",
    "initialPayment": "0.00",
    "initialPaymentText": "1 payment of",
    "apr": "11.50",
    "customerRate": "5.90",
    "acceptanceFee": "0.00",
    "isBalancedFee": false,
    "purchaseOptionFee": "10.00",
    "documentationFee": "540.00",
    "monthlyRental": "489.99",
    "finalPayment": "16197.50",
    "financeCommission": "540.00",
    "manufacturerSubsidy": "0.00",
    "brandSubsidy": "0.00",
    "amountFinanced": "23150.00",
    "excessMileage": "6.98",
    "totalAmountPayable": "33727.27",
    "vehicleFinancePayment": "489.99",
    "fixedCostMaintenanceCost": 0,
    "fixedCostMaintenanceInitialPayment": "0.00",
    "aerPayment": "11.51",
    "monthlyPayment": "489.99",
    "monthlyPaymentText": "23 payments of",
    "lastPayment": "16207.50",
    "lastPaymentText": "1 payment of",
    "isOffer": true,
    "legalText": "'
    "metallicPaintPrice": "575.00",
    "productLabel": "A5U73 A3 Saloon £2.5K DC 11.4% PCP Q4.2 22",
    "balancedFee": false,
    "offer": true '''
