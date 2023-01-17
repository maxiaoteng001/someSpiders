from lxml import etree
import scrapy
import re
import urllib
import json
import datetime
import sys
sys.path.append('..')
from utils import *


class TeslaSpider(scrapy.Spider):
    """
    特斯拉官方二手车
    """
    name = 'tesla_used'

    def __init__(self, name=None, **kwargs):
        self.client = MysqlHelper(dbconfig=dbconfig)
        super().__init__(name, **kwargs)

    def start_requests(self):
        models = ['m3', 'mx', 'my', 'ms']
        for model in models:
            yield self.make_request_from_data({'model': model})

    def make_request_from_data(self, data):
        data['retry_times'] = data.get('retry_times', 0) + 1
        if data.get('retry_times', 0) > 20:
            self.logger.info(f'failure_request: {json.dumps(data)}')
            return
        headers = {
            'Host': 'www.tesla.cn',
            'Cookie': 'AKA_A2=A',
            'user-agent': 'Mozilla/5.0 (Linux; Android 13; Pixel 4 XL Build/TP1A.221005.002; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/86.0.4240.99 XWEB/4317 MMWEBSDK/20221011 Mobile Safari/537.36 MMWEBID/411 MicroMessenger/8.0.30.2244(0x28001E44) WeChat/arm64 Weixin GPVersion/1 NetType/WIFI Language/zh_CN ABI/arm64 miniProgram/wx645b13cb3307661f',
            'accept': '*/*',
            'x-requested-with': 'com.tencent.mm',
            'sec-fetch-site': 'same-origin',
            'sec-fetch-mode': 'cors',
            'sec-fetch-dest': 'empty',
            'accept-language': 'zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7',
            'pragma': 'no-cache',
            'cache-control': 'no-cache',
        }
        query = {
            'query': {
                'model': data.get('model'),
                # 二手车 
                'condition': 'used', 
                'options': {'FleetSalesRegions': ['CN']}, 
                'arrangeby': 'Price', 
                'order': 'asc', 
                'market': 'CN',
                'language': 'zh', 
                'super_region': 
                'north america', 
                'lng': '', 
                'lat': '', 
                'zip': '', 
                'range': 0
            }, 
            'offset': data.get('offset', 0), 
            'count': 50, 
            'outsideOffset': 0, 
            'outsideSearch': False
        }
        
        query_string = {
            'query': json.dumps(query, separators=(',', ':'), ensure_ascii=False),
        }

        url = 'https://www.tesla.cn/inventory/api/v1/inventory-results?{}'.format(
            urllib.parse.urlencode(query_string))
        return scrapy.Request(url,
                              headers=headers,
                              callback=self.parse,
                              errback=self.errback_httpbin,
                              dont_filter=True,
                              meta=data
                              )

    def parse(self, response):
        res_data = json.loads(response.text)
        results = res_data.get('results')
        items = []
        drop_keys = ['OptionCodePricing']
        replace_keys = ['Language', 'TRIM', 'Year']
        item_keys = ['InTransit', 'ADL_OPTS', 'AUTOPILOT', 'AcquisitionSubType', 'AcquisitionType', 'ActualGAInDate', 'ActualVesselArrivalDate', 'AlternateCurrency', 'BATTERY', 'CABIN_CONFIG', 'CPORefurbishmentStatus', 'CPOWarrantyType', 'City', 'CompositorViews', 'CountryCode', 'CountryCodes', 'CountryOfOrigin', 'CurrencyCode', 'CurrencyCodes', 'CustomsClearance', 'DECOR', 'DRIVE', 'DamageDisclosure', 'DamageDisclosureGuids', 'DamageDisclosureStatus', 'DestinationHandlingFee', 'DisplayWarranty', 'EtaToCurrent', 'EtaToDelivery', 'ExpectedVesselArrivalDate', 'FactoryDepartureDate', 'FactoryGatedDate', 'FixedAssets', 'FleetAssignedLocationId', 'FleetSalesRegions', 'FlexibleOptionsData', 'ForecastedFactoryGatedDate', 'HEADLINER', 'HasDamagePhotos', 'HasOptionCodeData', 'INTERIOR', 'IncentivesDetails', 'InspectionDocumentGuid', 'InventoryPrice', 'IsChargingConnectorIncluded', 'IsDemo', 'IsLegacy', 'IsPreProdWithDisclaimer', 'IsTegra', 'Language', 'Languages', 'LeaseAvailabilityDate', 'LexiconDefaultOptions', 'ListingType', 'ListingTypes', 'ManufacturingOptionCodeList', 'MarketingInUseDate', 'MetroName', 'Model', 'Odometer', 'OdometerType', 'OnConfiguratorPricePercentage', 'OptionCodeData', 'OptionCodeList', 'OptionCodeListDisplayOnly', 'OptionCodePricing', 'OrderFee', 'OriginalDeliveryDate', 'OriginalInCustomerGarageDate', 'PAINT', 'PlannedGADailyDate', 'Price', 'PurchasePrice', 'ROOF', 'RegistrationDetails', 'SalesMetro', 'StateProvince', 'StateProvinceLongName', 'TRIM', 'TaxScheme', 'ThirdPartyHistoryUrl', 'TitleStatus', 'TitleSubtype', 'TradeInType', 'TransportFees', 'TrimCode', 'TrimName', 'Trt', 'TrtName', 'VIN', 'Variant', 'VehicleCity', 'VehiclePhotos', 'VehicleRegion', 'VehicleStatusDisplay', 'VehicleSubType', 'VehicleType', 'VesselDepartureDate', 'Vrl', 'VrlLocks', 'VrlName', 'WHEELS', 'WarrantyBatteryExpDate', 'WarrantyBatteryIsExpired', 'WarrantyBatteryMile', 'WarrantyBatteryYear', 'WarrantyData', 'WarrantyDriveUnitExpDate', 'WarrantyDriveUnitYear', 'WarrantyMile', 'WarrantyVehicleExpDate', 'WarrantyVehicleIsExpired', 'WarrantyYear', 'Year', 'UsedVehicleLimitedWarrantyMile', 'UsedVehicleLimitedWarrantyYear', 'WarrantyDriveUnitMile', 'OdometerTypeShort', 'DeliveryDateDisplay', 'TransportationFee', 'OptionCodeSpecs', 'CompositorViewsCustom', 'IsRangeStandard', 'geoPoints', 'HasMarketingOptions']
        for result in results:
            other_info = {}
            for key in drop_keys:
                result.pop(key, None)
            for key in replace_keys:
                result[key+'_'] = result.pop(key, None)
            new_keys = list(result.keys())
            for k in new_keys:
                if k not in item_keys:
                    other_info[k] = result.pop(k, None)
            result['other_info'] = other_info
            more_info = {
                'ts': str(datetime.datetime.today()),
                'ts_short': str(datetime.date.today()),
            }
            result.update(more_info)
            items.append(result)
        self.client.insert_many(
            'spiders.tesla_used_items', items)

    def errback_httpbin(self, failure):
        meta = failure.request.meta.copy()
        yield self.make_request_from_data(meta)
