from lxml import etree
import scrapy
import re
import urllib
import json
import datetime
import sys
sys.path.append('..')
from utils import *


class SomeSpider(scrapy.Spider):
    """
    懂车帝销量数据
    """
    name = 'dongchedi_rank_data'

    def __init__(self, name=None, **kwargs):
        self.client = MysqlHelper(dbconfig=dbconfig)
        super().__init__(name, **kwargs)

    def start_requests(self):
        month_info = [
            "202301",
            # "202011",
            # "202010",
            # "202009",
            # "202008",
            # "202007",
            # "202006",
            # "202005",
            # "202004",
            # "202003",
            # "202002",
            # "202001",
        ]
        for month in month_info:
            yield self.make_request_from_data({'month': month})

    def make_request_from_data(self, data):
        data['retry_times'] = data.get('retry_times', 0) + 1
        if data.get('retry_times', 0) > 20:
            self.logger.info(f'failure_request: {json.dumps(data)}')
            return
        headers = {
            'authority': 'www.dongchedi.com',
            'accept': '*/*',
            'accept-language': 'zh-CN,zh;q=0.9,en;q=0.8,en-US;q=0.7,zh-TW;q=0.6,ja;q=0.5',
            'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36',
        }
        query_string = {
            'aid': '1839',
            'app_name': 'auto_web_pc',
            'city_name': '',
            'count': '10',
            'offset': data.get('offset', 0),
            'month': data.get('month'),
            'new_energy_type': '',
            # 11 销量
            'rank_data_type': '11',
            'brand_id': '',
            'price': '',
            'manufacturer': '',
            # param: "0,1,2,3,4,5", text: "全部轿车" param: "10,11,12,13,14", text: "全部SUV" param: "20,21,22,23,24", text: "全部MPV" param: "1,2,3", text: "全部新能源"
            'outter_detail_type': '',
            'nation': '0',
        }
        url = 'https://www.dongchedi.com/motor/pc/car/rank_data?{}'.format(
            urllib.parse.urlencode(query_string))
        return scrapy.Request(url,
                              headers=headers,
                              callback=self.parse,
                              errback=self.errback_httpbin,
                              dont_filter=True,
                              meta=data
                              )

    def parse(self, response):
        try:
            res_data = json.loads(response.text)
            meta = response.meta
            rank_list = res_data.get('data').get('list')
            rank_infos = []
            for rank in rank_list:
                rank_info = {
                    'series_id': rank.get('series_id'),
                    "series_name": rank.get("series_name"),
                    "image": rank.get("image"),
                    "cur_rank": rank.get("rank"),
                    "min_price": rank.get("min_price"),
                    "max_price": rank.get("max_price"),
                    "last_rank": rank.get("last_rank"),
                    "sold_count": rank.get("count"),
                    "score": rank.get("score"),
                    "car_review_count": rank.get("car_review_count"),
                    "rank_text": rank.get("text"),
                    "show_trend": rank.get("show_trend"),
                    "descender_price": rank.get("descender_price"),
                    "offline_car_ids": rank.get("offline_car_ids"),
                    "online_car_ids": rank.get("online_car_ids"),
                    "series_pic_count": rank.get("series_pic_count"),
                    "brand_id": rank.get("brand_id"),
                    "outter_detail_type": rank.get("outter_detail_type"),
                    "brand_name": rank.get("brand_name"),
                    "sub_brand_id": rank.get("sub_brand_id"),
                    "sub_brand_name": rank.get("sub_brand_name"),
                    "price": rank.get("price"),
                    "dealer_price": rank.get("dealer_price"),
                    "has_dealer_price": rank.get("has_dealer_price"),
                    "review_tag_list": rank.get("review_tag_list"),
                    "part_id": rank.get("part_id"),
                    "month_info": meta.get('month'),
                    'ts': str(datetime.datetime.today()),
                    'ts_short': str(datetime.date.today()),
                }
                rank_infos.append(rank_info)
            self.client.insert_many('spiders.dongchedi_rank_data', rank_infos)

            # 翻页:
            if res_data.get('data').get('paging', {}).get('has_more'):
                new_data = {
                    'month': meta.get('month'),
                    'offset': meta.get('offset', 0) + 10
                }
                yield self.make_request_from_data(new_data)
        except:
            yield self.make_request_from_data(meta)

    def errback_httpbin(self, failure):
        meta = failure.request.meta.copy()
        yield self.make_request_from_data(meta)
