from lxml import etree
import scrapy
import re
import urllib
import json
import demjson3
import datetime
import sys
sys.path.append('..')
from utils import *


class MySpider(scrapy.Spider):
    """
    http://www.xatjbl.com/solution_complex.aspx
    """
    name = 'xian_tangjiu'

    def __init__(self, name=None, **kwargs):
        self.client = MysqlHelper(dbconfig=dbconfig)
        super().__init__(name, **kwargs)

    def start_requests(self):
        yield self.make_request_from_data({})

    def make_request_from_data(self, data):
        data['retry_times'] = data.get('retry_times', 0) + 1
        if data.get('retry_times', 0) > 20:
            self.logger.info(f'failure_request: {json.dumps(data)}')
            return
        headers = {
            'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36',
        }
        query_string = {
            'fid': 'n25:25:25',
        }
        if data.get('pageindex'):
            query_string['pageindex'] = data.get('pageindex')
        url = 'http://www.xatjbl.com/solution_complex.aspx?{}'.format(
            urllib.parse.urlencode(query_string))
        return scrapy.Request(url,
                              headers=headers,
                              callback=self.parse,
                              errback=self.errback_httpbin,
                              dont_filter=True,
                              meta=data
                              )

    def parse(self, response):
        meta = response.meta
        page_source = etree.HTML(response.text)
        item_tags = page_source.xpath('//li[@class="lb_map_li"]')

        for item_tag in item_tags:
            title_tag = item_tag.xpath('.//a[@class="lb_map_afl"]')[0]
            mark_info = demjson3.decode(title_tag.xpath('./@mark')[0])
            try:
                address = item_tag.xpath('.//span[@class="lb_map_afr_c04"]/span/text()')[0]
            except:
                address = mark_info.get('content')
            detail = {
                'title': mark_info.get('title'),
                'address': address,
                'isOpen': mark_info.get('isOpen'),
                'point': mark_info.get('point'),
                'ts': str(datetime.datetime.today()),
                'ts_short': str(datetime.date.today()),
            }
            self.client.insert('spiders.xian_tangjiu', detail)
        
        if len(item_tags)>=4:
            yield self.make_request_from_data({'pageindex': meta.get('pageindex', 1)+1})
     
    def errback_httpbin(self, failure):
        meta = failure.request.meta.copy()
        yield self.make_request_from_data(meta)
