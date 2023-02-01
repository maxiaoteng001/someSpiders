from lxml import etree
import traceback
import urllib
from urllib import parse
import mitmproxy.http
import sys
sys.path.append('..')
from utils import *
import logging
import datetime


def parse_poi(res_text, keyword, page):
    try:
        page_source = etree.HTML(res_text)
        dd_tags = page_source.xpath('//dd[@class="poi-list-item"]')
        poi_datas = []
        for dd_tag in dd_tags:
            url = 'http:' + dd_tag.xpath('./a[@class="react"]/@href')[0]
            try:
                poi_id = url.split('/')[-1]
            except:
                poi_id = ''
            poiname = dd_tag.xpath('.//span[@class="poiname"]/text()')[0]
            try:
                # <em class="star-text">3.5</em>
                start_text = dd_tag.xpath('.//em[@class="star-text"]/text()')[0]
            except:
                start_text = ''
            # data-com="locdist"
            try:
                lat = dd_tag.xpath('.//span[@data-com="locdist"]/@data-lat')[0]
                lng = dd_tag.xpath('.//span[@data-com="locdist"]/@data-lng')[0]
            except:
                lat = ''
                lng = ''
            try:
                region = dd_tag.xpath('.//a[@class=""]/text()')[0]
            except:
                region = ''

            poi_data = {
                'url': url,
                'poi_id': poi_id,
                'poiname': poiname,
                'start_text': start_text,
                'lat': lat,
                'lng': lng,
                'region': region,
                'keyword': keyword,
                'page': page,
                'ts': str(datetime.datetime.today()),
                'ts_short': str(datetime.date.today()),
            }
            poi_datas.append(poi_data)
        client = MysqlHelper(dbconfig=dbconfig)
        client.insert_many('spiders.meituan_poi', poi_datas)
    except:
        logging.warn('异常原因：{}'.format(traceback.format_exc()))


class MeituanH5:
    """
    美团h5
    """
    def __init__(self):
        # logger_file_by_spider(spider_name='mitmproxy_', log_level=logging.INFO)
        pass

    def response(self, flow: mitmproxy.http.HTTPFlow):

        if "i.meituan.com/s/a" in flow.request.url:
            keyword= flow.request.query.get('w')
            page= flow.request.query.get('p')
            logging.info(flow.request.url)
            parse_poi(flow.response.text, keyword, page)
        elif "i.meituan.com/s" in flow.request.url:
            keyword= urllib.parse.unquote(flow.request.url.split('?')[0].split('-')[-1])
            page= flow.request.query.get('p')
            logging.info(flow.request.url)
            parse_poi(flow.response.text, keyword, page)
        

addons = [MeituanH5()]
