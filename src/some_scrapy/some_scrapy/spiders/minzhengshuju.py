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
    季报
    https://www.mca.gov.cn/article/sj/tjjb/qgsj/
    https://www.mca.gov.cn/article/sj/tjjb/sjsj/
    月报
    https://www.mca.gov.cn/article/sj/tjyb/qgsj/
    https://www.mca.gov.cn/article/sj/tjyb/sjsj/
    """
    name = 'minzhengshuju'

    def __init__(self, name=None, **kwargs):
        self.client = MysqlHelper(dbconfig=dbconfig)
        super().__init__(name, **kwargs)

    def start_requests(self):
        # 月报没有婚姻登记和殡葬服务数据, 所以跑季报
        # urls = [
        #     'https://www.mca.gov.cn/article/sj/tjjb/sjsj/',
        #     # 'https://www.mca.gov.cn/article/sj/tjjb/sjsj/?2',
        #     # 'https://www.mca.gov.cn/article/sj/tjjb/sjsj/?3',
        #     # 'https://www.mca.gov.cn/article/sj/tjjb/sjsj/?4',
        #     # 'https://www.mca.gov.cn/article/sj/tjjb/sjsj/?5',
        # ]
        urls = self.client.query('select * from spiders.minzhengshuju_list')
        for url in urls:
            # yield self.make_request_from_data({'url': url})
            yield self.make_request_from_data(url)

    def make_request_from_data(self, data):
        data['retry_times'] = data.get('retry_times', 0) + 1
        if data.get('retry_times', 0) > 20:
            self.logger.info(f'failure_request: {json.dumps(data)}')
            return
        headers = {
            'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36',
        }
        url = data.get('url')
        if url.endswith('html'):
            return scrapy.Request(url,
                              headers=headers,
                              callback=self.parse_info,
                              errback=self.errback_httpbin,
                              dont_filter=True,
                              meta=data
                              )
        else:
            return scrapy.Request(url,
                              headers=headers,
                              callback=self.parse_list,
                              errback=self.errback_httpbin,
                              dont_filter=True,
                              meta=data
                              )

    def parse_list(self, response):
        meta = response.meta
        page_source = etree.HTML(response.text)
        article_tags = page_source.xpath('//ul[@class="alist_ul"]//tr')

        for article_tag in article_tags:
            try:
                title_tag = article_tag.xpath('.//a[@class="artitlelist"]')[0]
                title = title_tag.xpath('./@title')[0]
                url = 'https://www.mca.gov.cn' + title_tag.xpath('./@href')[0]
                pub_date = article_tag.xpath('.//td[@class="timedefault"]/text()')[0]
                try:
                    date_info = '-'.join(re.findall('(\d+?)年(\d+?)季度', title)[0])
                except:
                    date_info = ''
                detail = {
                    'title': title,
                    'url': url,
                    'date_info': date_info, 
                    'pub_date': pub_date,
                    'ts': str(datetime.datetime.today()),
                    'ts_short': str(datetime.date.today()),
                }
                self.client.insert('spiders.minzhengshuju_list', detail)
            except Exception as e:
                print(e)
        # 无需翻页, 只在第一次的时候遍历9页历史数据

    def parse_info(self, response):
        meta = response.meta
        # 解析年度和季度
        if re.findall('window.location.href="(\S+?)"', response.text):
            url = 'window.location.href="https://www.mca.gov.cn/article/sj/tjjb/2022/202201fssj.html'
            meta['url'] = re.findall('window.location.href="(\S+?)"', response.text)[0]
            yield self.make_request_from_data(meta)
            return
        url = meta.get('url')

        raw_data = {
            'url': url,
            'raw_data': response.text,
            'ts': str(datetime.datetime.today()),
            'ts_short': str(datetime.date.today()),
        }
        self.client.insert('spiders.minzhengshuju_tjjb_sjsj_raw_data', raw_data)

        date_info = meta.get('date_info')
        page_source = etree.HTML(response.text)
        row_tags = page_source.xpath('//tr')
        max_col = max([len(r.xpath('.//td')) for r in row_tags])
        header_tags = []

        # headers_info = [
        #     ['民政事业费累计支出', '亿元'],
        #     ['镇', '个'],
        #     ['乡', '个'],
        #     ['街道', '个'],
        #     ['区公所', '个'],
        #     ['提供住宿的民政机构', '个'],
        #     ['养老机构', '个'],
        #     ['精神疾病服务机构', '个'],
        #     ['儿童福利和救助机构	', '个'],
        #     ['其他提供住宿机构	', '个'],
        #     ['提供住宿的民政机构床位', '万张'],
        #     ['养老机构床位', '万张'],
        #     ['精神疾病服务机构床位', '张'],
        #     ['儿童福利和救助机构床位', '张'],
        #     ['其他提供住宿机构床位', '张'],
        #     ['城市低保人数', '万人'],
        #     ['城市低保户数', '万户'],
        #     ['城市低保平均标准', '元/人·月'],
        #     ['农村低保人数', '万人'],
        #     ['农村低保户数', '万户'],
        #     ['农村低保平均标准/月', '元/人·月'],
        #     ['农村低保平均标准/年', '元/人·年'],
        #     ['城市特困人员救助供养人数', '人'],
        #     ['农村特困人员救助供养人数', '万人'],
        #     ['生活无着的流浪乞讨人员救助 *', '人次'],
        #     ['临时救助 *', '万人次'],
        #     ['社区服务指导中心', '个'],
        #     ['社区服务中心', '个'],
        #     ['社区服务站', '个'],
        #     ['社区专项服务机构和设施', '个'],
        #     ['享受困难残疾人生活补贴人数', '万人'],
        #     ['享受重度残疾人护理补贴人数', '万人'],
        #     ['孤儿', '人'],
        #     ['集中养育孤儿', '人'],
        #     ['社会散居孤儿', '人'],
        #     ['收养登记 *', '件'],
        #     ['福利彩票销售 *', '亿元'],
        #     ['注册志愿者', '万人'],
        #     ['社会团体', '个'],
        #     ['民办非企业单位', '个'],
        #     ['基金会', '个'],
        #     ['村委会', '个'],
        #     ['社区居委会', '个'],
        #     ['结婚登记 *', '万对'],
        #     ['离婚登记 *', '万对'],
        #     ['火化遗体 *', '万具'],
        # ]
        details = []
        for row_tag in row_tags:
            # 行高 24 有效列名, 行高19 数据
            # tr_height = int(row_tag.xpath('./@height')[0] if row_tag.xpath('./@height') else 0)
            if not row_tag.xpath('.//td'):
                # 空行
                continue
            else:
                # 是数据
                tds = row_tag.xpath('.//td')
                province = tds[0].xpath('./text()')[0] if tds[0].xpath('./text()') else ''
                for index, td_tag in enumerate(tds):
                    if index == 0:
                        continue
                    detail = {
                        'date_info': date_info,
                        'province': province,
                        'col_index': f'col_{index}',
                        # 'unit': headers_info[index-1][1],
                        'value': td_tag.xpath('./text()')[0] if td_tag.xpath('./text()') else '',
                        'ts': str(datetime.datetime.today()),
                        'ts_short': str(datetime.date.today()),
                    }
                    details.append(detail)
            # elif tr_height == 24:
            #     # 稍后解析
            #     header_tags.append(row_tag)
        # 省级数据, 季报
        self.client.insert_many('spiders.minzhengshuju_tjjb_sjsj', details)

    def errback_httpbin(self, failure):
        meta = failure.request.meta.copy()
        yield self.make_request_from_data(meta)
