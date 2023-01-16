from lxml import etree
import scrapy
import re
import urllib
import json
import datetime
import sys
sys.path.append('..')
from utils import *


class BeijingMetroWeiboSpider(scrapy.Spider):
    """
    可以传入其他用户id来爬取最新微博
    访客cookie尽量只访问首页
    """
    name = 'beijing_metro_weibo'
    # start_urls = ['https://s.weibo.com/weibo?q=%23%E5%AE%A2%E6%B5%81%E8%A7%82%E5%AF%9F%23']

    def __init__(self, name=None, **kwargs):
        self.client = MysqlHelper(dbconfig=dbconfig)
        self.cookies = gen_visitor_info()
        super().__init__(name, **kwargs)

    def start_requests(self):
        # https://weibo.com/ajax/statuses/mymblog?uid=2778292197&page=1&feature=0
        users = [
            # 北京地铁, 爬取客流观察
            '2778292197',
            # 上海地铁
            '1742987497',
            # 广州地铁
            '2612249974',
            # 深圳地铁
            '2311331195',
            # 成都地铁 成都地铁运营
            '3170665862',
            # 武汉地铁
            '3186945861',
            # 重庆轨道交通
            '2152519810',
        ]
        for uid in users:
            yield self.make_request_from_data({'uid': uid})

    def make_request_from_data(self, data):
        data['retry_times'] = data.get('retry_times', 0) + 1
        if data.get('retry_times', 0) > 20:
            self.logger.info(f'failure_request: {json.dumps(data)}')
            return
        headers = {
            'Host': 'weibo.com',
            'server-version': 'v2023.01.13.2',
            'sec-ch-ua': '"Not?A_Brand";v="8", "Chromium";v="108", "Google Chrome";v="108"',
            'dnt': '1',
            'sec-ch-ua-mobile': '?0',
            'client-version': 'v2.37.31',
            'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36',
            'accept': 'application/json, text/plain, */*',
            # 'referer': 'https://weibo.com/2778292197',
            'accept-language': 'zh-CN,zh;q=0.9',
        }
        headers['Cookie'] = "; ".join(
            [str(x)+"="+str(y) for x, y in self.cookies.items()])
        query_string = {
            'uid': data.get('uid'),
            # 'page': '1',
            # 'feature': '0',
        }
        url = 'https://weibo.com/ajax/statuses/mymblog?{}'.format(
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
        blogs = res_data.get('data').get('list')
        blog_infos = []
        keliu_blog_infos = []
        for blog in blogs:
            if blog is None or blog.get('text_raw') is None or blog.get('created_at') is None:
                continue
            created_datetime = datetime.datetime.strptime(blog.get('created_at'), '%a %b %d %H:%M:%S %z %Y') if blog.get('created_at') else 0
            passenger_flow = None
            # 北京地铁
            if '客流观察' in blog.get('text_raw'):
                try:
                    data_date_str = re.findall('客流】([\d.]+?年[\d.]+?月[\d.]+?日)', blog.get('text_raw'))[0]
                    data_date = datetime.datetime.strptime(data_date_str, '%Y年%m月%d日').strftime('%Y-%m-%d')
                except:
                    data_date = ''
                try:
                    passenger_flow=float(re.findall('日客运量为([\d.]+?)[ ]*?万人次', blog.get('text_raw'))[0])*10000
                except:
                    passenger_flow = 0
            # 广州地铁
            # 悠悠报客流：昨日回顾】1月15日，线网总客流量为459.3万人次。广州天气：@广州天气  预测，今日白天： 多云；夜间：多云到晴；气温介于6到12℃之间。
            elif '线网总客流量' in blog.get('text_raw'):
                try:
                    data_date_month = re.findall('([\d.]+?)月', blog.get('text_raw'))[0]
                    if int(data_date_month) > created_datetime.month:
                        default_year = created_datetime.year - 1
                    else:
                        default_year = created_datetime.year
                    data_date_str = f'{default_year}年' + re.findall('([\d.]+?月[\d.]+?日)', blog.get('text_raw'))[0]
                    data_date = datetime.datetime.strptime(data_date_str, '%Y年%m月%d日').strftime('%Y-%m-%d')
                except:
                    data_date = ''
                try:
                    passenger_flow=float(re.findall('线网总客流量为([\d.]+?)[ ]*?万人次', blog.get('text_raw'))[0])*10000
                except:
                    passenger_flow = 0
            # 深圳地铁
            # 2023年1月15日（星期日），深圳地铁集团所辖15条运营线路（不含4号线）总客运量为335.87 万人次。详情见下图
            elif '深圳地铁集团所辖' in blog.get('text_raw'):
                try:
                    data_date_str = re.findall('([\d.]+?年[\d.]+?月[\d.]+?日)', blog.get('text_raw'))[0]
                    data_date = datetime.datetime.strptime(data_date_str, '%Y年%m月%d日').strftime('%Y-%m-%d')
                except:
                    data_date = ''
                try:
                    passenger_flow=float(re.findall('总客运量为([\d.]+?)[ ]*?万人次', blog.get('text_raw'))[0])*10000
                except:
                    passenger_flow = 0
            # 上海地铁
            # 【地铁网络客流】1月15日上海地铁总客流为402.5万人次。
            elif '上海地铁总客流' in blog.get('text_raw'):
                try:
                    data_date_month = re.findall('([\d.]+?)月', blog.get('text_raw'))[0]
                    if int(data_date_month) > created_datetime.month:
                        default_year = created_datetime.year - 1
                    else:
                        default_year = created_datetime.year
                    data_date_str = f'{default_year}年' + re.findall('([\d.]+?月[\d.]+?日)', blog.get('text_raw'))[0]
                    data_date = datetime.datetime.strptime(data_date_str, '%Y年%m月%d日').strftime('%Y-%m-%d')
                except:
                    data_date = ''
                try:
                    passenger_flow=float(re.findall('上海地铁总客流为([\d.]+?)[ ]*?万人次', blog.get('text_raw'))[0])*10000
                except:
                    passenger_flow = 0
            # 成都地铁
            # 【客流播报】1月15日成都轨道交通线网总计客运量349.46万乘次
            elif '成都轨道交通线网总计客运量' in blog.get('text_raw'):
                try:
                    data_date_month = re.findall('([\d.]+?)月', blog.get('text_raw'))[0]
                    if int(data_date_month) > created_datetime.month:
                        default_year = created_datetime.year - 1
                    else:
                        default_year = created_datetime.year
                    data_date_str = f'{default_year}年' + re.findall('([\d.]+?月[\d.]+?日)', blog.get('text_raw'))[0]
                    data_date = datetime.datetime.strptime(data_date_str, '%Y年%m月%d日').strftime('%Y-%m-%d')
                except:
                    data_date = ''
                try:
                    passenger_flow=float(re.findall('成都轨道交通线网总计客运量([\d.]+?)[ ]*?万乘次', blog.get('text_raw'))[0])*10000
                except:
                    passenger_flow = 0
            # 武汉地铁
            # 【昨日客流】1月15日（周日），武汉城市轨道交通线网客运量（含换乘）为196.3万乘次，其中客流前五名依次为：2号线汉口火车站、2号线江汉路站、2号线宝通寺站、4号线武汉火车站、4号线王家湾站。 ​​​
            elif '武汉城市轨道交通线网客运' in blog.get('text_raw'):
                try:
                    data_date_month = re.findall('([\d.]+?)月', blog.get('text_raw'))[0]
                    if int(data_date_month) > created_datetime.month:
                        default_year = created_datetime.year - 1
                    else:
                        default_year = created_datetime.year
                    data_date_str = f'{default_year}年' + re.findall('([\d.]+?月[\d.]+?日)', blog.get('text_raw'))[0]
                    data_date = datetime.datetime.strptime(data_date_str, '%Y年%m月%d日').strftime('%Y-%m-%d')
                except:
                    data_date = ''
                try:
                    passenger_flow=float(re.findall('为([\d.]+?)[ ]*?万乘次', blog.get('text_raw'))[0])*10000
                except:
                    passenger_flow = 0
            # 重庆地铁
            # #昨日客运量#1月15日，重庆轨道交通线网客运量193.8 万人次。 ​​​​​​
            elif '重庆轨道交通线网客运量' in blog.get('text_raw'):
                try:
                    data_date_month = re.findall('([\d.]+?)月', blog.get('text_raw'))[0]
                    if int(data_date_month) > created_datetime.month:
                        default_year = created_datetime.year - 1
                    else:
                        default_year = created_datetime.year
                    data_date_str = f'{default_year}年' + re.findall('([\d.]+?月[\d.]+?日)', blog.get('text_raw'))[0]
                    data_date = datetime.datetime.strptime(data_date_str, '%Y年%m月%d日').strftime('%Y-%m-%d')
                except:
                    data_date = ''
                try:
                    passenger_flow=float(re.findall('重庆轨道交通线网客运量([\d.]+?)[ ]*?万人次', blog.get('text_raw'))[0])*10000
                except:
                    passenger_flow = 0
            if passenger_flow:
                keliu_blog_info = {
                    'uid': blog.get('user').get('id'),
                    'blog_id': blog.get('id'),
                    'blog_text': blog.get('text_raw'),
                    'created_at_origin': blog.get('created_at'),
                    'created_datetime': created_datetime,
                    'passenger_flow': passenger_flow,
                    'data_date': data_date,
                    'ts': str(datetime.datetime.today()),
                    'ts_short': str(datetime.date.today()),
                }
                keliu_blog_infos.append(keliu_blog_info)
            blog_info = {
                'blog_id': blog.get('id'),
                'blog_text': blog.get('text_raw'),
                'created_at_origin': blog.get('created_at'),
                'created_datetime': created_datetime,
                'uid': blog.get('user').get('id'),
                'ts': str(datetime.datetime.today()),
                'ts_short': str(datetime.date.today()),
            }
            blog_infos.append(blog_info)
        self.client.insert_many('spiders.metro_passenger_flow', keliu_blog_infos)
        self.client.insert_many('spiders.weibo_info', blog_infos)

    def parse_html(self, response):
        page_source = etree.HTML(response.text)
        item_tags = page_source.xpath('//div[@action-type="feed_list_item"]')
        for item_tag in item_tags:
            try:
                blog_id = item_tag.xpath('./@mid')[0]
                # pub_date = item_tag.xpath('.//div[@class="from"]/a/text()')[0].strip()
                blog_text = ','.join(item_tag.xpath(
                    './/p[@node-type="feed_list_content"]/text()')).strip()
                passenger_flow = float(re.findall(
                    '日客运量为([\d.]+?)[ ]*?万人次', blog_text)[0])*10000
                data_date_str = re.findall(
                    '客流】([\d.]+?年[\d.]+?月[\d.]+?日)', blog_text)[0]
                data_date = datetime.datetime.strptime(
                    data_date_str, '%Y年%m月%d日').strftime('%Y-%m-%d')
                blog_info = {
                    'blog_id': blog_id,
                    'blog_text': blog_text,
                    'passenger_flow': passenger_flow,
                    'data_date': data_date,
                    'ts': str(datetime.datetime.today()),
                    'ts_short': str(datetime.date.today()),
                }
                self.client.insert('spiders.metro_passenger_flow', blog_info)
            except:
                self.logger.info("error_blog_id: {}".format(blog_id))

    def errback_httpbin(self, failure):
        meta = failure.request.meta.copy()
        yield self.make_request_from_data(meta)
