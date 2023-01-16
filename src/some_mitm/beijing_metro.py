import json
import re
import traceback
import mitmproxy.http
import sys
sys.path.append('..')
from utils import *
import logging
import datetime


def parse_weibo(blog_data):
    try:
        # res_data = json.loads(response.text)
        # blogs = res_data.get('items')
        print(blog_data)
        blog_infos = []
        keliu_blog_infos = []
        for blog in blog_data:
            blog = blog.get('data')
            if blog is None or blog.get('text') is None or blog.get('created_at') is None:
                continue
            created_datetime = datetime.datetime.strptime(blog.get('created_at'), '%a %b %d %H:%M:%S %z %Y') if blog.get('created_at') else 0
            passenger_flow = None
            # 北京地铁
            if '客流观察' in blog.get('text'):
                try:
                    data_date_str = re.findall('客流】([\d.]+?年[\d.]+?月[\d.]+?日)', blog.get('text'))[0]
                    data_date = datetime.datetime.strptime(data_date_str, '%Y年%m月%d日').strftime('%Y-%m-%d')
                except:
                    data_date = ''
                try:
                    passenger_flow=float(re.findall('日客运量为([\d.]+?)[ ]*?万人次', blog.get('text'))[0])*10000
                except:
                    passenger_flow = 0
            # 广州地铁
            # 悠悠报客流：昨日回顾】1月15日，线网总客流量为459.3万人次。广州天气：@广州天气  预测，今日白天： 多云；夜间：多云到晴；气温介于6到12℃之间。
            elif '线网总客流量' in blog.get('text'):
                try:
                    data_date_month = re.findall('([\d.]+?)月', blog.get('text'))[0]
                    if int(data_date_month) > created_datetime.month:
                        default_year = created_datetime.year - 1
                    else:
                        default_year = created_datetime.year
                    data_date_str = f'{default_year}年' + re.findall('([\d.]+?月[\d.]+?日)', blog.get('text'))[0]
                    data_date = datetime.datetime.strptime(data_date_str, '%Y年%m月%d日').strftime('%Y-%m-%d')
                except:
                    data_date = ''
                try:
                    passenger_flow=float(re.findall('线网总客流量为([\d.]+?)[ ]*?万人次', blog.get('text'))[0])*10000
                except:
                    passenger_flow = 0
            # 深圳地铁
            # 2023年1月15日（星期日），深圳地铁集团所辖15条运营线路（不含4号线）总客运量为335.87 万人次。详情见下图
            elif '深圳地铁集团所辖' in blog.get('text'):
                try:
                    data_date_str = re.findall('([\d.]+?年[\d.]+?月[\d.]+?日)', blog.get('text'))[0]
                    data_date = datetime.datetime.strptime(data_date_str, '%Y年%m月%d日').strftime('%Y-%m-%d')
                except:
                    data_date = ''
                try:
                    passenger_flow=float(re.findall('总客运量为([\d.]+?)[ ]*?万人次', blog.get('text'))[0])*10000
                except:
                    passenger_flow = 0
            # 上海地铁
            # 【地铁网络客流】1月15日上海地铁总客流为402.5万人次。
            elif '上海地铁总客流' in blog.get('text'):
                try:
                    data_date_month = re.findall('([\d.]+?)月', blog.get('text'))[0]
                    if int(data_date_month) > created_datetime.month:
                        default_year = created_datetime.year - 1
                    else:
                        default_year = created_datetime.year
                    data_date_str = f'{default_year}年' + re.findall('([\d.]+?月[\d.]+?日)', blog.get('text'))[0]
                    data_date = datetime.datetime.strptime(data_date_str, '%Y年%m月%d日').strftime('%Y-%m-%d')
                except:
                    data_date = ''
                try:
                    passenger_flow=float(re.findall('上海地铁总客流为([\d.]+?)[ ]*?万人次', blog.get('text'))[0])*10000
                except:
                    passenger_flow = 0
            # 成都地铁
            # 【客流播报】1月15日成都轨道交通线网总计客运量349.46万乘次
            elif '成都轨道交通线网总计客运量' in blog.get('text'):
                try:
                    data_date_month = re.findall('([\d.]+?)月', blog.get('text'))[0]
                    if int(data_date_month) > created_datetime.month:
                        default_year = created_datetime.year - 1
                    else:
                        default_year = created_datetime.year
                    data_date_str = f'{default_year}年' + re.findall('([\d.]+?月[\d.]+?日)', blog.get('text'))[0]
                    data_date = datetime.datetime.strptime(data_date_str, '%Y年%m月%d日').strftime('%Y-%m-%d')
                except:
                    data_date = ''
                try:
                    passenger_flow=float(re.findall('成都轨道交通线网总计客运量([\d.]+?)[ ]*?万乘次', blog.get('text'))[0])*10000
                except:
                    passenger_flow = 0
            # 武汉地铁
            # 【昨日客流】1月15日（周日），武汉城市轨道交通线网客运量（含换乘）为196.3万乘次，其中客流前五名依次为：2号线汉口火车站、2号线江汉路站、2号线宝通寺站、4号线武汉火车站、4号线王家湾站。 ​​​
            elif '武汉城市轨道交通线网客运' in blog.get('text'):
                try:
                    data_date_month = re.findall('([\d.]+?)月', blog.get('text'))[0]
                    if int(data_date_month) > created_datetime.month:
                        default_year = created_datetime.year - 1
                    else:
                        default_year = created_datetime.year
                    data_date_str = f'{default_year}年' + re.findall('([\d.]+?月[\d.]+?日)', blog.get('text'))[0]
                    data_date = datetime.datetime.strptime(data_date_str, '%Y年%m月%d日').strftime('%Y-%m-%d')
                except:
                    data_date = ''
                try:
                    passenger_flow=float(re.findall('为([\d.]+?)[ ]*?万乘次', blog.get('text'))[0])*10000
                except:
                    passenger_flow = 0
            # 重庆地铁
            # #昨日客运量#1月15日，重庆轨道交通线网客运量193.8 万人次。 ​​​​​​
            elif '重庆轨道交通线网客运量' in blog.get('text'):
                try:
                    data_date_month = re.findall('([\d.]+?)月', blog.get('text'))[0]
                    if int(data_date_month) > created_datetime.month:
                        default_year = created_datetime.year - 1
                    else:
                        default_year = created_datetime.year
                    data_date_str = f'{default_year}年' + re.findall('([\d.]+?月[\d.]+?日)', blog.get('text'))[0]
                    data_date = datetime.datetime.strptime(data_date_str, '%Y年%m月%d日').strftime('%Y-%m-%d')
                except:
                    data_date = ''
                try:
                    passenger_flow=float(re.findall('重庆轨道交通线网客运量([\d.]+?)[ ]*?万人次', blog.get('text'))[0])*10000
                except:
                    passenger_flow = 0
            if passenger_flow:
                keliu_blog_info = {
                    'uid': blog.get('user').get('id'),
                    'blog_id': blog.get('id'),
                    'blog_text': blog.get('text'),
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
                'blog_text': blog.get('text'),
                'created_at_origin': blog.get('create_at'),
                'created_datetime': created_datetime,
                'uid': blog.get('user').get('id'),
                'ts': str(datetime.datetime.today()),
                'ts_short': str(datetime.date.today()),
            }
            blog_infos.append(blog_info)
        client = MysqlHelper(dbconfig=dbconfig)
        print(keliu_blog_infos)
        client.insert_many('spiders.metro_passenger_flow', keliu_blog_infos)
        client.insert_many('spiders.weibo_info', blog_infos)
    except:
        logging.warn('异常原因：{}'.format(traceback.format_exc()))


class BeijingMetro:
    """
    北京地铁客流观察
    """
    def __init__(self):
        # logger_file_by_spider(spider_name='mitmproxy_', log_level=logging.INFO)
        pass

    def response(self, flow: mitmproxy.http.HTTPFlow):

        # 处理笔记
        if "profile/container_timeline" in flow.request.url:
            logging.info(flow.request.url)
            res_data = json.loads(flow.response.text)
            blogs = res_data.get('items')
            parse_weibo(blogs)
        elif 'searchall' in flow.request.url:
            logging.info(flow.request.url)
            res_data = json.loads(flow.response.text)
            cards = res_data.get('cards')[0].get('card_group')
            # 迎合上一轮结构
            blog_data = [{'data': card.get('mblog')} for card in cards]
            parse_weibo(blog_data)
        

addons = [BeijingMetro()]
