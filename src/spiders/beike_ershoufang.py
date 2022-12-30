import os
import datetime
import time
import logging
import traceback
import requests
import re
import sys
sys.path.append('..')
from utils import *


class BeikeErshoufang():
    """
    二手房总数, 以及首页的信息
    """

    def __init__(self):
        self.retry_times = 3
        self.mysql_client= MysqlHelper(dbconfig=dbconfig)

    @property
    def logger(self):
        return logging.getLogger()

    def start_requests(self):
        cities = self.mysql_client.query("select distinct city_id, abbr from spiders.beike_city_list")
        for city in cities:
            self.make_requests_for_data(city)

    def make_requests_for_data(self, data):
        try:
            # 先判断重试上限
            if data.get('retry_times', 0) > self.retry_times:
                return
            headers = {
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36"
            }
            url = 'https://{}.ke.com/ershoufang/'.format(data.get('abbr'))
            time.sleep(5)
            response = requests.get(url, headers=headers, timeout=5)
            if response.status_code == 200:
                self.parse(response, data)
            else:
                data['retry_times'] = data.get('retry_times', 0) + 1
                self.make_requests_for_data(data)
        except KeyboardInterrupt:
            print('意外退出')
            os._exit(0)
        except Exception as e:
            self.logger.info(e)

    def parse(self, response, meta):
        try:
            total_count = re.findall('count: (\S+?),', response.text)
            if total_count:
                total_count = total_count[0]
            else:
                total_count = 0
            count_detail = {
                'city_id': meta.get('city_id'),
                'total_count': total_count,
                'ts': str(datetime.datetime.today()),
                'ts_short': str(datetime.date.today()),
            }
            self.mysql_client.insert('spiders.beike_ershoufang_cnt', count_detail)
        except:
            self.logger.error('解析失败，返回：{}, 请求url: {}'.format(
                traceback.format_exc(), response.url))


if __name__ == '__main__':
    logger_init(log_path='../../logs', log_name='beike_ershoufang',
                    by_day=True, log_level=logging.INFO, streamHandler=True)
    BeikeErshoufang().start_requests()
