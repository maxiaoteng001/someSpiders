import os
import json
import datetime
import time
import logging
import traceback
import requests
import re
import sys
sys.path.append('..')
from utils import *


class Beijing120():
    """
    https://www.beijing120.com/content/9655
    """

    def __init__(self):
        self.retry_times = 3
        self.mysql_client= MysqlHelper(dbconfig=dbconfig)

    @property
    def logger(self):
        return logging.getLogger()

    def start_requests(self):
        self.make_requests_for_data({"page": 1})

    def make_requests_for_data(self, data):
        try:
            # 先判断重试上限
            if data.get('retry_times', 0) > self.retry_times:
                return
            headers = {
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36"
            }
            time.sleep(5)
            if data.get('page'):
                # 翻页获取列表
                post_data = {
                    'page': data.get('page'),
                    'channelId': '238',
                }
                response = requests.post('https://www.beijing120.com/page', headers=headers, data=post_data)
            elif data.get('content_id'):
                # 请求详情获取pdf url
                post_data = {
                    'channelId': '238',
                    'pageCurr': '1',
                }
                response = requests.post(f"https://www.beijing120.com/content/{data.get('content_id')}", headers=headers, data=post_data)
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
            if meta.get('page'):
                # 来自翻页, 解析content_id, 生成翻页请求
                res_data = json.loads(response.text)
                content_data = res_data.get("data")
                for content in content_data:
                    content_info = {
                        'channelId': content.get('channelId'),
                        'pub_date': content.get('date'),
                        'content_id': content.get('id'),
                        'title': content.get('title'),
                        'ts': str(datetime.datetime.today()),
                        'ts_short': str(datetime.date.today()),
                    }
                    self.mysql_client.insert('spiders.beijing120_content_list', content_info)
                if meta.get('page') == 1:
                    pages = res_data.get('pages')
                    for page in range(2, pages+1):
                        self.make_requests_for_data({"page": page})
            elif meta.get('content_id'):
                # 解析pdf_url
                pass
        except:
            self.logger.error('解析失败，返回：{}, 请求url: {}'.format(
                traceback.format_exc(), response.url))


if __name__ == '__main__':
    logger_init(log_path='../../logs', log_name='beijing120',
                    by_day=True, log_level=logging.INFO, streamHandler=True)
    Beijing120().start_requests()
