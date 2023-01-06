import os
import json
import datetime
import time
from lxml import etree
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
    # 解析每日上传paf的url
    """

    def __init__(self):
        self.retry_times = 3
        self.mysql_client= MysqlHelper(dbconfig=dbconfig)

    @property
    def logger(self):
        return logging.getLogger()

    def start_requests(self):
        contents = self.mysql_client.query("select distinct content_id from spiders.beijing120_content_list")
        for content in contents:
            self.make_requests_for_data(content)

    def make_requests_for_data(self, data):
        try:
            # 先判断重试上限
            if data.get('retry_times', 0) > self.retry_times:
                return
            headers = {
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36"
            }
            time.sleep(1)
            if data.get('pdf_url'):
                response = requests.get(data.get('pdf_url'))
                with open(f"../../pdf/{data.get('content_id')}.pdf", 'wb') as f:
                    f.write(response.content)
                print(f"{data.get('content_id')}成功下载pdf")
                return
            elif data.get('content_id'):
                # 请求详情获取pdf url
                post_data = {
                    'channelId': '238',
                    'pageCurr': '1',
                }
                response = requests.post(f"https://www.beijing120.com/content/{data.get('content_id')}", headers=headers, data=post_data)
            else:
                return
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
            # 解析pdf_url
            page_source = etree.HTML(response.text)
            pdf_url = page_source.xpath('//div[@class="article_all"]//p/a/@href')
            if pdf_url:
                pdf_url = pdf_url[0]
            else:
                pdf_url = None
            content_info = {
                'content_id': meta.get("content_id"),
                'pdf_url': pdf_url,
                'response': response.text,
                'ts': str(datetime.datetime.today()),
                'ts_short': str(datetime.date.today()),
            }
            self.mysql_client.insert('spiders.beijing120_content_info', content_info)
            if pdf_url:
                self.make_requests_for_data(content_info)
            print(f"{meta.get('content_id')}请求结束")
        except:
            self.logger.error('解析失败，返回：{}, 请求url: {}'.format(
                traceback.format_exc(), response.url))


if __name__ == '__main__':
    logger_init(log_path='../../logs', log_name='beijing120_content_info',
                    by_day=True, log_level=logging.INFO, streamHandler=True)
    Beijing120().start_requests()
