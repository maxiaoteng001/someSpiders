import os
import datetime
import logging
import traceback
import requests
import json
import sys
sys.path.append('..')
from utils import *


class Beike():
    """
    定期更新城市列表
    """

    def __init__(self):
        self.retry_times = 3

    @property
    def logger(self):
        return logging.getLogger()

    def start_requests(self):
        self.make_requests_for_data({})

    def make_requests_for_data(self, data):
        try:
            # 先判断重试上限
            if data.get('retry_times', 0) > self.retry_times:
                return

            cookies = {
                'lianjia_udid': 'ca4d8a754e66e527',
                'lianjia_ssid': '8d0b2b74-6c9a-4d21-bdd4-45a61d047862',
                'algo_session_id': 'c1dca612-976c-47b7-8e39-efec2105f7a9',
                'lianjia_uuid': '41320a21-c45e-4a37-aac8-f09ade16b635',
            }

            headers = {
                'x-req-id': '37b8487f-1af7-4da8-8448-5cf358894a21',
                'ketracetraceid': 'apps.api.ke.com-ca4d8a754e66e527-21104-1672310758235-36',
                'Page-Schema': 'SelectCityActivity',
                'Referer': 'homepage1',
                # 'Cookie': 'lianjia_udid=ca4d8a754e66e527;lianjia_ssid=8d0b2b74-6c9a-4d21-bdd4-45a61d047862;algo_session_id=c1dca612-976c-47b7-8e39-efec2105f7a9;lianjia_uuid=41320a21-c45e-4a37-aac8-f09ade16b635',
                'Lianjia-Device-Id': 'ca4d8a754e66e527',
                'User-Agent': 'Beike2.94.0;google Pixel+4+XL; Android 13',
                'extension': 'lj_duid=null&ketoken=020102h6SSN8TMz2pJ4xyCfknR+L+UBIAwijxAhAccqBEQG4WLqH9ACTblFosVFCnHPW0/lpxDnbX+Xei/y/1YARkTMg==&lj_android_id=ca4d8a754e66e527&lj_device_id_android=ca4d8a754e66e527&mac_id=',
                'Dynamic-SDK-VERSION': '1.1.0',
                'Lianjia-City-Id': '110000',
                'parentSceneId': '6433649911716795393',
                'source-global': '{}',
                'Lianjia-Channel': 'Android_ke_tencentd1',
                'Lianjia-Version': '2.94.0',
                'Lianjia-Im-Version': '2.34.0',
                'Lianjia-Recommend-Allowable': '1',
                'Authorization': 'MjAxODAxMTFfYW5kcm9pZDo0OGI4Mjg4M2NmNDdhYzAyZWQzMmFlZjI0ZmVhNjg0ODUxYzdkYWI5',
                'ip': '127.0.0.1',
                'lat': '39.946433',
                'lng': '116.443303',
                'beikeBaseData': '%7B%22appVersion%22%3A%222.94.0%22%2C%22duid%22%3A%22%22%2C%22fpid%22%3A%22020102h6SSN8TMz2pJ4xyCfknR%2BL%2BUBIAwijxAhAccqBEQG4WLqH9ACTblFosVFCnHPW0%2FlpxDnbX%2BXei%2Fy%2F1YARkTMg%5Cu003d%5Cu003d%22%7D',
                'Device-id-s': 'ca4d8a754e66e527;;020102h6SSN8TMz2pJ4xyCfknR+L+UBIAwijxAhAccqBEQG4WLqH9ACTblFosVFCnHPW0/lpxDnbX+Xei/y/1YARkTMg==',
                'Channel-s': 'Android_ke_tencentd1',
                'AppInfo-s': 'Beike;2.94.0;2940200',
                'Hardware-s': 'google;Pixel 4 XL',
                'SystemInfo-s': 'android;13',
                'WLL-KGSA': 'LJAPPVA accessKeyId=sjoe98HI099dhdD7; nonce=qofX0VHvW6WWFsMigec03DlgUcyHyRyp; timestamp=1672310758; signedHeaders=Device-id-s,AppInfo-s,User-Agent,Hardware-s,Channel-s,SystemInfo-s; signature=jA1iPpRWrxTQmioxIGpGAKbspkf4bNCz619ywqVxMxo=',
                'Host': 'apps.api.ke.com',
                'Pragma': 'no-cache',
                'Cache-Control': 'no-cache',
            }

            response = requests.get(
                'https://apps.api.ke.com/config/city/selectlist', cookies=cookies, headers=headers)
            if response.status_code == 200:
                self.parse(response)
            else:
                data['retry_times'] = data.get('retry_times', 0) + 1
                self.make_requests_for_data(data)
        except KeyboardInterrupt:
            print('意外退出')
            os._exit(0)
        except Exception as e:
            self.logger.info(e)

    def parse(self, response):
        try:
            data = json.loads(response.text)
            group_list = data.get('data').get('tab_list')[0].get('list')
            city_details = []
            for group in group_list:
                cities = group.get('cities')
                for city in cities:
                    detail = {
                        "city_id": city.get('id'),
                        "name": city.get('name'),
                        "abbr": city.get('abbr'),
                        "home_url": city.get('home_url'),
                        'ts': str(datetime.datetime.today()),
                        'ts_short': str(datetime.date.today()),
                    }
                    city_details.append(detail)
            client = MysqlHelper(dbconfig=dbconfig)
            client.insert_many("spiders.beike_city_list", city_details)
        except:
            self.logger.error('解析失败，返回：{}, 请求url: {}'.format(
                traceback.format_exc(), response.url))


if __name__ == '__main__':
    logger_init(log_path='../../logs', log_name='beike_citylist',
                    by_day=True, log_level=logging.INFO, streamHandler=True)
    Beike().start_requests()
