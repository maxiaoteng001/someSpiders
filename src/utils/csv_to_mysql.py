import datetime
import os
import csv
import re
from mysql_helper import MysqlHelper
from config import *


fields = [
    "origin_id",
    # 机构名称
    "hospital",
    # 急诊接诊人次（人次）
    "Number_of_emergency_visits",
    # 急诊抢救室已使用床位数
    "Number_of_beds_used",
    # 急诊抢救室实有床位数
    "The_actual_number_of_beds",
    # 急诊留观已使用床位数
    "Number_of_beds_used_for_emergency_observation",
    # 急诊留观实有床位数
    "Number_of_beds_available_for_emergency_observation",
    # 急诊抢救床位使用率（%）
    "Utilization_rate_of_emergency_beds",
    # 急诊留观床位使用率（%）
    "Utilization_rate_of_emergency_observation_beds",
    # 急诊拥挤等级
    "Emergency_room_crowding_level",
]


fields_v2 = [
    "origin_id",
    # 机构名称
    "hospital",
    # "等级",
    "grade",
    # "区县",
    "district",
    # 急诊接诊人次（人次）
    "Number_of_emergency_visits",
    # 急诊抢救室已使用床位数
    "Number_of_beds_used",
    # 急诊抢救室实有床位数
    "The_actual_number_of_beds",
    # 急诊留观已使用床位数
    "Number_of_beds_used_for_emergency_observation",
    # 急诊留观实有床位数
    "Number_of_beds_available_for_emergency_observation",
    # 急诊抢救床位使用率（%）
    "Utilization_rate_of_emergency_beds",
    # 急诊留观床位使用率（%）
    "Utilization_rate_of_emergency_observation_beds",
    # 急诊拥挤等级
    "Emergency_room_crowding_level",
]

def parse_csv_data_to_mysql(csv_path):
    details = []
    
    with open(csv_path, 'r', encoding='utf-8', newline='') as f:
        content = f.read()
        if '区县' in content:
            # 匹配两种表格
            fieldnames = fields_v2
            print('fields_v2 包含区县')
        else:
            fieldnames = fields
    with open(csv_path, 'r', encoding='utf-8', newline='') as f:
        csv_reader = csv.DictReader(f, fieldnames=fieldnames)
        for row in csv_reader:
            detail = dict(row)
            if detail.get('origin_id') is None:
                continue
            else:
                try:
                    origin_id = int(detail.get('origin_id'))
                except:
                    pass
            detail["ts"] = str(datetime.datetime.today())
            detail["ts_short"] = str(datetime.date.today())
            detail["content_id"] = csv_path.split('/')[-1].split('.')[0]
            for k, v in detail.items():
                if isinstance(v, str):
                    detail[k] = v.replace("\r", "")
                if k not in ("hospital", "grade", "district", "Emergency_room_crowding_level") and v:
                    # 正则提取[\d.]
                    detail[k] = re.sub(r'[^0-9.]', '', v)
            detail.pop('grade', None)
            detail.pop('district', None)
            details.append(detail)
    mysql_client= MysqlHelper(dbconfig=dbconfig)
    mysql_client.insert_many('spiders.beijing120_emergency_info', details)


cur_dir = os.path.abspath('.')
csv_dir = '../../csv'

files = os.listdir(csv_dir)
for file in files:
    csv_path = os.path.join(cur_dir, csv_dir, file)
    try:
        parse_csv_data_to_mysql(csv_path)
    except KeyboardInterrupt:
        break
    except Exception as e:
        print(e, csv_path)
