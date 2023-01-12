import datetime
import os
import re
import csv
from PyPDF2 import PdfReader
from mysql_helper import MysqlHelper
from config import *


def pdf_to_hospital_info(pdf_path):
    """
    pypdf2 能很好把超长字符串解析, 但是又无法正确分列
    这个方法只用正则解析头部的序号和hospital
    19北京市延庆区医院（北京大学第三医院延庆医院）473 14 12 8 6116.67133.33急诊拥挤等级高'
    """
    reader = PdfReader(pdf_path)
    hospital_info = []
    for page in reader.pages:
        text = page.extract_text()
        for line in text.split('\n'):
            if re.findall('(\d+?)([^\d]+?)[\d ,]', line):
                order_id, hospital = re.findall('(\d+?)([^\d]+?)[\d ,]', line)[0]
                info = {
                    'order_id': order_id,
                    'hospital': hospital,
                    'content_id': pdf_path.split('/')[-1].split('.')[0],
                }
                hospital_info.append(info)
    return hospital_info


cur_dir = os.path.abspath('.')
pdf_dir = '../../pdf'

files = os.listdir(pdf_dir)
for file in files:
    pdf_path = os.path.join(cur_dir, pdf_dir, file)
    try:
        hospital_info = pdf_to_hospital_info(pdf_path)
        # 写入mysql
        mysql_client= MysqlHelper(dbconfig=dbconfig)
        mysql_client.insert_many('spiders.beijing120_hospital_basic_info', hospital_info)
    except KeyboardInterrupt:
        break
    except Exception as e:
        print(e, pdf_path)
