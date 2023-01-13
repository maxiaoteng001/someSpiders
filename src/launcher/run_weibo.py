# -*- coding: utf-8 -*-
import subprocess
from apscheduler.schedulers.blocking import BlockingScheduler
import time
import os
import sys
BASE_DIR = os.path.join(os.path.abspath(os.path.dirname(__file__)), '..')
sys.path.append(BASE_DIR)
from utils import *


def start_up_spider(spider_name, pools=1):
    '''使用subprocess启动scrapy,
    标准输出重定向到null, 日志配置见settings.py

    Args:
        spider_name: 启动的爬虫名称
        pools: 启动的进程数, 默认1
    '''
    subpros = []
    s = 'scrapy crawl {} >/dev/null 2>&1'.format(spider_name)
    for _ in range(pools):
        subpro = subprocess.Popen(s, shell=True, stdout=None)
        subpros.append(subpro)
        time.sleep(2)

    for por in subpros:
        por.wait()


def crawler_beijing_metro():
    por = subprocess.Popen('source ../../venv_somespiders/bin/activate && cd ../some_scrapy && scrapy crawl beijing_metro_weibo >/dev/null 2>&1', shell=True, stdout=None, executable='/bin/bash')
    por.wait()


def cron_job():
    sched = BlockingScheduler()
    # 每日更新北京地铁最新微博
    sched.add_job(func=crawler_beijing_metro, trigger='cron', hour='10', minute='01', id='crawler_beijing_metro')

    sched.start()


if __name__ == '__main__':
    cron_job()
