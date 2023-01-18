# -*- coding: utf-8 -*-
import subprocess
from apscheduler.schedulers.blocking import BlockingScheduler
import datetime
import os
import sys
BASE_DIR = os.path.join(os.path.abspath(os.path.dirname(__file__)), '..')
sys.path.append(BASE_DIR)
from utils import *


def crawler_beike_ershoufang():
    por = subprocess.Popen('cd ../spiders && python3 beike_ershoufang.py &', shell=True, stdout=None, executable='/bin/bash')
    por.wait()


def crawler_beijing_metro():
    spider_name = "beijing_metro_weibo"
    log_path = f'/code/logs/{spider_name}_{datetime.datetime.strftime(datetime.datetime.now(), "%Y-%m-%d_%H_%M_%S")}.log'
    por = subprocess.Popen(f'cd ../some_scrapy && scrapy crawl {spider_name} --logfile {log_path} >/dev/null 2>&1', shell=True, stdout=None, executable='/bin/bash')
    por.wait()


def crawler_tesla_used():
    spider_name = "tesla_used"
    log_path = f'/code/logs/{spider_name}_{datetime.datetime.strftime(datetime.datetime.now(), "%Y-%m-%d_%H_%M_%S")}.log'
    por = subprocess.Popen(f'cd ../some_scrapy && scrapy crawl {spider_name} --logfile {log_path} >/dev/null 2>&1', shell=True, stdout=None, executable='/bin/bash')
    por.wait()


def cron_job():
    sched = BlockingScheduler()
    # 每日更新热门城市地铁最新微博
    sched.add_job(func=crawler_beijing_metro, trigger='cron', hour='12', minute='01', id='crawler_beijing_metro')

    # 贝壳二手房
    sched.add_job(func=crawler_beike_ershoufang, trigger='cron', hour='7', minute='01', id='crawler_beike_ershoufang')
    
    sched.add_job(func=crawler_tesla_used, trigger='cron', hour='*', minute='05', id='crawler_tesla_used')
    
    sched.start()


if __name__ == '__main__':
    cron_job()
