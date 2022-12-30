import logging
import logging.handlers
import datetime
import os


def logger_init(log_path=None, log_name='demo_log', log_level=logging.DEBUG, by_day=False, streamHandler=False):
    if log_path is None:
        raise Exception('请指定日志目录')
    try:
        if not os.path.exists(log_path):
            os.makedirs(log_path)
    except PermissionError:
        raise Exception('当前日志目录没有写入权限')
    # if os.access(log_path, os.W_OK) is False:
    #     raise Exception('当前日志目录没有写入权限')
    # 因为有多个进程，所以用时间戳做id, 延迟2s启动，日志名以分钟结尾
    if by_day:
        datetime_str = datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d')
    else:
        datetime_str = datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d_%H:%M:%S')
    log_path = os.path.join(log_path, '{}_{}.log'.format(log_name, datetime_str))
    
    # 创建file_handler
    file_handler = logging.handlers.TimedRotatingFileHandler(
                    filename=log_path,
                    when='midnight',
                    backupCount=20,
                    encoding="utf8",
                )
    formatter = logging.Formatter("%(asctime)s %(name)-10s %(filename)-15s %(lineno)-3s %(levelname)-8s %(message)s")
    file_handler.setFormatter(formatter)
    file_handler.setLevel(log_level)

    logging.root.setLevel(log_level)
    logging.root.addHandler(file_handler)

    if streamHandler:
        sh = logging.StreamHandler()
        sh.setLevel(log_level)
        sh.setFormatter(formatter)
        logging.root.addHandler(sh)


if __name__ == '__main__':
    logger_init(log_path='.', log_name='logger_helper')
    logger = logging.getLogger()
    logger.debug('哈哈')