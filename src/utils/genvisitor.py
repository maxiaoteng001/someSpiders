"""
新浪微博访客cookie生成
"""
import requests
import re
import random


def genvisitor():
    """
    通过注册新的session, 来刷新购物车
    """
    headers = {
        'authority': 'passport.weibo.com',
        'accept': '*/*',
        'accept-language': 'zh-CN,zh;q=0.9',
        'cache-control': 'max-age=0',
        'content-type': 'application/x-www-form-urlencoded',
        # 'cookie': 'PC_TOKEN=15d8b7c4ff',
        'if-modified-since': '0',
        'origin': 'https://passport.weibo.com',
        # 'referer': 'https://passport.weibo.com/visitor/visitor?entry=miniblog&a=enter&url=https%3A%2F%2Fweibo.com%2F2778292197%3Frefer_flag%3D1001030103_&domain=.weibo.com&ua=php-sso_sdk_client-0.6.36&_rand=1673616967.9789',
        'sec-ch-ua': '"Not?A_Brand";v="8", "Chromium";v="108", "Google Chrome";v="108"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"macOS"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36',
    }

    data = 'cb=gen_callback&fp=%7B%22os%22%3A%222%22%2C%22browser%22%3A%22Chrome108%2C0%2C0%2C0%22%2C%22fonts%22%3A%22undefined%22%2C%22screenInfo%22%3A%221440*900*30%22%2C%22plugins%22%3A%22Portable%20Document%20Format%3A%3Ainternal-pdf-viewer%3A%3APDF%20Viewer%7CPortable%20Document%20Format%3A%3Ainternal-pdf-viewer%3A%3AChrome%20PDF%20Viewer%7CPortable%20Document%20Format%3A%3Ainternal-pdf-viewer%3A%3AChromium%20PDF%20Viewer%7CPortable%20Document%20Format%3A%3Ainternal-pdf-viewer%3A%3AMicrosoft%20Edge%20PDF%20Viewer%7CPortable%20Document%20Format%3A%3Ainternal-pdf-viewer%3A%3AWebKit%20built-in%20PDF%22%7D'

    response = requests.post('https://passport.weibo.com/visitor/genvisitor', headers=headers, data=data, verify=False)
    result = {
        'tid': re.findall('"tid":"(\S+?)"', response.text)[0]
    }
    return result

def passport_vistor(tid_info):
    """
    cookie 和params 需要tid
    """
    cookies = {
        'tid': '{}__095'.format(tid_info.get('tid')),
        # 'PC_TOKEN': 'cd527625d0',
    }

    headers = {
        'Host': 'passport.weibo.com',
        # 'Cookie': 'tid=yCozPJgCU4r3STFMWkLpCgjvdeMdWtcOjaWoke4jsGo=__095; PC_TOKEN=cd527625d0',
        'sec-ch-ua': '"Not?A_Brand";v="8", "Chromium";v="108", "Google Chrome";v="108"',
        'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36',
        # 'referer': 'https://passport.weibo.com/visitor/visitor?entry=miniblog&a=enter&url=https%3A%2F%2Fweibo.com%2F2778292197&domain=.weibo.com&ua=php-sso_sdk_client-0.6.36&_rand=1673617935.8021',
        'accept-language': 'zh-CN,zh;q=0.9',
        'pragma': 'no-cache',
        'cache-control': 'no-cache',
    }

    params = {
        'a': 'incarnate',
        't': tid_info.get('tid'),
        'w': '2',
        'c': '095',
        'gc': '',
        'cb': 'cross_domain',
        'from': 'weibo',
        '_rand': random.random(),
    }

    response = requests.get('https://passport.weibo.com/visitor/visitor', params=params, cookies=cookies, headers=headers, verify=False)
    result = {
        'sub': re.findall('"sub":"(\S+?)"', response.text)[0],
        'subp': re.findall('"subp":"(\S+?)"', response.text)[0],
    }
    return result


def login_vistor(passport_info):
    """
    需要passport_vistor 的sub, subp
    """
    headers = {
        'Host': 'login.sina.com.cn',
        'sec-ch-ua': '"Not?A_Brand";v="8", "Chromium";v="108", "Google Chrome";v="108"',
        'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36',
        'sec-ch-ua-platform': '"macOS"',
        'accept': '*/*',
        'sec-fetch-site': 'cross-site',
        'sec-fetch-mode': 'no-cors',
        'sec-fetch-dest': 'script',
        'referer': 'https://passport.weibo.com/',
        'accept-language': 'zh-CN,zh;q=0.9',
        'pragma': 'no-cache',
        'cache-control': 'no-cache',
    }

    params = {
        'a': 'crossdomain',
        'cb': 'return_back',
        's': passport_info.get('sub'),
        'sp': passport_info.get('subp'),
        'from': 'weibo',
        '_rand': random.random(),
        'entry': 'miniblog',
    }

    response = requests.get('https://login.sina.com.cn/visitor/visitor', params=params, headers=headers, verify=False)
    if response.status_code == 200:
        # 追踪的cookie需要大些
        cookies = {
            'SUB': passport_info.get('sub'),
            'SUBP': passport_info.get('subp'),
        }
        return cookies
    else:
        return None


def gen_visitor_info():
    for _ in range(5):
        tid_info = genvisitor()
        passport_info = passport_vistor(tid_info=tid_info)
        cookies = login_vistor(passport_info=passport_info)
        if cookies:
            return cookies
    return {}


if __name__ == '__main__':
    cookies = gen_visitor_info()
    print(cookies)
