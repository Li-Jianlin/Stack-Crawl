import math
import os
import random
import re
import time
import urllib.request
import requests
from lxml import etree
import urllib.parse
import pandas as pd
from datetime import datetime, timedelta


def get_data_01(i):  # 使用网站一爬取数据，返回爬取到的网页数据
    url = 'https://www.528btc.com/e/extend/api/index.php?m=v2&c=coinlist'
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36 Edg/124.0.0.0',
        'Cookie': '__51vcke__3ExGyQaAoNSqsSUY=caeaa7e4-9ecf-5a7b-bea7-3f2060b306a2; __51vuft__3ExGyQaAoNSqsSUY=1713795198634; __51uvsct__3ExGyQaAoNSqsSUY=2; __vtins__3ExGyQaAoNSqsSUY=%7B%22sid%22%3A%20%2292fbd405-db2b-5d8e-bc74-b7bf87d4899c%22%2C%20%22vd%22%3A%202%2C%20%22stt%22%3A%2026746%2C%20%22dr%22%3A%2026746%2C%20%22expires%22%3A%201713879252152%2C%20%22ct%22%3A%201713877452152%7D',
        'Origin': 'https://www.528btc.com',
        'path': '/e/extend/api/index.php?m=v2&c=coinlist'
    }
    data = {  # 一次爬取20条数据
        'm': 'v2',
        'c': 'coinlist',
        'page': str(i)
    }

    data = urllib.parse.urlencode(data).encode('utf-8')
    request = urllib.request.Request(url=url, headers=headers, data=data)
    response = urllib.request.urlopen(request, timeout=10)
    content = response.read().decode('utf-8')
    return content


def get_data_02(index):  # 使用网站2爬取数据，返回爬取到的网页数据
    url = r"https://www.bi123.co/crypto-web/open/markets/spot/list"
    headers = {
        "Accept": "application/json",
        "Accept-Encoding": "gzip, deflate, br, zstd",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
        "Authorization": "eyJhbGciOiJIUzUxMiJ9.eyJleHAiOjE3MTY5NTkzNzEsInN1YiI6IntcImlkXCI6NDE4NSxcInVzZXJuYW1lXCI6XCIyMjg1Njg3NDY3QHFxLmNvbVwiLFwibmlja25hbWVcIjpcIjIyOCoqKiouY29tXCIsXCJwaG90b1VybFwiOlwiXCIsXCJkZXZpY2VOb1wiOm51bGwsXCJ1dWlkXCI6XCI1MjUxZTJmYzYxYWQ0NzI4YTk0NjI4Y2FjODNhOTY0YVwifSIsImNyZWF0ZWQiOjE3MTQzNjczNzEyNzJ9.ay2uCwigxVVEeZQpiVHr9dr20XnD6ywpMTTgAcXzSu0TWL_neZUEgxZZ87EeTekJc4mVZ6DJ45_CvHiFpFYXkQ",
        "Connection": "keep-alive",
        "Content-Length": "291",
        "Content-Type": "application/json;charset=UTF-8",
        "Cookie": "token=eyJhbGciOiJIUzUxMiJ9.eyJleHAiOjE3MTY5NTkzNzEsInN1YiI6IntcImlkXCI6NDE4NSxcInVzZXJuYW1lXCI6XCIyMjg1Njg3NDY3QHFxLmNvbVwiLFwibmlja25hbWVcIjpcIjIyOCoqKiouY29tXCIsXCJwaG90b1VybFwiOlwiXCIsXCJkZXZpY2VOb1wiOm51bGwsXCJ1dWlkXCI6XCI1MjUxZTJmYzYxYWQ0NzI4YTk0NjI4Y2FjODNhOTY0YVwifSIsImNyZWF0ZWQiOjE3MTQzNjczNzEyNzJ9.ay2uCwigxVVEeZQpiVHr9dr20XnD6ywpMTTgAcXzSu0TWL_neZUEgxZZ87EeTekJc4mVZ6DJ45_CvHiFpFYXkQ",
        "Host": "www.bi123.co",
        "Locale": "zh-CN",
        "Origin": "https://www.bi123.co",
        "Referer": "https://www.bi123.co/markets/spot",
        "Sec-Ch-Ua": r"\"Chromium\";v=\"124\", \"Microsoft Edge\";v=\"124\", \"Not-A.Brand\";v=\"99\"",
        "Sec-Ch-Ua-Mobile": "?0",
        "Sec-Ch-Ua-Platform": r"\"Windows\"",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36 Edg/124.0.0.0"
    }

    data = {  # 一次50个数据
        "current": index,
        "size": 50,
        "isAll": 'true',
        "sortName": "marketCap",
        "sortOrder": "desc",
        "symbol": "",
        "searchText": "",
        "isSubs": 'false',
        "tagsId": "",
        "groupId": 4758,
        "isSelect": 'false',
        "plate": "",
        "index": "",
        "plates": [],
        "metrics": [],
        "baseAsset": "",
        "quoteAsset": "",
        "contractType": "",
        "periodType": "",
        "exchange": ""
    }
    response = requests.post(url=url, json=data, headers=headers, timeout=10)
    data_json = response.json()
    return data_json


def data_parse_01(content, time_date_cn):  # 对网站一爬取的数据进行提取
    tree = etree.HTML(content)
    name = []
    price = []
    USDT_price = []
    percent_day = []
    total_price = []
    trade_volume_24hour = []  # 交易量
    circulating_supply = []  # 流通供应量
    star_index = 1
    end_index = 21
    for index in range(star_index, end_index):
        name_chinese = tree.xpath(f'//tr[{index}]/td[2]//div[@class="detail"]/text()')[0].strip()
        name_english = re.sub(r'[^\x00-\x7F]+', '', name_chinese)
        name.append(name_english)
        # price.append(tree.xpath(f'//tr[{index}]/td[3]/text()')[0].strip())
        USDT_price.append(
            float((tree.xpath(f'//tr[{index}]/td[4]/text()')[0].strip()).replace('$', '').replace(',', '')))

        total_price.append(tree.xpath(f'//tr[{index}]/td[6]/text()')[0].strip())
        trade_volume_24hour.append(tree.xpath(f'//tr[{index}]/td[7]/text()')[0].strip())
        circulating_supply.append(tree.xpath(f'//tr[{index}]/td[8]/text()')[0].strip())
    data_dict = {
        '币种': name,
        # '价格': price,
        'USDT价格': USDT_price,
        '市值': total_price,
        '交易量（24小时）': trade_volume_24hour,
        '流通供应量': circulating_supply,
        '时间': time_date_cn
    }

    return data_dict


def data_parse_02(data_json, time_date_cn):  # 对网站2爬取的数据进行提取
    records = data_json['data']['records']
    time_cur = time.strftime("%Y-%m-%d %H:%M")
    data_dict = {
        "币种": [],
        "USDT价格": [],
        "市值": [],
        "交易量（24小时）": [],
        "流通供应量": [],
        "时间": time_date_cn
    }
    for record in records:
        data_dict['币种'].append(record['symbol'])
        data_dict["USDT价格"].append(float(record['price']))
        data_dict["交易量（24小时）"].append(record["totalVolume"])
        data_dict["流通供应量"].append(record["totalSupply"])
        data_dict["市值"].append(record["marketCap"])
    df_data = pd.DataFrame(data_dict)
    df_data['时间'] = pd.to_datetime(df_data['时间'])
    return df_data



