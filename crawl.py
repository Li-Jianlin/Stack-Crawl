import math
import os
import random
import re
import time
import urllib.request
import requests
import zmail
from lxml import etree
import urllib.parse
import pandas as pd
from datetime import datetime, timedelta

from decline_swing21_7workdays import decline_7workday
from observe_ninehours_price_movement import observe_price_movement
from decline_and_curprice_lessthan_pre_30days_max_55percent import \
    decline_and_curprice_lessthan_pre_30days_max_55percent


def crawl_data_01(i):
    url = 'https://www.528btc.com/e/extend/api/index.php?m=v2&c=coinlist'
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36 Edg/124.0.0.0',
        'Cookie': '__51vcke__3ExGyQaAoNSqsSUY=caeaa7e4-9ecf-5a7b-bea7-3f2060b306a2; __51vuft__3ExGyQaAoNSqsSUY=1713795198634; __51uvsct__3ExGyQaAoNSqsSUY=2; __vtins__3ExGyQaAoNSqsSUY=%7B%22sid%22%3A%20%2292fbd405-db2b-5d8e-bc74-b7bf87d4899c%22%2C%20%22vd%22%3A%202%2C%20%22stt%22%3A%2026746%2C%20%22dr%22%3A%2026746%2C%20%22expires%22%3A%201713879252152%2C%20%22ct%22%3A%201713877452152%7D',
        'Origin': 'https://www.528btc.com',
        'path': '/e/extend/api/index.php?m=v2&c=coinlist'
    }
    data = {
        'm': 'v2',
        'c': 'coinlist',
        'page': str(i)
    }

    data = urllib.parse.urlencode(data).encode('utf-8')
    request = urllib.request.Request(url=url, headers=headers, data=data)
    response = urllib.request.urlopen(request, timeout=10)
    content = response.read().decode('utf-8')
    return content


def crawl_data_02(index):
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

    data = {
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


def data_parse_01(content, time_str):
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
        percent_day_text = tree.xpath(f'//tr[{index}]/td[5]/div[1]/text()')[0]
        percent_day_cleaned = re.sub(r'\s+', '', percent_day_text)
        percent_day.append(percent_day_cleaned)
        total_price.append(tree.xpath(f'//tr[{index}]/td[6]/text()')[0].strip())
        trade_volume_24hour.append(tree.xpath(f'//tr[{index}]/td[7]/text()')[0].strip())
        circulating_supply.append(tree.xpath(f'//tr[{index}]/td[8]/text()')[0].strip())
    data_dict = {
        '币种': name,
        # '价格': price,
        'USDT价格': USDT_price,
        '24小时跌涨幅': percent_day,
        '市值': total_price,
        '交易量（24小时）': trade_volume_24hour,
        '流通供应量': circulating_supply,
        '时间': time_str
    }
    return data_dict


def data_parse_02(data_json, time_str):
    records = data_json['data']['records']
    time_cur = time.strftime("%Y-%m-%d_%H-%M")
    data_dict = {
        "币种": [],
        "USDT价格": [],
        "24小时跌涨幅": [],
        "市值": [],
        "交易量（24小时）": [],
        "流通供应量": [],
        "时间": time_str
    }
    for record in records:
        data_dict['币种'].append(record['symbol'])
        data_dict["USDT价格"].append(float(record['price']))
        data_dict["24小时跌涨幅"].append(str(record["priceDayChange"]) + "%")
        data_dict["交易量（24小时）"].append(record["totalVolume"])
        data_dict["流通供应量"].append(record["totalSupply"])
        data_dict["市值"].append(record["marketCap"])
    df_data = pd.DataFrame(data_dict)
    return df_data


def deposit_file(df_combined):
    file_name_total_data = r'.\总表.csv'
    if os.path.exists(file_name_total_data):
        df_combined.to_csv(file_name_total_data, mode='a', encoding='utf-8', index=False, header=False)
        print('写入完成')
    else:
        df_combined.to_csv(file_name_total_data, mode='w', encoding='utf-8', index=False, header=True)
        print('写入完成')


def deposit_file_per_hour(df_combined_data):
    cur_file_path_per_hour = r".\每小时整点数据.csv"
    if os.path.exists(cur_file_path_per_hour):
        df_combined_data.to_csv(cur_file_path_per_hour, mode='a', encoding='utf-8', index=False, header=False)
    else:
        df_combined_data.to_csv(cur_file_path_per_hour, mode='w', encoding='utf-8', index=False, header=True)


def deposit_file_per_day_start(df_combined_data):
    cur_file_path_per_day = r".\每天0点数据.csv"
    if os.path.exists(cur_file_path_per_day):
        df_combined_data.to_csv(cur_file_path_per_day, mode='a', encoding='utf-8', index=False, header=False)
    else:
        df_combined_data.to_csv(cur_file_path_per_day, mode='w', encoding='utf-8', index=False, header=True)


def send_email(my_subject, my_content):
    msg = {
        'subject': my_subject,
        'content_text': my_content
    }
    # '3145971793@qq.com'
    server = zmail.server('2285687467@qq.com', 'wzekzueiuxpndida')
    server.send_mail(['2285687467@qq.com'], msg)


if __name__ == '__main__':
    file_path_crawl_error = r'.\爬取异常.csv'
    amount = int(input('你要爬取多少种货币数据？'))
    amount_9hours = 0
    amount_30days = 0
    amount_7workdays = 0
    use_crawl_url1 = True
    crawl_amount_url2 = 0
    while True:
        try:
            time_now_minutes = time.strftime('%M')
            time_hour = time.strftime('%H')
            df_combined = pd.DataFrame()
            time_str = time.strftime('%Y-%m-%d %H:%M')
            if int(time_now_minutes) % 5 == 0:
                try:
                    if use_crawl_url1:
                        print("开始爬取")
                        time_str = time.strftime('%Y-%m-%d %H:%M')
                        page = math.ceil(amount / 20)
                        for index in range(1, page + 1):
                            print(f"爬取第{index}页")
                            content = crawl_data_01(index)
                            data_dict = data_parse_01(content, time_str)
                            df_data = pd.DataFrame(data_dict)
                            df_combined = pd.concat([df_combined, df_data], ignore_index=True)
                    else:
                        raise Exception('弃用网址1，使用网址2')
                except Exception as e:
                    df_combined.drop(df_combined.index, inplace=True)
                    if os.path.exists(file_path_crawl_error):
                        with open(file_path_crawl_error, mode='a', encoding='utf-8') as writer:
                            writer.write(f'{time_str}:{e}\n')
                    else:
                        with open(file_path_crawl_error, mode='w', encoding='utf-8') as writer:
                            writer.write(f'{time_str}:{e}\n')
                    use_crawl_url1 = False
                    crawl_amount_url2 += 1
                    print(e)
                    print("使用备用网站开始爬取")
                    if crawl_amount_url2 == 290:
                        use_crawl_url1 = True
                        crawl_amount_url2 = 0
                    page = math.ceil(amount / 50)
                    df_combined.drop(df_combined.index, inplace=True)
                    for index in range(1, page + 1):
                        print(f"正在爬取第{index}页")
                        data_json = crawl_data_02(index)
                        df_data = data_parse_02(data_json, time_str)
                        df_combined = pd.concat([df_combined, df_data], ignore_index=True)
                deposit_file(df_combined=df_combined)
                if time_now_minutes == '00':
                    # 写入数据
                    deposit_file_per_hour(df_combined_data=df_combined)
                    # 进行前面10个小时的数据观测
                    observe_price_movement(time_str)
                if time_hour == '00' and time_now_minutes == '00':
                    decline_and_curprice_lessthan_pre_30days_max_55percent(time_str)
                    deposit_file_per_day_start(df_combined_data=df_combined)
                print('该次爬取已完成')
                while int(time.strftime('%M')) % 5 == 0:
                    print('休眠60秒')
                    time.sleep(60)
        except Exception as e:
            print(e)
            time_sleep_error = random.random() * (random.randint(50,60))
            time.sleep(time_sleep_error)
