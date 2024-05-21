import pandas as pd
from datetime import datetime, timedelta
import numpy as np
import zmail
import os


def send_email(my_subject, my_content):
    msg = {
        'subject': my_subject,
        'content_text': my_content
    }
    server = zmail.server('2285687467@qq.com', 'wzekzueiuxpndida')
    server.send_mail('2285687467@qq.com', msg)


def decline_7workday(now_time):  # 观测有一只币种在跌且振幅大于21%时，后面7日内有无在跌且当天起始价格小于观测日期起始价格的95%
    data_path_00hour = r'.\每天0点数据.csv'
    data_path_all_day = r'.\总表.csv'
    error_path = r'.\7天振幅观测函数异常.csv'
    send_message = ['币种\t\t起始日期\t\t截至日期\n']
    now_time = datetime.strptime(now_time,'%Y-%m-%d %H:%M')
    # 计算出8天前的时间
    start_day_time = now_time - timedelta(days=8)
    print(start_day_time)
    # 第二天的时间
    next_day_time = start_day_time + timedelta(days=1)
    print(next_day_time)
    # 计算出起始日期后面所有日期的时间
    date_7day = [start_day_time + timedelta(days=i) for i in range(1, 9)]
    # print(date_7day)
    # 提取出起始日期到当前时间的数据
    data_00_hour_all = pd.read_csv(data_path_00hour, index_col='时间', usecols=['时间', '币种', 'USDT价格'],
                                   low_memory=False, parse_dates=['时间']).loc[start_day_time:]
    # print(data_00_hour_all)
    # print(data_00_hour_all.info())
    # 删除掉有nan的行
    data_00_hour_all.dropna(inplace=True)
    # 判断天数是否足够
    unique_data_count = data_00_hour_all.index._data
    count_unique_datas = len(np.unique(unique_data_count))
    if count_unique_datas != 9:
        print("数据不足，结束函数")
        return
    start_day_time_data = data_00_hour_all.loc[start_day_time]
    next_dat_time_data = data_00_hour_all.loc[next_day_time]
    # 找出两天中共有的币种
    common_coin_name = start_day_time_data['币种'][start_day_time_data['币种'].isin(next_dat_time_data['币种'])]
    # 比较两天共有币种的价格，找出在跌的币种
    decline_coin_name = []
    for cur_coin_name_in_common in common_coin_name:
        cur_coin_name_in_common_start_day_price = start_day_time_data[start_day_time_data['币种'] == cur_coin_name_in_common]['USDT价格'].values[0]
        cur_coin_name_in_common_next_day_price = next_dat_time_data[next_dat_time_data['币种'] == cur_coin_name_in_common]['USDT价格'].values[0]
        # 判断币种是否在跌，如果在跌就记录下币种
        if cur_coin_name_in_common_start_day_price < cur_coin_name_in_common_next_day_price:
            decline_coin_name.append(cur_coin_name_in_common)
    # 提取起始日期的详细数据，找出最大值、最小值计算振幅是是否超过21
    start_day_detail_data = pd.read_csv(data_path_all_day, low_memory=False, parse_dates=['时间'], index_col='时间',
                                        usecols=['时间', 'USDT价格', '币种']).loc[start_day_time: start_day_time + timedelta(hours=24)]
    swing_than_21_coin_name = []
    for cur_decline_coin_name_in_decline in decline_coin_name:
        try:
            cur_coin_name_all_price_oneday = start_day_detail_data[start_day_detail_data['币种'] == cur_decline_coin_name_in_decline][
                'USDT价格'].values()
            max_price = cur_coin_name_all_price_oneday.max()
            min_price = cur_coin_name_all_price_oneday.min()
            start_price = start_day_time_data[start_day_time_data['币种'] == cur_decline_coin_name_in_decline]['USDT价格'].values[0]
            if (max_price - min_price) / start_price * 100 >= 21:
                # 记录振幅超过21的币种
                swing_than_21_coin_name.append(cur_decline_coin_name_in_decline)
        except Exception as e:
            if os.path.exists(error_path):
                with open(error_path, mode='a', encoding='utf-8') as writer:
                    writer.write(f'{start_day_time}{e}')
            else:
                with open(error_path, mode='w', encoding='utf-8') as writer:
                    writer.write(f'{start_day_time}{e}')
            continue
    # 对每一个振幅超过21的币种进行观测，看它后面7天有没有在跌且价格低于起始日价格95%。
    for cur_coin_name_in_swing in swing_than_21_coin_name:
        for time_index in range(0, 7):
            try:
                sta_cur_coin_price = start_day_time_data[start_day_time_data['币种'] == cur_coin_name_in_swing]['USDT价格'].values[0]
                cur_coin_name_price = data_00_hour_all.loc[date_7day[time_index]][
                    data_00_hour_all.loc[date_7day[time_index], '币种'] == cur_coin_name_in_swing]['USDT价格'].values[0]
                next_coin_name_price = data_00_hour_all.loc[date_7day[time_index + 1]][
                    data_00_hour_all.loc[date_7day[time_index + 1], '币种'] == cur_coin_name_in_swing]['USDT价格'].values[0]
                if cur_coin_name_price < next_coin_name_price and (cur_coin_name_price / sta_cur_coin_price) * 100 <= 95:
                    send_message.append(f'{cur_coin_name_in_swing}\t\t{start_day_time}\t\t{now_time}\n')
            except Exception as e:
                if os.path.exists(error_path):
                    with open(error_path, mode='a', encoding='utf-8') as writer:
                        writer.write(f'{start_day_time}{e}')
                else:
                    with open(error_path, mode='w', encoding='utf-8') as writer:
                        writer.write(f'{start_day_time}{e}')
                continue
    if len(send_message) != 1:
        print(send_message)
        subject = "振幅超过21%，7个工作日内在跌币种价格小于起始日的95%"
        send_email(my_subject=subject, my_content=send_message)


if __name__ == '__main__':
    start_date = '2024-05-08 00:00'
    decline_7workday(start_date)
