import os.path
import pandas as pd
from datetime import datetime, timedelta
import zmail
import numpy as np


def send_email(my_subject, my_content):
    msg = {
        'subject': my_subject,
        'content_text': my_content
    }
    server = zmail.server('2285687467@qq.com', 'wzekzueiuxpndida')
    server.send_mail('2285687467@qq.com', msg)


def decline_and_curprice_lessthan_pre_30days_max_55percent(area):
    if area == 'cn':
        file_path_everyday_data = r'.\中国每天跌涨幅.csv'
    else:
        file_path_everyday_data = r'.\美国每天跌涨幅.csv'
    send_message = ['币种\t\t观测时间\t\t价格最大时间\n']
    # 提取数据
    data_all_day = pd.read_csv(file_path_everyday_data,low_memory=False, index_col='时间',usecols=['币种','USDT价格','时间','每天跌涨幅'])
    # print(data_all_day)
    unique_index = data_all_day.index.unique().tolist()
    print(len(unique_index))
    if len(unique_index) < 31:
        print('数据不足')
        return
    # 找出在跌的币种
    cur_time = unique_index[-1]
    start_time = unique_index[-31]
    end_time = unique_index[-2]
    # print('cur',cur_time,'start',start_time,'end_time',end_time)
    cur_data = data_all_day.loc[cur_time]
    # print('cur_data',cur_data)
    decline_coin_name = cur_data[cur_data['每天跌涨幅'] < 0]['币种']
    # print('decline_coin',decline_coin_name)
    all_30day_data = data_all_day.loc[start_time:end_time]
    # print(all_30day_data)
    for cur_coin in decline_coin_name:
        try:
            cur_day_price = data_all_day.loc[cur_time][data_all_day.loc[cur_time, '币种'] == cur_coin]['USDT价格'].values[0]
            cur_coin_price_data = all_30day_data[all_30day_data['币种'] == cur_coin]['USDT价格']
            # print('cur_day_price',cur_day_price)
            max_price_index = cur_coin_price_data.idxmax()
            max_price = cur_coin_price_data.loc[max_price_index]
            # print('max_index',max_price_index,'max_price',max_price)
            if (cur_day_price / max_price) * 100 < 55:
                # print((cur_day_price / max_price) * 100)
                send_message.append(f'{cur_coin}\t\t{cur_time}\t\t{max_price_index}\n')
        except Exception as e:
            print(e)
            continue
    if len(send_message) != 1:
        subject = "某个币种和近30天最高价格的比值 --李建林"
        send_email(my_subject=subject, my_content=send_message)


if __name__ == "__main__":
    decline_and_curprice_lessthan_pre_30days_max_55percent('cn')
