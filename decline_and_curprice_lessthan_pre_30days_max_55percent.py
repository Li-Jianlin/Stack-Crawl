import os.path
import pandas as pd
from datetime import datetime, timedelta
import zmail
import numpy as np
import traceback


def send_email(my_subject, my_content):
    msg = {
        'subject': my_subject,
        'content_text': my_content
    }
    server = zmail.server('2285687467@qq.com', 'wzekzueiuxpndida')
    server.send_mail('2285687467@qq.com', msg)


def decline_and_curprice_lessthan_pre_30days_max_55percent(now_time):
    data_path = r".\30天测试数据.csv"
    errors_file_path = r'.\30天观测函数异常.csv'
    send_message = ['币种\t\t观测时间\t\t价格最大时间\n']
    # 前一天日期
    now_time = datetime.strptime(now_time, '%Y-%m-%d %H:%M')
    # pre_day_time = datetime.strptime(now_time,'%Y-%m-%d %H:%M') - timedelta(days=1)
    pre_day_time = now_time - timedelta(days= 1)
    # 前一天的30天前起始日期
    start_day_time = pre_day_time - timedelta(days=30)
    # print('now', now_time, 'pre_day', pre_day_time, 'start', start_day_time)
    # 计算出从30天前日期开始后面30天的时间列表
    all_30days_time = [start_day_time + timedelta(days=i) for i in range(30)]
    # print('all_30days_time', all_30days_time)
    # 读取从30天前到当天的所有数据，以时间为行索引，只读取时间、USDT价格、币种名
    try:
        data_32days = pd.read_csv(data_path, low_memory=False, index_col='时间', usecols=['时间', '币种', 'USDT价格'],
                                  parse_dates=['时间'], date_format='%Y-%m-%d %H:%M').loc[start_day_time:now_time]
        print(data_32days)
        print(data_32days.info())
    except Exception as message_error:
        if os.path.exists(errors_file_path):
            with open(errors_file_path, mode='a', encoding='utf-8') as file_writer:
                file_writer.write(f'操作时间{now_time}:{pre_day_time}{message_error}\n')
        else:
            with open(errors_file_path, mode='w', encoding='utf-8') as file_writer:
                file_writer.write(f'操作时间{now_time}:{pre_day_time}{message_error}\n')
    # 判断数据是否有32天，如果没有就说明数据不足，结束函数
    unique_datas_count = data_32days.index._data
    count_unique_datas = len(np.unique(unique_datas_count))
    if count_unique_datas < 32:
        print("数据不足32天，结束函数")
        return
    # 数据没有缺天数，就提取出当天和前一天的共同币种
    cur_coin_name = data_32days.loc[now_time]['币种']
    pre_coin_name = data_32days.loc[pre_day_time]['币种']
    common_coin_name = pre_coin_name[pre_coin_name.isin(cur_coin_name)]
    # 计算每个币种的跌涨幅，判断其是否在跌.前一天价格小于当天价格就在跌，进行后续步骤
    for coin_name in common_coin_name:
        try:
            cur_price_float = data_32days.loc[now_time][data_32days.loc[now_time, '币种'] == coin_name]['USDT价格'].values[0]
            pre_price_float = data_32days.loc[pre_day_time][data_32days.loc[pre_day_time, '币种'] == coin_name]['USDT价格'].values[0]
            if pre_price_float < cur_price_float:
                print(coin_name)
                print('cur',cur_price_float,'pre',pre_price_float)
                pre_data_in_all = data_32days.loc[all_30days_time[0]:all_30days_time[29],'USDT价格'][data_32days.loc[all_30days_time[0]:all_30days_time[29],'币种'] == coin_name]
                print(pre_data_in_all)
                pre_30_days_max_price_index = pre_data_in_all.idxmax()
                print(pre_30_days_max_price_index)
                pre_30_days_max_price = pre_data_in_all.loc[pre_30_days_max_price_index]
                print(pre_30_days_max_price)
                # 在跌就提取出前面30天的数据，找出价格最大那一天
                if (pre_price_float / pre_30_days_max_price) * 100 < 55:
                    send_message.append(f'{coin_name}\t\t{pre_day_time}\t\t{pre_30_days_max_price_index}\n')
                    print(send_message)
        except Exception as error_message:
            if os.path.exists(errors_file_path):
                with open(errors_file_path, mode='a', encoding='utf-8') as file_writer:
                    file_writer.write(f'操作时间{now_time}:{pre_day_time}{error_message}\n')
            else:
                with open(errors_file_path, mode='w', encoding='utf-8') as file_writer:
                    file_writer.write(f'操作时间{now_time}:{pre_day_time}{error_message}\n')
    if len(send_message) != 1:
        subject = "某个币种和近30天最高价格的比值 --李建林"
        send_email(my_subject=subject, my_content=send_message)


if __name__ == "__main__":
    decline_and_curprice_lessthan_pre_30days_max_55percent('2024-05-15 00:00')
