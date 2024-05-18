import os
import time
import traceback

import numpy as np
import zmail
import pandas as pd
from datetime import datetime, timedelta


def send_email(my_subject, my_content):
    msg = {
        'subject': my_subject,
        'content_text': my_content
    }

    server = zmail.server('2285687467@qq.com', 'wzekzueiuxpndida')
    server.send_mail('2285687467@qq.com', msg)


def observe_price_movement(now_time):
    # now_time = '2024-05-02 10:00'
    start_time = datetime.strptime(now_time, '%Y-%m-%d %H:%M') - timedelta(hours=9)
    file_path = r'.\每小时整点数据.csv'
    file_error_file_csv = r'.\9小时观测异常.csv'
    send_email_dict = []
    # 计算出开始和后面9个小时的时间
    print('start_time', start_time)
    all_hours_time_datetime = [start_time + timedelta(hours=i) for i in range(10)]
    send_email_dict.append("币种\t\t跌幅\t\t\t\t时间\n")
    # 从csv文件中读取传入开始的时间和后面9小时的数据，将时间列指定为索引，并且只读取时间、价格、币种三列数据
    try:
        data_df_all_ten_hours = pd.read_csv(file_path, index_col='时间', parse_dates=['时间'],
                                            usecols=['币种', 'USDT价格', '时间'], low_memory=False,
                                            ).loc[start_time:all_hours_time_datetime[9]]
        # print(data_df_all_ten_hours.info())
        # print(data_df_all_ten_hours)
        unique_data_count = data_df_all_ten_hours.index._data
        # print(unique_data_count)
        count_unique_datas = len(np.unique(unique_data_count))
        # print(count_unique_datas)
        if count_unique_datas != 10:
            return
        # 统计出当前时间和后面一个小时内都有的币种

        cur_data_coins_name = data_df_all_ten_hours.loc[start_time]['币种']
        # print(cur_data_coins_name)
        next_data_coins_name = data_df_all_ten_hours.loc[all_hours_time_datetime[1]]['币种']
        # print(next_data_coins_name)
        common_coins_name = cur_data_coins_name[cur_data_coins_name.isin(next_data_coins_name)]

    except KeyError as key_error:

        print(f'缺失数据：{key_error}')
        if os.path.exists(file_error_file_csv):
            with open(file_error_file_csv, 'a', encoding='utf-8') as file_error:
                file_error.write(f'{start_time}:缺失数据：{key_error}\n')
        else:
            with open(file_error_file_csv, 'w', encoding='utf-8') as file_error:
                file_error.write(f'{start_time}:缺失数据：{key_error}\n')
        return
    common_coin_file_path = r'.\9小时观测起点及币种.csv'
    if os.path.exists(common_coin_file_path):
        common_coins_name.to_csv(common_coin_file_path, mode='a', index=True, header=False)
    else:
        common_coins_name.to_csv(common_coin_file_path, mode='w', index=True, header=True)
    # print(common_coins_name)
    # 计算每个币种的增幅，如果在增且增幅大于4.5，就进行下一步判断

    # print(all_hours_time_str)
    for cur_coin_name in common_coins_name:
        try:
            cur_price_float = data_df_all_ten_hours.loc[start_time][
                data_df_all_ten_hours.loc[start_time, '币种'] == cur_coin_name]['USDT价格'].values[
                0]
            next_price_float = data_df_all_ten_hours.loc[all_hours_time_datetime[1]][
                data_df_all_ten_hours.loc[all_hours_time_datetime[1], '币种'] == cur_coin_name][
                'USDT价格'].values[0]
            percent = (next_price_float - cur_price_float) / cur_price_float * 100
            # print('cur_price', cur_price_float, 'next_price', next_price_float, 'percent', percent)
        except Exception as error_message:
            if os.path.exists(file_error_file_csv):
                with open(file_error_file_csv, mode='a', encoding='utf-8') as file_writer:
                    file_writer.write(
                        f'计算跌涨幅>4.5%函数处：时间{start_time}币种{cur_coin_name}出现{error_message}异常\n')
            else:
                with open(file_error_file_csv, mode='w', encoding='utf-8') as file_writer:
                    file_writer.write(
                        f'计算跌涨幅>4.5%函数处：时间{start_time}币种{cur_coin_name}出现{error_message}异常\n')
            continue
        # 增幅大于4.5%，就对后面9个小时进行观测
        if percent >= 4.5:
            sum_price = 0
            try:
                for time_index in range(2, len(all_hours_time_datetime)):
                    # 依次比较该币种当前价格与起始时间的价格大小
                    curr_price = float(
                        data_df_all_ten_hours.loc[all_hours_time_datetime[time_index]][data_df_all_ten_hours.loc[
                                                                                           all_hours_time_datetime[
                                                                                               time_index], '币种'] == cur_coin_name][
                            'USDT价格'].values[0])
                    pre_price = float(data_df_all_ten_hours.loc[all_hours_time_datetime[time_index - 1]][
                                          data_df_all_ten_hours.loc[
                                              all_hours_time_datetime[time_index - 1], '币种'] == cur_coin_name][
                                          'USDT价格'].values[0])
                    print('curr', curr_price, 'pre', pre_price)
                    # 前一次价格大于起始时间价格，当前价格小于起始时间价格，以起始价格来计算跌涨幅
                    if pre_price > cur_price_float and curr_price < cur_price_float:
                        percent_curr = (curr_price - cur_price_float) / cur_price_float * 100
                        sum_price += percent_curr
                        if sum_price <= -3.5:
                            send_email_dict.append(
                                f"{cur_coin_name}\t\t{sum_price:.3f}%\t{all_hours_time_datetime[0]}~~{all_hours_time_datetime[time_index]}\n")
                            break
                    elif pre_price < cur_price_float and curr_price < cur_price_float:
                        percent_curr = (curr_price - pre_price) / pre_price * 100
                        sum_price += percent_curr
                        if sum_price <= -3.5:
                            send_email_dict.append(
                                f"{cur_coin_name}\t\t{sum_price:.3f}%\t{all_hours_time_datetime[0]}~~{all_hours_time_datetime[time_index]}\n")
                            break
            except Exception as e:
                if os.path.exists(file_error_file_csv):
                    with open(file_error_file_csv, mode='a', encoding='utf-8') as file_writer:
                        file_writer.write(
                            f'计算价格小于标准值的跌幅代码处：时间{all_hours_time_datetime[time_index]}币种{cur_coin_name}出现{e}异常\n')
                else:
                    with open(file_error_file_csv, mode='w', encoding='utf-8') as file_writer:
                        file_writer.write(
                            f'计算价格小于标准值的跌幅代码处：时间{all_hours_time_datetime[time_index]}币种{cur_coin_name}出现{e}异常\n')
            print(send_email_dict)
    if len(send_email_dict) != 1:
        subject = "股票增幅超过4.5%时，后面九小时有跌幅超过-3.5%--李建林"
        print(send_email_dict)
        send_email(my_subject=subject, my_content=send_email_dict)


if __name__ == "__main__":
    observe_price_movement('2024-05-11 22:00')
