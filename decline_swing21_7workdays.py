import pandas as pd
from datetime import datetime, timedelta
from send_email import send_email_test


def decline_7workday(area):  # 观测有一只币种在跌且振幅大于21%时，后面7日内有无在跌且当天起始价格小于观测日期起始价格的95%
    if area == 'cn':
        file_path_cur_day = r'.\中国每天跌涨幅.csv'
        file_path_all_data = r'.\中国总表.csv'
    else:
        file_path_cur_day = r'.\美国每天跌涨幅.csv'
        file_path_all_data = r'.\美国总表.csv'
    send_message = ['币种\t\t起始日期\t\t截至日期\n']
    # 提取每天0点数据
    all_day_data = pd.read_csv(file_path_cur_day, low_memory=False, index_col='时间', usecols=['币种', 'USDT价格', '时间', '每天跌涨幅'])
    # 判断数据是否充足
    all_day_data.index = pd.to_datetime(all_day_data.index)
    unique_index = all_day_data.index.unique().sort_values()
    time_diff = unique_index.to_series().diff().dropna()
    all_diff_is_day = all(time_diff == pd.Timedelta(days=1))
    if not all_diff_is_day:
        print('缺少某一天数据')
        return
    if len(unique_index) < 8:
        print('数据不足')
        return
    # 找出在跌币种
    cur_day_data = all_day_data.loc[unique_index[-8]]
    decline_coin_cur_day = cur_day_data[cur_day_data['每天跌涨幅'] < 0]['币种']
    print('币种',decline_coin_cur_day)
    # 计算每个币种当天的振幅,找出符合条件的币种
    end_day_time = str(datetime.strptime(unique_index[-8], '%Y-%m-%d %H:%M:%S') + timedelta(hours=24))
    print(end_day_time)
    cur_detail_data = pd.read_csv(file_path_all_data, low_memory=False, index_col='时间', usecols=['币种', 'USDT价格', '时间']).loc[unique_index[-8] : end_day_time]
    print(cur_detail_data)
    coin_need_observe = []
    for cur_decline_coin_name in decline_coin_cur_day:
        max_price = cur_detail_data[cur_detail_data['币种'] == cur_decline_coin_name]['USDT价格'].max()
        min_price = cur_detail_data[cur_detail_data['币种'] == cur_decline_coin_name]['USDT价格'].min()
        start_price = cur_detail_data[cur_detail_data['币种'] == cur_decline_coin_name]['USDT价格'].values[0]
        # print('max',max_price,'min',min_price,'start',start_price)
        # print('swing',(max_price - min_price) / start_price * 100)
        if (max_price - min_price) / start_price * 100 >= 21:
            coin_need_observe.append(cur_decline_coin_name)
    print('coin',coin_need_observe)
    # 提取出7天的数据
    next_7day_data = all_day_data.loc[unique_index[-7]:unique_index[-1]]
    print(next_7day_data)
    # 对每个符合条件的币种进行后续7天的观测
    for cur_coin_name in coin_need_observe:
        start_price = cur_day_data[cur_day_data['币种'] == cur_coin_name]['USDT价格'].values[0]
        for index in range(-7, 0):
            cur_percent = next_7day_data.loc[unique_index[index]][next_7day_data.loc[unique_index[index],'币种'] == cur_coin_name]['每天跌涨幅'].values[0]
            # print('cur_percent',cur_percent)
            if cur_percent < 0:
                price_one_day = next_7day_data.loc[unique_index[index]][next_7day_data.loc[unique_index[index],'币种'] == cur_coin_name]['USDT价格'].values[0]
                # print('price_one_day',price_one_day)
                if (price_one_day / start_price) * 100 <= 95:
                    send_message.append(f'{cur_coin_name}\t{unique_index[-8]}\t\t{unique_index[index]}\n')
                    break
    if len(send_message) != 1:
        print(send_message)
        subject = "振幅超过21%，7个工作日内在跌币种价格小于起始日的95%"
        send_email_test(my_subject=subject, my_content=send_message)


if __name__ == '__main__':
    decline_7workday('cn')
