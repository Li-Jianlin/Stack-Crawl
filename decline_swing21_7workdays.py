import pandas as pd
from datetime import datetime, timedelta
from send_email import send_email_test
from calculate_amplitude import parse_time

def decline_7workday(area):  # 观测有一只币种在跌且振幅大于21%时，后面7日内有无在跌且当天起始价格小于观测日期起始价格的95%
    if area == 'cn':
        file_path_cur_day = r'.\中国每天跌涨幅7天函数测试.csv'
        file_path_all_data = r'.\中国总表7天函数测试.csv'
    else:
        file_path_cur_day = r'.\美国每天跌涨幅.csv'
        file_path_all_data = r'.\美国总表.csv'
    send_message = ['币种\t\t起始日期\t\t截至日期\n']
    # 提取每天0点数据
    all_day_data = pd.read_csv(file_path_cur_day, low_memory=False, usecols=['币种', 'USDT价格', '时间', '每天跌涨幅'])

    # 解析时间
    all_day_data['时间'] = all_day_data['时间'].apply(parse_time)

    # 判断数据是否充足
    if len(all_day_data['时间'].unique()) < 8:
        print('数据不足8天')
        return

    # 币种
    unique_coin = all_day_data['币种'].unique()

    # 时间
    unique_time = all_day_data['时间'].unique()

    # 设置多级索引
    all_day_data.set_index(['时间','币种'],inplace=True)
    complete_time_index = pd.date_range(start=unique_time[-8],end=unique_time[-1],freq='d')
    multi_index = pd.MultiIndex.from_product([complete_time_index,unique_coin],names=['时间','币种'])
    all_day_data = all_day_data.reindex(multi_index).sort_index()
    all_day_data.ffill(inplace=True)

    all_day_data.reset_index(inplace=True)
    all_day_data.set_index('时间',inplace=True)
    unique_index = all_day_data.index.unique()
    # 找出在跌币种
    cur_day_data = all_day_data.loc[unique_index[-8]]
    decline_coin_cur_day = cur_day_data[cur_day_data['每天跌涨幅'] < 0]['币种']
    # print('币种',decline_coin_cur_day)
    # 计算每个币种当天的振幅,找出符合条件的币种
    # print(unique_index[-8],type(unique_index[-8]))
    end_day_time = unique_index[-8] + timedelta(hours=24)
    # print(end_day_time,type(end_day_time))
    # print(end_day_time)

    cur_detail_data = pd.read_csv(file_path_all_data, low_memory=False, usecols=['币种', 'USDT价格', '时间'])
    cur_detail_data['时间'] = pd.to_datetime(cur_detail_data['时间'])

    # 删除缺失值
    cur_detail_data.dropna(inplace=True)
    # 删除重复值
    cur_detail_data.drop_duplicates(subset=['币种','时间'],inplace=True)
    cur_detail_data.set_index('时间',inplace=True)
    cur_detail_data = cur_detail_data.loc[unique_index[-8] : end_day_time]
    cur_detail_data.reset_index(inplace=True)
    # 设置多重索引
    cur_detail_data.set_index(['时间','币种'],inplace=True)

    # 时间
    unique_time = cur_detail_data.index.get_level_values('时间').unique()
    complete_time_index = pd.date_range(start=unique_time.min(),end=unique_time.max(),freq='5min')
    # 币种
    unique_coin = cur_detail_data.index.get_level_values('币种').unique()
    # print(unique_time,unique_coin)
    multi_index = pd.MultiIndex.from_product([complete_time_index,unique_coin],names=['时间','币种'])
    cur_detail_data = cur_detail_data.reindex(multi_index).sort_index()
    cur_detail_data.ffill(inplace=True)
    cur_detail_data.reset_index(inplace=True)
    cur_detail_data.set_index('时间',inplace=True)

    # print(cur_detail_data)
    coin_need_observe = []
    for cur_decline_coin_name in decline_coin_cur_day:
        max_price = cur_detail_data[cur_detail_data['币种'] == cur_decline_coin_name]['USDT价格'].max()
        min_price = cur_detail_data[cur_detail_data['币种'] == cur_decline_coin_name]['USDT价格'].min()
        start_price = cur_detail_data[cur_detail_data['币种'] == cur_decline_coin_name]['USDT价格'].values[0]
        # print('max',max_price,'min',min_price,'start',start_price)
        # print('swing',(max_price - min_price) / start_price * 100)
        if (max_price - min_price) / start_price * 100 >= 21:
            coin_need_observe.append(cur_decline_coin_name)
    # print('coin',coin_need_observe)
    # 提取出7天的数据
    next_7day_data = all_day_data.loc[unique_index[-7]:unique_index[-1]]
    # print(next_7day_data)
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
