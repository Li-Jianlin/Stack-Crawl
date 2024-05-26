import pandas as pd
from send_email import send_email_test
from calculate_amplitude import parse_time
epsilon = 1e-5


def observe_price_movement(area):
    if area == 'cn':
        file_path_hour_amplitude_data = r'.\中国每小时跌涨幅.csv'
    else:
        file_path_hour_amplitude_data = r'.\美国每小时跌涨幅.csv'
    # 先读取数据
    data_all_ten_hours = pd.read_csv(file_path_hour_amplitude_data, low_memory=False, usecols=['币种','USDT价格','时间','每小时跌涨幅'])
    # print(data_all_ten_hours)

    # 解析时间
    data_all_ten_hours['时间'] = data_all_ten_hours['时间'].apply(parse_time)
    if len(data_all_ten_hours['时间'].unique()) < 10:
        print('数据不足10小时')
        return
    # 设置多级索引
    data_all_ten_hours.set_index(['时间','币种'],inplace=True)

    # 币种
    unique_coin = data_all_ten_hours.index.get_level_values('币种').unique()

    # 时间
    unique_time = data_all_ten_hours.index.get_level_values('时间').unique()

    # 检测是否有缺失数据，有则进行填充
    complete_time_index = pd.date_range(start=unique_time[-10],end=unique_time[-1],freq='h')
    multi_index = pd.MultiIndex.from_product([complete_time_index,unique_coin],names=['时间','币种'])
    data_all_ten_hours = data_all_ten_hours.reindex(multi_index).sort_index()
    data_all_ten_hours.ffill(inplace=True)
    data_all_ten_hours.reset_index(inplace=True)
    data_all_ten_hours.set_index('时间',inplace=True)

    # 提取标签
    unique_index = data_all_ten_hours.index.unique()
    # print(unique_index)
    # print('unique_index:',unique_index)
    # 找出起始天日期
    start_time = unique_index[-10]
    # print('start_time',start_time)
    # 找出起始日增幅大于4.5的币种记录下来
    start_time_data = data_all_ten_hours.loc[start_time]
    coin_name_increase = start_time_data[start_time_data['每小时跌涨幅'] >= 4.5]['币种']
    print(coin_name_increase)
    # print(coin_name_increase)
    # 对这些币种进行观测
    next_nine_hour_data = data_all_ten_hours.loc[unique_index[-9]:]
    send_message = ["币种\t\t跌幅\t\t\t\t时间\n"]
    for coin_name in coin_name_increase:
        print(coin_name)
        sum_price = 0
        sta_price = start_time_data[start_time_data['币种'] == coin_name]['USDT价格'].values[0]
        print(sta_price)
        for index in range(-8, 0):
            try:
                pre_data = data_all_ten_hours.loc[unique_index[index-1]]
                cur_data = data_all_ten_hours.loc[unique_index[index]]
                pre_price = pre_data[pre_data['币种'] == coin_name]['USDT价格'].values[0]
                cur_price = cur_data[cur_data['币种'] == coin_name]['USDT价格'].values[0]
                # print('pre',pre_price,'cur',cur_price)
                if pre_price > sta_price and cur_price < sta_price:
                    if sta_price < epsilon:  # 当价格极小时会被看作0，可以直接进入下一循环
                        continue
                    percent = (cur_price - sta_price)/sta_price * 100
                    sum_price += percent
                    # print('percent',percent,'sum',sum_price)
                    if sum_price <= -3.5:
                        send_message.append(f'{coin_name}\t\t{sum_price:.3f}%\t{start_time}--{unique_index[index]}')
                        break
                if pre_price < sta_price and cur_price < sta_price:
                    if pre_price < epsilon:
                        pre_price = float('inf')
                    percent = (cur_price - pre_price)/pre_price * 100
                    sum_price += percent
                    # print('percent', percent, 'sum', sum_price)
                    if sum_price <= -3.5:
                        send_message.append(f'{coin_name}\t\t{sum_price:.3f}%\t{start_time}--{unique_index[index]}')
                        break
            except Exception as e:
                print(e)
    if len(send_message) != 1:
        subject = "股票增幅超过4.5%时，后面九小时有跌幅超过-3.5%--李建林"
        print(send_message)
        send_email_test(my_subject=subject, my_content=send_message)


if __name__ == "__main__":
    observe_price_movement('cn')
