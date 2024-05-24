import os.path
import pandas as pd
epsilon = 1e-10

def calculate_every_hour_amplitude(area):
    if area == 'cn':
        file_path_read = r'.\中国每小时整点数据.csv'
        file_path_write = r'.\中国每小时跌涨幅.csv'
    else:
        file_path_read = r'.\美国每小时整点数据.csv'
        file_path_write = r'.\美国每小时跌涨幅.csv'
    df_hours_data = pd.read_csv(file_path_read,index_col='时间',low_memory=False,usecols=['时间','USDT价格','币种'])
    unique_index = df_hours_data.index.unique().tolist()
    if len(unique_index) <= 1:
        print('数据不足')
        return
    # 处理缺失值
    df_hours_data.dropna(inplace=True)
    pre_time_index = unique_index[-2]
    cur_time_index = unique_index[-1]
    last_two_hours_data = df_hours_data.loc[pre_time_index:cur_time_index]
    # 找出公共的币种
    coin_name_in_pre = last_two_hours_data.loc[pre_time_index]['币种']
    coin_name_in_cur = last_two_hours_data.loc[cur_time_index]['币种']
    common_coin_name = coin_name_in_pre[coin_name_in_pre.isin(coin_name_in_cur)]
    data = {
        '币种':[],
        '每小时跌涨幅':[],
        'USDT价格':[],
        '时间':[]
    }
    for coin_name in common_coin_name:
        pre_price = last_two_hours_data.loc[pre_time_index][last_two_hours_data.loc[pre_time_index]['币种'] == coin_name]['USDT价格'].values[0]
        cur_price = last_two_hours_data.loc[cur_time_index][last_two_hours_data.loc[cur_time_index]['币种'] == coin_name]['USDT价格'].values[0]
        percent = (cur_price - pre_price)/pre_price * 100
        data['币种'].append(coin_name)
        data['每小时跌涨幅'].append(percent)
        data['USDT价格'].append(cur_price)
        data['时间'].append(cur_time_index)
    df_data = pd.DataFrame(data)
    if os.path.exists(file_path_write):
        df_data.to_csv(file_path_write,mode='a',encoding='utf-8',index=False,header=False)
    else:
        df_data.to_csv(file_path_write,mode='w',encoding='utf-8',index=False,header=True)

def calculate_everyday_amplitude(area):
    if area == 'cn':
        file_path_read = r'.\中国每天整点数据.csv'
        file_path_write = r'.\中国每天跌涨幅.csv'
    else:
        file_path_read = r'.\美国每天整点数据.csv'
        file_path_write = r'.\美国每天跌涨幅.csv'
    df_days_data = pd.read_csv(file_path_read,index_col='时间',low_memory=False,usecols=['时间','USDT价格','币种'])
    unique_index = df_days_data.index.unique().tolist()
    if len(unique_index) <= 1:
        print('数据不足')
        return
    # 处理缺失值
    df_days_data.dropna(inplace=True)
    pre_time_index = unique_index[-2]
    cur_time_index = unique_index[-1]
    last_two_days_data = df_days_data.loc[pre_time_index:cur_time_index]
    pre_time_coin = df_days_data.loc[pre_time_index]['币种']
    cur_time_coin = df_days_data.loc[cur_time_index]['币种']
    common_coin_name = pre_time_coin[pre_time_coin.isin(cur_time_coin)]
    data = {
        '币种':[],
        '每天跌涨幅':[],
        'USDT价格':[],
        '时间':[]
    }
    for coin_name in common_coin_name:
        try:
            pre_price = last_two_days_data.loc[pre_time_index][last_two_days_data.loc[pre_time_index]['币种'] == coin_name]['USDT价格'].values[0]
            cur_price = last_two_days_data.loc[cur_time_index][last_two_days_data.loc[cur_time_index]['币种'] == coin_name]['USDT价格'].values[0]
            if abs(pre_price) < epsilon:  # 当价格极小时将其赋为正无穷大，使结果为0
                pre_price = float('inf')
            percent = (cur_price - pre_price) / pre_price * 100
        except ValueError as e:
            print(e)
            continue
        data['币种'].append(coin_name)
        data['每天跌涨幅'].append(percent)
        data['USDT价格'].append(cur_price)
        data['时间'].append(cur_time_index)
        df_data = pd.DataFrame(data)
        if os.path.exists(file_path_write):
            df_data.to_csv(file_path_write,mode='a',encoding='utf-8',index=False,header=False)
        else:
            df_data.to_csv(file_path_write,mode='w',encoding='utf-8',index=False,header=True)