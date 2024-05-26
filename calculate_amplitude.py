import os.path
import pandas as pd
from datetime import datetime,timedelta
epsilon = 1e-5
def parse_time(time_str):
    try:
        return datetime.strptime(time_str, '%Y-%m-%d %H:%M:%S')
    except ValueError:
        try:
            return datetime.strptime(time_str, '%Y-%m-%d')
        except ValueError:
            return datetime.strptime(time_str, '%Y-%m-%d %H:%M')
def calculate_every_hour_amplitude(area):
    if area == 'cn':
        file_path_read = r'.\中国每小时整点数据.csv'
        file_path_write = r'.\中国每小时跌涨幅.csv'
    else:
        file_path_read = r'.\美国每小时整点数据.csv'
        file_path_write = r'.\美国每小时跌涨幅.csv'

    df_hours_data = pd.read_csv(file_path_read,low_memory=False,usecols=['时间','USDT价格','币种'])

    # print(df_hours_data)
    # 解析时间
    df_hours_data['时间'] = df_hours_data['时间'].apply(parse_time)
    if len(df_hours_data['时间'].unique()) == 1:
        print('只有1小时数据，数据不足')
        return
    # 删除重复值
    df_hours_data.drop_duplicates(subset=['时间','币种'],inplace=True)
    # 删除空值
    df_hours_data.dropna(inplace=True)

    # 设置多级索引
    df_hours_data.set_index(['时间','币种'],inplace=True)

    # 币种
    unique_coin = df_hours_data.index.get_level_values('币种').unique()

    # 时间
    unique_time = df_hours_data.index.get_level_values('时间').unique()
    complete_time_index = pd.date_range(start=unique_time[-2],end=unique_time[-1],freq='h')
    multi_index = pd.MultiIndex.from_product([complete_time_index,unique_coin],names=['时间','币种'])
    df_hours_data = df_hours_data.reindex(multi_index).sort_index()
    df_hours_data.ffill(inplace=True)
    df_hours_data.reset_index(inplace=True)
    df_hours_data.set_index('时间',inplace=True)
    unique_index = df_hours_data.index.unique()
    pre_time_index = unique_index[-2]
    cur_time_index = unique_index[-1]
    last_two_hours_data = df_hours_data.loc[pre_time_index:cur_time_index]
    # print(last_two_hours_data)
    # 找出公共的币种
    coin_name_in_pre = last_two_hours_data.loc[pre_time_index]['币种']
    # print(coin_name_in_pre)
    coin_name_in_cur = last_two_hours_data.loc[cur_time_index]['币种']
    common_coin_name = coin_name_in_pre[coin_name_in_pre.isin(coin_name_in_cur)]
    data = {
        '币种':[],
        '每小时跌涨幅': [],
        'USDT价格': [],
        '时间': []
    }
    for coin_name in common_coin_name:
        try:
            pre_price = last_two_hours_data.loc[pre_time_index][last_two_hours_data.loc[pre_time_index]['币种'] == coin_name]['USDT价格'].values[0]
            cur_price = last_two_hours_data.loc[cur_time_index][last_two_hours_data.loc[cur_time_index]['币种'] == coin_name]['USDT价格'].values[0]
            if pd.isna(pre_price) or pd.isna(cur_price) or pre_price == 0:
                percent = 0
            else:
                percent = (cur_price - pre_price) / pre_price * 100
        except Exception as e:
            print(e)
            continue
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
    df_days_data = pd.read_csv(file_path_read,low_memory=False,usecols=['时间', 'USDT价格', '币种'])
    df_days_data['时间'] = df_days_data['时间'].apply(parse_time)

    if len(df_days_data['时间'].unique()) == 1:
        print('只有一天数据，数据不足')
        return
    # 删除重复值
    df_days_data.drop_duplicates(subset=['时间','币种'], inplace=True)

    # 删除缺失值
    df_days_data.dropna(inplace=True)
    # 设置多级索引
    df_days_data.set_index(['时间','币种'] ,inplace=True)

    # 币种
    unique_coin = df_days_data.index.get_level_values('币种').unique()

    # 时间
    unique_time = df_days_data.index.get_level_values('时间').unique()
    complete_time_index = pd.date_range(start=unique_time[-2],end=unique_time[-1],freq='d')
    multi_index = pd.MultiIndex.from_product([complete_time_index,unique_coin],names=['时间','币种'])
    df_days_data = df_days_data.reindex(multi_index).sort_index()
    df_days_data.ffill(inplace=True)
    df_days_data.reset_index(inplace=True)
    df_days_data.set_index('时间',inplace=True)
    unique_index = df_days_data.index.unique()
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
            if pd.isna(pre_price) or pd.isna(cur_price) or pre_price == 0:
                percent = 0
            else:
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
            df_data.to_csv(file_path_write,mode='a', encoding='utf-8',index=False,header=False)
        else:
            df_data.to_csv(file_path_write,mode='w', encoding='utf-8',index=False,header=True)
if __name__ == '__main__':
    # calculate_every_hour_amplitude('cn')
    calculate_everyday_amplitude('cn')
    # calculate_every_hour_amplitude('us')
    # calculate_everyday_amplitude('us')