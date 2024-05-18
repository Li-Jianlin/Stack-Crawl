import pandas as pd


def calculate_time_difference(start_time_CN):
    # start_time_CN = input('请输入中国的起始日期(2024-04-24_00-00)')
    all_days = 3
    all_sheet_amounts = all_days + 1
    all_sheets = pd.read_excel(r'.\每小时整点数据.xlsx', sheet_name=None)
    start_index = list(all_sheets.keys()).index(start_time_CN)
    every_day_00_US_sheets = [sheet for sheet in list(all_sheets.keys())[start_index:] if sheet.endswith('_12-00')]
    # 提取出真正需要的标签页
    target_sheets_00_US = every_day_00_US_sheets[0:all_sheet_amounts]
    # 根据当前标签页和第二天标签页里的12点数据计算美国的跌涨幅
    for i in range(len(target_sheets_00_US) - 1):
        cur_data = pd.read_excel(r'.\每小时整点数据.xlsx', sheet_name=target_sheets_00_US[i])
        next_data = pd.read_excel(r'.\每小时整点数据.xlsx', sheet_name=target_sheets_00_US[i + 1])
        cur_coin_name = cur_data['币种']
        next_coin_name = next_data['币种']
        intersection_set = set(cur_coin_name).intersection(set(next_coin_name))
        exit_coin_name_data = list(intersection_set)
        for coin_name in exit_coin_name_data:
            print(coin_name)
            cur_price = float(
                cur_data[cur_data['币种'] == coin_name]['USDT价格'].values[0].replace('$', '').replace('$', ''))
            next_price = float(
                next_data[next_data['币种'] == coin_name]['USDT价格'].values[0].replace('$', '').replace(',', ''))
            print('cur', cur_price, type(cur_price), 'next', next_price, type(next_price))
            percent = (next_price - cur_price) / cur_price * 100
            print(percent)
            # 将算出的数据存储到excel中，时间存储为美国时间，需要存储币种名、跌涨幅、价格、


if __name__ == '__main__':
    start_time_CN = '2024-05-01_00-00'
    calculate_time_difference(start_time_CN)
