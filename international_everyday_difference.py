import pandas as pd
from datetime import datetime, timedelta

def deposit_file_us_data(df_combined):
    df_combined['时间'] = (datetime.strptime(df_combined['时间'] ,"%Y-%m-%d %H:%M") - timedelta(hours=12))
    print(df_combined)
def calculate_day_difference(start_time_cn):
    # 将中国时间转换为美国时间
    start_time_cn = datetime.strptime(start_time_cn,"%Y-%m-%d %H:%M")
    start_time_us = start_time_cn - timedelta(hours=12)
    # 创建一个csv文件存储美国时间的数据，即把爬取下来的数据时间进行变换，
    # 然后单独存入美国数据的文件中，每次计算美国的数据只需要从美国文件中提取数据

if __name__ == '__main__':
    df = {'币种':['a','b','c'],
          '时间':['2024-05-20 14:20','2024-05-20 15:20','2024-05-20 16:20']}
    pd_df = pd.DataFrame(df)
    print(pd_df)
    deposit_file_us_data(pd_df)
