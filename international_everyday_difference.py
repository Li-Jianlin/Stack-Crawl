import os.path
import pandas as pd
from datetime import datetime, timedelta
import numpy as np


def calculate_day_difference(start_time_cn):
    file_data_per_hours_name = r'.\美国每小时整点数据.csv'
    # 将中国时间转换为美国时间
    start_time_cn = datetime.strptime(start_time_cn,"%Y-%m-%d %H:%M")
    start_time_us = start_time_cn - timedelta(hours=12)
    # 从美国时间的数据文件中提取数据


if __name__ == '__main__':
    df = {'币种':['a','b','c','d'],
          '时间':['2024-05-20 14:20','2024-05-20 15:20','2024-05-20 16:20','2024-5-21 08:20'],
          '价格':[21.2,234.2,1124,np.nan]}
    pd_df = pd.DataFrame(df)
    print(pd_df)
 
