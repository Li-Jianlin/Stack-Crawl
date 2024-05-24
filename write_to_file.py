import os
from datetime import datetime,timedelta

def every_hours_to_csv_cn(df_combined):# 写入中国时间的每小时数据
    file_path = r'.\中国每小时整点数据.csv'
    if os.path.exists(file_path):
        df_combined.to_csv(file_path, mode='a', encoding='utf-8', index=False, header=False)
    else:
        df_combined.to_csv(file_path, mode='w', encoding='utf-8', index=False, header=True)

def everyday_to_csv_cn(df_combined):# 写入中国时间的每天0点数据
    file_path = r'.\中国每天0点数据.csv'
    if os.path.exists(file_path):
        df_combined.to_csv(file_path, mode='a', encoding='utf-8', index=False, header=False)
    else:
        df_combined.to_csv(file_path, mode='w', encoding='utf-8', index=False, header=True)

def oneday_all_data_to_csv_cn(df_combined): # 写入中国时间一整天的数据
    file_path = r'.\中国总表.csv'
    if os.path.exists(file_path):
        df_combined.to_csv(file_path, mode='a', encoding='utf-8', index=False, header=False)
    else:
        df_combined.to_csv(file_path, mode='w', encoding='utf-8', index=False, header=True)

def every_hours_to_csv_us(df_combined):# 写入美国时间的每小时数据
    file_path = r'.\美国每小时整点数据.csv'
    data_us = df_combined.copy()
    data_us['时间'] = data_us['时间'].apply(lambda x: x - timedelta(hours=12))
    if os.path.exists(file_path):
        data_us.to_csv(file_path, mode='a', encoding='utf-8', index=False, header=False)
    else:
        data_us.to_csv(file_path, mode='w', encoding='utf-8', index=False, header=True)

def everyday_to_csv_us(df_combined):# 写入美国每天0点数据
    file_path = r'.\美国每天0点数据.csv'
    data_us = df_combined.copy()
    data_us['时间'] = data_us['时间'].apply(lambda x: x - timedelta(hours=12))
    if os.path.exists(file_path):
        data_us.to_csv(file_path, mode='a', encoding='utf-8', index=False, header=False)
    else:
        data_us.to_csv(file_path, mode='w', encoding='utf-8', index=False, header=True)

def oneday_all_data_to_csv_us(df_combined):
    file_path = r'.\美国总表.csv'
    data_us = df_combined.copy()
    data_us['时间'] = data_us['时间'].apply(lambda x: x - timedelta(hours=12))
    if os.path.exists(file_path):
        data_us.to_csv(file_path, mode='a', encoding='utf-8', index=False, header=False)
    else:
        data_us.to_csv(file_path, mode='w', encoding='utf-8', index=False, header=True)