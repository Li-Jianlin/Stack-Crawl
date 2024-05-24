import datetime
import math
import os
import random
from decline_swing21_7workdays import decline_7workday
from observe_ninehours_price_movement import observe_price_movement
from decline_and_curprice_lessthan_pre_30days_max_55percent import \
    decline_and_curprice_lessthan_pre_30days_max_55percent
import time
import pandas as pd
import write_to_file
import crawl
import calculate_amplitude

if __name__ == "__main__":
    file_path_crawl_error = r'.\爬取异常.csv'
    amount = int(input('你要爬取多少种货币数据？'))
    amount_9hours = 0
    amount_30days = 0
    amount_7workdays = 0
    use_crawl_url1 = True
    crawl_amount_url2 = 0
    while True:
        try:
            time_now_minutes = time.strftime('%M')
            time_hour = time.strftime('%H')
            df_combined = pd.DataFrame()
            time_str_cn = time.strftime('%Y-%m-%d %H:%M')
            if int(time_now_minutes) % 5 == 0:
                try:
                    if use_crawl_url1:
                        print("开始爬取")
                        time_str_cn = time.strftime('%Y-%m-%d %H:%M')
                        time_date_cn = datetime.datetime.strptime(time_str_cn, '%Y-%m-%d %H:%M')
                        print(time_date_cn)
                        time_date_us = time_date_cn - datetime.timedelta(hours=12)
                        page = math.ceil(amount / 20)
                        for index in range(1, page + 1):
                            print(f"爬取第{index}页")
                            content = crawl.get_data_01(index)
                            data_dict = crawl.data_parse_01(content, time_date_cn)
                            df_data = pd.DataFrame(data_dict)
                            df_combined = pd.concat([df_combined, df_data], ignore_index=True)
                    else:
                        raise Exception('弃用网址1，使用网址2')
                except Exception as e:
                    df_combined.drop(df_combined.index, inplace=True)  # 清空之前的数据
                    use_crawl_url1 = False
                    crawl_amount_url2 += 1
                    print(e)
                    if crawl_amount_url2 == 300:
                        use_crawl_url1 = True
                        crawl_amount_url2 = 0
                    page = math.ceil(amount / 50)
                    for index in range(1, page + 1):
                        print(f"正在爬取第{index}页")
                        data_json = crawl.get_data_02(index)
                        df_data = crawl.data_parse_02(data_json, time_date_cn)
                        df_combined = pd.concat([df_combined, df_data], ignore_index=True)
                # 写入每5分钟数据
                write_to_file.oneday_all_data_to_csv_cn(df_combined)
                write_to_file.oneday_all_data_to_csv_us(df_combined)
                # 写入整点数据
                if time_now_minutes == '00':
                    write_to_file.every_hours_to_csv_cn(df_combined)
                    write_to_file.every_hours_to_csv_us(df_combined)
                    # 分别计算两个地区每小时跌涨幅
                    calculate_amplitude.calculate_every_hour_amplitude('cn')
                    calculate_amplitude.calculate_every_hour_amplitude('us')
                    observe_price_movement('cn')
                    observe_price_movement('us')
                # 写入0点数据
                if time_hour == '00' and time_now_minutes == '00':
                    write_to_file.everyday_to_csv_cn(df_combined)
                    calculate_amplitude.calculate_everyday_amplitude('cn')
                    decline_7workday('cn')
                    decline_and_curprice_lessthan_pre_30days_max_55percent('cn')
                if time_hour == '12' and time_now_minutes == '00':
                    write_to_file.everyday_to_csv_us(df_combined)
                    calculate_amplitude.calculate_everyday_amplitude('us')
                    decline_7workday('us')
                    decline_and_curprice_lessthan_pre_30days_max_55percent('us')
                print('该次爬取已完成')
                while int(time.strftime('%M')) % 5 == 0:
                    print('休眠60秒')
                    time.sleep(60)
        except Exception as e:
            print(e)
            time.sleep(60)
