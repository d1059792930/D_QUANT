#  -*- coding: utf-8 -*-

import schedule
from data.history_data import History_data
import time
from datetime import datetime
import threading

"""
每天下午15:30执行抓取，只有周一到周五才真正执行抓取任务
"""


def crawl_daily():
    hd = History_data()
    now_date = datetime.now()
    weekday = now_date.strftime('%w')

    # print(weekday)
    if 0 < int(weekday) < 6:
        now2 = now_date.strftime('%Y%m%d')
        now = now_date.strftime('%Y-%m-%d')
        start = '2000-01-01'
        threads = []
        t1 = threading.Thread(target=hd.save_data, kwargs={"ktype": "5", "start_date": start, "end_date": now})
        threads.append(t1)
        t2 = threading.Thread(target=hd.save_data, kwargs={"ktype": "15", "start_date": start, "end_date": now})
        threads.append(t2)
        t3 = threading.Thread(target=hd.save_data, kwargs={"ktype": "30", "start_date": start, "end_date": now})
        threads.append(t3)
        t4 = threading.Thread(target=hd.save_data, kwargs={"ktype": "60", "start_date": start, "end_date": now})
        threads.append(t4)
        t5 = threading.Thread(target=hd.save_data, kwargs={"ktype": "D", "start_date": start, "end_date": now})
        threads.append(t5)
        t6 = threading.Thread(target=hd.save_data, kwargs={"ktype": "W", "start_date": start, "end_date": now})
        threads.append(t6)
        t7 = threading.Thread(target=hd.save_data, kwargs={"ktype": "M", "start_date": start, "end_date": now})
        threads.append(t7)
        t8 = threading.Thread(target=hd.save_daily_data, kwargs={"start_date": start, "end_date": now2})
        threads.append(t8)
        for t in threads:
            t.setDaemon(True)
            t.start()
        t.join()
        hd.close_connection()


if __name__ == '__main__':
    # print(pow(3,2)) 表示3的2次方
    # print(pow(1.1,40))
    crawl_daily()
    # schedule.every().day.at("15:30").do(crawl_daily)
    # while True:
    #     schedule.run_pending()
    #     time.sleep(10)
