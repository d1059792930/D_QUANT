#  -*- coding: utf-8 -*-

from pymongo import ASCENDING
from utils.database import MONGO_DB
from pandas import DataFrame
import pandas as pd
import time
import tushare as ts
import sys


"""
数据处理子系统，主要完成的工作
为交易系统提供数据接口
"""


class DataModule:
    def __init__(self):
        self.connection=MONGO_DB
        self.db = self.connection['D_QUANT']

        # 传入对应的collection 和查询没条件
    def get_data(self,collection,query={},sort=[],limit=sys.maxsize):

        daily_cursor=None
        if sort:
            daily_cursor=self.db[collection].find(query).sort(sort).limit(limit)
        else:
            daily_cursor=self.db[collection].find(query).limit(limit)
        dailies_df = DataFrame([daily for daily in daily_cursor])
        return dailies_df

    def close_db(self):
        self.connection.close()

    def get_k_data(self, code, index=None, autype=None, period='D', begin_date=None, end_date=None):
        """
        获取指定股票代码在固定周期的数据
        :param code: 股票代码
        :param index: 是否是指数
        :param autype: 复权类型，None - 不复权，qfq - 前复权, hfq - 后复权
        :param period: K线周期，D - 日线(默认值)，W - 周线， M - 月线，M1 - 1分钟，M5 - 5分钟
        :param begin_date: 数据的开始日期
        :param end_date: 数据的结束日期
        :return: 包含K线的DataFrame
        """

        if index:
            # 如果是指数，则从daily数据集中查询数据
            daily_cursor = self.db['daily'].find(
                {'code': code,  'index': True, 'date': {'$gte': begin_date, '$lte': end_date}},
                sort=[('date', ASCENDING)],
                projection={'_id': False},
                batch_size=500)
        else:
            # 如果不是指数，则根据复权类型从相应的数据集中查询数据
            if autype is None:
                daily_cursor = self.db['data_' + period].find(
                    {'code': code, 'date': {'$gte': begin_date, '$lte': end_date}},
                    sort=[('date', ASCENDING)],
                    projection={'_id': False},
                    batch_size=500)
            else:
                daily_cursor = self.db['data_' + period].find(
                    {'code': code, 'date': {'$gte': begin_date, '$lte': end_date}},
                    sort=[('date', ASCENDING)],
                    # projection={'_id': False},
                    batch_size=500)
        dailies_df = DataFrame([daily for daily in daily_cursor])
        return dailies_df



if __name__ == '__main__':
    # # print(DataModule().get_k_data('000001',begin_date='2015-01-01',end_date='2018-01-01'))
    # df=DataModule().get_data(collection='gp_basic_info',query={'pe':{'$gt':0,'$lt':10}})
    # df.sort_values('pe', ascending=True, inplace=True)
    # print(pd.DataFrame(df,columns =['code','pe']))
    # for i in df['code']:
    #     dd = ts.get_sina_dd(i,date='2019-03-06')
    #     print(dd)
    print(ts.get_today_all())
