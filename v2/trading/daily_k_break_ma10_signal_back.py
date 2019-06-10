#  -*- coding: utf-8 -*-

import pymongo
import pandas as pd
"""
当日K线上穿和下穿10日均线的判断
"""

class DailyKBreakMA10Signal:


    def is_k_up_break_ma10(self,df,data_provider):
        """
        判断某只股票在某日是否满足K线上穿10日均线

        :param code: 股票代码
        :param begin_date: 开始日期
        :param end_date: 结束日期
        :return: True/False
        """
        # print(df['trade_date'])
        # print(df['code'])

        # 根据代码获取对应日期最新2条数据
        res=data_provider.get_data('data_D',{'code':df['code']},sort=[('date',pymongo.DESCENDING)],limit=2)
        # 计算收盘价和均价的差值
        res['delta'] = res['close'] - res['ma10']
        # 日期从新排列
        res.sort_values('date',ascending=1,inplace=True)
        # 对索引从新排序
        res.reset_index(drop=True, inplace=True)
        # print(res.loc[:,['delta','code','date','close',]])
        # print(res.index.size)
        for i in range(res.index.size-1):
            if res.loc[i]['delta']<= 0 < res.loc[i+1]['delta']:
                print(res.loc[i+1,['code']])
        # 上穿条件，前一天的收盘价 - 10平均价 <= 0 < 当前的收盘价 -10平均价
        is_break_up = 0

        return pd.Series([1,2,3], index=['a', 'b', 'c'])




    def is_k_down_break_ma10(self, code, begin_date, end_date):
        """
        判断某只股票在某日是否满足K线下穿10日均线

        :param code: 股票代码
        :param begin_date: 开始日期
        :param end_date: 结束日期
        :return: True/False
        """

        return 0

def k_up_break_ma10(df):
    # print(33333)
    print(df)
