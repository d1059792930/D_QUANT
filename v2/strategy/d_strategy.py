#  -*- coding: utf-8 -*-

import tushare as ts
from pandas import DataFrame
import matplotlib.pyplot as plt
from data.data_provider import DataModule
import pandas as pd
from trading.daily_k_break_ma10_signal import DailyKBreakMA10Signal

# D宽客策略
class D_strategy:
    def __init__(self):
        self.data_provider = DataModule()
        self.pro = ts.pro_api('cb5a4a456ab638d7886baa791096986c6987d57d4cc4523c7d58b96c')
        self.dailyKBreakMA10Signal=DailyKBreakMA10Signal()

    # 价格少于5块，pe<30的股票，收盘价上穿20日均线的股票。
    def stock_pool(self,begin_date='20190106', end_date='20190312'):
        data_provider=self.data_provider
        res=data_provider.get_data('data_daily_basic',{'trade_date':{'$gte':begin_date,'$lte':end_date},'pe':{'$gte':0,'$lte':10},'close':{'$lte':5}})
        # print(pd.DataFrame(res,columns =['trade_date','code','pe','close']))
        df=pd.DataFrame(res,columns =['trade_date','code','pe','close'])
        # self.dailyKBreakMA10Signal.is_k_up_break_ma10(data_provider=data_provider,code='',begin_date='20190311',end_date='20190311')
        # axis=1表示每次传入行,不传或者0表示列  is_k_up_break_ma10 必须有df 参数
        # df.sort()
        # data=df.apply(self.dailyKBreakMA10Signal.is_k_up_break_ma10,axis=1,**{'data_provider':data_provider})
        # print(data)
        limit_df=df.drop_duplicates(['trade_date']).reset_index(drop=True)
        # print(limit_df)
        limit=limit_df.index.size
        code_df=df.drop_duplicates(['code']).reset_index(drop=True)
        # print(code_df)
        date=end_date[0:4]+'-'+end_date[4:6]+'-'+end_date[6:8]
        code_dates={}
        for i in range(code_df.index.size):
            d=self.dailyKBreakMA10Signal.is_k_up_break_ma10(data_provider=data_provider,code=code_df.loc[i]['code'],date=date,limit=limit)
            code_dates[d[0]]=d[1]
        # for i in range(df.index.size):
        #     temp_date=df.loc[i]['trade_date']
        #     date=temp_date[0:4]+'-'+temp_date[4:6]+'-'+temp_date[6:8]
        #     d=self.dailyKBreakMA10Signal.is_k_up_break_ma10(data_provider=data_provider,code=df.loc[i]['code'],date=date)
        #     print(d)
        # data={'code':codes,'date':dates}
        # print(code_dates)
        data_provider.close_db()
        return code_dates

if __name__=="__main__":
    d=D_strategy()
    print(d.stock_pool())
    # 实时行情数据get_today_all()；
    # 历史分笔数据get_tick_data()；
    # 实时分笔数据get_realtime_quotes()；
    # 当日历史分笔数据get_today_ticks()；
    # 大盘指数行情列表get_index()；
    # 大单交易数据get_sina_dd()。
    # df=ts.get_realtime_quotes('000581')
    # print(df)


    # 投资组合回报计算；
    # SMA策略（简单平均线策略） ；
    # SMA_cross策略（均线交叉策略） ；
    # VWAP策略（交易量加权平均价格策略） ；
    # bbands策略（布林带策略） ；
    # RSI2策略（相对强弱指标策略） 。
#     /**
# * H1:=MAX(DYNAINFO(3),DYNAINFO(5));DYNAINFO(3)取得最新动态行情: 昨收   DYNAINFO(5)取得最新动态行情: 最高
# * L1:=MIN(DYNAINFO(3),DYNAINFO(6));DYNAINFO(6)取得最新动态行情: 最低
# *
# * @param H1List 放入昨日收盘价，最近动态最高
# * @param L1List 放入昨日收盘价，最近动态最低
# * @return
# */
# public static Double getZhiCheng(List H1List, List L1List) {
#     Double H1 = FunctionUtil.getHHV(H1List);
# Double L1 = FunctionUtil.getLLV(L1List);
# Double P1 = H1 - L1;
# double zhicheng = L1 + P1 * 0.5 / 8;
# return zhicheng;
# }
#
# /**
# * H1:=MAX(DYNAINFO(3),DYNAINFO(5));DYNAINFO(3)取得最新动态行情: 昨收   DYNAINFO(5)取得最新动态行情: 最高                                                                                                                                                                                        * @return
# */
# public static Double getZuLi(List H1List, List L1List) {
#     Double H1 = FunctionUtil.getHHV(H1List);
# Double L1 = FunctionUtil.getLLV(L1List);
# Double P1 = H1 - L1;
# double zuLi = L1 + P1 * 7 / 8;
# return zuLi;
# }
