import tushare as ts
import json
import pandas as pd
from utils.logger import QuantLogger

from utils.database import MONGO_DB
# gp基本信息  包含
# code,代码
# name,名称
# industry,所属行业
# area,地区
# pe,市盈率
# outstanding,流通股本(亿)
# totals,总股本(亿)
# totalAssets,总资产(万)
# liquidAssets,流动资产
# fixedAssets,固定资产
# reserved,公积金
# reservedPerShare,每股公积金
# esp,每股收益
# bvps,每股净资
# pb,市净率
# timeToMarket,上市日期
# undp,未分利润
# perundp, 每股未分配
# rev,收入同比(%)
# profit,利润同比(%)
# gpr,毛利率(%)
# npr,净利润率(%)
# holders,股东人数
class daily_basic:
    def __init__(self):
        self.connection = MONGO_DB
        self.pro = ts.pro_api('cb5a4a456ab638d7886baa791096986c6987d57d4cc4523c7d58b96c')
        self.logger = QuantLogger('daily_basic')

    def save_gp_info(self, start_date='20190312',end_date='20190312'):
        pro=self.pro
        d_quant=self.connection['D_QUANT']
        gp_info=d_quant['data_daily_basic']
        cur=d_quant['data_info']
        data_info=cur.find()
        gp_info.create_index('code')
        gp_info.create_index('ts_code')
        gp_info.create_index('trade_date')
        for i in data_info:
            try:
                #查询当前所有正常上市交易的股票列表
                data = pro.daily_basic(ts_code=i['ts_code'], start_date='20000101',end_date='20100101')
                data['sina_code']=i['sina_code']
                data['code']=i['symbol']
                gp_info.insert(json.loads(data.reset_index().to_json(orient='records')))
            except Exception:
                self.logger.error('错误code=%s' % (i['ts_code']))
                #避免2010年后上市的股票没有数据报错

            try:
                df= pro.daily_basic(ts_code=i['ts_code'], start_date=start_date,end_date=end_date)
                print(df)
                df['sina_code']=i['sina_code']
                df['code']=i['symbol']
                # gp_info.insert(json.loads(df.reset_index().to_json(orient='records')))
                self.logger.error('code=%s' % (i['ts_code']))
            except Exception:
                self.logger.error('错误code=%s' % (i['ts_code']))

    def close_connection(self):
        self.connection.close()

if __name__ == '__main__':
    #     ts_code	str	TS股票代码
    # trade_date	str	交易日期
    # close	float	当日收盘价
    # turnover_rate	float	换手率（%）
    # turnover_rate_f	float	换手率（自由流通股）
    # volume_ratio	float	量比
    # pe	float	市盈率（总市值/净利润）
    # pe_ttm	float	市盈率（TTM）
    # pb	float	市净率（总市值/净资产）
    # ps	float	市销率
    # ps_ttm	float	市销率（TTM）
    # total_share	float	总股本 （万）
    # float_share	float	流通股本 （万）
    # free_share	float	自由流通股本 （万）
    # total_mv	float	总市值 （万元）
    # circ_mv	float	流通市值（万元）


    bc=daily_basic()
    start_date='20190312'
    end_date='20190312'
    bc.save_gp_info(start_date)

    # cur=bc.connection['D_QUANT']['data_daily_basic']
    # # 聚合统计
    # results =cur.aggregate([{'$group': {'_id': '$ts_code', 'Totals' : {'$sum': 1}}}])
    # count1=0
    # for i in results:
    #     print(i)
    #     count1+=1
    # print(count1)
    bc.close_connection()

