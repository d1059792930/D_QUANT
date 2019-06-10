import tushare as ts
import json
import pandas as pd

from utils.database import MONGO_DB
#  股票基本信息
class BasicCrawler:
    def __init__(self):
        self.connection = MONGO_DB
        self.pro = ts.pro_api('cb5a4a456ab638d7886baa791096986c6987d57d4cc4523c7d58b96c')

    def save_gp_info(self):
        pro=self.pro
        gp_info=self.connection['D_QUANT']['data_info']
        #查询当前所有正常上市交易的股票列表
        data = pro.query('stock_basic', exchange='', list_status='L')
        data['sina_code']=data['ts_code'].map(lambda c:c.lower()[7:9]+c.lower()[0:6])
        print(data['sina_code'])
        # gp_info=self.connection['D_QUANT']['gp_basic_info']
        # #查询当前所有正常上市交易的股票列表
        # data = ts.get_stock_basics()
        gp_info.drop()
        gp_info.insert(json.loads(data.reset_index().to_json(orient='records')))

    def close_connection(self):
        self.connection.close()

if __name__ == '__main__':
    # pro=ts.pro_api('cb5a4a456ab638d7886baa791096986c6987d57d4cc4523c7d58b96c')
    # df = pro.daily_basic(ts_code='600966.SZ', start_date='20000101',end_date='20100101')
    # df2= pro.daily_basic(ts_code='000001.SZ', start_date='20100101',end_date='20190311')
    # print(pd.DataFrame(df, columns = ['trade_date', 'pe']).sort('trade_date'))
    # print(pd.DataFrame(df2, columns = ['trade_date', 'pe']).sort('trade_date'))
    # da=ts.get_hist_data('000001')
    # print(da)
    bc=BasicCrawler()
    bc.save_gp_info()
    # bc.close_connection()
    # trade_cal=pro.trade_cal(exchange='', start_date='20180101', end_date='20181231')
    # print(ts.get_stock_basics())
    # print(trade_cal)

