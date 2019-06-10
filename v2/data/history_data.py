import tushare as ts
from utils.database import MONGO_DB
import json
import pandas as pd
from utils.logger import QuantLogger
from time import ctime, sleep
from datetime import datetime


# 日线，分钟等历史数据
class History_data:
    def __init__(self):
        self.connection = MONGO_DB
        self.pro = ts.pro_api('cb5a4a456ab638d7886baa791096986c6987d57d4cc4523c7d58b96c')
        self.logger = QuantLogger('history_data')

    def save_data(self, ktype='D', start_date='2000-01-01', end_date=''):
        # ktype：数据类型，D=日k线 W=周 M=月 5=5分钟 15=15分钟 30=30分钟 60=60分钟，默认为D
        print("save_data..." + ktype)
        if end_date == '':
            end_date = datetime.now().strftime('%Y-%m-%d')
        db = self.connection['D_QUANT']
        # 根据条件删除
        # db['data_'+ktype].delete_many(filter={'date':{'$gte':'2019-03-07'}})
        gp_datas = db['data_info'].find()
        # db['data_'+ktype].drop()
        for i in gp_datas:
            try:
                print(i)
                code = i['ts_code'].split('.')[0]
                data = ts.get_hist_data(code, ktype=ktype, start=start_date, end=end_date)
                print(data)
                d = data.reset_index()
                d['code'] = code
                d['ts_code'] = i['ts_code']
                # 创建索引
                db['data_' + ktype].create_index('code')
                db['data_' + ktype].create_index('date')
                db['data_' + ktype].insert(json.loads(d.to_json(orient='records')))
                self.logger.info('code=' + i['ts_code'])
            except Exception:
                self.logger.error('错误code=%s' % (i['ts_code']))

    def save_daily_data(self, start_date='20000101', end_date=''):
        if end_date == '':
            end_date = datetime.now().strftime('%Y%m%d')
        db = self.connection['D_QUANT']
        gp_datas = db['data_info'].find()
        pro = self.pro
        print("save_daily_data...")
        # db['data_daily'].drop()
        for i in gp_datas:
            try:
                d = pro.daily(ts_code=i['ts_code'], start_date=start_date, end_date=end_date)
                # 格式化日期
                d['date'] = d['trade_date'].map(lambda x: x[0:4] + '-' + x[4:6] + '-' + x[6:8])
                d['code'] = i['ts_code'].split('.')[0]
                db['data_daily'].create_index('code')
                db['data_daily'].create_index('date')
                db['data_daily'].insert(json.loads(d.to_json(orient='records')))
                self.logger.info('code:' + i['ts_code'])
            except Exception:
                self.logger.error('错误，code=%s' % (i['ts_code']))

    def close_connection(self):
        self.connection.close()


if __name__ == '__main__':
    h = History_data()
    # 需要一个类型，一个类型的跑
    # ktype：数据类型，D=日k线 W=周 M=月 5=5分钟 15=15分钟 30=30分钟 60=60分钟，默认为D
    start_date = '2019-03-07'
    # h.save_data('5',start_date)
    # h.save_data('15',start_date)
    # h.save_data('30',start_date)
    # h.save_data('60',start_date)
    h.save_data('D', start_date)
    # h.save_data('W',start_date)
    # h.save_data('M',start_date)
    # h.save_daily_data()
    h.close_connection()
    # data=ts.get_hist_data('600966',ktype='D',start='2019-03-07')
    # print(data)
