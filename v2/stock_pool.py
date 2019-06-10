#  -*- coding: utf-8 -*-

import tushare as ts
from pandas import DataFrame
import matplotlib.pyplot as plt
from data.data_provider import DataModule
from utils.logger import QuantLogger
logger = QuantLogger('backtest')


def stock_pool(begin_date, end_date):
    """
    实现股票池选股逻辑，找到指定日期范围的候选股票
    条件：0 < PE < 30, 按从小到大排序，剔除停牌后，取前100个；再平衡周期：7个交易日
    :param begin_date: 开始日期
    :param end_date: 结束日期
    :return: tuple，再平衡的日期列表，以及一个dict(key: 再平衡日, value: 当期的股票列表)
    """


    # 股票池的再平衡周期
    rebalance_interval = 7

    # 因为上证指数没有停牌不会缺数，所以用它作为交易日历，
    szzz_hq_df = ts.get_k_data('000001', index=True, start=begin_date, end=end_date)
    all_dates = list(szzz_hq_df['date'])

    # 调整日和其对应的股票
    rebalance_date_codes_dict = dict()
    rebalance_dates =[]

    # 保存上一期的股票池
    last_phase_codes = []
    # 所有的交易日数
    dates_count = len(all_dates)
    # 用再平衡周期作为步长循环
    for index in range(0, dates_count, rebalance_interval):
        # 当前的调整日
        rebalance_date = all_dates[index]
        # 获取本期符合条件的备选股票
        this_phase_option_codes = get_option_codes(rebalance_date)
        # 本期入选的股票代码列表
        this_phase_codes = []

        # 找到在上一期的股票池，但是当前停牌的股票，保留在当期股票池中
        if len(last_phase_codes) > 0:
            for code in last_phase_codes:
                daily_k = ts.get_k_data(code, autype=None, start=rebalance_date, end=rebalance_date)
                if daily_k.size == 0:
                    this_phase_codes.append(code)

        print('上期停牌的股票：', flush=True)
        print(this_phase_codes, flush=True)

        # 剩余的位置用当前备选股票的
        option_size = len(this_phase_option_codes)
        if option_size > (100 - len(this_phase_codes)):
            this_phase_codes += this_phase_option_codes[0:100-len(this_phase_codes)]
        else:
            this_phase_codes += this_phase_option_codes

        # 当期股票池作为下次循环的上期股票池
        last_phase_codes = this_phase_codes

        # 保存到返回结果中
        rebalance_date_codes_dict[rebalance_date] = this_phase_codes
        rebalance_dates.append(rebalance_date)

    logger.info(rebalance_dates)
    logger.info("=================================================================")
    logger.info(rebalance_date_codes_dict)

    return rebalance_dates, rebalance_date_codes_dict

# 2---------------------
def get_option_codes(rebalance_date):
    """
    找到某个调整日符合股票池条件的股票列表
    :param rebalance_date: 再平衡日期
    :return: 股票代码列表
    """
    dm=DataModule()
    # 聚合统计
    results =dm.get_data(collection='data_daily_basic',query={'trade_date':rebalance_date.replace('-',''),'pe':{'$gt':0,'$lt':10}})
    # 从小到大排序
    results.sort_values('pe', ascending=True, inplace=True)
    # 返回排名靠前的100只股票代码
    return list(results['code'])[0:100]


def statistic_stock_pool_profit():
    """
    统计股票池的收益
    """
    # 设定评测周期
    # rebalance_dates, codes_dict = stock_pool('2008-01-01', '2018-06-30')
    rebalance_dates, codes_dict = stock_pool('2015-01-01', '2015-01-31')

    # 用DataFrame保存收益
    df_profit = DataFrame(columns=['profit', 'hs300'])

    df_profit.loc[rebalance_dates[0]] = {'profit': 0, 'hs300': 0}

    # 获取沪深300在统计周期内的第一天的值
    hs300_k = ts.get_k_data('000300', index=True, start=rebalance_dates[0], end=rebalance_dates[0])
    hs300_begin_value = hs300_k.loc[hs300_k.index[0]]['close']

    # 通过净值计算累计收益
    net_value = 1
    for _index in range(1, len(rebalance_dates) - 1):
        last_rebalance_date = rebalance_dates[_index - 1]
        current_rebalance_date = rebalance_dates[_index]
        # 获取上一期的股票池
        codes = codes_dict[last_rebalance_date]

        # 统计当前的收益
        profit_sum = 0
        # 参与统计收益的股票个数
        profit_code_count = 0
        for code in codes:
            daily_ks = ts.get_k_data(code, autype='hfq', start=last_rebalance_date, end=current_rebalance_date)

            index_size = daily_ks.index.size
            # 如果没有数据，则跳过，长期停牌
            if index_size == 0:
                continue
            # 买入价
            in_price = daily_ks.loc[daily_ks.index[0]]['close']
            # 卖出价
            out_price = daily_ks.loc[daily_ks.index[index_size - 1]]['close']
            # 股票池内所有股票的收益
            profit_sum += (out_price - in_price)/in_price
            profit_code_count += 1

        profit = round(profit_sum/profit_code_count, 4)

        hs300_k_current = ts.get_k_data('000300', index=True, start=current_rebalance_date, end=current_rebalance_date)
        hs300_close = hs300_k_current.loc[hs300_k_current.index[0]]['close']

        # 计算净值和累积收益
        net_value = net_value * (1 + profit)
        df_profit.loc[current_rebalance_date] = {
            'profit': round((net_value - 1) * 100, 4),
            'hs300': round((hs300_close - hs300_begin_value) * 100/hs300_begin_value, 4)}

        print(df_profit)

    # 绘制曲线
    df_profit.plot(title='Stock Pool Profit Statistic', kind='line')
    # 显示图像
    plt.show()

if __name__=="__main__":
    statistic_stock_pool_profit()
