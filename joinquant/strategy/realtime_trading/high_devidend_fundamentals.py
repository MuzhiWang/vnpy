# 高股息基本面策略

import pandas as pd
import numpy as np
import jqdata
from kuanke.wizard import *
from six import StringIO, BytesIO

from mysqltrade import *


def after_code_changed(context):
    now = context.current_dt.time()
    print("{0}替换了代码".format(now))
    unschedule_all()
    g.strategy = 'dougua1'  # 策略名
    g.filename = '纯高股息基本面修改资金.csv'  # 入金配置文件名称

    g.first = 1
    g.out_cash = 0
    g.high_limit_list = []
    g.hold_list = []
    set_option('use_real_price', True)
    set_benchmark('000300.XSHG')
    # 设置滑点为理想情况，纯为了跑分好看，实际使用注释掉为好
    set_slippage(FixedSlippage(0.005))  # 设置滑点
    # 设置交易成本
    set_order_cost(OrderCost(open_tax=0, close_tax=0.0005, open_commission=0.0003, close_commission=0.0003,
                             close_today_commission=0, min_commission=5), type='fund')
    # strategy
    g.stock_num = 5

    run_daily(prepare_stock_list, time='9:05', reference_security='000300.XSHG')  # 每日9:05跑一次股票池（不交易）
    run_daily(change_cash, "9:30")  # 检查入金
    run_weekly(my_Trader, 1, time='9:37')  # 每周的第一个交易日的9:31调仓
    run_daily(check_limit_up, time='14:00')  # 每日14:00检查一下持仓股中是否涨幅过大的票


# 1-1 根据最近一年分红除以当前总市值计算股息率并筛选
def get_dividend_ratio_filter_list(context, stock_list, sort, p1, p2):
    time1 = context.previous_date
    time0 = time1 - datetime.timedelta(days=365 * 3)
    # 获取分红数据，由于finance.run_query最多返回4000行，以防未来数据超限，最好把stock_list拆分后查询再组合
    interval = 1000  # 某只股票可能一年内多次分红，导致其所占行数大于1，所以interval不要取满4000
    list_len = len(stock_list)
    # 截取不超过interval的列表并查询
    q = query(finance.STK_XR_XD.code, finance.STK_XR_XD.a_registration_date, finance.STK_XR_XD.bonus_amount_rmb
              ).filter(
        finance.STK_XR_XD.a_registration_date >= time0,
        finance.STK_XR_XD.a_registration_date <= time1,
        finance.STK_XR_XD.code.in_(stock_list[:min(list_len, interval)]))
    df = finance.run_query(q)
    # 对interval的部分分别查询并拼接
    if list_len > interval:
        df_num = list_len // interval
        for i in range(df_num):
            q = query(finance.STK_XR_XD.code, finance.STK_XR_XD.a_registration_date, finance.STK_XR_XD.bonus_amount_rmb
                      ).filter(
                finance.STK_XR_XD.a_registration_date >= time0,
                finance.STK_XR_XD.a_registration_date <= time1,
                finance.STK_XR_XD.code.in_(stock_list[interval * (i + 1):min(list_len, interval * (i + 2))]))
            temp_df = finance.run_query(q)
            df = df.append(temp_df)
    dividend = df.fillna(0)
    dividend = dividend.groupby('code').sum()
    temp_list = list(dividend.index)  # query查询不到无分红信息的股票，所以temp_list长度会小于stock_list
    # 获取市值相关数据
    q = query(valuation.code, valuation.market_cap).filter(valuation.code.in_(temp_list))
    cap = get_fundamentals(q, date=time1)
    cap = cap.set_index('code')
    # 计算股息率
    DR = pd.concat([dividend, cap], axis=1)
    DR['dividend_ratio'] = (DR['bonus_amount_rmb'] / 10000) / DR['market_cap']
    # 排序并筛选
    DR = DR.sort_values(by=['dividend_ratio'], ascending=sort)
    print(len(DR))
    final_list = list(DR.index)[int(p1 * len(DR)):int(p2 * len(DR))]
    return final_list


def my_Trader(context):
    # all stocks
    dt_last = context.previous_date
    stocks = get_all_securities('stock', dt_last).index.tolist()
    choice = filter_kcbj_stock(stocks)
    choice = filter_st_stock(choice)  # 过滤ST
    choice = filter_paused_stock(choice)  # 过滤停牌股票
    choice = filter_limitup_stock(context, choice)  # 过滤涨停的股票
    choice = filter_limitdown_stock(context, choice)  # 过滤跌停的股票
    # 高股息(全市场最大10%)
    stock_list = get_dividend_ratio_filter_list(context, choice, False, 0, 0.1)
    print(stock_list)
    # PEG合理区间(PEG在0.1和2之间的)
    stock_list = peg_stock(context, stock_list, 0.1, 2)
    # 业绩筛选
    q = query(valuation.code).filter(
        valuation.code.in_(stock_list),
        indicator.inc_total_revenue_year_on_year > 3,
        indicator.inc_net_profit_year_on_year > 8,
        indicator.inc_return > 4.5)
    df = get_fundamentals(q)
    stockset = list(df['code'])

    stock_list = stockset[:g.stock_num]  # 选前前N只票
    cdata = get_current_data()
    # Sell
    for s in context.portfolio.positions:
        if (s not in stock_list) and (cdata[s].last_price < cdata[s].high_limit):
            log.info('Sell', s, cdata[s].name)
            order_target_(context, s, 0)
    # buy
    position_count = len(context.portfolio.positions)
    if g.stock_num > position_count:
        psize = (context.portfolio.available_cash + g.out_cash) / (g.stock_num - position_count)
        for s in stock_list:
            if s not in context.portfolio.positions:
                log.info('buy', s, cdata[s].name)
                order_value_(context, s, psize)
                if len(context.portfolio.positions) == g.stock_num:
                    break


# 准备股票池
def prepare_stock_list(context):
    # 获取已持有列表
    g.hold_list = []
    g.high_limit_list = []
    for position in list(context.portfolio.positions.values()):
        stock = position.security
        g.hold_list.append(stock)
    # 获取昨日涨停列表
    if g.hold_list != []:
        for stock in g.hold_list:
            df = get_price(stock, end_date=context.previous_date, frequency='daily', fields=['close', 'high_limit'],
                           count=1)
            if df['close'][0] >= df['high_limit'][0] * 0.98:  # 如果昨天有股票涨停，则放入列表
                g.high_limit_list.append(stock)


#  调整昨日涨停股票
def check_limit_up(context):
    # 获取持仓的昨日涨停列表
    current_data = get_current_data()
    if g.high_limit_list:
        for stock in g.high_limit_list:
            if current_data[stock].last_price < current_data[stock].high_limit:
                log.info("[%s]涨停打开，卖出" % stock)
                order_target_(context, stock, 0)
            else:
                log.info("[%s]涨停，继续持有" % stock)


# 过滤科创北交股票
def filter_kcbj_stock(stock_list):
    for stock in stock_list[:]:
        if stock[0] == '4' or stock[0] == '8' or stock[:2] == '68':
            stock_list.remove(stock)
    return stock_list


# 过滤停牌股票
def filter_paused_stock(stock_list):
    current_data = get_current_data()
    return [stock for stock in stock_list if not current_data[stock].paused]


# 过滤ST及其他具有退市标签的股票
def filter_st_stock(stock_list):
    current_data = get_current_data()
    return [stock for stock in stock_list
            if not current_data[stock].is_st
            and 'ST' not in current_data[stock].name
            and '*' not in current_data[stock].name
            and '退' not in current_data[stock].name]


# 过滤涨停的股票
def filter_limitup_stock(context, stock_list):
    current_data = get_current_data()

    # 已存在于持仓的股票即使涨停也不过滤，避免此股票再次可买，但因被过滤而导致选择别的股票
    return [stock for stock in stock_list if stock in context.portfolio.positions.keys()
            or current_data[stock].day_open < current_data[stock].high_limit]


# 过滤跌停的股票
def filter_limitdown_stock(context, stock_list):
    last_prices = history(1, unit='1m', field='close', security_list=stock_list)
    current_data = get_current_data()

    return [stock for stock in stock_list if stock in context.portfolio.positions.keys()
            or current_data[stock].day_open > current_data[stock].low_limit]


# 2-4 筛选PEG
def peg_stock(context, stock_list, pegmin, pegmax):
    q = query(valuation.code).filter(
        valuation.code.in_(stock_list),
        valuation.pe_ratio / indicator.inc_net_profit_year_on_year > pegmin,
        valuation.pe_ratio / indicator.inc_net_profit_year_on_year < pegmax)
    df = get_fundamentals(q)
    stock_list = list(df['code'])
    return stock_list


def change_cash(context):
    if g.first == 1:
        data1 = {
            '入金金额': 0,
            '是否生效': "否",
            '是否清仓后重新买入': "否"
        }
        df2 = pd.DataFrame([data1], columns=['入金金额', '是否生效', '是否清仓后重新买入'])
        write_file(g.filename, df2.to_csv(index=False), append=False)
        g.first = 0
    if g.out_cash < 0:
        inout_cash(g.out_cash, pindex=0)
        g.out_cash = 0
    else:
        try:

            df2 = pd.read_csv(BytesIO(read_file(g.filename)))
            log.info("入金文件", df2)
            single = df2.at[0, "是否生效"]
            log.info("入金信号", single)
            is_sell = df2.at[0, "是否清仓后重新买入"]
            if single == "是":
                if is_sell == "是":
                    log.info("入金后先清仓，再重新买入，清仓股票为", list(context.portfolio.positions.keys()))
                    for s in context.portfolio.positions:
                        order_target_(context, s, 0)
                df2.loc[0, "是否清仓后重新买入"] = "否"
                change_cash = df2.at[0, "入金金额"]
                log.info("入金金额", change_cash)
                if change_cash < 0:
                    g.out_cash = change_cash
                else:
                    inout_cash(change_cash, pindex=0)
                df2.loc[0, "是否生效"] = "否"
                log.info("重置后", df2)
                my_Trader(context)
                write_file(g.filename, df2.to_csv(index=False), append=False)

        except Exception as e:
            log.info("修改资金发生错误: {}".format(e))

        # end
