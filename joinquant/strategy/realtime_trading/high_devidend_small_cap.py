# 高股息小市值

import pandas as pd
import numpy as np
import jqdata
from kuanke.wizard import *
from six import StringIO, BytesIO

from mysqltrade import *
from jqfactor import get_factor_values


def after_code_changed(context):
    now = context.current_dt.time()
    print("{0}替换了代码".format(now))
    unschedule_all()
    g.strategy = 'dougua1'  # 策略名
    g.filename = '高股息小市值修改资金.csv'  # 入金配置文件名称

    g.first = 1
    g.out_cash = 0
    g.high_limit_list = []
    set_option('use_real_price', True)
    set_benchmark('000905.XSHG')
    # 模拟的时候，有滑点会导致没跌停也卖不掉的情况，所以滑点设置为0
    set_slippage(PriceRelatedSlippage(0))
    # 设置交易成本
    set_order_cost(OrderCost(open_tax=0, close_tax=0.0005, open_commission=0.0003, close_commission=0.0003,
                             close_today_commission=0, min_commission=5), type='fund')
    # strategy
    g.stock_num = 10
    g.stoploss_rate = 0.90  # 止损率为10%

    # 单日放量超过过去120天中最大量能的90%，就卖出
    g.HV_duration = 120
    g.HV_ratio = 0.9

    g.not_buy_again = []
    g.target_list = []

    g.reason_to_sell = 0

    run_daily(prepare_stock_list, time='9:05', reference_security='000300.XSHG')  # 每日9:05跑一次股票池（不交易）
    run_daily(change_cash, "9:30")  # 检查入金
    run_daily(no_trade, time='9:31')
    run_weekly(my_Trader, 2, time='9:39')  # 每周的第2个交易日的9:39调仓
    run_daily(stoploss, time='10:03')  # 止损函数
    run_daily(check_limit_up, time='14:25')  # 每日14:25检查一下持仓股中是否有涨停票
    run_daily(check_high_volume, time='14:47')  # 每日14:47检查一下是否异常放量，如果异常放量就卖出


# 1-1 根据最近一年分红除以当前总市值计算股息率并筛选
def get_dividend_ratio_filter_list(context, stock_list, sort, p1, p2):
    time1 = context.previous_date
    time0 = time1 - datetime.timedelta(days=365)
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
    # dividend = dividend.set_index('code')
    # print(dividend)
    dividend = dividend.groupby('code').sum()
    # print(dividend)
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


def no_trade(context):
    mon = context.current_dt.month
    day = context.current_dt.day
    if (mon == 4 or mon == 1) and day >= 19:
        s_list = list(context.portfolio.positions.keys())
        print(s_list)
        for s in s_list:
            order_target_value_(context, s, 0)
    else:
        check_remain_amount(context)


def my_Trader(context):
    mon = context.current_dt.month
    day = context.current_dt.day
    if (mon == 4 or mon == 1) and day >= 1:
        s_list = list(context.portfolio.positions.keys())
        print(s_list)
        for s in s_list:
            order_target_value_(context, s, 0)
        return

    # all stocks
    dt_last = context.previous_date
    stocks = get_all_securities('stock', dt_last).index.tolist()
    stocks = filter_kcbj_stock(stocks)
    # 高股息(全市场最大25%)
    stocks = get_dividend_ratio_filter_list(context, stocks, False, 0, 0.25)
    df = get_factor_values(stocks, "operating_revenue_ttm", end_date=dt_last, count=1)
    df = df["operating_revenue_ttm"].T

    df.columns = ["operating_revenue_ttm"]

    df = df[df["operating_revenue_ttm"] > 500000000]
    stocks = list(df.index)
    # fuandamental data，按照市值排序，市值越小排在越靠前
    df = get_fundamentals(
        query(valuation.code, valuation.market_cap, indicator.eps).filter(valuation.code.in_(stocks)).order_by(
            valuation.market_cap.asc()))
    df = df[df['eps'] > 0]

    choice = list(df.code)
    choice = filter_st_stock(choice)  # 过滤ST
    choice = filter_paused_stock(choice)  # 过滤停牌股票
    choice = filter_limitup_stock(context, choice)  # 过滤涨停的股票
    choice = filter_limitdown_stock(context, choice)  # 过滤跌停的股票
    choice = filter_highprice_stock(context, choice)  # 过滤股价高于9元的股票
    g.target_list = choice
    choice = choice[:g.stock_num]  # 选前前N只票
    cdata = get_current_data()
    # Sell
    for s in context.portfolio.positions:
        if (s not in choice):
            log.info('Sell', s, cdata[s].name)
            order_target_(context, s, 0)
    # buy
    g.not_buy_again = []
    position_count = len(context.portfolio.positions)
    if g.stock_num > position_count:
        psize = (context.portfolio.available_cash + g.out_cash) / (g.stock_num - position_count)
        for s in choice:
            if s not in context.portfolio.positions:
                log.info('buy', s, cdata[s].name)
                order_value_(context, s, psize)
                g.not_buy_again.append(s)
                if len(context.portfolio.positions) == g.stock_num:
                    break


# 准备股票池
def prepare_stock_list(context):
    # 获取已持有列表
    g.high_limit_list = []
    hold_list = list(context.portfolio.positions)
    if hold_list:
        panel = get_price(hold_list, end_date=context.previous_date, frequency='daily',
                          fields=['close', 'high_limit'],
                          count=1)

        df_close = panel['close'].T
        df_close.columns = ['close']
        print(df_close)
        df_high_limit = panel['high_limit'].T
        df_high_limit.columns = ['high_limit']
        print(df_high_limit)
        df = pd.concat([df_close, df_high_limit], axis=1)
        print(df)
        g.high_limit_list = df[df['close'] == df['high_limit']].index.tolist()


#  调整昨日涨停股票
def check_limit_up(context):
    # 获取持仓的昨日涨停列表
    current_data = get_current_data()
    if g.high_limit_list:
        for stock in g.high_limit_list:
            if current_data[stock].last_price < current_data[stock].high_limit:
                log.info("[%s]涨停打开，卖出" % stock)
                g.reason_to_sell = 1
                order_target_(context, stock, 0)
            else:
                log.info("[%s]涨停，继续持有" % stock)


def stoploss(context):
    for stock in context.portfolio.positions.keys():
        if context.portfolio.positions[stock].price < context.portfolio.positions[stock].avg_cost * g.stoploss_rate:
            order_target_value_(context, stock, 0)
            log.debug("止损,卖出{}".format(stock))


# 如果昨天有股票卖出或者买入失败，剩余的金额今天早上买入
def check_remain_amount(context):
    if g.reason_to_sell == 1:  # 判断提前售出原因，如果是涨停售出则次日再次交易，如果是止损售出则不交易
        g.hold_list = []
        for position in list(context.portfolio.positions.values()):
            stock = position.security
            g.hold_list.append(stock)
        if len(g.hold_list) < g.stock_num:
            target_list = g.target_list
            # 剔除本次调仓买入的股票，不再买入
            target_list = [stock for stock in target_list if stock not in g.not_buy_again]
            target_list = target_list[:min(g.stock_num, len(target_list))]
            log.info('有余额可用' + str(round((context.portfolio.cash), 2)) + '元。' + str(target_list))

            value = (context.portfolio.cash + g.out_cash) / (g.stock_num - len(g.hold_list))
            for stock in target_list:
                if context.portfolio.positions[stock].total_amount == 0:
                    order_target_value_(context, stock, value)
                    g.not_buy_again.append(stock)
                    if len(context.portfolio.positions) == g.stock_num:
                        break

        g.reason_to_sell = 0
    else:
        log.info('虽然有余额可用，但是为止损后余额，下周再交易')
        g.reason_to_sell = 0


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


# 2-4 过滤股价高于9元的股票
def filter_highprice_stock(context, stock_list):
    last_prices = history(1, unit='1m', field='close', security_list=stock_list)
    return [stock for stock in stock_list if stock in context.portfolio.positions.keys()
            or last_prices[stock][-1] < 9]


# 3-2 调整放量股票
def check_high_volume(context):
    current_data = get_current_data()
    for stock in context.portfolio.positions:
        if current_data[stock].paused == True:
            continue
        if current_data[stock].last_price == current_data[stock].high_limit:
            continue
        if context.portfolio.positions[stock].closeable_amount == 0:
            continue
        df_volume = get_bars(stock, count=g.HV_duration, unit='1d', fields=['volume'], include_now=True, df=True)
        if df_volume['volume'].values[-1] > g.HV_ratio * df_volume['volume'].values.max():
            log.info("[%s]天量，卖出" % stock)
            position = context.portfolio.positions[stock]
            order_target_value_(context, stock, 0)


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
