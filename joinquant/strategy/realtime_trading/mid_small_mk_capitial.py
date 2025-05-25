# 中小板小市值-国九升级版-1月和4月空仓-交易时间随机

from kuanke.wizard import *
from jqdata import *
import numpy as np
import pandas as pd
import talib
import datetime
import time
import math
from mysqltrade import *
from six import StringIO, BytesIO
import pandas as pd


## 初始化函数，设定要操作的股票、基准等等
def after_code_changed(context):
    now = context.current_dt.time()
    print("{0}替换了代码".format(now))
    unschedule_all()
    g.strategy = 'dougua1'  # 策略名
    g.filename = '中小板修改资金.csv'  # 入金配置文件名称

    g.first = 1
    g.out_cash = 0
    g.paused = []
    g.check_out_lists = []
    # 设定基准
    set_benchmark('000300.XSHG')
    # 设定滑点
    set_slippage(FixedSlippage(0))
    # True为开启动态复权模式，使用真实价格交易
    set_option('use_real_price', True)
    # 设定成交量比例
    set_option('order_volume_ratio', 1)
    # 股票类交易手续费是：买入时佣金万分之三，卖出时佣金万分之三加千分之一印花税, 每笔交易佣金最低扣5块钱
    set_order_cost(
        OrderCost(open_tax=0, close_tax=0.0005, open_commission=0.0003, close_commission=0.0003, min_commission=5),
        type='stock')

    # 容器初始化
    check_container_initialize()
    # 动态仓位、频率、计数初始化函数
    check_dynamic_initialize()
    # 股票筛选初始化函数
    check_stocks_initialize()
    # 出场初始化函数
    sell_initialize()
    # 入场初始化函数
    buy_initialize()
    # 计算时间范围内的分钟数
    start_minute = 9 * 60 + 35  # 9点35分转换为一天中的分钟数
    end_minute = 9 * 60 + 48  # 9点48分转换为一天中的分钟数
    # 生成随机分钟数
    random_minute = random.randint(start_minute, end_minute)
    # 将随机分钟数转换回小时和分钟
    random_hour = random_minute // 60
    random_minute = random_minute % 60
    g.trade_time = f"{random_hour}:{random_minute}"
    print("trade_time", g.trade_time)
    # 关闭提示
    log.set_level('order', 'info')

    # 运行函数
    run_daily(change_cash, "9:30")  # 检查入金
    run_daily(check_stocks, '9:31')  # 选股
    run_daily(main_stock_pick, g.trade_time)  # 买入卖出列表
    run_daily(trade, g.trade_time)  # 交易
    run_daily(no_zt_sell, '14:46')
    run_daily(selled_security_list_count, 'after_close')  # 卖出股票日期计数
    run_daily(after_market_close, 'after_close')  # 卖出股票日期计数


#######################！！！新手需要使用的地方！！！###################################################
##动态仓位、频率、计数初始化函数(持仓比例，选股频率，买入频率，卖出频率在这里设置)
def check_dynamic_initialize():
    # 个股最大持仓比重
    g.security_max_proportion = 1
    # 选股和买卖频率
    g.check_stocks_refresh_rate = 1
    # 最大建仓数量
    g.max_hold_stocknum = 10
    # 跌出多少排名卖出
    g.sell_rank = 12

    g.notrade_mon = [1, 4]  # 1月和4月空仓
    g.notrade_day = 1  # 1月和4月的1号后空仓
    # 下面这几项不用管
    # 买入频率
    g.buy_refresh_rate = 1
    # 卖出频率
    g.sell_refresh_rate = 1

    # 选股频率计数器
    g.check_stocks_days = 1
    # 机器学习选股频率计数器
    g.days = 0
    # 买卖交易频率计数器
    g.buy_trade_days = 0
    g.sell_trade_days = 0


## 股票池初筛设置函数(股票初筛在这里设置)
def check_stocks_initialize():
    # 是否过滤停盘
    g.filter_paused = True
    # 是否过滤退市
    g.filter_delisted = True
    # 是否只有ST
    g.only_st = False
    # 是否过滤ST
    g.filter_st = True
    # 股票池(填指数)
    g.security_universe_index = [
        'all_a_securities']  # 这里填写指数，全部股票就填['all_a_securities']，沪深300股票就填['000300.XSHG'],中证500就填['000905.XSHG'],沪深300+中证500就填['000300.XSHG','000905.XSHG']
    # 填成分股(填成分股)
    g.security_universe_user_securities = []
    # 行业列表
    g.industry_list = ["801010", "801020", "801030", "801040", "801050", "801080", "801110", "801120", "801130",
                       "801140", "801150", "801160", "801170", "801180", "801200", "801210", "801230", "801710",
                       "801720", "801730", "801740", "801750", "801760", "801770", "801780", "801790", "801880",
                       "801890"]
    # 概念列表
    g.concept_list = []
    # 黑名单
    g.blacklist = []
    g.notbuy = []


## 买入股票，卖出股票筛选函数
def main_stock_pick(context):
    if g.days % g.check_stocks_refresh_rate != 0:
        g.days += 1
        return
    g.sell_stock_list = []
    g.buy_stock_list = []

    mon = context.current_dt.month
    day = context.current_dt.day
    if (mon in g.notrade_mon) and day >= g.notrade_day:
        s_list = list(context.portfolio.positions.keys())
        print(s_list)
        for s in s_list:
            order_target_value_(context, s, 0)
        return

    ####自定义编辑范围#####

    # 中小板小市值

    g.check_out_lists = [stock for stock in g.check_out_lists if stock[0:1] == '0']  # 只买深市A股中小板

    q = query(
        valuation.code,
    ).filter(
        valuation.code.in_(g.check_out_lists),
        income.np_parent_company_owners > 0,  # 归属于母公司所有者的净利润(元)
        income.net_profit > 0,  # 净利润(元)
        indicator.adjusted_profit > 0,  # 扣非净利润（元）
        income.operating_revenue > 1e8  # 营业收入 (元)
    ).order_by(valuation.market_cap.asc()).limit(g.max_hold_stocknum * 5)
    df = get_fundamentals(q)
    final_list = df['code'].tolist()
    stockset = final_list[:20]
    stockset = [stock for stock in stockset if stock not in g.notbuy]  # 过滤卖掉的股票

    g.sell_stock_list1 = list(context.portfolio.positions.keys())

    g.ZT = []  # 涨停列表

    for stock in g.sell_stock_list1:

        df2 = attribute_history(stock, count=2, unit='1d', \
                                fields=['open', 'close', 'volume', 'high', 'low', 'money', 'high_limit', 'low_limit',
                                        'paused'], \
                                fq='pre')

        if stock in g.paused:
            continue
        elif df2['close'][-1] == df2['high_limit'][-1]:
            g.ZT.append(stock)

        elif (stock not in stockset[:g.sell_rank]) and (stock not in g.ZT):
            g.sell_stock_list.append(stock)

    for stock in stockset[:g.max_hold_stocknum]:
        if stock in g.sell_stock_list:
            pass
        else:
            g.buy_stock_list.append(stock)

    g.paused = []
    current_data = get_current_data()
    g.paused = [stock for stock in g.sell_stock_list1 if current_data[stock].paused]

    ####自定义编辑范围#####

    # log.info('卖出列表:',g.sell_stock_list)
    # log.info('购买列表:',g.buy_stock_list)
    g.days = 1
    return g.sell_stock_list, g.buy_stock_list


#######################！！！新手需要使用的地方！！！###################################################

# 不涨停就卖出

def no_zt_sell(context):
    c = []
    g.notbuy = []
    for stock in g.ZT:
        df2_3 = attribute_history(stock, count=1, unit='1m', fields=['close', 'high_limit'], fq='pre')
        if df2_3['close'][-1] == df2_3['high_limit'][-1]:
            c.append(stock)
        else:
            order_target_(context, stock, 0)
            g.notbuy.append(stock)


## 收盘后运行函数
def after_market_close(context):
    # 得到当天所有成交记录
    trades = get_trades()
    for _trade in trades.values():
        log.info('成交记录：' + str(_trade))
    # 打印账户总资产
    log.info('今日账户总资产：%s' % round(context.portfolio.total_value, 2))
    # log.info('##############################################################')
    # 计算当前仓位
    record(P=(100 - math.ceil(context.portfolio.available_cash / context.portfolio.total_value * 100)))


##容器初始化(有新的全局容器可以加到这里)(新手忽略这里)
def check_container_initialize():
    # 卖出股票列表
    g.sell_stock_list = []
    # 买入股票列表
    g.buy_stock_list = []
    # 获取未卖出的股票
    g.open_sell_securities = []
    # 卖出股票的dict
    g.selled_security_list = {}
    # 涨停股票列表
    g.ZT = []


## 出场初始化函数(新手忽略这里)
def sell_initialize():
    # 设定是否卖出buy_lists中的股票
    g.sell_will_buy = True

    # 固定出仓的数量或者百分比
    g.sell_by_amount = None
    g.sell_by_percent = None


## 入场初始化函数(新手忽略这里)
def buy_initialize():
    # 是否可重复买入
    g.filter_holded = False

    # 委托类型
    g.order_style_str = 'by_cap_mean'
    g.order_style_value = 100


## 股票初筛(新手忽略这里)
def check_stocks(context):
    if g.check_stocks_days % g.check_stocks_refresh_rate != 0:
        # 计数器加一
        g.check_stocks_days += 1
        return
    # 股票池赋值
    g.check_out_lists = get_security_universe(context, g.security_universe_index, g.security_universe_user_securities)
    # 行业过滤
    # g.check_out_lists = industry_filter(context, g.check_out_lists, g.industry_list)
    # 概念过滤
    # g.check_out_lists = concept_filter(context, g.check_out_lists, g.concept_list)
    # 过滤ST股票
    g.check_out_lists = st_filter(context, g.check_out_lists)
    # 过滤停牌股票
    g.check_out_lists = paused_filter(context, g.check_out_lists)
    # 过滤退市股票
    g.check_out_lists = delisted_filter(context, g.check_out_lists)
    # 过滤黑名单股票
    g.check_out_lists = [s for s in g.check_out_lists if s not in g.blacklist]
    # 计数器归一
    g.check_stocks_days = 1
    return


## 卖出未卖出成功的股票(新手忽略这里)
def sell_every_day(context):
    g.open_sell_securities = list(set(g.open_sell_securities))
    open_sell_securities = [s for s in context.portfolio.positions.keys() if s in g.open_sell_securities]
    if len(open_sell_securities) > 0:
        for stock in open_sell_securities:
            order_target_value_(context, stock, 0)
    g.open_sell_securities = [s for s in g.open_sell_securities if s in context.portfolio.positions.keys()]
    return


## 交易函数(新手忽略这里)
def trade(context):
    # 初始化买入列表
    buy_lists = []

    # 买入股票筛选
    if g.buy_trade_days % g.buy_refresh_rate == 0:
        # 获取 buy_lists 列表
        buy_lists = g.buy_stock_list
        # 过滤涨停股票
        buy_lists = high_limit_filter(context, buy_lists)
        # log.info('购买列表最终',buy_lists)

    # 卖出操作
    if g.sell_trade_days % g.sell_refresh_rate != 0:
        # 计数器加一
        g.sell_trade_days += 1
    else:
        # 卖出股票
        sell(context, buy_lists)
        # 计数器归一
        g.sell_trade_days = 1

    # 买入操作
    if g.buy_trade_days % g.buy_refresh_rate != 0:
        # 计数器加一
        g.buy_trade_days += 1
    else:
        # 卖出股票
        buy(context, buy_lists)
        # 计数器归一
        g.buy_trade_days = 1


##################################  交易函数群 ##################################(新手忽略)

# 交易函数 - 出场
def sell(context, buy_lists):
    # 获取 sell_lists 列表
    init_sl = context.portfolio.positions.keys()
    sell_lists = context.portfolio.positions.keys()

    # 判断是否卖出buy_lists中的股票
    if not g.sell_will_buy:
        sell_lists = [security for security in sell_lists if security not in buy_lists]

    ### _出场函数筛选-开始 ###
    sell_lists = g.sell_stock_list
    ### _出场函数筛选-结束 ###

    # 卖出股票
    if len(sell_lists) > 0:
        for stock in sell_lists:
            order_target_(context, stock, 0)

    # 获取卖出的股票, 并加入到 g.selled_security_list中
    selled_security_list_dict(context, init_sl)

    return


# 交易函数 - 入场
def buy(context, buy_lists):
    # 判断是否可重复买入
    buy_lists = holded_filter(context, buy_lists)

    # 获取最终的 buy_lists 列表
    Num = g.max_hold_stocknum - len(context.portfolio.positions)
    buy_lists = buy_lists[:Num]

    # 买入股票
    if len(buy_lists) > 0:
        # 分配资金
        for stock in buy_lists:
            position_count = len(context.portfolio.positions)
            if g.max_hold_stocknum > position_count:
                value = (context.portfolio.cash + g.out_cash) / (g.max_hold_stocknum - position_count)
                if context.portfolio.positions[stock].total_amount == 0:
                    order_target_value_(context, stock, value)
    return


###################################  公用函数群 ##################################(新手忽略)


## 过滤同一标的继上次卖出N天不再买入
def filter_n_tradeday_not_buy(security, n=0):
    try:
        if (security in g.selled_security_list.keys()) and (g.selled_security_list[security] < n):
            return False
        return True
    except:
        return True


## 是否可重复买入
def holded_filter(context, security_list):
    if not g.filter_holded:
        security_list = [stock for stock in security_list if stock not in context.portfolio.positions.keys()]
    # 返回结果
    return security_list


## 卖出股票加入dict
def selled_security_list_dict(context, security_list):
    selled_sl = [s for s in security_list if s not in context.portfolio.positions.keys()]
    if len(selled_sl) > 0:
        for stock in selled_sl:
            g.selled_security_list[stock] = 0


## 过滤停牌股票
def paused_filter(context, security_list):
    if g.filter_paused:
        current_data = get_current_data()
        security_list = [stock for stock in security_list if not current_data[stock].paused]
    # 返回结果
    return security_list


## 过滤退市股票
def delisted_filter(context, security_list):
    if g.filter_delisted:
        current_data = get_current_data()
        security_list = [stock for stock in security_list if
                         not (('退' in current_data[stock].name) or ('*' in current_data[stock].name))]
    # 返回结果
    return security_list


## 过滤ST股票
def st_filter(context, security_list):
    if g.only_st:
        current_data = get_current_data()
        security_list = [stock for stock in security_list if current_data[stock].is_st]
    else:
        if g.filter_st:
            current_data = get_current_data()
            security_list = [stock for stock in security_list if not current_data[stock].is_st]
    # 返回结果
    return security_list


# 过滤涨停股票
def high_limit_filter(context, security_list):
    current_data = get_current_data()
    security_list = [stock for stock in security_list if
                     not (current_data[stock].last_price >= current_data[stock].high_limit)]
    # 返回结果
    return security_list


# 获取股票股票池
def get_security_universe(context, security_universe_index, security_universe_user_securities):
    temp_index = []
    for s in security_universe_index:
        if s == 'all_a_securities':
            temp_index += list(get_all_securities(['stock'], context.current_dt.date()).index)
        else:
            temp_index += get_index_stocks(s)
    for x in security_universe_user_securities:
        temp_index += x
    return sorted(list(set(temp_index)))


# 行业过滤
def industry_filter(context, security_list, industry_list):
    if len(industry_list) == 0:
        # 返回股票列表
        return security_list
    else:
        securities = []
        for s in industry_list:
            temp_securities = get_industry_stocks(s)
            securities += temp_securities
        security_list = [stock for stock in security_list if stock in securities]
        # 返回股票列表
        return security_list


# 概念过滤
def concept_filter(context, security_list, concept_list):
    if len(concept_list) == 0:
        return security_list
    else:
        securities = []
        for s in concept_list:
            temp_securities = get_concept_stocks(s)
            securities += temp_securities
        security_list = [stock for stock in security_list if stock in securities]
        # 返回股票列表
        return security_list


## 卖出股票日期计数
def selled_security_list_count(context):
    # g.daily_risk_management = True
    if len(g.selled_security_list) > 0:
        for stock in g.selled_security_list.keys():
            g.selled_security_list[stock] += 1


# 获取交易日
def shifttradingday(date, shift):
    # 获取N天前的交易日日期
    # 获取所有的交易日，返回一个包含所有交易日的 list,元素值为 datetime.date 类型.
    tradingday = get_all_trade_days()
    # 得到date之后shift天那一天在列表中的行标号 返回一个数
    shiftday_index = list(tradingday).index(date) + shift
    # 根据行号返回该日日期 为datetime.date类型
    return tradingday[shiftday_index]


# 2.1 筛选审计意见
def filter_audit(context, code):
    # 获取审计意见，近三年内如果有不合格(report_type为2、3、4、5)的审计意见则返回False，否则返回True
    lstd = context.previous_date
    last_year = lstd.replace(year=lstd.year - 3, month=1, day=1)
    q = query(finance.STK_AUDIT_OPINION.code, finance.STK_AUDIT_OPINION.report_type
              ).filter(finance.STK_AUDIT_OPINION.code == code, finance.STK_AUDIT_OPINION.pub_date >= last_year)
    df = finance.run_query(q)
    df['report_type'] = df['report_type'].astype(str)
    contains_nums = df['report_type'].str.contains(r'2|3|4|5')
    return not contains_nums.any()


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
                write_file(g.filename, df2.to_csv(index=False), append=False)

        except Exception as e:
            log.info("修改资金发生错误: {}".format(e))

        # end
