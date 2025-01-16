import numpy as np
import pandas as pd
from jqdata import *
from pykalman import KalmanFilter
from scipy.stats import linregress


# 初始化函数
def initialize(context):
    # 设定基准,我这里用纳斯达克ETF作为基准，跑不赢纳斯达克ETF，就代表拉胯，不如直接卖纳斯达克ETF
    set_benchmark('513100.XSHG')
    # 用真实价格交易
    set_option('use_real_price', True)
    # etf盘口一档是0.001，每次成交基本要到第二档，甚至更多档，那么小资金可以设置滑点为0.002，大资金滑点设置为0.005
    set_slippage(FixedSlippage(0.005))
    # 设置交易成本，手续费设置为万二，最低5元
    set_order_cost(
        OrderCost(open_tax=0, close_tax=0, open_commission=0.0002, close_commission=0.0002, close_today_commission=0,
                  min_commission=5), type='fund')

    g.etf_num = 1  # 持有etf的数量，eft本身就代表一揽子股票，没必要持有多只分摊暴雷风险，建议持有1只即可

    g.momentum_type = 1  # 动量计算的方式，1：基于年化收益和判定系数打分的动量因子轮动。2：收益率动量。3：线性回归动量。

    # etf池子
    g.etf_list = [
        '518880.XSHG',  # 黄金ETF（最佳避险大宗商品）
        '513100.XSHG',  # 纳指100（美股最优资产）
        '159915.XSHE',  # 创业板100（成长股）
        '510300.XSHG',  # 沪深300（价值股，中大市值蓝筹股）
        '159628.XSHE',  # 国证2000（小盘股）
        # '510050.XSHG', #上证50
        # '510180.XSHG', #上证180
        # '161725.XSHE', #白酒
        # '159992.XSHE', #创新药
        # '560080.XSHG', #中药
        # '515700.XSHG', #新能源车
        # '515250.XSHG', #智能汽车
        # '515790.XSHG', #光伏
        # '515880.XSHG', #通信
        # '159819.XSHE', #人工智能
        # '512720.XSHG', #计算机（云计算，大数据）
        # '159740.XSHE', #恒生科技
        # '159985.XSHE', #豆粕
        # '162411.XSHE', #华宝油气
        # '159981.XSHE', #能源化工ETF

    ]

    run_daily(trade, '9:50')  # 每天运行


# 交易
def trade(context):
    if g.momentum_type == 1:
        target_list = get_rank1(g.etf_list)[:g.etf_num]
    if g.momentum_type == 2:
        target_list = get_rank2(g.etf_list)[:g.etf_num]
    if g.momentum_type == 3:
        target_list = get_rank3(g.etf_list)[:g.etf_num]

    # 检查均线条件
    hold_ratio = check_moving_average_condition(target_list, context)

    # 卖出
    hold_list = list(context.portfolio.positions)
    for etf in hold_list:
        if etf not in target_list:
            order_target_value(etf, 0)
            print('卖出' + str(etf))
        else:
            print('继续持有' + str(etf))

    # 买入目标ETF
    hold_list = list(context.portfolio.positions)
    if len(target_list) > 0:
        available_cash = context.portfolio.available_cash * hold_ratio
        if len(hold_list) < g.etf_num:
            value = available_cash / (g.etf_num - len(hold_list))
            for etf in target_list:
                if context.portfolio.positions[etf].total_amount == 0:
                    order_target_value(etf, value)
                    print(f"买入 {etf}")
    else:
        print('继续空仓')


# 动量因子1：基于年化收益和判定系数打分的动量因子轮动
def get_rank1(etf_list):
    momentum_day = 25  # 动量的天数，设置的太短轮动时间会很快，交易过于频繁，设置太长，灵敏度又很差，一个月左右较为合适。
    score_list = []
    for etf in etf_list:
        df = attribute_history(etf, momentum_day, '1d', ['close'])
        y = df['log'] = np.log(df.close)
        x = df['num'] = np.arange(df.log.size)
        slope, intercept = np.polyfit(x, y, 1)
        annualized_returns = math.pow(math.exp(slope), 250) - 1
        r_squared = 1 - (sum((y - (slope * x + intercept)) ** 2) / ((len(y) - 1) * np.var(y, ddof=1)))
        score = annualized_returns * r_squared
        score_list.append(score)
    df = pd.DataFrame(index=etf_list, data={'score': score_list})
    df = df.sort_values('score', ascending=False)
    print(df)
    rank_list = list(df.index)
    return rank_list


# 动量因子2：收益率动量
def get_rank2(etf_list):
    momentum_day = 25  # 动量的天数，设置的太短轮动时间会很快，交易过于频繁，设置太长，灵敏度又很差，一个月左右较为合适。
    score_list = []
    for stock in etf_list:
        df = attribute_history(stock, momentum_day, '1d', ['close'])
        score = np.polyfit(np.arange(momentum_day), df.close / df.close[0], 1)[0].real  # 乖离动量拟合
        score_list.append(score)
    df = pd.DataFrame(index=etf_list, data={'score': score_list})
    df = df.sort_values('score', ascending=False)
    print(df)
    rank_list = list(df.index)
    return rank_list


# 动量因子3：线性回归动量
def get_rank3(etf_list):
    momentum_day = 25  # 动量的天数，设置的太短轮动时间会很快，交易过于频繁，设置太长，灵敏度又很差，一个月左右较为合适。
    score_list = []
    x = np.arange(momentum_day)
    for etf in etf_list:
        y = attribute_history(etf, momentum_day, '1d', ['close'])['close']
        slope, intercept, r_value, p_value, std_err = linregress(x, y)
        score = slope
        score_list.append(score)

    df = pd.DataFrame(index=etf_list, data={'score': score_list})
    df = df.sort_values('score', ascending=False)
    print(df)
    rank_list = list(df.index)
    return rank_list


# 用来计算ETF昨日涨幅，暂时没用
def zhangfu(etf):
    df = attribute_history(etf, 2, '1d', ['close'])

    prepreday_close_price = df['close'].tolist()
    prepreday_close_price = prepreday_close_price[0]

    preday_close_price = df['close'].tolist()
    preday_close_price = preday_close_price[1]

    zf = (preday_close_price - prepreday_close_price) / prepreday_close_price

    return zf


def check_moving_average_condition(target_list, context):
    """
    检查均线条件：
    - 如果前一日收盘价在60日线之上，且60日线 > 120日线，120日线 > 250日线，则返回 1.0 （满仓）。
    - 否则返回 0.3 （30%仓位）。
    """
    for etf in target_list:
        # 获取历史数据
        df = attribute_history(etf, 250, '1d', ['close'])
        if len(df) < 250:
            continue  # 如果数据不足，跳过当前ETF

        ma60 = df['close'][-60:].mean()
        ma120 = df['close'][-120:].mean()
        ma250 = df['close'].mean()
        last_close = df['close'][-1]

        # 检查条件
        if last_close > ma60 and ma60 > ma120 and ma120 > ma250:
            print(f"{etf}: 满足满仓条件")
            return 1.0  # 满仓
        else:
            print(f"{etf}: 不满足满仓条件，调整为30%仓位")
            return 0.3  # 30%仓位

    # 如果所有ETF都不满足条件，默认30%仓位
    return 0.3
