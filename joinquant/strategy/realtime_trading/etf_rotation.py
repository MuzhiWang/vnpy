# ETF动量轮动

import numpy as np
import pandas as pd
from jqdata import *
from pykalman import KalmanFilter
from scipy.stats import linregress
from six import StringIO, BytesIO
from mysqltrade import *


## 初始化函数，设定要操作的股票、基准等等
def after_code_changed(context):
    now = context.current_dt.time()
    print("{0}替换了代码".format(now))
    unschedule_all()
    g.strategy = 'dougua1'  # 策略名
    g.filename = 'ETF动量轮动.csv'  # 入金配置文件名称

    g.first = 1
    g.out_cash = 0
    g.paused = []
    g.check_out_lists = []
    # 设定基准,我这里用纳斯达克ETF作为基准，跑不赢纳斯达克ETF，就代表拉胯，不如直接卖纳斯达克ETF
    set_benchmark('513100.XSHG')
    # 用真实价格交易
    set_option('use_real_price', True)
    # etf盘口一档是0.001，每次成交基本要到第二档，甚至更多档，那么小资金可以设置滑点为0.002，大资金滑点设置为0.005
    set_slippage(FixedSlippage(0))
    # 设置交易成本，手续费设置为万二，最低5元
    set_order_cost(
        OrderCost(open_tax=0, close_tax=0, open_commission=0.0002, close_commission=0.0002, close_today_commission=0,
                  min_commission=5), type='fund')

    g.strategy = 'dougua1'  # 策略名

    g.etf_num = 1  # 持有etf的数量，eft本身就代表一揽子股票，没必要持有多只分摊暴雷风险，建议持有1只即可

    g.momentum_type = 1  # 动量计算的方式，1：基于年化收益和判定系数打分的动量因子轮动。2：收益率动量。3：线性回归动量。

    # etf池子
    g.etf_list = [
        '518880.XSHG',  # 黄金ETF（最佳避险大宗商品）
        '513100.XSHG',  # 纳指100（美股最优资产）
        '159915.XSHE',  # 创业板100（成长股）
        # '512890.XSHG', #红利低波 （高股息）
        # '511010.XSHG', #国债ETF
        # '510300.XSHG', #沪深300（价值股，中大市值蓝筹股）
        # '512100.XSHG', #中证1000（小盘股）
        # '510050.XSHG', #上证50
        '510180.XSHG',  # 上证180
        # '161725.XSHE', #白酒
        # '159992.XSHE', #创新药
        # '560080.XSHG', #中药
        # '515700.XSHG', #新能源车
        # '515250.XSHG', #智能汽车
        # '515790.XSHG', #光伏
        # '515880.XSHG', #通信
        # '159819.XSHE', #人工智能
        # '512720.XSHG', #计算机（云计算，大数据）
        # '513030.XSHG', #德国DAX
        '159740.XSHE',  # 恒生科技
        # '159985.XSHE', #豆粕
        # '162411.XSHE', #华宝油气
        # '159981.XSHE', #能源化工ETF
        '159628.XSHE',  # 国证2000
        # '588080.XSHG'  #科创板

    ]

    run_daily(change_cash, "9:30")  # 检查入金
    run_daily(trade, '9:41')  # 每天运行


# 交易
def trade(context):
    if g.momentum_type == 1:
        target_list = get_rank1(g.etf_list)[:g.etf_num]
    if g.momentum_type == 2:
        target_list = get_rank2(g.etf_list)[:g.etf_num]
    if g.momentum_type == 3:
        target_list = get_rank3(g.etf_list)[:g.etf_num]
    if g.momentum_type == 4:
        target_list = get_rank3(g.etf_list)[:g.etf_num]
        # 卖出
    hold_list = list(context.portfolio.positions)
    for etf in hold_list:
        if etf not in target_list:
            order_target_value_(context, etf, 0)
            print('卖出' + str(etf))
        else:
            print('继续持有' + str(etf))

    # 买入
    hold_list = list(context.portfolio.positions)
    if len(target_list) > 0:
        if len(hold_list) < g.etf_num:
            value = context.portfolio.available_cash / (g.etf_num - len(hold_list))
            for etf in target_list:
                if context.portfolio.positions[etf].total_amount == 0:
                    order_target_value_(context, etf, value)
                    print('买入' + str(etf))
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


# 动量因子4：增加近期权重
def get_rank4(etf_list):
    momentum_day = 25
    scores = {}
    current_data = get_current_data()
    for etf in etf_list:
        df = attribute_history(etf, momentum_day + 1, "1d", ["close"])
        prices = df["close"].values
        # prices = np.append(df["close"].values, current_data[etf].last_price)
        y = np.log(prices)
        x = np.arange(len(y))
        weights = np.linspace(1, 2, len(y))
        slope, intercept = np.polyfit(x, y, 1, w=weights)
        annualized_returns = math.exp(slope * 250) - 1
        # 计算R²
        ss_res = np.sum(weights * (y - (slope * x + intercept)) ** 2)
        ss_tot = np.sum(weights * (y - np.mean(y)) ** 2)
        r2 = 1 - ss_res / ss_tot if ss_tot else 0
        scores[etf] = annualized_returns * r2

    print(scores)

    # 只保留得分在 (0, 5] 的ETF，并按得分降序排列
    rank_list = sorted(
        [etf for etf, score in scores.items() if -10 < score <= 10],
        key=lambda x: scores[x],
        reverse=True,
    )
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
