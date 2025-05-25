# 动量大类资产配置策略

import numpy as np
import pandas as pd
import datetime
from jqdata import *
from six import StringIO, BytesIO
from mysqltrade import *


## 初始化函数，设定要操作的股票、基准等等
def after_code_changed(context):
    now = context.current_dt.time()
    print("{0}替换了代码".format(now))
    unschedule_all()
    g.strategy = 'dougua1'  # 策略名
    g.filename = '动量大类资产配置策略.csv'  # 入金配置文件名称

    g.first = 1
    g.out_cash = 0
    g.paused = []
    g.check_out_lists = []

    # 设定基准
    set_benchmark('513500.XSHG')
    # 用真实价格交易
    set_option('use_real_price', True)
    # etf盘口一档是0.001，每次成交基本要到第二档，甚至更多档，那么小资金可以设置滑点为0.002，大资金滑点设置为0.005
    set_slippage(FixedSlippage(0.005))
    # 设置交易成本，手续费设置为万二，最低5元
    set_order_cost(
        OrderCost(open_tax=0, close_tax=0, open_commission=0.0002, close_commission=0.0002, close_today_commission=0,
                  min_commission=5), type='fund')

    g.stock_etf_num = 2  # 持有股票etf的数量
    g.bond_etf_num = 1  # 持有债券etf的数量
    g.commodity_etf_num = 2  # 持有商品etf的数量
    g.momentum_type = 1  # 动量计算的方式，1：基于年化收益和判定系数打分的动量因子轮动。2：收益率动量。3：线性回归动量。

    # ETF池及其初始权重
    g.asset_classes = {
        'stock': {
            'pool': [
                "513100.XSHG",  # 纳指100（美股最优资产）
                "513500.XSHG",  # 标普500
                "512890.XSHG",  # 红利低波 （高股息）
                "159915.XSHE",  # 创业板100（成长股）
                "510180.XSHG",  # 上证180
                "510300.XSHG",  # 沪深300
                "510500.XSHG",  # 中证500
                "512100.XSHG",  # 中证1000（小盘股）
                "159628.XSHE",  # 国证2000
                "159741.XSHE",  # 恒生科技
                "588080.XSHG"  # 科创板
            ],
            'weight': 0.5
        },
        'bond': {
            'pool': [
                "511010.XSHG",  # 5年国债ETF
                "511260.XSHG",  # 10年国债ETF
                "511090.XSHG"  # 30年国债ETF
            ],
            'weight': 0.35
        },
        'commodity': {
            'pool': [
                "518880.XSHG",  # 黄金ETF（最佳避险大宗商品）
                "159980.XSHE",  # 有色ETF
                "162411.XSHE",  # 华宝油气
                "159985.XSHE"  # 豆粕
            ],
            'weight': 0.15
        }
    }

    # 每天检查是否需要调整
    run_daily(change_cash, "9:30")  # 检查入金
    run_daily(trade, time='9:50')


# 交易逻辑
def trade(context):
    # 获取各类别优选ETF
    selected_etfs = []
    for asset_class in ['stock', 'bond', 'commodity']:
        pool = g.asset_classes[asset_class]['pool']
        num = getattr(g, f"{asset_class}_etf_num")

        ranked_list = get_rank1(pool)
        selected = ranked_list[:num]
        selected_etfs.extend(selected)

    # 卖出非优选ETF
    for etf in context.portfolio.positions:
        if etf not in selected_etfs:
            order_target_value_(context, etf, 0)
            print(f'卖出 {etf}')

    # 计算各类别分配资金
    total_value = context.portfolio.total_value
    for asset_class in ['stock', 'bond', 'commodity']:
        pool = g.asset_classes[asset_class]['pool']
        num = getattr(g, f"{asset_class}_etf_num")
        weight = g.asset_classes[asset_class]['weight']

        ranked_list = get_rank1(pool)
        selected = ranked_list[:num]

        if num > 0 and len(selected) > 0:
            class_value = total_value * weight

            # 新增商品类特殊处理逻辑
            if asset_class == 'commodity' and g.commodity_etf_num == 2:
                # 检查是否包含黄金ETF
                gold_etf = "518880.XSHG"
                if gold_etf in selected:
                    # 黄金比例10%，另一个品种5%
                    gold_value = class_value * 0.10 / 0.15  # 保持总权重15%不变
                    other_value = class_value * 0.05 / 0.15

                    # 分配资金
                    for etf in selected:
                        if etf == gold_etf:
                            target_value = gold_value
                        else:
                            target_value = other_value

                        current_value = context.portfolio.positions[etf].total_amount * \
                                        context.portfolio.positions[
                                            etf].price if etf in context.portfolio.positions else 0
                        if abs(current_value - target_value) / target_value > 0.1:
                            order_target_value_(context, etf, target_value)
                            print(f'商品类特殊配比: {gold_etf}(10%) 和 {etf}(5%)')
                else:
                    # 没有黄金时均分
                    per_etf_value = class_value / len(selected)
                    for etf in selected:
                        current_value = context.portfolio.positions[etf].total_amount * \
                                        context.portfolio.positions[
                                            etf].price if etf in context.portfolio.positions else 0
                        if abs(current_value - per_etf_value) / per_etf_value > 0.1:
                            order_target_value_(context, etf, per_etf_value)
                            print(f'商品类均分配置: {etf} 7.5%')
            else:
                # 其他类别正常分配
                per_etf_value = class_value / len(selected)
                for etf in selected:
                    current_value = context.portfolio.positions[etf].total_amount * \
                                    context.portfolio.positions[etf].price if etf in context.portfolio.positions else 0
                    if abs(current_value - per_etf_value) / per_etf_value > 0.1:
                        order_target_value_(context, etf, per_etf_value)
                        print(f'正常配置 {asset_class}类 {etf}')


# 动量因子1：基于年化收益和判定系数打分的动量因子轮动
def get_rank1(etf_list):
    momentum_day = 25
    score_dict = {}

    for etf in etf_list:
        df = attribute_history(etf, momentum_day, '1d', ['close'], df=True)
        if len(df) < momentum_day:
            continue

        y = np.log(df['close'])
        x = np.arange(len(y))
        slope, intercept = np.polyfit(x, y, 1)
        annualized_returns = math.pow(math.exp(slope), 250) - 1
        predicted = slope * x + intercept
        ss_res = np.sum((y - predicted) ** 2)
        ss_tot = np.sum((y - np.mean(y)) ** 2)
        r_squared = 1 - (ss_res / ss_tot) if ss_tot != 0 else 0

        score = annualized_returns * r_squared
        score_dict[etf] = score

    # 按得分排序
    sorted_etfs = sorted(score_dict.items(), key=lambda x: x[1], reverse=True)
    return [etf[0] for etf in sorted_etfs]


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



