# 全天候ETF

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
    g.filename = '全天候ETF.csv'  # 入金配置文件名称

    g.first = 1
    g.out_cash = 0
    g.paused = []
    g.check_out_lists = []
    # 设定基准
    set_benchmark('510300.XSHG')
    set_option('use_real_price', True)  # 使用真实价格交易
    set_slippage(FixedSlippage(0.002))  # 设置滑点
    set_order_cost(
        OrderCost(open_tax=0.001, close_tax=0.001, open_commission=0.0002, close_commission=0.0002, min_commission=5),
        type='fund'
    )
    g.print_debug = True

    # ETF池及其初始权重
    g.etf_list = [
        "511010.XSHG",  # 国债ETF
        # "511880.XSHG",  # 银华日利（货币基金）
        "518880.XSHG",  # 黄金ETF
        "513100.XSHG",  # 纳指100
        "513500.XSHG",  # 标普500
        # "513050.XSHG",  # 中概互联
        "512890.XSHG",  # 红利低波
        "159915.XSHE",  # 创业板100（成长股）
        "510300.XSHG",  # 沪深300（价值股，中大市值蓝筹股）
        "510500.XSHG",  # 中证500（中盘股）
        "512100.XSHG",  # 中证1000（小盘股）
        "159741.XSHE",  # 恒生科技
        "588080.XSHG",  # 科创板
        "159980.XSHE",  # 有色ETF
        "162411.XSHE",  # 华宝油
        "159985.XSHE",  # 豆粕ETF
    ]
    g.weights = [0.4, 0.1, 0.15, 0.05, 0.04, 0.04, 0.02, 0.03, 0.04, 0.04, 0.04, 0.02, 0.01, 0.02]  # 初始比例
    # g.weights = [0.4, 0.1, 0.2, 0.1, 0.07, 0.06, 0.07]

    g.rebalance_threshold = 0.25  # 调仓比例阈值
    g.rebalance_interval = 60  # 每3个月调整一次比例（按交易日计）
    g.rebalance_count = 0  # 调整日计数

    # 每天检查是否需要调整
    run_daily(change_cash, "9:30")  # 检查入金
    run_daily(trade, time='14:50')


# 每日交易逻辑
def trade(context):
    g.rebalance_count += 1
    total_value = context.portfolio.total_value  # 当前总资产
    current_positions = context.portfolio.positions  # 当前持仓

    # 目标持仓 {eft: target_values}
    target_values = {etf: total_value * weight for etf, weight in zip(g.etf_list, g.weights)}
    print_debug(f"当前持仓 {current_positions}")
    print_debug(f"目标持仓 {target_values}")

    # 检查每日比例偏差
    for etf, target_value in target_values.items():
        if etf in current_positions:
            current_value = current_positions[etf].total_amount * current_positions[etf].price
        else:
            current_value = 0

        # 计算当前持仓与目标持仓的偏差 %
        deviation = (current_value - target_value) / float(target_value)

        # 如果偏差超出阈值，调整持仓 卖出
        if deviation > g.rebalance_threshold:
            # sell_value = current_value - target_value * (1 + g.rebalance_threshold)
            sell_value = current_value - target_value
            order_target_value_(context, etf, target_value)
            print_debug(f"卖出 {etf} 超出部分 {sell_value}")
        # 如果偏差不足阈值，调整持仓 买入
        elif deviation < -g.rebalance_threshold:
            # buy_value = target_value * (1 - g.rebalance_threshold) - current_value
            buy_value = target_value - current_value
            order_target_value_(context, etf, target_value)
            print_debug(f"买入 {etf} 不足部分 {buy_value}")

    print_debug(f"每日交易后 当前持仓 {context.portfolio.positions}")

    # 每3个月检查一次比例
    if g.rebalance_count >= g.rebalance_interval:
        quarterly_rebalance(context, target_values)
        g.rebalance_count = 0  # 重置


# 每3个月调整比例
def quarterly_rebalance(context, target_values):
    current_positions = context.portfolio.positions
    for etf, target_value in target_values.items():
        if etf in current_positions:
            current_value = current_positions[etf].total_amount * current_positions[etf].price
        else:
            current_value = 0

        # 调整至目标比例
        if current_value != target_value:
            order_target_value_(context, etf, target_value)
            if current_value > target_value:
                print(f"季度调整：卖出 {etf} 多余部分 {current_value - target_value}")
            else:
                print(f"季度调整：买入 {etf} 不足部分 {target_value - current_value}")


def print_debug(str):
    if g.print_debug:
        print(str)


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


