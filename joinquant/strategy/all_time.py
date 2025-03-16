import numpy as np
import pandas as pd
import datetime
from jqdata import *

# 初始化函数
def initialize(context):
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
        "511880.XSHG",  # 银华日利（货币基金）
        "518880.XSHG",  # 黄金ETF
        "513100.XSHG",  # 纳指100
        "513500.XSHG",  # 标普500
        "512890.XSHG",  # 红利低波
        "159915.XSHE",  # 创业板100（成长股）
        "510300.XSHG",  # 沪深300（价值股，中大市值蓝筹股）
        "510500.XSHG",  # 中证500（中盘股）
        "512100.XSHG",  # 中证1000（小盘股）
        "588080.XSHG",  # 科创板
        "159980.XSHE",  # 有色ETF
        "162411.XSHE",  # 华宝油
        "159985.XSHE",  # 豆粕ETF
    ]
    g.weights = [0.35, 0.05, 0.1, 0.15, 0.05, 0.04, 0.04, 0.03, 0.03, 0.03, 0.03, 0.025, 0.05, 0.025]  # 初始比例

    g.rebalance_threshold = 0.25  # 调仓比例阈值
    g.rebalance_interval = 60  # 每3个月调整一次比例（按交易日计）
    g.rebalance_count = 0  # 调整日计数

    # 每天检查是否需要调整
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
            order_target_value(etf, target_value)
            print_debug(f"卖出 {etf} 超出部分 {sell_value}")
        # 如果偏差不足阈值，调整持仓 买入
        elif deviation < -g.rebalance_threshold:
            # buy_value = target_value * (1 - g.rebalance_threshold) - current_value
            buy_value = target_value - current_value
            order_target_value(etf, target_value)
            print_debug(f"买入 {etf} 不足部分 {buy_value}")

    print_debug(f"每日交易后 当前持仓 {context.portfolio.positions}")

    # 每3个月检查一次比例
    if g.rebalance_count >= g.rebalance_interval:
        quarterly_rebalance(context, target_values)
        g.rebalance_count = 0 # 重置

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
            order_target_value(etf, target_value)
            if current_value > target_value:
                print(f"季度调整：卖出 {etf} 多余部分 {current_value - target_value}")
            else:
                print(f"季度调整：买入 {etf} 不足部分 {target_value - current_value}")

def print_debug(str):
    if g.print_debug:
        print(str)


