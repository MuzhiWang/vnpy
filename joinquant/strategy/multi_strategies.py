from jqdata import *
import datetime
import math
import numpy as np
import pandas as pd


# 初始化函数，设置策略和配置
def initialize(context):
    set_benchmark("513100.XSHG")
    set_option("use_real_price", True)
    log.info("初始化 - 仅运行一次")

    set_slippage(FixedSlippage(0.02), type="stock")
    set_slippage(FixedSlippage(0.002), type="fund")

    set_order_cost(OrderCost(open_tax=0, close_tax=0.001, open_commission=0.0003, close_commission=0.0003,
                             close_today_commission=0, min_commission=5), type="stock")

    set_order_cost(OrderCost(open_tax=0, close_tax=0, open_commission=0, close_commission=0,
                             close_today_commission=0, min_commission=0), type="mmf")

    # 投资组合配置
    g.portfolio_value_proportion = [0, 0.5, 0.3, 0.2]  # 第一个子投资组合用于现金储备，后续子投资组合用于策略分配
    set_subportfolios([
        SubPortfolioConfig(context.portfolio.starting_cash * g.portfolio_value_proportion[i], "stock")
        for i in range(4)
    ])

    # 策略定义
    g.strategys = {}
    g.strategys["momentum_strategy"] = MomentumStrategy(context, subportfolio_index=0, name="动量策略")
    g.strategys["all_weather_strategy"] = AllWeatherStrategy(context, subportfolio_index=1, name="全天候策略")
    g.strategys["etf_rotation_strategy"] = RotationETFStrategy(context, subportfolio_index=2, name="ETF轮动策略")

    # 调度策略函数
    run_monthly(balance_subportfolios, 1, "9:00")  # 每月1日早上9:00执行子投资组合平衡

    if g.portfolio_value_proportion[1] > 0:
        run_daily(g.strategys["momentum_strategy"].prepare, "9:05")
        run_weekly(g.strategys["momentum_strategy"].adjust, 1, "9:37")
        run_daily(g.strategys["momentum_strategy"].check, "14:50")

    if g.portfolio_value_proportion[2] > 0:
        run_monthly(g.strategys["all_weather_strategy"].adjust, 1, "9:45")

    if g.portfolio_value_proportion[3] > 0:
        run_daily(g.strategys["etf_rotation_strategy"].adjust, "9:42")


# 策略基类
class Strategy:
    def __init__(self, context, subportfolio_index, name):
        self.subportfolio_index = subportfolio_index
        self.name = name

    def prepare(self, context):
        pass

    def adjust(self, context):
        pass

    def check(self, context):
        pass


# 动量策略实现
class MomentumStrategy(Strategy):
    def __init__(self, context, subportfolio_index, name):
        super().__init__(context, subportfolio_index, name)
        self.stock_pool = ["000001.XSHE", "000002.XSHE"]  # 示例股票池

    def prepare(self, context):
        log.info(f"准备 {self.name}")

    def adjust(self, context):
        log.info(f"调整 {self.name}")

    def check(self, context):
        log.info(f"检查 {self.name}")


# 全天候策略实现
class AllWeatherStrategy(Strategy):
    def __init__(self, context, subportfolio_index, name):
        super().__init__(context, subportfolio_index, name)
        self.etf_allocation = {
            "511010.XSHG": 0.4,  # Treasury ETF
            "518880.XSHG": 0.2,  # Gold ETF
        }

    def adjust(self, context):
        log.info(f"调整 {self.name}")


# ETF轮动策略实现
class RotationETFStrategy(Strategy):
    def __init__(self, context, subportfolio_index, name):
        super().__init__(context, subportfolio_index, name)
        self.etf_pool = ["518880.XSHG", "513100.XSHG"]

    def adjust(self, context):
        log.info(f"调整 {self.name}")


# 投资组合平衡函数
def balance_subportfolios(context):
    log.info("平衡子投资组合")
    for i in range(1, len(g.portfolio_value_proportion)):
        target = g.portfolio_value_proportion[i] * context.portfolio.total_value
        current_value = context.subportfolios[i].total_value

        if target > current_value:
            cash_to_transfer = min(context.subportfolios[0].available_cash, target - current_value)
            transfer_cash(from_pindex=0, to_pindex=i, cash=cash_to_transfer)  # 从子投资组合0转移资金到其他子投资组合

    log.info("完成子投资组合平衡")
