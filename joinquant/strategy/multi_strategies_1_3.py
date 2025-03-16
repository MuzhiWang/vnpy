# -*- coding: utf-8 -*-
# 导入函数库
from jqdata import *
from jqfactor import get_factor_values
import datetime
import math
import numpy as np
import pandas as pd
from scipy.optimize import minimize


# 初始化函数，设定基准等等
def initialize(context):
    # 打开防未来函数
    set_option("avoid_future_data", True)
    # 开启动态复权模式(真实价格)
    set_option("use_real_price", True)
    log.info("初始函数开始运行且全局只运行一次")
    log.set_level("order", "error")

    # 固定滑点设置ETF及股票
    set_slippage(FixedSlippage(0.002), type="fund")
    set_slippage(FixedSlippage(0.02), type="stock")
    set_order_cost(
        OrderCost(
            open_tax=0,
            close_tax=0.001,
            open_commission=0.0003,
            close_commission=0.0003,
            close_today_commission=0,
            min_commission=5,
        ),
        type="stock",
    )
    set_order_cost(
        OrderCost(
            open_tax=0,
            close_tax=0,
            open_commission=0,
            close_commission=0,
            close_today_commission=0,
            min_commission=0,
        ),
        type="mmf",
    )

    # 全局变量
    g.fill_stock = "511880.XSHG"  # 货币ETF,用于现金管理
    g.strategys = {}
    # 各子策略的权重
    g.portfolio_value_proportion = [0.3, 0.3, 0.1, 0.1, 0.2]
    # 记录每个子策略的持仓
    g.positions = {i: {} for i in range(len(g.portfolio_value_proportion))}

    # 初始化历史记录字典，记录每个策略每日的组合价值
    g.strategy_history = {}
    for name in ["搅屎棍策略", "全天候策略", "核心资产轮动策略", "简单ROA策略", "弱周期价投策略"]:
        g.strategy_history[name] = []  # 格式: [(date, value), (date, value), ...]

    # 子策略执行计划
    if g.portfolio_value_proportion[0] > 0:
        run_daily(jsg_prepare, "9:05")
        run_weekly(jsg_adjust, 1, "9:31")
        run_daily(jsg_check, "14:50")
    if g.portfolio_value_proportion[1] > 0:
        run_monthly(all_day_adjust, 1, "9:32")
    if g.portfolio_value_proportion[2] > 0:
        run_monthly(simple_roa_adjust, 1, "9:33")
    if g.portfolio_value_proportion[3] > 0:
        run_weekly(weak_cyc_adjust, 1, "9:34")
    # 每日剩余资金购买货币ETF
    run_daily(buy_fill_stock, "14:55")
    # 记录每日策略表现（建议在市场收盘后执行）
    run_daily(record_performance, "15:00")
    # 分析相关性（建议在记录后执行）
    run_daily(analyze_correlation, "15:05")


def process_initialize(context):
    g.strategys["搅屎棍策略"] = JSG_Strategy(context, index=0, name="搅屎棍策略")
    g.strategys["全天候策略"] = All_Day_Strategy(context, index=1, name="全天候策略")
    g.strategys["核心资产轮动策略"] = Rotation_ETF_Strategy(context, index=2, name="核心资产轮动策略")
    g.strategys["简单ROA策略"] = Simple_ROA_Strategy(context, index=3, name="简单ROA策略")
    g.strategys["弱周期价投策略"] = Weak_Cyc_Strategy(context, index=4, name="弱周期价投策略")


# 买入货币ETF
def buy_fill_stock(context):
    current_data = get_current_data()
    amount = int(context.portfolio.available_cash / current_data["511880.XSHG"].last_price)
    if amount >= 100:
        order(g.fill_stock, amount)


# 卖出货币ETF换现金
def get_cash(context, value):
    if g.fill_stock not in context.portfolio.positions:
        return
    current_data = get_current_data()
    amount = math.ceil(value / current_data[g.fill_stock].last_price / 100) * 100
    position = context.portfolio.positions[g.fill_stock].closeable_amount
    if amount >= 100:
        order(g.fill_stock, -min(amount, position))


def jsg_prepare(context):
    g.strategys["搅屎棍策略"].prepare()


def jsg_check(context):
    g.strategys["搅屎棍策略"].check()


def jsg_adjust(context):
    g.strategys["搅屎棍策略"].adjust()


def all_day_adjust(context):
    g.strategys["全天候策略"].adjust()


def rotation_etf_adjust(context):
    g.strategys["核心资产轮动策略"].adjust()


def simple_roa_adjust(context):
    g.strategys["简单ROA策略"].adjust()


def weak_cyc_adjust(context):
    g.strategys["弱周期价投策略"].adjust()


#################################
# 以下是记录表现和分析相关性的代码
#################################
def record_performance(context):
    """
    每日记录每个子策略的组合市值
    """
    for strat_name, strat in g.strategys.items():
        # 计算策略当前的持仓市值
        current_value = strat.get_total_value()
        # 将当前日期和市值记录到历史字典中
        g.strategy_history[strat_name].append((context.current_dt, current_value))
    log.info("Recorded performance for all sub-strategies.")


def analyze_correlation(context):
    """
    根据历史数据计算策略间的日收益率相关性，并输出相关性矩阵。
    """
    # 需要确保至少有两天的数据才能计算收益率
    sample_dates = None
    data_dict = {}
    for strat_name, history in g.strategy_history.items():
        if len(history) < 2:
            log.info("Not enough data to compute correlation for %s." % strat_name)
            return
        # 解压日期和市值
        dates, values = zip(*history)
        # 用日期作为索引，构造一个Series
        data_dict[strat_name] = pd.Series(values, index=pd.to_datetime(dates))
        # 记录 sample_dates 供后续参考
        if sample_dates is None:
            sample_dates = pd.to_datetime(dates)

    # 将各策略的市值合并为一个 DataFrame
    df = pd.DataFrame(data_dict)
    # 按日期排序
    df = df.sort_index()
    # 计算日收益率百分比
    returns = df.pct_change().dropna()
    # 计算相关性矩阵
    corr_matrix = returns.corr()

    log.info("Strategies Daily Returns Correlation Matrix:")
    log.info("\n" + str(corr_matrix))

    # 如果你需要进一步的分析或保存数据，可以考虑写入文件或使用其他方法进行可视化


#################################
# 以下是各个子策略的实现代码
#################################

# 策略基类
class Strategy:
    def __init__(self, context, index, name):
        self.context = context
        self.index = index
        self.name = name
        self.stock_sum = 1
        self.hold_list = []
        self.limit_up_list = []
        self.min_volume = 2000

    # 获取策略当前持仓市值
    def get_total_value(self):
        if not g.positions[self.index]:
            return 0
        return sum(
            self.context.portfolio.positions[key].price * value
            for key, value in g.positions[self.index].items()
        )

    # 准备今日所需数据(必须在调仓前运行)
    def _prepare(self):
        self.hold_list = list(g.positions[self.index].keys())
        if self.hold_list:
            df = get_price(
                self.hold_list,
                end_date=self.context.previous_date,
                frequency="daily",
                fields=["close", "high_limit"],
                count=1,
                panel=False,
                fill_paused=False,
            )
            df = df[df["close"] == df["high_limit"]]
            self.limit_up_list = list(df.code)
        else:
            self.limit_up_list = []

    # 检查昨日涨停票
    def _check(self):
        if self.limit_up_list:
            for stock in self.limit_up_list:
                self.order_target_value_(stock, 0)

    # 调仓(等权购买输入列表中的标的)
    def _adjust(self, target):
        portfolio = self.context.portfolio
        # 卖出不在目标列表且非涨停的股票
        for security in self.hold_list:
            if (security not in target) and (security not in self.limit_up_list):
                self.order_target_value_(security, 0)
        count = len(set(target) - set(self.hold_list))
        if count == 0 or self.stock_sum <= len(self.hold_list):
            return
        target_value = portfolio.total_value * g.portfolio_value_proportion[self.index]
        position_value = self.get_total_value()
        available_cash = portfolio.available_cash + (
            portfolio.positions[g.fill_stock].value if g.fill_stock in portfolio.positions else 0
        )
        value = max(0, min(target_value - position_value, available_cash))
        if value > portfolio.available_cash:
            get_cash(self.context, value - portfolio.available_cash)
        for security in target:
            if security not in self.hold_list:
                self.order_target_value_(security, value / count)

    # 调仓(target为字典，key为股票代码，value为目标市值)
    def _adjust2(self, targets):
        current_data = get_current_data()
        portfolio = self.context.portfolio
        for stock in self.hold_list:
            if stock not in targets:
                self.order_target_value_(stock, 0)
        for stock, target in targets.items():
            price = current_data[stock].last_price
            value = g.positions[self.index].get(stock, 0) * price
            if value - target > self.min_volume and value - target > price * 100:
                self.order_target_value_(stock, target)
        for stock, target in targets.items():
            price = current_data[stock].last_price
            value = g.positions[self.index].get(stock, 0) * price
            if target - value > self.min_volume and target - value > price * 100:
                if target - value > portfolio.available_cash:
                    get_cash(self.context, target - value - portfolio.available_cash)
                if portfolio.available_cash > price * 100:
                    self.order_target_value_(stock, target)

    # 自定义下单(涨跌停不交易)
    def order_target_value_(self, security, value):
        current_data = get_current_data()
        if current_data[security].paused:
            log.info(f"{security}: 今日停牌")
            return False
        if current_data[security].last_price == current_data[security].high_limit:
            log.info(f"{security}: 当前涨停")
            return False
        if current_data[security].last_price == current_data[security].low_limit:
            log.info(f"{security}: 当前跌停")
            return False
        price = current_data[security].last_price
        current_position = g.positions[self.index].get(security, 0)
        target_position = (int(value / price) // 100) * 100 if price != 0 else 0
        adjustment = target_position - current_position
        if adjustment != 0 and order(security, adjustment):
            g.positions[self.index][security] = target_position
            if target_position == 0:
                g.positions[self.index].pop(security, None)
            self.hold_list = list(g.positions[self.index].keys())
            return True
        else:
            return False

    # 基础过滤(过滤科创北交、ST、停牌、次新股)
    def filter_basic_stock(self, stock_list):
        current_data = get_current_data()
        return [
            stock for stock in stock_list
            if not current_data[stock].paused
               and not current_data[stock].is_st
               and "ST" not in current_data[stock].name
               and "*" not in current_data[stock].name
               and "退" not in current_data[stock].name
               and not (stock[0] == "4" or stock[0] == "8" or stock[:2] == "68" or stock[0] == "3")
               and not self.context.previous_date - get_security_info(stock).start_date < datetime.timedelta(375)
        ]

    # 过滤当前时间涨跌停的股票
    def filter_limitup_limitdown_stock(self, stock_list):
        current_data = get_current_data()
        return [
            stock for stock in stock_list
            if stock in self.hold_list
               or (current_data[stock].last_price < current_data[stock].high_limit and
                   current_data[stock].last_price > current_data[stock].low_limit)
        ]

    # 判断今天是否处于空仓月
    def is_empty_month(self):
        month = self.context.current_dt.month
        return month in self.pass_months


# 搅屎棍策略
class JSG_Strategy(Strategy):
    def __init__(self, context, index, name):
        super().__init__(context, index, name)
        self.stock_sum = 6
        self.num = 1
        self.pass_months = [1, 4]

    def getStockIndustry(self, stocks):
        industry = get_industry(stocks)
        return pd.Series({stock: info["sw_l1"]["industry_name"] for stock, info in industry.items() if "sw_l1" in info})

    def get_market_breadth(self):
        yesterday = self.context.previous_date
        stocks = get_index_stocks("000985.XSHG")
        count = 1
        h = get_price(
            stocks,
            end_date=yesterday,
            frequency="1d",
            fields=["close"],
            count=count + 20,
            panel=False,
        )
        h["date"] = pd.DatetimeIndex(h.time).date
        df_close = h.pivot(index="code", columns="date", values="close").dropna(axis=0)
        df_ma20 = df_close.rolling(window=20, axis=1).mean().iloc[:, -count:]
        df_bias = df_close.iloc[:, -count:] > df_ma20
        df_bias["industry_name"] = self.getStockIndustry(stocks)
        df_ratio = ((df_bias.groupby("industry_name").sum() * 100.0) / df_bias.groupby("industry_name").count()).round()
        top_values = df_ratio.loc[:, yesterday].nlargest(self.num)
        I = top_values.index.tolist()
        return I

    def filter(self):
        stocks = get_index_stocks("399101.XSHE")
        stocks = self.filter_basic_stock(stocks)
        stocks = (get_fundamentals(
            query(valuation.code).filter(
                valuation.code.in_(stocks),
                indicator.roa
            )
        ).head(20).code)
        stocks = self.filter_limitup_limitdown_stock(stocks)
        return stocks[: min(len(stocks), self.stock_sum)]

    def select(self):
        I = self.get_market_breadth()
        industries = {"银行I", "煤炭I", "采掘I", "钢铁I"}
        if not industries.intersection(I) and not self.is_empty_month():
            return self.filter()
        return []

    def prepare(self):
        self._prepare()

    def adjust(self):
        target = self.select()
        self._adjust(target)

    def check(self):
        self._check()


# 全天候ETF策略
class All_Day_Strategy(Strategy):
    def __init__(self, context, index, name):
        super().__init__(context, index, name)
        self.min_volume = 2000
        self.etf_pool = [
            "511260.XSHG",
            "518880.XSHG",
            "513100.XSHG",
            "515080.XSHG",
            "159980.XSHE",
            "162411.XSHE",
            "159985.XSHE",
        ]
        self.rates = [0.45, 0.2, 0.1, 0.1, 0.05, 0.05, 0.05]

    def adjust(self):
        self._prepare()
        total_value = self.context.portfolio.total_value * g.portfolio_value_proportion[self.index]
        targets = {etf: total_value * rate for etf, rate in zip(self.etf_pool, self.rates)}
        self._adjust2(targets)


# 核心资产轮动ETF策略
class Rotation_ETF_Strategy(Strategy):
    def __init__(self, context, index, name):
        super().__init__(context, index, name)
        self.stock_sum = 1
        self.etf_pool = [
            "518880.XSHG",
            "513100.XSHG",
            "159915.XSHE",
            "510180.XSHG",
        ]
        self.m_days = 25

    def MOM(self, etf):
        df = attribute_history(etf, self.m_days, "1d", ["close"])
        y = np.log(df["close"].values)
        n = len(y)
        x = np.arange(n)
        weights = np.linspace(1, 2, n)
        slope, intercept = np.polyfit(x, y, 1, w=weights)
        annualized_returns = math.pow(math.exp(slope), 250) - 1
        residuals = y - (slope * x + intercept)
        r_squared = 1 - (np.sum(weights * residuals ** 2) / np.sum(weights * (y - np.mean(y)) ** 2))
        return annualized_returns * r_squared

    def select(self):
        score_list = [self.MOM(etf) for etf in self.etf_pool]
        df = pd.DataFrame(index=self.etf_pool, data={"score": score_list})
        df = df.sort_values(by="score", ascending=False)
        df = df[(df["score"] > 0) & (df["score"] <= 5)]
        target = df.index.tolist()
        if not target:
            target = [self.fill_stock]
        return target[: min(len(target), self.stock_sum)]

    def adjust(self, context):
        target = self.select()
        self._prepare(context)
        self._adjust(context, target)


# 简单ROA策略
class Simple_ROA_Strategy(Strategy):
    def __init__(self, context, index, name):
        super().__init__(context, index, name)
        self.stock_sum = 1

    def filter(self):
        stocks = get_all_securities("stock", date=self.context.previous_date).index.tolist()
        stocks = self.filter_basic_stock(stocks)
        stocks = list(
            get_fundamentals(
                query(valuation.code, indicator.roa).filter(
                    valuation.code.in_(stocks),
                    valuation.pb_ratio > 0,
                    valuation.pb_ratio < 1,
                    indicator.adjusted_profit > 0,
                )
            ).sort_values(by="roa", ascending=False).head(20).code
        )
        stocks = self.filter_limitup_limitdown_stock(stocks)
        return stocks[: self.stock_sum] if stocks else []

    def adjust(self):
        self._prepare()
        target = self.filter()
        self._adjust(target)


# 弱周期价投策略
class Weak_Cyc_Strategy(Strategy):
    def __init__(self, context, index, name):
        super().__init__(context, index, name)
        self.stock_sum = 4
        self.bond_etf = "511260.XSHG"
        self.min_volume = 2000

    def select(self):
        yesterday = self.context.previous_date
        stocks = get_industry_stocks("HY010", date=yesterday)
        stocks = self.filter_basic_stock(stocks)
        stocks = get_fundamentals(
            query(valuation.code).filter(
                valuation.code.in_(stocks),
                valuation.market_cap > 500,
                valuation.pe_ratio < 20,
                indicator.roa > 0,
                indicator.gross_profit_margin > 30,
            ).order_by(valuation.market_cap.desc())
        ).code
        return list(stocks)

    def adjust(self):
        self._prepare()
        stocks = self.select()[: self.stock_sum]
        stocks.append(self.bond_etf)
        rates = [round(1 / (self.stock_sum + 2), 3)] * (len(stocks) - 1)
        rates.append(round(1 - sum(rates), 3))
        total_value = self.context.portfolio.total_value * g.portfolio_value_proportion[self.index]
        targets = {stock: total_value * rate for stock, rate in zip(stocks, rates)}
        self._adjust2(targets)
