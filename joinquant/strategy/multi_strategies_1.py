from jqdata import *
import math
import random
import numpy as np
import pandas as pd
from io import BytesIO
from scipy.stats import linregress
import datetime

# ----------------------------------------------------------------
# 1) Initialize. test with JX
# ----------------------------------------------------------------
def initialize(context):
    log.info("==> Multi-Strategy Initialize")

    set_benchmark("000300.XSHG")
    set_option("use_real_price", True)
    set_slippage(FixedSlippage(0.005), type="fund")
    set_order_cost(
        OrderCost(
            open_tax=0,
            close_tax=0.0005,
            open_commission=0.0003,
            close_commission=0.0003,
            min_commission=5
        ),
        type="fund"
    )

    # Initialize metric control settings
    g.metrics_settings = {
        'show_portfolio_values': True,      # Show each strategy's total value
        'show_position_ratios': True,       # Show each strategy's position ratio
        'show_correlations': True,          # Show correlation between strategies
        'show_returns': False,              # Show daily/weekly returns
        'show_drawdowns': False,            # Show drawdown metrics
        'log_correlation_matrix': True,     # Log correlation matrix to log panel
        'correlation_lookback': 20          # Days to look back for correlation
    }

    # subportfolios
    g.portfolio_value_proportion = [0.0, 0.25, 0.25, 0.25, 0.25]
    set_subportfolios([
        SubPortfolioConfig(context.portfolio.starting_cash * g.portfolio_value_proportion[i], "stock")
        for i in range(5)
    ])

    # Create global strategies dict
    g.strategys = {}
    g.strategys["etf_momentum"] = SingleETFMomentumStrategy(context, subportfolio_index=1, name="单一ETF动量策略")
    g.strategys["mid_small_cap"] = MidSmallCapStrategy(context, subportfolio_index=2, name="中小板小市值策略")
    g.strategys["all_time"] = AllTimeStrategy(context, subportfolio_index=3, name="全天候策略")
    g.strategys["high_div_peg"] = HighDividendPEGStrategy(context, subportfolio_index=4, name="高股息+PEG策略")

    # Initialize a global history dictionary to record daily total values for correlation analysis.
    g.strategy_history = {key: [] for key, strat in g.strategys.items()}

    # Monthly subportfolio rebalance
    run_monthly(do_balance_subportfolios, 1, time="9:00")

    # =========== Now schedule bridging functions instead of methods ===========
    # (A) Single ETF Momentum
    run_daily(do_single_etf_momentum_adjust, time="9:50")

    # (B) MidSmallCap
    run_daily(do_mid_small_cap_change_cash, "9:30")
    run_daily(do_mid_small_cap_check_stocks, "9:31")
    run_daily(do_mid_small_cap_main_stock_pick, g.strategys["mid_small_cap"].trade_time)
    run_daily(do_mid_small_cap_trade, g.strategys["mid_small_cap"].trade_time)
    run_daily(do_mid_small_cap_no_zt_sell, "14:49")
    run_daily(do_mid_small_cap_selled_security_list_count, "after_close")
    run_daily(do_mid_small_cap_after_market_close, "after_close")

    # (C) AllTime => daily at 14:50
    run_daily(do_all_time_adjust, time="14:50")

    # (D) HighDividendPEG
    run_daily(do_high_div_peg_prepare_stock_list, time="9:05")
    run_weekly(do_high_div_peg_my_trader, weekday=1, time="9:31")
    run_daily(do_high_div_peg_check_limit_up, time="14:00")

    # Schedule the daily metrics output (which now also computes correlation) to run after market close:
    run_daily(output_daily_metrics, time="after_close")


# ----------------------------------------------------------------
# 2) Bridge Functions (top-level) for scheduling
# ----------------------------------------------------------------

def do_balance_subportfolios(context):
    balance_subportfolios(context)

# --- Single ETF Momentum ---
def do_single_etf_momentum_adjust(context):
    g.strategys["etf_momentum"].adjust(context)

# --- MidSmallCap ---
def do_mid_small_cap_change_cash(context):
    g.strategys["mid_small_cap"].change_cash(context)

def do_mid_small_cap_check_stocks(context):
    g.strategys["mid_small_cap"].check_stocks(context)

def do_mid_small_cap_main_stock_pick(context):
    g.strategys["mid_small_cap"].main_stock_pick(context)

def do_mid_small_cap_trade(context):
    g.strategys["mid_small_cap"].trade(context)

def do_mid_small_cap_no_zt_sell(context):
    g.strategys["mid_small_cap"].no_zt_sell(context)

def do_mid_small_cap_selled_security_list_count(context):
    g.strategys["mid_small_cap"].selled_security_list_count(context)

def do_mid_small_cap_after_market_close(context):
    g.strategys["mid_small_cap"].after_market_close(context)

# --- AllTime ---
def do_all_time_adjust(context):
    g.strategys["all_time"].adjust(context)

# --- HighDividendPEG ---
def do_high_div_peg_prepare_stock_list(context):
    g.strategys["high_div_peg"].prepare_stock_list(context)

def do_high_div_peg_my_trader(context):
    g.strategys["high_div_peg"].my_trader(context)

def do_high_div_peg_check_limit_up(context):
    g.strategys["high_div_peg"].check_limit_up(context)


# ----------------------------------------------------------------
# 3) balance_subportfolios
# ----------------------------------------------------------------
def balance_subportfolios(context):
    log.info("==> balance_subportfolios called.")
    for i in range(1, len(g.portfolio_value_proportion)):
        target_val = g.portfolio_value_proportion[i] * context.portfolio.total_value
        current_val = context.subportfolios[i].total_value
        gap = target_val - current_val
        if gap > 0:
            from_cash = context.subportfolios[0].available_cash
            to_transfer = min(from_cash, gap)
            if to_transfer > 0:
                transfer_cash(from_pindex=0, to_pindex=i, cash=to_transfer)
                log.info(f"子组合0 => 子组合{i} 转移资金: {to_transfer}")


# ----------------------------------------------------------------
# 4) Strategy Base Class
# ----------------------------------------------------------------
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


# ----------------------------------------------------------------
# 5) SingleETFMomentumStrategy
# ----------------------------------------------------------------
class SingleETFMomentumStrategy(Strategy):
    def __init__(self, context, subportfolio_index, name):
        super().__init__(context, subportfolio_index, name)
        self.etf_num = 1
        self.momentum_type = 1
        self.etf_list = [
            '518880.XSHG', #黄金ETF（最佳避险大宗商品）
            '513100.XSHG', #纳指100（美股最优资产）
            '159915.XSHE', #创业板100（成长股）
            '512890.XSHG', #红利低波 （高股息）
            #'510300.XSHG', #沪深300（价值股，中大市值蓝筹股）
            #'512100.XSHG', #中证1000（小盘股）
            #'510050.XSHG', #上证50
            '510180.XSHG', #上证180
            #'161725.XSHE', #白酒
            #'159992.XSHE', #创新药
            #'560080.XSHG', #中药
            #'515700.XSHG', #新能源车
            #'515250.XSHG', #智能汽车
            #'515790.XSHG', #光伏
            #'515880.XSHG', #通信
            #'159819.XSHE', #人工智能
            #'512720.XSHG', #计算机（云计算，大数据）
            '159740.XSHE', #恒生科技
            #'159985.XSHE', #豆粕
            #'162411.XSHE', #华宝油气
            #'159981.XSHE', #能源化工ETF
            '159628.XSHE',  # 国证2000
        ]

    def adjust(self, context):
        log.info(f"[{self.name}] => adjust()")
        if self.momentum_type == 1:
            target_list = self.get_rank1(self.etf_list)[: self.etf_num]
        else:
            target_list = self.get_rank1(self.etf_list)[: self.etf_num]

        subp = context.subportfolios[self.subportfolio_index]
        hold_list = list(subp.positions.keys())

        # Sell
        for etf in hold_list:
            if etf not in target_list:
                order_target_value(etf, 0, pindex=self.subportfolio_index)
                log.info(f"[{self.name}] 卖出 {etf}")
            else:
                log.info(f"[{self.name}] 继续持有 {etf}")

        # Buy
        hold_list = list(subp.positions.keys())
        if target_list:
            available_cash = subp.available_cash
            to_fill = self.etf_num - len(hold_list)
            if to_fill > 0:
                each_val = available_cash / to_fill
                for etf in target_list:
                    if etf not in hold_list:
                        order_target_value(etf, each_val, pindex=self.subportfolio_index)
                        log.info(f"[{self.name}] 买入 {etf}")
        else:
            log.info(f"[{self.name}] => 空仓")

    def get_rank1(self, etf_list):
        day = 25
        score_list = []
        for etf in etf_list:
            df = attribute_history(etf, day, '1d', ['close'])
            if len(df) < day:
                score_list.append(float('-inf'))
                continue
            y = np.log(df['close'])
            x = np.arange(len(y))
            slope, intercept = np.polyfit(x, y, 1)
            ann_ret = math.pow(math.exp(slope), 250) - 1
            r2 = 1 - (sum((y - (slope*x + intercept))**2) / ((len(y)-1)*np.var(y, ddof=1)))
            score = ann_ret * r2
            score_list.append(score)
        df_rank = pd.DataFrame({'etf': etf_list, 'score': score_list}).sort_values('score', ascending=False)
        return list(df_rank['etf'])

    def check_moving_average_condition(self, target_list):
        for etf in target_list:
            df = attribute_history(etf, 250, '1d', ['close'])
            if len(df) < 250:
                continue
            ma60 = df['close'][-60:].mean()
            ma120 = df['close'][-120:].mean()
            ma250 = df['close'].mean()
            last_close = df['close'][-1]
            if last_close > ma60 and ma60 > ma120 and ma120 > ma250:
                return 1.0
            else:
                return 0.3
        return 0.3


# ----------------------------------------------------------------
# 6) MidSmallCapStrategy
# ----------------------------------------------------------------
class MidSmallCapStrategy(Strategy):
    def __init__(self, context, subportfolio_index, name):
        super().__init__(context, subportfolio_index, name)
        self.filename = '中小板修改资金.csv'
        self.first = 1
        self.out_cash = 0
        self.paused = []

        self.security_max_proportion = 1
        self.check_stocks_refresh_rate = 1
        self.max_hold_stocknum = 5
        self.sell_rank = 7
        self.notrade_mon = [1, 4]
        self.notrade_day = 1
        self.buy_refresh_rate = 1
        self.sell_refresh_rate = 1
        self.check_stocks_days = 1
        self.days = 0
        self.buy_trade_days = 0
        self.sell_trade_days = 0

        self.filter_paused = True
        self.filter_delisted = True
        self.only_st = False
        self.filter_st = True
        self.security_universe_index = ['all_a_securities']
        self.security_universe_user_securities = []
        self.blacklist = []
        self.notbuy = []

        self.sell_stock_list = []
        self.buy_stock_list = []
        self.open_sell_securities = []
        self.selled_security_list = {}
        self.ZT = []
        self.check_out_lists = []

        # Random daily trade time
        start_minute = 9 * 60 + 32
        end_minute = 9 * 60 + 45
        rdm_min = random.randint(start_minute, end_minute)
        rdm_hour = rdm_min // 60
        rdm_minute = rdm_min % 60
        self.trade_time = f"{rdm_hour}:{rdm_minute}"
        log.info(f"[{self.name}] 随机交易时间: {self.trade_time}")

    def change_cash(self, context):
        if self.first == 1:
            data1 = {'入金金额': 0, '是否生效': "否", '是否清仓后重新买入': "否"}
            df2 = pd.DataFrame([data1], columns=['入金金额','是否生效','是否清仓后重新买入'])
            write_file(self.filename, df2.to_csv(index=False), append=False)
            self.first = 0

        if self.out_cash < 0:
            inout_cash(self.out_cash, pindex=self.subportfolio_index)
            self.out_cash = 0
        else:
            try:
                df2 = pd.read_csv(BytesIO(read_file(self.filename)))
                single = df2.at[0, "是否生效"]
                is_sell = df2.at[0, "是否清仓后重新买入"]
                if single == "是":
                    if is_sell == "是":
                        subp = context.subportfolios[self.subportfolio_index]
                        for s in list(subp.positions.keys()):
                            order_target_value(s, 0, pindex=self.subportfolio_index)
                        df2.loc[0, "是否清仓后重新买入"] = "否"

                    change_cash_ = df2.at[0, "入金金额"]
                    if change_cash_ < 0:
                        self.out_cash = change_cash_
                    else:
                        inout_cash(change_cash_, pindex=self.subportfolio_index)

                    df2.loc[0, "是否生效"] = "否"
                    write_file(self.filename, df2.to_csv(index=False), append=False)

            except Exception as e:
                log.info(f"[{self.name}] 修改资金发生错误: {e}")

    def check_stocks(self, context):
        if self.check_stocks_days % self.check_stocks_refresh_rate != 0:
            self.check_stocks_days += 1
            return

        self.check_out_lists = self._get_security_universe(context)
        self.check_out_lists = self._st_filter(context, self.check_out_lists)
        self.check_out_lists = self._paused_filter(context, self.check_out_lists)
        self.check_out_lists = self._delisted_filter(context, self.check_out_lists)
        self.check_out_lists = [s for s in self.check_out_lists if s not in self.blacklist]

        self.check_stocks_days = 1

    def main_stock_pick(self, context):
        if self.days % self.check_stocks_refresh_rate != 0:
            self.days += 1
            return

        self.sell_stock_list = []
        self.buy_stock_list = []

        mon = context.current_dt.month
        day = context.current_dt.day
        if (mon in self.notrade_mon) and day >= self.notrade_day:
            subp = context.subportfolios[self.subportfolio_index]
            for s in list(subp.positions.keys()):
                order_target_value(s, 0, pindex=self.subportfolio_index)
            return

        check_list = [stock for stock in self.check_out_lists if stock.startswith('0')]

        q = query(valuation.code).filter(
            valuation.code.in_(check_list),
            income.np_parent_company_owners > 0,
            income.net_profit > 0,
            indicator.adjusted_profit > 0,
            income.operating_revenue > 1e8
        ).order_by(valuation.market_cap.asc()).limit(self.max_hold_stocknum * 5)
        df = get_fundamentals(q)
        final_list = df['code'].tolist()
        stockset = final_list[:20]
        stockset = [s for s in stockset if s not in self.notbuy]

        subp = context.subportfolios[self.subportfolio_index]
        hold_list = list(subp.positions.keys())
        self.ZT = []

        for stock in hold_list:
            df2 = attribute_history(stock, 2, '1d', fields=['close','high_limit','paused'], fq='pre')
            if stock in self.paused:
                continue
            elif df2['close'][-1] == df2['high_limit'][-1]:
                self.ZT.append(stock)

        for stock in hold_list:
            if (stock not in stockset[: self.sell_rank]) and (stock not in self.ZT):
                self.sell_stock_list.append(stock)

        for stock in stockset[: self.max_hold_stocknum]:
            if stock not in self.sell_stock_list:
                self.buy_stock_list.append(stock)

        current_data = get_current_data()
        self.paused = [s for s in hold_list if current_data[s].paused]

        log.info(f"[{self.name}] 卖出列表: {self.sell_stock_list}")
        log.info(f"[{self.name}] 买入列表: {self.buy_stock_list}")

        self.days = 1

    def trade(self, context):
        buy_lists = []
        if self.buy_trade_days % self.buy_refresh_rate == 0:
            buy_lists = self.buy_stock_list
            buy_lists = self._high_limit_filter(context, buy_lists)
            log.info(f"[{self.name}] 最终买入列表: {buy_lists}")
        else:
            self.buy_trade_days += 1

        if self.sell_trade_days % self.sell_refresh_rate == 0:
            self._do_sell(context, buy_lists)
            self.sell_trade_days = 1
        else:
            self.sell_trade_days += 1

        if self.buy_trade_days % self.buy_refresh_rate == 0:
            self._do_buy(context, buy_lists)
            self.buy_trade_days = 1
        else:
            self.buy_trade_days += 1

    def no_zt_sell(self, context):
        c = []
        self.notbuy = []
        subp = context.subportfolios[self.subportfolio_index]
        cd = get_current_data()
        for stock in self.ZT:
            df2_3 = attribute_history(stock, 1, '1m', fields=['close','high_limit'], fq='pre')
            if df2_3['close'][-1] == df2_3['high_limit'][-1]:
                c.append(stock)
            else:
                order_target_value(stock, 0, pindex=self.subportfolio_index)
                self.notbuy.append(stock)
        self.ZT = c

    def selled_security_list_count(self, context):
        if len(self.selled_security_list) > 0:
            for stock in self.selled_security_list.keys():
                self.selled_security_list[stock] += 1

    def after_market_close(self, context):
        trades = get_trades()
        for t in trades.values():
            log.info(f"[{self.name}] 成交记录: {t}")
        subp = context.subportfolios[self.subportfolio_index]
        log.info(f"[{self.name}] 收盘后: 子组合{self.subportfolio_index}资产: {round(subp.total_value,2)}")

        if subp.total_value > 0:
            pos_ratio = 1.0 - (subp.available_cash/subp.total_value)
        else:
            pos_ratio = 0
        record(**{f"{self.name}_Pos%": pos_ratio*100})

    # Helpers
    def _do_sell(self, context, buy_lists):
        subp = context.subportfolios[self.subportfolio_index]
        pos_before = list(subp.positions.keys())
        sell_lists = self.sell_stock_list
        for stock in sell_lists:
            order_target_value(stock, 0, pindex=self.subportfolio_index)
        pos_after = list(subp.positions.keys())
        selled = [s for s in pos_before if s not in pos_after]
        for s in selled:
            self.selled_security_list[s] = 0

    def _do_buy(self, context, buy_lists):
        subp = context.subportfolios[self.subportfolio_index]
        buy_lists = self._holded_filter(context, buy_lists)
        hold_count = len(subp.positions)
        slot = self.max_hold_stocknum - hold_count
        if slot <= 0:
            return
        final_buy = buy_lists[:slot]
        if len(final_buy) > 0:
            each_value = (subp.cash + self.out_cash) / len(final_buy)
            for stock in final_buy:
                if stock not in subp.positions:
                    order_target_value(stock, each_value, pindex=self.subportfolio_index)

    def _get_security_universe(self, context):
        temp = []
        for s in self.security_universe_index:
            if s == 'all_a_securities':
                temp += list(get_all_securities(['stock'], context.current_dt.date()).index)
            else:
                temp += get_index_stocks(s)
        for arr in self.security_universe_user_securities:
            temp += arr
        return sorted(list(set(temp)))

    def _paused_filter(self, context, security_list):
        if self.filter_paused:
            cd = get_current_data()
            return [s for s in security_list if not cd[s].paused]
        return security_list

    def _delisted_filter(self, context, security_list):
        if self.filter_delisted:
            cd = get_current_data()
            return [s for s in security_list if not(('退' in cd[s].name) or ('*' in cd[s].name))]
        return security_list

    def _st_filter(self, context, security_list):
        cd = get_current_data()
        if self.only_st:
            return [s for s in security_list if cd[s].is_st]
        if self.filter_st:
            return [s for s in security_list if (not cd[s].is_st and 'ST' not in cd[s].name and '*' not in cd[s].name)]
        return security_list

    def _high_limit_filter(self, context, security_list):
        cd = get_current_data()
        subp = context.subportfolios[self.subportfolio_index]
        holdset = set(subp.positions.keys())
        return [s for s in security_list if (s in holdset) or (cd[s].last_price < cd[s].high_limit)]

    def _holded_filter(self, context, buy_lists):
        filter_holded = False
        if not filter_holded:
            subp = context.subportfolios[self.subportfolio_index]
            holdset = set(subp.positions.keys())
            return [s for s in buy_lists if s not in holdset]
        return buy_lists


# ----------------------------------------------------------------
# 7) AllTimeStrategy
# ----------------------------------------------------------------
class AllTimeStrategy(Strategy):
    def __init__(self, context, subportfolio_index, name):
        super().__init__(context, subportfolio_index, name)
        self.etf_list = [
            "511010.XSHG",  # 国债ETF
            "511880.XSHG",  # 货币基金
            "518880.XSHG",  # 黄金
            "513100.XSHG",  # 纳指100
            "513500.XSHG",  # 标普500
            "512890.XSHG",  # 红利低波
            "159915.XSHE",  # 创业板100
            "510300.XSHG",  # 沪深300
            "510500.XSHG",  # 中证500
            "512100.XSHG",  # 中证1000
            "159980.XSHE",  # 有色ETF
            "162411.XSHE",  # 华宝油
            "159985.XSHE",  # 豆粕ETF
        ]
        self.weights = [0.35, 0.05, 0.1, 0.15, 0.05, 0.04, 0.04, 0.04, 0.04, 0.04, 0.025, 0.05, 0.025]

        self.rebalance_threshold = 0.25
        self.rebalance_interval = 60
        self.rebalance_count = 0

    def adjust(self, context):
        self.rebalance_count += 1
        subp = context.subportfolios[self.subportfolio_index]
        total_value = subp.total_value
        current_positions = subp.positions

        # 目标持仓
        target_values = {etf: total_value * w for etf, w in zip(self.etf_list, self.weights)}

        log.info(f"[{self.name}] 当前持仓: {list(current_positions.keys())}")
        log.info(f"[{self.name}] 目标持仓: {target_values}")

        # 每日调仓(根据偏离阈值)
        for etf, target_val in target_values.items():
            if etf in current_positions:
                pos = current_positions[etf]
                current_val = pos.total_amount * pos.price
            else:
                current_val = 0

            if target_val > 0:
                deviation = (current_val - target_val) / float(target_val)
            else:
                deviation = 0

            if deviation > self.rebalance_threshold:
                order_target_value(etf, target_val, pindex=self.subportfolio_index)
                log.info(f"[{self.name}] 卖出 {etf}, 超出部分 {current_val - target_val}")
            elif deviation < -self.rebalance_threshold:
                order_target_value(etf, target_val, pindex=self.subportfolio_index)
                log.info(f"[{self.name}] 买入 {etf}, 不足 {target_val - current_val}")

        log.info(f"[{self.name}] 调仓后持仓: {list(subp.positions.keys())}")

        # 每60个交易日做一次全局rebalance
        if self.rebalance_count >= self.rebalance_interval:
            self._quarterly_rebalance(context, target_values)
            self.rebalance_count = 0

    def _quarterly_rebalance(self, context, target_values):
        subp = context.subportfolios[self.subportfolio_index]
        current_positions = subp.positions
        for etf, tgt_val in target_values.items():
            if etf in current_positions:
                pos = current_positions[etf]
                cur_val = pos.total_amount * pos.price
            else:
                cur_val = 0
            if abs(cur_val - tgt_val) > 1:
                order_target_value(etf, tgt_val, pindex=self.subportfolio_index)
                if cur_val > tgt_val:
                    log.info(f"[{self.name}] 季度调整: 卖出 {etf} {cur_val - tgt_val}")
                else:
                    log.info(f"[{self.name}] 季度调整: 买入 {etf} {tgt_val - cur_val}")


# ----------------------------------------------------------------
# 8) HighDividendPEGStrategy
# ----------------------------------------------------------------
class HighDividendPEGStrategy(Strategy):
    def __init__(self, context, subportfolio_index, name):
        super().__init__(context, subportfolio_index, name)
        self.high_limit_list = []
        self.hold_list = []
        self.stock_num = 5
        log.info(f"[{self.name}] 初始化完成 - subportfolio #{self.subportfolio_index}")

    def prepare_stock_list(self, context):
        log.info(f"[{self.name}] => prepare_stock_list()")
        self.hold_list = []
        self.high_limit_list = []

        subp = context.subportfolios[self.subportfolio_index]
        for position in subp.positions.values():
            self.hold_list.append(position.security)

        if self.hold_list:
            for stock in self.hold_list:
                df = get_price(stock, end_date=context.previous_date, frequency='daily',
                               fields=['close','high_limit'], count=1)
                if df['close'][0] >= df['high_limit'][0]*0.98:
                    self.high_limit_list.append(stock)

        log.info(f"[{self.name}] hold_list => {self.hold_list}")
        log.info(f"[{self.name}] high_limit_list => {self.high_limit_list}")

    def my_trader(self, context):
        log.info(f"[{self.name}] => my_trader()")
        dt_last = context.previous_date
        all_stocks = get_all_securities(types=['stock'], date=dt_last).index.tolist()

        choice = self._filter_kcbj_stock(all_stocks)
        choice = self._filter_st_stock(choice)
        choice = self._filter_paused_stock(choice)
        choice = self._filter_limitup_stock(context, choice)
        choice = self._filter_limitdown_stock(context, choice)

        stock_list = self._get_dividend_ratio_filter_list(context, choice, sort_asc=False, p1=0, p2=0.1)
        stock_list = self._peg_stock(context, stock_list, 0.1, 2)

        q = query(valuation.code).filter(
            valuation.code.in_(stock_list),
            indicator.inc_total_revenue_year_on_year > 3,
            indicator.inc_net_profit_year_on_year > 8,
            indicator.inc_return > 4.5
        )
        df = get_fundamentals(q)
        final_stockset = list(df['code'])
        final_stockset = final_stockset[: self.stock_num]
        log.info(f"[{self.name}] Final pick => {final_stockset}")

        subp = context.subportfolios[self.subportfolio_index]
        cdata = get_current_data()
        hold_positions = subp.positions

        # Sell
        for s in list(hold_positions.keys()):
            if (s not in final_stockset) and (cdata[s].last_price < cdata[s].high_limit):
                log.info(f"[{self.name}] SELL {s} {cdata[s].name}")
                order_target_value(s, 0, pindex=self.subportfolio_index)

        # Buy
        position_count = len(subp.positions)
        if self.stock_num > position_count:
            psize = subp.available_cash / (self.stock_num - position_count)
            for s in final_stockset:
                if s not in subp.positions:
                    log.info(f"[{self.name}] BUY {s} {cdata[s].name}")
                    order_value(s, psize, pindex=self.subportfolio_index)
                    if len(subp.positions) >= self.stock_num:
                        break

    def check_limit_up(self, context):
        log.info(f"[{self.name}] => check_limit_up()")
        subp = context.subportfolios[self.subportfolio_index]
        cdata = get_current_data()

        if self.high_limit_list:
            for stock in self.high_limit_list[:]:
                if cdata[stock].last_price < cdata[stock].high_limit:
                    log.info(f"[{self.name}] {stock} 涨停打开, 卖出")
                    order_target_value(stock, 0, pindex=self.subportfolio_index)
                    self.high_limit_list.remove(stock)
                else:
                    log.info(f"[{self.name}] {stock} 仍在涨停, 继续持有")

    # Helpers
    def _get_dividend_ratio_filter_list(self, context, stock_list, sort_asc, p1, p2):
        time1 = context.previous_date
        time0 = time1 - datetime.timedelta(days=365*3)

        interval = 1000
        df_all = pd.DataFrame()
        n = len(stock_list)
        for start in range(0, n, interval):
            subset = stock_list[start : start+interval]
            q = query(
                finance.STK_XR_XD.code,
                finance.STK_XR_XD.a_registration_date,
                finance.STK_XR_XD.bonus_amount_rmb
            ).filter(
                finance.STK_XR_XD.a_registration_date >= time0,
                finance.STK_XR_XD.a_registration_date <= time1,
                finance.STK_XR_XD.code.in_(subset)
            )
            df_chunk = finance.run_query(q)
            df_all = df_all.append(df_chunk)

        df_all = df_all.fillna(0)
        df_div = df_all.groupby('code').sum()

        q2 = query(valuation.code, valuation.market_cap).filter(valuation.code.in_(df_div.index))
        cap_df = get_fundamentals(q2, date=time1)
        cap_df.set_index('code', inplace=True)

        DR = pd.concat([df_div, cap_df], axis=1)
        DR['dividend_ratio'] = (DR['bonus_amount_rmb'] / 10000.0) / DR['market_cap']
        DR = DR.sort_values('dividend_ratio', ascending=sort_asc)

        idx1 = int(p1 * len(DR))
        idx2 = int(p2 * len(DR))
        final_list = list(DR.index[idx1 : idx2])
        return final_list

    def _filter_kcbj_stock(self, stock_list):
        result = []
        for s in stock_list:
            if s.startswith('4') or s.startswith('8') or s.startswith('68'):
                continue
            result.append(s)
        return result

    def _filter_st_stock(self, stock_list):
        cd = get_current_data()
        ret = []
        for s in stock_list:
            if not cd[s].is_st and 'ST' not in cd[s].name and '*' not in cd[s].name and '退' not in cd[s].name:
                ret.append(s)
        return ret

    def _filter_paused_stock(self, stock_list):
        cd = get_current_data()
        return [s for s in stock_list if not cd[s].paused]

    def _filter_limitup_stock(self, context, stock_list):
        cd = get_current_data()
        subp = context.subportfolios[self.subportfolio_index]
        holds = set(subp.positions.keys())
        return [s for s in stock_list if (s in holds) or (cd[s].day_open < cd[s].high_limit)]

    def _filter_limitdown_stock(self, context, stock_list):
        cd = get_current_data()
        subp = context.subportfolios[self.subportfolio_index]
        holds = set(subp.positions.keys())
        return [s for s in stock_list if (s in holds) or (cd[s].day_open > cd[s].low_limit)]

    def _peg_stock(self, context, stock_list, pegmin, pegmax):
        q = query(valuation.code).filter(
            valuation.code.in_(stock_list),
            (valuation.pe_ratio / indicator.inc_net_profit_year_on_year) > pegmin,
            (valuation.pe_ratio / indicator.inc_net_profit_year_on_year) < pegmax
        )
        df = get_fundamentals(q)
        return list(df['code'])


# ----------------------------------------------------------------
# 9) inout_cash Helper
# ----------------------------------------------------------------
def inout_cash(amount, pindex=0):
    if amount > 0:
        transfer_cash(from_pindex=0, to_pindex=pindex, cash=amount)
    elif amount < 0:
        transfer_cash(from_pindex=pindex, to_pindex=0, cash=abs(amount))


# ----------------------------------------------------------------
# 10) Enhanced Output Daily Metrics with Switch Controllers
# ----------------------------------------------------------------
def output_daily_metrics(context):
    # Record individual strategy metrics and append today's total value to history.
    for key, strat in g.strategys.items():
        subp = context.subportfolios[strat.subportfolio_index]
        total_value = subp.total_value
        available_cash = subp.available_cash
        pos_ratio = (total_value - available_cash) / total_value * 100 if total_value > 0 else 0

        # Only record metrics based on settings
        metrics_to_record = {}
        if g.metrics_settings['show_portfolio_values']:
            metrics_to_record[f"{strat.name}_Value"] = total_value
        if g.metrics_settings['show_position_ratios']:
            metrics_to_record[f"{strat.name}_Pos%"] = pos_ratio

        if metrics_to_record:
            record(**metrics_to_record)

        # Log regardless of UI metrics (helps with debugging)
        log.info(f"Strategy {strat.name}: Total Value = {total_value:.2f}, Position Ratio = {pos_ratio:.2f}%")

        # Always append to history for correlation calculations
        g.strategy_history[key].append((context.current_dt, total_value))

    # Build a DataFrame from the full recorded history.
    history_dict = {}
    for strat_name, records in g.strategy_history.items():
        dates = [r[0] for r in records]
        values = [r[1] for r in records]
        history_dict[strat_name] = pd.Series(values, index=pd.to_datetime(dates))
    df = pd.DataFrame(history_dict).sort_index()

    # Ensure we have at least 2 days to compute returns.
    if df.shape[0] < 2:
        return

    # Calculate returns
    returns = df.pct_change().dropna()

    # Record returns if enabled
    if g.metrics_settings['show_returns']:
        for strat_name in returns.columns:
            if not returns[strat_name].empty:
                record(**{f"{strat_name}_DailyReturn%": returns[strat_name].iloc[-1] * 100})

    # Calculate correlation matrix using lookback period
    lookback = min(g.metrics_settings['correlation_lookback'], len(returns))
    recent_returns = returns.tail(lookback)
    corr_matrix = recent_returns.corr()

    # Log correlation matrix if enabled
    if g.metrics_settings['log_correlation_matrix']:
        log.info(f"Correlation Matrix of Daily Returns (last {lookback} days):")
        log.info("\n" + corr_matrix.to_string())

    # Record individual correlations if enabled
    if g.metrics_settings['show_correlations']:
        keys = list(corr_matrix.columns)
        for i in range(len(keys)):
            for j in range(i + 1, len(keys)):
                pair_key = f"Corr_{keys[i]}_{keys[j]}"
                corr_value = corr_matrix.loc[keys[i], keys[j]]
                if not np.isnan(corr_value):
                    record(**{pair_key: corr_value})

    # Calculate and record drawdowns if enabled
    if g.metrics_settings['show_drawdowns']:
        for strat_name in df.columns:
            strat_series = df[strat_name]
            rolling_max = strat_series.cummax()
            drawdown = (strat_series - rolling_max) / rolling_max * 100
            current_dd = drawdown.iloc[-1] if not drawdown.empty else 0
            record(**{f"{strat_name}_DD%": current_dd})