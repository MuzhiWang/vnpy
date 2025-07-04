import datetime
import math
from kuanke.wizard import log
import mysqltrade as mt

class PendingOrder:
    """
    表示一个待执行的拆分订单。
    """
    def __init__(self, func, context, security, shares=None, value=None, exec_time=None, idx=None, sell_all=False):
        """
        :param func: 下单函数，按股数买/卖 (签名 func(context, security, shares))
        :param context: 策略上下文
        :param security: 证券代码
        :param shares: 股数 (正数=买入, 负数=卖出)
        :param exec_time: 执行时间
        """
        self.func = func
        self.context = context
        self.security = security
        self.shares = shares # 绝对股数 (正数=买入, 负数=卖出)
        self.value = value  # 可选，订单市值
        self.exec_time = exec_time
        self.executed_time = None  # 记录实际执行时间
        self.idx = idx  # 可选，订单索引，用于跟踪或调试
        self.sell_all = sell_all  # 是否卖出全部持仓，若为 True 则 shares & value 被忽略

    def execute(self, context=None):
        if self.shares is None and self.value is None:
            log.error(f"[PendingOrder] 执行订单失败: {self.security} 股数和市值都未指定")
            return
        if self.exec_time is None:
            log.error(f"[PendingOrder] 执行订单失败: {self.security} 执行时间未指定")
            return

        """执行 pending 订单"""
        ctx = context or self.context
        try:
            val = self.shares if self.shares is not None else self.value
            if self.sell_all:
                val = 0 # 卖出全部持仓时，股数和市值都忽略，直接卖空
            res = self.func(ctx, self.security, val)
            self.executed_time = ctx.current_dt
            if res is not None:
                log.info(f"[PendingOrder] 执行订单: {self.security}, 第{self.idx + 1} 股数: {self.shares} 市值: {self.value}, 结果: {res}")
            else:
                log.warn(f"[PendingOrder] 执行订单 返回 None: {self.security} 股数: {self.shares} 市值: {self.value}")
        except Exception as e:
            log.error(f"[PendingOrder] 执行订单失败: {self.security} 股数: {self.shares} 市值: {self.value}, 错误: {e}")
            raise e  # 重新抛出异常以便上层捕获


class Balance:
    """
    Represents the account balance information, considering both current cash
    and pending orders that will affect the balance.
    """

    DEFAULT_GET_REMAINING_SECURITIES_DIFF = 100  # 默认允许的持仓差异，用于判断是否仍持有某证券

    def __init__(self, current_cash=0, pending_buy=0, pending_sell=0):
        self.current_cash = current_cash  # Current cash balance
        self.pending_buy = pending_buy  # Cash reserved for pending buy orders
        self.pending_sell = pending_sell  # Expected cash from pending sell orders

        # 持仓信息
        self.positions = {}               # 当前持仓 {security: amount}
        self.pending_sell_securities = {} # 待卖出证券 {security: amount}


    @property
    def available_cash(self):
        """
        Returns the available cash balance considering pending orders.
        This is the amount available for new purchases.
        """
        return max(0, self.current_cash - self.pending_buy + self.pending_sell)

    @property
    def total_projected_cash(self):
        """
        Returns the projected total cash after all pending orders are executed.
        """
        return self.current_cash - self.pending_buy + self.pending_sell

    def get_remaining_securities(self):
        """
        返回考虑待卖出订单后仍将持有的证券列表。
        排除了计划全部卖出的证券。
        """
        result = []
        for security, amount in self.positions.items():
            sell_amount = self.pending_sell_securities.get(security, 0)
            if abs(amount - sell_amount) > Balance.DEFAULT_GET_REMAINING_SECURITIES_DIFF:  # 允许小于1000金额的误差
                result.append(security)
        return result

    def get_remaining_securities_count(self):
        """
        返回考虑待卖出订单后仍将持有的证券数量。
        """
        return len(self.get_remaining_securities())

    def __str__(self):
        return (f"Balance(current={self.current_cash:.2f}, "
                f"pending_buy={self.pending_buy:.2f}, "
                f"pending_sell={self.pending_sell:.2f}, "
                f"available={self.available_cash:.2f})")


class SplitOrderManager:
    """
    将大额市值/股数订单拆分为小额订单并定时执行。
    使用 get_current_data() 获取实时价格，按最小交易单位 100 股校验，支持最大拆单数限制。
    拆单后的第一笔即时执行，后续在 pending 队列中等待指定时间执行。

    提供接口：
      - order_target_value_(context, security, amount)
      - order_value_(context, security, amount)
      - order_target_(context, security, shares) (按目标持仓拆单)
      - order_(context, security, shares)        (按需交易股数拆单)
    """
    def __init__(self, get_current_data_func, split_threshold=50000, max_value=50000, max_splits=4, interval_minutes=4):
        """
        :param get_current_data_func: 获取实时数据的函数，签名为 func() -> dict[security, DataFrame]
        :param split_threshold: 小于等于该市值直接一次性按目标持仓下单
        :param max_value : 单笔最大市值（元），用于计算单笔最大股数
        :param max_splits: 最大拆单笔数
        :param interval_minutes: 每笔拆单的时间间隔（分钟）
        """
        self.get_current_data = get_current_data_func
        self.split_threshold = split_threshold
        self.max_value = max_value
        self.max_splits = max_splits
        self.interval = datetime.timedelta(minutes=interval_minutes)
        self.pending = []  # 存储 PendingOrder 实例

    def _get_price(self, context, security):
        """使用 get_current_data() 获取最新价格"""
        data = self.get_current_data(context, security)
        return data[security].last_price

    def _get_current_position(self, context, security):
        """
        获取当前持仓股数，若无持仓则返回 0。
        :param context: 策略上下文
        :param security: 证券代码
        :return: 当前持仓股数 (int)
        """
        if security in context.portfolio.positions:
            return context.portfolio.positions[security].total_amount or 0
        return 0

    def _compute_share_slices(self, abs_shares, max_value_shares):
        """
        按绝对股数拆分成多个 leg，保证每笔 ≤ max_value_shares 且 ≥100 股。
        """
        splits_required = math.ceil(abs_shares / max_value_shares)
        splits = min(max(splits_required, 2), self.max_splits)

        base = abs_shares // splits
        legs = [base] * splits
        remainder = abs_shares - base * splits
        if remainder:
            legs[-1] += remainder

        # 如果最后一笔 <100 股，则合并到前一笔
        if legs[-1] < 100 and len(legs) >= 2:
            legs[-2] += legs[-1]
            legs.pop()
        return legs

    def _compute_value_slices(self, abs_value):
        """
        按绝对市值拆分成多个 leg，保证每笔 ≤ max_value 且 ≥100 股。
        """
        splits_required = math.ceil(abs_value / self.max_value)
        splits = min(max(splits_required, 2), self.max_splits)

        base = abs_value // splits
        legs = [base] * splits
        remainder = abs_value - base * splits
        if remainder:
            legs[-1] += remainder

        # # 如果最后一笔 <100 股对应的市值，则合并到前一笔
        # if legs[-1] < 100:
        #     legs[-2] += legs[-1]
        #     legs.pop()
        return legs

    # def _schedule_shares(self, context, security, share_legs, sign):
    #     """
    #     先执行第一笔，后续按 interval 挂入 pending。
    #     :param share_legs: list[int] 每笔绝对股数 (≥100)
    #     :param sign: +1 买, -1 卖
    #     """
    #     now = context.current_dt
    #     # 当前持仓股数
    #     current_pos = self._get_current_position(context, security)
    #     first_shares = share_legs[0] * sign + current_pos
    #     action = "买入" if sign > 0 else "卖出"
    #     log.info(f"[拆单管理] 执行第1笔{action} {security} 股数: {abs(first_shares)}")
    #     mt.order_target_(context, security, first_shares)
    #
    #     for i, shares in enumerate(share_legs[1:], start=1):
    #         exec_time = now + self.interval * i
    #         po = PendingOrder(mt.order_target_, context, security, shares * sign, None, exec_time, i)
    #         if i == len(share_legs) - 1 and sign < 0:
    #             # 最后一笔卖出时，设置 sell_all=True
    #             po.sell_all = True
    #         self.pending.append(po)
    #         log.info(f"[拆单管理] 安排第{i + 2}笔{action}: {security} 股数: {shares} 执行时间: {exec_time}")

    def _schedule_value(self, context, security, value_legs, sign, target_value):
        """
        先执行第一笔，后续按 interval 挂入 pending。
        :param value_legs: list[float] 每笔绝对市值 (≥100 股)
        :param sign: +1 买, -1 卖
        """
        now = context.current_dt
        # 当前持仓股数
        current_pos = self._get_current_position(context, security)
        first_value = value_legs[0] * sign + current_pos * self._get_price(context, security)
        action = "买入" if sign > 0 else "卖出"
        log.info(f"[拆单管理] 执行第1笔{action} {security} 市值: {abs(first_value)}")
        mt.order_target_value_(context, security, first_value)

        for i, value in enumerate(value_legs[1:], start=1):
            exec_time = now + self.interval * i
            po = PendingOrder(mt.order_target_value_, context, security, None, value * sign, exec_time, i)
            if i == len(value_legs) - 1 and target_value == 0:
                # 最后一笔卖出时，设置 sell_all=True
                po.sell_all = True
            self.pending.append(po)
            log.info(f"[拆单管理] 安排第{i + 1}笔{action}: {security} 市值: {value} 执行时间: {exec_time}")

    def place_order_value(self, context, security, target_value):
        """
        按市值 target_value 下单: 自动识别买卖方向，并拆分交易。
        :param context: 策略上下文
        :param security: 证券代码
        :param target_value: 目标市值 (元)
        """
        price = self._get_price(context, security)
        if price is None or price <= 0:
            log.error(f"[拆单管理] 无法获取 {security} 最新价格，跳过")
            return

        # # 目标持仓股数 (100 股单位)
        # target_shares = int((target_value / price) // 100) * 100
        # if target_shares < 100 and target_value > 0:
        #     log.warn(f"[拆单管理] 金额 {target_value} 对应 <100 股({target_shares}股)，跳过 {security}")
        #     return

        # 当前持仓股数
        current_pos = self._get_current_position(context, security)
        current_value = current_pos * price
        log.info(f"[拆单管理] 当前 {security} 持仓: {current_pos}, 市值: {current_value} 元, 目标市值: {target_value} 元")

        # 需调整市值 (正买, 负卖)
        delta_value = target_value - current_value
        # if abs(delta_value) < price * 100:
        #     log.warn(f"[拆单管理] {security} 目标市值 {target_value}元, 当前市值 {current_value}元, 差额 <100股市值, 跳过")
        #     return

        # 若市值 ≤ 阈值, 直接调整持仓到目标
        if abs(delta_value) <= self.split_threshold:
            log.info(f"[拆单管理] 直接调整持仓 {security} 到目标市值 {target_value}元")
            return mt.order_target_value_(context, security, target_value)

        # 拆单：计算市值拆分
        abs_value = abs(delta_value)
        value_slices = self._compute_value_slices(abs_value)
        action = "买入" if delta_value > 0 else "卖出"
        log.info(f"[拆单管理] 拆分 {security} {action} 总市值 {abs_value}元 分 {len(value_slices)} 笔: {value_slices}")

        sign = 1 if delta_value > 0 else -1
        self._schedule_value(context, security, value_slices, sign, target_value)

    def order_target_value_(self, context, security, target_value):
        """与 mysql_trade order_target_value_ 同名接口，按市值拆分下单"""
        log.info(f"[拆单管理] 请求 {security} 目标市值 {target_value} 元")
        return self.place_order_value(context, security, target_value)

    def order_value_(self, context, security, amount):
        """与 mysql_trade order_value_ 同名接口，按市值拆分下单

        :param context: 策略上下文
        :param security: 证券代码
        :param amount: 交易金额，正数买入，负数卖出
        """
        log.info(f"[拆单管理] 请求 {security} {'买入' if amount > 0 else '卖出'}市值 {abs(amount)} 元")

        price = self._get_price(context, security)
        if price is None or price <= 0:
            log.error(f"[拆单管理] 无法获取 {security} 最新价格，跳过")
            return

        # 当前持仓市值
        current_pos = self._get_current_position(context, security)
        current_value = current_pos * price

        # 目标市值 = 当前市值 + 要交易的金额
        target_value = current_value + amount

        return self.place_order_value(context, security, target_value)

    def order_target_(self, context, security, shares):
        """与 mysql_trade order_target_ 同名接口，按目标持仓拆单下单"""
        log.info(f"[拆单管理] 请求 {security} 目标持仓 {shares} 股")
        price = self._get_price(context, security)
        if price is None or price <= 0:
            log.error(f"[拆单管理] 无法获取 {security} 最新价格，跳过")
            return
        # 目标持仓股数
        target_shares = shares
        target_value = target_shares * price
        return self.place_order_value(context, security, target_value)

    # def order_(self, context, security, shares):
    #     """与 mysql_trade order_ 同名接口，按需交易股数拆单下单"""
    #     price = self._get_price(context, security)
    #     if price is None or price <= 0:
    #         log.error(f"[拆单管理] 无法获取 {security} 最新价格，跳过")
    #         return
    #     abs_shares = abs(shares)
    #     if abs_shares < 100:
    #         log.info(f"[拆单管理] 请求交易 {abs_shares} 股 <100 股，跳过 {security}")
    #         return
    #     # 需买或需卖股数 = shares (正买, 负卖)
    #     sign = 1 if shares > 0 else -1
    #     # 单笔最大股数直接用 max_leg 计算的股票数
    #     # 先获取拆单所需最大股数限制(按市值换算): max_leg_shares
    #     max_leg_shares = int((self.max_value / price) // 100) * 100
    #     if max_leg_shares < 100:
    #         # 无法拆单时，直接一次性下单
    #         log.info(f"[拆单管理] max_leg({self.max_value}元) 对应 <100 股，直接下单 {abs_shares} 股 {security}")
    #         return mt.order(context, security, shares)
    #     share_legs = self._compute_share_slices(abs_shares, max_leg_shares)
    #     action = "买入" if shares > 0 else "卖出"
    #     log.info(f"[拆单管理] 拆分 {security} {action} 总股数 {abs_shares} 分 {len(share_legs)} 笔: {share_legs}")
    #     self._schedule_shares(context, security, share_legs, sign)

    def execute_pending(self, context):
        """
        每个 Bar 调用，执行到期拆单
        若订单执行失败，会重新安排在该股票最后一笔订单之后执行
        """
        now = context.current_dt
        ready = [p for p in self.pending if p.exec_time <= now]
        for p in ready:
            self.pending.remove(p)  # 先移除，避免重复处理

            sec = p.security
            shares = p.shares
            action = "未知"
            current_pos = self._get_current_position(context, sec)
            if p.shares is not None:
                action = "买入" if shares > 0 else "卖出"
                target_shares = current_pos + shares
                if target_shares < 0:
                    log.warn(f"[拆单管理] 执行 {action} {sec} 股数 {abs(shares)} 时，目标持仓 {target_shares} 股 < 0")
                    target_shares = 0  # 全部卖出

                # 更新 pending 订单的 shares 为目标持仓
                p.shares = target_shares
                log.info(f"[拆单管理] 执行待处理拆单{action}: {sec} 第{p.idx + 1}单 股数 {abs(shares)}")
            elif p.value is not None:
                action = "买入" if p.value > 0 else "卖出"
                current_value = current_pos * self._get_price(context, sec)
                target_value = current_value + p.value
                if target_value < 0:
                    log.warn(f"[拆单管理] 执行 {action} {sec} 市值 {abs(p.value)} 时，目标市值 {target_value} < 0")
                    target_value = 0  # 全部卖出

                # 更新 pending 订单的 value 为目标市值
                p.value = target_value
                log.info(f"[拆单管理] 执行待处理拆单{action}: {sec} 第{p.idx + 1}单 市值 {abs(p.value)}")

            try:
                # 尝试执行订单
                p.execute(context)
            except Exception as e:
                # 查找同一股票的所有订单
                same_security_orders = [o for o in self.pending if o.security == sec]

                if same_security_orders:
                    # 找到同一股票的最后一笔订单
                    last_order = max(same_security_orders, key=lambda o: o.exec_time)
                    new_exec_time = last_order.exec_time + self.interval
                else:
                    # 没有其他订单，按默认延迟执行
                    new_exec_time = now + self.interval

                # 更新执行时间和重试计数
                p.exec_time = new_exec_time

                # 重新添加到待执行列表
                self.pending.append(p)
                log.info(f"[拆单管理] 重新安排失败的{action}订单: {sec}, 新执行时间: {new_exec_time}")

    def get_pending(self):
        """返回待执行订单 (security, shares, exec_time) 列表"""
        return [(p.security, p.shares, p.exec_time) for p in self.pending]

    def get_available_balance(self, context) -> Balance:
        """
        计算可用现金余额和未来持仓状态，考虑待处理订单的影响。

        考虑因素:
        - 为待处理买入订单预留的现金
        - 待处理卖出订单预计收到的现金
        - 待卖出的证券数量

        :param context: 策略上下文，包含投资组合信息
        :return: Balance 对象，包含详细的现金和持仓信息
        """
        # Start with current cash balance
        current_cash = context.portfolio.cash
        balance = Balance(current_cash, 0, 0)
        # 填充当前持仓信息
        for security, position in context.portfolio.positions.items():
            balance.positions[security] = position.total_amount

        # Process all pending orders
        for pending_order in self.pending:
            security = pending_order.security
            price = self._get_price(context, security)

            if price is None or price <= 0:
                log.error(f"[拆单管理] get_available_balance: 无法获取 {security} 价格，忽略该笔待处理订单")
                continue

            if pending_order.value is not None:
                # 处理按市值下单的订单
                if pending_order.value < 0:
                    # 卖出 - 将增加现金
                    sell_value = abs(pending_order.value)
                    balance.pending_sell += sell_value

                    # 估算卖出股数
                    current_pos = balance.positions.get(security, 0)
                    if current_pos > 0:
                        approx_shares = min(current_pos, int(sell_value / price))
                        if security not in balance.pending_sell_securities:
                            balance.pending_sell_securities[security] = 0
                        balance.pending_sell_securities[security] += approx_shares
                else:
                    # 买入 - 将使用现金
                    balance.pending_buy += pending_order.value

            elif pending_order.shares is not None:
                # 处理按股数下单的订单
                if pending_order.shares < 0:
                    # 卖出 - 将增加现金
                    sell_shares = abs(pending_order.shares)
                    balance.pending_sell += sell_shares * price

                    if security not in balance.pending_sell_securities:
                        balance.pending_sell_securities[security] = 0
                    balance.pending_sell_securities[security] += sell_shares
                else:
                    # 买入 - 将使用现金
                    balance.pending_buy += pending_order.shares * price

            elif pending_order.sell_all:
                # 处理卖出全部持仓的订单
                current_pos = balance.positions.get(security, 0)
                if current_pos > 0:
                    sell_value = current_pos * price
                    balance.pending_sell += sell_value
                    balance.pending_sell_securities[security] = current_pos  # 全部卖出

        log.info(f"[拆单管理] 余额计算: 当前现金={balance.current_cash:.2f}, "
                 f"待买入={balance.pending_buy:.2f}, "
                 f"待卖出={balance.pending_sell:.2f}, "
                 f"可用={balance.available_cash:.2f}")

        return balance
