import datetime
import math
from kuanke.wizard import log
import mysqltrade as mt

class PendingOrder:
    """
    表示一个待执行的拆分订单。
    """
    def __init__(self, func, context, security, shares, exec_time):
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
        self.shares = shares
        self.exec_time = exec_time

    def execute(self, context=None):
        """执行 pending 订单"""
        ctx = context or self.context
        return self.func(ctx, self.security, self.shares)

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
    def __init__(self, get_current_data_func, split_threshold=50000, max_leg=50000, max_splits=4, interval_minutes=4):
        """
        :param get_current_data_func: 获取实时数据的函数，签名为 func() -> dict[security, DataFrame]
        :param split_threshold: 小于等于该市值直接一次性按目标持仓下单
        :param max_leg: 拆单时单笔最大市值(按市值拆单)或最大股数(按股数拆单时)
        :param max_splits: 最大拆单笔数
        :param interval_minutes: 每笔拆单的时间间隔（分钟）
        """
        self.get_current_data = get_current_data_func
        self.split_threshold = split_threshold
        self.max_leg = max_leg
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

    def _compute_share_legs(self, abs_shares, max_leg_shares):
        """
        按绝对股数拆分成多个 leg，保证每笔 ≤ max_leg_shares 且 ≥100 股。
        """
        splits_required = math.ceil(abs_shares / max_leg_shares)
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

    def _schedule_shares(self, context, security, share_legs, sign):
        """
        先执行第一笔，后续按 interval 挂入 pending。
        :param share_legs: list[int] 每笔绝对股数 (≥100)
        :param sign: +1 买, -1 卖
        """
        now = context.current_dt
        first_shares = share_legs[0] * sign
        action = "买入" if sign > 0 else "卖出"
        log.info(f"[拆单管理] 执行第1笔{action} {security} 股数: {abs(first_shares)}")
        mt.order_target_(context, security, first_shares)

        for i, shares in enumerate(share_legs[1:], start=1):
            exec_time = now + self.interval * i
            po = PendingOrder(mt.order_target_, context, security, shares * sign, exec_time)
            self.pending.append(po)
            log.info(f"[拆单管理] 安排第{i+1}笔{action}: {security} 股数: {shares} 执行时间: {exec_time}")

    def place_order_value(self, context, security, total_amount):
        """
        按市值 total_amount 下单: 自动识别买卖方向，并拆分交易。
        """
        price = self._get_price(context, security)
        if price is None or price <= 0:
            log.error(f"[拆单管理] 无法获取 {security} 最新价格，跳过")
            return

        # 目标持仓股数 (100 股单位)
        target_shares = int((total_amount / price) // 100) * 100
        if target_shares < 100:
            log.info(f"[拆单管理] 金额 {total_amount} 对应 <100 股({target_shares}股)，跳过 {security}")
            return

        # 当前持仓股数
        current_pos = self._get_current_position(context, security)

        # 需调整股数 (正买, 负卖)
        delta_shares = target_shares - current_pos
        if abs(delta_shares) < 100:
            log.info(f"[拆单管理] {security} 目标 {target_shares} 股, 当前 {current_pos} 股, 差额 <100, 跳过")
            return

        # 若市值 ≤ 阈值, 直接调整持仓到目标
        if total_amount <= self.split_threshold:
            log.info(f"[拆单管理] 直接调整持仓 {security} 到 {target_shares} 股")
            return mt.order_target_(context, security, target_shares)

        # 拆单：计算单笔最大股数
        max_leg_shares = int((self.max_leg / price) // 100) * 100
        if max_leg_shares < 100:
            log.info(f"[拆单管理] max_leg({self.max_leg}元) 对应 <100 股, 直接调整 {security} 到 {target_shares} 股")
            return mt.order_target_(context, security, target_shares)

        abs_shares = abs(delta_shares)
        share_legs = self._compute_share_legs(abs_shares, max_leg_shares)
        action = "买入" if delta_shares > 0 else "卖出"
        log.info(f"[拆单管理] 拆分 {security} {action} 总股数 {abs_shares} 分 {len(share_legs)} 笔: {share_legs}")

        sign = 1 if delta_shares > 0 else -1
        self._schedule_shares(context, security, share_legs, sign)

    def order_target_value_(self, context, security, amount):
        """与 mysql_trade order_target_value_ 同名接口，按市值拆分下单"""
        return self.place_order_value(context, security, amount)

    def order_value_(self, context, security, amount):
        """与 mysql_trade order_value_ 同名接口，按市值拆分下单"""
        return self.place_order_value(context, security, amount)

    def order_target_(self, context, security, shares):
        """与 mysql_trade order_target_ 同名接口，按目标持仓拆单下单"""
        price = self._get_price(context, security)
        if price is None or price <= 0:
            log.error(f"[拆单管理] 无法获取 {security} 最新价格，跳过")
            return
        # 目标持仓股数
        target_shares = shares
        total_amount = target_shares * price
        return self.place_order_value(context, security, total_amount)

    def order_(self, context, security, shares):
        """与 mysql_trade order_ 同名接口，按需交易股数拆单下单"""
        price = self._get_price(context, security)
        if price is None or price <= 0:
            log.error(f"[拆单管理] 无法获取 {security} 最新价格，跳过")
            return
        abs_shares = abs(shares)
        if abs_shares < 100:
            log.info(f"[拆单管理] 请求交易 {abs_shares} 股 <100 股，跳过 {security}")
            return
        # 需买或需卖股数 = shares (正买, 负卖)
        sign = 1 if shares > 0 else -1
        # 单笔最大股数直接用 max_leg 计算的股票数
        # 先获取拆单所需最大股数限制(按市值换算): max_leg_shares
        max_leg_shares = int((self.max_leg / price) // 100) * 100
        if max_leg_shares < 100:
            # 无法拆单时，直接一次性下单
            log.info(f"[拆单管理] max_leg({self.max_leg}元) 对应 <100 股，直接下单 {abs_shares} 股 {security}")
            return mt.order(context, security, shares)
        share_legs = self._compute_share_legs(abs_shares, max_leg_shares)
        action = "买入" if shares > 0 else "卖出"
        log.info(f"[拆单管理] 拆分 {security} {action} 总股数 {abs_shares} 分 {len(share_legs)} 笔: {share_legs}")
        self._schedule_shares(context, security, share_legs, sign)

    def execute_pending(self, context):
        """
        每个 Bar 调用，执行到期拆单
        """
        now = context.current_dt
        ready = [p for p in self.pending if p.exec_time <= now]
        for p in ready:
            sec = p.security
            shares = p.shares
            action = "买入" if shares > 0 else "卖出"
            current_pos = self._get_current_position(context, sec)
            target_shares = current_pos + shares
            if target_shares < 0:
                log.warning(f"[拆单管理] 执行 {action} {sec} 股数 {abs(shares)} 时，目标持仓 {target_shares} 股 < 0，跳过")
                continue

            # 更新 pending 订单的 shares 为目标持仓
            p.shares = target_shares
            log.info(f"[拆单管理] 执行待处理拆单{action}: {sec} 股数 {abs(shares)}")
            p.execute(context)
            self.pending.remove(p)

    def get_pending(self):
        """返回待执行订单 (security, shares, exec_time) 列表"""
        return [(p.security, p.shares, p.exec_time) for p in self.pending]
