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
        :param func: 下单函数 (按股数)
        :param context: 策略上下文
        :param security: 证券代码
        :param shares: 数量 (整数股数)
        :param exec_time: 执行时间
        """
        self.func = func
        self.context = context
        self.security = security
        self.shares = shares
        self.exec_time = exec_time

    def execute(self, context=None):
        """执行订单"""
        ctx = context or self.context
        return mt.order_target_(ctx, self.security, self.shares)

class SplitOrderManager:
    """
    将大额市值订单拆分为按股数的小额订单并定时执行。
    采用最小交易单位 100 股为限。
    支持最大拆单数限制。
    """
    def __init__(self, price_func, split_threshold=50000, max_leg=50000, max_splits=4, interval_minutes=4):
        """
        :param price_func: 用于获取最新价格的函数，签名 price_func(context, security) -> price
        :param split_threshold: 市值阈值，小于等于该市值直接下单
        :param max_leg: 每笔拆单最大市值，用于计算最多股数
        :param max_splits: 最大拆单笔数
        :param interval_minutes: 拆单间隔（分钟）
        """
        self.price_func = price_func
        self.split_threshold = split_threshold
        self.max_leg = max_leg
        self.max_splits = max_splits
        self.interval = datetime.timedelta(minutes=interval_minutes)
        # 存储 PendingOrder 实例
        self.pending = []

    def _get_price(self, context, security):
        """获取最新价格"""
        data = self.price_func(context, security)
        return data[security].last_price

    def _compute_share_legs(self, total_amount, price):
        """
        计算按市值 total_amount 对应的股数，并拆分为多个 100 股为单位的 leg 列表。
        返回每笔股数列表。
        """
        # 总可交易股数，向下取整 100 的倍数
        total_shares = int((total_amount / price) // 100) * 100
        if total_shares < 100:
            return []  # 股数不足以交易
        # 每笔最多股数
        max_leg_shares = int((self.max_leg / price) // 100) * 100
        if max_leg_shares < 100:
            max_leg_shares = 100
        # 需要最少拆分笔数
        splits_needed = math.ceil(total_shares / max_leg_shares)
        splits = min(max(splits_needed, 2), self.max_splits)
        # 基础股数
        base = int((total_shares / splits) // 100) * 100
        legs = [base] * splits
        remainder = total_shares - base * splits
        if remainder >= 100:
            legs[-1] += remainder
        return legs

    def _schedule(self, func, context, security, share_legs):
        """执行第一笔，安排后续 PendingOrder"""
        now = context.current_dt
        first_shares = share_legs[0]
        log.info(f"[拆单管理] 执行第1笔: {security} 股数: {first_shares}")
        func(context, security, first_shares)
        # 安排后续
        for i, shares in enumerate(share_legs[1:], start=1):
            exec_time = now + self.interval * i
            po = PendingOrder(func, context, security, shares, exec_time)
            self.pending.append(po)
            log.info(f"[拆单管理] 安排第{i+1}笔: {security} 股数: {shares} 执行时间: {exec_time}")

    def place_order_value(self, context, security, total_amount):
        """
        按市值 total_amount 下单，超过阈值则拆分。
        内部转换为股数并调用按股数的 mt.order_target_。
        """
        price = self._get_price(context, security)
        if total_amount <= self.split_threshold:
            # 直接下单，根据市值换算股数
            shares = int((total_amount / price) // 100) * 100
            if shares >= 100:
                log.info(f"[拆单管理] 直接市值下单: {security} 股数: {shares}")
                return mt.order_target_(context, security, shares)
            else:
                log.info(f"[拆单管理] 市值不足下单: {security} 总金额: {total_amount}")
                return
        # 拆单
        legs = self._compute_share_legs(total_amount, price)
        if not legs:
            log.info(f"[拆单管理] 拆单后股数不足: {security} 总金额: {total_amount}")
            return
        log.info(f"[拆单管理] 拆分订单: {security} 市值 {total_amount} 转换股数分 {len(legs)} 笔: {legs}")
        self._schedule(mt.order_target_, context, security, legs)

    def execute_pending(self, context):
        """
        每个 bar 调用，执行到期 PendingOrder
        """
        now = context.current_dt
        ready = [p for p in self.pending if p.exec_time <= now]
        for p in ready:
            log.info(f"[拆单管理] 执行待处理拆单: {p.security} 股数: {p.shares} 执行时间: {p.exec_time}")
            p.execute(context)
            self.pending.remove(p)

    def get_pending(self):
        """返回待执行订单列表"""
        return [(p.security, p.shares, p.exec_time) for p in self.pending]

    # 暴露接口给策略使用
    def order_target_value_(self, context, security, amount):
        """与 mysql_trade order_target_value_ 同名接口，按市值拆分。"""
        return self.place_order_value(context, security, amount)

    def order_value_(self, context, security, amount):
        """与 mysql_trade order_value_ 同名接口，按市值拆分。"""
        return self.place_order_value(context, security, amount)

    def order_target_(self, context, security, shares):
        """与 mysql_trade order_target_ 同名接口，直接下单。"""
        return mt.order_target_(context, security, shares)
