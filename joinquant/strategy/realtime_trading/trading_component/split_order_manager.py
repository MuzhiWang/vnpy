import datetime
import math
from kuanke.wizard import log
import mysqltrade as mt

class SplitOrderManager:
    """
    将大额订单拆分为小额订单并定时执行。
    提供与 mysql_trade 中 order_, order_target_, order_value_, order_target_value_ 相同接口。
    拆单逻辑改进：按等额分配，支持最大拆单数限制。
    """
    def __init__(self, split_threshold=50000, max_leg=50000, max_splits=4, interval_minutes=4):
        """
        :param split_threshold: 小于等于该值的金额直接下单
        :param max_leg: 每笔拆单的最大金额（用于计算最少拆单笔数）
        :param max_splits: 最大拆单笔数
        :param interval_minutes: 后续拆单间隔（分钟）
        """
        self.split_threshold = split_threshold
        self.max_leg = max_leg
        self.max_splits = max_splits
        self.interval = datetime.timedelta(minutes=interval_minutes)
        # pending 存储待执行的拆单订单，每项 dict 包含 func, context, security, amount, exec_time
        self.pending = []

    def _should_split(self, amount):
        """判断是否需要拆单"""
        return amount > self.split_threshold

    def _compute_splits(self, total):
        """
        按等额原则计算拆单笔数及各笔金额:
        - splits_required = ceil(total/max_leg)
        - splits = min(max(splits_required, 2), max_splits)
        - base = total // splits, 最后一笔加剩余
        """
        # 计算最低拆分数，保证每笔不超过 max_leg
        splits_required = math.ceil(total / self.max_leg)
        # 限制拆单笔数在 [2, max_splits]
        splits = min(max(splits_required, 2), self.max_splits)
        # 基础金额
        base = total // splits
        legs = [base] * splits
        # 分配剩余
        remainder = total - base * splits
        if remainder:
            legs[-1] += remainder
        return legs

    def _schedule(self, func, context, security, legs):
        """立即执行第1笔，并按间隔安排后续订单"""
        now = context.current_dt
        # 执行第一笔
        first_amt = legs[0]
        log.info(f"[拆单管理] 执行第1笔: {security} 金额: {first_amt}")
        func(context, security, first_amt)
        # 安排后续
        for i, amt in enumerate(legs[1:], start=1):
            exec_time = now + self.interval * i
            self.pending.append({
                'func': func,
                'context': context,
                'security': security,
                'amount': amt,
                'exec_time': exec_time
            })
            log.info(f"[拆单管理] 安排第{i+1}笔: {security} 金额: {amt} 执行时间: {exec_time}")

    def place(self, func, context, security, amount):
        """
        入口：根据总金额决定直接下单或拆单
        :param func: 下单函数，签名 func(context, security, amount)
        """
        if not self._should_split(amount):
            log.info(f"[拆单管理] 直接下单: {security} 金额: {amount}")
            return func(context, security, amount)
        # 计算拆分
        legs = self._compute_splits(amount)
        log.info(f"[拆单管理] 拆分订单: {security} 总额{amount} 分{len(legs)}笔: {legs}")
        # 执行拆单
        self._schedule(func, context, security, legs)

    def execute_pending(self, context):
        """
        执行所有到达执行时间的拆单订单，需在每个 bar 调用
        """
        now = context.current_dt
        ready = [p for p in self.pending if p['exec_time'] <= now]
        for p in ready:
            func = p['func']
            sec = p['security']
            amt = p['amount']
            log.info(f"[拆单管理] 执行待处理拆单: {sec} 金额: {amt}")
            func(p['context'], sec, amt)
            self.pending.remove(p)

    def order_(self, context, security, amount):
        """调用 mysql_trade.order_ 并拆单"""
        return self.place(mt.order_, context, security, amount)

    def order_target_(self, context, security, amount):
        """调用 mysql_trade.order_target_ 并拆单"""
        return self.place(mt.order_target_, context, security, amount)

    def order_value_(self, context, security, amount):
        """调用 mysql_trade.order_value_ 并拆单"""
        return self.place(mt.order_value_, context, security, amount)

    def order_target_value_(self, context, security, amount):
        """调用 mysql_trade.order_target_value_ 并拆单"""
        return self.place(mt.order_target_value_, context, security, amount)

    def get_pending(self):
        """返回待执行订单列表 (security, amount, exec_time)"""
        return [(p['security'], p['amount'], p['exec_time']) for p in self.pending]
