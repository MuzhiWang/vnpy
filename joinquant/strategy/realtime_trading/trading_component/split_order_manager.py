import datetime
import math
from kuanke.wizard import log
import mysqltrade as mt
from collections import defaultdict
from enum import Enum
from typing import Optional, Dict, Any, Callable


class OrderExecutionPolicy(Enum):
    """订单执行策略枚举"""
    DEFAULT = "default"  # 默认行为，跟随pending队列，无特殊逻辑
    CANCEL_NEXT_DAY = "cancel_next_day"  # 次日取消
    CANCEL_AFTER_TIME = "cancel_after_time"  # 指定时间后取消
    CUSTOM_CONDITION = "custom_condition"  # 自定义条件


class OrderExecutionConfig:
    """订单执行配置类"""

    def __init__(
            self,
            policy: OrderExecutionPolicy = OrderExecutionPolicy.DEFAULT,
            cancel_after_hours: Optional[int] = None,
            cancel_after_datetime: Optional[datetime.datetime] = None,
            buy_order_policy: Optional[OrderExecutionPolicy] = None,
            sell_order_policy: Optional[OrderExecutionPolicy] = None,
            custom_cancel_condition: Optional[Callable] = None,
            apply_to_buy_orders: bool = True,
            apply_to_sell_orders: bool = True
    ):
        """
        初始化订单执行配置

        :param policy: 基础执行策略
        :param cancel_after_hours: 多少小时后取消
        :param cancel_after_datetime: 指定时间后取消
        :param buy_order_policy: 买入订单特定策略
        :param sell_order_policy: 卖出订单特定策略
        :param custom_cancel_condition: 自定义取消条件函数
        :param apply_to_buy_orders: 是否应用于买入订单
        :param apply_to_sell_orders: 是否应用于卖出订单
        """
        self.policy = policy
        self.cancel_after_hours = cancel_after_hours
        self.cancel_after_datetime = cancel_after_datetime
        self.buy_order_policy = buy_order_policy
        self.sell_order_policy = sell_order_policy
        self.custom_cancel_condition = custom_cancel_condition
        self.apply_to_buy_orders = apply_to_buy_orders
        self.apply_to_sell_orders = apply_to_sell_orders

    def get_effective_policy(self, is_buy_order: bool) -> OrderExecutionPolicy:
        """获取对特定订单类型生效的策略"""
        if is_buy_order and self.buy_order_policy is not None:
            return self.buy_order_policy
        elif not is_buy_order and self.sell_order_policy is not None:
            return self.sell_order_policy
        else:
            return self.policy

    def should_apply_to_order(self, is_buy_order: bool) -> bool:
        """判断配置是否适用于指定类型的订单"""
        if is_buy_order:
            return self.apply_to_buy_orders
        else:
            return self.apply_to_sell_orders


class PendingOrder:
    """
    表示一个待执行的拆分订单。
    """

    def __init__(
            self,
            func,
            context,
            security,
            shares=None,
            value=None,
            exec_time=None,
            idx=None,
            sell_all=False,
            origin=None,
            execution_config: Optional[OrderExecutionConfig] = None,
            is_buy_order: Optional[bool] = None,  # 新增：明确指定是否为买入订单
    ):
        """
        :param func: 下单函数，按股数买/卖 (签名 func(context, security, shares))
        :param context: 策略上下文
        :param security: 证券代码
        :param shares: 股数 (正数=买入, 负数=卖出)
        :param value: 市值
        :param exec_time: 执行时间
        :param idx: 订单索引
        :param sell_all: 是否卖出全部持仓
        :param origin: 原始订单信息
        :param execution_config: 执行配置
        :param is_buy_order: 明确指定是否为买入订单（避免运行时计算）
        """
        self.func = func
        self.context = context
        self.security = security
        self.shares = shares  # 绝对股数 (正数=买入, 负数=卖出)
        self.value = value  # 可选，订单市值
        self.exec_time = exec_time
        self.executed_time = None  # 记录实际执行时间
        self.idx = idx  # 可选，订单索引，用于跟踪或调试
        self.sell_all = sell_all  # 是否卖出全部持仓，若为 True 则 shares & value 被忽略
        self.origin = origin or {}  # 原始订单信息

        # 执行配置相关
        self.execution_config = execution_config or OrderExecutionConfig()
        self.original_exec_date = exec_time.date() if exec_time else None
        self.original_exec_time = exec_time
        self.first_attempt_time = exec_time

        # 买入订单判断 - 必须明确确定
        if is_buy_order is not None:
            self.is_buy_order = is_buy_order
        elif self.shares is not None:
            self.is_buy_order = self.shares > 0
        elif self.sell_all:
            self.is_buy_order = False  # 卖出全部持仓肯定是卖出订单
        else:
            # 对于基于市值的订单，必须明确指定买卖方向
            raise ValueError(f"[PendingOrder] 无法确定订单 {security} 的买卖方向，必须明确指定 is_buy_order 参数")

    def should_cancel_order(self, context) -> tuple[bool, str]:
        """
        检查订单是否应该被取消
        :param context: 策略上下文
        :return: (是否取消, 取消原因)
        """
        if not self.execution_config.should_apply_to_order(self.is_buy_order):
            return False, ""

        current_time = context.current_dt
        current_date = current_time.date()

        effective_policy = self.execution_config.get_effective_policy(self.is_buy_order)

        # 根据不同策略判断是否取消
        if effective_policy == OrderExecutionPolicy.DEFAULT:
            return False, ""

        elif effective_policy == OrderExecutionPolicy.CANCEL_NEXT_DAY:
            if self.original_exec_date and current_date > self.original_exec_date:
                return True, f"次日取消策略: 原始日期 {self.original_exec_date}, 当前日期 {current_date}"

        elif effective_policy == OrderExecutionPolicy.CANCEL_AFTER_TIME:
            if self.execution_config.cancel_after_hours is not None:
                if (self.first_attempt_time and
                        current_time >= self.first_attempt_time + datetime.timedelta(
                            hours=self.execution_config.cancel_after_hours)):
                    return True, f"超过指定时间: {self.execution_config.cancel_after_hours}小时"

            if (self.execution_config.cancel_after_datetime is not None and
                    current_time >= self.execution_config.cancel_after_datetime):
                return True, f"超过指定时间点: {self.execution_config.cancel_after_datetime}"

        elif effective_policy == OrderExecutionPolicy.CUSTOM_CONDITION:
            if self.execution_config.custom_cancel_condition is not None:
                try:
                    should_cancel, reason = self.execution_config.custom_cancel_condition(self, context)
                    if should_cancel:
                        return True, f"自定义条件: {reason}"
                except Exception as e:
                    log.error(f"[PendingOrder] 自定义取消条件执行失败: {e}")

        return False, ""

    def execute(self, context=None):
        if self.shares is None and self.value is None:
            log.error(f"[PendingOrder] 执行订单失败: {self.security} 股数和市值都未指定 | 原始订单: {self.origin}")
            return
        if self.exec_time is None:
            log.error(f"[PendingOrder] 执行订单失败: {self.security} 执行时间未指定 | 原始订单: {self.origin}")
            return

        """执行 pending 订单"""
        ctx = context or self.context
        try:
            val = self.shares if self.shares is not None else self.value
            # 新增：防止目标市值为负数
            if self.value is not None:
                val = max(0, self.value)
            if self.sell_all:
                val = 0  # 卖出全部持仓时，股数和市值都忽略，直接卖空
            res = self.func(ctx, self.security, val)
            self.executed_time = ctx.current_dt
            if res is not None:
                log.info(
                    f"[PendingOrder] 执行订单: {self.security}, 第{self.idx + 1} 股数: {self.shares} 市值: {self.value}, 结果: {res} | 原始订单: {self.origin}")
            else:
                log.warn(
                    f"[PendingOrder] 执行订单 返回 None: {self.security} 股数: {self.shares} 市值: {self.value} | 原始订单: {self.origin}")
            return res
        except Exception as e:
            log.error(
                f"[PendingOrder] 执行订单失败: {self.security} 股数: {self.shares} 市值: {self.value}, 错误: {e} | 原始订单: {self.origin}")
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
        self.positions = {}  # 当前持仓 {security: amount}
        self.pending_sell_securities = {}  # 待卖出证券 {security: amount}

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

    def __init__(self, get_current_data_func, split_threshold=50000, max_value=50000, max_splits=4, interval_minutes=4,
                 default_execution_config: Optional[OrderExecutionConfig] = None):
        """
        :param get_current_data_func: 获取实时数据的函数，签名为 func() -> dict[security, DataFrame]
        :param split_threshold: 小于等于该市值直接一次性按目标持仓下单
        :param max_value : 单笔最大市值（元），用于计算单笔最大股数
        :param max_splits: 最大拆单笔数
        :param interval_minutes: 每笔拆单的时间间隔（分钟）
        :param default_execution_config: 默认的订单执行配置，如果个别订单没有指定配置则使用此默认配置
        """
        self.get_current_data = get_current_data_func
        self.split_threshold = split_threshold
        self.max_value = max_value
        self.max_splits = max_splits
        self.interval = datetime.timedelta(minutes=interval_minutes)
        self.pending = []  # 存储 PendingOrder 实例
        self.default_execution_config = default_execution_config or OrderExecutionConfig()  # 默认执行配置

    def _get_effective_execution_config(self, execution_config: Optional[OrderExecutionConfig]) -> OrderExecutionConfig:
        """
        获取有效的执行配置，优先使用传入的配置，否则使用默认配置
        :param execution_config: 传入的执行配置
        :return: 有效的执行配置
        """
        return execution_config if execution_config is not None else self.default_execution_config

    def set_default_execution_config(self, config: OrderExecutionConfig):
        """
        设置默认执行配置
        :param config: 新的默认执行配置
        """
        self.default_execution_config = config
        log.info(f"[PendingOrder] 已更新默认执行配置: {config.policy}")

    def get_default_execution_config(self) -> OrderExecutionConfig:
        """
        获取当前的默认执行配置
        :return: 当前默认执行配置
        """
        return self.default_execution_config

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

    def _compute_value_slices(self, abs_value) -> list:
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

    def _is_order_successful(self, order_result):
        """
        检查订单是否执行成功

        :param order_result: 订单执行结果
        :return: True 如果订单成功执行，False 如果失败
        """
        if order_result is None:
            return False

        # 检查订单是否有实际成交量
        filled = getattr(order_result, "filled", 0)
        if filled == 0:
            return False

        # 检查订单状态
        status = str(getattr(order_result, "status", ""))
        if status == "canceled":
            return False

        # 检查订单金额（可选，作为额外保护）
        amount = getattr(order_result, "amount", 0)
        if amount == 0:
            return False

        return True

    def _has_pending_sell_all_order(self, security) -> bool:
        """
        检查指定证券是否已有待执行的卖出全部持仓订单
        :param security: 证券代码
        :return: True 如果存在卖出全部持仓的待执行订单
        """
        for order in self.pending:
            if order.security == security and order.sell_all:
                return True
        return False

    def _get_max_pending_target_value(self, security) -> int:
        """
        获取指定证券的最大待执行目标市值
        :param security: 证券代码
        :return: 最大目标市值，如果没有待执行订单则返回0
        """
        max_value = 0
        for order in self.pending:
            if order.security == security and order.value is not None:
                max_value = max(max_value, order.value)
        return max_value

    def _check_buy_order_validity(self, context, security, target_value) -> bool:
        """
        检查买入订单的有效性，防止重复买入
        :param context: 策略上下文
        :param security: 证券代码
        :param target_value: 目标市值
        :return: True 如果订单有效，False 如果应该阻止
        """
        current_pos = self._get_current_position(context, security)
        price = self._get_price(context, security)
        current_value = current_pos * price

        # 只检查买入订单（目标市值 > 当前市值）
        if target_value <= current_value:
            return True

        # 检查是否有更大的待执行买入订单
        max_pending_value = self._get_max_pending_target_value(security)

        if max_pending_value >= target_value:
            log.warn(f"[PendingOrder] 阻止 {security} 买入订单: 已存在更大的待执行买入订单 "
                     f"(待执行: {max_pending_value:.2f} 元 >= 新订单: {target_value:.2f} 元)")
            return False

        return True

    def _check_sell_order_validity(self, context, security, target_value):
        """
        检查卖出订单的有效性，防止过度卖出
        :param context: 策略上下文
        :param security: 证券代码
        :param target_value: 目标市值
        :return: True 如果订单有效，False 如果应该阻止
        """
        # 🔥 NEW: 如果已有卖出全部持仓的订单，拒绝新的卖出订单
        if self._has_pending_sell_all_order(security):
            log.warn(f"[PendingOrder] 阻止 {security} 卖出订单: 已存在卖出全部持仓的待执行订单")
            return False

        # 计算当前持仓和已计划卖出的数量
        current_pos = self._get_current_position(context, security)
        price = self._get_price(context, security)
        current_value = current_pos * price

        # 🔥 NEW: 计算已计划卖出的总市值
        planned_sell_value = 0
        for order in self.pending:
            if order.security == security:
                if order.sell_all:
                    planned_sell_value = current_value  # 如果有sell_all，则已计划卖出全部
                    break
                elif order.value is not None and order.value < current_value:
                    # 这是一个减仓订单（目标市值 < 当前市值）
                    sell_amount = current_value - order.value
                    planned_sell_value += sell_amount

        # 🔥 NEW: 检查新订单是否会导致过度卖出
        if target_value < current_value:  # 这是一个卖出订单
            additional_sell = current_value - target_value
            total_planned_sell = planned_sell_value + additional_sell

            if total_planned_sell > current_value:
                log.warn(f"[PendingOrder] 阻止 {security} 卖出订单: 总计划卖出 {total_planned_sell:.2f} 元 "
                         f"> 当前持仓 {current_value:.2f} 元")
                return False

        return True

    def _check_order_validity(self, context, security, target_value):
        """
        检查订单的有效性，包括买入和卖出订单的验证
        :param context: 策略上下文
        :param security: 证券代码
        :param target_value: 目标市值
        :return: True 如果订单有效，False 如果应该阻止
        """
        current_pos = self._get_current_position(context, security)
        price = self._get_price(context, security)
        current_value = current_pos * price

        if target_value > current_value:
            # 买入订单检查
            return self._check_buy_order_validity(context, security, target_value)
        elif target_value < current_value:
            # 卖出订单检查
            return self._check_sell_order_validity(context, security, target_value)
        else:
            # 目标市值等于当前市值，无需操作
            log.info(f"[PendingOrder] {security} 目标市值 {target_value:.2f} 元等于当前市值，无需操作")
            return False

    def _schedule_value(self, context, security, value_legs, sign, target_value, origin=None,
                        execution_config: Optional[OrderExecutionConfig] = None):
        """
        第一笔立即下单，后续进入pending队列。若第一笔失败，进入pending并顺延所有pending。
        """
        now = context.current_dt
        price = self._get_price(context, security)
        current_pos = self._get_current_position(context, security)
        current_value = current_pos * price

        # 使用有效的执行配置
        effective_config = self._get_effective_execution_config(execution_config)

        # 确定是否为买入订单
        is_buy_order = sign > 0

        accum = current_value
        first_order_failed = False

        for i, delta in enumerate(value_legs):
            accum += delta * sign
            if i == 0:
                # 立即下第一笔
                log.info(f"[PendingOrder] 立即执行第1笔{'买入' if sign > 0 else '卖出'}: {security} 目标市值: {accum}")
                po = PendingOrder(
                    mt.order_target_value_,
                    context,
                    security,
                    None,
                    accum,
                    now,
                    i,
                    origin=origin,
                    execution_config=effective_config,
                    is_buy_order=is_buy_order
                )
                res = po.execute(context)
                if not self._is_order_successful(res):
                    # 如果失败，则丢入pending
                    log.warn(f"[PendingOrder] 第1笔{security}执行失败，加入pending队列并顺延所有pending")
                    first_order_failed = True
                    # 第一笔失败时，重新安排到第一个时间slot
                    po.exec_time = now + self.interval
                    self.pending.append(po)
                    log.info(f"[PendingOrder] 第1笔失败订单已重新安排到: {po.exec_time}")
                else:
                    log.info(f"[PendingOrder] 第1笔{security}执行成功")
            else:
                # 如果第一笔失败，所有后续订单都要往后顺延一个时间slot
                if first_order_failed:
                    exec_time = now + self.interval * (i + 1)
                else:
                    exec_time = now + self.interval * i
                po = PendingOrder(
                    mt.order_target_value_,
                    context,
                    security,
                    None,
                    accum,
                    exec_time,
                    i,
                    origin=origin,
                    execution_config=effective_config,
                    is_buy_order=is_buy_order
                )
                if i == len(value_legs) - 1 and target_value == 0:
                    po.sell_all = True
                    po.is_buy_order = False  # 卖出全部持仓肯定是卖出订单
                self.pending.append(po)
                log.info(
                    f"[PendingOrder] 安排第{i + 1}笔{'买入' if sign > 0 else '卖出'}: {security} 目标市值: {accum} 执行时间: {exec_time}")

    def place_order_value(self, context, security, target_value,
                          execution_config: Optional[OrderExecutionConfig] = None):
        """
        按市值 target_value 下单: 自动识别买卖方向，并拆分交易。
        :param context: 策略上下文
        :param security: 证券代码
        :param target_value: 目标市值 (元)
        :param execution_config: 执行配置
        """
        price = self._get_price(context, security)
        if price is None or price <= 0:
            log.error(f"[PendingOrder] 无法获取 {security} 最新价格，跳过")
            return

        # 当前持仓股数
        current_pos = self._get_current_position(context, security)
        current_value = current_pos * price
        log.info(
            f"[PendingOrder] 当前 {security} 持仓: {current_pos}, 市值: {current_value} 元, 目标市值: {target_value} 元")

        # 检查订单的有效性（买入和卖出）
        if not self._check_order_validity(context, security, target_value):
            log.error(f"[PendingOrder] 订单被阻止: {security} 目标市值 {target_value} 元")
            return

        # 需调整市值 (正买, 负卖)
        delta_value = target_value - current_value

        # 若市值 ≤ 阈值, 直接调整持仓到目标
        if abs(delta_value) <= self.split_threshold:
            log.info(f"[PendingOrder] 直接调整持仓 {security} 到目标市值 {target_value}元")
            return mt.order_target_value_(context, security, target_value)

        # 拆单：计算市值拆分
        abs_value = abs(delta_value)
        value_slices = self._compute_value_slices(abs_value)
        action = "买入" if delta_value > 0 else "卖出"
        log.info(
            f"[PendingOrder] 拆分 {security} {action} 总市值 {abs_value}元 分 {len(value_slices)} 笔: {value_slices}")

        sign = 1 if delta_value > 0 else -1

        origin_info = {
            "order_time": context.current_dt,
            "original_target_value": target_value,
            "split_count": len(value_slices),
            "action": action,
            "security": security,
        }
        self._schedule_value(context, security, value_slices, sign, target_value, origin=origin_info,
                             execution_config=execution_config)

    def order_target_value_(self, context, security, target_value,
                            execution_config: Optional[OrderExecutionConfig] = None):
        """与 mysql_trade order_target_value_ 同名接口，按市值拆分下单"""
        log.info(f"[PendingOrder] 请求 {security} 目标市值 {target_value} 元")
        return self.place_order_value(context, security, target_value, execution_config)

    def order_value_(self, context, security, amount, execution_config: Optional[OrderExecutionConfig] = None):
        """与 mysql_trade order_value_ 同名接口，按市值拆分下单

        :param context: 策略上下文
        :param security: 证券代码
        :param amount: 交易金额，正数买入，负数卖出
        :param execution_config: 执行配置
        """
        log.info(f"[PendingOrder] 请求 {security} {'买入' if amount > 0 else '卖出'}市值 {abs(amount)} 元")

        price = self._get_price(context, security)
        if price is None or price <= 0:
            log.error(f"[PendingOrder] 无法获取 {security} 最新价格，跳过")
            return

        # 当前持仓市值
        current_pos = self._get_current_position(context, security)
        current_value = current_pos * price

        # 目标市值 = 当前市值 + 要交易的金额
        target_value = current_value + amount

        return self.place_order_value(context, security, target_value, execution_config)

    def order_target_(self, context, security, shares, execution_config: Optional[OrderExecutionConfig] = None):
        """与 mysql_trade order_target_ 同名接口，按目标持仓拆单下单"""
        log.info(f"[PendingOrder] 请求 {security} 目标持仓 {shares} 股")
        price = self._get_price(context, security)
        if price is None or price <= 0:
            log.error(f"[PendingOrder] 无法获取 {security} 最新价格，跳过")
            return
        # 目标持仓股数
        target_shares = shares
        target_value = target_shares * price
        return self.place_order_value(context, security, target_value, execution_config)

    def _cancel_orders_by_policy(self, context):
        """
        根据执行策略取消订单
        :param context: 策略上下文
        """
        orders_to_remove = []

        for order in self.pending:
            should_cancel, reason = order.should_cancel_order(context)
            if should_cancel:
                orders_to_remove.append(order)
                action = "买入" if order.is_buy_order else "卖出"
                log.info(f"[PendingOrder] 取消{action}订单: {order.security} - {reason} | 原始订单: {order.origin}")

        # 从pending列表中移除取消的订单
        for order in orders_to_remove:
            self.pending.remove(order)

        if orders_to_remove:
            log.info(f"[PendingOrder] 已取消 {len(orders_to_remove)} 个订单")

    def execute_pending(self, context):
        """
        每轮只执行每只股票pending队列中的第一单，其它pending即使到时间也要等待前一单完成
        若订单执行失败，会重新安排在该股票最后一笔订单之后执行
        """
        from collections import defaultdict

        # 首先检查并取消符合取消策略的订单
        self._cancel_orders_by_policy(context)

        now = context.current_dt
        pending_by_sec = defaultdict(list)

        # 按 security 分组
        for p in self.pending:
            pending_by_sec[p.security].append(p)

        for sec, plist in pending_by_sec.items():
            # 按 exec_time 升序，找最早的pending单
            plist.sort(key=lambda o: o.exec_time)
            first_order = plist[0]
            # 只处理已到时间的pending单
            if first_order.exec_time <= now:
                self.pending.remove(first_order)
                shares = first_order.shares
                action = "未知"
                current_pos = self._get_current_position(context, sec)
                if first_order.shares is not None:
                    action = "买入" if shares > 0 else "卖出"
                    target_shares = current_pos + shares
                    if target_shares < 0:
                        log.warn(
                            f"[PendingOrder] 执行 {action} {sec} 股数 {abs(shares)} 时，目标持仓 {target_shares} 股 < 0 | 原始订单: {first_order.origin}")
                        target_shares = 0
                    first_order.shares = target_shares
                    log.info(
                        f"[PendingOrder] 执行待处理拆单{action}: {sec} 第{first_order.idx + 1}单 股数 {abs(shares)} | 原始订单: {first_order.origin}")
                elif first_order.value is not None:
                    # 按市值下单 - 这是主要的订单类型
                    current_pos = self._get_current_position(context, sec)
                    current_price = self._get_price(context, sec)

                    current_value = current_pos * current_price
                    target_value = first_order.value

                    action = "买入" if target_value > current_value else "卖出"

                    target_value = max(0, first_order.value)
                    log.info(
                        f"[PendingOrder] 执行待处理拆单{action}: {sec} 第{first_order.idx + 1}单 目标市值 {target_value} | 原始订单: {first_order.origin}")

                try:
                    # 尝试执行订单
                    res = first_order.execute(context)

                    if not self._is_order_successful(res):
                        log.warn(
                            f"[PendingOrder] 执行 {action} {sec} 订单失败或无成交量，重新安排. Res: {res} | 原始订单: {first_order.origin}")
                        self.reschedule_order(self.pending, sec, self.interval, now, first_order, action)
                except Exception as e:
                    log.error(f"[PendingOrder] 执行 {action} {sec} 订单异常: {e} | 原始订单: {first_order.origin}")
                    self.reschedule_order(self.pending, sec, self.interval, now, first_order, action)

    def reschedule_order(self, pending_orders, security, interval, current_time, order, action):
        """
        Reschedules an order by updating its execution time and re-adding it to the pending list.

        :param pending_orders: List of pending orders.
        :param security: Security code of the order.
        :param interval: Time interval for rescheduling.
        :param current_time: Current time.
        :param order: The order to be rescheduled.
        :param action: Action type (e.g., "买入" or "卖出").
        """
        """
        当有拆单失败时，把该 security 的所有 pending 顺序整体往后移动一位，并把失败订单放在第一个未完成订单的位置
        """
        # 取出该股票所有 pending，按 exec_time 排序
        same_security_orders = [o for o in pending_orders if o.security == security]
        same_security_orders.sort(key=lambda o: o.exec_time)

        # 新 pending 队列（不包含当前 order）
        others = [o for o in same_security_orders if o is not order]

        if others:
            # 新时间表：第一个pending slot 给失败订单，后面依次往后推
            # 1. 失败订单执行时间设为第一个pending的原exec_time
            old_times = [o.exec_time for o in others]
            order.exec_time = old_times[0]
            # 2. 依次让其它pending订单时间往后顺延（第二个时间给原第一个，第三个给原第二个...）
            for i in range(len(others)):
                if i + 1 < len(old_times):
                    others[i].exec_time = old_times[i + 1]
                else:
                    others[i].exec_time = old_times[-1] + interval

        else:
            # 没有别的订单，延后一个interval
            order.exec_time = current_time + interval

        # 放回 pending_orders（避免重复，加前先移除）
        pending_orders[:] = [o for o in pending_orders if o.security != security or o is order]
        pending_orders.append(order)
        pending_orders.extend(others)
        log.info(
            f"[PendingOrder] 重新安排失败的{action}订单: {security}, 新执行时间: {order.exec_time} | 原始订单: {order.origin}")

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
                log.error(f"[PendingOrder] get_available_balance: 无法获取 {security} 价格，忽略该笔待处理订单")
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

        log.info(f"[PendingOrder] 余额计算: 当前现金={balance.current_cash:.2f}, "
                 f"待买入={balance.pending_buy:.2f}, "
                 f"待卖出={balance.pending_sell:.2f}, "
                 f"可用={balance.available_cash:.2f}")

        return balance


# 使用示例和工厂函数
class OrderExecutionConfigFactory:
    """订单执行配置工厂类，提供常用配置的便捷创建方法"""

    @staticmethod
    def default():
        """创建默认配置（跟随pending队列，无特殊逻辑）"""
        return OrderExecutionConfig(
            policy=OrderExecutionPolicy.DEFAULT
        )

    @staticmethod
    def cancel_buying_next_day():
        """创建次日取消买入订单的配置"""
        return OrderExecutionConfig(
            buy_order_policy=OrderExecutionPolicy.CANCEL_NEXT_DAY,
            apply_to_buy_orders=True,
            apply_to_sell_orders=False
        )

    @staticmethod
    def cancel_after_hours(hours: int, apply_to_both: bool = True):
        """创建指定小时后取消的配置"""
        return OrderExecutionConfig(
            policy=OrderExecutionPolicy.CANCEL_AFTER_TIME,
            cancel_after_hours=hours,
            apply_to_buy_orders=apply_to_both,
            apply_to_sell_orders=apply_to_both
        )

    @staticmethod
    def different_policies_for_buy_sell(buy_policy: OrderExecutionPolicy, sell_policy: OrderExecutionPolicy, **kwargs):
        """为买入和卖出订单创建不同的策略配置"""
        return OrderExecutionConfig(
            buy_order_policy=buy_policy,
            sell_order_policy=sell_policy,
            apply_to_buy_orders=True,
            apply_to_sell_orders=True,
            **kwargs
        )

    @staticmethod
    def custom_condition(condition_func: Callable):
        """创建自定义条件配置"""
        return OrderExecutionConfig(
            policy=OrderExecutionPolicy.CUSTOM_CONDITION,
            custom_cancel_condition=condition_func
        )


# 使用示例：
"""
# 1. 创建带默认配置的SplitOrderManager（默认行为，跟随pending队列）
default_config = OrderExecutionConfigFactory.default()
split_manager = SplitOrderManager(
    get_current_data_func=get_current_data,
    default_execution_config=default_config
)

# 所有订单都会跟随正常的pending队列逻辑，无特殊取消条件
split_manager.order_target_value_(context, security, 10000)  # 使用默认配置
split_manager.order_value_(context, security, 5000)  # 使用默认配置

# 2. 次日取消买入订单的配置
cancel_buy_config = OrderExecutionConfigFactory.cancel_buying_next_day()
split_manager.order_target_value_(context, security, 10000, execution_config=cancel_buy_config)

# 3. 指定时间后取消
time_config = OrderExecutionConfigFactory.cancel_after_hours(2)
split_manager.order_target_value_(context, security, 10000, execution_config=time_config)

# 4. 运行时更改默认配置
new_default = OrderExecutionConfigFactory.cancel_buying_next_day()
split_manager.set_default_execution_config(new_default)

# 现在所有新的买入订单都会在次日取消
split_manager.order_target_value_(context, security, 10000)  # 使用新默认配置

# 5. 获取当前默认配置
current_default = split_manager.get_default_execution_config()
print(f"当前默认策略: {current_default.policy}")

# 6. 不同场景的配置示例：

# 场景A: 保守策略 - 买入订单次日取消，卖出订单默认行为
conservative_config = OrderExecutionConfig(
    buy_order_policy=OrderExecutionPolicy.CANCEL_NEXT_DAY,
    sell_order_policy=OrderExecutionPolicy.DEFAULT,
    apply_to_buy_orders=True,
    apply_to_sell_orders=True
)

# 场景B: 时间限制策略 - 2小时后取消所有订单
time_limited_config = OrderExecutionConfig(
    policy=OrderExecutionPolicy.CANCEL_AFTER_TIME,
    cancel_after_hours=2
)

# 场景C: 混合策略 - 买入订单2小时后取消，卖出订单默认行为
hybrid_config = OrderExecutionConfig(
    buy_order_policy=OrderExecutionPolicy.CANCEL_AFTER_TIME,
    sell_order_policy=OrderExecutionPolicy.DEFAULT,
    cancel_after_hours=2,
    apply_to_buy_orders=True,
    apply_to_sell_orders=True
)

# 创建不同策略的管理器
conservative_manager = SplitOrderManager(get_current_data, default_execution_config=conservative_config)

# 7. 自定义取消条件示例
def market_volatility_condition(order, context):
    '''如果市场波动率超过5%，取消所有待处理订单'''
    current_time = context.current_dt
    market_hours_passed = (current_time - order.first_attempt_time).total_seconds() / 3600

    if market_hours_passed > 4:  # 超过4小时自动取消
        return True, "超过4个交易小时"

    # 检查价格波动（需要在order中存储原始价格）
    if hasattr(order.origin, 'original_price'):
        current_price = context.get_current_data()[order.security].last_price
        price_change = abs(current_price - order.origin['original_price']) / order.origin['original_price']
        if price_change > 0.05:
            return True, f"价格波动超过5%: {price_change:.2%}"

    return False, ""

custom_config = OrderExecutionConfig(
    policy=OrderExecutionPolicy.CUSTOM_CONDITION,
    custom_cancel_condition=market_volatility_condition
)

custom_manager = SplitOrderManager(get_current_data, default_execution_config=custom_config)

# 8. 工厂方法结合SplitOrderManager
def create_default_split_manager(get_current_data_func):
    '''创建默认型拆单管理器（正常pending队列行为）'''
    config = OrderExecutionConfigFactory.default()
    return SplitOrderManager(get_current_data_func, default_execution_config=config)

def create_conservative_split_manager(get_current_data_func):
    '''创建保守型拆单管理器（买入订单次日取消）'''
    config = OrderExecutionConfigFactory.cancel_buying_next_day()
    return SplitOrderManager(get_current_data_func, default_execution_config=config)

def create_time_limited_split_manager(get_current_data_func, hours=2):
    '''创建时间限制型拆单管理器（指定小时后取消）'''
    config = OrderExecutionConfigFactory.cancel_after_hours(hours)
    return SplitOrderManager(get_current_data_func, default_execution_config=config)

# 使用工厂方法
manager = create_conservative_split_manager(get_current_data)
manager.order_target_value_(context, "000001.XSHE", 100000)  # 自动应用保守策略

# 9. 配置的继承和覆盖优先级：
# 优先级（从高到低）：
# 1. 单个订单指定的execution_config参数
# 2. SplitOrderManager的default_execution_config
# 3. OrderExecutionConfig()的默认值（DEFAULT策略，跟随pending队列）

# 示例：
manager_with_default = SplitOrderManager(
    get_current_data,
    default_execution_config=OrderExecutionConfigFactory.cancel_buying_next_day()
)

# 这个订单使用默认配置（次日取消买入）
manager_with_default.order_target_value_(context, "000001.XSHE", 50000)

# 这个订单使用特殊配置（2小时后取消），覆盖默认配置
special_config = OrderExecutionConfigFactory.cancel_after_hours(2)
manager_with_default.order_target_value_(context, "000002.XSHE", 50000, execution_config=special_config)

# 10. 简化的策略类型：
# - DEFAULT: 跟随正常pending队列，无特殊取消逻辑
# - CANCEL_NEXT_DAY: 次日取消
# - CANCEL_AFTER_TIME: 指定时间后取消
# - CUSTOM_CONDITION: 自定义取消条件

# 所有策略都基于取消机制，无重试计数或复杂逻辑
"""