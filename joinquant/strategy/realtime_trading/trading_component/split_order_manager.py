import datetime
import functools
from kuanke.wizard import log
import mysql_trade as mt
from server.utils.tornado_utils import write_error


class SplitOrderManager:
    """
    Manage splitting of large orders into smaller chunks and scheduling execution.
    Provides methods order_, order_target_, order_value_, order_target_value_ that mirror mysql_trade.
    """
    def __init__(self, split_threshold=50_000, max_single=50_000, interval_minutes=4):
        self.split_threshold = split_threshold
        self.max_single = max_single
        self.interval = interval_minutes
        # pending: list of tuples (func, context, security, amount, exec_time, args, kwargs)
        self.pending = []

    def place_order(self, func):

        @wraps(func)
        def wrapper(*args, **kwargs):
            context = kwargs.get('context') or args[0]
            security = kwargs.get('security') or args[1]

            return func(args, kwargs)

        return wrapper

    def place(self, func, context, security, total_amount, *args, **kwargs):
        """
        Splits or places the order via provided func(context, security, amount, *args, **kwargs).
        """
        now = context.current_dt
        # Direct order if below threshold
        if total_amount <= self.split_threshold:
            log.info(f"[SplitOrder] Direct: {security} @ {total_amount}")
            return func(context, security, total_amount, *args, **kwargs)

        # Determine splits
        if total_amount <= 2 * self.split_threshold:
            splits = 2
        elif total_amount <= 3 * self.split_threshold:
            splits = 3
        else:
            splits = 4

        log.info(f"[SplitOrder] Splitting {security}: {total_amount} into {splits} legs")
        remaining = total_amount

        # First leg immediate
        first_amt = min(self.max_single, remaining)
        func(context, security, first_amt, *args, **kwargs)
        log.info(f"[SplitOrder] Leg 1 executed: {first_amt}")
        remaining -= first_amt

        # Schedule subsequent legs
        for i in range(1, splits):
            if remaining <= 0:
                break
            amt = remaining if i == splits - 1 else min(self.max_single, remaining)
            exec_time = now + datetime.timedelta(minutes=i * self.interval)
            self.pending.append((func, context, security, amt, exec_time, args, kwargs))
            log.info(f"[SplitOrder] Scheduled leg {i+1}: {security} @ {amt} @ {exec_time}")
            remaining -= amt

    def execute_pending(self, context):
        """
        Execute scheduled orders whose time has come. Call via run_daily.
        """
        now = context.current_dt
        ready = [(idx, rec) for idx, rec in enumerate(self.pending) if rec[4] <= now]
        for idx, (func, ctx, sec, amt, _, args, kwargs) in sorted(ready, key=lambda x: x[0], reverse=True):
            log.info(f"[SplitOrder] Executing pending: {sec} @ {amt}")
            func(ctx, sec, amt, *args, **kwargs)
            self.pending.pop(idx)

    def get_pending(self):
        """List pending orders for inspection: returns (security, amount, exec_time)."""
        return [(sec, amt, et) for (_, _, sec, amt, et, _, _) in self.pending]

    def wrap(self, fn):
        """
        Decorator style wrapper: binds fn for this manager instance.
        """
        @functools.wraps(fn)
        def wrapper(context, security, amount, *args, **kwargs):
            return self.place(fn, context, security, amount, *args, **kwargs)
        return wrapper

    @place_order
    def order_(self, context, security, amount, *args, **kwargs):
        return mt.order_(context, security, amount, *args, **kwargs)

    @place_order
    def order_target_(self, context, security, amount, *args, **kwargs):
        return mt.order_target_(context, security, amount, *args, **kwargs)

    @place_order
    def order_value_(self, context, security, amount, *args, **kwargs):
        return self.wrap(mt.order_value_)(context, security, amount, *args, **kwargs)

    @place_order
    def order_target_value_(self, context, security, amount, *args, **kwargs):
        return self.wrap(mt.order_target_value_)(context, security, amount, *args, **kwargs)
