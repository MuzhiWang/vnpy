def _place_split_order(context, security, total_amount):
    """
    拆单下单，根据总金额拆分若干笔小额订单并安排执行
    """
    current_time = context.current_dt
    # 小额订单直接执行
    if total_amount <= g.split_order_threshold:
        log.info(f"[SplitOrder] 小额订单直接执行: {security}, 金额: {total_amount}")
        order_value(security, total_amount)
        return

    # 需要拆单
    remaining = total_amount
    # 计算拆单笔数
    if total_amount <= 2 * g.split_order_threshold:
        num_splits = 2
    elif total_amount <= 3 * g.split_order_threshold:
        num_splits = 3
    else:
        num_splits = 4

    log.info(f"[SplitOrder] 订单拆分: {security}, 总金额: {total_amount}, 拆分为: {num_splits} 笔")

    # 第1笔立即执行
    first_amount = min(g.max_single_order, remaining)
    order_value(security, first_amount)
    log.info(f"[SplitOrder] 第1笔: {security}, 金额: {first_amount}")
    remaining -= first_amount

    # 安排后续拆单执行
    for i in range(1, num_splits):
        if remaining <= 0:
            break
        # 最后1笔用尽剩余金额
        if i == num_splits - 1:
            split_amount = remaining
        else:
            split_amount = min(g.max_single_order, remaining)
        execution_time = current_time + datetime.timedelta(minutes=i * g.split_interval_minutes)
        g.pending_orders.append((security, split_amount, execution_time))
        log.info(f"[SplitOrder] 安排第{i+1}笔: {security}, 金额: {split_amount}, 执行时间: {execution_time}")
        remaining -= split_amount


def handle_all_pending_orders(context):
    """
    定时执行到点的拆单订单
    """
    current_time = context.current_dt
    # 筛选可以执行的订单索引
    to_exec = [(idx, ord) for idx, ord in enumerate(g.pending_orders) if ord[2] <= current_time]
    # 逆序执行并删除
    for idx, (security, amount, _) in sorted(to_exec, key=lambda x: x[0], reverse=True):
        log.info(f"[SplitOrder] 执行拆单订单: {security}, 金额: {amount}")
        order_value(security, amount)
        g.pending_orders.pop(idx)