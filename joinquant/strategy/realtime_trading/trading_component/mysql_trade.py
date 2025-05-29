# MySQLTrade

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import OperationalError
import sqlalchemy.engine.url as url
import threading
import json
import MySQLdb
from functools import wraps
from kuanke.user_space_api import *
import time

"""
通过mysql传递交易信号

注意：
    1)使用前将MysqlTrade类中的host,port, password改成自己的
    2)选择策略mode：mode = 0, 测试：策略历史回测(back_test)时，发送交易信号；
                  mode = 1, 正式：策略模拟交易(sim_trade)时，发送交易信号。
"""


class MysqlTrade:
    host = 'douguastock.mysql.rds.aliyuncs.com'
    port = 3306
    dbname = 'trunksprofessor_3545'
    dbuser = 'trunksprofessor_3545'
    dbpassword = 'trunksprofessor_3545'
    mode = 1  # 0: 测试，1：正式

    @staticmethod
    def trade_signal(func):

        @wraps(func)
        def wrapper(*args, **kwargs):
            context = kwargs.get('context') or args[0]
            security = kwargs.get('security') or args[1]
            # 下单前的现金、股票数量
            pre_cash = context.portfolio.available_cash
            pre_amt = 0
            if security in context.portfolio.positions:
                pre_amt = context.portfolio.positions[security].total_amount
            #
            my_order = func(*args, **kwargs)
            if my_order is not None:
                if my_order.is_buy:  # 买入，看现金
                    new_cash = context.portfolio.available_cash  # 10W
                    new_amt = context.portfolio.positions[security].total_amount
                    amt = new_amt - pre_amt
                    pct = round(1.0 - new_cash / pre_cash, 4)  # 1 - 10/15 = 1/3, 即买入现有现金的1/3
                else:  # 卖出，看持仓
                    if security not in context.portfolio.positions:
                        new_amt = 0
                    else:
                        new_amt = context.portfolio.positions[security].total_amount  # 卖出后，持有的数量500股
                    #
                    amt = pre_amt - new_amt

                    pct = round(1.0 - new_amt / pre_amt, 4)  # 1-500/2000 = 3/4, 即卖出现持仓的股票的3/4
                #
                security = security[:7] + ('SS' if security[-1] == 'G' else 'SZ')

                time = my_order.add_time.strftime('%Y-%m-%d %H:%M:%S')
                action = 'BUY' if my_order.is_buy else 'SELL'
                code = security
                amt = amt
                pct = pct
                strategy = g.strategy
                data = {
                    'time': my_order.add_time.strftime('%Y-%m-%d %H:%M:%S'),
                    'action': 'BUY' if my_order.is_buy else 'SELL',
                    'code': security,
                    'amt': amt,
                    'pct': pct,
                    'strategy': g.strategy
                }
                log.info(data)

                if context.run_params.type == 'sim_trade' or MysqlTrade.mode == 0:  # mode = 0: 测试； mode == 1：正式

                    engine, conn = MysqlTrade.connect_to_mysql()
                    if conn != "fail":
                        Base.metadata.create_all(engine)
                        Session = sessionmaker(bind=engine)
                        session = Session()
                        order = Stock(time=time, action=action, code=code, amt=amt, pct=pct, strategy=strategy,
                                      isconsum='NO', mode=MysqlTrade.mode)
                        # session.merge(order)
                        session.add(order)
                        session.commit()
                        session.close()

            return my_order

        return wrapper

    @staticmethod
    def connect_to_mysql():
        uri = url.URL(
            drivername="mysql+mysqldb",
            host=MysqlTrade.host,
            port=MysqlTrade.port,
            username=MysqlTrade.dbuser,
            password=MysqlTrade.dbpassword,
            database=MysqlTrade.dbname,
        )

        max_attempts = 5
        attempts = 0
        while attempts < max_attempts:
            try:

                # 尝试连接MySQL数据库
                engine = create_engine(uri, encoding='utf-8', echo=False,
                                       pool_size=100, pool_recycle=3600,
                                       pool_pre_ping=True)
                conn = engine.connect()
                log.info("成功连接到MySQL数据库！")
                return engine, conn
            except Exception as e:
                # 如果连接失败，打印错误信息并尝试重新连接
                log.info(f"连接MySQL数据库失败，错误信息：{e}")
                attempts += 1
                time.sleep(10)
        log.info(f"连接MySQL数据库失败，已达到最大尝试次数：{max_attempts}")
        engine = "fail"
        conn = "fail"
        return engine, conn


Base = declarative_base()


class Stock(Base):
    __tablename__ = 'stocks'

    id = Column(Integer, primary_key=True, autoincrement=True)
    time = Column(String(255))
    action = Column(String(255))
    code = Column(String(255))
    amt = Column(String(255))
    pct = Column(String(255))
    strategy = Column(String(255))
    isconsum = Column(String(255))
    mode = Column(String(255))


@MysqlTrade.trade_signal
def order_(context, security, amount, style=None):
    _order = order(security, amount, style)
    return _order


@MysqlTrade.trade_signal
def order_target_(context, security, amount, style=None):
    _order = order_target(security, amount, style)
    return _order


@MysqlTrade.trade_signal
def order_value_(context, security, value, style=None):
    _order = order_value(security, value, style)
    return _order


@MysqlTrade.trade_signal
def order_target_value_(context, security, value, style=None):
    _order = order_target_value(security, value, style)
    return _order
