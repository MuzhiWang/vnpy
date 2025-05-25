# PT下单代码

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import OperationalError
import sqlalchemy.engine.url as url
import threading
from sqlalchemy import and_
import pandas as pd
import pymysql
import time as d_time


def initialize(context):
    g.email_count = 1
    set_commission(commission_ratio=0.0003, min_commission=5.0, type="STOCK")
    run_interval(context, trademain, seconds = 3)


def trademain(context):

    host = 'douguastock.mysql.rds.aliyuncs.com'
    port = 3306
    dbname = 'trunksprofessor_3545'
    dbuser = 'trunksprofessor_3545'
    dbpassword = 'trunksprofessor_3545'

    is_use_email = False #填写True代表启用邮箱，False表示不启用邮箱。注意：使用邮箱必须填写三个信息
    send_email_name = 'XXXXXXXXX@qq.com' #填写发送的邮箱地址
    get_email_name = ['312XXXXX@qq.com'] #填写接收的邮箱地址
    smtp_code_info = '这里填写smtp授权码' #邮箱的smtp授权码，注意，不是邮箱密码

    try:
        uri = url.URL(
        drivername="mysql+pymysql",
        host=host,
        port=port,
        username=dbuser,
        password=dbpassword,
        database=dbname,
        )
        engine = create_engine(uri, encoding='utf-8',echo=False,pool_size=10, max_overflow=5,pool_recycle=3600)

        Base.metadata.create_all(engine)

        Session = sessionmaker(bind=engine)
        session = Session()

        sell_order_list = session.query(Stock).filter(and_(Stock.strategy == "dougua1",Stock.action == "SELL",Stock.mode == "1")).all() #获取卖出股票列表
        buy_order_list = session.query(Stock).filter(and_(Stock.strategy == "dougua1",Stock.action == "BUY",Stock.mode == "1")).all() #获取买入股票列表

        email_info = []
        for order_info in sell_order_list:
            amt = -int(order_info.amt)
            stock = order_info.code
            time = order_info.time
            info = {"推送股票":stock,"交易数量":amt,"交易时间":time}
            log.info("推送的卖出信息：%s" % info)
            buy_limit,sell_limit = get_trade_limit_price(stock)
            order_id = order_market(stock, amt, 4, limit_price=sell_limit) #市价单委托，最优五档即时成交剩余撤销；
            session.delete(order_info)
            email_info.append(info)
        if len(email_info)>0:
            log.info('卖出订单已全部委托，30秒后开始买入')
            d_time.sleep(30) #延迟30秒

        for order_info in buy_order_list:
            amt = int(order_info.amt)
            stock = order_info.code
            time = order_info.time
            info = {"推送股票":stock,"交易数量":amt,"交易时间":time}
            log.info("推送的买入信息：%s" % info)
            buy_limit,sell_limit = get_trade_limit_price(stock)
            order_id = order_market(stock, amt, 4, limit_price=buy_limit) #市价单委托，最优五档即时成交剩余撤销；
            session.delete(order_info)
            email_info.append(info)

        session.commit()
        session.close()
        engine.dispose()

        df_email_info = pd.DataFrame(email_info)
        if not df_email_info.empty and is_use_email:
            email(send_email_name, get_email_name, smtp_code_info,df_email_info)

        elif df_email_info.empty:
            log.info("此刻没有需要交易的股票" )

    except Exception as e:
        log.info("发生错误: {}".format(e))
        if is_use_email and g.email_count == 1:
            email_info = "策略代码错误,请尽快检查"
            email(send_email_name, get_email_name, smtp_code_info,email_info)
            g.email_count = 0
        return(e)

def before_trading_start(context, data):

    g.email_count = 1


def email(send_email_name, get_email_name, smtp_code_info,email_info):
    try:
        send_email(send_email_name, get_email_name, smtp_code_info, info=email_info)
    except Exception as e:
        print("发送邮件失败: {}".format(e))
    return(e)



def get_trade_limit_price(stock):

    df = get_snapshot(stock)

    last_price = df[stock]['last_px']
    high_limit = df[stock]['up_px']
    low_limit = df[stock]['down_px']

    buy_limit = min(round(max(last_price*1.02-0.01,last_price+0.1),2),high_limit)
    sell_limit= max(round(min(last_price*0.98+0.01,last_price-0.1),2),low_limit)

    return buy_limit,sell_limit



Base = declarative_base()

class Stock(Base):
    __tablename__ = 'stocks'

    id = Column(Integer, primary_key=True, autoincrement = True)
    time = Column(String(255))
    action = Column(String(255))
    code = Column(String(255))
    amt = Column(String(255))
    pct = Column(String(255))
    strategy = Column(String(255))
    isconsum = Column(String(255))
    mode = Column(String(255))

    def handle_data(context, data):
        pass
