'''
import sqlite3
import pyodbc

db_path = 'order_log.db'
con_str = f"DRIVER={{SQLite3 ODBC Driver}};SERVER=localhost;DATABASE={db_path};Trusted_connection=yes"
db = pyodbc.connect(con_str)
'''

import os
import csv

order_log_paths = []
trade_log_paths = []
securities_path = 'ListingSecurityList.csv'

for n in os.listdir('OrderLog'):
    p = os.path.join('OrderLog', n)

    if not os.path.isdir(p):
        continue

    ps = os.listdir(p)
    ps.sort()
    order_log_filename, trade_log_filename = ps
    order_log_path = os.path.join(p, order_log_filename)
    trade_log_path = os.path.join(p, trade_log_filename)
    order_log_paths.append(order_log_path)
    trade_log_paths.append(trade_log_path)

# print(order_log_paths)
# print(trade_log_paths)


from sqlalchemy import create_engine, event, text
from sqlalchemy import Column, Index, Boolean, Integer, Float, String, DateTime
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.pool import Pool

from dateutil.parser import parse
from dateutil.relativedelta import relativedelta

engine = create_engine(
    'sqlite:///order_log.db',
    isolation_level='SERIALIZABLE',
)

Session = sessionmaker(bind=engine)


class _Base:
    def to_dict(model_instance, query_instance=None):
        if hasattr(model_instance, '__table__'):
            return {
                c.name: getattr(model_instance, c.name)
                for c in model_instance.__table__.columns
            }
        else:
            cols = query_instance.column_descriptions
            
            return {
                cols[i]['name']: model_instance[i]
                for i in range(len(cols))
            }


    @classmethod
    def from_dict(cls, dict, model_instance):
        for c in model_instance.__table__.columns:
            setattr(model_instance, c.name, dict[c.name])


Base = declarative_base(cls=_Base)


class OrderLog(Base):
    __tablename__ = 'order_log'

    '''
    NO,SECCODE,BUYSELL,TIME,ORDERNO,ACTION,PRICE,VOLUME,TRADENO,TRADEPRICE
    1,PLSM,S,100000000,1,1,0.32,36000,,
    '''
    id = Column(Integer, primary_key=True)
    no = Column(Integer, index=True)
    seccode = Column(String, index=True)
    buysell = Column(String)
    time = Column(Integer, index=True)
    orderno = Column(Integer, index=True)
    action = Column(Integer)
    price = Column(Float)
    volume = Column(Float)
    tradeno = Column(Integer)
    tradeprice = Column(Float)


class TradeLog(Base):
    __tablename__ = 'trade_log'

    '''
    TRADENO,SECCODE,TIME,BUYORDERNO,SELLORDERNO,PRICE,VOLUME
    2516556767,SBER,100000,7592,9361,74.38,200
    '''
    id = Column(Integer, primary_key=True)
    tradeno = Column(Integer, index=True)
    seccode = Column(String, index=True)
    time = Column(Integer, index=True)
    buyorderno = Column(Integer)
    sellorderno = Column(Integer)
    price = Column(Float)
    volume = Column(Float)


class Security(Base):
    __tablename__ = 'securities'

    '''
    RN,SUPERTYPE,TRADE_CODE
    1,Акции,CBOM
    '''
    id = Column(Integer, primary_key=True)
    no = Column(Integer, index=True)
    seccode = Column(String, index=True)
    supertype = Column(String)


Base.metadata.create_all(engine)

# 2.a: Запрашивает в базе данных таблицу-классификатор торгуемых ценных бумаг.
sql_query = 'select * from securities'
securities = list(engine.execute(text(sql_query)))
print(securities)

# 2.b: Предлагает пользователю ввести тикер и момент времени в течение торговой сессий [можно узнать на сайте Московской биржи или определить из данных orderlog’а].
print('enter seccode:')
seccode = input()
print('seccode:', seccode)

print('enter timestamp:')
dt = input()
dt = parse(dt)
timestamp = dt.timestamp()
print('dt:', dt)
print('timestamp:', timestamp)

sql_query = f'select * from trade_log where seccode="{seccode}" order by ABS(time - {timestamp}) limit 1'
trade_logs = list(engine.execute(text(sql_query)))
print(trade_logs)

# 2.c: По введенным данным запрашивает из соответствующей таблице БД данные о потоке заявок по указанному тикеру до указанного момента времени.
sql_query = f'select sum(volume) from trade_log where seccode="{seccode}" and time <= {timestamp}'
trade_logs = list(engine.execute(text(sql_query)))
print(trade_logs)

# 2.d: По полученным данным построит «стакан» и визуализирует его в виде таблицы и графика; на графике в явном виде отмечает такие аспекты ликвидности как сжатость (бид-аск спрэд) и глубину (объем на лучших ценах покупки и продажи).
sql_query = f'select price, sum(volume) from order_log where seccode="{seccode}" and buysell = "B" and time <= {timestamp} and price > 0 group by price'
buy_order_logs = list(engine.execute(text(sql_query)))
print('buy_order_logs:', buy_order_logs)

sql_query = f'select price, sum(volume) from order_log where seccode="{seccode}" and buysell = "S" and time <= {timestamp} and price > 0 group by price'
sell_order_logs = list(engine.execute(text(sql_query)))
print('sell_order_logs:', sell_order_logs)

best_5_buy_order_logs = sorted(buy_order_logs, key=lambda n: n[0])[:5]
best_5_sell_order_logs = sorted(sell_order_logs, key=lambda n: -n[0])[:5]
print('best_5_buy_order_logs:', best_5_buy_order_logs)
print('best_5_sell_order_logs:', best_5_sell_order_logs)

min_buy = min(best_5_buy_order_logs)[0]
min_sell = min(best_5_sell_order_logs)[0]
spread = min_sell - min_buy
avg_price = (min_sell + min_buy) / 2.0
print('spread:', spread, '(', min_sell, '-', min_buy, ')')
print('avg_price:', avg_price)

import numpy as np
import matplotlib.pyplot as plt

fig, ax = plt.subplots()
ax.plot([n[0] for n in best_5_buy_order_logs], [n[1] for n in best_5_buy_order_logs], 'k--', label='Bid')
ax.plot([n[0] for n in best_5_sell_order_logs], [n[1] for n in best_5_sell_order_logs], 'k:', label='Ask')
legend = ax.legend(loc='upper center', shadow=True, fontsize='x-large')
plt.show()

# 2.e: Выявляет заявки типа «айзберг» и сохраняет информацию их номерах, выявленных в них скрытых объемах и времени обнаружения в виде таблицы.
sql_query = f'select volume, seccode, count(*) as c from trade_log where seccode="{seccode}" and time <= {timestamp} group by volume, seccode having count(*) > 1 order by c desc limit 100'
iceberg_trade_logs = list(engine.execute(text(sql_query)))
print('iceberg_trade_logs:', iceberg_trade_logs)

# 2.d: (Бонус) отправить информацию о скрытых объемах обратно в базу данных.
sql_query = f'create view if not exists iceberg_trade_logs as select volume, seccode, count(*) as c from trade_log group by volume, seccode having count(*) > 1 order by c desc'
engine.execute(text(sql_query))

sql_query = f'select * from iceberg_trade_logs where seccode="{seccode}" limit 100'
iceberg_trade_logs = list(engine.execute(text(sql_query)))
print('iceberg_trade_logs:', iceberg_trade_logs)
