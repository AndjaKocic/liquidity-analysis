# problem 1.1, 1.2

import os
import csv
from datetime import timedelta

order_log_paths = []
trade_log_paths = []
securities_path = 'ListingSecurityList.csv'

for n in sorted(os.listdir('OrderLog')):
    p = os.path.join('OrderLog', n)

    if not os.path.isdir(p):
        continue

    ps = sorted(os.listdir(p))
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

import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

engine = create_engine(
    'sqlite:///liquidity-analysis.db',
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


class Security(Base):
    __tablename__ = 'security'

    '''
    SUPERTYPE,INSTRUMENT_TYPE,TRADE_CODE
    Акции,Пай биржевого ПИФа,CBOM
    '''
    id = Column(Integer, primary_key=True)
    seccode = Column(String, index=True)
    instrument_type = Column(String)
    supertype = Column(String)


class PrefferedStockOrderLog(Base):
    __tablename__ = 'preffered_stock_order_log'

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


class OrdinaryStockOrderLog(Base):
    __tablename__ = 'ordinary_stock_order_log'

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


class BondOrderLog(Base):
    __tablename__ = 'bond_order_log'

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


Base.metadata.create_all(engine)
session = Session()

# 2.a
print('enter seccode (SBER):')
seccode = input() or 'SBER'
print('seccode:', seccode)

print('enter time_step (1) in minutes:')
time_step = input() or '1'
time_step = float(time_step) * 60.0
print('time_step:', time_step)

print('enter from_timestamp (2015-09-01 00:00:00):')
dt = input() or '2015-09-01 00:00:00'
dt = parse(dt)
from_timestamp = dt.timestamp()
print('from_timestamp:', from_timestamp)

print('enter to_timestamp (2015-09-01 12:00:00):')
dt = input() or '2015-09-01 00:10:00'
dt = parse(dt)
to_timestamp = dt.timestamp()
print('to_timestamp:', to_timestamp)

# 2.b
current = from_timestamp

while current < to_timestamp:
    b = current
    e = current + time_step

    buy_order_logs = []
    sql_query = f'select price, sum(volume) from preffered_stock_order_log where seccode="{seccode}" and buysell = "B" and time between {b} and {e} and price > 0 group by price order by price desc'
    buy_order_logs += list(engine.execute(text(sql_query)))
    sql_query = f'select price, sum(volume) from ordinary_stock_order_log where seccode="{seccode}" and buysell = "B" and time between {b} and {e} and price > 0 group by price order by price desc'
    buy_order_logs += list(engine.execute(text(sql_query)))
    sql_query = f'select price, sum(volume) from bond_order_log where seccode="{seccode}" and buysell = "B" and time between {b} and {e} and price > 0 group by price order by price desc'
    buy_order_logs += list(engine.execute(text(sql_query)))
    print('buy_order_logs:', buy_order_logs)

    sell_order_logs = []
    sql_query = f'select price, sum(volume) from preffered_stock_order_log where seccode="{seccode}" and buysell = "S" and time between {b} and {e} and price > 0 group by price order by price asc'
    sell_order_logs += list(engine.execute(text(sql_query)))
    sql_query = f'select price, sum(volume) from ordinary_stock_order_log where seccode="{seccode}" and buysell = "S" and time between {b} and {e} and price > 0 group by price order by price asc'
    sell_order_logs += list(engine.execute(text(sql_query)))
    sql_query = f'select price, sum(volume) from bond_order_log where seccode="{seccode}" and buysell = "S" and time between {b} and {e} and price > 0 group by price order by price asc'
    sell_order_logs += list(engine.execute(text(sql_query)))
    print('sell_order_logs:', sell_order_logs)

    if not buy_order_logs:
        current += time_step
        continue

    if not sell_order_logs:
        current += time_step
        continue

    best_5_buy_order_logs = buy_order_logs[:5]
    best_5_sell_order_logs = sell_order_logs[:5]
    print('best_5_buy_order_logs:', best_5_buy_order_logs)
    print('best_5_sell_order_logs:', best_5_sell_order_logs)

    min_buy = max(best_5_buy_order_logs)[0]
    min_sell = min(best_5_sell_order_logs)[0]
    spread = min_buy - min_sell
    avg_price = (min_sell + min_buy) / 2.0
    print('spread:', spread, '(', min_sell, '-', min_buy, ')')
    print('avg_price:', avg_price)

    best_buy_volume = max(best_5_buy_order_logs)[1]
    best_sell_volume = min(best_5_sell_order_logs)[1]
    print('best_buy_volume:', best_buy_volume)
    print('best_sell_volume:', best_sell_volume)

    current += time_step

# 2.c
print('v (10):')
v = input() or '10'
v = int(v)
print('v:', v)

print('operation, buy or sell (buy):')
operation = input() or 'buy'
print('operation:', operation)

current = from_timestamp
buy_tx_cost_items = []
sell_tx_cost_items = []

while current < to_timestamp:
    b = current
    e = current + time_step

    buy_order_logs = []
    sql_query = f'select price, sum(volume) from preffered_stock_order_log where seccode="{seccode}" and buysell = "B" and time between {b} and {e} and price > 0 and volume = {v} group by price order by price desc'
    buy_order_logs += list(engine.execute(text(sql_query)))
    sql_query = f'select price, sum(volume) from ordinary_stock_order_log where seccode="{seccode}" and buysell = "B" and time between {b} and {e} and price > 0 and volume = {v} group by price order by price desc'
    buy_order_logs += list(engine.execute(text(sql_query)))
    sql_query = f'select price, sum(volume) from bond_order_log where seccode="{seccode}" and buysell = "B" and time between {b} and {e} and price > 0 and volume = {v} group by price order by price desc'
    buy_order_logs += list(engine.execute(text(sql_query)))
    print('buy_order_logs:', buy_order_logs)

    sell_order_logs = []
    sql_query = f'select price, sum(volume) from preffered_stock_order_log where seccode="{seccode}" and buysell = "S" and time between {b} and {e} and price > 0 and volume = {v} group by price order by price asc'
    sell_order_logs += list(engine.execute(text(sql_query)))
    sql_query = f'select price, sum(volume) from ordinary_stock_order_log where seccode="{seccode}" and buysell = "S" and time between {b} and {e} and price > 0 and volume = {v} group by price order by price asc'
    sell_order_logs += list(engine.execute(text(sql_query)))
    sql_query = f'select price, sum(volume) from bond_order_log where seccode="{seccode}" and buysell = "S" and time between {b} and {e} and price > 0 and volume = {v} group by price order by price asc'
    sell_order_logs += list(engine.execute(text(sql_query)))
    print('sell_order_logs:', sell_order_logs)

    sql_query = f'select price, sum(volume) from trade_log where seccode="{seccode}" and buyorderno is not null and time between {b} and {e} and price > 0 and volume = {v} group by price'
    buy_trade_logs = list(engine.execute(text(sql_query)))
    print('buy_trade_logs:', buy_trade_logs)

    sql_query = f'select price, sum(volume) from trade_log where seccode="{seccode}" and sellorderno is not null and time between {b} and {e} and price > 0 and volume = {v} group by price'
    sell_trade_logs = list(engine.execute(text(sql_query)))
    print('sell_trade_logs:', sell_trade_logs)

    if not buy_order_logs:
        current += time_step
        continue

    if not sell_order_logs:
        current += time_step
        continue

    if not buy_trade_logs:
        current += time_step
        continue

    if not sell_trade_logs:
        current += time_step
        continue

    best_5_buy_order_logs = buy_order_logs[:5]
    best_5_sell_order_logs = sell_order_logs[:5]
    print('best_5_buy_order_logs:', best_5_buy_order_logs)
    print('best_5_sell_order_logs:', best_5_sell_order_logs)

    min_buy = max(best_5_buy_order_logs)[0]
    min_sell = min(best_5_sell_order_logs)[0]
    spread = min_buy - min_sell
    avg_price = (min_sell + min_buy) / 2.0
    print('spread:', spread, '(', min_sell, '-', min_buy, ')')
    print('avg_price:', avg_price)

    best_buy_volume = max(best_5_buy_order_logs)[1]
    best_sell_volume = min(best_5_sell_order_logs)[1]
    print('best_buy_volume:', best_buy_volume)
    print('best_sell_volume:', best_sell_volume)

    buy_trade_average_price = sum(n[0] for n in buy_trade_logs) / len(buy_trade_logs)
    sell_trade_average_price = sum(n[0] for n in sell_trade_logs) / len(sell_trade_logs)
    print('buy_trade_average_price:', buy_trade_average_price)
    print('sell_trade_average_price:', sell_trade_average_price)

    buy_tx_cost = abs(avg_price - buy_trade_average_price)
    sell_tx_cost = abs(avg_price - sell_trade_average_price)
    buy_tx_volume = sum(n[1] for n in buy_trade_logs)
    sell_tx_volume = sum(n[1] for n in sell_trade_logs)
    print('buy_tx_cost:', buy_tx_cost)
    print('sell_tx_cost:', sell_tx_cost)

    buy_tx_cost_items.append((current, buy_tx_cost, buy_tx_volume))
    sell_tx_cost_items.append((current, sell_tx_cost, sell_tx_volume))

    current += time_step

# 2.d.1
fig, ax = plt.subplots()

if operation == 'buy':
    avg = sum(n[1] for n in buy_tx_cost_items) / len(buy_tx_cost_items)
    median = (
        buy_tx_cost_items[len(buy_tx_cost_items) // 2][1] +
        buy_tx_cost_items[(len(buy_tx_cost_items) // 2) + 1][1]
    ) / 2.0
    ax.plot([n[0] for n in buy_tx_cost_items], [n[1] for n in buy_tx_cost_items], 'k', label='Buy')
    ax.plot([n[0] for n in buy_tx_cost_items], [avg for n in buy_tx_cost_items], 'k', label='Average')
    ax.plot([n[0] for n in buy_tx_cost_items], [median for n in buy_tx_cost_items], 'k--', label='Median')

    x = buy_tx_cost_items[0][0] + (buy_tx_cost_items[-1][0] - buy_tx_cost_items[0][0]) * 0.05
    ax.axvline(x, color='r')
    x = buy_tx_cost_items[0][0] + (buy_tx_cost_items[-1][0] - buy_tx_cost_items[0][0]) * 0.95
    ax.axvline(x, color='b')
elif operation == 'sell':
    avg = sum(n[1] for n in sell_tx_cost_items) / len(sell_tx_cost_items)
    median = (
        sell_tx_cost_items[len(sell_tx_cost_items) // 2][1] +
        sell_tx_cost_items[(len(sell_tx_cost_items) // 2) + 1][1]
    ) / 2.0
    ax.plot([n[0] for n in sell_tx_cost_items], [n[1] for n in sell_tx_cost_items], 'k', label='Sell')
    ax.plot([n[0] for n in sell_tx_cost_items], [avg for n in sell_tx_cost_items], 'k', label='Average')
    ax.plot([n[0] for n in sell_tx_cost_items], [median for n in sell_tx_cost_items], 'k--', label='Median')

    x = sell_tx_cost_items[0][0] + (sell_tx_cost_items[-1][0] - sell_tx_cost_items[0][0]) * 0.05
    ax.axvline(x, color='r')
    x = sell_tx_cost_items[0][0] + (sell_tx_cost_items[-1][0] - sell_tx_cost_items[0][0]) * 0.95
    ax.axvline(x, color='b')

legend = ax.legend(loc='upper center', shadow=True, fontsize='x-large')
plt.savefig('problem3d1.png')

# 2.d.2
fig, ax = plt.subplots()

if operation == 'buy':
    histogram_buy_tx_cost_items = {}

    for t, p, vol in buy_tx_cost_items:
        try:
            histogram_buy_tx_cost_items[p] += vol
        except KeyError as e:
            histogram_buy_tx_cost_items[p] = vol

    histogram_buy_tx_cost_items = sorted(histogram_buy_tx_cost_items.items())
    ax.plot([n[0] for n in histogram_buy_tx_cost_items], [n[1] for n in histogram_buy_tx_cost_items], 'k', label='Buy')

    x = histogram_buy_tx_cost_items[0][0] + (histogram_buy_tx_cost_items[-1][0] - histogram_buy_tx_cost_items[0][0]) * 0.99
    ax.axvline(x, color='r')
elif operation == 'sell':
    histogram_sell_tx_cost_items = {}

    for t, p, vol in sell_tx_cost_items:
        try:
            histogram_sell_tx_cost_items[p] += vol
        except KeyError as e:
            histogram_sell_tx_cost_items[p] = vol

    histogram_sell_tx_cost_items = sorted(histogram_sell_tx_cost_items.items())
    ax.plot([n[0] for n in histogram_sell_tx_cost_items], [n[1] for n in histogram_sell_tx_cost_items], 'k--', label='Sell')

    x = histogram_sell_tx_cost_items[0][0] + (histogram_sell_tx_cost_items[-1][0] - histogram_sell_tx_cost_items[0][0]) * 0.99
    ax.axvline(x, color='r')

legend = ax.legend(loc='upper center', shadow=True, fontsize='x-large')
plt.savefig('problem3d2.png')

session.commit()
session.close()
