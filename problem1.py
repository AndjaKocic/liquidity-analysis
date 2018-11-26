'''
import sqlite3
import pyodbc

db_path = 'order_log.db'
con_str = f"DRIVER={{SQLite3 ODBC Driver}};SERVER=localhost;DATABASE={db_path};Trusted_connection=yes"
db = pyodbc.connect(con_str)
'''

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


from sqlalchemy import create_engine, event
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
session = Session()

print('insert order_log')

for p in order_log_paths[:1]:
    dt = p[-12:-4]
    dt = parse(dt)

    with open(p) as csvfile:
        reader = csv.DictReader(csvfile)
        order_logs = []

        for row in reader:
            order_log = OrderLog(
                no = row['NO'],
                seccode = row['SECCODE'],
                buysell = row['BUYSELL'],
                time = (dt + timedelta(milliseconds=int(row['TIME']) - 100000000)).timestamp(),
                orderno = row['ORDERNO'],
                action = row['ACTION'],
                price = row['PRICE'],
                volume = row['VOLUME'],
                tradeno = row['TRADENO'],
                tradeprice = row['TRADEPRICE'] or 0,
            )

            order_logs.append(order_log)

            if len(order_logs) > 100_000:
                break
        
        session.add_all(order_logs)
        session.flush()

print('insert trade_log')

for p in trade_log_paths[:1]:
    dt = p[-12:-4]
    dt = parse(dt)

    with open(p) as csvfile:
        reader = csv.DictReader(csvfile)
        trade_logs = []

        for row in reader:
            trade_log = TradeLog(
                tradeno = row['TRADENO'],
                seccode = row['SECCODE'],
                time = (dt + timedelta(seconds=int(row['TIME']) - 100000)).timestamp(),
                buyorderno = row['BUYORDERNO'],
                sellorderno = row['SELLORDERNO'],
                price = row['PRICE'],
                volume = row['VOLUME'],
            )

            trade_logs.append(trade_log)

            if len(trade_logs) > 100_000:
                break
        
        session.add_all(trade_logs)
        session.flush()

print('insert securities')

with open(securities_path) as csvfile:
    reader = csv.DictReader(csvfile)
    securities = []

    for row in reader:
        rn = row['RN']
        supertype = row['SUPERTYPE']
        seccode = row['TRADE_CODE']

        security = Security(
            no = rn,
            supertype = supertype,
            seccode = seccode,
        )

        securities.append(security)

    session.add_all(securities)
    session.flush()

session.commit()
session.close()
