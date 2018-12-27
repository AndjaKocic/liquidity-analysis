# problem 1.1, 1.2
import os
import csv
from datetime import timedelta

from dateutil.parser import parse
from dateutil.relativedelta import relativedelta

from db import Session, Security, PrefferedStockOrderLog, OrdinaryStockOrderLog, BondOrderLog, TradeLog


def populate_db(session):
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

    print('insert securities')

    with open(securities_path) as csvfile:
        reader = csv.DictReader(csvfile)
        securities = []

        for row in reader:
            supertype = row['SUPERTYPE']
            instrument_type = row['INSTRUMENT_TYPE']
            seccode = row['TRADE_CODE']

            security = Security(
                supertype = supertype,
                instrument_type = instrument_type,
                seccode = seccode,
            )

            securities.append(security)

        session.add_all(securities)
        session.flush()


    print('insert order_log')

    # for p in order_log_paths:
    for p in order_log_paths[:1]:
        dt = p[-12:-4]
        dt = parse(dt)

        with open(p) as csvfile:
            reader = csv.DictReader(csvfile)
            order_logs = []

            for row in reader:
                seccode = row['SECCODE']

                q = session.query(Security)
                q = q.filter(Security.seccode==seccode)
                security = q.first()

                if not security:
                    continue

                if not security.instrument_type:
                    continue

                if security.supertype == 'Облигации':
                    order_log = BondOrderLog(
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
                elif security.supertype == 'Акции':
                    if security.instrument_type == 'Акция обыкновенная':
                        order_log = OrdinaryStockOrderLog(
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
                    elif security.instrument_type == 'Акция привилегированная':
                        order_log = PrefferedStockOrderLog(
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
                    else:
                        continue
                else:
                    continue

                order_logs.append(order_log)

                if len(order_logs) > 500_000:
                    break
            
            session.add_all(order_logs)
            session.flush()

    print('insert trade_log')

    # for p in trade_log_paths:
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

def main():
    session = Session()
    populate_db(session)
    session.commit()
    session.close()


if __name__ == '__main__':
    main()
