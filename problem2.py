# problem 2
import os
import csv

from dateutil.parser import parse
from dateutil.relativedelta import relativedelta

import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

from db import engine, text, Session, Security, PrefferedStockOrderLog, OrdinaryStockOrderLog, BondOrderLog, TradeLog


securities = None
seccode = 'SBER'
timestamp = '2015-09-01 12:00:00'


def get_all_securities(session, ask_to_save=False):
    # 2.a: Запрашивает в базе данных таблицу-классификатор торгуемых ценных бумаг.
    global securities
    sql_query = 'select * from security'
    securities = list(engine.execute(text(sql_query)))

    if ask_to_save:
        path = 'securities.csv'
        print(f'Enter path where to save securities ({path!r}):')
        path = input() or path

        with open(path, 'w') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(['SUPERTYPE', 'INSTRUMENT_TYPE', 'TRADE_CODE'])
            writer.writerows(securities)


def get_logs_for_seccode_timestamp(session, ask_to_save=False):
    # 2.b: Предлагает пользователю ввести тикер и момент времени в течение торговой сессий [можно узнать на сайте Московской биржи или определить из данных orderlog’а].
    global seccode, timestamp

    print(f'enter seccode ({seccode}):')
    seccode = input() or seccode
    # print('seccode:', seccode)

    print(f'enter timestamp ({timestamp}):')
    timestamp = input() or timestamp
    timestamp = parse(timestamp)
    timestamp = timestamp.timestamp()
    # print('timestamp:', timestamp)

    sql_query = f'select * from trade_log where seccode="{seccode}" order by ABS(time - {timestamp}) limit 1'
    trade_logs = list(engine.execute(text(sql_query)))
    # print(trade_logs)

    # 2.c: По введенным данным запрашивает из соответствующей таблице БД данные о потоке заявок по указанному тикеру до указанного момента времени.
    sql_query = f'select sum(volume) from trade_log where seccode="{seccode}" and time <= {timestamp}'
    trade_logs = list(engine.execute(text(sql_query)))
    # print(trade_logs)

    # 2.d: По полученным данным построит «стакан» и визуализирует его в виде таблицы и графика; на графике в явном виде отмечает такие аспекты ликвидности как сжатость (бид-аск спрэд) и глубину (объем на лучших ценах покупки и продажи).
    buy_order_logs = []
    sql_query = f'select price, sum(volume) from preffered_stock_order_log where seccode="{seccode}" and buysell = "B" and time <= {timestamp} and price > 0 group by price order by price desc'
    buy_order_logs += list(engine.execute(text(sql_query)))
    sql_query = f'select price, sum(volume) from ordinary_stock_order_log where seccode="{seccode}" and buysell = "B" and time <= {timestamp} and price > 0 group by price order by price desc'
    buy_order_logs += list(engine.execute(text(sql_query)))
    sql_query = f'select price, sum(volume) from bond_order_log where seccode="{seccode}" and buysell = "B" and time <= {timestamp} and price > 0 group by price order by price desc'
    buy_order_logs += list(engine.execute(text(sql_query)))
    # print('buy_order_logs:', buy_order_logs)

    sell_order_logs = []
    sql_query = f'select price, sum(volume) from preffered_stock_order_log where seccode="{seccode}" and buysell = "S" and time <= {timestamp} and price > 0 group by price order by price asc'
    sell_order_logs += list(engine.execute(text(sql_query)))
    sql_query = f'select price, sum(volume) from ordinary_stock_order_log where seccode="{seccode}" and buysell = "S" and time <= {timestamp} and price > 0 group by price order by price asc'
    sell_order_logs += list(engine.execute(text(sql_query)))
    sql_query = f'select price, sum(volume) from bond_order_log where seccode="{seccode}" and buysell = "S" and time <= {timestamp} and price > 0 group by price order by price asc'
    sell_order_logs += list(engine.execute(text(sql_query)))
    # print('sell_order_logs:', sell_order_logs)

    best_5_buy_order_logs = buy_order_logs[:5]
    best_5_sell_order_logs = sell_order_logs[:5]
    # print('best_5_buy_order_logs:', best_5_buy_order_logs)
    # print('best_5_sell_order_logs:', best_5_sell_order_logs)

    min_buy = max(best_5_buy_order_logs)[0]
    min_sell = min(best_5_sell_order_logs)[0]
    spread = min_buy - min_sell
    avg_price = (min_sell + min_buy) / 2.0
    print('spread:', spread, '(', min_sell, '-', min_buy, ')')
    print('avg_price:', avg_price)

    fig, ax = plt.subplots()
    ax.plot([n[0] for n in best_5_buy_order_logs], [n[1] for n in best_5_buy_order_logs], 'k--', label='Bid')
    ax.plot([n[0] for n in best_5_sell_order_logs], [n[1] for n in best_5_sell_order_logs], 'k:', label='Ask')
    legend = ax.legend(loc='upper center', shadow=True, fontsize='x-large')

    path = 'problem2d.png'

    if ask_to_save:
        print(f'Enter path where to save market depth ({path!r}):')
        path = input() or path
    
    plt.savefig(path)

    # 2.e: Выявляет заявки типа «айзберг» и сохраняет информацию их номерах, выявленных в них скрытых объемах и времени обнаружения в виде таблицы.
    sql_query = f'select volume, seccode, count(*) as c from trade_log where seccode="{seccode}" and time <= {timestamp} group by volume, seccode having count(*) > 1 order by c desc limit 100'
    iceberg_trade_logs = list(engine.execute(text(sql_query)))
    # print('iceberg_trade_logs:', iceberg_trade_logs)

    if ask_to_save:
        path = 'iceberg_trade_logs.csv'
        print(f'Enter path where to save iceberg trade logs ({path!r}):')
        path = input() or path

        with open(path, 'w') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(['VOLUME', 'SECCODE', 'COUNT'])
            writer.writerows(iceberg_trade_logs)

    # 2.d: (Бонус) отправить информацию о скрытых объемах обратно в базу данных.
    sql_query = f'create view if not exists iceberg_trade_logs as select volume, seccode, count(*) as c from trade_log group by volume, seccode having count(*) > 1 order by c desc'
    engine.execute(text(sql_query))
    sql_query = f'select * from iceberg_trade_logs where seccode="{seccode}" limit 100'
    iceberg_trade_logs = list(engine.execute(text(sql_query)))
    # print('iceberg_trade_logs:', iceberg_trade_logs)


def main():
    session = Session()
    get_all_securities(session)
    get_logs_for_seccode_timestamp(session)
    session.close()


if __name__ == '__main__':
    main()
