# problem 3
import os
import csv
from datetime import timedelta

from dateutil.parser import parse
from dateutil.relativedelta import relativedelta

import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

from db import engine, text, Session, Security, PrefferedStockOrderLog, OrdinaryStockOrderLog, BondOrderLog, TradeLog


seccode = 'SBER'
time_step = '1'
from_timestamp = '2015-09-01 00:00:00'
to_timestamp = '2015-09-01 00:10:00'
v = '10'
operation = 'buy'


def get_price_volume_for_seccode_timestamps(session):
    # 2.a
    global seccode, time_step, from_timestamp, to_timestamp

    print(f'enter seccode ({seccode}):')
    seccode = input() or seccode
    print('seccode:', seccode)

    print(f'enter time_step ({time_step}) in minutes:')
    time_step = input() or time_step
    time_step = float(time_step) * 60.0
    print('time_step:', time_step)

    print(f'enter from_timestamp ({from_timestamp}):')
    dt = input() or from_timestamp
    dt = parse(dt)
    from_timestamp = dt.timestamp()
    print('from_timestamp:', from_timestamp)

    print(f'enter to_timestamp ({to_timestamp}):')
    dt = input() or to_timestamp
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


def get_price_volume_for_v_operation(session):
    # 2.c
    global v, operation

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


def main():
    session = Session()
    get_price_volume_for_seccode_timestamps(session)
    get_price_volume_for_v_operation(session)
    session.close()


if __name__ == '__main__':
    main()
