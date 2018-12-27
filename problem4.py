# problem 4
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
from problem2 import get_all_securities, get_logs_for_seccode_timestamp
from problem3 import get_price_volume_for_seccode_timestamps, get_price_volume_for_v_operation


def menu(session):
    while True:
        print('')
        print('Select report:')
        print('')
        print('(1) get all securities')
        print('(2) get orders/trades for seccode timestamp')
        print('(3) get price/volume for seccode timestamps')
        print('(4) get price volume for v operation')
        print('(0) quit')
        print('')
        print('Type your choice (1):')
        item = input() or '1'
        print('menu item:', item)

        if item == '1':
            get_all_securities(session, ask_to_save=True)
        elif item == '2':
            get_logs_for_seccode_timestamp(session, ask_to_save=True)
        elif item == '3':
            get_price_volume_for_seccode_timestamps(session, ask_to_save=True)
        elif item == '4':
            get_price_volume_for_v_operation(session, ask_to_save=True)
        elif item == '0':
            break
        else:
            print('invalid menu item')


def main():
    session = Session()
    menu(session)
    session.close()


if __name__ == '__main__':
    main()
