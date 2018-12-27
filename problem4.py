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


def main():
    session = Session()
    session.close()


if __name__ == '__main__':
    main()
