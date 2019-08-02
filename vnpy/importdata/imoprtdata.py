#!/usr/bin/env python
# encoding: UTF-8

from time import time
import pymongo
import csv
from datetime import datetime, timedelta
from vnpy.trader.vtObject import VtBarData


"""
import规则：
修改filename,指向本地csv
修改symol 例如 btc eos
其中dbName = VnTrader_1Min_Db 是数据库的名称，如果想要修改也是可以的
在loadTBCsv中,如果导入的是现货数据,

在下面的bar.vtSymbol中 如果是现货修改为OKEX 如果是交割OKEXF  如果是永续修改为OKEXS
其余格式均不用动
由于数据库更新时采取的是 "更新的办法" 因此如果有重复数据,没关系
"""


def example1():
    fileName = "C:\\OKEX数据\\okex\\1m\\more\\Kline_Q_1m_201805_201905_btc_usd.csv"
    dbName = 'VnTrader_1Min_Db'
    symbol = "btc"
    loadTbCsv(fileName, dbName, symbol)


def loadTbCsv(fileName, dbName, symbol):
    """将TradeBlazer导出的csv格式的历史分钟数据插入到Mongo数据库中"""
    start = time()
    print(u'开始读取CSV文件%s中的数据插入到%s的%s中' % (fileName, dbName, symbol))

    # 锁定集合，并创建索引
    client = pymongo.MongoClient("localhost", 27017)
    collection = client[dbName][symbol]
    collection.create_index([('datetime', pymongo.ASCENDING)], unique=True)

    # 读取数据和插入到数据库
    # reader = csv.reader(file(fileName, 'r'))
    reader = csv.reader(open(fileName, 'r'))
    header = True
    for d in reader:
        if header:
            header = False
            continue
        bar = VtBarData()
        bar.vtSymbol = symbol + ".OKEXF"
        bar.symbol = symbol

        bar.exchange = "OKEXF"
        bar.gatewayName = "OKEXF"

        bar.datetime = datetime.strptime(d[0], "%Y-%m-%d %H:%M:%S")

        bar.datetime_start = bar.datetime
        bar.datetime_end = bar.datetime + timedelta(minutes=1)
        bar.tradedate = bar.datetime.strftime("%Y-%m-%d")
        # timestart, timeend, tradeday = timeStartEnd(bar.datetime, "rb", INTERVAL_1M)
        # bar.tradedate = tradeday.strftime("%Y-%m-%d")

        bar.time = bar.datetime.strftime("%H:%M:%S")

        bar.open = float(d[1])
        bar.high = float(d[2])
        bar.low = float(d[3])
        bar.close = float(d[4])

        bar.volume = float(d[5])
        bar.openInterest = 0

        # bar.time = datetime.strftime((d[0].split(' ')[1] + ":00"), "%H:%M:%S")

        bar.interval = "min_01"

        flt = {'datetime': bar.datetime}
        collection.update_one(flt, {'$set': bar.__dict__}, upsert=True)
        print(bar.datetime)

    print(u'插入完毕，耗时：%s' % (time() - start))


if __name__ == "__main__":
    example1()