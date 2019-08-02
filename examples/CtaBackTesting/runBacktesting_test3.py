#!/usr/bin/env python
# encoding: UTF-8

"""
展示数字货币如何执行策略回测。
"""

from vnpy.trader.app.ctaStrategy.ctaBacktesting_cryptospot import BacktestingEngine, MINUTE_DB_NAME
from vnpy.trader.app.ctaStrategy.strategy.strategyDoubleMa_test import DoubleMaStrategy

if __name__ == '__main__':
    # 创建回测引擎
    engine = BacktestingEngine()

    # 设置引擎的回测模式为K线
    engine.setBacktestingMode(engine.BAR_MODE)

    # 设置回测用的数据起始日期,起始日期十天
    engine.setStartDate('2018-01-03', 10)

    # 设置产品相关参数
    engine.setSlippage(0)           # 测试品种1跳
    # engine.setRate(0.3 / 10000)   # 万0.3
    engine.setRate(3/10000)         # 千分之二
    engine.setPriceTick(0.1)        # 测试最小价格变动
    engine.setSize(1)               # 设置单位大小
    engine.setCapital(100000)         # 本金

    # 设置使用的历史数据库
    engine.setDatabase(MINUTE_DB_NAME, "btc")

    # 在引擎中创建策略对象,可以设置参数和策略名称
    d = {}
    engine.initStrategy(DoubleMaStrategy, d)

    # 开始跑回测
    engine.runBacktesting()
    #############################################################
    # 加入回测结果可视化
    from vnpy.trader.app.ctaStrategy.view_engine import ViewEngine
    import talib
    import numpy
    # 初始化,进行数据的计算与提取
    vnpy_view = ViewEngine(engine)
    # 提取K线数据,
    time, data, vol = vnpy_view.kline_data_get("hour_1")
    # 画K线
    k = vnpy_view.kline_add(time, data, "buy and sell point")
    # 提取买卖点
    resultList = vnpy_view.result_trade['resultList']
    # 时间处理,将成交时间与绘图时间对齐,例如成交时间为03分钟,但是绘图是用小时K线,对齐
    tradelist = vnpy_view.buy_sell_signal_generate(resultList)
    # 生成买卖点的图
    l = vnpy_view.buy_sell_line(tradelist)

    # 提取指标数据
    am = vnpy_view.data_get("hour_1")
    # 对指标数据进行处理
    x_data = am.time
    y_data = am.close

    #smooth = engine.strategy.smooth
    #ma1 = list(am.sma(10, True))
    #ma2 = list(am.sma(40, True))
    #l1 = vnpy_view.index_line_add("ma10", x_data, ma1)
    #l2 = vnpy_view.index_line_add("ma40", x_data, ma2)
    #l2 = vnpy_view.index_line_add("smooth", x_data, smooth)
    # K线叠加买卖点
    k.overlap(l)
    # k.overlap(l1)
    #k.overlap(l2)

    #############################################################
    # 逐笔回测统计
    capitallist = vnpy_view.result_trade["capitalList"]
    drawdownlist = vnpy_view.result_trade["drawdownList"]
    pnllist = vnpy_view.result_trade["pnlList"]
    poslist = vnpy_view.result_trade["posList"]

    l1 = vnpy_view.capital_line(capitallist)
    l2 = vnpy_view.drawdown_line(drawdownlist)
    bar1 = vnpy_view.pnl_bar(pnllist)
    bar2 = vnpy_view.pos_bar(poslist)
    table = vnpy_view.trade_result_table()

    #############################################################
    # 逐日回测统计
    xdata = vnpy_view.result_daily["date"]
    ydata = vnpy_view.result_daily["balance"]
    l_1 = vnpy_view.daily_capital_line(xdata, ydata)
    ydata = vnpy_view.result_daily["drawdown"]
    l_2 = vnpy_view.daily_drawdown_line(xdata, ydata)
    ydata = vnpy_view.result_daily["ddPercent"]
    l_3 = vnpy_view.daily_ddpercent_line(xdata, ydata)
    ydata = vnpy_view.result_daily["netPnl"]
    bar_1 = vnpy_view.daily_pnl_bar(xdata, ydata)
    l_4 = vnpy_view.base_line()

    from pyecharts.charts import Page

    page = Page()
    # 添加买卖点
    page.add(k)
    # 添加逐日图
    page.add(l1)
    page.add(l2)
    page.add(bar1)
    page.add(bar2)
    # 添加逐笔图
    page.add(l_1)
    page.add(l_4)
    page.add(l_2)
    page.add(l_3)
    page.add(bar_1)
    page.render("doublema.html")

    vnpy_view.saveResult("doublema.xlsx")





















