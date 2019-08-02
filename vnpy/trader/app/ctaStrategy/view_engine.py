#!/usr/bin/env python
# encoding: UTF-8

from pyecharts.charts import Kline, Bar, Line, Page
from pyecharts import options as opts
from pyecharts.globals import ThemeType
from pyecharts.components import Table
from pyecharts.options import ComponentTitleOpts
import datetime
from copy import copy, deepcopy
from vnpy.trader.vtConstant import *
import pandas as pd


def view_kline(time, data, title=None):
    """K线图"""
    k = Kline(init_opts=opts.InitOpts(width="1800px", height="900px", theme="dark"))
    # 添加x轴数据
    k.add_xaxis(time)
    # 添加图例和y轴数据,颜色 红涨绿跌
    k.add_yaxis("kline", data, itemstyle_opts=opts.ItemStyleOpts(
        color="#db6969",
        color0="#02691e",
        border_color="#8A0000",
        border_color0="#008F28", ))
    # 是否是脱离0值比例,双数值轴的散点图中比较有用

    k.set_global_opts(xaxis_opts=opts.AxisOpts(is_scale=True))
    k.set_global_opts(yaxis_opts=opts.AxisOpts(is_scale=True))
    if title:
        k.set_global_opts(title_opts=opts.TitleOpts(title=title))
    # 添加滑动条,可左右滑动
    k.set_global_opts(datazoom_opts=opts.DataZoomOpts(type_="inside"))
    # 十字星
    k.set_global_opts(tooltip_opts=opts.TooltipOpts(axis_pointer_type="cross"))
    return k


def view_line(x, y, legend):
    l = Line(init_opts=opts.InitOpts(width="1800px", height="900px"))
    l.add_xaxis(x)
    l.add_yaxis(legend, y)
    # linestype 表明这个是什么类型的线,颜色是什么样的
    l.add_yaxis(legend, y,
                symbol="triangle",
                symbol_size=30,
                is_connect_nones=False,
                # 这里是区域设置
                areastyle_opts=opts.AreaStyleOpts(color="#eda8a8"),
                # 这里是线的属性设置
                linestyle_opts=opts.LineStyleOpts(color="black", width=4),
                # 这里是每个点的属性
                itemstyle_opts=opts.ItemStyleOpts(color="yellow", opacity=0.6))

    return l


class ViewEngine:
    """回测结果展示功能在这里"""
    def __init__(self, engine):
        self.engine = engine
        # 逐日结果
        self.result_daily = None
        # 逐日结果的统计
        self.d = None
        # 逐日结果的列表
        self.resultList = None
        # 逐笔结果的列表
        self.result_trade = None
        self.width = "1800px"
        self.height = "900px"
        self.init()

    def init(self):
        """初始化，计算回测结果"""
        self.engine.calculateDailyResult()
        self.result_daily, self.d, self.resultList = self.engine.calculateDailyStatistics()
        self.result_trade = self.engine.calculateBacktestingResult()

    def kline_data_get(self, interval):
        """提取K线数据,这里提取的是list对象"""
        am = self.data_get(interval)

        time = am.time
        self.time = time
        open = am.openArray
        high = am.highArray
        low = am.lowArray
        close = am.closeArray
        vol = am.volumeArray

        data = []
        for i in range(len(open)):
            data.append([open[i], close[i], low[i], high[i]])
        return time, data, vol

    def kline_add(self, time, data, titlename):
        self.kline = Kline(init_opts=opts.InitOpts(width=self.width, height=self.height, theme=ThemeType.PURPLE_PASSION))
        k = self.kline
        # 是否是脱离0值比例,双数值轴的散点图中比较有用
        k.set_global_opts(xaxis_opts=opts.AxisOpts(is_scale=True))
        k.set_global_opts(yaxis_opts=opts.AxisOpts(is_scale=True))
        k.set_global_opts(title_opts=opts.TitleOpts(title=titlename))
        # 添加滑动条,可左右滑动
        k.set_global_opts(datazoom_opts=opts.DataZoomOpts(type_="inside"))
        # 添加x轴数据
        k.add_xaxis(time)
        # 添加图例和y轴数据
        k.add_yaxis("kline", data, itemstyle_opts=opts.ItemStyleOpts(
            color="#ec0000",
            color0="#00da3c",
            border_color="#8A0000",
            border_color0="#008F28", ))

        # 十字星
        # k.set_global_opts(tooltip_opts=opts.TooltipOpts(axis_pointer_type="cross"))
        return k

    def buy_sell_signal_generate(self, resultList):
        """输入所有交易，产生买卖点图形信号"""
        tradelist = []
        for resu in resultList:

            entryDt = resu.entryDt.strftime("%Y-%m-%d %H:%M:%S")
            exitDt = resu.exitDt.strftime("%Y-%m-%d %H:%M:%S")
            # 进场时间与K线时间对齐
            if entryDt in self.time:
                pass
            else:
                first = True
                for ti in self.time:
                    if first:
                        # 修改进场时间,先让entryDt=ti
                        entryDt = ti
                        # 如果进场时间初次大于等于
                        if datetime.datetime.strptime(ti, "%Y-%m-%d %H:%M:%S") >= resu.entryDt:
                            # 如果ti已经大于entryDt了，就不再更新entryDt了
                            first = False
            # 出场时间与K线时间对齐
            if exitDt in self.time:
                pass
            else:
                first = True
                for ti in self.time:
                    if first:
                        # 修改出场时间,先让exitDt=ti
                        exitDt = ti
                        # 如果出场时间初次大于等于
                        if datetime.datetime.strptime(ti, "%Y-%m-%d %H:%M:%S") > resu.exitDt:
                            # 如果ti已经大于exitDt了，就不再更新exitDt了
                            first = False

            entryPrice = resu.entryPrice
            entryvolume = abs(resu.volume)

            exitPrice = resu.exitPrice
            if resu.volume < 0:
                direction = -1
            else:
                direction = 1
            if resu.pnl > 0:
                pnl = 1
            else:
                pnl = -1
            tradelist.append([entryDt, entryPrice, entryvolume, exitDt, exitPrice, entryvolume, direction, pnl])
        return tradelist

    def buy_sell_line(self, tradelist):
        l = Line(init_opts=opts.InitOpts(width=self.width, height=self.height))
        l.set_global_opts(datazoom_opts=opts.DataZoomOpts(type_="inside"))
        x = self.time
        y = [None for i in range(len(x))]
        l.add_xaxis(x)
        l1 = Line(init_opts=opts.InitOpts(width=self.width, height=self.height))
        l2 = Line(init_opts=opts.InitOpts(width=self.width, height=self.height))
        l3 = Line(init_opts=opts.InitOpts(width=self.width, height=self.height))
        l4 = Line(init_opts=opts.InitOpts(width=self.width, height=self.height))
        l1.set_global_opts(datazoom_opts=opts.DataZoomOpts(type_="inside"))
        l2.set_global_opts(datazoom_opts=opts.DataZoomOpts(type_="inside"))
        l3.set_global_opts(datazoom_opts=opts.DataZoomOpts(type_="inside"))
        l4.set_global_opts(datazoom_opts=opts.DataZoomOpts(type_="inside"))
        for d in tradelist:
            # 多头
            if d[6] == 1:
                # 多头盈利
                if d[7] > 0:
                    l1.add_xaxis([d[0], d[3]])
                    l1.add_yaxis("long_win", [d[1], d[4]],
                                 is_connect_nones=True,
                                 linestyle_opts=opts.LineStyleOpts(color="#FFD700", width=5),
                                 itemstyle_opts=opts.ItemStyleOpts(color="#FFD700", opacity=0.6))
                # 多头亏损
                else:
                    l2.add_xaxis([d[0], d[3]])
                    l2.add_yaxis("long_loss", [d[1], d[4]],
                                 is_connect_nones=True,
                                 linestyle_opts=opts.LineStyleOpts(color="#0088F0", width=3),
                                 itemstyle_opts=opts.ItemStyleOpts(color="#0088F0", opacity=0.6))
            # 空头
            elif d[6] == -1:
                # 空头盈利
                if d[7] > 0:
                    l3.add_xaxis([d[0], d[3]])
                    l3.add_yaxis("short_win", [d[1], d[4]],
                                 is_connect_nones=True,
                                 linestyle_opts=opts.LineStyleOpts(color="#80008f", width=5),
                                 itemstyle_opts=opts.ItemStyleOpts(color="#80008f", opacity=0.6))
                # 空头亏损
                else:
                    l4.add_xaxis([d[0], d[3]])
                    l4.add_yaxis("short_loss", [d[1], d[4]],
                                 is_connect_nones=True,
                                 linestyle_opts=opts.LineStyleOpts(color="#00FA9A", width=3),
                                 itemstyle_opts=opts.ItemStyleOpts(color="#00FA9A", opacity=0.6))
        l.overlap(l1)
        l.overlap(l2)
        l.overlap(l3)
        l.overlap(l4)
        return l

    def data_get(self, interval):
        """输入周期,提取数据"""
        if interval == INTERVAL_1M:
            am = copy(self.engine.strategy.am_min01)
        elif interval == INTERVAL_3M:
            am = copy(self.engine.strategy.am_min03)
        elif interval == INTERVAL_5M:
            am = copy(self.engine.strategy.am_min05)
        elif interval == INTERVAL_15M:
            am = copy(self.engine.strategy.am_min15)
        elif interval == INTERVAL_30M:
            am = copy(self.engine.strategy.am_min30)
        elif interval == INTERVAL_60M:
            am = copy(self.engine.strategy.am_hour1)
        elif interval == INTERVAL_120M:
            am = copy(self.engine.strategy.am_hour2)
        elif interval == INTERVAL_180M:
            am = copy(self.engine.strategy.am_hour3)
        elif interval == INTERVAL_240M:
            am = copy(self.engine.strategy.am_hour4)
        elif interval == INTERVAL_1D:
            am = copy(self.engine.strategy.am_day1)
        else:
            am = None
            print("时间周期输入错误，请重新输入")
        return am

    def index_line_add(self, name, x_data, y_data):
        """指标线的可视化 输入为横轴，名称 数据  """
        l = Line(init_opts=opts.InitOpts(width=self.width, height=self.height))
        l.set_global_opts(datazoom_opts=opts.DataZoomOpts(type_="inside"))
        l.add_xaxis(x_data)
        # linestype 表明这个是什么类型的线,颜色是什么样的
        l.add_yaxis(name, y_data,
                    # 这里是线的属性设置
                    is_symbol_show=False,
                    linestyle_opts=opts.LineStyleOpts(width=3))
        return l

############################################
    def getByTradeResult(self):
        # 存储逐笔盈亏的结果到本地
        d = self.result_trade

        rd = {}
        # 逐笔交易结果
        rd['第一笔交易'] = d['timeList'][0]
        rd['最后一笔交易'] = d['timeList'][-1]

        rd['总交易次数'] = formatNumber(d['totalResult'])
        rd['总盈亏'] = formatNumber(d['capital'])
        rd['最大回撤'] = formatNumber(min(d['drawdownList']))

        rd['平均每笔盈利'] = formatNumber(d['capital'] / d['totalResult'])
        rd['平均每笔滑点'] = formatNumber(d['totalSlippage'] / d['totalResult'])
        rd['平均每笔佣金'] = formatNumber(d['totalCommission'] / d['totalResult'])

        rd['胜率'] = formatNumber(d['winningRate'])
        rd['盈利交易平均值'] = formatNumber(d['averageWinning'])
        rd['亏损交易平均值'] = formatNumber(d['averageLosing'])
        rd['盈亏比'] = formatNumber(d['profitLossRatio'])
        rd = pd.Series(rd)
        # 逐笔交易详细信息

        resultList = d['resultList']
        rl_list = []
        for i in range(len(resultList)):
            rl = []
            rl.append(d['timeList'][i].strftime("%Y-%m-%d %H:%M:%S"))
            rl.append(resultList[i].entryDt.strftime("%Y-%m-%d %H:%M:%S"))
            rl.append(formatNumber(resultList[i].entryPrice))
            rl.append(resultList[i].exitDt.strftime("%Y-%m-%d %H:%M:%S"))
            rl.append(formatNumber(resultList[i].exitPrice))
            rl.append(resultList[i].volume)
            rl.append(formatNumber(resultList[i].turnover))
            rl.append(formatNumber(resultList[i].commission))
            rl.append(formatNumber(resultList[i].slippage))
            rl.append(formatNumber(resultList[i].pnl))
            rl.append(formatNumber(d['capitalList'][i]))
            rl.append(formatNumber(d['drawdownList'][i]))
            rl_list.append(rl)
        columns = ['时间', '开仓时间', '开仓价格', '平仓时间', '平仓价格', '交易数量',
                   '成交金额', '手续费成本', '滑点成本', '净盈亏', '累计盈亏', '回撤']
        rl_list = pd.DataFrame(rl_list, columns=columns)
        return rd, rl_list

    # 逐笔交易展示与图表
    def capital_line(self, balance):
        l = Line(init_opts=opts.InitOpts(width=self.width, height="300px"))
        x = [i for i in range(len(balance))]
        l.add_xaxis(x)
        l.add_yaxis("balance", balance,
                    is_symbol_show=False,
                    linestyle_opts=opts.LineStyleOpts(color="red", width=4),
                    )
        l.set_global_opts(title_opts=opts.TitleOpts(title="逐笔结果展示"))
        return l

    def drawdown_line(self, drawdown):
        l = Line(init_opts=opts.InitOpts(width=self.width, height="300px"))
        x = [i for i in range(len(drawdown))]
        l.add_xaxis(x)
        l.add_yaxis("drawdown", drawdown,
                    is_symbol_show=False,
                    linestyle_opts=opts.LineStyleOpts(color="green", width=2),
                    areastyle_opts=opts.AreaStyleOpts(opacity=0.7, color="green"),
                    )
        return l

    def pnl_bar(self, pnl):
        b = Bar(init_opts=opts.InitOpts(width=self.width, height="300px"))
        x = [i for i in range(len(pnl))]
        b.add_xaxis(x)
        b.add_yaxis("pnl", pnl, label_opts=opts.LabelOpts(is_show=False),)
        return b

    def pos_bar(self, pos):
        b = Bar(init_opts=opts.InitOpts(width=self.width, height="300px"))
        x = [i for i in range(len(pos))]
        b.add_xaxis(pos)
        b.add_yaxis("pos", pos, label_opts=opts.LabelOpts(is_show=False),)
        return b

    def trade_result_table(self):

        table = Table()

        headers = ["NAME", "VALUE"]
        d = self.result_trade
        rows = [
            ["第一笔交易时间", d['timeList'][0].strftime("%Y-%m-%d %H:%M:%S")],
            ["最后一笔交易时间", d['timeList'][-1].strftime("%Y-%m-%d %H:%M:%S")],
            ["总交易次数", d['totalResult']],
            ["总盈亏", d['capital']],
            ["最大回撤", formatNumber(d['maxDrawdown'])],
            ["平均每笔盈利", formatNumber(d['capital']/d['totalResult'])],
            ["平均每笔滑点", formatNumber(d['totalSlippage']/d['totalResult'])],
            ["平均每笔佣金", formatNumber(d['totalCommission']/d['totalResult'])],
            ["胜率", formatNumber(d['winningRate'])],
            ["盈利交易平均值", formatNumber(d['averageWinning'])],
            ["亏损交易平均值", formatNumber(d['averageLosing'])],
            ["盈亏比", formatNumber(d['profitLossRatio'])],
        ]
        table.add(headers, rows).set_global_opts(
            title_opts=ComponentTitleOpts(title="逐笔交易统计结果")
        )
        return table

###########################################

    def getByDayResult(self, d=None, result=None, resultList=None):
        """存储逐日计算的结果"""

        d = self.result_daily
        result = self.d
        resultList = self.resultList

        # 存储逐日盈亏的结果到本地

        rd = {}
        # 逐日交易结果
        rd['首个交易日'] = result['startDate']
        rd['最后交易日'] = result['endDate']

        rd['总交易日'] = result['totalDays']
        rd['盈利交易日'] = result['profitDays']
        rd['亏损交易日'] = result['lossDays']

        rd['起始资金'] = self.engine.capital
        rd['结束资金'] = formatNumber(result['endBalance'])

        rd['总收益率'] = formatNumber(result['totalReturn'])
        rd['年化收益'] = formatNumber(result['annualizedReturn'])
        rd['总盈亏'] = formatNumber(result['totalNetPnl'])
        rd['最大回撤'] = formatNumber(result['maxDrawdown'])
        rd['百分比最大回撤'] = formatNumber(result['maxDdPercent'])

        rd['总手续费'] = formatNumber(result['totalCommission'])
        rd['总滑点'] = formatNumber(result['totalSlippage'])
        rd['总成交金额'] = formatNumber(result['totalTurnover'])
        rd['总成交笔数'] = formatNumber(result['totalTradeCount'])

        rd['日均盈亏'] = formatNumber(result['dailyNetPnl'])
        rd['日均手续费'] = formatNumber(result['dailyCommission'])
        rd['日均滑点'] = formatNumber(result['dailySlippage'])
        rd['日均成交金额'] = formatNumber(result['dailyTurnover'])
        rd['日均成交笔数'] = formatNumber(result['dailyTradeCount'])

        rd['日均收益率'] = formatNumber(result['dailyReturn'])
        rd['收益标准差'] = formatNumber(result['returnStd'])
        rd['Sharpe Ratio'] = formatNumber(result['sharpeRatio'])

        rd = pd.Series(rd)
        # 逐日交易详细信息

        rl_list = []
        for i in range(len(resultList)):
            rl = []
            rl.append(resultList[i].date)
            rl.append(formatNumber(resultList[i].previousClose))
            rl.append(formatNumber(resultList[i].closePrice))
            rl.append(formatNumber(resultList[i].tradeCount))
            rl.append(formatNumber(resultList[i].openPosition))
            rl.append(formatNumber(resultList[i].closePosition))
            rl.append(formatNumber(resultList[i].tradingPnl))
            rl.append(formatNumber(resultList[i].positionPnl))
            rl.append(formatNumber(resultList[i].totalPnl))
            rl.append(formatNumber(resultList[i].turnover))
            rl.append(formatNumber(resultList[i].commission))
            rl.append(formatNumber(resultList[i].slippage))
            rl.append(formatNumber(resultList[i].netPnl))

            rl.append(formatNumber(d['balance'][i]))
            rl.append(formatNumber(d['return'][i]))
            rl.append(formatNumber(d['highLevel'][i]))
            rl.append(formatNumber(d['drawdown'][i]))
            rl.append(formatNumber(d['ddPercent'][i]))
            rl_list.append(rl)

        columns = ['交易日期', '昨收', '今收', '成交数量',
                   '开盘持仓', '收盘持仓', '交易盈亏', '持仓盈亏', '总盈亏', '成交量',
                   '手续费', '滑点', '净盈亏', '账户资金',
                   '收益率', '资金最高', '最大回撤', '最大回撤百分比']
        rl_list = pd.DataFrame(rl_list, columns=columns)

        return rd, rl_list

        # 逐笔交易展示与图表

    def daily_capital_line(self, date, balance):
        l = Line(init_opts=opts.InitOpts(width=self.width, height="300px"))
        l.add_xaxis(date)
        l.add_yaxis("daily_balance", balance,
                    is_symbol_show=False,
                    linestyle_opts=opts.LineStyleOpts(color="red", width=4),
                    )
        l.set_global_opts(title_opts=opts.TitleOpts(title="逐日结果展示"))
        return l

    def daily_drawdown_line(self, date, drawdown):
        l = Line(init_opts=opts.InitOpts(width=self.width, height="300px"))
        l.add_xaxis(date)
        l.add_yaxis("daily_drawdown", drawdown,
                    is_symbol_show=False,
                    linestyle_opts=opts.LineStyleOpts(color="green", width=2),
                    areastyle_opts=opts.AreaStyleOpts(opacity=0.7, color="green"),
                    )
        return l

    def daily_ddpercent_line(self, date, drawdown):
        l = Line(init_opts=opts.InitOpts(width=self.width, height="300px"))
        l.add_xaxis(date)
        l.add_yaxis("daily_ddpercent", drawdown,
                    is_symbol_show=False,
                    linestyle_opts=opts.LineStyleOpts(color="green", width=2),
                    areastyle_opts=opts.AreaStyleOpts(opacity=0.7, color="green"),
                    )
        return l

    def daily_pnl_bar(self, date, pnl):
        b = Bar(init_opts=opts.InitOpts(width=self.width, height="300px"))
        b.add_xaxis(date)
        b.add_yaxis("daily_pnl", pnl, label_opts=opts.LabelOpts(is_show=False),)
        return b

    def base_line(self):
        """基准线与收益率线"""
        date = self.result_daily["date"]
        close_list = self.get_base_close()
        balance_list = self.get_balance_close()
        l = Line(init_opts=opts.InitOpts(width=self.width, height="300px"))
        l.add_xaxis(date)
        l.add_yaxis("close", close_list,
                    is_symbol_show=False,
                    linestyle_opts=opts.LineStyleOpts(color="#B22222", width=4),
                    )
        l.add_yaxis("account", balance_list,
                    is_symbol_show=False,
                    linestyle_opts=opts.LineStyleOpts(color="#FF6347", width=4),
                    )
        return l

    def get_base_close(self):
        base = self.resultList[0].closePrice
        close_list = []
        for result in self.resultList:
            close_list.append(round(result.closePrice/base, 4))
        return close_list

    def get_balance_close(self):
        base = self.result_daily["balance"][0]
        balance_list = []
        for balance in self.result_daily["balance"]:
            balance_list.append(round(balance/base, 4))
        return balance_list

###################################################################
    def saveResult(self, savePath="test.xlsx"):

        print('逐日逐笔结果开始保存！')
        writer = pd.ExcelWriter(savePath)

        # 逐笔结果保存
        rd, rl_list = self.getByTradeResult()

        rd.to_excel(excel_writer=writer, sheet_name='逐笔结果')
        rl_list.to_excel(excel_writer=writer, sheet_name='逐笔详细')


        # 逐日结果
        rd, rl_list = self.getByDayResult()

        rd.to_excel(excel_writer=writer, sheet_name='逐日结果')
        rl_list.to_excel(excel_writer=writer, sheet_name='逐日详细')

        writer.save()
        writer.close()

        print('逐日逐笔结果保存完毕！')


#----------------------------------------------------------------------
def formatNumber(n=2):
    """格式化数字到字符串"""
    rn = float("{0}".format(n))
    # rn = round(n, 2)        # 保留两位小数
    return rn



