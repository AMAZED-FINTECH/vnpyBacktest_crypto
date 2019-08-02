#!/usr/bin/env python
# encoding: UTF-8

from pyecharts import Kline, Line, Overlap
import datetime
from copy import copy
from vnpy.trader.vtConstant import *
import pandas as pd

class backtest_view(object):
    """作图：K线 指标线 买卖点连起来的线"""
    def __init__(self):

        self.kline = Kline()
        self.kline2 = Kline()
        # 初始化一个指标线
        self.line = Line()
        self.line_long_win = Line()
        self.line_long_loss = Line()
        self.line_short_win = Line()
        self.line_short_loss = Line()

    def kline_add(self, time, data):

        self.kline.add("Kline", time, data,
                       is_datazoom_show=True,
                       is_datazoom_extra_show=True,
                       is_legend_show=True,
                       is_label_show=True,
                       is_more_utils=True,
                       is_xaxislabel_align=True,
                       is_yaxislabel_align=True,
                       tooltip_axispointer_type="cross",
                       # mark_point=["max", "min"]
                       )

        self.kline2.add("Kline2", time, data,
                        is_datazoom_show=True,
                        is_datazoom_extra_show=True,
                        is_legend_show=True,
                        is_label_show=True,
                        is_more_utils=True,
                        is_xaxislabel_align=True,
                        is_yaxislabel_align=True,
                        tooltip_axispointer_type="cross",
                        )
        d = [data[0][1] for i in range(len(time))]
        self.time = time
        self.line_long_win.add("empty", time, d)
        self.line_long_loss.add("empty", time, d)
        self.line_short_win.add("empty", time, d)
        self.line_long_loss.add("empty", time, d)

    def line_add(self, name, x_data, y_data):
        """指标线的可视化 输入为横轴，名称 数据  """
        self.line.add(name, x_data, y_data,
                      is_datazoom_show=True,
                      is_datazoom_extra_show=True,
                      is_legend_show=True,
                      # is_label_show=True,
                      is_more_utils=True,
                      is_xaxislabel_align=True,
                      is_yaxislabel_align=True,
                      # mark_point=["max", "min"]
                      )


    def buy_sell_add(self, trade):
        """输入所有交易，产生买卖点图形信号"""
        tradelist = []
        for resu in trade:

            entryDt = resu.entryDt.strftime("%Y-%m-%d %H:%M:%S")
            exitDt = resu.exitDt.strftime("%Y-%m-%d %H:%M:%S")

            if entryDt in self.time:
                pass
            else:
                first = True
                for ti in self.time:
                    if first:
                        # 先让entryDt=ti
                        entryDt = ti
                        if datetime.datetime.strptime(ti, "%Y-%m-%d %H:%M:%S") > resu.entryDt:
                            # 如果ti已经大于entryDt了，就不再更新entryDt了
                            first = False

            if exitDt in self.time:
                pass
            else:
                first = True
                for ti in self.time:
                    if first:
                        # 先让exitDt=ti
                        exitDt = ti
                        if datetime.datetime.strptime(ti, "%Y-%m-%d %H:%M:%S") > resu.exitDt:
                            # 如果ti已经大于exitDt了，就不再更新exitDt了
                            first = False


            # entryDt = resu.entryDt.strftime("%Y-%m-%d %H:%M:%S")
            # exitDt = resu.exitDt.strftime("%Y-%m-%d %H:%M:%S")
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

        for d in tradelist:
            # 多头
            if d[6] == 1:
                # 多头盈利
                if d[7] > 0:
                    self.line_long_win.add(name="long win", x_axis=[d[0], d[3]], y_axis=[d[1], d[4]],
                                           is_datazoom_show=True,
                                           is_datazoom_extra_show=True,
                                           line_color="#000000",
                                           line_width=5,
                                           is_xaxislabel_align=True,
                                           is_yaxislabel_align=True,
                                           is_axisline_show=True,
                                           )

                # 多头亏损
                else:
                    self.line_long_loss.add(name="long loss", x_axis=[d[0], d[3]], y_axis=[d[1], d[4]],
                                            is_datazoom_show=True,
                                            is_datazoom_extra_show=True,
                                            line_color="#696969",
                                            line_width=3,
                                            is_xaxislabel_align=True,
                                            is_yaxislabel_align=True,
                                            is_axisline_show=True,
                                            )
            # 空头
            elif d[6] == -1:
                # 空头盈利
                if d[7] > 0:
                    self.line_short_win.add(name="short win", x_axis=[d[0], d[3]], y_axis=[d[1], d[4]],
                                            is_datazoom_show=True,
                                            is_datazoom_extra_show=True,
                                            line_color="#008000",
                                            line_width=5,
                                            is_xaxislabel_align=True,
                                            is_yaxislabel_align=True,
                                            is_axisline_show=True,
                                            )

                # 空头亏损
                else:
                    self.line_short_loss.add(name="short loss", x_axis=[d[0], d[3]], y_axis=[d[1], d[4]],
                                             is_datazoom_show=True,
                                             is_datazoom_extra_show=True,
                                             line_color="#7CFC00",
                                             line_width=3,
                                             is_xaxislabel_align=True,
                                             is_yaxislabel_align=True,
                                             is_axisline_show=True,
                                             )


    def render_add(self, path="test.html"):

        overlap = Overlap(width=1600, height=800, )

        overlap.add(self.kline)
        overlap.add(self.line)
        overlap.add(self.line_long_win)
        overlap.add(self.line_long_loss)
        overlap.add(self.line_short_win)
        overlap.add(self.line_short_loss)

        # overlap.add(self.kline2, is_add_yaxis=True, yaxis_index=1)
        overlap.render(path)

class money_view(object):

    def __init__(self):
        self.kline = Kline()
        self.line = Line()
        self.empty_line = Line()

    def kline_add(self, time, data):
        """添加日K线"""

        self.kline.add("Kline", time, data,
                       is_datazoom_show=True,
                       is_datazoom_extra_show=True,
                       is_legend_show=True,
                       is_label_show=True,
                       is_more_utils=True,
                       is_xaxislabel_align=True,
                       is_yaxislabel_align=True,
                       mark_point=["max", "min"],
                       is_xaxis_show=True,
                       is_axisline_show=True,
                       tooltip_axispointer_type="cross",
                       )

    def empty_line_add(self, time):
        data = [1 for i in range(len(time))]
        self.empty_line.add("Empty", time, data,
                            is_datazoom_show=True,
                            is_datazoom_extra_show=True,
                            is_legend_show=True,
                            is_more_utils=True,
                            is_xaxislabel_align=True,
                            is_yaxislabel_align=True,
                            is_axisline_show=True,
                            is_xaxis_show=True
                            )

    def base_line_add(self, x_data, y_data):
        """基准线的可视化 输入为横轴，名称 数据  """
        self.line.add("基准线", x_data, y_data,
                      line_width=4,
                      is_datazoom_show=True,
                      is_datazoom_extra_show=True,
                      is_legend_show=True,
                      is_more_utils=True,
                      is_xaxislabel_align=True,
                      is_yaxislabel_align=True,
                      is_axisline_show=True,
                      line_color="#696969",)


    def money_line_add(self, x_data, y_data):
        """资金曲线的可视化 输入为横轴，名称 数据  """
        self.line.add("资金曲线", x_data, y_data,
                      line_width=5,
                      is_datazoom_show=True,
                      is_datazoom_extra_show=True,
                      is_legend_show=True,
                      is_more_utils=True,
                      is_xaxislabel_align=True,
                      is_yaxislabel_align=True,
                      is_axisline_show=True,
                      line_color="#000000")

    def drawdown_line_add(self, x_data, y_data):
        """资金曲线的可视化 输入为横轴，名称 数据  """
        self.line.add("回撤", x_data, y_data,
                      line_width=3,
                      is_datazoom_show=True,
                      is_datazoom_extra_show=True,
                      is_legend_show=True,
                      is_more_utils=True,
                      line_color="#1e90ff",
                      is_xaxislabel_align=True,
                      is_yaxislabel_align=True,
                      is_axisline_show=True,
                      is_fill=True,
                      area_color="#add8e6",
                      area_opacity=0.3
                      )


    def other_line_add(self, name, x_data, y_data):
        """其他曲线的可视化 输入为名称, 横轴, 数据  """
        self.line.add(name, x_data, y_data,
                      line_width=3,
                      is_datazoom_show=True,
                      is_datazoom_extra_show=True,
                      is_legend_show=True,
                      is_more_utils=True,
                      # line_color="#1e90ff",
                      is_fill=True,
                      is_xaxislabel_align=True,
                      is_yaxislabel_align=True,
                      is_axisline_show=True,
                      area_color="#add8e6",)


    def render_add(self, path="test1.html"):
        overlap = Overlap(width=1600, height=800, )
        # overlap.add(self.empty_line)
        overlap.add(self.kline)
        overlap.add(self.line, is_add_yaxis=True, yaxis_index=1)

        # overlap.add(self.line, yaxis_index=0)
        # overlap.add(self.kline, is_add_yaxis=True, yaxis_index=3)
        overlap.render(path=path)


class vnpy_backtest_view():
    """将VNPY的回测结果可视化展示"""
    def __init__(self, engine):
        self.engine = engine
        self.init()

    def init(self):
        """初始化，计算回测结果"""
        self.engine.calculateDailyResult()
        self.result_daily, self.d, self.resultList = self.engine.calculateDailyStatistics()
        self.result_trade = self.engine.calculateBacktestingResult()

    def data_withdraw(self, interval):
        if interval == INTERVAL_1M:
            am = copy(self.engine.strategy.am_min01)
        elif interval == INTERVAL_5M:
            am = copy(self.engine.strategy.am_min05)
        elif interval == INTERVAL_15M:
            am = copy(self.engine.strategy.am_min15)
        elif interval == INTERVAL_30M:
            am = copy(self.engine.strategy.am_min30)
        elif interval == INTERVAL_60M:
            am = copy(self.engine.strategy.am_min60)
        elif interval == INTERVAL_1D:
            am = copy(self.engine.strategy.am_day01)
        else:
            am = None
            print("时间周期输入错误，请重新输入")

        time = am.time
        open = am.open
        high = am.high
        low = am.low
        close = am.close
        vol = am.volume

        data = []
        for i in range(len(open)):
            data.append([open[i], close[i], low[i], high[i]])
        return time, data, vol

    def buy_sell_view(self, interval=INTERVAL_60M, li=None, titlename="test_kline.html"):
        # 回测K线和买卖点的可视化
        # 输入所用周期，比如回测是在15分钟线上，就输入15分钟线的周期，回测是在60分钟线上，就输入60分钟线的周期
        if interval == INTERVAL_1M:
            time, data, vol = self.data_withdraw(INTERVAL_1M)
        elif interval == INTERVAL_5M:
            time, data, vol = self.data_withdraw(INTERVAL_5M)
        elif interval == INTERVAL_15M:
            time, data, vol = self.data_withdraw(INTERVAL_15M)
        elif interval == INTERVAL_30M:
            time, data, vol = self.data_withdraw(INTERVAL_30M)
        elif interval == INTERVAL_60M:
            time, data, vol = self.data_withdraw(INTERVAL_60M)
        elif interval == INTERVAL_1D:
            time, data, vol = self.data_withdraw(INTERVAL_1D)
        else:
            time, data, vol = None, None, None
            print("输入K线周期有问题，请重新输入")
        # 准备作图
        bv = backtest_view()
        bv.kline_add(time, data)
        if not li:
            ma1 = self.engine.strategy.am_min60.sma(10, array=True)
            ma2 = self.engine.strategy.am_min60.sma(40, array=True)
            bv.line_add("ma1", time, ma1)
            bv.line_add("ma2", time, ma2)
        else:
            for li_ in li:
                name = li_[0]
                data = li_[1]
                bv.line_add(name, time, data)

        resultList = self.result_trade['resultList']
        bv.buy_sell_add(resultList)
        bv.render_add(titlename)

    def daily_result_view(self, titlename="test_money.html"):
        # 提取数据
        time, data, vol = self.data_withdraw(INTERVAL_1D)
        # 初始化资金曲线图
        mv = money_view()
        # 添加K线
        mv.kline_add(time, data)

        # 添加基准曲线
        c = []
        close = [i[3] for i in data]
        for cl in close:
            c.append(cl / close[0])

        mv.base_line_add(time, c)

        # 添加资金曲线
        balance = self.result_daily['balance']
        ba = [1 for x in range(0, len(time) - len(balance))]
        for bal in balance:
            ba.append(bal / balance[0])
        mv.money_line_add(time, ba)
        # 添加回撤曲线
        drawdown = self.result_daily['ddPercent']
        dr = [0 for x in range(0, len(time) - len(drawdown))]
        for dra in drawdown:
            dr.append(dra / 100)
        mv.drawdown_line_add(time, dr)

        mv.empty_line_add(time)
        # 生成html
        mv.render_add(titlename)

    def showResult(self):
        self.engine.showBacktestingResult()

    def showDailyResult(self):
        self.engine.showDailyResult()

    # ----------------------------------------------------------------------
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

    # ----------------------------------------------------------------------
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


   # ----------------------------------------------------------------------
    def saveResult(self, savePath="test.xlsx", pathByTrade=None, pathByDay=None):

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
def formatNumber(n=8):
    """格式化数字到字符串"""
    rn = float("{0}".format(n))
    # rn = round(n, 2)        # 保留两位小数
    return rn



