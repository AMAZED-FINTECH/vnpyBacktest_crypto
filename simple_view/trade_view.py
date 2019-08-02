#!/usr/bin/env python
# encoding: UTF-8

from pyecharts import Kline, Line, Overlap
import datetime
from copy import copy
from vnpy.trader.vtConstant import *
import pandas as pd

class trade_view(object):
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


    def buy_sell_add(self, tradelist):
        """输入所有交易，产生买卖点图形信号"""

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


    def render_add(self, path="render.html"):

        overlap = Overlap(width=1600, height=800, )

        overlap.add(self.kline)
        # overlap.add(self.line)
        overlap.add(self.line_long_win)
        overlap.add(self.line_long_loss)
        overlap.add(self.line_short_win)
        overlap.add(self.line_short_loss)

        # overlap.add(self.kline2, is_add_yaxis=True, yaxis_index=1)
        overlap.render(path)


if __name__ == "__main__":
    data = [[2320.26, 2320.26, 2287.3, 2362.94], [2300, 2291.3, 2288.26, 2308.38],
          [2295.35, 2346.5, 2295.35, 2345.92], [2347.22, 2358.98, 2337.35, 2363.8],
          [2360.75, 2382.48, 2347.89, 2383.76], [2383.43, 2385.42, 2371.23, 2391.82],
          [2377.41, 2419.02, 2369.57, 2421.15], [2425.92, 2428.15, 2417.58, 2440.38],
          [2411, 2433.13, 2403.3, 2437.42], [2432.68, 2334.48, 2427.7, 2441.73],
          [2430.69, 2418.53, 2394.22, 2433.89], [2416.62, 2432.4, 2414.4, 2443.03],
          [2441.91, 2421.56, 2418.43, 2444.8], [2420.26, 2382.91, 2373.53, 2427.07],
          [2383.49, 2397.18, 2370.61, 2397.94], [2378.82, 2325.95, 2309.17, 2378.82],
          [2322.94, 2314.16, 2308.76, 2330.88], [2320.62, 2325.82, 2315.01, 2338.78],
          [2313.74, 2293.34, 2289.89, 2340.71], [2297.77, 2313.22, 2292.03, 2324.63],
          [2322.32, 2365.59, 2308.92, 2366.16], [2364.54, 2359.51, 2330.86, 2369.65],
          [2332.08, 2273.4, 2259.25, 2333.54], [2274.81, 2326.31, 2270.1, 2328.14],
          [2333.61, 2347.18, 2321.6, 2351.44], [2340.44, 2324.29, 2304.27, 2352.02],
          [2326.42, 2318.61, 2314.59, 2333.67], [2314.68, 2310.59, 2296.58, 2320.96],
          [2309.16, 2286.6, 2264.83, 2333.29], [2282.17, 2263.97, 2253.25, 2286.33],
          [2255.77, 2270.28, 2253.31, 2276.22]]


    time = ["2017-7-{} 00:00:00".format(i + 1) for i in range(31)]

    # trade 是list  顺序： 开仓时间，开仓价格，开仓数量，退出时间，退出价格，退出数量，方向（1和-1），盈亏情况（1和-1）
    tradelist = [[time[3], data[3][1], 1, time[8], data[8][1], 1, 1, 1],
                 [time[10], data[10][1], 1, time[16], data[16][1], 1, 1, -1],
                 [time[18], data[18][1], 1, time[23], data[23][1], 1, -1, -1],
                 [time[25], data[25][1], 1, time[30], data[30][1], 1, -1, 1]
                 ]
    tv = trade_view()
    tv.kline_add(time, data)
    tv.buy_sell_add(tradelist)
    tv.render_add()





