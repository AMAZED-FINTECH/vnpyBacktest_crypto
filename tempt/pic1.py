#!/usr/bin/env python
# -*- coding: utf-8 -*-
from pyecharts import Line, Overlap

line = Line()




def example1(time_list, base_list, net_list, close_list):
    overlap = Overlap(width=1600, height=800, )
    line = Line()

    line.add("BTC基准线", time_list, base_list,
              is_datazoom_show=True,
              is_datazoom_extra_show=True,
              is_legend_show=True,
              # is_label_show=True,
              is_more_utils=True,
              is_xaxislabel_align=True,
              is_yaxislabel_align=True,
              line_color="#7CFC00",
              line_width=3,
              tooltip_axispointer_type="cross",
              # mark_point=["max", "min"]
              )
    line.add("净值", time_list, net_list,
             is_datazoom_show=True,
             is_datazoom_extra_show=True,
             is_legend_show=True,
             # is_label_show=True,
             is_more_utils=True,
             is_xaxislabel_align=True,
             is_yaxislabel_align=True,
             line_color="#00ff00",
             line_width=5,
             tooltip_axispointer_type="cross",
             # mark_point=["max", "min"]
             )
    line2 = Line()
    line2.add("BTC价格", time_list, close_list,
             is_datazoom_show=True,
             is_datazoom_extra_show=True,
             is_legend_show=True,
             # is_label_show=True,
             is_more_utils=True,
             is_xaxislabel_align=True,
             is_yaxislabel_align=True,
             line_color="#696969",
             line_width=3,
             # mark_point=["max", "min"]
             )

    overlap.add(line)
    overlap.add(line2, is_add_yaxis=True, yaxis_index=1)
    overlap.render()


if __name__ == "__main__":
    import pandas as pd
    df = pd.read_excel("net.xlsx")
    time_list = []
    for i in range(len(df)):
        time_list.append(df["date"][i].strftime("%Y%m%d"))
    base_list = df["base"].tolist()
    net_list = df["net"].tolist()
    close_list = df["price"].tolist()
    example1(time_list, base_list, net_list, close_list)