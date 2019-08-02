import pyecharts.options as opts
from example.commons import Faker
from pyecharts.charts import Line


def line_base() -> Line:
    c = Line()
    c.add_xaxis(Faker.choose())
    c.add_yaxis("商家A", Faker.values())
    c.set_global_opts(legend_opts=opts.LegendOpts(type_="scroll", textstyle_opts=opts.TextStyleOpts(color="#000000")))
    c.set_global_opts(title_opts=opts.TitleOpts(title="Line-基本示例"))
    c.render()
    return c


if __name__ == "__main__":
    line_base()