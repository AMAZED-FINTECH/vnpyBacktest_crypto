对VNPY回测框架的提取与改版
1 调整收益计算方法，使之适合数字货币合约
  包括调整每日收益、手续费计算、滑点计算等
2 对各个环节对时间的考虑，为了计算日收益，同时最小化改动VNPY,
  改动了原版的datetime，增加了tradedate
  科目         名称      对象
  VtBarData   datetime  datetime
  engine      datetime  datetime
  VtTradeData datetime  datetime
  
  VtBarData   tradedate str
  engine      tradedate str
  VtTradeData tradedate str
  
如此修改之后，当数据输入数据库的时候，需要考量
当数据输入数据库的时候，直接把tradedate一步修改到位，以后不需要再改


新版的例子在examples 里面的runBacktesting_test3.py里面


  
  
 