import pandas as pd
import akshare as ak
# akshare参考文档: https://akshare.akfamily.xyz/tutorial.html

all_share = ak.stock_zh_a_spot_em()                         # 东财 A 股实时行情数据 -- 按天进行抓取
all_share_new = ak.stock_kc_a_spot_em()                     # 东财科创板实时行情数据
all_share_hk = ak.stock_hk_main_board_spot_em()             # 港股主板实时行情

share_history = ak.stock_zh_a_daily("sh603843", "20200101", "21000118")                      # A 股针对某个股票历史行情数据(日频)
share_fin_abstract = ak.stock_financial_abstract("600004")  # 财务摘要
share_fin_report = ak.stock_financial_report_sina()         # 三大财务报表

sector_name = ak.stock_board_industry_name_em()             # 行业板块-板块名称
sector_compose = ak.stock_board_industry_cons_em()          # 行业板块-板块成份

share_index_info = ak.index_stock_info()                    # 股票指数-成份股-最新成份股
share_index_compose = ak.index_stock_cons("000009")         # 股票指数-成份股-最新成份股

gold_price = ak.spot_hist_sge()                             # 黄金价格
re_ratio = ak.macro_china_reserve_requirement_ratio()       # 存款准备金率
interest  = ak.macro_china_lpr()                            # 中国-利率-1年期 5年期 短期借贷 长期借贷
currency_boc = ak.currency_boc_safe()                       # 人民币汇率中间价

share_history.to_csv("C:/Users/26288/Desktop/stock_data/Ashare-main/data/share_history.csv", index=False, encoding='utf-8-sig')

