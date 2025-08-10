import time
import akshare as ak
from core.resource_manager import ResourceManager
from utils.database_utils import *
from utils.dataframe_utils import *
from core.taskNode import TaskNode
import pandasql as ps


"""
This module provides functions to fetch various types of stock market data, 
including real-time stock status, historical data, financial reports, 
and more. It relies on the AkShare library to interact with financial data sources.

This module encapsulates akshare package, which standardizes all functions.
"""

# TODO: 以下task中最后的两个插入函数的正确性均未被验证
# 用于更新股票的基础信息
class UpdataRealSector(TaskNode):
    def _custom_task(self, resource_config, task_params=None):
        try:
            # 以下代码用于获取及时的板块信息
            share_index_realtime_sina = ak.stock_zh_index_spot_sina()                          # 股票指数实时行情数据-新浪
            reserved_indexes = ["上证指数", "科创综指", "深证成指", "上证50", "沪深300", "中证100", "中证500", "中证1000"]
            share_index_realtime_sina = share_index_realtime_sina[share_index_realtime_sina['名称'].isin(reserved_indexes)]
            share_index_realtime_sina = preprocess_index_info(share_index_realtime_sina)
            
            share_index_info = ak.stock_board_industry_name_em()                               # 获取东方财富板块信息
            share_index_info = preprocess_sector_info(share_index_info)
            sector_df = merge_df(share_index_realtime_sina, share_index_info, if_pad=True)
            formatted_time = get_formatted_time()
            sector_df["update_time"] = formatted_time
            
            # postgre数据库操作
            db_manager = ResourceManager.create("postgres", resource_config)
            session = db_manager.get_session()
            
            # 以下代码用于数据库初始化与更新时
            init = task_params.get("init", False) if task_params else False
            if init:
                # task_params = {}
                # task_params["share_index_realtime"] = share_index_realtime_sina
                # task_params["share_index_info"] = share_index_info
                # save_sector_index_csv(resource_config, task_params)
                
                index_sector_list = fetch_index_sector_from_folder(index_folder="./data/index_data/index", sector_folder="./data/index_data/sector")
                # 以下函数构建了sector_info, sector_components与两表间的映射关系
                initialize_sector_and_stock(session, index_sector_list, sector_df)
                
            # insert_dataframe_to_table(sector_df, "RealtimeSector", session)
            return {"status": "success", "data": sector_df, "error": None}
        except Exception as e:
            return {"status": "failed", "data": None, "error": str(e)}
    

# fetch latest status of all A share stocks
class FetchAllAShareSpot(TaskNode):
    def _custom_task(self, resource_config, params=None):
        try:
            kc_data = ak.stock_kc_a_spot_em()
            a_data = ak.stock_zh_a_spot_em()
            data = merge_df(kc_data, a_data)
            data = preprocess_all_a_share_spot(data)
            data = clean_df(data)
            return {"status": "success", "data": data, "error": None}
        except Exception as e:
            return {"status": "failed", "data": None, "error": str(e)}

# fetch latest status of all KeChuang stocks 
class FetchKCAShareSpot(TaskNode):
    def _custom_task(self, resource_config, params=None):
        try:
            data = ak.stock_kc_a_spot_em()
            return {"status": "success", "data": data, "error": None}
        except Exception as e:
            return {"status": "failed", "data": None, "error": str(e)}

# fetch latest status of all HongKong stocks
class FetchHKMainSpot(TaskNode):
    def _custom_task(self, resource_config, params=None):
        try:
            data = ak.stock_hk_main_board_spot_em()
            return {"status": "success", "data": data, "error": None}
        except Exception as e:
            return {"status": "failed", "data": None, "error": str(e)}

# fetch history status of a specific stock in A share
class FetchAShareHistory(TaskNode):
    def _custom_task(self, resource_config, params=None):
        try:   
            symbol = params.get("stock_code")
            symbol_with_prefix = add_prefix(params.get("stock_code"))     
            data = ak.stock_zh_a_daily(symbol=symbol_with_prefix, start_date=params.get("start_date"), end_date=params.get("end_date"))
            data['symbol'] = symbol
            data = preprocess_a_share_history(data)
            data = clean_df(data)
            return {"status": "success", "data": data, "error": None}
        except Exception as e:
            return {"status": "failed", "data": None, "error": str(e)}

# fetch coarse history financial reports of a specific stock in A share
class FetchFinAbstract(TaskNode):
    def _custom_task(self, resource_config, params=None):
        try:
            stock_code = params["stock_code"]
            data = ak.stock_financial_abstract(stock_code)
            return {"status": "success", "data": data, "error": None}
        except Exception as e:
            return {"status": "failed", "data": None, "error": str(e)}

class FetchFinReport(TaskNode):
    def _custom_task(self, resource_config, params=None):
        try:
            # 以下函数获取的数据不太全
            # data = ak.stock_financial_report_sina(add_prefix(task_params.get("stock_code")))
            symbol = add_prefix(params.get("stock_code")).upper()
            balance_sheet = ak.stock_balance_sheet_by_report_em(symbol)
            profit_sheet = ak.stock_profit_sheet_by_report_em(symbol)
            cash_flow_sheet = ak.stock_cash_flow_sheet_by_report_em(symbol)
            
            # 提取每个表中的 REPORT_DATE 列，仅保留交集中的日期条目
            balance_dates = set(balance_sheet['REPORT_DATE'])
            profit_dates = set(profit_sheet['REPORT_DATE'])
            cash_flow_dates = set(cash_flow_sheet['REPORT_DATE'])
            common_dates = balance_dates.intersection(profit_dates).intersection(cash_flow_dates)
            balance_sheet_filtered = balance_sheet[balance_sheet['REPORT_DATE'].isin(common_dates)]
            profit_sheet_filtered = profit_sheet[profit_sheet['REPORT_DATE'].isin(common_dates)]
            cash_flow_sheet_filtered = cash_flow_sheet[cash_flow_sheet['REPORT_DATE'].isin(common_dates)]

            # 使用 merge 方法拼接表（sql的join），列名冲突时冲突列添加后缀_drop，并在拼接完后被丢弃
            data = balance_sheet_filtered.merge(profit_sheet_filtered, on='REPORT_DATE', how='outer', suffixes=('', '_drop'))
            data = data.merge(cash_flow_sheet_filtered, on='REPORT_DATE', how='outer', suffixes=('', '_drop'))
            data = data.loc[:, ~data.columns.str.endswith('_drop')]
            
            # 数据格式转化
            data["REPORT_DATE"] = pd.to_datetime(data["REPORT_DATE"])
            data = check_df_ORM_consistency(FinanceStatement, data)
            
            if params.get("convert", False):
                all_pivots = []
                fin_reports = data
            
                # 映射关系
                subject_into_pth = params.get("subject_into", "./data/3main_chart/3finance_charts_main_subject.csv")
                csv_data = pd.read_csv(subject_into_pth)
                attribute_to_subject = dict(zip(csv_data["attribute"], csv_data["subject"]))

                # 找出所有财报项目列（即存在于映射中的列）,并替换列名
                attribute_cols = [col for col in fin_reports.columns if col in attribute_to_subject]
                renamed_cols = {attr: attribute_to_subject[attr] for attr in attribute_cols}
                fin_reports_renamed = fin_reports.rename(columns=renamed_cols)

                # 提取日期列
                if "REPORT_DATE" in fin_reports_renamed.columns:
                    fin_reports_renamed["report_date"] = pd.to_datetime(fin_reports_renamed["REPORT_DATE"])
                else:
                    raise ValueError("未找到 'REPORT_DATE' 列，无法提取日期")
                
                fin_reports_renamed['report_date'] = fin_reports_renamed['report_date'].dt.strftime('%Y-%m-%d')
                for chart_name in csv_data["chart"].unique():
                    # 找出该类下的所有财务科目
                    subject_list = csv_data[csv_data["chart"] == chart_name]["subject"].tolist()
                    available_subjects = [col for col in subject_list if col in fin_reports_renamed.columns]
                    if not available_subjects:
                        print(f"[跳过] 类别 {chart_name} 中无匹配列，已跳过。")
                        continue
                    cols = ["report_date"] + available_subjects
                    df_class = fin_reports_renamed[cols].copy()

                    # 将其变成“财务项目为行、日期为列”的结构
                    df_melt = df_class.melt(id_vars=["report_date"], var_name="subject", value_name="value")
                    df_pivot = df_melt.pivot_table(index="subject", columns="report_date", values="value", aggfunc="mean")
                    df_pivot = df_pivot.sort_index(axis=1, ascending=False)
                    
                    # subject -> 序号 映射
                    df_pivot = df_pivot.reset_index()
                    subject_to_index = dict(zip(csv_data["subject"], csv_data["序号"]))
                    df_pivot["序号"] = df_pivot["subject"].map(subject_to_index)
                    df_pivot = df_pivot.sort_values(by="序号")
                    df_pivot = df_pivot.drop(columns=["序号"])
                    df_pivot = df_pivot.reset_index()
                    df_pivot = df_pivot.drop(columns=["index"])
                    
                    df_pivot = df_pivot.rename(columns={'subject': '日期'})
                    all_pivots.append(df_pivot)
                if all_pivots:
                    # pd.concat 默认按行拼接，设置 axis=1 可按列拼接
                    data = pd.concat(all_pivots, axis=1)
            return {"status": "success", "data": data, "error": None}
        except Exception as e:
            return {"status": "failed", "data": None, "error": str(e)}
        
    def _register_pipeline(self, result):
        code = self.params.get("stock_code")
        workflow_list = []
        pipeline_config = {}
        workflow_id = f"share_analyze_{code}"
        pipeline_config["name"] = workflow_id  # pipeline 实例名
        pipeline_config["template"] = "share_analyse_workflow"  # pipeline 实例名
        pipeline_config["params"] = {}
        pipeline_config["params"]["stock_code"] = code  # 全局注入参数
        workflow_list.append(pipeline_config)
        return workflow_list


# 获取目标股票代码（根据配置确定是否注册新pipeline）
class FetchTargetShare(TaskNode):
    def _custom_task(self, resource_config, params=None):
        try:
            source = params.get("source", "")
            if source == "db":
                db_manager = ResourceManager.create("postgres", resource_config)
                db_session = db_manager.get_session()
                sql = params.get("sql", "")
                postgre_data = query_with_sqlalchemy_df(db_session, sql)
                return {"status": "success", "data": postgre_data, "error": None}
            elif source == "cache":
                # 从 params 中获取 DataFrame ，其名称要与对应sql中的名称相同！！！
                ashare_data = params.get("ashare_data", pd.DataFrame())
                # 从 params 中获取 SQL 语句
                sql = params.get("sql", "")
                
                if ashare_data.empty or not sql:
                    return {"status": "failed", "data": None, "error": "No data or SQL query provided for cache source."}
                
                try:
                    # 使用 pandasql 对 DataFrame 执行 SQL 查询
                    # 注意：pandasql会通过locals()将当前作用域的变量转为db，并在执行sql时查找对应名称的db
                    cache_data = ps.sqldf(sql, locals())
                    return {"status": "success", "data": cache_data, "error": None}
                except Exception as e:
                    return {"status": "failed", "data": None, "error": f"Failed to query cache data: {e}"}
            else:
                return {"status": "failed", "data": None, "error": "Invalid source specified."}

        except Exception as e:
            return {"status": "failed", "data": None, "error": str(e)}
        
    def _register_pipeline(self, result):
        workflow_list = []
        for code in result['symbol'].tolist():  # 假设 dataframe 中有 'code'
            pipeline_config = {}

            # 设置唯一 workflow 名（如 share_analyze_000001）
            workflow_id = f"fetch_share_data_{code}"
            pipeline_config["name"] = workflow_id  # pipeline 实例名
            pipeline_config["template"] = "share_data_fetch_workflow"  # pipeline 实例名
            pipeline_config["params"] = {}
            pipeline_config["params"]["stock_code"] = code  # 全局注入参数
            workflow_list.append(pipeline_config)
        return workflow_list
    
# 获取目标股票代码（根据配置确定是否注册新pipeline）
class FetchTargetShareFromCache(TaskNode):
    def _custom_task(self, resource_config, params=None):
        try:
            db_manager = ResourceManager.create("postgres", resource_config)
            db_session = db_manager.get_session()
            sql = params.get("sql")
            postgre_data = query_with_sqlalchemy_df(db_session, sql)
            return {"status": "success", "data": postgre_data, "error": None}

        except Exception as e:
            return {"status": "failed", "data": None, "error": str(e)}
        
    def _register_pipeline(self, result):
        workflow_list = []
        for code in result['symbol'].tolist():  # 假设 dataframe 中有 'code'
            pipeline_config = {}

            # 设置唯一 workflow 名（如 share_analyze_000001）
            workflow_id = f"fetch_share_data_{code}"
            pipeline_config["name"] = workflow_id  # pipeline 实例名
            pipeline_config["template"] = "share_data_fetch_workflow"  # pipeline 实例名
            pipeline_config["params"] = {}
            pipeline_config["params"]["stock_code"] = code  # 全局注入参数
            workflow_list.append(pipeline_config)
        return workflow_list

# TODO:fetch all stocks in a specific share index
class FetchGoldPrice(TaskNode):
    def _custom_task(self, resource_config, params=None):
        try:
            data = ak.spot_hist_sge()
            return {"status": "success", "data": data, "error": None}
        except Exception as e:
            return {"status": "failed", "data": None, "error": str(e)}