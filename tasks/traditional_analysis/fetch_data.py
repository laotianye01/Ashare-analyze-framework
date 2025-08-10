from core.resource_manager import ResourceManager
from utils.database_utils import *
from utils.dataframe_utils import *
from core.taskNode import TaskNode

# TODO: 以下函数目前仅用于测试
class FetchCompanyData(TaskNode):
    def _custom_task(self, resource_config, params=None):
        try:
            db_manager = ResourceManager.create("postgres", resource_config)
            session = db_manager.get_session()
            symbol = params.get("stock_code")
            all_info = {}
            
            # 获取该公司相关的新闻
            sql = f"""
            SELECT symbol, keyword, title, content, timestamp
            FROM stock_news
            WHERE symbol = '{symbol}'
            ORDER BY timestamp DESC
            LIMIT 20;
            """
            news = query_with_sqlalchemy_df(session, sql)
            news.to_csv("./cache/tmp1.csv")
            
            # 获取该公司历史股价数据
            sql = f"""
            SELECT * FROM ashare
            WHERE symbol = '{symbol}'
            AND update_time >= '2025-01-01'
            AND EXTRACT(HOUR FROM update_time) = 23
            AND EXTRACT(MINUTE FROM update_time) = 59
            AND EXTRACT(SECOND FROM update_time) = 59;
            """
            data = query_with_sqlalchemy_df(session, sql)
            data.to_csv("./cache/tmp2.csv")
            
            # 获取该公司财报信息
            sql = f"""
            SELECT *
            FROM finance_statements
            WHERE "SECURITY_CODE" = '{symbol}'
            ORDER BY "REPORT_DATE" DESC
            LIMIT 3;
            """
            report = query_with_sqlalchemy_df(session, sql)
            report.to_csv("./cache/tmp3.csv")
            
            # 此处的写法不合理（仅测试），一个任务仅应当有一个返回值！！！
            all_info["news"] = news
            all_info["data"] = data
            all_info["report"] = report
        
            return {"status": "success", "data": all_info, "error": None}
        except Exception as e:
            return {"status": "failed", "data": None, "error": str(e)}