import re
from tasks import *
from utils.database_utils import *
from utils.dataframe_utils import *
from core import ResourceManager

'''
该文件用于task函数的测试
'''


def test_fetch_bing_news():
    resource_conf = './config/resource/resource_conf.json'
    task_params = {
        "query": "中国 财经 新闻",
        "max_results": 15,
        "whitelist": [
            "finance.sina.com.cn",
            "cn.reuters.com"
        ]
    }
    
    fun = FetchAILLMChat()
    result = fun.custom_task(resource_conf, task_params).get("data")
    print(result)
    
def test_AILLM():
    resource_conf = './config/resource/resource_conf.json'
    task_params = {
        "use_long_term_memory": False,
        "vector_database_index": "",
        "user_prompt": "你将作为我的新闻分析员。下面为我想你提供的若干条财经新闻，请你选出其中对中国金融市场有影响的新闻，并基于其内容，提取关键信息，并将新闻摘要返回给我",
        "net_search_prompt": """
            七部门：加快构建科技金融体制
            2025-05-15 10:16七部门出台15项举措 引导更多金融资源流向科技创新领域
            2025-05-15 10:15国办印发国务院2025年度立法工作计划
        """
    }

    fun = FetchAILLMChat()
    result = fun.custom_task(resource_conf, task_params).get("data")
    print(result)

# -----------以下内容用于测试函数用于测试大盘数据的插入 + 目标股票的获取（读取数据库并分析）+ 目标股票历史数据与财报的获取 + 对应数据的存储----------- #
def get_ER():
    resource_conf = './config/resource/resource_conf.json'
    db_manager = ResourceManager.create("postgres", resource_conf)
    session = db_manager.get_session()
    get_db_ER(session)
    
def gen_ORM():
    csv_file = "./cache/3main_chart/3finance_charts_main_subject.csv"
    output_file = "./core/resource/my_orm.py"
    generate_ORM_from_csv(csv_file, output_file)

# 以下函数用于测试大盘数据的获取，存储与读取（通过）
def test_postgre_insert_and_query():
    resource_conf = './config/resource/resource_conf.json'
    task_params = {}
    fun_ashare = FetchAllAShareSpot()
    result = fun_ashare.custom_task(resource_conf, task_params).get("data")
    df_data = result.get("data")
    df_data = df_data.nlargest(100, 'turnover')
    df_data.to_csv("./cache/ashare_current.csv")
    
    # 在数据库中与dataframe中分别执行筛选，验证所得结果是否正确
    sql = """
        SELECT *
        FROM ashare
        WHERE update_time = (
            SELECT MAX(update_time)
            FROM ashare
        )
        ORDER BY turnover DESC NULLS LAST
        LIMIT 100;
    """
    db_manager = ResourceManager.create("postgres", resource_conf)
    db_session = db_manager.get_session()
    postgre_data = query_with_sqlalchemy_df(db_session, sql)
    diff1 = df_data[~df_data.isin(postgre_data)].dropna()
    # 找出 df_data1 中不在 df_data2 中的行
    
    print("df_data1 中不在 df_data2 中的行：")
    print(diff1)
    
def test_analyze():
    resource_conf = './config/resource/resource_conf.json'
    task_params = {
        "stock_code": "603123"
    }
    fun_company = FetchCompanyData()
    data = fun_company.custom_task(resource_conf, task_params).get("data")
    
    task_params = {
        "data": data
    }
    fun_draw = DrawGraph()
    fun_draw.custom_task(resource_conf, task_params)
    
    task_params = {
        "use_long_term_memory": False,
        "vector_database_index": "",
        "user_prompt": "你将作为我的分析员。下面为我将你提供的若干信息，请你根据有关信息，生成对应公司的的简报，并分析其未来经营情况，与市场可能对其产生的预期",
        "all_data": data
    }
    fun = FetchAILLMChat()
    result = fun.custom_task(resource_conf, task_params)
    
def test_postgre():
    resource_conf = './config/resource/resource_conf.json'
    
    task_params = {}
    fun_all = FetchAllAShareSpot()
    result = fun_all.custom_task(resource_conf, task_params).get("data")
    df_data = result.get("data")
    df_data = df_data.nlargest(100, 'turnover')
    
    task_params = {}
    task_params["init"] = True
    fun_update = UpdataRealSector()
    fun_update.custom_task(resource_conf, task_params)
    
    sql = """
        SELECT r.* FROM ashare r
        JOIN sector_components sc ON r.symbol = sc.stock_code
        JOIN sector_info s ON sc.sector_name = s.name WHERE s.name = '上证指数'
        AND r.update_time = (
            SELECT MAX(update_time)
            FROM ashare
        )
        ORDER BY r.turnover DESC
        LIMIT 2;
    """
    
    db_manager = ResourceManager.create("postgres", resource_conf)
    db_session = db_manager.get_session()
    postgre_data = query_with_sqlalchemy_df(db_session, sql)
    
    # 提取股票代码
    top_stock_codes = postgre_data['symbol'].tolist()
    # 获取股票历史数据（考虑将历史数据存储到每天的23:59:59）
    task_params = {
        "stock_code": "",
        "start_date": "20250101",
        "end_date": "21000101"
    }
    core_subjects = {}
    for stock_code in top_stock_codes:
        if len(stock_code) == 6: 
            task_params["stock_code"] = stock_code
            # 获取股票历史数据
            fun_history = FetchAShareHistory()
            his_stock = fun_history.custom_task(resource_conf, task_params).get("data")
            if his_stock is None:  # 股票属于创业板
                continue

            # 获取股票三大财报
            fun_reports = FetchFinReport()
            fin_reports = fun_reports.custom_task(resource_conf, task_params).get("data")
            fin_reports = filter_financial_report_by_label(fin_reports, 0)
            core_subjects[stock_code] = fin_reports
            
    # 获取股票相关新闻
    history_news = {}
    for code, _ in core_subjects.items():
        task_params = {}
        task_params["stock_code"] = code
        fun_news = FetchStockNews()
        news = fun_news.custom_task(resource_conf, task_params).get("data")
        history_news[code] = news
        
    user_prompt = "你将作为我的分析员。下面为我将你提供的若干信息，请你根据有关信息，生成对应公司的的简报，并分析其未来经营情况，与市场可能对其产生的预期"
    for code, reports in core_subjects.items():
        news = history_news.get(code).head(20).to_csv(index=False, sep=',')
        reports = reports.head(3).to_csv(index=False, sep=',')
        related_data = "\n财报数据：\n" + reports + "\n" + "\n新闻数据：\n" + news + "\n"
        length_of_string = len(related_data)
        print(f"字符串的长度（字符数）: {length_of_string}")
        
        net_search_prompt = ""
        task_params = {
            "use_long_term_memory": False,
            "vector_database_index": "",
            "user_prompt": user_prompt,
            "related_data": related_data,
            "net_search_prompt": net_search_prompt
        }
        fun_ai = FetchAILLMChat()
        result = fun_ai.custom_task(resource_conf, task_params)
        print(result)
        

def test_save_fin_report():
    
    codes = [
        "000015",
    ]
    resource_conf = './config/resource/resource_conf.json'
    
    for code in codes:
        try:
            task_params = {}
            task_params["stock_code"] = code
            
            # 获取股票三大财报
            fetchReports = FetchFinReport()
            fin_reports = fetchReports._custom_task(resource_conf, task_params).get("data")
            
            # 映射关系
            subject_into_pth="./data/3main_chart/3finance_charts_main_subject.csv"
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

                # 保存
                output_dir = "./cache/"
                output_path = os.path.join(output_dir, f"{code}_{chart_name}_report.csv")
                df_pivot.to_csv(output_path, encoding="utf-8-sig")
                
            print(f"已保存 {code} 报表")
            
        except Exception as e:
            print(f"throw excption: {e}")
        
        time.sleep(2)

if __name__ == "__main__":
    # test_AILLM()
    # test_fetch_bing_news()
    # test_postgre_insert_and_query()
    # gen_ORM()
    # test_postgre()
    # get_ER()
    # test_analyze()
    test_save_fin_report()
