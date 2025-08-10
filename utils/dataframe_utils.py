# db_utils.py
from math import isinf, isnan
from datetime import datetime, timedelta
from sqlalchemy import inspect
import os
import pandas as pd

from core.resource.database_table import *


# ---------------- 以下为dataframe相关的操作，包括格式转化，清理等内容 ----------------- # 
def preprocess_sector_info(df):
    columns_to_keep = ['板块名称', '板块代码', '最新价', '涨跌幅']
    df = df[columns_to_keep]
    column_mapping = {
        '板块名称': 'name',
        '板块代码': 'code',
        '最新价': 'price',
        '涨跌幅': 'change_percent'
    }
    df = df.rename(columns=column_mapping)
    return df

def preprocess_index_info(df):
    columns_to_keep = ['名称', '代码', '最新价', '涨跌幅', '成交量']
    df = df[columns_to_keep]
    column_mapping = {
        '名称': 'name',
        '代码': 'code',
        '最新价': 'price',
        '涨跌幅': 'change_percent',
        '成交量': 'volume'
    }
    df = df.rename(columns=column_mapping)
    df['code'] = df['code'].apply(process_code)
        
    return df

# 以下两段代码用于指数 + 行业基础数据库的初始化
def preprocess_stock_index(df):
    columns_to_keep = ['品种代码', '品种名称']
    df = df[columns_to_keep]
    column_mapping = {
        '品种代码': 'symbol',
        '品种名称': 'name',
    }
    df = df.rename(columns=column_mapping)
    df['symbol'] = df['symbol'].apply(process_symbol)
    return df

def preprocess_stock_sector(df):
    columns_to_keep = ['代码', '名称', '最新价', '涨跌幅', '成交量', '最高', '最低', '今开', '昨收', '换手率', '市盈率-动态', '市净率']
    df = df[columns_to_keep]
    column_mapping = {
        "代码": "symbol",
        "名称": "name",
        "最新价": "price",
        "涨跌幅": "change_percent",
        "成交量": "volume",
        "振幅": "amplitude",
        "最高": "high",
        "最低": "low",
        "今开": "open",
        "昨收": "previous_close",
        "换手率": "turnover",
        "市盈率-动态": "pe_dynamic",
        "市净率": "pb_ratio",
    }
    df = df.rename(columns=column_mapping)
    df['symbol'] = df['symbol'].apply(process_symbol)
    return df

def preprocess_all_a_share_spot(df):
    # 删除无用列
    if "序号" in df.columns:
        df = df.drop(columns=["序号"])

    # 重命名列，使其匹配数据库字段
    df = df.rename(columns={
        "代码": "symbol",
        "名称": "name",
        "最新价": "price",
        "涨跌幅": "change_percent",
        "涨跌额": "change_amount",
        "成交量": "volume",
        "成交额": "amount",
        "振幅": "amplitude",
        "最高": "high",
        "最低": "low",
        "今开": "open",
        "昨收": "previous_close",
        "量比": "volume_ratio",
        "换手率": "turnover",
        "市盈率-动态": "pe_dynamic",
        "市净率": "pb_ratio",
        "总市值": "total_value",
        "流通市值": "circulating_value",
        "涨速": "speed",
        "5分钟涨跌": "change_5min",
        "60日涨跌幅": "change_60d",
        "年初至今涨跌幅": "change_ytd"
    })

    formatted_time = get_formatted_time()
    df['symbol'] = df['symbol'].apply(process_symbol)
    df["update_time"] = formatted_time
    return df

def preprocess_stock_news(df, symbol):
    columns_to_keep = ['关键词', '新闻标题', '新闻内容', '发布时间', '文章来源', '新闻链接']
    df = df[columns_to_keep]
    column_mapping = {
        '关键词': 'keyword',
        '新闻标题': 'title',
        '新闻内容': 'content',
        '发布时间': 'timestamp',
        '文章来源': 'source',
        '新闻链接': 'url'
    }
    df = df.rename(columns=column_mapping)
    df['symbol'] = symbol
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    return df

def preprocess_a_share_history(df):
    df = pd.DataFrame(df)
    # 1. 删除第一列（序号列）和 'outstanding_share' 列（假设第一列是默认的索引列，可以直接删除 'outstanding_share' 列）
    df = df.drop(columns=['outstanding_share'])
    # 2. 重命名 'date' 列为 'update_time'并修改 'update_time' 列的日期格式
    df = df.rename(columns={'date': 'update_time'})
    df['update_time'] = pd.to_datetime(df['update_time']).dt.strftime('%Y-%m-%d 23:59:59')
    return df

# 检查并修正 'symbol' 列
def process_symbol(value):
    if isinstance(value, int):  # 检查是否为整数
        value = str(value)  # 转换为字符串
    if len(value) < 6:  # 如果长度小于 6，前面补零
        value = value.zfill(6)
    return value

# 筛选和处理 code 列
def process_code(code):
    if len(code) == 8:
        return code[-6:]  # 保留后 6 位
    else:
        return None       # 长度不为 8 的丢弃
    
# 用于ORM匹配的数据清理(TODO:目前仅处理浮点数)
def check_df_ORM_consistency(ORMclass, df):
    # 获取ORM类中定义为float类型的列名
    mapper = inspect(ORMclass).mapper
    float_columns = [c.name for c in mapper.columns if c.type.__class__.__name__ == 'Float']

    # 清洗DataFrame中的数据
    for col in float_columns:
        df[col] = pd.to_numeric(df[col], errors='coerce')
        
    return df

def get_formatted_time():
    # 获取当前系统时间并转化为 YYYY/MM/DD HH:MM:SS 格式，仅精确到小时（需要符合TIMESTAMP的格式）
    now = datetime.now()
    current_date = now.date()
    current_hour = now.hour
    current_minute = now.minute

    # 15:00 - 次日 09:20：记录前一天的 15:00
    if current_hour > 15:
        formatted_time = current_date.strftime("%Y-%m-%d") + " 15:00:00"
    elif (current_hour < 9) or (current_hour == 9 and current_minute < 20):
        formatted_time = (current_date - timedelta(days=1)).strftime("%Y-%m-%d") + f" {15}:00:00"
    # 11:30 - 13:00：记录 11:00
    elif (current_hour == 11 and current_minute >= 30) or (current_hour < 13):
        formatted_time = current_date.strftime("%Y-%m-%d") + " 11:00:00"
    else:
        formatted_time = now.strftime("%Y-%m-%d %H:00:00")
        
    return formatted_time

# 用于合并两个dataframe（依据if_pad判断是否要保留所有列）
def merge_df(df_1, df_2, if_pad=False):
    if if_pad:
        all_columns = list(set(df_1.columns) | set(df_2.columns))
        df_1 = df_1.reindex(columns=all_columns)
        df_2 = df_2.reindex(columns=all_columns)
        merged_data = pd.concat([df_1, df_2], ignore_index=True)
    else:
        common_columns = list(set(df_1.columns) & set(df_2.columns))
        df_1 = df_1[common_columns]
        df_2 = df_2[common_columns]
        merged_data = pd.concat([df_1, df_2], ignore_index=True)
    
    return merged_data

# 用于对dict进行清洗
def clean_row_dict(row_dict):
    cleaned = {}
    for k, v in row_dict.items():
        if pd.isna(v) or (isinstance(v, float) and (isnan(v) or isinf(v))):
            cleaned[k] = None  # 用于 SQL NULL
        elif isinstance(v, (float, int)) and abs(v) > 1e18:
            # 如果某列异常值极大（超出 bigint 范围），可以选择设为 None 或截断
            cleaned[k] = None  # 或 round(v, 2)
        else:
            cleaned[k] = v
    return cleaned

def clean_df(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # 替换 inf/-inf 为 None
    df.replace([float('inf'), float('-inf')], None, inplace=True)

    # 替换 NaN 为 None
    df = df.where(pd.notnull(df), None)

    # 对所有 float64 / int64 类型列，处理超大值为 None
    threshold = 1e18
    numeric_cols = df.select_dtypes(include=['float64', 'int64']).columns
    for col in numeric_cols:
        mask = df[col].abs() > threshold
        df.loc[mask, col] = None

    return df

def fetch_index_sector_from_folder(index_folder, sector_folder):
    index_sector_list = {}
    for filename in os.listdir(index_folder):
        if filename.endswith(".csv"):
            file_path = os.path.join(index_folder, filename)
            df = pd.read_csv(file_path, index_col=0)
            # 使用文件名（不包括扩展名）作为键
            key = os.path.splitext(filename)[0]
            index_sector_list[key] = df

    # 加载 sector 文件夹中的 CSV 文件
    for filename in os.listdir(sector_folder):
        if filename.endswith(".csv"):
            file_path = os.path.join(sector_folder, filename)
            df = pd.read_csv(file_path, index_col=0)
            # 使用文件名（不包括扩展名）作为键
            key = os.path.splitext(filename)[0]
            index_sector_list[key] = df
            
    return index_sector_list

# 该函数未提供创业板代码的转换
def add_prefix(stock_code: str) -> str:
    if stock_code.startswith('6'):
        return 'sh' + stock_code
    elif stock_code.startswith('0') or stock_code.startswith('3'):
        return 'sz' + stock_code
    else:
        return ''
    
def filter_financial_report_by_label(fin_reports, target_label, subject_into_pth="./data/3main_chart/3finance_charts_main_subject.csv"):
    csv_data = pd.read_csv(subject_into_pth)
    filtered_attributes = csv_data[csv_data['label'] == target_label]['attribute'].tolist()
    # 添加固定列（如 SECURITY_CODE, REPORT_DATE 等）
    fixed_columns = ['SECURITY_CODE', 'ORG_CODE', 'ORG_TYPE', 'REPORT_DATE', 'REPORT_TYPE']
    columns_to_keep = fixed_columns + filtered_attributes
    # 筛选 fin_reports 中的对应列
    filtered_fin_reports = fin_reports[columns_to_keep]
    return filtered_fin_reports