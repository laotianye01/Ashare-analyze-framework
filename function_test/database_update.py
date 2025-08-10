# stock_info_sync.py

import akshare as ak
import pandas as pd
from tqdm import tqdm        
from sqlalchemy import create_engine, Table, MetaData
from sqlalchemy.dialects.postgresql import insert as pg_insert
from datetime import datetime
from math import isnan, isinf

# ------------------ 数据库连接配置 ------------------
db_user = ""
db_password = ""
db_host = "localhost"
db_port = "5432"
db_name = "stock_db"

# 创建 SQLAlchemy 数据库引擎
engine = create_engine(f"postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}")

# ------------------ 加载表结构 ------------------
metadata = MetaData()
metadata.reflect(bind=engine)

# 加载 realtime_quote 表与 stock_info 表结构
realtime_table = metadata.tables['date_stock']
stock_info_table = metadata.tables['stock_info']

# ------------------ 获取 A 股实时数据 ------------------
def fetch_realtime_data():
    df = ak.stock_zh_a_spot_em()

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
        "成交额": "turnover",
        "振幅": "amplitude",
        "最高": "high",
        "最低": "low",
        "今开": "open",
        "昨收": "previous_close",
        "量比": "volume_ratio",
        "换手率": "turnover_rate",
        "市盈率-动态": "pe_dynamic",
        "市净率": "pb_ratio",
        "总市值": "total_value",
        "流通市值": "circulating_value",
        "涨速": "speed",
        "5分钟涨跌": "change_5min",
        "60日涨跌幅": "change_60d",
        "年初至今涨跌幅": "change_ytd"
    })

    # 获取当前系统时间并转化为 YYYY/MM/DD 格式
    now = datetime.now()
    formatted_time = now.strftime("%Y/%m/%d")
    df["update_time"] = formatted_time

    return df


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


def clean_realtime_df(df: pd.DataFrame) -> pd.DataFrame:
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


# ------------------ 插入 realtime_quote 表数据 ------------------
def insert_realtime_quote(df: pd.DataFrame):
    """
    将实时行情数据写入 realtime_quote 表，
    如果主键冲突（symbol + update_time），则进行更新。
    """
    with engine.begin() as conn:
        for _, row in tqdm(df.iterrows(), total=len(df), desc="update date_stock: ", unit="row"):    # 使用tqdm库微循环函数添加进度条
            row_dict = row.to_dict()
            # row_dict = clean_row_dict(row_dict)  # 数据清洗

            # 构建插入语句，向表realtime_table中插入row_dict
            stmt = pg_insert(realtime_table).values(row_dict)

            # 使用 PostgreSQL 的 INSERT ... ON CONFLICT，即遇到 symbol + update_time 冲突时更新其他字段（只跳过主键）
            stmt = stmt.on_conflict_do_update(
                index_elements=['symbol', 'update_time'],
                set_={k: stmt.excluded[k] for k in row_dict if k not in ['symbol', 'update_time']}
            )

            conn.execute(stmt)

# ------------------ 插入 stock_info 表数据 ------------------
def insert_stock_info(df: pd.DataFrame):
    """
    仅将 symbol 和 name 写入 stock_info 表（去重），
    如果 symbol 已存在则更新 name 与 exchange。
    """
    df_unique = df[['symbol', 'name', 'update_time']].drop_duplicates()

    # 判断股票属于哪个交易所（简单规则）
    df_unique["exchange"] = df_unique["symbol"].apply(lambda x: "sh" if x.startswith("6") else "sz")

    with engine.begin() as conn:
        for _, row in tqdm(df_unique.iterrows(), total=len(df_unique), desc="update stock_info: ", unit="row"):
            row_dict = row.to_dict()

            stmt = pg_insert(stock_info_table).values(row_dict)

            # 冲突时更新 name、exchange 和 updated_at
            stmt = stmt.on_conflict_do_update(
                index_elements=['symbol'],
                set_={
                    "name": stmt.excluded.name,
                    "exchange": stmt.excluded.exchange,
                    "update_time": stmt.excluded.update_time
                }
            )

            conn.execute(stmt)

# ------------------ 主函数：同步流程 ------------------
def sync_all():
    print("🚀 开始同步 realtime_quote 和 stock_info 数据 ...")
    df = fetch_realtime_data()
    df = clean_realtime_df(df)

    # 可选：保存为 CSV 备份一份
    df.to_csv("C:/Users/26288/Desktop/stock_data/Ashare-main/data/share_all.csv", index=False, encoding='utf-8-sig')

    insert_realtime_quote(df)
    insert_stock_info(df)

    print(f"✅ 同步完成，共处理 {len(df)} 条记录")

# ------------------ 启动主程序 ------------------
if __name__ == "__main__":
    sync_all()
