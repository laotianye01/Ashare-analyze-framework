# db_utils.py
import csv
from math import isinf, isnan
import time
from sqlalchemy import insert, select, update, text
from sqlalchemy.inspection import inspect
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session
from sqlalchemy_schemadisplay import create_schema_graph
from datetime import datetime, timedelta
from tqdm import tqdm
import pandas as pd
import akshare as ak

import core.resource as resource
from core.resource.database_table import *
from utils.dataframe_utils import *

'''
该文件提供了爬虫到数据库间格式转换的接口，用于将获取到的dataframe文件转化为postgre数据库所接受的格式
执行有关数据库的操作（增删改查）
'''

# 用于绘制数据库的 ER 图（运行该代码要安装dot，相当麻烦）
def get_db_ER(session, save_path="./cache/er_diagram.png"):
    engine = session.bind
    metadata = MetaData()
    metadata.reflect(bind=engine)
    graph = create_schema_graph(
        engine=engine,         # 传递 engine 参数,
        metadata=metadata,
        show_datatypes=False,  # 不显示数据类型
        show_indexes=False,    # 不显示索引
        rankdir='LR',          # 从左到右布局
        concentrate=False      # 不合并重复的边
    )

    graph.write_png(save_path)

# ---------------- 以下为postgre数据库相关的操作，包括数据增删改查等内容 ----------------- # 
def insert_dataframe_to_table(df: pd.DataFrame, orm_class_name: str, session: Session, drop_column=False):
    """
    通用函数：将 DataFrame 数据插入到任意 ORM 表中。
    - DataFrame 的列必须为 ORM 中字段的子集。
    - 自动使用主键作为 ON CONFLICT 更新条件。
    - 非主键字段将被更新。
    """
    # 动态加载ORM(数据库table定义类)
    orm_class = getattr(resource, orm_class_name)
    table = orm_class.__table__
    mapper = inspect(orm_class)

    # ORM 主键
    pk_columns = [col.name for col in mapper.primary_key]
    if not pk_columns:
        raise ValueError(f"{orm_class.__tablename__} 表没有定义主键，无法使用 ON CONFLICT。")

    # 检查 DataFrame 列是否为 ORM 字段子集
    orm_columns = set(c.name for c in mapper.columns)
    df_columns = set(df.columns)
    extra_columns = df_columns - orm_columns
    
    if drop_column:
        df.drop(columns=extra_columns, inplace=True)
    else:
        if extra_columns:
            raise ValueError(f"DataFrame 包含 ORM 表中未定义的字段: {extra_columns}")

    for _, row in tqdm(df.iterrows(), total=len(df), desc=f"insert {table.name}", unit="row"):
        row_dict = row.to_dict()

        stmt = pg_insert(table).values(row_dict)

        # ON CONFLICT：根据主键是否冲突，插入数据或更新非主键字段
        stmt = stmt.on_conflict_do_update(
            index_elements=pk_columns,
            set_={k: stmt.excluded[k] for k in row_dict if k not in pk_columns}
        )
        session.execute(stmt)

    session.commit()
    
# 该函数将通过session执行一条sql查询语句
def query_with_sqlalchemy_df(session: Session, sql: str) -> pd.DataFrame:
    """
    使用 SQLAlchemy 执行原始 SQL 并返回 pandas.DataFrame。
    """
    result = session.execute(text(sql))
    rows = [dict(row._mapping) for row in result.fetchall()]
    return pd.DataFrame(rows)

# 以下函数用于初始化股票基础信息，板块信息与其之间的对应关系
def initialize_sector_and_stock(session: Session, index_sector_list: dict, code_map):
    """
    初始化股票与板块信息以及它们之间的对应关系。
    
    :param session: SQLAlchemy 的 Session 实例
    :param index_sector_list: dict，结构为 {sector_name: pd.DataFrame(symbol, name)}
    """
    current_time = datetime.now()
    formatted_time = current_time.strftime("%Y-%m-%d")
    
    sector_info_records = []
    sector_component_set = set()
    stock_info_dfs = []
    
    columns_to_keep = ['name', 'code']
    code_map = code_map[columns_to_keep]

    for sector_name, df in index_sector_list.items():
        if df.empty:
            continue
        
        sector_code = code_map.loc[code_map['name'] == sector_name, 'code'].values
        if len(sector_code) == 0 or not isinstance(sector_code[0], str):
            print(f"Warning: No code found for sector '{sector_name}'. Skipping.")
            continue
        sector_code = sector_code[0]
        sector_info_records.append({
            "name": sector_name,
            "code": sector_code,
            "update_time": formatted_time
        })

        # 为该 sector 添加 sector_component 关系数据
        for _, row in df.iterrows():
            stock_code = row["symbol"]
            stock_code = process_symbol(stock_code)
            sector_component_set.add((sector_name, stock_code))

        # 增加 update_time 字段，用于 stock_info 插入
        df = df.copy()
        df["update_time"] = formatted_time
        df['symbol'] = df['symbol'].apply(process_symbol)
        stock_info_dfs.append(df)

    # 合并所有股票信息 DataFrame
    all_stock_info_df = pd.concat(stock_info_dfs, ignore_index=True).drop_duplicates(subset=["symbol"])
    # 插入或更新 StockInfo 表
    insert_dataframe_to_table(all_stock_info_df, "StockInfo", session)
    # 插入或更新 SectorInfo 表
    sector_info_df = pd.DataFrame(sector_info_records)
    insert_dataframe_to_table(sector_info_df, "SectorInfo", session)
    
    # 插入对应关系对
    existing_pairs = set(session.query(SectorComponent.sector_name, SectorComponent.stock_code).all())
    to_insert = [
        SectorComponent(sector_name=sector, stock_code=stock)
        for (sector, stock) in sector_component_set
        if (sector, stock) not in existing_pairs
    ]
    if to_insert:
        session.bulk_save_objects(to_insert)
    session.commit()
    
    
def generate_ORM_from_csv(csv_file, output_file):
    # 读取字段名
    import pandas as pd
    data = pd.read_csv(csv_file)  # './cache/3main_chart/3finance_charts_main_subject.csv'

    # 提取 attribute 列作为字段名
    attributes = data['attribute'].tolist()

    # 开始拼接 ORM 类代码
    lines = [
        "from sqlalchemy import Column, Float, String, Date",
        "from sqlalchemy.ext.declarative import declarative_base",
        "",
        "Base = declarative_base()",
        "",
        "class FinanceStatement(Base):",
        "    __tablename__ = 'finance_statement'",
        "",
        "    ts_code = Column(String, primary_key=True)",
        "    report_date = Column(Date, primary_key=True)",
    ]

    # 添加 attribute 列中的字段
    for attr in attributes:
        # 假设所有字段都是 Float 类型，你可以根据需要调整字段类型
        lines.append(f"    {attr} = Column(Float)")

    # 写入到 .py 文件
    with open(output_file, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

    print(f"ORM 类定义已写入 {output_file}")
    
    
def generate_ORM_from_csv_header(csv_file, output_file):
    # 读取字段名
    with open(csv_file, newline='', encoding='utf-8') as f:
        reader = csv.reader(f)
        headers = next(reader)

    # 开始拼接 ORM 类代码
    lines = [
        "from sqlalchemy import Column, Float, String, Date",
        "from sqlalchemy.ext.declarative import declarative_base",
        "",
        "Base = declarative_base()",
        "",
        "class FinanceStatement(Base):",
        "    __tablename__ = 'finance_statement'",
        "",
        "    ts_code = Column(String, primary_key=True)",
        "    report_date = Column(Date, primary_key=True)",
    ]

    for header in headers:
        # 避免重复添加主键字段
        if header not in {"ts_code", "report_date"}:
            lines.append(f"    {header} = Column(Float)")

    # 写入到 .py 文件
    with open(output_file, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

    print(f"ORM 类定义已写入 {output_file}")
    
def save_sector_index_csv(task_params=None):
    index_sector_list = {}
    share_index_realtime_sina = task_params.get("share_index_realtime")
    share_index_info = task_params.get("share_index_info")
    for _, row in share_index_realtime_sina.iterrows():
        try: 
            code = row['code']  
            idx = row['name'] 
            data = ak.index_stock_cons(code)                       # 需要传入指数代码（399639）
            data = preprocess_stock_index(data)
            data.to_csv("./cache/index/{}.csv".format(idx))
            index_sector_list[idx] = data                         # 股票指数的成份股目录
            time.sleep(5) 
        except Exception as e:
            print(f"Error processing index {idx}: {e}")
            continue
        
    for _, row in share_index_info.iterrows():
        try:
            idx = row["name"]
            data = ak.stock_board_industry_cons_em(idx)           # 直接传入板块名称即可（如小金属）
            data = preprocess_stock_sector(data)
            columns_to_keep = ['symbol', 'name']
            data = data[columns_to_keep]
            data.to_csv("./cache/sector/{}.csv".format(idx))
            index_sector_list[idx] = data                        # 获取东方财富板块最新成份股
            time.sleep(5)
        except Exception as e:
            print(f"Error processing index {idx}: {e}")
            continue


