from sqlalchemy import create_engine, MetaData
from sqlalchemy.orm import sessionmaker
from core.logger import get_logger

logger = get_logger("ResourceManager")


# PostgreSQL数据库连接
class PostgresDBManager:
    def __init__(self, db_url: str, pool_pre_ping: bool, pool_size: int, max_overflow: int):
        self.engine = create_engine(
            db_url,
            pool_pre_ping=pool_pre_ping,               # 避免使用失效连接
            pool_size=pool_size,                       # 可配置，根据负载优化
            max_overflow=max_overflow,                 # 额外连接上限
            connect_args={"client_encoding": "utf8"},  # 设置客户端编码为 UTF-8
        )
        self.SessionLocal = sessionmaker(bind=self.engine, autocommit=False, autoflush=False)
        self.metadata = MetaData()
        self.metadata.reflect(bind=self.engine)

    def get_session(self):
        return self.SessionLocal()

    def get_table(self, table_name: str):
        if table_name not in self.metadata.tables:
            raise ValueError(f"Table {table_name} not found.")
        return self.metadata.tables[table_name]

    def ping(self):
        try:
            with self.engine.connect() as conn:
                conn.execute("SELECT 1")
            return True
        except Exception as e:
            logger.error(f"Ping failed: {e}")
            return False