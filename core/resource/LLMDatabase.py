import json
import psycopg2
import numpy as np
from typing import Dict, Optional, List

from core.logger import get_logger
logger = get_logger("ResourceManager") 


# PGVector 统一向量数据库管理器
class VectorDatabase:
    def __init__(self, dim: int, table_name: str, db_params: Optional[dict] = None):
        self.dim = dim
        self.table_name = table_name
        self.db_params = db_params

        assert db_params is not None
        self.connect = psycopg2.connect(**db_params)
        self.cursor = self.connect.cursor()
        
    def insert(self, embeddings: List[np.ndarray], texts: List[str], metadatas: List[dict]):
        """
        用于更新向量数据库
        """
        assert len(embeddings) == len(texts) == len(metadatas), "List lengths must match"

        sql = f"""INSERT INTO {self.table_name} (embedding, content, metadata) VALUES (%s, %s, %s)"""
        for emb, text, meta in zip(embeddings, texts, metadatas):
            self.cursor.execute(sql, (emb.tolist(), text, json.dumps(meta)))
        self.connect.commit()
        
    # TODO: 该函数需要进一步的修改，以保证长期记忆返回的有效性
    def get_history(self, query: str, top_k: int = 5) -> List[str]:
        """
        使用嵌入向量检索与 query 最相关的文本内容，用于拼接上下文 history
        """
        from utils.embedding_module import embed_text

        query_vec = embed_text(query)
        results = []

        sql = f"""
        SELECT embedding, content, metadata FROM {self.table_name}
        ORDER BY embedding <-> %s::vector
        LIMIT %s;
        """
        self.cursor.execute(sql, (query_vec.tolist(), top_k))
        rows = self.cursor.fetchall()
        for row in rows:
            item = {
                "embedding": np.array(row[0]),  # VECTOR 类型
                "content": row[1],              # TEXT 类型
                "metadata": row[2]              # JSONB 类型
            }
            results.append(item)

        return [r["content"] for r in results if r["content"]]
    
    def close(self):
        self.cursor.close()
        self.connect.close()
    

class VectorDatabaseManager:
    """
    统一封装向量管理器，支持PGVector
    """

    def __init__(self, dim: int, db_params: Optional[dict] = None):
        self.dim = dim
        self.db_params = db_params
        
        self.vector_database_cache: Dict[str, VectorDatabase] = {}  # 长期数据库缓存

        assert db_params is not None
        self.connect = psycopg2.connect(**db_params)
        self.cursor = self.connect.cursor()

    def check_table_exists(self, table_name: str) -> bool:
        self.cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_name=%s
            );
        """, (table_name,))
        return self.cursor.fetchone()[0]
        
    def create_table(self, table_name: str) -> VectorDatabase:
        myDatabase = None
        try:
            # 先启用 pgvector 扩展
            self.cursor.execute("CREATE EXTENSION IF NOT EXISTS vector;")
            self.connect.commit()  # 提交事务

            # 创建表，假设向量维度dim=384
            create_table_sql = f"""
            CREATE TABLE {table_name} (
                id SERIAL PRIMARY KEY,
                embedding VECTOR({self.dim}),
                content TEXT,
                metadata JSONB,
                created_at TIMESTAMP DEFAULT now()
            );
            """
            self.cursor.execute(create_table_sql)
            self.connect.commit()
            
            myDatabase = VectorDatabase(self.dim, table_name, self.db_params)
        except Exception as e:
            print(f"database init fail: {e}")
            
        return myDatabase
    
    # 不存在则创建，存在则直接返回
    def get_table(self, table_name: str) -> VectorDatabase:
        vectorDB = None
        if table_name in self.vector_database_cache:
            return self.vector_database_cache[table_name]
        elif not self.check_table_exists(table_name):
            vectorDB = self.create_table(table_name)
            logger.info(f"股票 {table_name} 的PGVector表 {table_name} 创建成功")
        else:
            vectorDB = VectorDatabase(self.dim, table_name, self.db_params)
            
        # TODO: 此处可能需要使用FIFO
        self.vector_database_cache[table_name] = vectorDB
        return vectorDB

    def close(self):
        self.cursor.close()
        self.connect.close()


# TODO: 此处可能会新增多个数据库的接口
class StockMemoryManager:
    """
    管理每一只股票对应的长期向量数据库
    """

    def __init__(self, db_params: dict, dim: int = 384):
        self.db_params = db_params
        self.dim = dim
        self.vector_database_managers = VectorDatabaseManager(dim=self.dim, db_params=self.db_params)
        
    def get_vector_database(self, stock_code: str, memory_type: str = "stock") -> VectorDatabase:
        vectorDB = None
        
        try:
            # TODO: 此处命名规则需要修改
            table_name = memory_type + '_' + stock_code
            vectorDB = self.vector_database_managers.get_table(table_name)
        except Exception as e:
            logger.error(f"fail to load vector database: {e}")
        
        return vectorDB

    def close_all(self):
        for manager in self.vector_managers.values():
            manager.close()
        self.vector_managers.clear()