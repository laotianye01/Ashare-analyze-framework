# resource_manager.py
import os
import json
from core.logger import get_logger
from core.resource.agent import AILLM
from core.resource.LLMDatabase import StockMemoryManager
from core.resource.postgre import PostgresDBManager
from core.resource.searcher import BingSearcher

logger = get_logger("ResourceManager")

"""
无需实例化 ResourceManager 对象，该类提供静态方法用于资源初始化，工厂方法被保存在FACTORY_REGISTRY中
调用方法：传入函数名（注册在工厂方法列表中） + 配置文件
llm = ResourceManager.create("LLM", resource_config)
"""

def create_postgres(config: dict):
    if "postgres" not in config:
        raise ValueError("Missing 'postgres' in resource config")
    
    postgres_config = config.get("postgres", {})
    return PostgresDBManager(
        db_url=postgres_config.get("db_url"),
        pool_pre_ping=postgres_config.get("pool_pre_ping"),
        pool_size=postgres_config.get("pool_size"),
        max_overflow=postgres_config.get("max_overflow"),
    )
    
    
def create_searcher(config: dict):
    if "searcher" not in config:
        raise ValueError("Missing 'searcher' in resource config")
    
    searcher_config = config.get("searcher", {})
    return BingSearcher(
        driver_path=searcher_config.get("driver_path")
    )


def create_LLMDatabase(config: dict):
    if "LLMMemoryManager" not in config:
        raise ValueError("Missing 'LLMMemoryManager' in resource config")
    
    LLMMemoryManager_config = config.get("LLMMemoryManager", {})
    return StockMemoryManager(
        db_params=LLMMemoryManager_config.get("db_params"),
        dim=LLMMemoryManager_config.get("dim"),
    )


def create_agent(config: dict):
    if "AILLM" not in config:
        raise ValueError("Missing 'AILLM' in resource config")
    
    AILLM_config = config.get("AILLM", {})
    return AILLM(
        api_key=AILLM_config.get("api_key"),
        endpoint_url=AILLM_config.get("endpoint_url"),
        model_name=AILLM_config.get("model_name"),
        temperature=AILLM_config.get("temperature"),
        max_tokens=AILLM_config.get("max_tokens"),
        use_short_term_memory=AILLM_config.get("use_short_term_memory"),
    )


FACTORY_REGISTRY = {
    "postgres": create_postgres,
    "searcher": create_searcher,
    "LLM": create_agent,
    "LLMdatabase": create_LLMDatabase,
}

class ResourceManager:

    @classmethod
    def create(cls, name: str, full_config_dir: str):
        if name not in FACTORY_REGISTRY:
            raise ValueError(f"No factory registered for resource '{name}'")
        
        with open(full_config_dir, 'r', encoding='utf-8') as file:
            full_config = json.load(file)
            
        return FACTORY_REGISTRY[name](full_config)
