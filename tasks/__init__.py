# tasks/__init__.py

# market function
from tasks.market_data.fetch_share_data import FetchAllAShareSpot, FetchTargetShare, FetchAShareHistory, FetchKCAShareSpot, FetchGoldPrice, FetchHKMainSpot, UpdataRealSector, FetchFinAbstract, FetchFinReport, FinanceStatement
from tasks.market_data.fetch_Chinese_market_data import FetchCurrentMidPrice, FetchIntrestRate, FetchReserveRatio

# news function
from tasks.news_data.fetch_news import FetchDailyNews, FetchBingNews, FetchStockNews

# AI agent
from tasks.ai_agent_analysis.fetch_ali_llm_chat import FetchAILLMChat

# data and traditional analyze
from tasks.traditional_analysis.fetch_data import FetchCompanyData
from tasks.traditional_analysis.visualize import DrawGraph

TASK_CLASS_REGISTRY = {
    # market function
    "FetchAllAShareSpot": FetchAllAShareSpot,
    "FetchTargetShare": FetchTargetShare,
    "FetchAShareHistory": FetchAShareHistory,
    "FetchKCAShareSpot": FetchKCAShareSpot,
    "FetchGoldPrice": FetchGoldPrice,
    "FetchHKMainSpot": FetchHKMainSpot,
    "UpdataRealSector": UpdataRealSector,
    "FetchFinAbstract": FetchFinAbstract,
    "FetchFinReport": FetchFinReport,
    "FinanceStatement": FinanceStatement,
    
    "FetchCurrentMidPrice": FetchCurrentMidPrice,
    "FetchIntrestRate": FetchIntrestRate,
    "FetchReserveRatio": FetchReserveRatio,
    
    # news function
    "FetchDailyNews": FetchDailyNews,
    "FetchBingNews": FetchBingNews,
    "FetchStockNews": FetchStockNews,
    
    # AI agent
    "FetchAILLMChat": FetchAILLMChat,
    
    # data and traditional analyze
    "FetchCompanyData": FetchCompanyData,
    "DrawGraph": DrawGraph,
}
